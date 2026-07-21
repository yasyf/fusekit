package sourceauthority

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

type securePathSource struct{}

type directScanValue struct {
	entry PhysicalEntry
	err   error
}

type directScanSession struct {
	cancel context.CancelFunc
	values <-chan directScanValue
	done   <-chan struct{}
	result <-chan error

	nextMu    sync.Mutex
	stateMu   sync.Mutex
	pending   *PhysicalEntry
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func newSecurePathSource() PathSource { return securePathSource{} }

func (securePathSource) RootIdentity(ctx context.Context, root RootSpec) (FileIdentity, error) {
	if err := validateTaskRootDeclaration(root); err != nil {
		return FileIdentity{}, err
	}
	if root.ExpectedIdentity != (FileIdentity{}) {
		return FileIdentity{}, errors.New("sourceauthority: root identity discovery cannot accept a caller-supplied identity")
	}
	if err := ctx.Err(); err != nil {
		return FileIdentity{}, err
	}
	rootFD, err := openSecureRootPath(root)
	if err != nil {
		return FileIdentity{}, err
	}
	defer func() { _ = unix.Close(rootFD) }()
	return rootIdentityFromFD(rootFD)
}

func (s securePathSource) Stat(ctx context.Context, root RootSpec, relative string) (PhysicalEntry, error) {
	if err := validatePinnedTaskRoot(root); err != nil {
		return PhysicalEntry{}, err
	}
	if err := validateTaskRelative(root, relative); err != nil {
		return PhysicalEntry{}, err
	}
	rootFD, err := openSecureRoot(root)
	if err != nil {
		if root.Kind == RootFile && errors.Is(err, unix.ENOENT) {
			return PhysicalEntry{Root: root.ID, Relative: relative}, nil
		}
		return PhysicalEntry{}, err
	}
	defer func() { _ = unix.Close(rootFD) }()
	volume, err := rootIdentityFromFD(rootFD)
	if err != nil {
		return PhysicalEntry{}, err
	}
	entry, err := statAt(ctx, root, volume.VolumeUUID, rootFD, relative)
	if err != nil {
		return PhysicalEntry{}, err
	}
	if err := validateRootPathStillPinned(root); err != nil {
		return PhysicalEntry{}, err
	}
	return entry, nil
}

func (s securePathSource) BeginScan(ctx context.Context, roots []RootSpec) (ScanSession, error) {
	if _, err := validateFSEventsOpen(roots, nil); err != nil {
		return nil, err
	}
	scanCtx, cancel := context.WithCancel(ctx)
	values := make(chan directScanValue, 1)
	done := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		defer close(done)
		defer close(values)
		err := s.scanAll(scanCtx, roots, func(entry PhysicalEntry) error {
			select {
			case values <- directScanValue{entry: entry}:
				return nil
			case <-scanCtx.Done():
				return scanCtx.Err()
			}
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			select {
			case values <- directScanValue{err: err}:
			case <-scanCtx.Done():
			}
		}
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		result <- err
	}()
	return &directScanSession{cancel: cancel, values: values, done: done, result: result}, nil
}

func (s *directScanSession) Next(ctx context.Context, limit int) (ScanPage, error) {
	if limit <= 0 || limit > maxScanPageSize {
		return ScanPage{}, errors.New("sourceauthority: source scan limit is invalid")
	}
	s.nextMu.Lock()
	defer s.nextMu.Unlock()
	s.stateMu.Lock()
	if s.closed {
		s.stateMu.Unlock()
		return ScanPage{}, ErrClosed
	}
	s.stateMu.Unlock()
	page := ScanPage{Entries: make([]PhysicalEntry, 0, limit)}
	if s.pending != nil {
		page.Entries = append(page.Entries, *s.pending)
		s.pending = nil
	}
	for len(page.Entries) < limit {
		value, ok, err := s.receive(ctx)
		if err != nil {
			_ = s.shutdown()
			return ScanPage{}, err
		}
		if !ok {
			_ = s.shutdown()
			return page, nil
		}
		page.Entries = append(page.Entries, value.entry)
	}
	value, ok, err := s.receive(ctx)
	if err != nil {
		_ = s.shutdown()
		return ScanPage{}, err
	}
	if !ok {
		_ = s.shutdown()
		return page, nil
	}
	s.pending = &value.entry
	page.Next = "more"
	return page, nil
}

func (s *directScanSession) receive(ctx context.Context) (directScanValue, bool, error) {
	select {
	case value, ok := <-s.values:
		if ok && value.err != nil {
			return directScanValue{}, false, value.err
		}
		return value, ok, nil
	case <-ctx.Done():
		return directScanValue{}, false, ctx.Err()
	}
}

func (s *directScanSession) Close() error {
	return s.shutdown()
}

func (s *directScanSession) shutdown() error {
	s.stateMu.Lock()
	s.closed = true
	s.stateMu.Unlock()
	s.closeOnce.Do(func() {
		s.cancel()
		<-s.done
		s.closeErr = <-s.result
	})
	return s.closeErr
}

func (s securePathSource) scanAll(ctx context.Context, roots []RootSpec, emit func(PhysicalEntry) error) error {
	if emit == nil {
		return errors.New("sourceauthority: secure scan sink is required")
	}
	if _, err := validateFSEventsOpen(roots, nil); err != nil {
		return err
	}
	for _, root := range roots {
		rootFD, openErr := openSecureRoot(root)
		if openErr != nil {
			return openErr
		}
		identity, identityErr := rootIdentityFromFD(rootFD)
		if identityErr == nil {
			if root.Kind == RootFile {
				var entry PhysicalEntry
				entry, identityErr = statAt(ctx, root, identity.VolumeUUID, rootFD, ".")
				if identityErr == nil {
					identityErr = emit(entry)
				}
			} else {
				var rootStatus unix.Stat_t
				if identityErr = unix.Fstat(rootFD, &rootStatus); identityErr == nil {
					identityErr = walkSecureDirectory(ctx, root, identity.VolumeUUID, rootFD, uint64(rootStatus.Dev), emit)
				}
			}
		}
		_ = unix.Close(rootFD)
		if identityErr != nil {
			return identityErr
		}
		if err := validateRootPathStillPinned(root); err != nil {
			return err
		}
	}
	return nil
}

func validateTaskRootDeclaration(root RootSpec) error {
	if root.Authority == "" || root.ID == "" || root.Generation == 0 ||
		(root.Kind != RootFile && root.Kind != RootDirectory) || !filepath.IsAbs(root.Path) ||
		filepath.Clean(root.Path) != root.Path || strings.ContainsRune(root.Path, 0) {
		return errors.New("sourceauthority: invalid source task root")
	}
	return nil
}

func validatePinnedTaskRoot(root RootSpec) error {
	if err := validateTaskRootDeclaration(root); err != nil {
		return err
	}
	if err := validateFileIdentity(root.ExpectedIdentity); err != nil {
		return errors.Join(errors.New("sourceauthority: source task root is missing its identity fence"), err)
	}
	return nil
}

func validateTaskRelative(root RootSpec, relative string) error {
	if root.Kind == RootFile && relative != "." {
		return errors.New("sourceauthority: exact-file root only accepts relative dot")
	}
	if relative == "." {
		return nil
	}
	if relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) || strings.ContainsRune(relative, 0) {
		return errors.New("sourceauthority: invalid source task relative path")
	}
	return nil
}

func openSecureRoot(root RootSpec) (int, error) {
	if err := validatePinnedTaskRoot(root); err != nil {
		return -1, err
	}
	fd, err := openSecureRootPath(root)
	if err != nil {
		return -1, err
	}
	identity, err := rootIdentityFromFD(fd)
	if err != nil || identity != root.ExpectedIdentity {
		_ = unix.Close(fd)
		return -1, errors.Join(ErrSourceChanged, err)
	}
	return fd, nil
}

func openSecureRootPath(root RootSpec) (int, error) {
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, err
	}
	components := strings.Split(strings.TrimPrefix(root.Path, "/"), "/")
	for index, component := range components {
		if component == "" {
			continue
		}
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
		if index != len(components)-1 || root.Kind == RootDirectory {
			flags |= unix.O_DIRECTORY
		}
		next, openErr := unix.Openat(fd, component, flags, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return -1, fmt.Errorf("sourceauthority: securely open source root: %w", openErr)
		}
		fd = next
	}
	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil ||
		(root.Kind == RootFile && status.Mode&unix.S_IFMT != unix.S_IFREG) ||
		(root.Kind == RootDirectory && status.Mode&unix.S_IFMT != unix.S_IFDIR) {
		_ = unix.Close(fd)
		return -1, errors.Join(errors.New("sourceauthority: source root kind changed"), err)
	}
	return fd, nil
}

func validateRootPathStillPinned(root RootSpec) error {
	fd, err := openSecureRoot(root)
	if err != nil {
		return err
	}
	return unix.Close(fd)
}

func statAt(
	ctx context.Context,
	root RootSpec,
	volume string,
	rootFD int,
	relative string,
) (PhysicalEntry, error) {
	entry := PhysicalEntry{Root: root.ID, Relative: relative}
	if err := ctx.Err(); err != nil {
		return PhysicalEntry{}, err
	}
	if relative == "." {
		var status unix.Stat_t
		if err := unix.Fstat(rootFD, &status); err != nil {
			return PhysicalEntry{}, err
		}
		identity, err := platformFileIdentity(rootFD, "", 0, volume, status)
		if err != nil {
			return PhysicalEntry{}, err
		}
		return physicalEntryFromStat(entry, identity, status, "")
	}
	parent, leaf, err := openRelativeParent(rootFD, relative)
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return entry, nil
		}
		return PhysicalEntry{}, err
	}
	defer func() { _ = unix.Close(parent) }()
	var status unix.Stat_t
	if err := unix.Fstatat(parent, leaf, &status, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return entry, nil
		}
		return PhysicalEntry{}, err
	}
	return physicalChildAt(ctx, entry, volume, parent, leaf, status)
}

func physicalChildAt(
	ctx context.Context,
	entry PhysicalEntry,
	volume string,
	parentFD int,
	leaf string,
	status unix.Stat_t,
) (PhysicalEntry, error) {
	switch status.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		fd, err := unix.Openat(parentFD, leaf, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		if err != nil {
			return PhysicalEntry{}, err
		}
		defer func() { _ = unix.Close(fd) }()
		var pinned unix.Stat_t
		if err := unix.Fstat(fd, &pinned); err != nil || !samePinnedStat(status, pinned) {
			return PhysicalEntry{}, errors.Join(ErrSourceChanged, err)
		}
		identity, err := platformFileIdentity(fd, "", 0, volume, pinned)
		if err != nil {
			return PhysicalEntry{}, err
		}
		return physicalEntryFromStat(entry, identity, pinned, "")
	case unix.S_IFDIR:
		fd, err := unix.Openat(parentFD, leaf, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		if err != nil {
			return PhysicalEntry{}, err
		}
		defer func() { _ = unix.Close(fd) }()
		var pinned unix.Stat_t
		if err := unix.Fstat(fd, &pinned); err != nil || !samePinnedStat(status, pinned) {
			return PhysicalEntry{}, errors.Join(ErrSourceChanged, err)
		}
		identity, err := platformFileIdentity(fd, "", 0, volume, pinned)
		if err != nil {
			return PhysicalEntry{}, err
		}
		return physicalEntryFromStat(entry, identity, pinned, "")
	case unix.S_IFLNK:
		if status.Size < 0 || status.Size > maxScanSnapshotBytes {
			return PhysicalEntry{}, errors.New("sourceauthority: symlink target exceeds its bounded size")
		}
		target := make([]byte, status.Size+1)
		count, err := unix.Readlinkat(parentFD, leaf, target)
		if err != nil {
			return PhysicalEntry{}, err
		}
		if int64(count) != status.Size {
			return PhysicalEntry{}, ErrSourceChanged
		}
		var after unix.Stat_t
		if err := unix.Fstatat(parentFD, leaf, &after, unix.AT_SYMLINK_NOFOLLOW); err != nil || !samePinnedStat(status, after) {
			return PhysicalEntry{}, errors.Join(ErrSourceChanged, err)
		}
		identity, err := platformFileIdentity(parentFD, leaf, unix.AT_SYMLINK_NOFOLLOW, volume, after)
		if err != nil {
			return PhysicalEntry{}, err
		}
		return physicalEntryFromStat(entry, identity, after, string(target[:count]))
	default:
		return PhysicalEntry{}, errors.New("sourceauthority: unsupported physical object kind")
	}

}

func physicalEntryFromStat(
	entry PhysicalEntry,
	identity FileIdentity,
	status unix.Stat_t,
	linkTarget string,
) (PhysicalEntry, error) {
	entry.Exists = true
	entry.Identity = identity
	entry.Mode = uint32(status.Mode)
	entry.UID = status.Uid
	entry.GID = status.Gid
	entry.Size = status.Size
	entry.LinkTarget = linkTarget
	switch status.Mode & unix.S_IFMT {
	case unix.S_IFREG:
		entry.Kind = PhysicalFile
	case unix.S_IFDIR:
		entry.Kind = PhysicalDirectory
	case unix.S_IFLNK:
		entry.Kind = PhysicalSymlink
	default:
		return PhysicalEntry{}, errors.New("sourceauthority: unsupported physical object kind")
	}
	metadata := sha256.New()
	_, _ = fmt.Fprintf(metadata, "%s\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%d\x00%s",
		entry.Identity.VolumeUUID, entry.Identity.Inode, entry.Identity.BirthtimeSec,
		entry.Identity.BirthtimeNsec, entry.Mode, entry.UID, entry.GID, entry.Size, status.Mtim.Sec, status.Mtim.Nsec,
		status.Ctim.Sec, status.Ctim.Nsec, entry.LinkTarget)
	copy(entry.MetadataFingerprint[:], metadata.Sum(nil))
	content := sha256.New()
	_, _ = fmt.Fprintf(content, "%d\x00%d\x00%d\x00%d\x00%s", entry.Size, status.Mtim.Sec, status.Mtim.Nsec, entry.Mode, entry.LinkTarget)
	copy(entry.ContentFingerprint[:], content.Sum(nil))
	return entry, nil
}

func samePinnedStat(left, right unix.Stat_t) bool {
	return left.Dev == right.Dev && left.Ino == right.Ino && left.Mode == right.Mode && left.Uid == right.Uid &&
		left.Gid == right.Gid && left.Size == right.Size &&
		left.Mtim == right.Mtim && left.Ctim == right.Ctim
}

func openRelativeParent(rootFD int, relative string) (int, string, error) {
	components := strings.Split(relative, string(filepath.Separator))
	fd, err := unix.Dup(rootFD)
	if err != nil {
		return -1, "", err
	}
	for _, component := range components[:len(components)-1] {
		next, openErr := unix.Openat(fd, component, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return -1, "", openErr
		}
		fd = next
	}
	return fd, components[len(components)-1], nil
}

func walkSecureDirectory(
	ctx context.Context,
	root RootSpec,
	volume string,
	rootFD int,
	rootDevice uint64,
	emit func(PhysicalEntry) error,
) error {
	type directoryFrame struct {
		fd        int
		base      string
		directory *os.File
	}
	duplicate, err := unix.Dup(rootFD)
	if err != nil {
		return err
	}
	stack := []directoryFrame{{
		fd: duplicate, directory: os.NewFile(uintptr(duplicate), "source-directory"),
	}}
	defer func() {
		for index := range stack {
			_ = stack[index].directory.Close()
		}
	}()
	for len(stack) != 0 {
		frame := &stack[len(stack)-1]
		entries, readErr := frame.directory.ReadDir(256)
		if readErr != nil && len(entries) == 0 {
			if errors.Is(readErr, os.ErrClosed) {
				return readErr
			}
			_ = frame.directory.Close()
			stack = stack[:len(stack)-1]
			continue
		}
		for _, child := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			relative := child.Name()
			if frame.base != "" {
				relative = filepath.Join(frame.base, child.Name())
			}
			var status unix.Stat_t
			if err := unix.Fstatat(frame.fd, child.Name(), &status, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				return err
			}
			if err := sameSourceDevice(rootDevice, status); err != nil {
				return err
			}
			entry, err := physicalChildAt(ctx, PhysicalEntry{Root: root.ID, Relative: relative}, volume, frame.fd, child.Name(), status)
			if err != nil {
				return err
			}
			if err := emit(entry); err != nil {
				return err
			}
			if entry.Kind == PhysicalDirectory {
				childFD, err := unix.Openat(frame.fd, child.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
				if err != nil {
					return err
				}
				var pinned unix.Stat_t
				if err := unix.Fstat(childFD, &pinned); err != nil || !samePinnedStat(status, pinned) || uint64(pinned.Dev) != rootDevice {
					_ = unix.Close(childFD)
					if err == nil {
						err = ErrSourceChanged
					}
					return err
				}
				stack = append(stack, directoryFrame{
					fd: childFD, base: relative,
					directory: os.NewFile(uintptr(childFD), "source-directory"),
				})
			}
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return readErr
		}
	}
	return nil
}

func sameSourceDevice(rootDevice uint64, status unix.Stat_t) error {
	if uint64(status.Dev) != rootDevice {
		return errors.New("sourceauthority: source descendant crossed its pinned volume")
	}
	return nil
}

func scanRootsDigest(roots []RootSpec) [32]byte {
	hash := sha256.New()
	for _, root := range roots {
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00%s\x00%d\x00%d\n", root.Authority, root.ID, root.Path, root.Kind, root.Generation)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func rootIdentityFromFD(fd int) (FileIdentity, error) {
	var status unix.Stat_t
	if err := unix.Fstat(fd, &status); err != nil {
		return FileIdentity{}, err
	}
	return platformRootIdentity(fd, status)
}

var _ PathSource = securePathSource{}
