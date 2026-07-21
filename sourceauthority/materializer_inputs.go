package sourceauthority

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/yasyf/fusekit/tenant"
	"golang.org/x/sys/unix"
)

const maxMaterializationInputBytes int64 = 1 << 30

type immutableSnapshot struct {
	mu     sync.RWMutex
	file   *os.File
	size   int64
	closed bool
}

type immutableSnapshotSet struct {
	values []*immutableSnapshot
	once   sync.Once
	err    error
}

func (s *immutableSnapshot) Open(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.file == nil {
		return nil, ErrClosed
	}
	return io.NopCloser(io.NewSectionReader(s.file, 0, s.size)), nil
}

func (s *immutableSnapshot) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.file == nil {
		return nil
	}
	return s.file.Close()
}

func (s *immutableSnapshotSet) Close() error {
	s.once.Do(func() {
		for _, value := range s.values {
			s.err = errors.Join(s.err, value.close())
		}
	})
	return s.err
}

func prepareMaterializerTask(
	ctx context.Context,
	runtimeDir string,
	task MaterializationTask,
) (_ MaterializerTask, snapshots *immutableSnapshotSet, resultErr error) {
	if err := validateChildMaterializationTask(task); err != nil {
		return MaterializerTask{}, nil, err
	}
	snapshots = &immutableSnapshotSet{}
	defer func() {
		if resultErr != nil {
			_ = snapshots.Close()
		}
	}()
	roots := make(map[RootID]RootSpec, len(task.Roots))
	for _, root := range task.Roots {
		roots[root.ID] = root
	}
	inputs := make([]MaterializerInput, len(task.Request.Inputs))
	var total int64
	for index, reference := range task.Request.Inputs {
		root := roots[reference.Root]
		entry, content, err := snapshotMaterializerInput(ctx, runtimeDir, root, reference.Relative, task.Expected[index])
		if err != nil {
			return MaterializerTask{}, snapshots, err
		}
		if content != nil {
			total += content.size
			if total < 0 || total > maxMaterializationInputBytes {
				_ = content.close()
				return MaterializerTask{}, snapshots, errors.New("sourceauthority: materialization inputs exceed their bounded size")
			}
			snapshots.values = append(snapshots.values, content)
		}
		inputs[index] = MaterializerInput{Physical: entry, Content: content}
	}
	return MaterializerTask{
		Fence: task.Fence, Tenants: append([]tenant.TenantSpec(nil), task.Tenants...),
		Logical: task.Request.Logical, Payload: append([]byte(nil), task.Request.Payload...), Inputs: inputs,
	}, snapshots, nil
}

func snapshotMaterializerInput(
	ctx context.Context,
	runtimeDir string,
	root RootSpec,
	relative string,
	expected PhysicalEntry,
) (PhysicalEntry, *immutableSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return PhysicalEntry{}, nil, err
	}
	rootFD, err := openSecureRoot(root)
	if err != nil {
		return PhysicalEntry{}, nil, err
	}
	defer func() { _ = unix.Close(rootFD) }()
	identity, err := rootIdentityFromFD(rootFD)
	if err != nil {
		return PhysicalEntry{}, nil, err
	}
	entry, err := statAt(ctx, root, identity.VolumeUUID, rootFD, relative)
	if err != nil {
		return PhysicalEntry{}, nil, err
	}
	if !samePhysical(entry, expected) {
		return PhysicalEntry{}, nil, ErrSourceChanged
	}
	if entry.Kind != PhysicalFile {
		return entry, nil, nil
	}
	fd, err := openPinnedRegular(rootFD, relative)
	if err != nil {
		return PhysicalEntry{}, nil, err
	}
	source := os.NewFile(uintptr(fd), "source-input")
	defer func() { _ = source.Close() }()
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return PhysicalEntry{}, nil, err
	}
	pinnedIdentity, err := platformFileIdentity(fd, "", 0, identity.VolumeUUID, before)
	if err != nil {
		return PhysicalEntry{}, nil, err
	}
	pinned, err := physicalEntryFromStat(
		PhysicalEntry{Root: root.ID, Relative: relative}, pinnedIdentity, before, "",
	)
	if err != nil || !samePhysical(pinned, expected) {
		return PhysicalEntry{}, nil, errors.Join(ErrSourceChanged, err)
	}
	if before.Size < 0 || before.Size > maxMaterializationInputBytes {
		return PhysicalEntry{}, nil, errors.New("sourceauthority: materialization input exceeds its bounded size")
	}
	temporary, err := os.CreateTemp(runtimeDir, "source-input-")
	if err != nil {
		return PhysicalEntry{}, nil, err
	}
	path := temporary.Name()
	if err := os.Chmod(path, 0o600); err != nil {
		_ = temporary.Close()
		_ = os.Remove(path)
		return PhysicalEntry{}, nil, err
	}
	if err := os.Remove(path); err != nil {
		_ = temporary.Close()
		return PhysicalEntry{}, nil, err
	}
	reader := io.NewSectionReader(source, 0, before.Size)
	_, copyErr := io.CopyBuffer(temporary, reader, make([]byte, sourceTaskChunkSize))
	var after unix.Stat_t
	statErr := unix.Fstat(fd, &after)
	if copyErr != nil || statErr != nil || !samePinnedStat(before, after) {
		_ = temporary.Close()
		return PhysicalEntry{}, nil, errors.Join(ErrSourceChanged, copyErr, statErr)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return PhysicalEntry{}, nil, err
	}
	if err := validateRootPathStillPinned(root); err != nil {
		_ = temporary.Close()
		return PhysicalEntry{}, nil, err
	}
	return pinned, &immutableSnapshot{file: temporary, size: before.Size}, nil
}

func openPinnedRegular(rootFD int, relative string) (int, error) {
	if relative == "." {
		fd, err := unix.Dup(rootFD)
		if err != nil {
			return -1, err
		}
		var status unix.Stat_t
		if err := unix.Fstat(fd, &status); err != nil || status.Mode&unix.S_IFMT != unix.S_IFREG {
			_ = unix.Close(fd)
			return -1, errors.Join(errors.New("sourceauthority: materialization input is not regular"), err)
		}
		return fd, nil
	}
	parent, leaf, err := openRelativeParent(rootFD, relative)
	if err != nil {
		return -1, err
	}
	defer func() { _ = unix.Close(parent) }()
	return unix.Openat(parent, leaf, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
}

var _ ImmutableContent = (*immutableSnapshot)(nil)
