//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"errors"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

const (
	directoryPage = 128
	invalidHandle = ^uint64(0)
)

type routeSource interface {
	Resolver
	RoutePage(context.Context, RouteCursor, int) (RoutePage, error)
}

type fileHandle struct {
	mu       sync.Mutex
	pin      *PinnedRoute
	fs       *CatalogFS
	object   catalog.Object
	snapshot *NativeSnapshot
	staging  *NativeWriteStage
	dirty    bool
}

type directoryHandle struct {
	mu            sync.Mutex
	pin           *PinnedRoute
	fs            *CatalogFS
	parent        catalog.ObjectID
	revision      catalog.Revision
	cursor        catalog.SnapshotCursor
	entries       []Entry
	complete      bool
	root          bool
	routes        []rootEntry
	routeCursor   RouteCursor
	routeComplete bool
	rootPins      []*PinnedRoute
}

type rootEntry struct {
	name  string
	entry Entry
}

// FuseFS presents one catalog-backed native root through immutable tenant routes.
type FuseFS struct {
	fuse.FileSystemBase

	catalog  NativeCatalog
	resolver routeSource
	inodes   *InodeRegistry
	uid      uint32
	gid      uint32
	created  fuse.Timespec

	mu          sync.Mutex
	nextHandle  uint64
	files       map[uint64]*fileHandle
	directories map[uint64]*directoryHandle

	mutationMu       sync.Mutex
	mutationLanes    map[catalog.TenantID]*sync.Mutex
	initOnce         sync.Once
	initialized      chan struct{}
	rootCatalogEpoch atomic.Uint64
}

// NewFuseFS constructs the callback adapter without mounting it.
func NewFuseFS(source NativeCatalog, resolver Resolver) (*FuseFS, error) {
	if source == nil {
		return nil, errors.New("mountmux: nil callback catalog")
	}
	routes, ok := resolver.(routeSource)
	if !ok {
		return nil, errors.New("mountmux: callback resolver does not expose immutable routes")
	}
	now := time.Now()
	return &FuseFS{
		catalog: source, resolver: routes, inodes: NewInodeRegistry(),
		uid: uint32(os.Getuid()), gid: uint32(os.Getgid()),
		created:    fuse.Timespec{Sec: now.Unix(), Nsec: int64(now.Nanosecond())},
		nextHandle: 1, files: make(map[uint64]*fileHandle),
		directories: make(map[uint64]*directoryHandle), mutationLanes: make(map[catalog.TenantID]*sync.Mutex),
		initialized: make(chan struct{}),
	}, nil
}

// Init records the native lifecycle callback before any filesystem operation is admitted.
func (fs *FuseFS) Init() { fs.initOnce.Do(func() { close(fs.initialized) }) }

func (fs *FuseFS) rootReadEpoch() uint64 { return fs.rootCatalogEpoch.Load() }

// FusePassthroughOnly reports that this filesystem serves catalog snapshots by handle.
func (*FuseFS) FusePassthroughOnly() bool { return false }

func (fs *FuseFS) rootStat(stat *fuse.Stat_t) {
	*stat = fuse.Stat_t{
		Ino: mountRootInode, Mode: fuse.S_IFDIR | 0o755, Nlink: 2,
		Uid: fs.uid, Gid: fs.gid, Atim: fs.created, Mtim: fs.created,
		Ctim: fs.created, Birthtim: fs.created, Blksize: 4096,
	}
}

func (fs *FuseFS) objectStat(entry Entry, stat *fuse.Stat_t) int {
	if entry.Inode == mountRootInode {
		return -int(syscall.EIO)
	}
	mode := entry.Object.Mode
	switch entry.Object.Kind {
	case catalog.KindDirectory:
		mode |= fuse.S_IFDIR
	case catalog.KindFile:
		mode |= fuse.S_IFREG
	case catalog.KindSymlink:
		mode |= fuse.S_IFLNK
	default:
		return -int(syscall.EIO)
	}
	nlink := uint32(1)
	if entry.Object.Kind == catalog.KindDirectory {
		nlink = 2
	}
	metadataTime := fuse.Timespec{Sec: int64(entry.Object.MetadataRevision)}
	contentTime := fuse.Timespec{Sec: int64(entry.Object.ContentRevision)}
	*stat = fuse.Stat_t{
		Ino: entry.Inode, Mode: mode, Nlink: nlink, Uid: fs.uid, Gid: fs.gid,
		Size: entry.Object.Size, Atim: contentTime, Mtim: contentTime,
		Ctim: metadataTime, Birthtim: metadataTime, Blksize: 4096,
		Blocks: (entry.Object.Size + 511) / 512,
	}
	return 0
}

// Readlink returns an exact catalog target without materializing body content.
func (fs *FuseFS) Readlink(value string) (int, string) {
	pin, _, entry, err := fs.resolve(context.Background(), value)
	if err != nil {
		return errno(err), ""
	}
	defer pin.Release()
	if entry.Object.Kind != catalog.KindSymlink {
		return -int(syscall.EINVAL), ""
	}
	return 0, entry.Object.LinkTarget
}

func splitTenantPath(value string) (string, []string, error) {
	if value == "/" {
		return "", nil, nil
	}
	if value == "" || value[0] != '/' || path.Clean(value) != value {
		return "", nil, catalog.ErrInvalidObject
	}
	parts := strings.Split(strings.TrimPrefix(value, "/"), "/")
	for _, part := range parts {
		if appleDouble(part) {
			return "", nil, catalog.ErrNotFound
		}
	}
	return parts[0], parts[1:], nil
}

func (fs *FuseFS) pinPath(ctx context.Context, value string) (*PinnedRoute, *CatalogFS, []string, error) {
	name, parts, err := splitTenantPath(value)
	if err != nil {
		return nil, nil, nil, err
	}
	if name == "" {
		return nil, nil, nil, catalog.ErrInvalidObject
	}
	pin, err := fs.resolver.Pin(ctx, name)
	if err != nil {
		return nil, nil, nil, err
	}
	view := newCatalogFS(fs.catalog, pin.Route.Tenant, pin.Route.Generation, fs.inodes)
	return pin, view, parts, nil
}

func lookupParts(ctx context.Context, view *CatalogFS, parts []string) (Entry, error) {
	entry, err := view.Root(ctx)
	if err != nil {
		return Entry{}, err
	}
	for _, name := range parts {
		if entry.Object.Kind != catalog.KindDirectory {
			return Entry{}, syscall.ENOTDIR
		}
		entry, err = view.LookupName(ctx, entry.Object.ID, name)
		if err != nil {
			return Entry{}, err
		}
	}
	return entry, nil
}

func (fs *FuseFS) resolve(ctx context.Context, value string) (*PinnedRoute, *CatalogFS, Entry, error) {
	pin, view, parts, err := fs.pinPath(ctx, value)
	if err != nil {
		return nil, nil, Entry{}, err
	}
	entry, err := lookupParts(ctx, view, parts)
	if err != nil {
		pin.Release()
		return nil, nil, Entry{}, err
	}
	return pin, view, entry, nil
}

func (fs *FuseFS) resolveParent(ctx context.Context, value string) (*PinnedRoute, *CatalogFS, Entry, string, error) {
	pin, view, parts, err := fs.pinPath(ctx, value)
	if err != nil {
		return nil, nil, Entry{}, "", err
	}
	if len(parts) == 0 {
		pin.Release()
		return nil, nil, Entry{}, "", syscall.EPERM
	}
	name := parts[len(parts)-1]
	if appleDouble(name) {
		pin.Release()
		return nil, nil, Entry{}, "", syscall.EACCES
	}
	parent, err := lookupParts(ctx, view, parts[:len(parts)-1])
	if err != nil {
		pin.Release()
		return nil, nil, Entry{}, "", err
	}
	if parent.Object.Kind != catalog.KindDirectory {
		pin.Release()
		return nil, nil, Entry{}, "", syscall.ENOTDIR
	}
	return pin, view, parent, name, nil
}

// Getattr serves stable catalog identity and revision attributes.
func (fs *FuseFS) Getattr(value string, stat *fuse.Stat_t, handle uint64) int {
	if value == "/" {
		fs.rootStat(stat)
		return 0
	}
	if handle != invalidHandle {
		fs.mu.Lock()
		opened := fs.files[handle]
		fs.mu.Unlock()
		if opened != nil {
			entry, err := opened.fs.entry(opened.object)
			if err != nil {
				return errno(err)
			}
			if rc := fs.objectStat(entry, stat); rc != 0 {
				return rc
			}
			if opened.staging != nil {
				size := opened.staging.Size()
				stat.Size = size
				stat.Blocks = (size + 511) / 512
			}
			return 0
		}
	}
	pin, _, entry, err := fs.resolve(context.Background(), value)
	if err != nil {
		return errno(err)
	}
	defer pin.Release()
	return fs.objectStat(entry, stat)
}

// Open opens one immutable catalog snapshot or a private write staging file.
func (fs *FuseFS) Open(value string, flags int) (int, uint64) {
	pin, view, entry, err := fs.resolve(context.Background(), value)
	if err != nil {
		return errno(err), invalidHandle
	}
	if entry.Object.Kind != catalog.KindFile {
		pin.Release()
		if entry.Object.Kind == catalog.KindSymlink {
			return -int(syscall.ELOOP), invalidHandle
		}
		return -int(syscall.EISDIR), invalidHandle
	}
	if flags&syscall.O_ACCMODE != syscall.O_RDONLY {
		if pin.Spec.Traits.Access != tenant.ReadWrite {
			pin.Release()
			return -int(syscall.EROFS), invalidHandle
		}
		opened, err := fs.newWriteHandle(context.Background(), pin, view, entry.Object)
		if err != nil {
			pin.Release()
			return errno(err), invalidHandle
		}
		if flags&syscall.O_TRUNC != 0 {
			if err := opened.staging.Truncate(0); err != nil {
				_ = opened.staging.Close()
				pin.Release()
				return errno(err), invalidHandle
			}
			opened.dirty = true
		}
		return 0, fs.storeFile(opened)
	}
	head, err := view.Head(context.Background())
	if err != nil {
		pin.Release()
		return errno(err), invalidHandle
	}
	snapshot, err := view.Open(context.Background(), entry.Object.ID, head)
	if err != nil {
		pin.Release()
		return errno(err), invalidHandle
	}
	return 0, fs.storeFile(&fileHandle{pin: pin, fs: view, object: snapshot.Object, snapshot: snapshot})
}

// Read reads from the exact open-time snapshot or private write staging file.
func (fs *FuseFS) Read(_ string, buffer []byte, offset int64, handle uint64) int {
	opened := fs.file(handle)
	if opened == nil {
		return -int(syscall.EBADF)
	}
	if offset < 0 {
		return -int(syscall.EINVAL)
	}
	var n int
	var err error
	if opened.staging != nil {
		n, err = opened.staging.ReadAt(buffer, offset)
	} else {
		n, err = opened.snapshot.ReadAt(buffer, offset)
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return errno(err)
	}
	return n
}

// Release closes the catalog snapshot and releases its exact generation pin.
func (fs *FuseFS) Release(_ string, handle uint64) int {
	opened := fs.takeFile(handle)
	if opened == nil {
		return -int(syscall.EBADF)
	}
	err := fs.commitWrite(context.Background(), opened)
	if opened.snapshot != nil {
		err = errors.Join(err, opened.snapshot.Close())
	}
	if opened.staging != nil {
		err = errors.Join(err, opened.staging.Close())
	}
	err = errors.Join(err, opened.pin.Release())
	return errno(err)
}

// Opendir captures one exact tenant generation and immutable catalog revision.
func (fs *FuseFS) Opendir(value string) (int, uint64) {
	if value == "/" {
		return 0, fs.storeDirectory(&directoryHandle{root: true})
	}
	pin, view, entry, err := fs.resolve(context.Background(), value)
	if err != nil {
		return errno(err), invalidHandle
	}
	if entry.Object.Kind != catalog.KindDirectory {
		pin.Release()
		return -int(syscall.ENOTDIR), invalidHandle
	}
	revision, err := view.Head(context.Background())
	if err != nil {
		pin.Release()
		return errno(err), invalidHandle
	}
	return 0, fs.storeDirectory(&directoryHandle{pin: pin, fs: view, parent: entry.Object.ID, revision: revision})
}

// Readdir pages one immutable catalog snapshot and never rebuilds the tenant root.
func (fs *FuseFS) Readdir(_ string, fill func(string, *fuse.Stat_t, int64) bool, offset int64, handle uint64) int {
	directory := fs.directory(handle)
	if directory == nil || offset < 0 {
		return -int(syscall.EBADF)
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if directory.root {
		if err := fs.loadRootDirectory(context.Background(), directory, 0); err != nil {
			return errno(err)
		}
	}
	if offset == 0 && !fill(".", nil, 1) {
		return 0
	}
	if offset <= 1 && !fill("..", nil, 2) {
		return 0
	}
	index := int(offset) - 2
	if index < 0 {
		index = 0
	}
	if directory.root {
		for {
			if err := fs.loadRootDirectory(context.Background(), directory, index); err != nil {
				return errno(err)
			}
			if index >= len(directory.routes) {
				return 0
			}
			var stat fuse.Stat_t
			if rc := fs.objectStat(directory.routes[index].entry, &stat); rc != 0 {
				return rc
			}
			if !fill(directory.routes[index].name, &stat, int64(index+3)) {
				return 0
			}
			index++
		}
	}
	for {
		if err := fs.loadDirectory(context.Background(), directory, index); err != nil {
			return errno(err)
		}
		if index >= len(directory.entries) {
			return 0
		}
		entry := directory.entries[index]
		index++
		if appleDouble(entry.Object.Name) {
			continue
		}
		var stat fuse.Stat_t
		if rc := fs.objectStat(entry, &stat); rc != 0 {
			return rc
		}
		if !fill(entry.Object.Name, &stat, int64(index+2)) {
			return 0
		}
	}
}

// Releasedir releases the exact tenant-generation pin retained by Opendir.
func (fs *FuseFS) Releasedir(_ string, handle uint64) int {
	directory := fs.takeDirectory(handle)
	if directory == nil {
		return -int(syscall.EBADF)
	}
	if directory.pin != nil {
		if err := directory.pin.Release(); err != nil {
			return errno(err)
		}
	}
	var releaseErr error
	for _, pin := range directory.rootPins {
		releaseErr = errors.Join(releaseErr, pin.Release())
	}
	return errno(releaseErr)
}

func (fs *FuseFS) loadRootDirectory(ctx context.Context, directory *directoryHandle, index int) error {
	for index >= len(directory.routes) && !directory.routeComplete {
		page, err := fs.resolver.RoutePage(ctx, directory.routeCursor, MaxRoutePageSize)
		if err != nil {
			return err
		}
		if page.Snapshot == 0 ||
			(directory.routeCursor.Snapshot != 0 && page.Snapshot != directory.routeCursor.Snapshot) ||
			len(page.Routes) > MaxRoutePageSize {
			return catalog.ErrIntegrity
		}
		previous := directory.routeCursor.After
		for _, route := range page.Routes {
			if previous != "" && strings.Compare(previous, route.Name) >= 0 {
				return catalog.ErrIntegrity
			}
			previous = route.Name
		}
		if page.Next != nil && (len(page.Routes) == 0 ||
			page.Next.Snapshot != page.Snapshot ||
			page.Next.After != page.Routes[len(page.Routes)-1].Name) {
			return catalog.ErrIntegrity
		}
		pagePins := make([]*PinnedRoute, 0, len(page.Routes))
		pageEntries := make([]rootEntry, 0, len(page.Routes))
		for _, route := range page.Routes {
			pin, err := fs.resolver.Pin(ctx, route.Name)
			if err != nil {
				for _, acquired := range pagePins {
					_ = acquired.Release()
				}
				return err
			}
			pagePins = append(pagePins, pin)
			if pin.Route != route {
				for _, acquired := range pagePins {
					_ = acquired.Release()
				}
				return tenant.ErrGenerationConflict
			}
			view := newCatalogFS(fs.catalog, route.Tenant, route.Generation, fs.inodes)
			entry, err := view.Root(ctx)
			if err != nil {
				for _, acquired := range pagePins {
					_ = acquired.Release()
				}
				return err
			}
			pageEntries = append(pageEntries, rootEntry{name: route.Name, entry: entry})
		}
		directory.rootPins = append(directory.rootPins, pagePins...)
		directory.routes = append(directory.routes, pageEntries...)
		fs.rootCatalogEpoch.Add(1)
		if page.Next == nil {
			directory.routeComplete = true
			continue
		}
		directory.routeCursor = *page.Next
	}
	return nil
}

func (fs *FuseFS) loadDirectory(ctx context.Context, directory *directoryHandle, index int) error {
	for index >= len(directory.entries) && !directory.complete {
		page, err := directory.fs.ReadDir(ctx, directory.parent, directory.revision, directory.cursor, directoryPage)
		if err != nil {
			return err
		}
		directory.entries = append(directory.entries, page.Entries...)
		if page.Next == nil {
			directory.complete = true
			continue
		}
		directory.cursor = *page.Next
	}
	return nil
}

func (fs *FuseFS) storeFile(opened *fileHandle) uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	handle := fs.nextHandle
	fs.nextHandle++
	fs.files[handle] = opened
	return handle
}

func (fs *FuseFS) file(handle uint64) *fileHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.files[handle]
}

func (fs *FuseFS) takeFile(handle uint64) *fileHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	opened := fs.files[handle]
	delete(fs.files, handle)
	return opened
}

func (fs *FuseFS) storeDirectory(directory *directoryHandle) uint64 {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	handle := fs.nextHandle
	fs.nextHandle++
	fs.directories[handle] = directory
	return handle
}

func (fs *FuseFS) directory(handle uint64) *directoryHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.directories[handle]
}

func (fs *FuseFS) takeDirectory(handle uint64) *directoryHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	directory := fs.directories[handle]
	delete(fs.directories, handle)
	return directory
}

func errno(err error) int {
	if err == nil {
		return 0
	}
	var system syscall.Errno
	if errors.As(err, &system) {
		return -int(system)
	}
	switch {
	case errors.Is(err, catalog.ErrNotFound):
		return -int(syscall.ENOENT)
	case errors.Is(err, catalog.ErrConflict), errors.Is(err, catalog.ErrMutationConflict):
		return -int(syscall.EEXIST)
	case errors.Is(err, catalog.ErrGenerationMismatch),
		errors.Is(err, tenant.ErrGenerationConflict), errors.Is(err, tenant.ErrTenantChanging):
		return -int(syscall.ESTALE)
	case errors.Is(err, catalog.ErrMutationActive), errors.Is(err, catalog.ErrMutationClaimed),
		errors.Is(err, catalog.ErrMutationRecoveryRequired):
		return -int(syscall.EBUSY)
	case errors.Is(err, context.Canceled):
		return -int(syscall.EINTR)
	case errors.Is(err, context.DeadlineExceeded):
		return -int(syscall.ETIMEDOUT)
	case errors.Is(err, catalog.ErrInvalidObject), errors.Is(err, catalog.ErrInvalidTransition):
		return -int(syscall.EINVAL)
	case errors.Is(err, catalog.ErrIntegrity), errors.Is(err, ErrInodeCollision):
		return -int(syscall.EIO)
	default:
		return -int(syscall.EIO)
	}
}

var _ fuse.FileSystemInterface = (*FuseFS)(nil)
