// Package mountmux projects catalog tenants as filesystem-neutral mounted children.
package mountmux

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

var inodeDomain = []byte("fusekit.mountmux.inode.v1\x00")

const mountRootInode = uint64(1)

// ErrInodeCollision means two catalog identities projected to the same kernel cache key.
var ErrInodeCollision = errors.New("mountmux: inode candidate collision")

// ErrUnknownInode means no catalog identity is bound to a kernel cache key.
var ErrUnknownInode = errors.New("mountmux: unknown inode")

// Entry is one catalog object with its deterministic kernel cache key.
type Entry struct {
	Object catalog.Object
	Inode  uint64
}

// DirectoryPage is one immutable catalog snapshot page.
type DirectoryPage struct {
	Revision catalog.Revision
	Entries  []Entry
	Next     *catalog.SnapshotCursor
}

// NativeCatalog is the exact generation-fenced remote catalog surface consumed
// by native callbacks.
type NativeCatalog interface {
	Root(context.Context, catalog.TenantID, catalog.Generation) (catalog.Object, error)
	Head(context.Context, catalog.TenantID, catalog.Generation) (catalog.Revision, error)
	Lookup(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID) (catalog.Object, error)
	LookupName(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID, string) (catalog.Object, error)
	Snapshot(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision, catalog.SnapshotCursor, int) (catalog.SnapshotPage, error)
	Open(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (*NativeSnapshot, error)
	OpenWrite(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (*NativeWriteStage, error)
	Mutate(context.Context, catalog.TenantID, catalog.Generation, catalogproto.MutationRequest, io.Reader) (catalogproto.MutationResponse, error)
}

// NativeSnapshot is one exact object revision owned by the remote catalog
// generation.
type NativeSnapshot struct {
	Object catalog.Object
	Source interface {
		io.Reader
		io.ReaderAt
		io.Closer
	}
}

// Read forwards sequential reads to the exact snapshot.
func (s *NativeSnapshot) Read(buffer []byte) (int, error) { return s.Source.Read(buffer) }

// ReadAt forwards random reads to the exact snapshot.
func (s *NativeSnapshot) ReadAt(buffer []byte, offset int64) (int, error) {
	return s.Source.ReadAt(buffer, offset)
}

// Close releases the exact snapshot.
func (s *NativeSnapshot) Close() error { return s.Source.Close() }

// NativeWriteStage is one worker-owned mutable staging handle.
type NativeWriteStage struct {
	Object catalog.Object
	Source interface {
		io.ReaderAt
		io.WriterAt
		Truncate(int64) error
		Sync() error
		Size() int64
		Commit(context.Context) (catalog.Object, error)
		io.Closer
	}
}

// ReadAt reads mutable staged bytes.
func (s *NativeWriteStage) ReadAt(buffer []byte, offset int64) (int, error) {
	return s.Source.ReadAt(buffer, offset)
}

// WriteAt writes mutable staged bytes.
func (s *NativeWriteStage) WriteAt(buffer []byte, offset int64) (int, error) {
	return s.Source.WriteAt(buffer, offset)
}

// Truncate changes the staged size.
func (s *NativeWriteStage) Truncate(size int64) error { return s.Source.Truncate(size) }

// Sync durably syncs staged bytes.
func (s *NativeWriteStage) Sync() error { return s.Source.Sync() }

// Size returns the current staged size.
func (s *NativeWriteStage) Size() int64 { return s.Source.Size() }

// Commit publishes the current staged bytes and retains the mutable handle.
func (s *NativeWriteStage) Commit(ctx context.Context) (catalog.Object, error) {
	object, err := s.Source.Commit(ctx)
	if err == nil {
		s.Object = object
	}
	return object, err
}

// Close discards the mutable staging handle.
func (s *NativeWriteStage) Close() error { return s.Source.Close() }

// CatalogFS is one exact tenant generation projected through the mount presentation.
type CatalogFS struct {
	catalog    NativeCatalog
	tenant     catalog.TenantID
	generation catalog.Generation
	inodes     *InodeRegistry
}

// InodeRegistry verifies every 64-bit kernel key against its full catalog identity.
type InodeRegistry struct {
	candidate func(catalog.ObjectID) uint64
	inodesMu  sync.RWMutex
	byObject  map[catalog.ObjectID]uint64
	byInode   map[uint64]catalog.ObjectID
}

// NewCatalogFS binds a mounted child to one exact tenant generation.
func NewCatalogFS(ctx context.Context, source NativeCatalog, tenant catalog.TenantID, generation catalog.Generation) (*CatalogFS, error) {
	return NewCatalogFSWithRegistry(ctx, source, tenant, generation, NewInodeRegistry())
}

// NewCatalogFSWithRegistry binds a mounted child to a process-wide verified inode registry.
func NewCatalogFSWithRegistry(
	ctx context.Context,
	source NativeCatalog,
	tenant catalog.TenantID,
	generation catalog.Generation,
	inodes *InodeRegistry,
) (*CatalogFS, error) {
	if source == nil {
		return nil, errors.New("mountmux: nil catalog")
	}
	if inodes == nil {
		return nil, errors.New("mountmux: nil inode registry")
	}
	if generation == 0 {
		return nil, fmt.Errorf("%w: mount generation is zero", catalog.ErrInvalidTransition)
	}
	fs := newCatalogFS(source, tenant, generation, inodes)
	if _, err := fs.Root(ctx); err != nil {
		return nil, err
	}
	return fs, nil
}

// NewInodeRegistry constructs one process-wide full-identity verifier.
func NewInodeRegistry() *InodeRegistry {
	return newInodeRegistry(InodeForObject)
}

// Tenant returns the bound catalog tenant identity.
func (fs *CatalogFS) Tenant() catalog.TenantID { return fs.tenant }

// Generation returns the bound tenant generation.
func (fs *CatalogFS) Generation() catalog.Generation { return fs.generation }

// InodeForObject derives a stable nonzero kernel cache-key candidate from an opaque ObjectID.
//
// The 64-bit result is not object identity. CatalogFS verifies its full ObjectID binding and
// rejects collisions.
func InodeForObject(id catalog.ObjectID) uint64 {
	digest := sha256.New()
	_, _ = digest.Write(inodeDomain)
	_, _ = digest.Write(id[:])
	sum := digest.Sum(nil)
	for offset := 0; offset < len(sum); offset += 8 {
		inode := binary.BigEndian.Uint64(sum[offset:])
		if inode > mountRootInode {
			return inode
		}
	}
	return mountRootInode + 1
}

// Head returns the current tenant-local catalog revision.
func (fs *CatalogFS) Head(ctx context.Context) (catalog.Revision, error) {
	return fs.catalog.Head(ctx, fs.tenant, fs.generation)
}

// Root returns the tenant's stable mounted root object.
func (fs *CatalogFS) Root(ctx context.Context) (Entry, error) {
	object, err := fs.catalog.Root(ctx, fs.tenant, fs.generation)
	if err != nil {
		return Entry{}, err
	}
	if !object.Visibility.Mount {
		return Entry{}, fmt.Errorf("%w: tenant root is not mount-visible", catalog.ErrIntegrity)
	}
	return fs.entry(object)
}

// Lookup returns a live mount-visible object by opaque identity.
func (fs *CatalogFS) Lookup(ctx context.Context, id catalog.ObjectID) (Entry, error) {
	object, err := fs.catalog.Lookup(ctx, fs.tenant, fs.generation, id)
	if err != nil {
		return Entry{}, err
	}
	return fs.entry(object)
}

// LookupName returns a live mount-visible child by its catalog binding.
func (fs *CatalogFS) LookupName(ctx context.Context, parent catalog.ObjectID, name string) (Entry, error) {
	object, err := fs.catalog.LookupName(ctx, fs.tenant, fs.generation, parent, name)
	if err != nil {
		return Entry{}, err
	}
	return fs.entry(object)
}

// ReadDir returns one page from an immutable directory snapshot.
func (fs *CatalogFS) ReadDir(
	ctx context.Context,
	parent catalog.ObjectID,
	revision catalog.Revision,
	cursor catalog.SnapshotCursor,
	limit int,
) (DirectoryPage, error) {
	page, err := fs.catalog.Snapshot(ctx, fs.tenant, fs.generation, parent, revision, cursor, limit)
	if err != nil {
		return DirectoryPage{}, err
	}
	entries := make([]Entry, len(page.Objects))
	for index, object := range page.Objects {
		entries[index], err = fs.entry(object)
		if err != nil {
			return DirectoryPage{}, err
		}
	}
	return DirectoryPage{Revision: page.Revision, Entries: entries, Next: page.Next}, nil
}

// Open pins and opens one exact mount-visible object revision.
func (fs *CatalogFS) Open(ctx context.Context, id catalog.ObjectID, revision catalog.Revision) (*NativeSnapshot, error) {
	entry, err := fs.Lookup(ctx, id)
	if err != nil {
		return nil, err
	}
	if entry.Object.Kind != catalog.KindFile {
		return nil, fmt.Errorf("%w: only regular files can be opened", catalog.ErrInvalidObject)
	}
	return fs.catalog.Open(ctx, fs.tenant, fs.generation, id, revision)
}

// OpenWrite opens worker-owned mutable staging seeded from one exact file
// revision.
func (fs *CatalogFS) OpenWrite(ctx context.Context, id catalog.ObjectID, revision catalog.Revision) (*NativeWriteStage, error) {
	entry, err := fs.Lookup(ctx, id)
	if err != nil {
		return nil, err
	}
	if entry.Object.Kind != catalog.KindFile {
		return nil, fmt.Errorf("%w: only regular files can be opened for write", catalog.ErrInvalidObject)
	}
	return fs.catalog.OpenWrite(ctx, fs.tenant, fs.generation, id, revision)
}

// Readlink returns one symlink's exact target without opening body content.
func (fs *CatalogFS) Readlink(ctx context.Context, id catalog.ObjectID) (string, error) {
	entry, err := fs.Lookup(ctx, id)
	if err != nil {
		return "", err
	}
	if entry.Object.Kind != catalog.KindSymlink {
		return "", fmt.Errorf("%w: object is not a symlink", catalog.ErrInvalidObject)
	}
	return entry.Object.LinkTarget, nil
}

// Mutate submits one closed native mutation against the current exact head.
func (fs *CatalogFS) Mutate(ctx context.Context, request catalogproto.MutationRequest, content io.Reader) (catalogproto.MutationResponse, error) {
	head, err := fs.Head(ctx)
	if err != nil {
		return catalogproto.MutationResponse{}, err
	}
	requestID, err := newMutationRequestID()
	if err != nil {
		return catalogproto.MutationResponse{}, err
	}
	request.Protocol = catalogproto.Version
	request.RequestID = requestID
	request.Generation = uint64(fs.generation)
	request.ExpectedRevision = uint64(head)
	response, err := fs.catalog.Mutate(ctx, fs.tenant, fs.generation, request, content)
	if err != nil {
		return catalogproto.MutationResponse{}, err
	}
	if response.RequestID == nil || *response.RequestID != request.RequestID ||
		response.MutationID == nil || response.Revision != uint64(head+1) {
		return catalogproto.MutationResponse{}, fmt.Errorf("%w: mutation response does not prove the requested revision", catalog.ErrIntegrity)
	}
	mutation, err := catalog.ParseMutationID(string(*response.MutationID))
	if err != nil || mutation.TargetRevision() != head+1 {
		return catalogproto.MutationResponse{}, fmt.Errorf("%w: mutation response carries an invalid derived identity", catalog.ErrIntegrity)
	}
	return response, nil
}

func newMutationRequestID() (catalogproto.MutationRequestID, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", fmt.Errorf("mountmux: generate mutation request id: %w", err)
	}
	return catalogproto.MutationRequestID(hex.EncodeToString(id[:])), nil
}

// ResolveInode returns the exact ObjectID previously bound to inode.
func (fs *CatalogFS) ResolveInode(inode uint64) (catalog.ObjectID, error) {
	fs.inodes.inodesMu.RLock()
	defer fs.inodes.inodesMu.RUnlock()
	id, found := fs.inodes.byInode[inode]
	if !found {
		return catalog.ObjectID{}, ErrUnknownInode
	}
	return id, nil
}

func newCatalogFS(source NativeCatalog, tenant catalog.TenantID, generation catalog.Generation, inodes *InodeRegistry) *CatalogFS {
	return &CatalogFS{
		catalog: source, tenant: tenant, generation: generation, inodes: inodes,
	}
}

func newInodeRegistry(candidate func(catalog.ObjectID) uint64) *InodeRegistry {
	return &InodeRegistry{
		candidate: candidate,
		byObject:  make(map[catalog.ObjectID]uint64), byInode: make(map[uint64]catalog.ObjectID),
	}
}

func (fs *CatalogFS) entry(object catalog.Object) (Entry, error) {
	return fs.inodes.entry(object)
}

func (r *InodeRegistry) entry(object catalog.Object) (Entry, error) {
	r.inodesMu.Lock()
	defer r.inodesMu.Unlock()
	if inode, found := r.byObject[object.ID]; found {
		return Entry{Object: object, Inode: inode}, nil
	}
	inode := r.candidate(object.ID)
	if inode <= mountRootInode {
		return Entry{}, fmt.Errorf("%w: reserved candidate %d for %s", ErrInodeCollision, inode, object.ID)
	}
	if existing, found := r.byInode[inode]; found && existing != object.ID {
		return Entry{}, fmt.Errorf("%w: inode %d maps %s and %s", ErrInodeCollision, inode, existing, object.ID)
	}
	r.byObject[object.ID] = inode
	r.byInode[inode] = object.ID
	return Entry{Object: object, Inode: inode}, nil
}
