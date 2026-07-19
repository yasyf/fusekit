// Package mountmux projects catalog tenants as filesystem-neutral mounted children.
package mountmux

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/yasyf/fusekit/catalog"
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

// CatalogFS is one exact tenant generation projected through the mount presentation.
type CatalogFS struct {
	catalog    *catalog.Catalog
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
func NewCatalogFS(ctx context.Context, source *catalog.Catalog, tenant catalog.TenantID, generation catalog.Generation) (*CatalogFS, error) {
	return NewCatalogFSWithRegistry(ctx, source, tenant, generation, NewInodeRegistry())
}

// NewCatalogFSWithRegistry binds a mounted child to a process-wide verified inode registry.
func NewCatalogFSWithRegistry(
	ctx context.Context,
	source *catalog.Catalog,
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
	metadata, err := source.Tenant(ctx, tenant)
	if err != nil {
		return nil, err
	}
	if !metadata.Presentations.Has(catalog.PresentationMount) {
		return nil, fmt.Errorf("%w: tenant has no mount presentation", catalog.ErrInvalidObject)
	}
	fs := newCatalogFS(source, tenant, generation, inodes)
	if err := fs.requireGeneration(ctx); err != nil {
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
	if err := fs.requireGeneration(ctx); err != nil {
		return 0, err
	}
	head, err := fs.catalog.Head(ctx, fs.tenant)
	if err != nil {
		return 0, err
	}
	if err := fs.requireGeneration(ctx); err != nil {
		return 0, err
	}
	return head, nil
}

// Root returns the tenant's stable mounted root object.
func (fs *CatalogFS) Root(ctx context.Context) (Entry, error) {
	if err := fs.requireGeneration(ctx); err != nil {
		return Entry{}, err
	}
	object, err := fs.catalog.Root(ctx, fs.tenant)
	if err != nil {
		return Entry{}, err
	}
	if !object.Visibility.Mount {
		return Entry{}, fmt.Errorf("%w: tenant root is not mount-visible", catalog.ErrIntegrity)
	}
	if err := fs.requireGeneration(ctx); err != nil {
		return Entry{}, err
	}
	return fs.entry(object)
}

// Lookup returns a live mount-visible object by opaque identity.
func (fs *CatalogFS) Lookup(ctx context.Context, id catalog.ObjectID) (Entry, error) {
	if err := fs.requireGeneration(ctx); err != nil {
		return Entry{}, err
	}
	object, err := fs.catalog.Lookup(ctx, fs.tenant, catalog.PresentationMount, id)
	if err != nil {
		return Entry{}, err
	}
	if err := fs.requireGeneration(ctx); err != nil {
		return Entry{}, err
	}
	return fs.entry(object)
}

// LookupName returns a live mount-visible child by its catalog binding.
func (fs *CatalogFS) LookupName(ctx context.Context, parent catalog.ObjectID, name string) (Entry, error) {
	if err := fs.requireGeneration(ctx); err != nil {
		return Entry{}, err
	}
	object, err := fs.catalog.LookupName(ctx, fs.tenant, catalog.PresentationMount, parent, name)
	if err != nil {
		return Entry{}, err
	}
	if err := fs.requireGeneration(ctx); err != nil {
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
	if err := fs.requireGeneration(ctx); err != nil {
		return DirectoryPage{}, err
	}
	page, err := fs.catalog.Snapshot(ctx, fs.tenant, catalog.EnumerationScope{
		Kind: catalog.EnumerationContainer, Presentation: catalog.PresentationMount, Parent: parent,
	}, revision, cursor, limit)
	if err != nil {
		return DirectoryPage{}, err
	}
	if err := fs.requireGeneration(ctx); err != nil {
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
func (fs *CatalogFS) Open(ctx context.Context, id catalog.ObjectID, revision catalog.Revision) (*catalog.SnapshotHandle, error) {
	return fs.catalog.OpenAt(ctx, fs.tenant, catalog.PresentationMount, fs.generation, id, revision)
}

// StageContent stores immutable bytes for a later prepared mutation.
func (fs *CatalogFS) StageContent(ctx context.Context, source io.Reader) (catalog.ContentRef, error) {
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.ContentRef{}, err
	}
	ref, err := fs.catalog.StageContent(ctx, source)
	if err != nil {
		return catalog.ContentRef{}, err
	}
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.ContentRef{}, err
	}
	return ref, nil
}

// BeginMutation durably routes one namespace intent through the catalog journal.
func (fs *CatalogFS) BeginMutation(
	ctx context.Context,
	id catalog.MutationID,
	expectedHead catalog.Revision,
	intent catalog.MutationIntent,
) (catalog.PreparedMutation, error) {
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.PreparedMutation{}, err
	}
	prepared, err := fs.catalog.BeginMutation(ctx, id, fs.tenant, expectedHead, intent)
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.PreparedMutation{}, err
	}
	return prepared, nil
}

// ClaimMutation durably fences one external source attempt.
func (fs *CatalogFS) ClaimMutation(ctx context.Context, id catalog.MutationID, owner catalog.MutationOwnerID) (catalog.PreparedMutation, error) {
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.PreparedMutation{}, err
	}
	prepared, err := fs.catalog.ClaimMutation(ctx, id, owner)
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.PreparedMutation{}, err
	}
	return prepared, nil
}

// MarkMutationApplied records proof that a fenced source attempt settled.
func (fs *CatalogFS) MarkMutationApplied(ctx context.Context, id catalog.MutationID, claim catalog.MutationClaim) (catalog.PreparedMutation, error) {
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.PreparedMutation{}, err
	}
	prepared, err := fs.catalog.MarkMutationApplied(ctx, id, claim)
	if err != nil {
		return catalog.PreparedMutation{}, err
	}
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.PreparedMutation{}, err
	}
	return prepared, nil
}

// CommitMutation publishes one externally applied mutation as a catalog revision.
func (fs *CatalogFS) CommitMutation(ctx context.Context, id catalog.MutationID) (catalog.NamespaceMutationResult, error) {
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.NamespaceMutationResult{}, err
	}
	result, err := fs.catalog.CommitMutation(ctx, id)
	if err != nil {
		return catalog.NamespaceMutationResult{}, err
	}
	if err := fs.requireGeneration(ctx); err != nil {
		return catalog.NamespaceMutationResult{}, err
	}
	return result, nil
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

func (fs *CatalogFS) requireGeneration(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := fs.catalog.LoadTenantState(ctx, fs.tenant)
	if err != nil {
		return err
	}
	if state.Generation != fs.generation || state.ActivatedGeneration != fs.generation {
		return fmt.Errorf(
			"%w: got %d, current %d, activated %d",
			catalog.ErrGenerationMismatch, fs.generation, state.Generation, state.ActivatedGeneration,
		)
	}
	return nil
}

func newCatalogFS(source *catalog.Catalog, tenant catalog.TenantID, generation catalog.Generation, inodes *InodeRegistry) *CatalogFS {
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
