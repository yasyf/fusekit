//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"fmt"
	"io"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

// NativeCatalog is the exact persistent transport surface consumed by native callbacks.
type NativeCatalog interface {
	Root(context.Context, catalog.TenantID, catalog.Generation) (catalog.Object, error)
	Head(context.Context, catalog.TenantID, catalog.Generation) (catalog.Revision, error)
	Lookup(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID) (catalog.Object, error)
	LookupName(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID, string) (catalog.Object, error)
	Snapshot(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision, catalog.SnapshotCursor, int) (catalog.SnapshotPage, error)
	Open(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (*NativeSnapshot, error)
	Mutate(context.Context, catalog.TenantID, catalog.Generation, catalogproto.MutationRequest, io.Reader) (catalogproto.MutationResponse, error)
}

// NativeSnapshot is one exact object revision materialized inside the killable child.
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

type nativeView struct {
	catalog    NativeCatalog
	tenant     catalog.TenantID
	generation catalog.Generation
	inodes     *InodeRegistry
}

func newNativeView(source NativeCatalog, tenantID catalog.TenantID, generation catalog.Generation, inodes *InodeRegistry) *nativeView {
	return &nativeView{catalog: source, tenant: tenantID, generation: generation, inodes: inodes}
}

func (v *nativeView) Tenant() catalog.TenantID { return v.tenant }

func (v *nativeView) Generation() catalog.Generation { return v.generation }

func (v *nativeView) Head(ctx context.Context) (catalog.Revision, error) {
	return v.catalog.Head(ctx, v.tenant, v.generation)
}

func (v *nativeView) Root(ctx context.Context) (Entry, error) {
	object, err := v.catalog.Root(ctx, v.tenant, v.generation)
	if err != nil {
		return Entry{}, err
	}
	return v.entry(object)
}

func (v *nativeView) Lookup(ctx context.Context, id catalog.ObjectID) (Entry, error) {
	object, err := v.catalog.Lookup(ctx, v.tenant, v.generation, id)
	if err != nil {
		return Entry{}, err
	}
	return v.entry(object)
}

func (v *nativeView) LookupName(ctx context.Context, parent catalog.ObjectID, name string) (Entry, error) {
	object, err := v.catalog.LookupName(ctx, v.tenant, v.generation, parent, name)
	if err != nil {
		return Entry{}, err
	}
	return v.entry(object)
}

func (v *nativeView) ReadDir(ctx context.Context, parent catalog.ObjectID, revision catalog.Revision, cursor catalog.SnapshotCursor, limit int) (DirectoryPage, error) {
	page, err := v.catalog.Snapshot(ctx, v.tenant, v.generation, parent, revision, cursor, limit)
	if err != nil {
		return DirectoryPage{}, err
	}
	entries := make([]Entry, len(page.Objects))
	for index, object := range page.Objects {
		entries[index], err = v.entry(object)
		if err != nil {
			return DirectoryPage{}, err
		}
	}
	return DirectoryPage{Revision: page.Revision, Entries: entries, Next: page.Next}, nil
}

func (v *nativeView) Open(ctx context.Context, id catalog.ObjectID, revision catalog.Revision) (*NativeSnapshot, error) {
	return v.catalog.Open(ctx, v.tenant, v.generation, id, revision)
}

func (v *nativeView) Mutate(ctx context.Context, request catalogproto.MutationRequest, content io.Reader) (catalogproto.MutationResponse, error) {
	head, err := v.Head(ctx)
	if err != nil {
		return catalogproto.MutationResponse{}, err
	}
	id, err := catalog.NewMutationID()
	if err != nil {
		return catalogproto.MutationResponse{}, err
	}
	request.Protocol = catalogproto.Version
	request.OperationID = catalogproto.MutationID(id.String())
	request.Generation = uint64(v.generation)
	request.ExpectedRevision = uint64(head)
	response, err := v.catalog.Mutate(ctx, v.tenant, v.generation, request, content)
	if err != nil {
		return catalogproto.MutationResponse{}, err
	}
	if response.OperationID == nil || *response.OperationID != request.OperationID || response.Revision != uint64(head+1) {
		return catalogproto.MutationResponse{}, fmt.Errorf("%w: mutation response does not prove the requested revision", catalog.ErrIntegrity)
	}
	return response, nil
}

func (v *nativeView) entry(object catalog.Object) (Entry, error) {
	return v.inodes.entry(object)
}
