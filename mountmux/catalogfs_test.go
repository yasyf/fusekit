package mountmux

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

func TestCatalogFSCancellationStopsBeforeCatalogWork(t *testing.T) {
	_, fs, root := newTestFS(t, "cancellation")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fs.ReadDir(ctx, root.Object.ID, 0, catalog.SnapshotCursor{}, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDir(canceled) = %v, want context.Canceled", err)
	}
}

func TestCatalogFSMutateFencesRequestEchoAndDerivedMutationIdentity(t *testing.T) {
	backend, fs, root := newTestFS(t, "mutation-identity")
	objectID := catalogproto.ObjectID(root.Object.ID.String())
	request := catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindDelete, Disposition: catalogproto.MutationDispositionNamespace,
		ObjectID: &objectID,
	}
	var captured catalogproto.MutationRequestID
	backend.mutate = func(
		_ context.Context,
		_ catalog.TenantID,
		_ catalog.Generation,
		request catalogproto.MutationRequest,
		_ io.Reader,
	) (catalogproto.MutationResponse, error) {
		captured = request.RequestID
		mutation := catalogproto.MutationID(catalogFSMutationForRevision(2).String())
		return catalogproto.MutationResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
			RequestID: &request.RequestID, MutationID: &mutation, Revision: 2,
		}, nil
	}
	if _, err := fs.Mutate(context.Background(), request, nil); err != nil {
		t.Fatalf("Mutate(exact identity): %v", err)
	}
	decoded, err := hex.DecodeString(string(captured))
	if err != nil || len(decoded) != 16 {
		t.Fatalf("request id = %q, want exact 16-byte correlation identity", captured)
	}

	backend.mutate = func(
		_ context.Context,
		_ catalog.TenantID,
		_ catalog.Generation,
		request catalogproto.MutationRequest,
		_ io.Reader,
	) (catalogproto.MutationResponse, error) {
		wrong := []byte(request.RequestID)
		if wrong[0] == '0' {
			wrong[0] = '1'
		} else {
			wrong[0] = '0'
		}
		requestID := catalogproto.MutationRequestID(wrong)
		mutation := catalogproto.MutationID(catalogFSMutationForRevision(2).String())
		return catalogproto.MutationResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
			RequestID: &requestID, MutationID: &mutation, Revision: 2,
		}, nil
	}
	if _, err := fs.Mutate(context.Background(), request, nil); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("Mutate(mismatched request identity) = %v, want integrity", err)
	}

	backend.mutate = func(
		_ context.Context,
		_ catalog.TenantID,
		_ catalog.Generation,
		request catalogproto.MutationRequest,
		_ io.Reader,
	) (catalogproto.MutationResponse, error) {
		mutation := catalogproto.MutationID(catalogFSMutationForRevision(3).String())
		return catalogproto.MutationResponse{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
			RequestID: &request.RequestID, MutationID: &mutation, Revision: 2,
		}, nil
	}
	if _, err := fs.Mutate(context.Background(), request, nil); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("Mutate(wrong derived target) = %v, want integrity", err)
	}
}

type testNativeCatalog struct {
	store  *catalog.Catalog
	root   catalog.Object
	mutate func(
		context.Context,
		catalog.TenantID,
		catalog.Generation,
		catalogproto.MutationRequest,
		io.Reader,
	) (catalogproto.MutationResponse, error)
}

func (c *testNativeCatalog) requireGeneration(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation) error {
	state, err := c.store.LoadTenantState(ctx, tenant)
	if err != nil {
		return err
	}
	if state.Generation != generation || state.ActivatedGeneration != generation {
		return catalog.ErrGenerationMismatch
	}
	return nil
}

func (c *testNativeCatalog) Root(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation) (catalog.Object, error) {
	if err := c.requireGeneration(ctx, tenant, generation); err != nil {
		return catalog.Object{}, err
	}
	return c.root, nil
}

func (c *testNativeCatalog) Head(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation) (catalog.Revision, error) {
	if err := c.requireGeneration(ctx, tenant, generation); err != nil {
		return 0, err
	}
	return c.root.Revision, nil
}

func (c *testNativeCatalog) Lookup(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, id catalog.ObjectID) (catalog.Object, error) {
	if err := c.requireGeneration(ctx, tenant, generation); err != nil {
		return catalog.Object{}, err
	}
	return c.store.Lookup(ctx, tenant, catalog.PresentationMount, id)
}

func (c *testNativeCatalog) LookupName(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, parent catalog.ObjectID, name string) (catalog.Object, error) {
	if err := c.requireGeneration(ctx, tenant, generation); err != nil {
		return catalog.Object{}, err
	}
	return c.store.LookupName(ctx, tenant, catalog.PresentationMount, parent, name)
}

func (c *testNativeCatalog) Snapshot(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, parent catalog.ObjectID, revision catalog.Revision, cursor catalog.SnapshotCursor, limit int) (catalog.SnapshotPage, error) {
	if err := c.requireGeneration(ctx, tenant, generation); err != nil {
		return catalog.SnapshotPage{}, err
	}
	return c.store.Snapshot(ctx, tenant, catalog.EnumerationScope{
		Kind: catalog.EnumerationContainer, Presentation: catalog.PresentationMount, Parent: parent,
	}, revision, cursor, limit)
}

func (c *testNativeCatalog) Open(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, id catalog.ObjectID, revision catalog.Revision) (*NativeSnapshot, error) {
	if err := c.requireGeneration(ctx, tenant, generation); err != nil {
		return nil, err
	}
	handle, err := c.store.OpenAt(
		ctx, catalog.RetentionOwner("mountmux-test"), tenant,
		catalog.PresentationMount, generation, id, revision,
	)
	if err != nil {
		return nil, err
	}
	return &NativeSnapshot{Object: handle.Object, Source: handle}, nil
}

func (c *testNativeCatalog) OpenWrite(context.Context, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.Revision) (*NativeWriteStage, error) {
	return nil, errors.New("test native catalog: write staging is not configured")
}

func (c *testNativeCatalog) Mutate(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation, request catalogproto.MutationRequest, content io.Reader) (catalogproto.MutationResponse, error) {
	if err := c.requireGeneration(ctx, tenant, generation); err != nil {
		return catalogproto.MutationResponse{}, err
	}
	if c.mutate != nil {
		return c.mutate(ctx, tenant, generation, request, content)
	}
	return catalogproto.MutationResponse{}, errors.New("test native catalog: mutation transport is not configured")
}

func newTestFS(t *testing.T, name string) (*testNativeCatalog, *CatalogFS, Entry) {
	t.Helper()
	ctx := context.Background()
	source, err := catalog.Open(ctx, filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatalf("catalog.Open: %v", err)
	}
	t.Cleanup(func() {
		if err := source.Close(); err != nil {
			t.Errorf("catalog.Close: %v", err)
		}
	})
	tenant, err := catalog.NewTenantID(name)
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	rootObject, err := source.CreateTenant(ctx, tenant, catalog.CaseSensitive, catalog.PresentMount)
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if _, err := source.SaveTenantState(ctx, 0, catalog.TenantStateRecord{
		Tenant: tenant, Generation: 1,
		Desired: rootObject.Revision, Observed: rootObject.Revision,
		Verified: rootObject.Revision, Applied: rootObject.Revision,
		ActivatedGeneration: 1,
	}); err != nil {
		t.Fatalf("SaveTenantState: %v", err)
	}
	backend := &testNativeCatalog{store: source, root: rootObject}
	fs, err := NewCatalogFS(ctx, backend, tenant, 1)
	if err != nil {
		t.Fatalf("NewCatalogFS: %v", err)
	}
	root, err := fs.Root(ctx)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	if root.Object.ID != rootObject.ID {
		t.Fatalf("root ID = %s, want %s", root.Object.ID, rootObject.ID)
	}
	return backend, fs, root
}

func mustHead(t *testing.T, fs *CatalogFS) catalog.Revision {
	t.Helper()
	head, err := fs.Head(context.Background())
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	return head
}

func catalogFSMutationForRevision(revision catalog.Revision) catalog.MutationID {
	var mutation catalog.MutationID
	binary.BigEndian.PutUint64(mutation[:8], uint64(revision))
	mutation[len(mutation)-1] = byte(revision)
	return mutation
}
