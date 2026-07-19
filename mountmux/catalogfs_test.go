package mountmux

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
)

func TestCatalogFSUsesStableCatalogIdentityAndVerifiedInodes(t *testing.T) {
	ctx := context.Background()
	known := catalog.ObjectID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if got := InodeForObject(known); got != 17574606804234376151 {
		t.Fatalf("InodeForObject(known) = %d", got)
	}
	backend, fs, root := newTestFS(t, "identity")
	first := createFile(t, fs, root.Object.ID, "first", "one", true)
	second := createFile(t, fs, root.Object.ID, "second", "two", true)

	lookedUp, err := fs.Lookup(ctx, first.ID)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	byName, err := fs.LookupName(ctx, root.Object.ID, first.Name)
	if err != nil {
		t.Fatalf("LookupName: %v", err)
	}
	if lookedUp.Object.ID != first.ID || byName.Object.ID != first.ID || lookedUp.Inode != byName.Inode {
		t.Fatalf("identity drift: lookup=%+v name=%+v object=%+v", lookedUp, byName, first)
	}
	resolved, err := fs.ResolveInode(lookedUp.Inode)
	if err != nil || resolved != first.ID {
		t.Fatalf("ResolveInode = %s, %v; want %s", resolved, err, first.ID)
	}

	other, err := NewCatalogFS(ctx, backend, fs.Tenant(), fs.Generation())
	if err != nil {
		t.Fatalf("NewCatalogFS(second binding): %v", err)
	}
	rebound, err := other.Lookup(ctx, first.ID)
	if err != nil {
		t.Fatalf("Lookup(second binding): %v", err)
	}
	if rebound.Inode != lookedUp.Inode || rebound.Inode != InodeForObject(first.ID) {
		t.Fatalf("inode = %d, rebound = %d, candidate = %d", lookedUp.Inode, rebound.Inode, InodeForObject(first.ID))
	}

	colliding := newCatalogFS(backend, fs.Tenant(), fs.Generation(), newInodeRegistry(func(catalog.ObjectID) uint64 { return 7 }))
	if _, err := colliding.Lookup(ctx, first.ID); err != nil {
		t.Fatalf("Lookup(first collision candidate): %v", err)
	}
	if _, err := colliding.Lookup(ctx, second.ID); !errors.Is(err, ErrInodeCollision) {
		t.Fatalf("Lookup(colliding object) = %v, want ErrInodeCollision", err)
	}
	if _, err := colliding.ResolveInode(8); !errors.Is(err, ErrUnknownInode) {
		t.Fatalf("ResolveInode(unknown) = %v, want ErrUnknownInode", err)
	}
	reserved := newCatalogFS(backend, fs.Tenant(), fs.Generation(), newInodeRegistry(func(catalog.ObjectID) uint64 { return mountRootInode }))
	if _, err := reserved.Root(ctx); !errors.Is(err, ErrInodeCollision) {
		t.Fatalf("Root(reserved inode) = %v, want ErrInodeCollision", err)
	}
}

func TestCatalogFSReadDirPagesOnePinnedSnapshot(t *testing.T) {
	ctx := context.Background()
	_, fs, root := newTestFS(t, "snapshot")
	first := createFile(t, fs, root.Object.ID, "first", "one", true)
	second := createFile(t, fs, root.Object.ID, "second", "two", true)

	page, err := fs.ReadDir(ctx, root.Object.ID, 0, catalog.SnapshotCursor{}, 1)
	if err != nil {
		t.Fatalf("ReadDir(first): %v", err)
	}
	if page.Revision == 0 || len(page.Entries) != 1 || page.Next == nil {
		t.Fatalf("first page = %+v", page)
	}
	third := createFile(t, fs, root.Object.ID, "third", "three", true)
	next, err := fs.ReadDir(ctx, root.Object.ID, page.Revision, *page.Next, 1)
	if err != nil {
		t.Fatalf("ReadDir(second): %v", err)
	}
	if next.Revision != page.Revision || len(next.Entries) != 1 || next.Next != nil {
		t.Fatalf("second page = %+v", next)
	}
	ids := []catalog.ObjectID{page.Entries[0].Object.ID, next.Entries[0].Object.ID}
	slices.SortFunc(ids, func(left, right catalog.ObjectID) int { return bytes.Compare(left[:], right[:]) })
	want := []catalog.ObjectID{first.ID, second.ID}
	slices.SortFunc(want, func(left, right catalog.ObjectID) int { return bytes.Compare(left[:], right[:]) })
	if !slices.Equal(ids, want) || slices.Contains(ids, third.ID) {
		t.Fatalf("snapshot objects = %v, want %v without %s", ids, want, third.ID)
	}
}

func TestCatalogFSOpenPinsReplacedTombstoneContent(t *testing.T) {
	ctx := context.Background()
	_, fs, root := newTestFS(t, "replace")
	target := createFile(t, fs, root.Object.ID, "settings.json", "old", true)
	handle, err := fs.Open(ctx, target.ID, target.Revision)
	if err != nil {
		t.Fatalf("Open(target): %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })
	source := createFile(t, fs, root.Object.ID, ".settings.json.tmp", "new", false)

	head := mustHead(t, fs)
	name := target.Name
	mode := target.Mode
	visibility := target.Visibility
	result := commitMutation(t, fs, head, catalog.MutationIntent{
		SourceID: "mount-test", Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Replace: &catalog.ReplaceMutation{
			Source: source.ID, Target: target.ID, Parent: &target.Parent, Name: &name, Mode: &mode, Visibility: &visibility,
		},
	})
	if result.Primary.ID != source.ID || result.Secondary == nil || result.Secondary.ID != target.ID || !result.Secondary.Tombstone {
		t.Fatalf("replace result = %+v", result)
	}
	if _, err := fs.Lookup(ctx, target.ID); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("Lookup(tombstoned target) = %v, want ErrNotFound", err)
	}
	bound, err := fs.LookupName(ctx, root.Object.ID, target.Name)
	if err != nil || bound.Object.ID != source.ID {
		t.Fatalf("replacement binding = %+v, %v", bound, err)
	}
	pinned, err := io.ReadAll(handle)
	if err != nil {
		t.Fatalf("ReadAll(pinned target): %v", err)
	}
	if string(pinned) != "old" || handle.Object.ID != target.ID || handle.Object.Revision != target.Revision {
		t.Fatalf("pinned handle = %q %+v", pinned, handle.Object)
	}
	current, err := fs.Open(ctx, source.ID, result.Primary.Revision)
	if err != nil {
		t.Fatalf("Open(replacement): %v", err)
	}
	defer func() {
		if err := current.Close(); err != nil {
			t.Errorf("Close(replacement): %v", err)
		}
	}()
	body, err := io.ReadAll(current)
	if err != nil || string(body) != "new" {
		t.Fatalf("ReadAll(replacement) = %q, %v", body, err)
	}
}

func TestCatalogFSReadlinkUsesMetadataAndOpenRejectsSymlink(t *testing.T) {
	ctx := context.Background()
	_, fs, root := newTestFS(t, "readlink")
	head := mustHead(t, fs)
	result := commitMutation(t, fs, head, catalog.MutationIntent{
		SourceID: "mount-test", Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Create: &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: root.Object.ID, Name: "current", Kind: catalog.KindSymlink, Mode: 0o777,
			ContentRevision: 1, LinkTarget: "../settings.json", Visibility: catalog.Visibility{Mount: true},
		}},
	})
	target, err := fs.Readlink(ctx, result.Primary.ID)
	if err != nil || target != "../settings.json" {
		t.Fatalf("Readlink = %q, %v", target, err)
	}
	if _, err := fs.Open(ctx, result.Primary.ID, result.Primary.Revision); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("Open(symlink) = %v, want ErrInvalidObject", err)
	}
	file := createFile(t, fs, root.Object.ID, "regular", "body", true)
	if _, err := fs.Readlink(ctx, file.ID); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("Readlink(file) = %v, want ErrInvalidObject", err)
	}
}

func TestCatalogFSFencesEveryRequestByGeneration(t *testing.T) {
	ctx := context.Background()
	backend, fs, root := newTestFS(t, "generation")
	file := createFile(t, fs, root.Object.ID, "file", "body", true)
	state, err := backend.store.LoadTenantState(ctx, fs.Tenant())
	if err != nil {
		t.Fatalf("LoadTenantState: %v", err)
	}
	state.Generation++
	state.ActivatedGeneration = state.Generation
	if _, err := backend.store.SaveTenantState(ctx, state.Version, state); err != nil {
		t.Fatalf("SaveTenantState: %v", err)
	}
	if _, err := fs.Lookup(ctx, file.ID); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("Lookup(stale generation) = %v, want ErrGenerationMismatch", err)
	}
	if _, err := fs.ReadDir(ctx, root.Object.ID, 0, catalog.SnapshotCursor{}, 10); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("ReadDir(stale generation) = %v, want ErrGenerationMismatch", err)
	}
	if _, err := fs.Open(ctx, file.ID, file.Revision); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("Open(stale generation) = %v, want ErrGenerationMismatch", err)
	}
	if _, err := fs.Mutate(ctx, catalogproto.MutationRequest{}, nil); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("Mutate(stale generation) = %v, want ErrGenerationMismatch", err)
	}
}

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
		Kind: catalogproto.MutationKindDelete, ObjectID: &objectID,
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
	return c.store.Root(ctx, tenant)
}

func (c *testNativeCatalog) Head(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation) (catalog.Revision, error) {
	if err := c.requireGeneration(ctx, tenant, generation); err != nil {
		return 0, err
	}
	return c.store.Head(ctx, tenant)
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
		Tenant: tenant, Generation: 1, ActivatedGeneration: 1,
	}); err != nil {
		t.Fatalf("SaveTenantState: %v", err)
	}
	backend := &testNativeCatalog{store: source}
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

func createFile(t *testing.T, fs *CatalogFS, parent catalog.ObjectID, name, body string, visible bool) catalog.Object {
	t.Helper()
	store := fs.catalog.(*testNativeCatalog).store
	ref, err := store.StageContent(context.Background(), bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("StageContent(%s): %v", name, err)
	}
	head := mustHead(t, fs)
	result := commitMutation(t, fs, head, catalog.MutationIntent{
		SourceID: "mount-test", Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Create: &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: parent, Name: name, Kind: catalog.KindFile, Mode: 0o600,
			ContentRevision: 1, Content: ref, Convergence: catalog.Convergence{Desired: 1},
			Visibility: catalog.Visibility{Mount: visible},
		}},
	})
	return result.Primary
}

func commitMutation(
	t *testing.T,
	fs *CatalogFS,
	expectedHead catalog.Revision,
	intent catalog.MutationIntent,
) catalog.NamespaceMutationResult {
	t.Helper()
	ctx := context.Background()
	store := fs.catalog.(*testNativeCatalog).store
	prepared, err := store.BeginMutation(ctx, fs.tenant, expectedHead, intent)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	id := prepared.OperationID
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatalf("NewMutationOwnerID: %v", err)
	}
	claimed, err := store.ClaimMutation(ctx, id, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	if _, err := store.MarkMutationApplied(ctx, id, *claimed.Claim); err != nil {
		t.Fatalf("MarkMutationApplied: %v", err)
	}
	result, err := store.CommitMutation(ctx, fs.tenant, id)
	if err != nil {
		t.Fatalf("CommitMutation: %v", err)
	}
	return result
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
