package mountmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestCatalogFSUsesStableCatalogIdentityAndVerifiedInodes(t *testing.T) {
	ctx := context.Background()
	known := catalog.ObjectID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	if got := InodeForObject(known); got != 17574606804234376151 {
		t.Fatalf("InodeForObject(known) = %d", got)
	}
	source, fs, root := newTestFS(t, "identity")
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

	other, err := NewCatalogFS(ctx, source, fs.Tenant(), fs.Generation())
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

	colliding := newCatalogFS(source, fs.Tenant(), fs.Generation(), newInodeRegistry(func(catalog.ObjectID) uint64 { return 7 }))
	if _, err := colliding.Lookup(ctx, first.ID); err != nil {
		t.Fatalf("Lookup(first collision candidate): %v", err)
	}
	if _, err := colliding.Lookup(ctx, second.ID); !errors.Is(err, ErrInodeCollision) {
		t.Fatalf("Lookup(colliding object) = %v, want ErrInodeCollision", err)
	}
	if _, err := colliding.ResolveInode(8); !errors.Is(err, ErrUnknownInode) {
		t.Fatalf("ResolveInode(unknown) = %v, want ErrUnknownInode", err)
	}
	reserved := newCatalogFS(source, fs.Tenant(), fs.Generation(), newInodeRegistry(func(catalog.ObjectID) uint64 { return mountRootInode }))
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
	id := mustMutationID(t)
	name := target.Name
	mode := target.Mode
	visibility := target.Visibility
	result := commitMutation(t, fs, id, head, catalog.MutationIntent{
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

func TestCatalogFSFencesEveryRequestByGeneration(t *testing.T) {
	ctx := context.Background()
	source, fs, root := newTestFS(t, "generation")
	file := createFile(t, fs, root.Object.ID, "file", "body", true)
	state, err := source.LoadTenantState(ctx, fs.Tenant())
	if err != nil {
		t.Fatalf("LoadTenantState: %v", err)
	}
	state.Generation++
	state.ActivatedGeneration = state.Generation
	if _, err := source.SaveTenantState(ctx, state.Version, state); err != nil {
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
	if _, err := fs.BeginMutation(ctx, mustMutationID(t), mustHeadCatalog(t, source, fs.Tenant()), catalog.MutationIntent{}); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("BeginMutation(stale generation) = %v, want ErrGenerationMismatch", err)
	}
}

func TestCatalogFSMutationUsesPreparedCatalogSeam(t *testing.T) {
	ctx := context.Background()
	_, fs, root := newTestFS(t, "mutation")
	ref, err := fs.StageContent(ctx, bytes.NewBufferString("body"))
	if err != nil {
		t.Fatalf("StageContent: %v", err)
	}
	id := mustMutationID(t)
	head := mustHead(t, fs)
	intent := catalog.MutationIntent{
		SourceID: "mount-test", Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
		Create: &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: root.Object.ID, Name: "file", Kind: catalog.KindFile, Mode: 0o600,
			ContentRevision: 1, Content: ref, Convergence: catalog.Convergence{Desired: 1},
			Visibility: catalog.Visibility{Mount: true},
		}},
	}
	prepared, err := fs.BeginMutation(ctx, id, head, intent)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	if prepared.State != catalog.MutationPrepared || prepared.OperationID != id || prepared.ExpectedHead != head {
		t.Fatalf("prepared mutation = %+v", prepared)
	}
	if _, err := fs.LookupName(ctx, root.Object.ID, "file"); !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("LookupName(before source apply) = %v, want ErrNotFound", err)
	}
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatalf("NewMutationOwnerID: %v", err)
	}
	claimed, err := fs.ClaimMutation(ctx, id, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	if _, err := fs.MarkMutationApplied(ctx, id, *claimed.Claim); err != nil {
		t.Fatalf("MarkMutationApplied: %v", err)
	}
	result, err := fs.CommitMutation(ctx, id)
	if err != nil {
		t.Fatalf("CommitMutation: %v", err)
	}
	if result.Primary.ID == (catalog.ObjectID{}) || result.Mutation.Revision != head+1 || result.Primary.Revision != head+1 {
		t.Fatalf("committed result = %+v", result)
	}
	bound, err := fs.LookupName(ctx, root.Object.ID, "file")
	if err != nil || bound.Object.ID != result.Primary.ID {
		t.Fatalf("committed binding = %+v, %v", bound, err)
	}
}

func TestCatalogFSCancellationStopsBeforeCatalogWork(t *testing.T) {
	_, fs, root := newTestFS(t, "cancellation")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fs.ReadDir(ctx, root.Object.ID, 0, catalog.SnapshotCursor{}, 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("ReadDir(canceled) = %v, want context.Canceled", err)
	}
	if _, err := fs.StageContent(ctx, bytes.NewBufferString("unreachable")); !errors.Is(err, context.Canceled) {
		t.Fatalf("StageContent(canceled) = %v, want context.Canceled", err)
	}
	streamCtx, streamCancel := context.WithCancel(context.Background())
	reader := &cancelingReader{cancel: streamCancel}
	if _, err := fs.StageContent(streamCtx, reader); !errors.Is(err, context.Canceled) {
		t.Fatalf("StageContent(canceled mid-stream) = %v, want context.Canceled", err)
	}
	if reader.reads != 1 {
		t.Fatalf("source reads after cancellation = %d, want 1", reader.reads)
	}
}

type cancelingReader struct {
	cancel context.CancelFunc
	reads  int
}

func (r *cancelingReader) Read(buffer []byte) (int, error) {
	r.reads++
	if r.reads != 1 {
		return 0, errors.New("source read after cancellation")
	}
	r.cancel()
	return copy(buffer, "partial"), nil
}

func newTestFS(t *testing.T, name string) (*catalog.Catalog, *CatalogFS, Entry) {
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
	rootObject, err := source.CreateTenant(ctx, mustMutationID(t), tenant, catalog.CaseSensitive, catalog.PresentMount)
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	if _, err := source.SaveTenantState(ctx, 0, catalog.TenantStateRecord{
		Tenant: tenant, Generation: 1, ActivatedGeneration: 1,
	}); err != nil {
		t.Fatalf("SaveTenantState: %v", err)
	}
	fs, err := NewCatalogFS(ctx, source, tenant, 1)
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
	return source, fs, root
}

func createFile(t *testing.T, fs *CatalogFS, parent catalog.ObjectID, name, body string, visible bool) catalog.Object {
	t.Helper()
	ref, err := fs.StageContent(context.Background(), bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("StageContent(%s): %v", name, err)
	}
	head := mustHead(t, fs)
	result := commitMutation(t, fs, mustMutationID(t), head, catalog.MutationIntent{
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
	id catalog.MutationID,
	expectedHead catalog.Revision,
	intent catalog.MutationIntent,
) catalog.NamespaceMutationResult {
	t.Helper()
	ctx := context.Background()
	if _, err := fs.BeginMutation(ctx, id, expectedHead, intent); err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatalf("NewMutationOwnerID: %v", err)
	}
	claimed, err := fs.ClaimMutation(ctx, id, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	if _, err := fs.MarkMutationApplied(ctx, id, *claimed.Claim); err != nil {
		t.Fatalf("MarkMutationApplied: %v", err)
	}
	result, err := fs.CommitMutation(ctx, id)
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

func mustHeadCatalog(t *testing.T, source *catalog.Catalog, tenant catalog.TenantID) catalog.Revision {
	t.Helper()
	head, err := source.Head(context.Background(), tenant)
	if err != nil {
		t.Fatalf("catalog.Head: %v", err)
	}
	return head
}

func mustMutationID(t *testing.T) catalog.MutationID {
	t.Helper()
	id, err := catalog.NewMutationID()
	if err != nil {
		t.Fatalf("NewMutationID: %v", err)
	}
	return id
}
