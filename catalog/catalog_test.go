package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

func fileProviderInterestOwner(domain string) InterestOwner {
	return InterestOwner{
		Presentation: PresentationFileProvider,
		Domain:       causal.DomainID(domain),
		Generation:   1,
	}
}

func TestReplaceKeepsSourceIdentityAndOldHandleContent(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "replace", CaseSensitive)
	source := createTestFile(t, c, tenant, root.ID, ".target.tmp", "new")
	target := createTestFile(t, c, tenant, root.ID, "target", "old")
	handle, err := c.OpenAt(context.Background(), testRetentionOwner, tenant, PresentationFileProvider, 1, target.ID, target.Revision)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = handle.Close() })

	result, err := c.Replace(context.Background(), tenant, source.ID, target.ID)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if result.Source.ID != source.ID || result.Source.Name != "target" {
		t.Fatalf("source after replace = %+v, want id=%s name=target", result.Source, source.ID)
	}
	if result.Target.ID != target.ID || !result.Target.Tombstone {
		t.Fatalf("target after replace = %+v, want tombstoned id=%s", result.Target, target.ID)
	}
	got, err := c.LookupName(context.Background(), tenant, PresentationFileProvider, root.ID, "target")
	if err != nil {
		t.Fatalf("LookupName(target): %v", err)
	}
	if got.ID != source.ID {
		t.Fatalf("target binding id = %s, want source %s", got.ID, source.ID)
	}
	if _, err := c.LookupName(context.Background(), tenant, PresentationFileProvider, root.ID, ".target.tmp"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupName(temp) err = %v, want ErrNotFound", err)
	}
	if _, err := c.Lookup(context.Background(), tenant, PresentationFileProvider, target.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(old target) err = %v, want ErrNotFound", err)
	}
	content, err := io.ReadAll(handle)
	if err != nil {
		t.Fatalf("read old handle: %v", err)
	}
	if string(content) != "old" {
		t.Fatalf("old handle content = %q, want old", content)
	}

	page, err := c.ChangesSince(context.Background(), tenant, EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}, CompleteChangeCursor(target.Revision), 10)
	if err != nil {
		t.Fatalf("ChangesSince: %v", err)
	}
	if len(page.Changes) != 2 {
		t.Fatalf("changes = %d, want 2", len(page.Changes))
	}
	if page.Changes[0].Kind != ChangeDelete || page.Changes[0].Object.ID != target.ID ||
		page.Changes[1].Kind != ChangeUpsert || page.Changes[1].Object.ID != source.ID ||
		page.Changes[0].Revision != page.Changes[1].Revision {
		t.Fatalf("replace changes = %+v, want ordered target delete/source upsert at one revision", page.Changes)
	}
}

func TestCasePolicyAndTenantLocalHeads(t *testing.T) {
	c := newTestCatalog(t)
	insensitive, rootA := createTestTenant(t, c, "insensitive", CaseInsensitive)
	sensitive, rootB := createTestTenant(t, c, "sensitive", CaseSensitive)
	created := createTestFile(t, c, insensitive, rootA.ID, "Straße", "a")
	got, err := c.LookupName(context.Background(), insensitive, PresentationFileProvider, rootA.ID, "STRASSE")
	if err != nil {
		t.Fatalf("case-folded LookupName: %v", err)
	}
	if got.ID != created.ID {
		t.Fatalf("case-folded id = %s, want %s", got.ID, created.ID)
	}
	createTestFile(t, c, sensitive, rootB.ID, "Straße", "b")
	if _, err := c.LookupName(context.Background(), sensitive, PresentationFileProvider, rootB.ID, "STRASSE"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("case-sensitive LookupName err = %v, want ErrNotFound", err)
	}
	headA, err := c.Head(context.Background(), insensitive)
	if err != nil {
		t.Fatalf("Head(A): %v", err)
	}
	headB, err := c.Head(context.Background(), sensitive)
	if err != nil {
		t.Fatalf("Head(B): %v", err)
	}
	if headA != 2 || headB != 2 {
		t.Fatalf("tenant heads = %d/%d, want independent 2/2", headA, headB)
	}
	createTestFile(t, c, insensitive, rootA.ID, "only-a", "x")
	headB2, _ := c.Head(context.Background(), sensitive)
	if headB2 != headB {
		t.Fatalf("tenant B head advanced from %d to %d after tenant A write", headB, headB2)
	}
}

func TestMutationFailpointsAreCrashAtomic(t *testing.T) {
	points := []string{
		mutationAfterBegin,
		mutationAfterRevision,
		mutationAfterApply,
		mutationAfterJournal,
		mutationAfterOutbox,
		mutationBeforeCommit,
		mutationAfterCommit,
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "catalog.sqlite")
			base, err := Open(ctx, path)
			if err != nil {
				t.Fatalf("Open(base): %v", err)
			}
			tenant, root := createTestTenant(t, base, "failpoint", CaseSensitive)
			before, _ := base.Head(ctx, tenant)
			if err := base.Close(); err != nil {
				t.Fatalf("Close(base): %v", err)
			}

			boom := errors.New("simulated crash")
			fired := false
			faulted, err := open(ctx, path, func(candidate string) error {
				if !fired && candidate == point {
					fired = true
					return boom
				}
				return nil
			})
			if err != nil {
				t.Fatalf("open(faulted): %v", err)
			}
			ref := stageTestContent(t, faulted, "payload")
			prepared, err := faulted.BeginMutation(ctx, tenant, before, MutationIntent{
				SourceID: "test",
				Origin:   testCausalOrigin(),
				Create:   &CreateMutation{Spec: fileSpec(root.ID, "new", ref, 1)},
			})
			if err != nil {
				t.Fatalf("BeginMutation: %v", err)
			}
			mutation := prepared.OperationID
			_, err = faulted.finishTestNamespaceMutation(ctx, prepared)
			if !errors.Is(err, boom) {
				t.Fatalf("Create err = %v, want failpoint", err)
			}
			if err := faulted.Close(); err != nil {
				t.Fatalf("Close(faulted): %v", err)
			}

			recovered, err := Open(ctx, path)
			if err != nil {
				t.Fatalf("Open(recovered): %v", err)
			}
			t.Cleanup(func() { _ = recovered.Close() })
			_, mutationErr := recovered.Mutation(ctx, tenant, mutation)
			outbox, outboxErr := claimConvergenceOutboxForTest(ctx, recovered)
			head, _ := recovered.Head(ctx, tenant)
			if point == mutationAfterCommit {
				if mutationErr != nil || outboxErr != nil || outbox == nil || len(outbox.Commits) != 1 ||
					outbox.Commits[0].Tenant != causal.TenantID(tenant) || outbox.Commits[0].CatalogRevision != causal.CatalogRevision(before+1) ||
					outbox.Change.OperationID != causalOperationID(mutation) || head != before+1 {
					t.Fatalf("post-commit recovery mutation=%v outbox=%+v/%v head=%d, want committed head=%d", mutationErr, outbox, outboxErr, head, before+1)
				}
				replayed, err := recovered.PreparedMutation(ctx, tenant, mutation)
				if err != nil {
					t.Fatalf("PreparedMutation(retry): %v", err)
				}
				result, err := recovered.finishTestNamespaceMutation(ctx, replayed)
				if err != nil || result.Primary.Name != "new" {
					t.Fatalf("idempotent retry = %+v, %v", result.Primary, err)
				}
				return
			}
			if !errors.Is(mutationErr, ErrNotFound) || outboxErr != nil || outbox != nil || head != before {
				t.Fatalf("pre-commit recovery mutation=%v outbox=%v head=%d, want absent head=%d", mutationErr, outboxErr, head, before)
			}
			if _, err := recovered.LookupName(ctx, tenant, PresentationFileProvider, root.ID, "new"); !errors.Is(err, ErrNotFound) {
				t.Fatalf("partial object survived: %v", err)
			}
		})
	}
}

func TestSnapshotPaginationStaysAtPinnedRevision(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "paging", CaseSensitive)
	for i := 0; i < 7; i++ {
		createTestFile(t, c, tenant, root.ID, fmt.Sprintf("f-%02d", i), fmt.Sprint(i))
	}
	revision, _ := c.Head(context.Background(), tenant)
	scope := EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}
	first, err := c.Snapshot(context.Background(), tenant, scope, revision, SnapshotCursor{}, 3)
	if err != nil {
		t.Fatalf("Snapshot(first): %v", err)
	}
	if first.Next == nil || len(first.Objects) != 3 {
		t.Fatalf("first page = %+v, want 3 and next", first)
	}
	newObject := createTestFile(t, c, tenant, root.ID, "after-snapshot", "later")
	seen := append([]Object(nil), first.Objects...)
	cursor := *first.Next
	for {
		page, err := c.Snapshot(context.Background(), tenant, scope, revision, cursor, 3)
		if err != nil {
			t.Fatalf("Snapshot(next): %v", err)
		}
		seen = append(seen, page.Objects...)
		if page.Next == nil {
			break
		}
		cursor = *page.Next
	}
	if len(seen) != 7 {
		t.Fatalf("snapshot objects = %d, want 7 children", len(seen))
	}
	for _, obj := range seen {
		if obj.ID == newObject.ID {
			t.Fatalf("new object %s leaked into pinned snapshot", newObject.ID)
		}
	}
}

func TestStaleAnchorAfterCompaction(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "stale", CaseSensitive)
	first := createTestFile(t, c, tenant, root.ID, "first", "1")
	createTestFile(t, c, tenant, root.ID, "second", "2")
	if _, err := maintainTestUntilIdle(context.Background(), c, tenant, first.Revision); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	scope := EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}
	_, err := c.ChangesSince(context.Background(), tenant, scope, CompleteChangeCursor(first.Revision-1), 10)
	var stale *StaleAnchorError
	if !errors.As(err, &stale) || stale.Floor != first.Revision {
		t.Fatalf("ChangesSince err = %v, want floor %d", err, first.Revision)
	}
	_, err = c.Snapshot(context.Background(), tenant, scope, first.Revision-1, SnapshotCursor{}, 10)
	if !errors.As(err, &stale) {
		t.Fatalf("Snapshot err = %v, want StaleAnchorError", err)
	}
}

func TestConcurrentReplaceHasOneWinner(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "concurrent", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "target", "old")
	sources := []Object{
		createTestFile(t, c, tenant, root.ID, "temp-a", "a"),
		createTestFile(t, c, tenant, root.ID, "temp-b", "b"),
	}
	var wg sync.WaitGroup
	results := make(chan error, len(sources))
	for _, source := range sources {
		wg.Add(1)
		go func(source Object) {
			defer wg.Done()
			_, err := c.Replace(context.Background(), tenant, source.ID, target.ID)
			results <- err
		}(source)
	}
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
			continue
		}
		if !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrMutationActive) && !errors.Is(err, errMutationHeadChanged) {
			t.Fatalf("losing Replace err = %v, want ErrNotFound, ErrMutationActive, or head change", err)
		}
	}
	if wins != 1 {
		t.Fatalf("replace winners = %d, want 1", wins)
	}
	bound, err := c.LookupName(context.Background(), tenant, PresentationFileProvider, root.ID, "target")
	if err != nil {
		t.Fatalf("LookupName(target): %v", err)
	}
	if bound.ID != sources[0].ID && bound.ID != sources[1].ID {
		t.Fatalf("target id = %s, want one source", bound.ID)
	}
}

func TestRandomReplacePreservesUniqueLiveBindings(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "property", CaseSensitive)
	rng := rand.New(rand.NewSource(42))
	objects := make([]Object, 0, 40)
	for i := 0; i < 40; i++ {
		objects = append(objects, createTestFile(t, c, tenant, root.ID, fmt.Sprintf("o-%02d", i), fmt.Sprint(i)))
	}
	for len(objects) > 1 {
		sourceIndex := rng.Intn(len(objects))
		targetIndex := rng.Intn(len(objects) - 1)
		if targetIndex >= sourceIndex {
			targetIndex++
		}
		source, target := objects[sourceIndex], objects[targetIndex]
		result, err := c.Replace(context.Background(), tenant, source.ID, target.ID)
		if err != nil {
			t.Fatalf("Replace(%s,%s): %v", source.ID, target.ID, err)
		}
		if result.Source.ID != source.ID || result.Target.ID != target.ID {
			t.Fatalf("identity changed: %+v", result)
		}
		next := objects[:0]
		for _, object := range objects {
			switch object.ID {
			case target.ID:
				continue
			case source.ID:
				object = result.Source
			}
			next = append(next, object)
		}
		objects = next
	}
}

func TestContentRevisionIsExactAndDeleteRecreateGetsNewIdentity(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "revision", CaseSensitive)
	first := createTestFile(t, c, tenant, root.ID, "file", "one")
	secondRef := stageTestContent(t, c, "two")
	_, err := c.Revise(context.Background(), tenant, first.ID, RevisionSpec{
		Parent: root.ID, Name: "file", Mode: first.Mode,
		Content:     &ContentUpdate{Revision: first.ContentRevision, Ref: secondRef},
		Convergence: first.Convergence,
		Visibility:  first.Visibility,
	})
	if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("same content revision err = %v, want ErrInvalidTransition", err)
	}
	revised, err := c.Revise(context.Background(), tenant, first.ID, RevisionSpec{
		Parent: root.ID, Name: "file", Mode: first.Mode,
		Content:     &ContentUpdate{Revision: first.ContentRevision + 1, Ref: secondRef},
		Convergence: Convergence{Desired: first.ContentRevision + 1},
		Visibility:  first.Visibility,
	})
	if err != nil {
		t.Fatalf("Revise: %v", err)
	}
	if revised.ID != first.ID || revised.Hash != secondRef.Hash || revised.Size != secondRef.Size {
		t.Fatalf("revised object = %+v", revised)
	}
	tombstone, err := c.Delete(context.Background(), tenant, revised.ID)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !tombstone.Tombstone {
		t.Fatal("Delete did not return tombstone")
	}
	recreated := createTestFile(t, c, tenant, root.ID, "file", "three")
	if recreated.ID == first.ID {
		t.Fatalf("recreated path reused deleted identity %s", first.ID)
	}
}

func TestMutationIdentityIsDerivedFromSemanticRequest(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "mutation-id", CaseSensitive)
	ctx := context.Background()
	head := mustCatalogHead(t, c, tenant)
	firstRef := stageTestContent(t, c, "content")
	secondRef := stageTestContent(t, c, "content")
	first, err := c.BeginMutation(ctx, tenant, head, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(),
		Create: &CreateMutation{Spec: fileSpec(root.ID, "one", firstRef, 1)},
	})
	if err != nil {
		t.Fatalf("BeginMutation(first): %v", err)
	}
	replayed, err := c.BeginMutation(ctx, tenant, head, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(),
		Create: &CreateMutation{Spec: fileSpec(root.ID, "one", secondRef, 1)},
	})
	if err != nil {
		t.Fatalf("BeginMutation(replay): %v", err)
	}
	if replayed.OperationID != first.OperationID {
		t.Fatalf("semantic replay id = %s, want %s", replayed.OperationID, first.OperationID)
	}
	if _, err := c.BeginMutation(ctx, tenant, head, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(),
		Create: &CreateMutation{Spec: fileSpec(root.ID, "two", secondRef, 1)},
	}); !errors.Is(err, ErrMutationActive) {
		t.Fatalf("different request err = %v, want ErrMutationActive", err)
	}
}

func newTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	c, err := Open(context.Background(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func mustCatalogHead(t *testing.T, c *Catalog, tenant TenantID) Revision {
	t.Helper()
	head, err := c.Head(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	return head
}

func createTestTenant(t *testing.T, c *Catalog, name string, policy CasePolicy) (TenantID, Object) {
	t.Helper()
	tenant, err := NewTenantID(name)
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	root, err := c.CreateTenant(context.Background(), tenant, policy, PresentMount|PresentFileProvider)
	if err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	return tenant, root
}

func createTestFile(t *testing.T, c *Catalog, tenant TenantID, parent ObjectID, name, content string) Object {
	t.Helper()
	ensureTestGeneration(t, c, tenant, 1)
	ref := stageTestContent(t, c, content)
	obj, err := c.Create(context.Background(), tenant, fileSpec(parent, name, ref, 1))
	if err != nil {
		t.Fatalf("Create(%s): %v", name, err)
	}
	return obj
}

func ensureTestGeneration(t *testing.T, c *Catalog, tenant TenantID, generation Generation) {
	t.Helper()
	state, err := c.LoadTenantState(context.Background(), tenant)
	if errors.Is(err, ErrStateNotFound) {
		if _, err := c.SaveTenantState(context.Background(), 0, TenantStateRecord{Tenant: tenant, Generation: generation}); err != nil {
			t.Fatalf("SaveTenantState(insert): %v", err)
		}
		return
	}
	if err != nil {
		t.Fatalf("LoadTenantState: %v", err)
	}
	if state.Generation == generation {
		return
	}
	state.Generation = generation
	if _, err := c.SaveTenantState(context.Background(), state.Version, state); err != nil {
		t.Fatalf("SaveTenantState(update): %v", err)
	}
}

func stageTestContent(t *testing.T, c *Catalog, content string) ContentRef {
	t.Helper()
	ref, err := c.StageContent(context.Background(), bytes.NewBufferString(content))
	if err != nil {
		t.Fatalf("StageContent: %v", err)
	}
	return ref
}

func fileSpec(parent ObjectID, name string, content ContentRef, revision Revision) CreateSpec {
	return CreateSpec{
		Parent: parent, Name: name, Kind: KindFile, Mode: 0o600,
		ContentRevision: revision, Content: content,
		Convergence: Convergence{Desired: revision}, Visibility: Visibility{Mount: true, FileProvider: true},
	}
}

func testQuarantine(revision Revision) *Quarantine {
	return &Quarantine{
		Lane: QuarantineLaneCatalogMutation, Revision: revision,
		Cause: QuarantineCauseConflict, Detail: "test conflict", Since: time.Unix(123, 0).UTC(),
	}
}
