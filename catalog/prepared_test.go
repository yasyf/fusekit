package catalog

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
)

func TestPreparedMutationRequiresExternalApplyAndKeepsContentOnFailure(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "prepared-apply", CaseSensitive)
	ref := stageTestContent(t, c, "payload")
	intent := testCreateIntent(root.ID, "file", ref)
	prepared, err := c.BeginMutation(ctx, tenant, mustCatalogHead(t, c, tenant), intent)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	id := prepared.OperationID
	if id == (MutationID{}) || prepared.State != MutationPrepared {
		t.Fatalf("prepared = %+v", prepared)
	}
	boom := errors.New("source rejected write")
	owner := mustMutationOwner(t)
	claimed, err := c.ClaimMutation(ctx, id, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	prepared, err = c.PreparedMutation(ctx, tenant, id)
	if err != nil || prepared.State != MutationApplying || prepared.Claim == nil {
		t.Fatalf("prepared after unsettled external failure %v = %+v, %v", boom, prepared, err)
	}
	if _, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, "file"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("namespace visible before apply: %v", err)
	}
	if replayed, err := c.ClaimMutation(ctx, id, owner); err != nil || replayed.Claim == nil || *replayed.Claim != *claimed.Claim {
		t.Fatalf("duplicate ClaimMutation replay = %+v, %v", replayed, err)
	}
	stale := *claimed.Claim
	claimed, err = c.ReclaimMutation(ctx, id, stale, owner)
	if err != nil {
		t.Fatalf("ReclaimMutation after settled failure: %v", err)
	}
	if replayed, err := c.ReclaimMutation(ctx, id, stale, owner); err != nil || replayed.Claim == nil || *replayed.Claim != *claimed.Claim {
		t.Fatalf("ReclaimMutation replay = %+v, %v", replayed, err)
	}
	reader, err := c.OpenMutationContent(ctx, tenant, claimed.OperationID)
	if err != nil {
		t.Fatalf("OpenMutationContent: %v", err)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := reader.Settle(nil); err != nil {
		t.Fatalf("Settle content: %v", err)
	}
	if err := reader.Wait(ctx); err != nil {
		t.Fatalf("Wait content: %v", err)
	}
	if string(content) != "payload" {
		t.Fatalf("external content = %q", content)
	}
	result, err := c.finishTestNamespaceMutation(ctx, claimed)
	if err != nil {
		t.Fatalf("finish mutation: %v", err)
	}
	if result.Primary.Name != "file" || result.Mutation.ID != id {
		t.Fatalf("result = %+v", result)
	}
	pending, err := c.PendingMutation(ctx, tenant)
	if err != nil || pending != nil {
		t.Fatalf("pending after commit = %+v, %v", pending, err)
	}
}

func TestPreparedMutationReplaysExternalApplyAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	tenant, root := createTestTenant(t, c, "prepared-replay", CaseSensitive)
	ref := stageTestContent(t, c, "restart-payload")
	prepared, err := c.BeginMutation(
		ctx, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref),
	)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	id := prepared.OperationID
	var applies atomic.Int32
	claimed, err := c.ClaimMutation(ctx, id, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	applies.Add(1)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(restart): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	pending, err := c.PendingMutation(ctx, tenant)
	if err != nil || pending == nil || pending.State != MutationApplying || pending.Claim == nil {
		t.Fatalf("restart pending = %+v, %v", pending, err)
	}
	if _, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow()); err != nil {
		t.Fatalf("tenant maintenance(prepared stage): %v", err)
	}
	if _, err := c.MaintainGlobal(ctx, testMaintenanceNow()); err != nil {
		t.Fatalf("global maintenance(prepared stage): %v", err)
	}
	claimed, err = c.ReclaimMutation(ctx, id, *pending.Claim, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ReclaimMutation(restart): %v", err)
	}
	applies.Add(1)
	reader, err := c.OpenMutationContent(ctx, tenant, claimed.OperationID)
	if err != nil {
		t.Fatalf("OpenMutationContent(replay): %v", err)
	}
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("Read replay content: %v", err)
	}
	if err := reader.Settle(nil); err != nil {
		t.Fatalf("Settle replay content: %v", err)
	}
	if err := reader.Wait(ctx); err != nil {
		t.Fatalf("Wait replay content: %v", err)
	}
	if _, err := c.finishTestNamespaceMutation(ctx, claimed); err != nil {
		t.Fatalf("finish mutation: %v", err)
	}
	if applies.Load() != 2 {
		t.Fatalf("external apply count = %d, want initial + idempotent replay", applies.Load())
	}
}

func TestPreparedMutationReplaysLostCommitResponseAcrossRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	boom := errors.New("crash after catalog commit")
	var armed atomic.Bool
	c, err := open(ctx, path, func(point string) error {
		if point == sourceDriverAfterVisibilityCommitPoint && armed.CompareAndSwap(true, false) {
			return boom
		}
		return nil
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	tenant, root := createTestTenant(t, c, "prepared-catalog-crash", CaseSensitive)
	ref := stageTestContent(t, c, "payload")
	prepared, err := c.BeginMutation(
		ctx, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref),
	)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	id := prepared.OperationID
	claimed, err := c.ClaimMutation(ctx, id, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	armed.Store(true)
	if _, err := c.finishTestNamespaceMutation(ctx, claimed); !errors.Is(err, boom) {
		t.Fatalf("finish mutation crash = %v, want boom", err)
	}
	if _, err := c.Mutation(ctx, tenant, id); err != nil {
		t.Fatalf("catalog mutation was not committed: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(restart): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	replayed, err := c.PreparedMutation(ctx, tenant, id)
	if err != nil || replayed.State != MutationCommitted {
		t.Fatalf("committed mutation after lost response = %+v, %v", replayed, err)
	}
	if _, err := c.finishTestNamespaceMutation(ctx, replayed); err != nil {
		t.Fatalf("finish mutation(recover): %v", err)
	}
	head, err := c.Head(ctx, tenant)
	if err != nil || head != prepared.ExpectedHead+1 {
		t.Fatalf("head = %d, %v, want %d", head, err, prepared.ExpectedHead+1)
	}
}

func TestConcurrentPreparedApplyAndCommitCoalesce(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "prepared-concurrent", CaseSensitive)
	ref := stageTestContent(t, c, "payload")
	prepared, err := c.BeginMutation(
		ctx, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref),
	)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	id := prepared.OperationID
	owner := mustMutationOwner(t)
	claimed, err := c.ClaimMutation(ctx, id, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	if replayed, err := c.ClaimMutation(ctx, id, owner); err != nil || replayed.Claim == nil || *replayed.Claim != *claimed.Claim {
		t.Fatalf("concurrent duplicate ClaimMutation replay = %+v, %v", replayed, err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	applyResult := make(chan error, 1)
	go func() {
		close(started)
		<-release
		_, err := c.finishTestNamespaceMutation(ctx, claimed)
		applyResult <- err
	}()
	<-started
	responsive, cancel := context.WithTimeout(ctx, concurrencyTestTimeout)
	if _, err := c.Head(responsive, tenant); err != nil {
		cancel()
		t.Fatalf("Head during external apply: %v", err)
	}
	if _, err := c.Lookup(responsive, tenant, PresentationFileProvider, root.ID); err != nil {
		cancel()
		t.Fatalf("Lookup during external apply: %v", err)
	}
	cancel()
	close(release)
	if err := <-applyResult; err != nil {
		t.Fatalf("finish mutation: %v", err)
	}
	commitResults := make(chan error, 2)
	for range 2 {
		go func() { _, err := c.finishTestNamespaceMutation(ctx, claimed); commitResults <- err }()
	}
	for range 2 {
		if err := <-commitResults; err != nil {
			t.Fatalf("finish mutation: %v", err)
		}
	}
	head, err := c.Head(ctx, tenant)
	if err != nil || head != prepared.ExpectedHead+1 {
		t.Fatalf("head = %d, %v, want %d", head, err, prepared.ExpectedHead+1)
	}
}

func TestConcurrentMutationReclaimHasOneFenceWinner(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "prepared-reclaim-race", CaseSensitive)
	ref := stageTestContent(t, c, "payload")
	prepared, err := c.BeginMutation(
		ctx, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref),
	)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	id := prepared.OperationID
	claimed, err := c.ClaimMutation(ctx, id, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	stale := *claimed.Claim
	type reclaimResult struct {
		mutation PreparedMutation
		err      error
	}
	results := make(chan reclaimResult, 2)
	for range 2 {
		owner := mustMutationOwner(t)
		go func() {
			mutation, err := c.ReclaimMutation(ctx, id, stale, owner)
			results <- reclaimResult{mutation: mutation, err: err}
		}()
	}
	var winner PreparedMutation
	conflicts := 0
	for range 2 {
		result := <-results
		switch {
		case result.err == nil:
			winner = result.mutation
		case errors.Is(result.err, ErrMutationClaimed):
			conflicts++
		default:
			t.Fatalf("ReclaimMutation: %v", result.err)
		}
	}
	if conflicts != 1 || winner.Claim == nil || winner.Claim.Epoch != stale.Epoch+1 {
		t.Fatalf("reclaim race winner=%+v conflicts=%d", winner, conflicts)
	}
	if _, err := c.finishTestNamespaceMutation(ctx, winner); err != nil {
		t.Fatalf("finish mutation(winner): %v", err)
	}
}

func TestPreparedMutationRejectsCorruptHeadDriftAndStaysUncommitted(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "prepared-head-conflict", CaseSensitive)
	ref := stageTestContent(t, c, "payload")
	started, err := c.BeginMutation(
		ctx, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref),
	)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	id := started.OperationID
	claimed, err := c.ClaimMutation(ctx, id, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	if _, err := c.db.ExecContext(ctx,
		"UPDATE tenants SET head = head + 1 WHERE tenant = ?", string(tenant)); err != nil {
		t.Fatalf("inject expected-head drift: %v", err)
	}
	if _, err := c.finishTestNamespaceMutation(ctx, claimed); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("finish mutation corrupt head drift = %v, want integrity failure", err)
	}
	prepared, err := c.PreparedMutation(ctx, tenant, id)
	if err != nil || prepared.State != MutationApplying {
		t.Fatalf("prepared uncommitted state = %+v, %v", prepared, err)
	}
	if _, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, "file"); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("corrupt head drift lookup = %v, want integrity failure", err)
	}
}

func TestPreparedReplaceRejectsAbsentOrMismatchedPrivateSource(t *testing.T) {
	ctx := context.Background()
	store := newTestCatalog(t)
	tenant, root := createTestTenant(t, store, "private-replace-owner", CaseSensitive)
	target := createTestFile(t, store, tenant, root.ID, "settings.json", "old")
	ref := stageTestContent(t, store, "new")
	spec := fileSpec(root.ID, ".settings.json.tmp", ref, 1)
	spec.Visibility = Visibility{}
	private, err := store.Create(ctx, tenant, spec)
	if err != nil {
		t.Fatalf("Create(private): %v", err)
	}
	capability, found, err := readPrivatePromotionSource(ctx, store.readDB, tenant, private.ID, "test")
	if err != nil || !found {
		t.Fatalf("private capability = %+v, found %t, err %v", capability, found, err)
	}
	otherTenant, otherRoot := createTestTenant(t, store, "private-replace-other", CaseSensitive)
	otherTarget := createTestFile(t, store, otherTenant, otherRoot.ID, "settings.json", "other")

	tests := []struct {
		name     string
		tenant   TenantID
		sourceID string
		source   ObjectID
		target   ObjectID
	}{
		{name: "absent object", tenant: tenant, sourceID: "test", source: ObjectID{0xff}, target: target.ID},
		{name: "wrong source owner", tenant: tenant, sourceID: "other-source", source: private.ID, target: target.ID},
		{name: "wrong tenant", tenant: otherTenant, sourceID: "test", source: private.ID, target: otherTarget.ID},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			head, err := store.Head(ctx, test.tenant)
			if err != nil {
				t.Fatal(err)
			}
			creator := capability.Mutation
			_, err = store.BeginMutation(ctx, test.tenant, head, MutationIntent{
				SourceID: test.sourceID, Origin: testCausalOrigin(),
				Disposition: MutationDispositionNamespace,
				Replace:     &ReplaceMutation{Source: test.source, Target: test.target, PrivateCreator: &creator},
			})
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("BeginMutation = %v, want ErrNotFound", err)
			}
		})
	}

	durable, found, err := readPrivatePromotionSource(ctx, store.readDB, tenant, private.ID, "test")
	if err != nil || !found || durable.ObjectID != private.ID {
		t.Fatalf("private source after rejected replacements = %+v, found %t, err %v", durable, found, err)
	}
}

func testCreateIntent(parent ObjectID, name string, ref ContentRef) MutationIntent {
	return MutationIntent{
		SourceID: "test-source", SourceMetadata: "operation-metadata", Disposition: MutationDispositionNamespace,
		Origin: testCausalOrigin(),
		Create: &CreateMutation{Spec: fileSpec(parent, name, ref, 1)},
	}
}

func mustMutationOwner(t *testing.T) MutationOwnerID {
	t.Helper()
	owner, err := NewMutationOwnerID()
	if err != nil {
		t.Fatalf("NewMutationOwnerID: %v", err)
	}
	return owner
}
