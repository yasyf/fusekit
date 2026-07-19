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
	id := mustMutation(t)
	intent := testCreateIntent(root.ID, "file", ref)
	prepared, err := c.BeginMutation(ctx, id, tenant, mustCatalogHead(t, c, tenant), intent)
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	if prepared.OperationID != id || prepared.State != MutationPrepared {
		t.Fatalf("prepared = %+v", prepared)
	}
	if _, err := c.CommitMutation(ctx, id); !errors.Is(err, ErrMutationNotApplied) {
		t.Fatalf("CommitMutation before apply err = %v, want ErrMutationNotApplied", err)
	}
	boom := errors.New("source rejected write")
	owner := mustMutationOwner(t)
	claimed, err := c.ClaimMutation(ctx, id, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	prepared, err = c.PreparedMutation(ctx, id)
	if err != nil || prepared.State != MutationApplying || prepared.Claim == nil {
		t.Fatalf("prepared after unsettled external failure %v = %+v, %v", boom, prepared, err)
	}
	if _, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, "file"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("namespace visible before apply: %v", err)
	}
	if _, err := c.ClaimMutation(ctx, id, owner); !errors.Is(err, ErrMutationClaimed) {
		t.Fatalf("duplicate ClaimMutation = %v, want ErrMutationClaimed", err)
	}
	stale := *claimed.Claim
	claimed, err = c.ReclaimMutation(ctx, id, stale, owner)
	if err != nil {
		t.Fatalf("ReclaimMutation after settled failure: %v", err)
	}
	if _, err := c.MarkMutationApplied(ctx, id, stale); !errors.Is(err, ErrMutationClaimed) {
		t.Fatalf("MarkMutationApplied with stale fence = %v, want ErrMutationClaimed", err)
	}
	reader, err := c.OpenMutationContent(ctx, claimed.OperationID)
	if err != nil {
		t.Fatalf("OpenMutationContent: %v", err)
	}
	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close content: %v", err)
	}
	if string(content) != "payload" {
		t.Fatalf("external content = %q", content)
	}
	if _, err := c.MarkMutationApplied(ctx, id, *claimed.Claim); err != nil {
		t.Fatalf("MarkMutationApplied: %v", err)
	}
	if _, err := c.MarkMutationApplied(ctx, id, *claimed.Claim); err != nil {
		t.Fatalf("MarkMutationApplied idempotent retry: %v", err)
	}
	result, err := c.CommitMutation(ctx, id)
	if err != nil {
		t.Fatalf("CommitMutation: %v", err)
	}
	if result.Primary.Name != "file" || result.Mutation.ID != id {
		t.Fatalf("result = %+v", result)
	}
	pending, err := c.PendingMutations(ctx, tenant)
	if err != nil || len(pending) != 0 {
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
	id := mustMutation(t)
	if _, err := c.BeginMutation(ctx, id, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref)); err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
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
	pending, err := c.PendingMutations(ctx, tenant)
	if err != nil || len(pending) != 1 || pending[0].State != MutationApplying || pending[0].Claim == nil {
		t.Fatalf("restart pending = %+v, %v", pending, err)
	}
	if err := c.Compact(ctx, tenant, pending[0].ExpectedHead); err != nil {
		t.Fatalf("Compact(prepared stage): %v", err)
	}
	claimed, err = c.ReclaimMutation(ctx, id, *pending[0].Claim, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ReclaimMutation(restart): %v", err)
	}
	applies.Add(1)
	reader, err := c.OpenMutationContent(ctx, claimed.OperationID)
	if err != nil {
		t.Fatalf("OpenMutationContent(replay): %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close replay content: %v", err)
	}
	if _, err := c.MarkMutationApplied(ctx, id, *claimed.Claim); err != nil {
		t.Fatalf("MarkMutationApplied(replay): %v", err)
	}
	if _, err := c.CommitMutation(ctx, id); err != nil {
		t.Fatalf("CommitMutation: %v", err)
	}
	if applies.Load() != 2 {
		t.Fatalf("external apply count = %d, want initial + idempotent replay", applies.Load())
	}
}

func TestPreparedMutationRecoversCatalogCommitBeforeIntentRetirement(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	boom := errors.New("crash after catalog commit")
	var armed atomic.Bool
	c, err := open(ctx, path, func(point string) error {
		if point == preparedAfterCatalogCommit && armed.CompareAndSwap(true, false) {
			return boom
		}
		return nil
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	tenant, root := createTestTenant(t, c, "prepared-catalog-crash", CaseSensitive)
	id := mustMutation(t)
	ref := stageTestContent(t, c, "payload")
	prepared, err := c.BeginMutation(ctx, id, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref))
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	markTestMutationApplied(t, c, id)
	armed.Store(true)
	if _, err := c.CommitMutation(ctx, id); !errors.Is(err, boom) {
		t.Fatalf("CommitMutation crash = %v, want boom", err)
	}
	if _, err := c.Mutation(ctx, id); err != nil {
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
	pending, err := c.PendingMutations(ctx, tenant)
	if err != nil || len(pending) != 1 || pending[0].State != MutationApplied {
		t.Fatalf("pending after catalog commit crash = %+v, %v", pending, err)
	}
	if _, err := c.CommitMutation(ctx, id); err != nil {
		t.Fatalf("CommitMutation(recover): %v", err)
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
	id := mustMutation(t)
	ref := stageTestContent(t, c, "payload")
	prepared, err := c.BeginMutation(ctx, id, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref))
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	owner := mustMutationOwner(t)
	claimed, err := c.ClaimMutation(ctx, id, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	if _, err := c.ClaimMutation(ctx, id, owner); !errors.Is(err, ErrMutationClaimed) {
		t.Fatalf("concurrent duplicate ClaimMutation = %v, want ErrMutationClaimed", err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	applyResult := make(chan error, 1)
	go func() {
		close(started)
		<-release
		_, err := c.MarkMutationApplied(ctx, id, *claimed.Claim)
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
		t.Fatalf("MarkMutationApplied: %v", err)
	}
	commitResults := make(chan error, 2)
	for range 2 {
		go func() { _, err := c.CommitMutation(ctx, id); commitResults <- err }()
	}
	for range 2 {
		if err := <-commitResults; err != nil {
			t.Fatalf("CommitMutation: %v", err)
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
	id := mustMutation(t)
	ref := stageTestContent(t, c, "payload")
	if _, err := c.BeginMutation(ctx, id, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref)); err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
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
	if _, err := c.MarkMutationApplied(ctx, id, stale); !errors.Is(err, ErrMutationClaimed) {
		t.Fatalf("stale claim after reclaim race = %v, want ErrMutationClaimed", err)
	}
	if _, err := c.MarkMutationApplied(ctx, id, *winner.Claim); err != nil {
		t.Fatalf("MarkMutationApplied(winner): %v", err)
	}
}

func TestPreparedMutationHeadConflictEntersDurableRecovery(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "prepared-head-conflict", CaseSensitive)
	id := mustMutation(t)
	ref := stageTestContent(t, c, "payload")
	if _, err := c.BeginMutation(ctx, id, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref)); err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	markTestMutationApplied(t, c, id)
	if _, err := c.db.ExecContext(ctx,
		"UPDATE tenants SET head = head + 1 WHERE tenant = ?", string(tenant)); err != nil {
		t.Fatalf("inject expected-head drift: %v", err)
	}
	if _, err := c.CommitMutation(ctx, id); !errors.Is(err, ErrMutationRecoveryRequired) {
		t.Fatalf("CommitMutation head conflict = %v, want recovery", err)
	}
	prepared, err := c.PreparedMutation(ctx, id)
	if err != nil || prepared.State != MutationRecoveryRequired {
		t.Fatalf("prepared recovery state = %+v, %v", prepared, err)
	}
	if _, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, "file"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("catalog published impossible source mutation: %v", err)
	}
	otherRef := stageTestContent(t, c, "other")
	if _, err := c.BeginMutation(ctx, mustMutation(t), tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "other", otherRef)); !errors.Is(err, ErrMutationActive) {
		t.Fatalf("second BeginMutation = %v, want ErrMutationActive", err)
	}
}

func TestPreparedMutationBlocksOtherHeadChangingTransactions(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "prepared-head-guard", CaseSensitive)
	interest, err := c.AddInterest(ctx, mustMutation(t), tenant, root.ID, fileProviderInterestOwner("existing"), 1)
	if err != nil {
		t.Fatalf("AddInterest(setup): %v", err)
	}
	id := mustMutation(t)
	ref := stageTestContent(t, c, "payload")
	prepared, err := c.BeginMutation(ctx, id, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref))
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	markTestMutationApplied(t, c, id)
	if _, err := c.AddInterest(ctx, mustMutation(t), tenant, root.ID, fileProviderInterestOwner("interleaver"), 1); !errors.Is(err, ErrMutationActive) {
		t.Fatalf("AddInterest during applied intent = %v, want ErrMutationActive", err)
	}
	if _, err := c.RemoveInterest(ctx, mustMutation(t), tenant, interest.ID); !errors.Is(err, ErrMutationActive) {
		t.Fatalf("RemoveInterest during applied intent = %v, want ErrMutationActive", err)
	}
	head, err := c.Head(ctx, tenant)
	if err != nil || head != prepared.ExpectedHead {
		t.Fatalf("head after rejected interleave = %d, %v, want %d", head, err, prepared.ExpectedHead)
	}
	if _, err := c.CommitMutation(ctx, id); err != nil {
		t.Fatalf("CommitMutation: %v", err)
	}
}

func TestBeginMutationRejectsCommittedOperationIDBeforeExternalApply(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "prepared-operation-id", CaseSensitive)
	id := mustMutation(t)
	if _, err := c.AddInterest(ctx, id, tenant, root.ID, fileProviderInterestOwner("existing"), 1); err != nil {
		t.Fatalf("AddInterest: %v", err)
	}
	ref := stageTestContent(t, c, "payload")
	if _, err := c.BeginMutation(ctx, id, tenant, mustCatalogHead(t, c, tenant), testCreateIntent(root.ID, "file", ref)); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("BeginMutation reused committed operation id = %v, want ErrMutationConflict", err)
	}
	if _, err := c.PreparedMutation(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected operation id created prepared intent: %v", err)
	}
}

func testCreateIntent(parent ObjectID, name string, ref ContentRef) MutationIntent {
	return MutationIntent{
		SourceID: "test-source", SourceMetadata: "operation-metadata",
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

func markTestMutationApplied(t *testing.T, c *Catalog, id MutationID) PreparedMutation {
	t.Helper()
	claimed, err := c.ClaimMutation(context.Background(), id, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	applied, err := c.MarkMutationApplied(context.Background(), id, *claimed.Claim)
	if err != nil {
		t.Fatalf("MarkMutationApplied: %v", err)
	}
	return applied
}
