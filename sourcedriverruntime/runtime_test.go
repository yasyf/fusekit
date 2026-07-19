package sourcedriverruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
)

const runtimeTestAuthority causal.SourceAuthorityID = "driver-test"

func TestHundredTargetAuthorityUsesOneDriverCallAndAdvancesAllWatermarks(t *testing.T) {
	targets := runtimeTargets(100)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	first, err := runtime.Reconcile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !first.Changed || first.Stage.Checkpoint.TargetCount != uint64(len(targets)) {
		t.Fatalf("first authority result = %+v", first)
	}
	driver.mu.Lock()
	refreshCalls, snapshotCalls, changeCalls := driver.refreshCalls, driver.snapshotCalls, driver.changeCalls
	driver.mu.Unlock()
	if refreshCalls != 1 || snapshotCalls != 1 || changeCalls != 0 {
		t.Fatalf("100-target snapshot calls = refresh %d snapshot %d changes %d", refreshCalls, snapshotCalls, changeCalls)
	}
	assertStoreTargetWatermarks(t, store, targets, 1)

	driver.mu.Lock()
	driver.head = "head-2"
	driver.mu.Unlock()
	second, err := runtime.Reconcile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !second.Changed || second.Stage.Checkpoint.TargetCount != uint64(len(targets)) {
		t.Fatalf("delta authority result = %+v", second)
	}
	driver.mu.Lock()
	refreshCalls, snapshotCalls, changeCalls = driver.refreshCalls, driver.snapshotCalls, driver.changeCalls
	driver.mu.Unlock()
	if refreshCalls != 2 || snapshotCalls != 1 || changeCalls != 1 {
		t.Fatalf("100-target delta calls = refresh %d snapshot %d changes %d", refreshCalls, snapshotCalls, changeCalls)
	}
	assertStoreTargetWatermarks(t, store, targets, 2)
}

func TestResumeStageDeclaresAtMostOnePageFromTenThousandTargetsPerTurn(t *testing.T) {
	targets := make([]catalog.SourceDriverTarget, sourcedriver.MaxTargets)
	for index := range targets {
		targets[index] = catalog.SourceDriverTarget{
			Tenant: catalog.TenantID(fmt.Sprintf("tenant-%05d", index)), Generation: 1,
		}
	}
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	config := normalizeConfig(runtimeTestConfig(store, driver, targets))
	runtime := &Runtime{config: config}
	identity := runtimeTestIdentity(config, catalog.SourceDriverSnapshot, "", "head-1", 0)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}

	for turn := 1; turn <= 3; turn++ {
		state, err := store.PendingSourceDriverStage(t.Context(), runtimeTestAuthority)
		if err != nil || state == nil {
			t.Fatalf("turn %d pending stage = %+v, %v", turn, state, err)
		}
		if _, err := runtime.resumeStage(t.Context(), *state); !errors.Is(err, errProgressPending) {
			t.Fatalf("turn %d resume = %v, want durable progress", turn, err)
		}
		driver.mu.Lock()
		declareCalls, snapshotCalls := driver.declareCalls, driver.snapshotCalls
		driver.mu.Unlock()
		if declareCalls != turn || snapshotCalls != 0 {
			t.Fatalf("turn %d driver calls = declarations %d snapshots %d", turn, declareCalls, snapshotCalls)
		}
	}
}

func TestResumeStageStagesAtMostOneLargeSnapshotPagePerTurn(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	driver.objects = make([]sourcedriver.Projection, 513)
	for index := range driver.objects {
		driver.objects[index] = runtimeProjection(targets[0], sourcedriver.LogicalID(fmt.Sprintf("item-%04d", index)))
	}
	config := runtimeTestConfig(store, driver, targets)
	config.PageLimit = 1
	config = normalizeConfig(config)
	runtime := &Runtime{config: config}
	identity := runtimeTestIdentity(config, catalog.SourceDriverSnapshot, "", "head-1", 0)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}

	for turn := 1; turn <= 4; turn++ {
		state, err := store.PendingSourceDriverStage(t.Context(), runtimeTestAuthority)
		if err != nil || state == nil {
			t.Fatalf("turn %d pending stage = %+v, %v", turn, state, err)
		}
		if _, err := runtime.resumeStage(t.Context(), *state); !errors.Is(err, errProgressPending) {
			t.Fatalf("turn %d resume = %v, want durable progress", turn, err)
		}
		driver.mu.Lock()
		snapshotCalls := driver.snapshotCalls
		driver.mu.Unlock()
		wantSnapshots := max(0, turn-1)
		if snapshotCalls != wantSnapshots {
			t.Fatalf("turn %d snapshot calls = %d, want %d", turn, snapshotCalls, wantSnapshots)
		}
	}
}

func TestTargetDeclarationLostResponseRecoversByExactInspection(t *testing.T) {
	targets := runtimeTargets(300)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	driver.declareFailures = 1
	runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	result, err := runtime.Reconcile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	state, calls := driver.targetSet, driver.declareCalls
	driver.mu.Unlock()
	if !result.Changed || !state.Complete || state.DeclaredCount != uint64(len(targets)) || calls != 3 {
		t.Fatalf("lost declaration response = result %+v state %+v calls %d", result, state, calls)
	}
}

func TestUnchangedHeadWithNewTargetGenerationRebindsAndForcesSnapshot(t *testing.T) {
	oldTargets := runtimeTargets(1)
	oldDigest, err := catalog.SourceDriverTargetsDigest(oldTargets)
	if err != nil {
		t.Fatal(err)
	}
	newTargets := append([]catalog.SourceDriverTarget(nil), oldTargets...)
	newTargets[0].Generation = 2
	store := newRuntimeTestStore(newTargets)
	store.checkpoint = &catalog.SourceDriverCheckpoint{
		Authority: runtimeTestAuthority, FleetOwner: "fleet", AuthorityGeneration: 1,
		DeclarationDigest: [32]byte{1}, TargetCount: 1, TargetsDigest: oldDigest,
		Token: "head-1", SourceRevision: 1,
	}
	driver := newRuntimeTestDriver(newTargets[0])
	config := runtimeTestConfig(store, driver, newTargets)
	config.AuthorityGeneration = 2
	runtime, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	result, err := runtime.Reconcile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	snapshotCalls := driver.snapshotCalls
	driver.mu.Unlock()
	if !result.Changed || snapshotCalls != 1 || result.Checkpoint.AuthorityGeneration != 2 ||
		result.Checkpoint.SnapshotRequired != 0 || result.Checkpoint.SourceRevision != 2 {
		t.Fatalf("generation-rebound snapshot = %+v calls=%d", result, snapshotCalls)
	}
}

func TestConstructionResumesPendingCursorWithoutReplayingPriorPage(t *testing.T) {
	targets := runtimeTargets(2)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	driver.objects = append(driver.objects, sourcedriver.Projection{
		Tenant: targets[0].Tenant, Generation: causal.Generation(targets[0].Generation),
		ID: "second", Parent: "", Name: "second", Kind: catalog.KindDirectory,
		Mode: 0o755, Visibility: catalog.Visibility{Mount: true},
	})
	config := runtimeTestConfig(store, driver, targets)
	config.PageLimit = 1
	config = normalizeConfig(config)
	identity := runtimeTestIdentity(config, catalog.SourceDriverSnapshot, "", "head-1", 0)
	request := runtimeSnapshotRequest(config, "head-1", nil)
	first, err := driver.Snapshot(t.Context(), runtimeTestAuthority, request)
	if err != nil || first.Next == nil {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	driver.mu.Lock()
	driver.snapshotCalls = 0
	driver.requestedAfter = nil
	driver.mu.Unlock()
	cursor, err := encodeCursor(first.Next)
	if err != nil {
		t.Fatal(err)
	}
	store.pending = &catalog.SourceDriverStageState{
		Identity:    identity,
		TargetEpoch: 1,
		Stage: catalog.SourcePublicationStageRef{
			Authority: runtimeTestAuthority, Operation: identity.Operation,
			Sequence: 1, Items: 1, Bytes: 1, Revision: 1, Digest: [32]byte{1},
		},
		Cursor: cursor, PageDigest: first.Digest,
	}
	runtime, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	driver.mu.Lock()
	snapshotCalls := driver.snapshotCalls
	after := append([]sourcedriver.LogicalID(nil), driver.requestedAfter...)
	driver.mu.Unlock()
	if snapshotCalls != 1 || len(after) != 1 || after[0] != first.Next.After {
		t.Fatalf("restart calls = %d after = %v; want only continuation after %q", snapshotCalls, after, first.Next.After)
	}
	store.mu.Lock()
	pending, checkpoint := store.pending, store.checkpoint
	store.mu.Unlock()
	if pending != nil || checkpoint == nil || checkpoint.Token != "head-1" {
		t.Fatalf("restart recovery = pending %+v checkpoint %+v", pending, checkpoint)
	}
}

func TestConstructionRejectsPendingCursorUnderDifferentPageLimitBeforeDriverCall(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	driver.objects = append(driver.objects, sourcedriver.Projection{
		Tenant: targets[0].Tenant, Generation: causal.Generation(targets[0].Generation),
		ID: "second", Name: "second", Kind: catalog.KindDirectory,
		Mode: 0o755, Visibility: catalog.Visibility{Mount: true},
	})
	config := runtimeTestConfig(store, driver, targets)
	config.PageLimit = 1
	config = normalizeConfig(config)
	identity := runtimeTestIdentity(config, catalog.SourceDriverSnapshot, "", "head-1", 0)
	first, err := driver.Snapshot(t.Context(), runtimeTestAuthority, runtimeSnapshotRequest(config, "head-1", nil))
	if err != nil || first.Next == nil {
		t.Fatalf("first page = %+v, %v", first, err)
	}
	cursor, err := encodeCursor(first.Next)
	if err != nil {
		t.Fatal(err)
	}
	store.pending = &catalog.SourceDriverStageState{
		Identity:    identity,
		TargetEpoch: 1,
		Stage: catalog.SourcePublicationStageRef{
			Authority: runtimeTestAuthority, Operation: identity.Operation,
			Sequence: 1, Items: 1, Bytes: 1, Revision: 1, Digest: [32]byte{1},
		},
		Cursor: cursor, PageDigest: first.Digest,
	}
	driver.mu.Lock()
	driver.snapshotCalls = 0
	driver.mu.Unlock()
	config.PageLimit = 2
	if _, err := NewRuntime(t.Context(), config); !errors.Is(err, sourcedriver.ErrInvalidValue) {
		t.Fatalf("cross-limit restart = %v, want invalid value", err)
	}
	driver.mu.Lock()
	snapshotCalls := driver.snapshotCalls
	driver.mu.Unlock()
	if snapshotCalls != 0 {
		t.Fatalf("cross-limit restart dispatched %d driver pages", snapshotCalls)
	}
}

func TestConstructionAbortsPriorGenerationPublicationBeforeRebind(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	config := normalizeConfig(runtimeTestConfig(store, driver, targets))
	identity := runtimeTestIdentity(config, catalog.SourceDriverSnapshot, "", "head-1", 0)
	store.pending = &catalog.SourceDriverStageState{
		Identity:    identity,
		TargetEpoch: 1,
		Stage: catalog.SourcePublicationStageRef{
			Authority: runtimeTestAuthority, Operation: identity.Operation,
			Sequence: 1, Items: 1, Bytes: 1, Revision: 1, Digest: [32]byte{1},
		},
	}
	config.AuthorityGeneration = 2

	runtime, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	store.mu.Lock()
	pending, aborts := store.pending, store.aborts
	store.mu.Unlock()
	driver.mu.Lock()
	snapshotCalls, changeCalls := driver.snapshotCalls, driver.changeCalls
	driver.mu.Unlock()
	if pending != nil || aborts != 1 || snapshotCalls != 0 || changeCalls != 0 {
		t.Fatalf("prior generation recovery = pending %+v aborts %d snapshot %d changes %d", pending, aborts, snapshotCalls, changeCalls)
	}
}

func TestConstructionFinishesPendingMutationBeforeAdmission(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	config := normalizeConfig(runtimeTestConfig(store, driver, targets))
	identity := runtimeTestIdentity(config, catalog.SourceDriverMutation, "head-1", "head-2", 1)
	identity.SnapshotReason = 0
	identity.Cause = causal.CauseDaemonWrite
	identity.Mutation = catalog.MutationID{4}
	identity.MutationTenant = targets[0].Tenant
	identity.MutationGeneration = targets[0].Generation
	identity.Claim = catalog.MutationClaim{Owner: catalog.MutationOwnerID{1}, Epoch: 1}
	identity.MutationRequestDigest = [sha256.Size]byte{5}
	receipt := sourcedriver.MutationReceipt{
		OperationID: identity.Mutation, State: sourcedriver.MutationApplied,
		RequestDigest: identity.MutationRequestDigest, Expected: "head-1", Committed: "head-2",
	}
	var err error
	receipt.Digest, err = sourcedriver.MutationReceiptDigest(receipt)
	if err != nil {
		t.Fatal(err)
	}
	identity.MutationReceiptDigest = receipt.Digest
	driver.receipt = &receipt
	proof := catalog.SourceDriverMutationReceiptProof{
		ToToken: string(receipt.Committed), Digest: receipt.Digest,
	}
	store.reservation = &catalog.SourceDriverMutationReservation{
		SourceDriverMutationReservationRequest: catalog.SourceDriverMutationReservationRequest{
			Mutation: identity.Mutation, Claim: identity.Claim, Authority: identity.Authority,
			FleetOwner: identity.FleetOwner, AuthorityGeneration: identity.AuthorityGeneration,
			DeclarationDigest: identity.DeclarationDigest, TargetCount: identity.TargetCount,
			TargetsDigest: identity.TargetsDigest,
			Target: catalog.SourceDriverTarget{
				Tenant: identity.MutationTenant, Generation: identity.MutationGeneration,
			},
			FromToken: identity.FromToken, Predecessor: identity.Predecessor,
			Operation: identity.Operation, SourceOperation: identity.SourceOperation, ChangeID: identity.ChangeID,
		},
		TargetEpoch: 1, DeclaredTargetCount: identity.TargetCount,
		TargetCursor: targets[len(targets)-1].Tenant, TargetsPrepared: true,
		RequestDigest: identity.MutationRequestDigest, RequestBound: true, Receipt: &proof,
	}
	store.reservedTargets = append([]catalog.SourceDriverTarget(nil), targets...)
	store.prepared[identity.Mutation] = catalog.PreparedMutation{
		OperationID: identity.Mutation, Tenant: identity.MutationTenant, Kind: catalog.MutationRevise,
		State: catalog.MutationApplying,
		Intent: catalog.MutationIntent{
			SourceID: string(identity.Authority), Origin: catalog.CausalOrigin{Cause: identity.Cause},
			Revise: &catalog.ReviseMutation{Object: catalog.ObjectID{3}, Spec: catalog.RevisionSpec{
				Parent: catalog.ObjectID{4}, Name: "item", Mode: 0o755,
			}},
		},
		Source: &catalog.SourceMutationContext{
			Operation: catalog.SourceMutationOperation{
				Kind: catalog.MutationRevise, Name: "item", ObjectKind: catalog.KindDirectory,
			},
			Object: &catalog.SourceLocator{
				SourceAuthority: identity.Authority, SourceKey: "item", SourceRevision: identity.Predecessor,
			},
			Parent: &catalog.SourceLocator{
				SourceAuthority: identity.Authority, SourceKey: "root", SourceRevision: identity.Predecessor,
			},
		},
		Claim: &identity.Claim,
	}
	store.pending = &catalog.SourceDriverStageState{
		Identity:    identity,
		TargetEpoch: 1,
		Stage: catalog.SourcePublicationStageRef{
			Authority: runtimeTestAuthority, Operation: identity.Operation,
			Sequence: 1, Items: 1, Bytes: 1, Revision: 2, Digest: [32]byte{2},
		},
	}
	runtime, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	store.mu.Lock()
	pending, commits, committed, checkpoint := store.pending, store.mutationCommits, store.committed, store.checkpoint
	store.mu.Unlock()
	driver.mu.Lock()
	acknowledged, sourceReceipt := driver.acknowledged, driver.receipt
	driver.mu.Unlock()
	if pending != nil || commits != 1 || committed == nil || !committed.Acknowledged || !committed.Forgotten ||
		checkpoint == nil || checkpoint.AuthorityGeneration != 1 || !acknowledged || sourceReceipt != nil {
		t.Fatalf("prior mutation recovery = pending %+v commits %d committed %+v checkpoint %+v ack %v receipt %+v",
			pending, commits, committed, checkpoint, acknowledged, sourceReceipt)
	}
}

func TestDeltaCompactionFallsBackToFencedSnapshot(t *testing.T) {
	targets := runtimeTargets(3)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if _, err := runtime.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	driver.head = "head-2"
	driver.snapshotRequired = true
	driver.mu.Unlock()

	result, err := runtime.Reconcile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Changed || result.Checkpoint.Token != "head-2" {
		t.Fatalf("snapshot fallback = %+v", result)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.aborts != 1 || store.required != catalog.SourceDriverSnapshotExpiredFloor {
		t.Fatalf("fallback proof = aborts %d required %d", store.aborts, store.required)
	}
}

func TestMutationLostResponseInspectsExactRequestThenCommitsTerminally(t *testing.T) {
	targets := runtimeTargets(2)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if _, err := runtime.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	driver.loseApply = true
	driver.mu.Unlock()
	targetCheckpoint, err := store.SourceDriverTargetCheckpoint(
		t.Context(), runtimeTestAuthority, targets[0].Tenant, targets[0].Generation,
	)
	if err != nil {
		t.Fatal(err)
	}
	object := catalog.SourceLocator{SourceAuthority: runtimeTestAuthority, SourceKey: "item", SourceRevision: 1}
	root, err := catalog.DeriveSourceDriverRootKey(runtimeTestAuthority, targets[0].Tenant)
	if err != nil {
		t.Fatal(err)
	}
	parent := catalog.SourceLocator{SourceAuthority: runtimeTestAuthority, SourceKey: root, SourceRevision: 1}
	prepared := catalog.PreparedMutation{
		OperationID: catalog.MutationID{2}, Tenant: targets[0].Tenant, Kind: catalog.MutationRevise,
		State: catalog.MutationApplying, ExpectedHead: targetCheckpoint.CatalogRevision,
		Intent: catalog.MutationIntent{
			SourceID: string(runtimeTestAuthority), Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
			Revise: &catalog.ReviseMutation{Object: catalog.ObjectID{3}, Spec: catalog.RevisionSpec{
				Parent: catalog.ObjectID{4}, Name: "item", Mode: 0o755,
			}},
		},
		Source: &catalog.SourceMutationContext{
			Operation: catalog.SourceMutationOperation{
				Kind: catalog.MutationRevise, Name: "item", ObjectKind: catalog.KindDirectory,
			},
			Object: &object, Parent: &parent,
		},
		Claim: &catalog.MutationClaim{Owner: catalog.MutationOwnerID{1}, Epoch: 1},
	}
	result, err := runtime.ApplyPreparedMutation(t.Context(), prepared, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Receipt.State != sourcedriver.MutationApplied || result.Receipt.Committed != "head-2" ||
		result.Stage.MutationResult == nil {
		t.Fatalf("mutation result = %+v", result)
	}
	driver.mu.Lock()
	applyCalls, inspectCalls := driver.applyCalls, driver.inspectCalls
	driver.mu.Unlock()
	store.mu.Lock()
	mutationCommits := store.mutationCommits
	store.mu.Unlock()
	if applyCalls != 1 || inspectCalls != 2 || mutationCommits != 1 {
		t.Fatalf("mutation calls = apply %d inspect %d commits %d", applyCalls, inspectCalls, mutationCommits)
	}
}

func TestCommittedMutationSettlementFailureIsRecoveredBeforeNextReconcile(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if _, err := runtime.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	targetCheckpoint, err := store.SourceDriverTargetCheckpoint(
		t.Context(), runtimeTestAuthority, targets[0].Tenant, targets[0].Generation,
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err := catalog.DeriveSourceDriverRootKey(runtimeTestAuthority, targets[0].Tenant)
	if err != nil {
		t.Fatal(err)
	}
	prepared := catalog.PreparedMutation{
		OperationID: catalog.MutationID{9}, Tenant: targets[0].Tenant, Kind: catalog.MutationRevise,
		State: catalog.MutationApplying, ExpectedHead: targetCheckpoint.CatalogRevision,
		Intent: catalog.MutationIntent{
			SourceID: string(runtimeTestAuthority), Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
			Revise: &catalog.ReviseMutation{Object: catalog.ObjectID{3}, Spec: catalog.RevisionSpec{
				Parent: catalog.ObjectID{4}, Name: "item", Mode: 0o755,
			}},
		},
		Source: &catalog.SourceMutationContext{
			Operation: catalog.SourceMutationOperation{
				Kind: catalog.MutationRevise, Name: "item", ObjectKind: catalog.KindDirectory,
			},
			Object: &catalog.SourceLocator{
				SourceAuthority: runtimeTestAuthority, SourceKey: "item", SourceRevision: 1,
			},
			Parent: &catalog.SourceLocator{
				SourceAuthority: runtimeTestAuthority, SourceKey: root, SourceRevision: 1,
			},
		},
		Claim: &catalog.MutationClaim{Owner: catalog.MutationOwnerID{1}, Epoch: 1},
	}
	driver.mu.Lock()
	driver.settleFailures = 1
	driver.mu.Unlock()
	if _, err := runtime.ApplyPreparedMutation(t.Context(), prepared, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ApplyPreparedMutation settlement loss = %v", err)
	}
	store.mu.Lock()
	if store.committed == nil || store.committed.Acknowledged || store.committed.Forgotten || store.mutationCommits != 1 {
		t.Fatalf("durable receipt after settlement loss = %+v commits=%d", store.committed, store.mutationCommits)
	}
	store.mu.Unlock()
	if _, err := runtime.Reconcile(t.Context()); err != nil {
		t.Fatalf("Reconcile recovery: %v", err)
	}
	replayed, err := runtime.SettleCommittedMutation(t.Context(), prepared.OperationID)
	if err != nil || replayed.Stage.MutationResult == nil || replayed.Receipt.OperationID != prepared.OperationID {
		t.Fatalf("SettleCommittedMutation replay = %+v, %v", replayed, err)
	}
	store.mu.Lock()
	committed := *store.committed
	store.mu.Unlock()
	driver.mu.Lock()
	applyCalls := driver.applyCalls
	driver.mu.Unlock()
	if !committed.Acknowledged || !committed.Forgotten || applyCalls != 1 {
		t.Fatalf("settlement recovery = %+v apply calls=%d", committed, applyCalls)
	}
}

func TestMutationReservationPrecedesDriverIOAndReceiptPrecedesStage(t *testing.T) {
	targets := runtimeTargets(2)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if _, err := runtime.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{21})
	store.mu.Lock()
	store.events = nil
	store.loseRecord = true
	store.mu.Unlock()
	driver.mu.Lock()
	driver.event = store.recordEvent
	driver.loseApply = true
	driver.mu.Unlock()
	result, err := runtime.ApplyPreparedMutation(t.Context(), prepared, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Receipt.State != sourcedriver.MutationApplied {
		t.Fatalf("mutation receipt = %+v", result.Receipt)
	}
	store.mu.Lock()
	events := append([]string(nil), store.events...)
	store.mu.Unlock()
	firstDriver := firstRuntimeEvent(events, "driver-")
	if firstDriver < 0 || eventIndex(events, "reserve") >= firstDriver ||
		eventIndex(events, "prepare-targets") >= firstDriver ||
		eventIndex(events, "bind-request") >= firstDriver {
		t.Fatalf("pre-I/O event order = %v", events)
	}
	if eventIndex(events, "record-receipt") >= eventIndex(events, "begin-stage") {
		t.Fatalf("receipt/stage event order = %v", events)
	}
	driver.mu.Lock()
	applyCalls, inspectCalls := driver.applyCalls, driver.inspectCalls
	driver.mu.Unlock()
	if applyCalls != 1 || inspectCalls != 2 {
		t.Fatalf("lost-response recovery calls = apply %d inspect %d events=%v", applyCalls, inspectCalls, events)
	}
}

func TestMutationRecoveryDistinguishesAppliedFromNotApplied(t *testing.T) {
	t.Run("applied", func(t *testing.T) {
		targets := runtimeTargets(1)
		store := newRuntimeTestStore(targets)
		driver := newRuntimeTestDriver(targets[0])
		runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = runtime.Close() })
		if _, err := runtime.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{22})
		store.mu.Lock()
		store.recordFailures = 1
		store.mu.Unlock()
		if _, err := runtime.ApplyPreparedMutation(t.Context(), prepared, nil); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first apply = %v, want receipt persistence loss", err)
		}
		result, err := runtime.ApplyPreparedMutation(t.Context(), prepared, nil)
		if err != nil || result.Receipt.State != sourcedriver.MutationApplied {
			t.Fatalf("applied recovery = %+v, %v", result, err)
		}
		driver.mu.Lock()
		applyCalls, inspectCalls := driver.applyCalls, driver.inspectCalls
		driver.mu.Unlock()
		if applyCalls != 1 || inspectCalls != 2 {
			t.Fatalf("applied recovery calls = apply %d inspect %d", applyCalls, inspectCalls)
		}
	})

	t.Run("not-applied", func(t *testing.T) {
		targets := runtimeTargets(1)
		store := newRuntimeTestStore(targets)
		driver := newRuntimeTestDriver(targets[0])
		runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = runtime.Close() })
		if _, err := runtime.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{23})
		driver.mu.Lock()
		driver.applyFailures = 1
		driver.mu.Unlock()
		if _, err := runtime.ApplyPreparedMutation(t.Context(), prepared, nil); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("first apply = %v, want unapplied failure", err)
		}
		result, err := runtime.ApplyPreparedMutation(t.Context(), prepared, nil)
		if err != nil || result.Receipt.State != sourcedriver.MutationApplied {
			t.Fatalf("not-applied recovery = %+v, %v", result, err)
		}
		driver.mu.Lock()
		applyCalls := driver.applyCalls
		driver.mu.Unlock()
		if applyCalls != 2 {
			t.Fatalf("not-applied recovery calls = apply %d, want retry", applyCalls)
		}
	})
}

func TestMutationReservationFailureDispatchesNoDriverIO(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if _, err := runtime.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{24})
	store.mu.Lock()
	store.events = nil
	store.reserveFailure = catalog.ErrGenerationMismatch
	store.mu.Unlock()
	driver.mu.Lock()
	driver.event = store.recordEvent
	driver.mu.Unlock()
	if _, err := runtime.ApplyPreparedMutation(t.Context(), prepared, nil); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("reservation fence = %v", err)
	}
	store.mu.Lock()
	events := append([]string(nil), store.events...)
	store.mu.Unlock()
	if firstRuntimeEvent(events, "driver-") != -1 {
		t.Fatalf("reservation failure dispatched driver I/O: %v", events)
	}
}

func TestAppliedMutationAfterTargetAdvancePublishesCurrentResetAndSettlesOldRef(t *testing.T) {
	oldTargets := runtimeTargets(1)
	store := newRuntimeTestStore(oldTargets)
	driver := newRuntimeTestDriver(oldTargets[0])
	first, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, oldTargets))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePreparedDirectoryRevise(t, store, oldTargets[0], catalog.MutationID{25})
	store.mu.Lock()
	store.recordFailures = 1
	store.mu.Unlock()
	if _, err := first.ApplyPreparedMutation(t.Context(), prepared, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pre-crash apply = %v", err)
	}
	driver.mu.Lock()
	oldRef := driver.mutationTargetSet
	driver.mu.Unlock()
	if oldRef == (sourcedriver.TargetSetRef{}) {
		t.Fatal("external mutation did not bind its old target set")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	currentTargets := runtimeTargets(2)
	currentDigest, err := catalog.SourceDriverTargetsDigest(currentTargets)
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.targets = append([]catalog.SourceDriverTarget(nil), currentTargets...)
	store.targetEpoch = 2
	store.checkpoint.SnapshotRequired = catalog.SourceDriverSnapshotReset
	store.targetCheckpoints[currentTargets[1].Tenant] = catalog.SourceDriverTargetCheckpoint{
		SourceDriverTarget: currentTargets[1], SourceRevision: store.checkpoint.SourceRevision, CatalogRevision: 1,
	}
	store.mu.Unlock()
	driver.mu.Lock()
	driver.objects = []sourcedriver.Projection{
		runtimeProjection(currentTargets[0], "item-0"),
		runtimeProjection(currentTargets[1], "item-1"),
	}
	driver.mu.Unlock()
	config := runtimeTestConfig(store, driver, currentTargets)
	config.targetsDigest = currentDigest
	second, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	result, err := second.ApplyPreparedMutation(t.Context(), prepared, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Stage.Identity.Mode != catalog.SourceDriverMutation ||
		result.Stage.Identity.SnapshotReason != catalog.SourceDriverSnapshotReset ||
		result.Stage.Identity.TargetCount != uint64(len(currentTargets)) ||
		result.Stage.Identity.TargetsDigest != currentDigest || result.Stage.Checkpoint.TargetEpoch != 2 {
		t.Fatalf("split-identity mutation stage = %+v", result.Stage)
	}
	driver.mu.Lock()
	applyCalls := driver.applyCalls
	snapshotSet, settlementSet := driver.lastSnapshotSet, driver.lastSettlementSet
	driver.mu.Unlock()
	if applyCalls != 1 || snapshotSet == oldRef || snapshotSet.TargetEpoch != 2 ||
		snapshotSet.TargetsDigest != currentDigest || settlementSet != oldRef {
		t.Fatalf("split identities = apply %d old=%+v snapshot=%+v settlement=%+v",
			applyCalls, oldRef, snapshotSet, settlementSet)
	}
	store.mu.Lock()
	reservation := store.reservation
	store.mu.Unlock()
	if reservation != nil {
		t.Fatalf("forgotten old reservation remains: %+v", reservation)
	}
}

func TestPendingMutationTargetAdvanceAbortsAndRebuildsAcrossRestart(t *testing.T) {
	for _, test := range []struct {
		name         string
		failChanges  bool
		failPrepare  bool
		wantSequence uint64
	}{
		{name: "after_begin", failChanges: true, wantSequence: 0},
		{name: "after_append", failPrepare: true, wantSequence: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			oldTargets := runtimeTargets(1)
			store := newRuntimeTestStore(oldTargets)
			driver := newRuntimeTestDriver(oldTargets[0])
			first, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, oldTargets))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			prepared := runtimePreparedDirectoryRevise(t, store, oldTargets[0], catalog.MutationID{31})
			driver.mu.Lock()
			driver.changeFailures = btoi(test.failChanges)
			driver.mu.Unlock()
			store.mu.Lock()
			store.prepareFailures = btoi(test.failPrepare)
			store.mu.Unlock()
			if _, err := first.ApplyPreparedMutation(t.Context(), prepared, nil); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("seed pending mutation = %v", err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			if store.pending == nil || store.pending.Stage.Sequence != test.wantSequence ||
				store.reservation == nil || store.reservation.Receipt == nil {
				t.Fatalf("seeded state pending=%+v reservation=%+v", store.pending, store.reservation)
			}
			oldOperation := store.reservation.Operation
			oldSourceOperation := store.reservation.SourceOperation
			oldChange := store.reservation.ChangeID
			store.targets = runtimeTargets(2)
			store.targetEpoch++
			store.loseAbort = true
			store.mu.Unlock()
			driver.mu.Lock()
			oldRef := driver.mutationTargetSet
			driver.objects = []sourcedriver.Projection{
				runtimeProjection(runtimeTargets(2)[0], "item-0"),
				runtimeProjection(runtimeTargets(2)[1], "item-1"),
			}
			driver.mu.Unlock()

			newTargets := runtimeTargets(2)
			if _, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, newTargets)); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ErrRetainedMutationLiability) {
				t.Fatalf("lost abort recovery = %v", err)
			}
			store.mu.Lock()
			if store.pending != nil || store.reservation == nil || store.reservation.Receipt == nil ||
				store.reservation.Operation != oldOperation || store.reservation.SourceOperation != oldSourceOperation ||
				store.reservation.ChangeID != oldChange || store.aborts != 1 {
				t.Fatalf("lost abort evidence pending=%+v reservation=%+v aborts=%d",
					store.pending, store.reservation, store.aborts)
			}
			store.mu.Unlock()

			recovered, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, newTargets))
			if err != nil {
				t.Fatal(err)
			}
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
			driver.mu.Lock()
			applyCalls := driver.applyCalls
			snapshotRef, settlementRef := driver.lastSnapshotSet, driver.lastSettlementSet
			driver.mu.Unlock()
			store.mu.Lock()
			checkpoint, reservation, pending := *store.checkpoint, store.reservation, store.pending
			committed := store.committed
			store.mu.Unlock()
			if applyCalls != 1 || snapshotRef.TargetEpoch != 2 || snapshotRef == oldRef || settlementRef != oldRef ||
				checkpoint.TargetEpoch != 2 || checkpoint.TargetCount != 2 || reservation != nil || pending != nil ||
				committed == nil || committed.Result.Identity.Operation != oldOperation ||
				committed.Result.Identity.SourceOperation != oldSourceOperation ||
				committed.Result.Identity.ChangeID != oldChange || !committed.Forgotten {
				t.Fatalf("recovered bridge apply=%d snapshot=%+v old=%+v settlement=%+v checkpoint=%+v reservation=%+v pending=%+v committed=%+v",
					applyCalls, snapshotRef, oldRef, settlementRef, checkpoint, reservation, pending, committed)
			}
		})
	}
}

func TestPendingDeltaTargetAdvanceAbortsAndRebuildsAcrossRestart(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		name := "after_begin"
		if terminal {
			name = "after_append_prepare"
		}
		t.Run(name, func(t *testing.T) {
			oldTargets := runtimeTargets(1)
			store := newRuntimeTestStore(oldTargets)
			driver := newRuntimeTestDriver(oldTargets[0])
			config := normalizeConfig(runtimeTestConfig(store, driver, oldTargets))
			first, err := NewRuntime(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			predecessor := store.checkpoint.SourceRevision
			store.mu.Unlock()
			identity := runtimeTestIdentity(config, catalog.SourceDriverDelta, "head-1", "head-2", predecessor)
			identity.SnapshotReason = 0
			if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
				t.Fatal(err)
			}
			if terminal {
				state, err := store.AppendSourceDriverStage(t.Context(), identity, catalog.SourceDriverStagePage{
					Digest: [sha256.Size]byte{9}, Complete: true,
				})
				if err != nil || len(state.Cursor) != 0 {
					t.Fatalf("seed terminal = %+v, %v", state, err)
				}
				if _, err := store.PrepareSourceDriverPublicationBatch(t.Context(), identity); err != nil {
					t.Fatal(err)
				}
			}
			newTargets := runtimeTargets(2)
			store.mu.Lock()
			store.targets = append([]catalog.SourceDriverTarget(nil), newTargets...)
			store.targetEpoch++
			store.loseAbort = true
			store.mu.Unlock()
			driver.mu.Lock()
			driver.head = "head-2"
			driver.objects = []sourcedriver.Projection{
				runtimeProjection(newTargets[0], "item-0"), runtimeProjection(newTargets[1], "item-1"),
			}
			driver.mu.Unlock()
			if _, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, newTargets)); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost abort = %v", err)
			}
			recovered, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, newTargets))
			if err != nil {
				t.Fatal(err)
			}
			result, err := recovered.Reconcile(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			aborts, pending := store.aborts, store.pending
			store.mu.Unlock()
			driver.mu.Lock()
			snapshotRef, changeCalls := driver.lastSnapshotSet, driver.changeCalls
			driver.mu.Unlock()
			if aborts != 1 || pending != nil || result.Checkpoint.TargetEpoch != 2 ||
				result.Checkpoint.TargetCount != 2 || snapshotRef.TargetEpoch != 2 || changeCalls != 0 {
				t.Fatalf("delta rebuild aborts=%d pending=%+v result=%+v snapshot=%+v changes=%d",
					aborts, pending, result, snapshotRef, changeCalls)
			}
		})
	}
}

func TestConstructionReleasesUnboundMutationReservation(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	config := runtimeTestConfig(store, driver, targets)
	first, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{41})
	if _, err := first.reservePreparedMutation(t.Context(), prepared); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	reservation := store.reservation
	store.mu.Unlock()
	if reservation != nil {
		t.Fatalf("unbound reservation survived restart: %+v", reservation)
	}
}

func TestConstructionRestrictsBoundUnappliedReservationToExactRetry(t *testing.T) {
	for _, state := range []sourcedriver.MutationState{
		sourcedriver.MutationNotFound,
		sourcedriver.MutationPrepared,
	} {
		t.Run(fmt.Sprintf("state_%d", state), func(t *testing.T) {
			targets := runtimeTargets(1)
			store := newRuntimeTestStore(targets)
			driver := newRuntimeTestDriver(targets[0])
			config := runtimeTestConfig(store, driver, targets)
			first, err := NewRuntime(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{42})
			reservation, request := runtimeBindMutationReservation(t, first, prepared)
			if state == sourcedriver.MutationPrepared {
				receipt := sourcedriver.MutationReceipt{
					OperationID: reservation.Mutation, State: state,
					RequestDigest: reservation.RequestDigest, Expected: request.Expected,
				}
				receipt.Digest, err = sourcedriver.MutationReceiptDigest(receipt)
				if err != nil {
					t.Fatal(err)
				}
				driver.mu.Lock()
				driver.receipt = &receipt
				driver.mu.Unlock()
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			recovered, err := NewRuntime(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := recovered.Reconcile(t.Context()); !errors.Is(err, ErrRetainedMutationLiability) {
				t.Fatalf("ordinary reconcile during liability = %v", err)
			}
			other := prepared
			other.OperationID = catalog.MutationID{43}
			if _, err := recovered.ApplyPreparedMutation(t.Context(), other, nil); !errors.Is(err, ErrRetainedMutationLiability) {
				t.Fatalf("different mutation admitted = %v", err)
			}
			if _, err := recovered.ApplyPreparedMutation(t.Context(), prepared, nil); err != nil {
				t.Fatal(err)
			}
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
			driver.mu.Lock()
			applyCalls := driver.applyCalls
			driver.mu.Unlock()
			store.mu.Lock()
			left := store.reservation
			store.mu.Unlock()
			if applyCalls != 1 || left != nil {
				t.Fatalf("exact retry apply=%d reservation=%+v", applyCalls, left)
			}
		})
	}
}

func TestCancelledExactRetryRetainsBoundReservationAcrossRestart(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	config := runtimeTestConfig(store, driver, targets)
	first, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{47})
	runtimeBindMutationReservation(t, first, prepared)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	restricted, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := restricted.ApplyPreparedMutation(cancelled, prepared, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled retry = %v", err)
	}
	if err := restricted.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	reservation := store.reservation
	store.mu.Unlock()
	if reservation == nil || !reservation.RequestBound || reservation.Receipt != nil {
		t.Fatalf("cancel released bound evidence: %+v", reservation)
	}
	restarted, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.ApplyPreparedMutation(t.Context(), prepared, nil); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReservationPreflightFailureDoesNotOpenContent(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	runtime, err := NewRuntime(t.Context(), runtimeTestConfig(store, driver, targets))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if _, err := runtime.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{48})
	body := []byte("replacement")
	ref := catalog.ContentRef{Stage: catalog.StageID{1}, Hash: sha256.Sum256(body), Size: int64(len(body))}
	prepared.Intent.Revise.Spec.Content = &catalog.ContentUpdate{Revision: 2, Ref: ref}
	prepared.Source.Operation.ObjectKind = catalog.KindFile
	prepared.Source.Operation.HasContent = true
	store.mu.Lock()
	store.prepared[prepared.OperationID] = prepared
	store.mu.Unlock()
	if _, err := runtime.reservePreparedMutation(t.Context(), prepared); err != nil {
		t.Fatal(err)
	}
	prepared.Source.Object.SourceRevision++
	source := &runtimeProbeSource{Reader: bytes.NewReader(body)}
	if _, err := runtime.ApplyPreparedMutation(t.Context(), prepared, source); !errors.Is(err, catalog.ErrSourceLocatorStale) {
		t.Fatalf("preflight failure = %v", err)
	}
	source.mu.Lock()
	reads, settlements := source.reads, source.settlements
	source.mu.Unlock()
	driver.mu.Lock()
	applyCalls := driver.applyCalls
	driver.mu.Unlock()
	store.mu.Lock()
	reservation := store.reservation
	store.mu.Unlock()
	if reads != 0 || settlements != 1 || applyCalls != 0 || reservation == nil || reservation.RequestBound {
		t.Fatalf("content preflight reads=%d settlements=%d apply=%d reservation=%+v",
			reads, settlements, applyCalls, reservation)
	}
}

func TestConstructionTransformsPendingDeltaSnapshotRequired(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	config := normalizeConfig(runtimeTestConfig(store, driver, targets))
	first, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	predecessor := store.checkpoint.SourceRevision
	store.mu.Unlock()
	identity := runtimeTestIdentity(config, catalog.SourceDriverDelta, "head-1", "head-2", predecessor)
	identity.SnapshotReason = 0
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	driver.snapshotRequired = true
	driver.mu.Unlock()
	recovered, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	aborts, required, checkpoint, pending := store.aborts, store.required, store.checkpoint, store.pending
	store.mu.Unlock()
	driver.mu.Lock()
	changeCalls, snapshotCalls := driver.changeCalls, driver.snapshotCalls
	driver.mu.Unlock()
	if aborts != 1 || required != catalog.SourceDriverSnapshotExpiredFloor || pending != nil ||
		checkpoint == nil || checkpoint.Token != "head-2" || checkpoint.SnapshotRequired != 0 ||
		changeCalls != 1 || snapshotCalls != 2 {
		t.Fatalf("snapshot-required recovery aborts=%d required=%d checkpoint=%+v pending=%+v changes=%d snapshots=%d",
			aborts, required, checkpoint, pending, changeCalls, snapshotCalls)
	}
}

func TestConstructionTransformsPendingMutationSnapshotRequired(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	config := runtimeTestConfig(store, driver, targets)
	first, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{44})
	driver.mu.Lock()
	driver.changeFailures = 1
	driver.mu.Unlock()
	if _, err := first.ApplyPreparedMutation(t.Context(), prepared, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("seed pending mutation = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	driver.snapshotRequired = true
	driver.mu.Unlock()
	recovered, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	aborts, required, reservation, pending := store.aborts, store.required, store.reservation, store.pending
	store.mu.Unlock()
	driver.mu.Lock()
	applyCalls, snapshotCalls, settlement := driver.applyCalls, driver.snapshotCalls, driver.lastSettlementSet
	driver.mu.Unlock()
	if aborts != 1 || required != catalog.SourceDriverSnapshotReset || reservation != nil || pending != nil ||
		applyCalls != 1 || snapshotCalls != 2 || settlement == (sourcedriver.TargetSetRef{}) {
		t.Fatalf("mutation snapshot-required aborts=%d required=%d reservation=%+v pending=%+v apply=%d snapshots=%d settlement=%+v",
			aborts, required, reservation, pending, applyCalls, snapshotCalls, settlement)
	}
}

func TestPendingMutationTopologyTOCTOURebuildsCurrentReset(t *testing.T) {
	for _, phase := range []string{"target_prepare", "publication_prepare"} {
		t.Run(phase, func(t *testing.T) {
			targets := runtimeTargets(1)
			store := newRuntimeTestStore(targets)
			driver := newRuntimeTestDriver(targets[0])
			config := runtimeTestConfig(store, driver, targets)
			first, err := NewRuntime(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Reconcile(t.Context()); err != nil {
				t.Fatal(err)
			}
			prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{45})
			driver.mu.Lock()
			driver.changeFailures = 1
			driver.mu.Unlock()
			if _, err := first.ApplyPreparedMutation(t.Context(), prepared, nil); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("seed pending = %v", err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			store.advanceEpochOnTargetPrepare = phase == "target_prepare"
			store.advanceEpochOnPublicationPrepare = phase == "publication_prepare"
			store.mu.Unlock()
			recovered, err := NewRuntime(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			aborts, checkpoint, pending, reservation := store.aborts, store.checkpoint, store.pending, store.reservation
			store.mu.Unlock()
			driver.mu.Lock()
			applyCalls, snapshotRef := driver.applyCalls, driver.lastSnapshotSet
			driver.mu.Unlock()
			if aborts != 1 || checkpoint == nil || checkpoint.TargetEpoch != 2 || pending != nil ||
				reservation != nil || applyCalls != 1 || snapshotRef.TargetEpoch != 2 {
				t.Fatalf("TOCTOU recovery aborts=%d checkpoint=%+v pending=%+v reservation=%+v apply=%d snapshot=%+v",
					aborts, checkpoint, pending, reservation, applyCalls, snapshotRef)
			}
		})
	}
}

func TestPendingMutationAuthorityDriftRetainsLiabilityBeforeDriverIO(t *testing.T) {
	targets := runtimeTargets(1)
	store := newRuntimeTestStore(targets)
	driver := newRuntimeTestDriver(targets[0])
	config := runtimeTestConfig(store, driver, targets)
	first, err := NewRuntime(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared := runtimePreparedDirectoryRevise(t, store, targets[0], catalog.MutationID{46})
	driver.mu.Lock()
	driver.changeFailures = 1
	driver.mu.Unlock()
	if _, err := first.ApplyPreparedMutation(t.Context(), prepared, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("seed pending = %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	driver.mu.Lock()
	before := [5]int{driver.refreshCalls, driver.snapshotCalls, driver.changeCalls, driver.applyCalls, driver.declareCalls}
	driver.mu.Unlock()
	config.AuthorityGeneration++
	config.DeclarationDigest = [sha256.Size]byte{9}
	if _, err := NewRuntime(t.Context(), config); !errors.Is(err, ErrRetainedMutationLiability) {
		t.Fatalf("authority drift = %v", err)
	}
	driver.mu.Lock()
	after := [5]int{driver.refreshCalls, driver.snapshotCalls, driver.changeCalls, driver.applyCalls, driver.declareCalls}
	driver.mu.Unlock()
	store.mu.Lock()
	pending, reservation, aborts := store.pending, store.reservation, store.aborts
	store.mu.Unlock()
	if before != after || pending == nil || reservation == nil || reservation.Receipt == nil || aborts != 0 {
		t.Fatalf("drift dispatched I/O before=%v after=%v pending=%+v reservation=%+v aborts=%d",
			before, after, pending, reservation, aborts)
	}
}

func TestPendingIntermediateCursorTargetAdvanceAbortsBeforeContinuation(t *testing.T) {
	oldTargets := runtimeTargets(1)
	store := newRuntimeTestStore(oldTargets)
	driver := newRuntimeTestDriver(oldTargets[0])
	driver.objects = append(driver.objects, runtimeProjection(oldTargets[0], "second"))
	config := runtimeTestConfig(store, driver, oldTargets)
	config.PageLimit = 1
	config = normalizeConfig(config)
	identity := runtimeTestIdentity(config, catalog.SourceDriverSnapshot, "", "head-1", 0)
	request := runtimeSnapshotRequest(config, "head-1", nil)
	first, err := driver.Snapshot(t.Context(), runtimeTestAuthority, request)
	if err != nil || first.Next == nil {
		t.Fatalf("seed first page = %+v, %v", first, err)
	}
	cursor, err := encodeCursor(first.Next)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	state, err := store.AppendSourceDriverStage(t.Context(), identity, catalog.SourceDriverStagePage{
		Cursor: cursor, Digest: first.Digest,
	})
	if err != nil || state.Stage.Sequence != 1 || len(state.Cursor) == 0 {
		t.Fatalf("seed intermediate = %+v, %v", state, err)
	}
	newTargets := runtimeTargets(2)
	store.mu.Lock()
	store.targets = append([]catalog.SourceDriverTarget(nil), newTargets...)
	store.targetEpoch++
	store.mu.Unlock()
	driver.mu.Lock()
	driver.snapshotCalls = 0
	driver.requestedAfter = nil
	driver.objects = []sourcedriver.Projection{
		runtimeProjection(newTargets[0], "item-0"), runtimeProjection(newTargets[1], "item-1"),
	}
	driver.mu.Unlock()
	newConfig := runtimeTestConfig(store, driver, newTargets)
	newConfig.PageLimit = 1
	recovered, err := NewRuntime(t.Context(), newConfig)
	if err != nil {
		t.Fatal(err)
	}
	result, err := recovered.Reconcile(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	aborts, pending := store.aborts, store.pending
	store.mu.Unlock()
	driver.mu.Lock()
	snapshotRef := driver.lastSnapshotSet
	driver.mu.Unlock()
	if aborts != 1 || pending != nil || result.Checkpoint.TargetEpoch != 2 ||
		result.Checkpoint.TargetCount != 2 || snapshotRef.TargetEpoch != 2 {
		t.Fatalf("intermediate recovery aborts=%d pending=%+v result=%+v snapshot=%+v",
			aborts, pending, result, snapshotRef)
	}
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}

func assertStoreTargetWatermarks(
	t *testing.T,
	store *runtimeTestStore,
	targets []catalog.SourceDriverTarget,
	revision causal.Revision,
) {
	t.Helper()
	for _, target := range targets {
		watermark, err := store.SourceDriverTargetCheckpoint(
			t.Context(), runtimeTestAuthority, target.Tenant, target.Generation,
		)
		if err != nil || watermark.SourceRevision != revision {
			t.Fatalf("target watermark = %+v, %v, want source revision %d", watermark, err, revision)
		}
	}
}

func runtimePreparedDirectoryRevise(
	t *testing.T,
	store *runtimeTestStore,
	target catalog.SourceDriverTarget,
	operation catalog.MutationID,
) catalog.PreparedMutation {
	t.Helper()
	checkpoint, err := store.SourceDriverTargetCheckpoint(
		t.Context(), runtimeTestAuthority, target.Tenant, target.Generation,
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err := catalog.DeriveSourceDriverRootKey(runtimeTestAuthority, target.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	prepared := catalog.PreparedMutation{
		OperationID: operation, Tenant: target.Tenant, Kind: catalog.MutationRevise,
		State: catalog.MutationApplying, ExpectedHead: checkpoint.CatalogRevision,
		Intent: catalog.MutationIntent{
			SourceID: string(runtimeTestAuthority), Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite},
			Revise: &catalog.ReviseMutation{Object: catalog.ObjectID{3}, Spec: catalog.RevisionSpec{
				Parent: catalog.ObjectID{4}, Name: "item", Mode: 0o755,
			}},
		},
		Source: &catalog.SourceMutationContext{
			Operation: catalog.SourceMutationOperation{
				Kind: catalog.MutationRevise, Name: "item", ObjectKind: catalog.KindDirectory,
			},
			Object: &catalog.SourceLocator{
				SourceAuthority: runtimeTestAuthority, SourceKey: "item", SourceRevision: checkpoint.SourceRevision,
			},
			Parent: &catalog.SourceLocator{
				SourceAuthority: runtimeTestAuthority, SourceKey: root, SourceRevision: checkpoint.SourceRevision,
			},
		},
		Claim: &catalog.MutationClaim{Owner: catalog.MutationOwnerID{1}, Epoch: 1},
	}
	store.mu.Lock()
	store.prepared[operation] = prepared
	store.mu.Unlock()
	return prepared
}

func runtimeBindMutationReservation(
	t *testing.T,
	runtime *Runtime,
	prepared catalog.PreparedMutation,
) (catalog.SourceDriverMutationReservation, sourcedriver.MutationRequest) {
	t.Helper()
	reservation, err := runtime.reservePreparedMutation(t.Context(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	for !reservation.TargetsPrepared {
		reservation, err = runtime.config.Store.PrepareSourceDriverMutationReservationBatch(
			t.Context(), reservation.Mutation, reservation.Claim,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	request, hasContent, err := mutationRequestFor(prepared)
	if err != nil {
		t.Fatal(err)
	}
	request, err = mutationRequestFromReservation(prepared, request, hasContent, reservation)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err = runtime.bindMutationRequest(t.Context(), reservation, digest)
	if err != nil {
		t.Fatal(err)
	}
	return reservation, request
}

func runtimeProjection(target catalog.SourceDriverTarget, id sourcedriver.LogicalID) sourcedriver.Projection {
	return sourcedriver.Projection{
		Tenant: target.Tenant, Generation: causal.Generation(target.Generation),
		ID: id, Name: string(id), Kind: catalog.KindDirectory,
		Mode: 0o755, Visibility: catalog.Visibility{Mount: true},
	}
}

func eventIndex(events []string, want string) int {
	for index, event := range events {
		if event == want {
			return index
		}
	}
	return -1
}

func firstRuntimeEvent(events []string, prefix string) int {
	for index, event := range events {
		if len(event) >= len(prefix) && event[:len(prefix)] == prefix {
			return index
		}
	}
	return -1
}

func runtimeTargets(count int) []catalog.SourceDriverTarget {
	targets := make([]catalog.SourceDriverTarget, count)
	for index := range targets {
		targets[index] = catalog.SourceDriverTarget{
			Tenant: catalog.TenantID(fmt.Sprintf("tenant-%03d", index)), Generation: 1,
		}
	}
	return targets
}

func runtimeTestConfig(
	store *runtimeTestStore,
	driver *runtimeTestDriver,
	targets []catalog.SourceDriverTarget,
) Config {
	return Config{
		Store: store, Driver: driver, Authority: runtimeTestAuthority,
		FleetOwner: "fleet", AuthorityGeneration: 1, DeclarationDigest: [32]byte{1},
		Targets: targets, PageLimit: 128,
	}
}

func runtimeTestIdentity(
	config Config,
	mode catalog.SourceDriverMode,
	from, to string,
	predecessor causal.Revision,
) catalog.SourceDriverStageIdentity {
	return catalog.SourceDriverStageIdentity{
		Authority: config.Authority, FleetOwner: config.FleetOwner,
		AuthorityGeneration: config.AuthorityGeneration, DeclarationDigest: config.DeclarationDigest,
		TargetCount: uint64(len(config.Targets)), TargetsDigest: config.targetsDigest,
		Operation: causal.OperationID{1}, SourceOperation: causal.OperationID{2},
		ChangeID: causal.ChangeID{3}, Cause: causal.CauseExternalUnattributed,
		Mode: mode, SnapshotReason: catalog.SourceDriverSnapshotInitial,
		FromToken: from, ToToken: to, Predecessor: predecessor,
	}
}

func runtimeSnapshotRequest(config Config, revision sourcedriver.RevisionToken, cursor *sourcedriver.PageCursor) sourcedriver.SnapshotRequest {
	targetSet, _ := config.targetSetRef(1)
	return sourcedriver.SnapshotRequest{
		TargetSet: targetSet, Revision: revision, Cursor: cursor, Limit: config.PageLimit,
	}
}

type runtimeTestStore struct {
	mu sync.Mutex

	targets                          []catalog.SourceDriverTarget
	checkpoint                       *catalog.SourceDriverCheckpoint
	targetCheckpoints                map[catalog.TenantID]catalog.SourceDriverTargetCheckpoint
	pending                          *catalog.SourceDriverStageState
	aborts                           int
	releases                         int
	required                         catalog.SourceDriverSnapshotReason
	loseAppend                       bool
	loseAbort                        bool
	prepareFailures                  int
	commitFailures                   int
	advanceEpochOnTargetPrepare      bool
	advanceEpochOnPublicationPrepare bool
	mutationCommits                  int
	committed                        *catalog.SourceDriverCommittedReceipt
	targetEpochChecks                int
	targetEpoch                      uint64
	reservation                      *catalog.SourceDriverMutationReservation
	reservedTargets                  []catalog.SourceDriverTarget
	prepared                         map[catalog.MutationID]catalog.PreparedMutation
	events                           []string
	recordFailures                   int
	loseRecord                       bool
	reserveFailure                   error
}

func newRuntimeTestStore(targets []catalog.SourceDriverTarget) *runtimeTestStore {
	return &runtimeTestStore{
		targets:           append([]catalog.SourceDriverTarget(nil), targets...),
		targetCheckpoints: make(map[catalog.TenantID]catalog.SourceDriverTargetCheckpoint),
		prepared:          make(map[catalog.MutationID]catalog.PreparedMutation),
		targetEpoch:       1,
	}
}

func (s *runtimeTestStore) PreparedMutation(
	_ context.Context,
	tenant catalog.TenantID,
	id catalog.MutationID,
) (catalog.PreparedMutation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prepared, found := s.prepared[id]
	if !found || prepared.Tenant != tenant {
		return catalog.PreparedMutation{}, catalog.ErrNotFound
	}
	return prepared, nil
}

func (s *runtimeTestStore) SourceDriverCheckpoint(
	context.Context, causal.SourceAuthorityID,
) (catalog.SourceDriverCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkpoint == nil {
		return catalog.SourceDriverCheckpoint{}, catalog.ErrNotFound
	}
	return *s.checkpoint, nil
}

func (s *runtimeTestStore) SourceDriverTargetCheckpoint(
	_ context.Context,
	_ causal.SourceAuthorityID,
	tenant catalog.TenantID,
	generation catalog.Generation,
) (catalog.SourceDriverTargetCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, found := s.targetCheckpoints[tenant]
	if !found {
		return catalog.SourceDriverTargetCheckpoint{}, catalog.ErrNotFound
	}
	if value.Generation != generation {
		return catalog.SourceDriverTargetCheckpoint{}, catalog.ErrGenerationMismatch
	}
	return value, nil
}

func (s *runtimeTestStore) PendingSourceDriverStage(
	context.Context, causal.SourceAuthorityID,
) (*catalog.SourceDriverStageState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil {
		return nil, nil
	}
	value := *s.pending
	value.Cursor = append([]byte(nil), value.Cursor...)
	return &value, nil
}

func (s *runtimeTestStore) RequireSourceDriverSnapshot(
	_ context.Context,
	_ causal.SourceAuthorityID,
	_ string,
	reason catalog.SourceDriverSnapshotReason,
) (catalog.SourceDriverCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.required = reason
	if s.checkpoint == nil {
		return catalog.SourceDriverCheckpoint{}, catalog.ErrNotFound
	}
	s.checkpoint.SnapshotRequired = reason
	return *s.checkpoint, nil
}

func (s *runtimeTestStore) RebindSourceDriverCheckpoint(
	_ context.Context,
	request catalog.SourceDriverCheckpointRebind,
) (catalog.SourceDriverCheckpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkpoint == nil || *s.checkpoint != request.Expected || s.pending != nil {
		return catalog.SourceDriverCheckpoint{}, catalog.ErrMutationConflict
	}
	s.checkpoint.AuthorityGeneration = request.AuthorityGeneration
	s.checkpoint.DeclarationDigest = request.DeclarationDigest
	digest, err := catalog.SourceDriverTargetsDigest(s.targets)
	if err != nil {
		return catalog.SourceDriverCheckpoint{}, err
	}
	if s.checkpoint.TargetCount != uint64(len(s.targets)) || s.checkpoint.TargetsDigest != digest {
		s.targetEpoch++
	}
	s.checkpoint.TargetEpoch = s.targetEpoch
	s.checkpoint.TargetCount = uint64(len(s.targets))
	s.checkpoint.TargetsDigest = digest
	s.checkpoint.SnapshotRequired = catalog.SourceDriverSnapshotReset
	return *s.checkpoint, nil
}

func (s *runtimeTestStore) BeginSourceDriverStage(_ context.Context, identity catalog.SourceDriverStageIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "begin-stage")
	if s.pending != nil {
		if s.pending.Identity == identity {
			return nil
		}
		return catalog.ErrMutationConflict
	}
	s.pending = &catalog.SourceDriverStageState{
		Identity:    identity,
		TargetEpoch: s.targetEpoch,
		Stage: catalog.SourcePublicationStageRef{
			Authority: identity.Authority, Operation: identity.Operation, Revision: identity.Predecessor,
			Digest: [32]byte{1},
		},
	}
	return nil
}

func (s *runtimeTestStore) ValidateSourceDriverTargetEpoch(
	_ context.Context,
	_ causal.SourceAuthorityID,
	targetEpoch uint64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.targetEpochChecks++
	if targetEpoch != s.targetEpoch {
		return catalog.ErrGenerationMismatch
	}
	return nil
}

func (s *runtimeTestStore) SourceDriverTargetEpoch(
	_ context.Context,
	_ causal.SourceAuthorityID,
) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.targetEpoch, nil
}

func (s *runtimeTestStore) PrepareSourceDriverTargetDeclarationBatch(
	_ context.Context,
	identity catalog.SourceDriverStageIdentity,
) (catalog.SourceDriverTargetDeclarationState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.advanceEpochOnTargetPrepare {
		s.advanceEpochOnTargetPrepare = false
		s.targetEpoch++
	}
	return catalog.SourceDriverTargetDeclarationState{
		TargetEpoch: s.targetEpoch, DeclaredCount: uint64(len(s.targets)), TargetCount: uint64(len(s.targets)),
		Digest: identity.TargetsDigest, Prepared: true,
	}, nil
}

func (s *runtimeTestStore) ReserveSourceDriverMutation(
	_ context.Context,
	request catalog.SourceDriverMutationReservationRequest,
) (catalog.SourceDriverMutationReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "reserve")
	if s.reserveFailure != nil {
		return catalog.SourceDriverMutationReservation{}, s.reserveFailure
	}
	if s.reservation != nil {
		if s.reservation.SourceDriverMutationReservationRequest != request {
			return catalog.SourceDriverMutationReservation{}, catalog.ErrMutationConflict
		}
		return *s.reservation, nil
	}
	reservation := catalog.SourceDriverMutationReservation{
		SourceDriverMutationReservationRequest: request,
		TargetEpoch:                            s.targetEpoch,
	}
	s.reservation = &reservation
	s.reservedTargets = nil
	return reservation, nil
}

func (s *runtimeTestStore) SourceDriverMutationReservation(
	_ context.Context,
	mutation catalog.MutationID,
) (catalog.SourceDriverMutationReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservation == nil || s.reservation.Mutation != mutation {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrNotFound
	}
	return *s.reservation, nil
}

func (s *runtimeTestStore) ActiveSourceDriverMutationReservation(
	_ context.Context,
	authority causal.SourceAuthorityID,
) (*catalog.SourceDriverMutationReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservation == nil || s.reservation.Authority != authority || s.reservation.Committed {
		return nil, nil
	}
	reservation := *s.reservation
	return &reservation, nil
}

func (s *runtimeTestStore) PrepareSourceDriverMutationReservationBatch(
	_ context.Context,
	mutation catalog.MutationID,
	claim catalog.MutationClaim,
) (catalog.SourceDriverMutationReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "prepare-targets")
	if s.reservation == nil || s.reservation.Mutation != mutation || s.reservation.Claim != claim {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrMutationClaimed
	}
	if s.reservation.TargetEpoch != s.targetEpoch {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrGenerationMismatch
	}
	s.reservedTargets = append([]catalog.SourceDriverTarget(nil), s.targets...)
	s.reservation.DeclaredTargetCount = uint64(len(s.reservedTargets))
	s.reservation.TargetsPrepared = true
	if len(s.reservedTargets) != 0 {
		s.reservation.TargetCursor = s.reservedTargets[len(s.reservedTargets)-1].Tenant
	}
	return *s.reservation, nil
}

func (s *runtimeTestStore) BindSourceDriverMutationRequest(
	_ context.Context,
	mutation catalog.MutationID,
	claim catalog.MutationClaim,
	digest [sha256.Size]byte,
) (catalog.SourceDriverMutationReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "bind-request")
	if s.reservation == nil || s.reservation.Mutation != mutation || s.reservation.Claim != claim ||
		!s.reservation.TargetsPrepared {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrInvalidTransition
	}
	if s.reservation.RequestBound && s.reservation.RequestDigest != digest {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrMutationConflict
	}
	s.reservation.RequestDigest = digest
	s.reservation.RequestBound = true
	return *s.reservation, nil
}

func (s *runtimeTestStore) ReleaseUnboundSourceDriverMutationReservation(
	_ context.Context,
	mutation catalog.MutationID,
	claim catalog.MutationClaim,
	targetEpoch uint64,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservation == nil {
		return nil
	}
	if s.reservation.Mutation != mutation || s.reservation.Claim != claim ||
		s.reservation.TargetEpoch != targetEpoch || s.reservation.RequestBound || s.reservation.Receipt != nil {
		return catalog.ErrMutationConflict
	}
	s.reservation = nil
	s.reservedTargets = nil
	return nil
}

func (s *runtimeTestStore) SourceDriverMutationReservationTargets(
	_ context.Context,
	mutation catalog.MutationID,
	after catalog.TenantID,
	limit int,
) (catalog.SourceDriverTargetPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.reservation == nil || s.reservation.Mutation != mutation || !s.reservation.TargetsPrepared {
		return catalog.SourceDriverTargetPage{}, catalog.ErrInvalidTransition
	}
	start := 0
	for start < len(s.reservedTargets) && s.reservedTargets[start].Tenant <= after {
		start++
	}
	end := min(start+limit, len(s.reservedTargets))
	page := catalog.SourceDriverTargetPage{
		Targets: append([]catalog.SourceDriverTarget(nil), s.reservedTargets[start:end]...),
	}
	if end < len(s.reservedTargets) {
		page.Next = page.Targets[len(page.Targets)-1].Tenant
	}
	return page, nil
}

func (s *runtimeTestStore) RecordSourceDriverMutationReceipt(
	_ context.Context,
	mutation catalog.MutationID,
	claim catalog.MutationClaim,
	proof catalog.SourceDriverMutationReceiptProof,
) (catalog.SourceDriverMutationReservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, "record-receipt")
	if s.reservation == nil || s.reservation.Mutation != mutation || s.reservation.Claim != claim ||
		!s.reservation.RequestBound {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrInvalidTransition
	}
	if s.recordFailures > 0 {
		s.recordFailures--
		return catalog.SourceDriverMutationReservation{}, context.DeadlineExceeded
	}
	if s.reservation.Receipt != nil && *s.reservation.Receipt != proof {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrMutationConflict
	}
	s.reservation.Receipt = &proof
	if s.loseRecord {
		s.loseRecord = false
		return catalog.SourceDriverMutationReservation{}, context.DeadlineExceeded
	}
	return *s.reservation, nil
}

func (s *runtimeTestStore) recordEvent(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *runtimeTestStore) SourceDriverStageTargets(
	_ context.Context,
	_ causal.SourceAuthorityID,
	_ causal.OperationID,
	after catalog.TenantID,
	limit int,
) ([]catalog.SourceDriverTarget, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	start := 0
	for start < len(s.targets) && s.targets[start].Tenant <= after {
		start++
	}
	end := min(start+limit, len(s.targets))
	return append([]catalog.SourceDriverTarget(nil), s.targets[start:end]...), nil
}

func (s *runtimeTestStore) AppendSourceDriverStage(
	_ context.Context,
	identity catalog.SourceDriverStageIdentity,
	page catalog.SourceDriverStagePage,
) (catalog.SourceDriverStageState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil || s.pending.Identity != identity || s.pending.Stage.Sequence != page.Sequence {
		return catalog.SourceDriverStageState{}, catalog.ErrMutationConflict
	}
	items := uint64(len(page.Entries))
	if items == 0 {
		items = 1
	}
	s.pending.Stage.Sequence++
	s.pending.Stage.Items += items
	s.pending.Stage.Bytes++
	s.pending.Stage.Revision = identity.Predecessor + 1
	s.pending.Stage.Digest = sha256.Sum256(append(s.pending.Stage.Digest[:], page.Digest[:]...))
	s.pending.Cursor = append([]byte(nil), page.Cursor...)
	s.pending.PageDigest = page.Digest
	value := *s.pending
	if s.loseAppend {
		s.loseAppend = false
		return catalog.SourceDriverStageState{}, context.DeadlineExceeded
	}
	return value, nil
}

func (s *runtimeTestStore) CommitSourceDriverStage(
	_ context.Context,
	state catalog.SourceDriverStageState,
) (catalog.SourceDriverStageResult, error) {
	return s.commit(state, false)
}

func (s *runtimeTestStore) PrepareSourceDriverPublicationBatch(
	_ context.Context,
	identity catalog.SourceDriverStageIdentity,
) (catalog.SourceDriverPreparationState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.advanceEpochOnPublicationPrepare {
		s.advanceEpochOnPublicationPrepare = false
		s.targetEpoch++
	}
	if s.prepareFailures > 0 {
		s.prepareFailures--
		return catalog.SourceDriverPreparationState{}, context.DeadlineExceeded
	}
	if s.pending == nil || s.pending.Identity != identity || s.pending.TargetEpoch != s.targetEpoch {
		return catalog.SourceDriverPreparationState{}, catalog.ErrGenerationMismatch
	}
	return catalog.SourceDriverPreparationState{
		Authority: string(identity.Authority), Publication: identity.Operation,
		SourceRevision: uint64(identity.Predecessor + 1), TargetCount: identity.TargetCount,
		PreparedTargets: identity.TargetCount, Prepared: true,
	}, nil
}

func (s *runtimeTestStore) CommitSourceDriverMutation(
	_ context.Context,
	state catalog.SourceDriverStageState,
) (catalog.SourceDriverStageResult, error) {
	return s.commit(state, true)
}

func (s *runtimeTestStore) commit(
	state catalog.SourceDriverStageState,
	mutation bool,
) (catalog.SourceDriverStageResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.commitFailures > 0 {
		s.commitFailures--
		return catalog.SourceDriverStageResult{}, context.DeadlineExceeded
	}
	if s.pending == nil || state.Identity != s.pending.Identity || len(s.pending.Cursor) != 0 {
		return catalog.SourceDriverStageResult{}, catalog.ErrMutationConflict
	}
	if state.TargetEpoch != s.targetEpoch {
		return catalog.SourceDriverStageResult{}, catalog.ErrGenerationMismatch
	}
	checkpoint := catalog.SourceDriverCheckpoint{
		Authority: state.Identity.Authority, FleetOwner: state.Identity.FleetOwner,
		AuthorityGeneration: state.Identity.AuthorityGeneration,
		DeclarationDigest:   state.Identity.DeclarationDigest, TargetCount: state.Identity.TargetCount,
		TargetEpoch: state.TargetEpoch, TargetsDigest: state.Identity.TargetsDigest, Token: state.Identity.ToToken,
		SourceRevision: state.Identity.Predecessor + 1, SourceOperation: state.Identity.SourceOperation,
		ChangeID: state.Identity.ChangeID, Cause: state.Identity.Cause, Origin: state.Identity.Origin,
		OriginGeneration: state.Identity.OriginGeneration,
	}
	s.checkpoint = &checkpoint
	for _, target := range s.targets {
		prior := s.targetCheckpoints[target.Tenant]
		value := catalog.SourceDriverTargetCheckpoint{
			SourceDriverTarget: target, SourceRevision: checkpoint.SourceRevision,
			TargetEpoch: checkpoint.TargetEpoch, CatalogRevision: prior.CatalogRevision + 1,
		}
		if value.CatalogRevision == 0 {
			value.CatalogRevision = 1
		}
		s.targetCheckpoints[target.Tenant] = value
	}
	s.pending = nil
	result := catalog.SourceDriverStageResult{
		Identity: state.Identity, Proof: state.Stage, Checkpoint: checkpoint,
	}
	if mutation {
		s.mutationCommits++
		if s.reservation == nil || s.reservation.Mutation != state.Identity.Mutation {
			return catalog.SourceDriverStageResult{}, catalog.ErrMutationConflict
		}
		s.reservation.Committed = true
		result.MutationResult = &catalog.NamespaceMutationResult{Mutation: catalog.MutationRecord{
			ID: state.Identity.Mutation, Tenant: state.Identity.MutationTenant,
		}}
	}
	s.committed = &catalog.SourceDriverCommittedReceipt{Result: result}
	return result, nil
}

func (s *runtimeTestStore) PendingSourceDriverCommittedReceipt(
	context.Context, causal.SourceAuthorityID,
) (*catalog.SourceDriverCommittedReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed == nil || s.committed.Forgotten {
		return nil, nil
	}
	value := *s.committed
	return &value, nil
}

func (s *runtimeTestStore) CommittedSourceDriverMutation(
	_ context.Context, _ causal.SourceAuthorityID, mutation catalog.MutationID,
) (*catalog.SourceDriverCommittedReceipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed == nil || s.committed.Result.Identity.Mode != catalog.SourceDriverMutation ||
		s.committed.Result.Identity.Mutation != mutation {
		return nil, nil
	}
	value := *s.committed
	return &value, nil
}

func (s *runtimeTestStore) AcknowledgeSourceDriverCommittedReceipt(
	_ context.Context, result catalog.SourceDriverStageResult,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed == nil || s.committed.Result.Identity.Operation != result.Identity.Operation {
		return catalog.ErrNotFound
	}
	s.committed.Acknowledged = true
	return nil
}

func (s *runtimeTestStore) ForgetSourceDriverCommittedReceipt(
	_ context.Context, result catalog.SourceDriverStageResult,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.committed == nil || !s.committed.Acknowledged ||
		s.committed.Result.Identity.Operation != result.Identity.Operation {
		return catalog.ErrInvalidTransition
	}
	s.committed.Forgotten = true
	if result.Identity.Mode == catalog.SourceDriverMutation && s.reservation != nil &&
		s.reservation.Mutation == result.Identity.Mutation {
		s.reservation = nil
		s.reservedTargets = nil
	}
	return nil
}

func (s *runtimeTestStore) AbortSourceDriverStage(_ context.Context, identity catalog.SourceDriverStageIdentity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending == nil || s.pending.Identity != identity {
		return catalog.ErrMutationConflict
	}
	s.aborts++
	s.pending = nil
	if s.loseAbort {
		s.loseAbort = false
		return context.DeadlineExceeded
	}
	return nil
}

func (s *runtimeTestStore) StageOwnedContent(ctx context.Context, source contentstream.Source) (catalog.ContentRef, error) {
	body, err := io.ReadAll(source)
	settleErr := source.Settle(err)
	waitErr := source.Wait(ctx)
	if err != nil || settleErr != nil || waitErr != nil {
		return catalog.ContentRef{}, errors.Join(err, settleErr, waitErr)
	}
	return catalog.ContentRef{Stage: catalog.StageID{1}, Hash: sha256.Sum256(body), Size: int64(len(body))}, nil
}

func (s *runtimeTestStore) ReleaseUnclaimedContent(_ context.Context, refs []catalog.ContentRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.releases += len(refs)
	return nil
}

type runtimeTestDriver struct {
	mu sync.Mutex

	head              sourcedriver.RevisionToken
	objects           []sourcedriver.Projection
	snapshotRequired  bool
	snapshotFailures  int
	changeFailures    int
	loseApply         bool
	receipt           *sourcedriver.MutationReceipt
	refreshCalls      int
	snapshotCalls     int
	changeCalls       int
	applyCalls        int
	inspectCalls      int
	requestedAfter    []sourcedriver.LogicalID
	acknowledged      bool
	settleFailures    int
	targetSet         sourcedriver.TargetSetState
	targetSets        map[sourcedriver.TargetSetID]sourcedriver.TargetSetState
	mutationTargetSet sourcedriver.TargetSetRef
	lastSnapshotSet   sourcedriver.TargetSetRef
	lastSettlementSet sourcedriver.TargetSetRef
	applyFailures     int
	declareFailures   int
	declareCalls      int
	event             func(string)
}

func newRuntimeTestDriver(target catalog.SourceDriverTarget) *runtimeTestDriver {
	return &runtimeTestDriver{
		head: "head-1", targetSets: make(map[sourcedriver.TargetSetID]sourcedriver.TargetSetState),
		objects: []sourcedriver.Projection{{
			Tenant: target.Tenant, Generation: causal.Generation(target.Generation),
			ID: "item", Parent: "", Name: "item", Kind: catalog.KindDirectory,
			Mode: 0o755, Visibility: catalog.Visibility{Mount: true},
		}},
	}
}

func (d *runtimeTestDriver) Refresh(context.Context, causal.SourceAuthorityID) (sourcedriver.Head, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.refreshCalls++
	return sourcedriver.Head{Revision: d.head}, nil
}

func (d *runtimeTestDriver) InspectTargetSet(
	_ context.Context,
	_ causal.SourceAuthorityID,
	ref sourcedriver.TargetSetRef,
) (sourcedriver.TargetSetState, error) {
	if d.event != nil {
		d.event("driver-inspect-targets")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	state, found := d.targetSets[ref.ID]
	if !found || state.Ref != ref {
		return sourcedriver.TargetSetState{}, sourcedriver.ErrNotFound
	}
	return state, nil
}

func (d *runtimeTestDriver) DeclareTargetSet(
	_ context.Context,
	authority causal.SourceAuthorityID,
	page sourcedriver.TargetSetPage,
) (sourcedriver.TargetSetState, error) {
	if d.event != nil {
		d.event("driver-declare-targets")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.declareCalls++
	state, found := d.targetSets[page.Ref.ID]
	if !found {
		var err error
		state, err = sourcedriver.NewTargetSetState(authority, page.Ref)
		if err != nil {
			return sourcedriver.TargetSetState{}, err
		}
	}
	next, err := sourcedriver.ApplyTargetSetPage(state, page)
	if err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	d.targetSet = next
	d.targetSets[page.Ref.ID] = next
	if d.declareFailures > 0 {
		d.declareFailures--
		return sourcedriver.TargetSetState{}, context.DeadlineExceeded
	}
	return next, nil
}

func (d *runtimeTestDriver) Snapshot(
	_ context.Context,
	_ causal.SourceAuthorityID,
	request sourcedriver.SnapshotRequest,
) (sourcedriver.SnapshotPage, error) {
	if d.event != nil {
		d.event("driver-snapshot")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.snapshotCalls++
	d.lastSnapshotSet = request.TargetSet
	if d.snapshotFailures > 0 {
		d.snapshotFailures--
		return sourcedriver.SnapshotPage{}, context.DeadlineExceeded
	}
	if request.Cursor != nil {
		d.requestedAfter = append(d.requestedAfter, request.Cursor.After)
	}
	start := 0
	if request.Cursor != nil {
		start = int(request.Cursor.Page)
	}
	end := min(start+request.Limit, len(d.objects))
	values := append([]sourcedriver.Projection(nil), d.objects[start:end]...)
	digest, err := sourcedriver.SnapshotPageDigest(request.Revision, values)
	if err != nil {
		return sourcedriver.SnapshotPage{}, err
	}
	page := sourcedriver.SnapshotPage{Revision: request.Revision, Objects: values, Digest: digest}
	if end < len(d.objects) {
		last := values[len(values)-1]
		pageNumber := uint32(1)
		if request.Cursor != nil {
			pageNumber = request.Cursor.Page + 1
		}
		next, err := sourcedriver.NewPageCursor(
			request.TargetSet, sourcedriver.PageSnapshot, "", request.Revision, pageNumber, request.Limit,
			sourcedriver.PagePosition{Tenant: last.Tenant, Generation: last.Generation, ID: last.ID}, nil, digest,
		)
		if err != nil {
			return sourcedriver.SnapshotPage{}, err
		}
		page.Next = &next
	}
	return page, nil
}

func (d *runtimeTestDriver) ChangesSince(
	_ context.Context,
	_ causal.SourceAuthorityID,
	request sourcedriver.ChangesRequest,
) (sourcedriver.ChangePage, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.changeCalls++
	if d.changeFailures > 0 {
		d.changeFailures--
		return sourcedriver.ChangePage{}, context.DeadlineExceeded
	}
	if d.snapshotRequired {
		d.snapshotRequired = false
		return sourcedriver.ChangePage{}, &sourcedriver.SnapshotRequiredError{From: request.From, Head: request.To}
	}
	object := d.objects[0]
	change := sourcedriver.Change{
		Kind: sourcedriver.ChangeUpsert, Tenant: object.Tenant, Generation: object.Generation,
		Sequence: 1, ID: object.ID, Object: &object,
	}
	digest, err := sourcedriver.ChangePageDigest(request.From, request.To, []sourcedriver.Change{change})
	return sourcedriver.ChangePage{
		From: request.From, To: request.To, Changes: []sourcedriver.Change{change}, Digest: digest,
	}, err
}

func (d *runtimeTestDriver) OpenContent(
	context.Context,
	causal.SourceAuthorityID,
	sourcedriver.ContentRef,
) (contentstream.Source, error) {
	return &runtimeByteSource{Reader: bytes.NewReader([]byte("body"))}, nil
}

func (d *runtimeTestDriver) ApplyMutation(
	_ context.Context,
	_ causal.SourceAuthorityID,
	request sourcedriver.MutationRequest,
	content contentstream.Source,
) (sourcedriver.MutationReceipt, error) {
	if d.event != nil {
		d.event("driver-apply")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.applyCalls++
	if d.applyFailures > 0 {
		d.applyFailures--
		return sourcedriver.MutationReceipt{}, context.DeadlineExceeded
	}
	if request.HasContent {
		body, err := io.ReadAll(content)
		if err != nil || int64(len(body)) != request.ContentSize || sha256.Sum256(body) != request.ContentHash {
			return sourcedriver.MutationReceipt{}, errors.Join(sourcedriver.ErrIntegrity, err)
		}
	} else if content != nil {
		return sourcedriver.MutationReceipt{}, errors.New("unexpected mutation content")
	}
	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	receipt := sourcedriver.MutationReceipt{
		OperationID: request.OperationID, State: sourcedriver.MutationApplied, RequestDigest: requestDigest,
		Expected: request.Expected, Committed: "head-2",
	}
	if request.Context.Operation.Kind == catalog.MutationCreate {
		receipt.Result = "created"
	}
	receipt.Digest, err = sourcedriver.MutationReceiptDigest(receipt)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	d.receipt = &receipt
	d.mutationTargetSet = request.TargetSet
	d.head = "head-2"
	if d.loseApply {
		d.loseApply = false
		return sourcedriver.MutationReceipt{}, context.DeadlineExceeded
	}
	return receipt, nil
}

func (d *runtimeTestDriver) InspectMutation(
	_ context.Context,
	_ causal.SourceAuthorityID,
	id catalog.MutationID,
	requestDigest [sha256.Size]byte,
) (sourcedriver.MutationReceipt, error) {
	if d.event != nil {
		d.event("driver-inspect-mutation")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inspectCalls++
	if d.receipt == nil {
		return sourcedriver.MutationReceipt{OperationID: id, State: sourcedriver.MutationNotFound}, nil
	}
	if d.receipt.RequestDigest != requestDigest {
		return sourcedriver.MutationReceipt{}, sourcedriver.ErrConflict
	}
	return *d.receipt, nil
}

func (d *runtimeTestDriver) SettleMutation(
	_ context.Context,
	_ causal.SourceAuthorityID,
	settlement sourcedriver.MutationSettlement,
) error {
	if d.event != nil {
		d.event("driver-settle")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.settleFailures > 0 {
		d.settleFailures--
		return context.DeadlineExceeded
	}
	if d.receipt == nil || d.receipt.OperationID != settlement.OperationID ||
		d.receipt.RequestDigest != settlement.RequestDigest || d.receipt.Digest != settlement.ReceiptDigest ||
		(d.mutationTargetSet != (sourcedriver.TargetSetRef{}) && d.mutationTargetSet != settlement.TargetSet) {
		return sourcedriver.ErrConflict
	}
	d.lastSettlementSet = settlement.TargetSet
	switch settlement.Kind {
	case sourcedriver.MutationSettlementAcknowledge:
		d.acknowledged = true
	case sourcedriver.MutationSettlementForget:
		if !d.acknowledged {
			return sourcedriver.ErrConflict
		}
		d.receipt = nil
	case sourcedriver.MutationSettlementAbandon:
		return sourcedriver.ErrConflict
	default:
		return sourcedriver.ErrInvalidValue
	}
	return nil
}

type runtimeByteSource struct {
	*bytes.Reader
	mu      sync.Mutex
	settled bool
	err     error
}

func (s *runtimeByteSource) Settle(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.settled {
		s.settled = true
		s.err = err
	}
	return s.err
}

func (s *runtimeByteSource) Wait(context.Context) error { return nil }

type runtimeProbeSource struct {
	*bytes.Reader
	mu          sync.Mutex
	reads       int
	settlements int
}

func (s *runtimeProbeSource) Read(body []byte) (int, error) {
	s.mu.Lock()
	s.reads++
	s.mu.Unlock()
	return s.Reader.Read(body)
}

func (s *runtimeProbeSource) Settle(error) error {
	s.mu.Lock()
	s.settlements++
	s.mu.Unlock()
	return nil
}

func (s *runtimeProbeSource) Wait(context.Context) error { return nil }

var _ Store = (*runtimeTestStore)(nil)
var _ sourcedriver.Driver = (*runtimeTestDriver)(nil)
