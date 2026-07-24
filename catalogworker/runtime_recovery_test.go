package catalogworker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

func TestRuntimeRecoveryWirePagesHaveExactCountAndDigestBounds(t *testing.T) {
	record := testSourceRuntimeProcessRecord(t)
	receipt := sourceAuthorityRuntimeReapReceiptForWorkerTest(t, record)
	closed := []catalog.SourceAuthorityRuntimeFence{
		{Owner: "owner", Generation: 1, Authority: "alpha", Epoch: [16]byte{1}},
		{Owner: "owner", Generation: 1, Authority: "beta", Epoch: [16]byte{2}},
	}
	digest, err := catalog.SourceAuthorityRuntimeRecoveryDigest(receipt, closed)
	if err != nil {
		t.Fatal(err)
	}
	summary := catalog.SourceAuthorityRuntimeRecoverySummary{
		ReceiptDigest: receipt.Digest, ClosedCount: 2, ClosedDigest: digest,
	}
	request := catalog.SourceAuthorityRuntimeRecoveryPageRequest{
		Receipt: receipt, Limit: catalog.SourceAuthorityRuntimeRecoveryPageLimit,
	}
	page := catalog.SourceAuthorityRuntimeRecoveryPage{
		Summary: summary, Start: 1, Closed: closed,
	}
	if err := page.Validate(request); err != nil {
		t.Fatalf("exact recovery page: %v", err)
	}
	request.Limit++
	if err := request.Validate(); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("over-limit recovery page request = %v, want ErrInvalidObject", err)
	}
	request.Limit = catalog.SourceAuthorityRuntimeRecoveryPageLimit
	page.Closed[0], page.Closed[1] = page.Closed[1], page.Closed[0]
	if err := page.Validate(request); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("unordered recovery page = %v, want ErrIntegrity", err)
	}
}

func TestManagerReplaysPluralRuntimeRecoveryAcrossWorkerGenerationLoss(t *testing.T) {
	manager, _ := newTestManager(t)
	owner := catalog.SourceAuthorityFleetOwnerID("worker-runtime-recovery")
	authorities := []causal.SourceAuthorityID{"alpha", "beta"}
	declarations, authoritiesDigest, declarationsDigest := testSourceAuthorityFleet(t, authorities)
	stage, err := manager.ReconcileSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetReconcileRequest{
			Owner: owner, Generation: 1, Declarations: declarations,
			Complete: true, AuthorityCount: uint64(len(authorities)),
			AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcknowledgeSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetAcknowledgement{
			Owner: owner, Generation: 1, AuthorityCount: uint64(len(authorities)),
			AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
			StageDigest: stage.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}
	process := testSourceRuntimeProcessRecord(t)
	closed := make([]catalog.SourceAuthorityRuntimeFence, 0, len(authorities))
	for index, authority := range authorities {
		fence := catalog.SourceAuthorityRuntimeFence{
			Owner: owner, Generation: 1, Authority: authority,
			Epoch: [16]byte{byte(index + 1)},
		}
		if err := manager.TakeoverSourceAuthorityRuntime(
			t.Context(),
			catalog.SourceAuthorityRuntimeTakeover{
				Ref: catalog.SourceAuthorityRuntimeRef{
					Owner: owner, Generation: 1, Authority: authority,
				},
				Epoch: fence.Epoch, Process: process,
			},
		); err != nil {
			t.Fatal(err)
		}
		closed = append(closed, fence)
	}
	receipt := sourceAuthorityRuntimeReapReceiptForWorkerTest(t, process)
	generation := manager.current
	committed, err := generation.client.BeginRecoverReapedSourceAuthorityRuntimes(
		t.Context(), receipt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	result, err := manager.RecoverReapedSourceAuthorityRuntimes(t.Context(), receipt)
	if err != nil {
		t.Fatalf("replay after worker generation loss: %v", err)
	}
	if result.Summary != committed || !reflect.DeepEqual(result.Closed, closed) {
		t.Fatalf("plural recovery = %+v, want summary %+v closed %+v", result, committed, closed)
	}
	floor := proc.ReapReceiptFloor{
		LedgerID: receipt.LedgerID, RecoveryID: recoveryid.SourceOwner, Sequence: receipt.Sequence,
	}
	invalidFloor := floor
	invalidFloor.RecoveryID = recoveryid.CatalogWorker
	if err := manager.AcknowledgeSourceAuthorityRuntimeRecovery(
		t.Context(), invalidFloor,
	); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("wrong acknowledgement class = %v, want invalid object", err)
	}
	if err := manager.AcknowledgeSourceAuthorityRuntimeRecovery(t.Context(), floor); err != nil {
		t.Fatal(err)
	}
	if err := manager.AcknowledgeSourceAuthorityRuntimeRecovery(t.Context(), floor); err != nil {
		t.Fatalf("acknowledgement replay: %v", err)
	}
}

func TestManagerReplaysCommittedSourceMutationAcrossWorkerGenerationLoss(t *testing.T) {
	manager, launcher := newTestManager(t)
	authority := causal.SourceAuthorityID("mutation-replay")
	provision := testTenantProvision(t, "source-mutation-replay")
	provision.ContentSourceID = string(authority)
	fixture := installCurrentWorkerTenantForTest(t, manager, provision)
	provision = fixture.Provision
	identity := catalog.SourceDriverStageIdentity{
		Authority: authority, FleetOwner: fixture.FleetOwner, AuthorityGeneration: 1,
		DeclarationDigest: fixture.DeclarationDigest,
		TargetCount:       fixture.Checkpoint.TargetCount, TargetsDigest: fixture.Checkpoint.TargetsDigest,
		Operation: causal.OperationID{1}, SourceOperation: causal.OperationID{2}, ChangeID: causal.ChangeID{3},
		Cause: causal.CauseExternalUnattributed, Mode: catalog.SourceDriverDelta,
		FromToken: fixture.Checkpoint.Token, ToToken: "head-2",
		Predecessor: fixture.Checkpoint.SourceRevision,
	}
	if err := manager.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	snapshotState, err := manager.AppendSourceDriverStage(t.Context(), identity, catalog.SourceDriverStagePage{
		Sequence: 0, Digest: [sha256.Size]byte{1},
		PredecessorDigest: catalog.SourceDriverPagePredecessorDigest(nil, [sha256.Size]byte{}), Complete: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForWorkerTest(t, manager, identity)
	snapshotResult, err := manager.CommitSourceDriverStage(t.Context(), snapshotState)
	if err != nil {
		t.Fatal(err)
	}
	ackGeneration := manager.current
	if err := ackGeneration.client.AcknowledgeSourceDriverCommittedReceipt(t.Context(), snapshotResult); err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(ackGeneration); err != nil {
		t.Fatal(err)
	}
	if err := manager.AcknowledgeSourceDriverCommittedReceipt(t.Context(), snapshotResult); err != nil {
		t.Fatalf("source driver receipt acknowledgement replay after generation loss: %v", err)
	}
	forgetGeneration := manager.current
	if err := forgetGeneration.client.ForgetSourceDriverCommittedReceipt(t.Context(), snapshotResult); err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(forgetGeneration); err != nil {
		t.Fatal(err)
	}
	if err := manager.ForgetSourceDriverCommittedReceipt(t.Context(), snapshotResult); err != nil {
		t.Fatalf("source driver receipt forget replay after generation loss: %v", err)
	}
	targetPage, err := manager.SourceDriverCommittedTargetCheckpoints(
		t.Context(), snapshotResult.Identity.Authority, snapshotResult.Identity.Operation,
		"", 1,
	)
	if err != nil || len(targetPage.Targets) != 1 ||
		targetPage.Targets[0].TargetEpoch != snapshotResult.Checkpoint.TargetEpoch {
		t.Fatalf("SourceDriverCommittedTargetCheckpoints: %+v, %v", targetPage, err)
	}
	prepared, err := manager.BeginMutation(t.Context(), provision.Tenant, targetPage.Targets[0].CatalogRevision, catalog.MutationIntent{
		SourceID: "driver", Origin: catalog.CausalOrigin{Cause: causal.CauseDaemonWrite}, Disposition: catalog.MutationDispositionNamespace,
		Create: &catalog.CreateMutation{Spec: catalog.CreateSpec{
			Parent: provision.Root, Name: "created", Kind: catalog.KindDirectory, Mode: 0o700,
			Visibility: catalog.Visibility{Mount: true},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	mutationOwner, err := catalog.NewMutationOwnerID()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err = manager.ClaimMutation(t.Context(), prepared.OperationID, mutationOwner)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err = manager.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err = manager.SetMutationSourceResult(t.Context(), prepared.OperationID, *prepared.Claim, catalog.SourceLocator{
		SourceAuthority: authority, SourceRevision: prepared.Source.Parent.SourceRevision, SourceKey: "created",
	})
	if err != nil {
		t.Fatal(err)
	}
	mutationIdentity := catalog.SourceDriverStageIdentity{
		Authority: authority, FleetOwner: fixture.FleetOwner, AuthorityGeneration: 1,
		DeclarationDigest: fixture.DeclarationDigest,
		TargetCount:       1, TargetsDigest: fixture.Checkpoint.TargetsDigest,
		Operation: causal.OperationID{4}, SourceOperation: causal.OperationID{5}, ChangeID: causal.ChangeID{6},
		Cause: causal.CauseDaemonWrite, Mode: catalog.SourceDriverMutation,
		FromToken: snapshotResult.Checkpoint.Token, ToToken: "head-3",
		Predecessor: snapshotResult.Checkpoint.SourceRevision,
		Mutation:    prepared.OperationID, MutationTenant: provision.Tenant,
		MutationGeneration: provision.Generation, MutationResult: "created",
		MutationRequestDigest: [sha256.Size]byte{7}, MutationReceiptDigest: [sha256.Size]byte{8},
		Claim: *prepared.Claim,
	}
	reservation, err := manager.ReserveSourceDriverMutation(t.Context(), catalog.SourceDriverMutationReservationRequest{
		Mutation: mutationIdentity.Mutation, Claim: mutationIdentity.Claim,
		Authority: mutationIdentity.Authority, FleetOwner: mutationIdentity.FleetOwner,
		AuthorityGeneration: mutationIdentity.AuthorityGeneration,
		DeclarationDigest:   mutationIdentity.DeclarationDigest,
		TargetCount:         mutationIdentity.TargetCount, TargetsDigest: mutationIdentity.TargetsDigest,
		Target: catalog.SourceDriverTarget{
			Tenant: mutationIdentity.MutationTenant, Generation: mutationIdentity.MutationGeneration,
		},
		FromToken: mutationIdentity.FromToken, Predecessor: mutationIdentity.Predecessor,
		Operation: mutationIdentity.Operation, SourceOperation: mutationIdentity.SourceOperation,
		ChangeID: mutationIdentity.ChangeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	for !reservation.TargetsPrepared {
		reservation, err = manager.PrepareSourceDriverMutationReservationBatch(
			t.Context(), mutationIdentity.Mutation, mutationIdentity.Claim,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	reservation, err = manager.BindSourceDriverMutationRequest(
		t.Context(), mutationIdentity.Mutation, mutationIdentity.Claim,
		mutationIdentity.MutationRequestDigest,
	)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err = manager.RecordSourceDriverMutationReceipt(
		t.Context(), mutationIdentity.Mutation, mutationIdentity.Claim,
		catalog.SourceDriverMutationReceiptProof{
			ToToken: mutationIdentity.ToToken, Result: mutationIdentity.MutationResult,
			Digest: mutationIdentity.MutationReceiptDigest,
		},
	)
	if err != nil || reservation.Receipt == nil {
		t.Fatalf("record source driver mutation receipt = %+v, %v", reservation, err)
	}
	receiptGeneration := manager.current
	if err := manager.poison(receiptGeneration); err != nil {
		t.Fatal(err)
	}
	activeReservation, err := manager.ActiveSourceDriverMutationReservation(t.Context(), authority)
	if err != nil || activeReservation == nil || activeReservation.Mutation != mutationIdentity.Mutation ||
		activeReservation.Receipt == nil || *activeReservation.Receipt != *reservation.Receipt {
		t.Fatalf("active reservation after worker generation loss = %+v, %v", activeReservation, err)
	}
	if err := manager.BeginSourceDriverStage(t.Context(), mutationIdentity); err != nil {
		t.Fatal(err)
	}
	mutationState, err := manager.AppendSourceDriverStage(t.Context(), mutationIdentity, catalog.SourceDriverStagePage{
		Sequence: 0, Digest: [sha256.Size]byte{2},
		PredecessorDigest: catalog.SourceDriverPagePredecessorDigest(nil, [sha256.Size]byte{}), Complete: true,
		Entries: []catalog.SourceDriverStageEntry{{
			Tenant: provision.Tenant, Generation: provision.Generation, ChangeSequence: 1, Key: "created",
			Object: &catalog.SourceObject{Key: "created", Name: "created", Kind: catalog.KindDirectory,
				Mode: 0o700, Visibility: catalog.Visibility{Mount: true}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	generation := manager.current
	prepareSourceDriverPublicationForWorkerTest(t, generation.client, mutationIdentity)
	committed, err := generation.client.CommitSourceDriverMutation(t.Context(), mutationState)
	if err != nil {
		t.Fatal(err)
	}
	committedHead, err := generation.client.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.poison(generation); err != nil {
		t.Fatal(err)
	}
	if !launcher.process(t, 2).reaped() {
		t.Fatal("lost worker generation was not reaped")
	}
	replayed, err := manager.CommitSourceDriverMutation(t.Context(), mutationState)
	if err != nil {
		t.Fatalf("source mutation replay after generation loss: %v", err)
	}
	if !reflect.DeepEqual(replayed, committed) {
		t.Fatalf("replayed source mutation = %+v, want %+v", replayed, committed)
	}
	replayedHead, err := manager.Head(t.Context(), provision.Tenant)
	if err != nil || replayedHead != committedHead {
		t.Fatalf("replayed tenant head = %d, %v, want %d", replayedHead, err, committedHead)
	}
	receipt, err := manager.CommittedSourceDriverMutation(t.Context(), authority, prepared.OperationID)
	if err != nil || receipt == nil || !reflect.DeepEqual(receipt.Result, committed) {
		t.Fatalf("exact committed mutation receipt = %+v, %v", receipt, err)
	}
}

type sourceDriverPublicationPreparer interface {
	PrepareSourceDriverPublicationBatch(
		context.Context,
		catalog.SourceDriverStageIdentity,
	) (catalog.SourceDriverPreparationState, error)
}

func prepareSourceDriverPublicationForWorkerTest(
	t *testing.T,
	store sourceDriverPublicationPreparer,
	identity catalog.SourceDriverStageIdentity,
) {
	t.Helper()
	for step := 0; step < 1024; step++ {
		state, err := store.PrepareSourceDriverPublicationBatch(t.Context(), identity)
		if err != nil {
			t.Fatalf("PrepareSourceDriverPublicationBatch step %d: %v", step, err)
		}
		if state.Prepared {
			return
		}
	}
	t.Fatal("source driver publication preparation did not converge")
}

func sourceAuthorityRuntimeReapReceiptForWorkerTest(
	t *testing.T,
	record proc.Record,
) proc.ReapReceipt {
	t.Helper()
	ledgerID := proc.ReceiptLedgerID{1}
	const sequence = uint64(1)
	payload, err := json.Marshal(struct {
		LedgerID         proc.ReceiptLedgerID `json:"ledger_id"`
		Sequence         uint64               `json:"sequence"`
		Record           proc.Record          `json:"record"`
		ReaperGeneration proc.OwnerGeneration `json:"reaper_generation"`
		Outcome          proc.ReapOutcome     `json:"outcome"`
	}{
		LedgerID: ledgerID, Sequence: sequence,
		Record: record, ReaperGeneration: catalogWorkerOwnerGeneration("worker-runtime-recovery-successor"),
		Outcome: proc.ReapAbsent,
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt := proc.ReapReceipt{
		LedgerID: ledgerID, Sequence: sequence,
		Record: record, ReaperGeneration: catalogWorkerOwnerGeneration("worker-runtime-recovery-successor"),
		Outcome: proc.ReapAbsent, Digest: sha256.Sum256(payload),
	}
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	return receipt
}
