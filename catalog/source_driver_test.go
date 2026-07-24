package catalog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceDriverSnapshotAndDeltaCommitOneAuthorityAcrossTargets(t *testing.T) {
	store, provisions, declaration, targets := newSourceDriverCatalog(t, "driver-a", "driver-b")
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 1,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatalf("BeginSourceDriverStage: %v", err)
	}
	page := sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{1}, Complete: true,
		Entries: []SourceDriverStageEntry{{
			Tenant: provisions[0].Tenant, Generation: provisions[0].Generation, Key: "directory",
			Object: &SourceObject{
				Key: "directory", Name: "directory", Kind: KindDirectory,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	})
	staged, err := store.AppendSourceDriverStage(t.Context(), identity, page)
	if err != nil {
		t.Fatalf("AppendSourceDriverStage: %v", err)
	}
	pending, err := store.PendingSourceDriverStage(t.Context(), identity.Authority)
	if err != nil || pending == nil || !equalSourceDriverStageState(*pending, staged) {
		t.Fatalf("pending stage = %+v, %v, want %+v", pending, err, staged)
	}
	appendReplay, err := store.AppendSourceDriverStage(t.Context(), identity, page)
	if err != nil || !equalSourceDriverStageState(appendReplay, staged) {
		t.Fatalf("append replay = %+v, %v, want %+v", appendReplay, err, staged)
	}
	prepareSourceDriverPublicationForTest(t, store, identity)
	result, err := store.CommitSourceDriverStage(t.Context(), staged)
	if err != nil {
		t.Fatalf("CommitSourceDriverStage: %v", err)
	}
	resultTargets := sourceDriverResultTargets(t, store, result)
	if result.Checkpoint.Token != "snapshot-token" || result.Checkpoint.SourceRevision != 1 ||
		len(resultTargets) != 2 {
		t.Fatalf("snapshot result = %+v", result)
	}
	for index, target := range resultTargets {
		wantRevision := Revision(1)
		if index == 0 {
			wantRevision = 2
		}
		if target.SourceRevision != 1 || target.CatalogRevision != wantRevision {
			t.Fatalf("snapshot target = %+v", target)
		}
	}
	replayed, err := store.CommitSourceDriverStage(t.Context(), staged)
	if err != nil || replayed.ReceiptDigest != result.ReceiptDigest {
		t.Fatalf("commit replay = %+v, %v, want digest %x", replayed, err, result.ReceiptDigest)
	}
	checkpoint, err := store.SourceDriverCheckpoint(t.Context(), identity.Authority)
	if err != nil || checkpoint != result.Checkpoint {
		t.Fatalf("checkpoint = %+v, %v, want %+v", checkpoint, err, result.Checkpoint)
	}
	for _, target := range targets {
		watermark, err := store.SourceDriverTargetCheckpoint(
			t.Context(), identity.Authority, target.Tenant, target.Generation,
		)
		if err != nil || watermark.SourceRevision != 1 {
			t.Fatalf("target checkpoint = %+v, %v", watermark, err)
		}
	}

	delta := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverDelta, 0,
		"snapshot-token", "delta-token", 1, 2,
	)
	if err := store.BeginSourceDriverStage(t.Context(), delta); err != nil {
		t.Fatalf("BeginSourceDriverStage(delta): %v", err)
	}
	deltaStage, err := store.AppendSourceDriverStage(t.Context(), delta, sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{2}, Complete: true,
	}))
	if err != nil {
		t.Fatalf("AppendSourceDriverStage(delta): %v", err)
	}
	prepareSourceDriverPublicationForTest(t, store, delta)
	deltaResult, err := store.CommitSourceDriverStage(t.Context(), deltaStage)
	if err != nil {
		t.Fatalf("CommitSourceDriverStage(delta): %v", err)
	}
	if deltaResult.Checkpoint.Token != "delta-token" || deltaResult.Checkpoint.SourceRevision != 2 {
		t.Fatalf("delta checkpoint = %+v", deltaResult.Checkpoint)
	}
	for index, target := range sourceDriverResultTargets(t, store, deltaResult) {
		wantRevision := Revision(1)
		if index == 0 {
			wantRevision = 2
		}
		if target.SourceRevision != 2 || target.CatalogRevision != wantRevision {
			t.Fatalf("no-op delta target = %+v", target)
		}
	}
	if _, err := store.LookupName(
		t.Context(), provisions[0].Tenant, PresentationMount, provisions[0].Root, "directory",
	); err != nil {
		t.Fatalf("snapshot object disappeared after no-op delta: %v", err)
	}
	var observers int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_observer_streams WHERE source_authority = ?`,
		string(identity.Authority)).Scan(&observers); err != nil {
		t.Fatal(err)
	}
	if observers != 0 {
		t.Fatalf("semantic driver created %d observer rows", observers)
	}
}

func TestSourceDriverTargetEpochAllowsSameGenerationResetAndRejectsABAReplay(t *testing.T) {
	store, _, declaration, targets := newSourceDriverCatalog(t, "driver-a")
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 10,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	staged, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{10}, Complete: true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, identity)
	result, err := store.CommitSourceDriverStage(t.Context(), staged)
	if err != nil {
		t.Fatal(err)
	}

	additional := testTenantProvision(t, "driver-b", 1)
	additional.ContentSourceID = "driver-authority"
	if _, err := provisionTenantForTest(t, store, t.Context(), additional); err != nil {
		t.Fatal(err)
	}
	newTargets := sourceDriverTargetsForProvisions(t, additional)
	newTargets = append(targets, newTargets...)
	sameGeneration := sourceDriverIdentityForTest(
		declaration, newTargets, SourceDriverDelta, 0,
		"snapshot-token", "invalid-token", 1, 11,
	)
	if err := store.BeginSourceDriverStage(t.Context(), sameGeneration); !errors.Is(err, ErrSourceRequiresSnapshot) {
		t.Fatalf("same-generation delta after target change = %v, want snapshot fence", err)
	}
	resetB := sourceDriverIdentityForTest(
		declaration, newTargets, SourceDriverSnapshot, SourceDriverSnapshotReset,
		"", "reset-b-token", 1, 12,
	)
	if err := store.BeginSourceDriverStage(t.Context(), resetB); err != nil {
		t.Fatalf("BeginSourceDriverStage(reset B): %v", err)
	}
	beforeDeclaration, err := store.SourceDriverCheckpoint(t.Context(), identity.Authority)
	if err != nil || beforeDeclaration != result.Checkpoint {
		t.Fatalf("provisional reset changed checkpoint before target proof: %+v, %v", beforeDeclaration, err)
	}
	prepareSourceDriverTargetDeclarationForTest(t, store, resetB)
	checkpointB, err := store.SourceDriverCheckpoint(t.Context(), identity.Authority)
	if err != nil {
		t.Fatal(err)
	}
	digestB, err := SourceDriverTargetsDigest(newTargets)
	if err != nil {
		t.Fatal(err)
	}
	if checkpointB.TargetEpoch <= result.Checkpoint.TargetEpoch || checkpointB.TargetCount != uint64(len(newTargets)) ||
		checkpointB.TargetsDigest != digestB || checkpointB.SnapshotRequired != SourceDriverSnapshotReset {
		t.Fatalf("verified B checkpoint = %+v, initial = %+v", checkpointB, result.Checkpoint)
	}
	if err := store.AbortSourceDriverStage(t.Context(), resetB); err != nil {
		t.Fatal(err)
	}

	nextFleet := reconcileSourceAuthorityFleetForTest(t, store, "driver-owner", 1, 2, "driver-authority")
	acknowledgeSourceAuthorityFleetForTest(t, store, nextFleet)
	rebind := SourceDriverCheckpointRebind{
		Expected: checkpointB, AuthorityGeneration: 2, DeclarationDigest: declaration,
	}
	rebound, err := store.RebindSourceDriverCheckpoint(t.Context(), rebind)
	if err != nil {
		t.Fatalf("RebindSourceDriverCheckpoint: %v", err)
	}
	if rebound.AuthorityGeneration != 2 || rebound.SnapshotRequired != SourceDriverSnapshotReset ||
		rebound.TargetEpoch != checkpointB.TargetEpoch || rebound.TargetsDigest != checkpointB.TargetsDigest {
		t.Fatalf("rebound checkpoint = %+v", rebound)
	}
	replayed, err := store.RebindSourceDriverCheckpoint(t.Context(), rebind)
	if err != nil || replayed != rebound {
		t.Fatalf("RebindSourceDriverCheckpoint(replay) = %+v, %v, want %+v", replayed, err, rebound)
	}
	if err := removeTenantForTest(t, store, t.Context(), additional.Tenant, additional.Generation); err != nil {
		t.Fatal(err)
	}
	resetA := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotReset,
		"", "reset-a-token", 1, 13,
	)
	resetA.AuthorityGeneration = 2
	if err := store.BeginSourceDriverStage(t.Context(), resetA); err != nil {
		t.Fatalf("BeginSourceDriverStage(reset A): %v", err)
	}
	prepareSourceDriverTargetDeclarationForTest(t, store, resetA)
	checkpointA2, err := store.SourceDriverCheckpoint(t.Context(), identity.Authority)
	if err != nil {
		t.Fatal(err)
	}
	if checkpointA2.TargetEpoch <= checkpointB.TargetEpoch || checkpointA2.TargetsDigest != result.Checkpoint.TargetsDigest ||
		checkpointA2.TargetCount != result.Checkpoint.TargetCount {
		t.Fatalf("A to B to A checkpoint reused stale identity: initial=%+v B=%+v A2=%+v", result.Checkpoint, checkpointB, checkpointA2)
	}
}

func TestCommitSourceDriverMutationIsOnlyTerminalPreparedCommit(t *testing.T) {
	store, provisions, declaration, targets := newSourceDriverCatalog(t, "driver-a", "driver-b")
	snapshot := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 20,
	)
	if err := store.BeginSourceDriverStage(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	staged, err := store.AppendSourceDriverStage(t.Context(), snapshot, sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{20}, Complete: true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, snapshot)
	snapshotResult, err := store.CommitSourceDriverStage(t.Context(), staged)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeSourceDriverCommittedReceipt(t.Context(), snapshotResult); err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetSourceDriverCommittedReceipt(t.Context(), snapshotResult); err != nil {
		t.Fatal(err)
	}

	provision := provisions[0]
	head, err := store.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.BeginMutation(t.Context(), provision.Tenant, head, MutationIntent{
		SourceID: "driver", Origin: CausalOrigin{Cause: causal.CauseDaemonWrite},
		Create: &CreateMutation{Spec: CreateSpec{
			Parent: provision.Root, Name: "created", Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		}},
	})
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	owner := mustMutationOwner(t)
	prepared, err = store.ClaimMutation(t.Context(), prepared.OperationID, owner)
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	prepared, err = store.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim)
	if err != nil {
		t.Fatalf("PrepareMutationSource: %v", err)
	}
	prepared, err = store.SetMutationSourceResult(t.Context(), prepared.OperationID, *prepared.Claim, SourceLocator{
		SourceAuthority: "driver-authority", SourceRevision: prepared.Source.Parent.SourceRevision,
		SourceKey: "created",
	})
	if err != nil {
		t.Fatalf("SetMutationSourceResult: %v", err)
	}
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverMutation, 0,
		"snapshot-token", "mutation-token", 1, 21,
	)
	identity.Cause = causal.CauseDaemonWrite
	identity.Mutation = prepared.OperationID
	identity.MutationTenant = provision.Tenant
	identity.MutationGeneration = provision.Generation
	identity.MutationResult = "created"
	identity.MutationRequestDigest = [sha256.Size]byte{1}
	identity.MutationReceiptDigest = [sha256.Size]byte{2}
	identity.Claim = *prepared.Claim
	reserveSourceDriverMutationForTest(t, store, identity)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatalf("BeginSourceDriverStage(mutation): %v", err)
	}
	mutationEntries := make([]SourceDriverStageEntry, 0, len(provisions))
	for _, targetProvision := range provisions {
		mutationEntries = append(mutationEntries, SourceDriverStageEntry{
			Tenant: targetProvision.Tenant, Generation: targetProvision.Generation,
			ChangeSequence: 1, Key: "created",
			Object: &SourceObject{
				Key: "created", Name: "created", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		})
	}
	mutationStage, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{21}, Complete: true,
		Entries: mutationEntries,
	}))
	if err != nil {
		t.Fatalf("AppendSourceDriverStage(mutation): %v", err)
	}
	prepareSourceDriverPublicationForTest(t, store, identity)
	if _, err := store.CommitSourceDriverStage(t.Context(), mutationStage); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("CommitSourceDriverStage mutation = %v", err)
	}
	committed, err := store.CommitSourceDriverMutation(t.Context(), mutationStage)
	if err != nil {
		t.Fatalf("CommitSourceDriverMutation: %v", err)
	}
	if committed.MutationResult == nil || committed.MutationResult.Mutation.ID != prepared.OperationID ||
		committed.MutationResult.Primary.Name != "created" {
		t.Fatalf("mutation result = %+v", committed.MutationResult)
	}
	for _, target := range sourceDriverResultTargets(t, store, committed) {
		if target.CatalogRevision != head+1 {
			t.Fatalf("cross-tenant mutation target = %+v", target)
		}
	}
	replayed, err := store.CommitSourceDriverMutation(t.Context(), mutationStage)
	if err != nil || replayed.ReceiptDigest != committed.ReceiptDigest {
		t.Fatalf("mutation replay = %+v, %v", replayed, err)
	}
	reservation, err := store.SourceDriverMutationReservation(t.Context(), prepared.OperationID)
	if err != nil || !reservation.Committed || !reservation.RequestBound || reservation.Receipt == nil {
		t.Fatalf("committed mutation reservation = %+v, %v", reservation, err)
	}
	if active, err := store.ActiveSourceDriverMutationReservation(t.Context(), identity.Authority); err != nil || active != nil {
		t.Fatalf("active reservation after commit = %+v, %v", active, err)
	}
	laterHead, err := store.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	later, err := store.BeginMutation(t.Context(), provision.Tenant, laterHead, MutationIntent{
		SourceID: "driver", Origin: CausalOrigin{Cause: causal.CauseDaemonWrite},
		Create: &CreateMutation{Spec: CreateSpec{
			Parent: provision.Root, Name: "created-later", Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	later, err = store.ClaimMutation(t.Context(), later.OperationID, mustMutationOwner(t))
	if err != nil {
		t.Fatal(err)
	}
	later, err = store.PrepareMutationSource(t.Context(), later.OperationID, *later.Claim)
	if err != nil {
		t.Fatal(err)
	}
	later, err = store.SetMutationSourceResult(t.Context(), later.OperationID, *later.Claim, SourceLocator{
		SourceAuthority: identity.Authority, SourceRevision: later.Source.Parent.SourceRevision,
		SourceKey: "created-later",
	})
	if err != nil {
		t.Fatal(err)
	}
	laterIdentity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverMutation, 0,
		identity.ToToken, "mutation-token-later", identity.Predecessor+1, 22,
	)
	laterIdentity.Cause = causal.CauseDaemonWrite
	laterIdentity.Mutation = later.OperationID
	laterIdentity.MutationTenant = provision.Tenant
	laterIdentity.MutationGeneration = provision.Generation
	laterIdentity.MutationResult = "created-later"
	laterIdentity.MutationRequestDigest = [sha256.Size]byte{3}
	laterIdentity.MutationReceiptDigest = [sha256.Size]byte{4}
	laterIdentity.Claim = *later.Claim
	laterReservation, err := store.ReserveSourceDriverMutation(
		t.Context(), sourceDriverMutationReservationRequestForIdentity(laterIdentity),
	)
	if err != nil {
		t.Fatalf("reserve after prior committed reservation: %v", err)
	}
	if err := store.ReleaseUnboundSourceDriverMutationReservation(
		t.Context(), later.OperationID, *later.Claim, laterReservation.TargetEpoch,
	); err != nil {
		t.Fatalf("release later reservation: %v", err)
	}
	pending, err := store.PreparedMutation(t.Context(), provision.Tenant, prepared.OperationID)
	if err != nil || pending.State != MutationCommitted {
		t.Fatalf("prepared state = %+v, %v", pending, err)
	}
	committedReceipt, err := store.PendingSourceDriverCommittedReceipt(t.Context(), identity.Authority)
	if err != nil || committedReceipt == nil || committedReceipt.Acknowledged || committedReceipt.Forgotten ||
		committedReceipt.Result.ReceiptDigest != committed.ReceiptDigest {
		t.Fatalf("pending committed receipt = %+v, %v", committedReceipt, err)
	}
	byMutation, err := store.CommittedSourceDriverMutation(t.Context(), identity.Authority, prepared.OperationID)
	if err != nil || byMutation == nil || byMutation.Result.ReceiptDigest != committed.ReceiptDigest {
		t.Fatalf("committed mutation receipt = %+v, %v", byMutation, err)
	}
	authorities, err := store.PendingSourceDriverReceiptAuthorities(t.Context(), "", 1)
	if err != nil || len(authorities.Authorities) != 1 || authorities.Authorities[0] != identity.Authority || authorities.Next != "" {
		t.Fatalf("pending receipt authorities = %+v, %v", authorities, err)
	}
	if err := store.AcknowledgeSourceDriverCommittedReceipt(t.Context(), committed); err != nil {
		t.Fatalf("AcknowledgeSourceDriverCommittedReceipt: %v", err)
	}
	if err := store.AcknowledgeSourceDriverCommittedReceipt(t.Context(), committed); err != nil {
		t.Fatalf("AcknowledgeSourceDriverCommittedReceipt replay: %v", err)
	}
	acknowledged, err := store.PendingSourceDriverCommittedReceipt(t.Context(), identity.Authority)
	if err != nil || acknowledged == nil || !acknowledged.Acknowledged || acknowledged.Forgotten {
		t.Fatalf("acknowledged committed receipt = %+v, %v", acknowledged, err)
	}
	if reservation, err = store.SourceDriverMutationReservation(t.Context(), prepared.OperationID); err != nil ||
		!reservation.Committed {
		t.Fatalf("acknowledged mutation reservation = %+v, %v", reservation, err)
	}
	if err := store.ForgetSourceDriverCommittedReceipt(t.Context(), committed); err != nil {
		t.Fatalf("ForgetSourceDriverCommittedReceipt: %v", err)
	}
	if err := store.ForgetSourceDriverCommittedReceipt(t.Context(), committed); err != nil {
		t.Fatalf("ForgetSourceDriverCommittedReceipt replay: %v", err)
	}
	if _, err := store.SourceDriverMutationReservation(t.Context(), prepared.OperationID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("forgotten mutation reservation = %v", err)
	}
	if receipt, err := store.PendingSourceDriverCommittedReceipt(t.Context(), identity.Authority); err != nil || receipt != nil {
		t.Fatalf("pending receipt after forget = %+v, %v", receipt, err)
	}
	authorities, err = store.PendingSourceDriverReceiptAuthorities(t.Context(), "", 1)
	if err != nil || len(authorities.Authorities) != 0 || authorities.Next != "" {
		t.Fatalf("pending receipt authorities after forget = %+v, %v", authorities, err)
	}
	byMutation, err = store.CommittedSourceDriverMutation(t.Context(), identity.Authority, prepared.OperationID)
	if err != nil || byMutation == nil || !byMutation.Acknowledged || !byMutation.Forgotten ||
		byMutation.Result.ReceiptDigest != committed.ReceiptDigest {
		t.Fatalf("forgotten exact mutation receipt = %+v, %v", byMutation, err)
	}
}

func reserveSourceDriverMutationForTest(t *testing.T, store *Catalog, identity SourceDriverStageIdentity) SourceDriverMutationReservation {
	t.Helper()
	reservation, err := store.ReserveSourceDriverMutation(
		t.Context(), sourceDriverMutationReservationRequestForIdentity(identity),
	)
	if err != nil {
		t.Fatalf("ReserveSourceDriverMutation: %v", err)
	}
	for !reservation.TargetsPrepared {
		reservation, err = store.PrepareSourceDriverMutationReservationBatch(
			t.Context(), identity.Mutation, identity.Claim,
		)
		if err != nil {
			t.Fatalf("PrepareSourceDriverMutationReservationBatch: %v", err)
		}
	}
	reservation, err = store.BindSourceDriverMutationRequest(
		t.Context(), identity.Mutation, identity.Claim, identity.MutationRequestDigest,
	)
	if err != nil {
		t.Fatalf("BindSourceDriverMutationRequest: %v", err)
	}
	reservation, err = store.RecordSourceDriverMutationReceipt(t.Context(), identity.Mutation, identity.Claim,
		SourceDriverMutationReceiptProof{
			ToToken: identity.ToToken, Result: identity.MutationResult, Digest: identity.MutationReceiptDigest,
		})
	if err != nil {
		t.Fatalf("RecordSourceDriverMutationReceipt: %v", err)
	}
	return reservation
}

func sourceDriverMutationReservationRequestForIdentity(
	identity SourceDriverStageIdentity,
) SourceDriverMutationReservationRequest {
	return SourceDriverMutationReservationRequest{
		Mutation: identity.Mutation, Claim: identity.Claim, Authority: identity.Authority,
		FleetOwner: identity.FleetOwner, AuthorityGeneration: identity.AuthorityGeneration,
		DeclarationDigest: identity.DeclarationDigest, TargetCount: identity.TargetCount,
		TargetsDigest: identity.TargetsDigest,
		Target:        SourceDriverTarget{Tenant: identity.MutationTenant, Generation: identity.MutationGeneration},
		FromToken:     identity.FromToken, Predecessor: identity.Predecessor,
		Operation: identity.Operation, SourceOperation: identity.SourceOperation,
		ChangeID: identity.ChangeID,
	}
}

func TestSourceDriverRejectsCrossPageReorderDuplicateAndStaleTarget(t *testing.T) {
	store, provisions, declaration, targets := newSourceDriverCatalog(t, "driver-a")
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 30,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	first := sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{30}, Cursor: []byte("first"),
		Entries: []SourceDriverStageEntry{{
			Tenant: provisions[0].Tenant, Generation: provisions[0].Generation, Key: "b",
			Object: &SourceObject{
				Key: "b", Name: "b", Kind: KindDirectory,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}},
	})
	firstState, err := store.AppendSourceDriverStage(t.Context(), identity, first)
	if err != nil {
		t.Fatal(err)
	}
	for name, entry := range map[string]SourceDriverStageEntry{
		"reordered": {
			Tenant: provisions[0].Tenant, Generation: provisions[0].Generation, Key: "a",
			Object: &SourceObject{Key: "a", Name: "a", Kind: KindDirectory, Visibility: Visibility{Mount: true}},
		},
		"duplicate": {
			Tenant: provisions[0].Tenant, Generation: provisions[0].Generation, Key: "b",
			Object: &SourceObject{Key: "b", Name: "b", Kind: KindDirectory, Visibility: Visibility{Mount: true}},
		},
		"stale_generation": {
			Tenant: provisions[0].Tenant, Generation: provisions[0].Generation + 1, Key: "c",
			Object: &SourceObject{Key: "c", Name: "c", Kind: KindDirectory, Visibility: Visibility{Mount: true}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(firstState, SourceDriverStagePage{
				Digest: [sha256.Size]byte{31}, Cursor: []byte("second"), Entries: []SourceDriverStageEntry{entry},
			}))
			if name == "stale_generation" {
				if !errors.Is(err, ErrGenerationMismatch) {
					t.Fatalf("AppendSourceDriverStage = %v, want generation mismatch", err)
				}
			} else if !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("AppendSourceDriverStage = %v, want tuple rejection", err)
			}
		})
	}
}

func TestSourceDriverPageTerminalAndPredecessorFences(t *testing.T) {
	store, _, declaration, targets := newSourceDriverCatalog(t, "driver-a")
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 31,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	for name, page := range map[string]SourceDriverStagePage{
		"complete_with_cursor": sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
			Digest: [sha256.Size]byte{1}, Cursor: []byte("next"), Complete: true,
		}),
		"incomplete_without_cursor": sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
			Digest: [sha256.Size]byte{2},
		}),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := store.AppendSourceDriverStage(t.Context(), identity, page); !errors.Is(err, ErrInvalidObject) {
				t.Fatalf("AppendSourceDriverStage = %v, want invalid object", err)
			}
		})
	}
	forged := sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{3}, Cursor: []byte("next"),
	})
	forged.PredecessorDigest[0] ^= 0xff
	if _, err := store.AppendSourceDriverStage(t.Context(), identity, forged); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("forged initial predecessor = %v, want invalid transition", err)
	}
	first := sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{4}, Cursor: []byte("next"),
	})
	firstState, err := store.AppendSourceDriverStage(t.Context(), identity, first)
	if err != nil {
		t.Fatal(err)
	}
	second := sourceDriverPageForTest(firstState, SourceDriverStagePage{
		Digest: [sha256.Size]byte{5}, Complete: true,
	})
	wrongSecond := second
	wrongSecond.PredecessorDigest[0] ^= 0xff
	if _, err := store.AppendSourceDriverStage(t.Context(), identity, wrongSecond); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("forged second predecessor = %v, want invalid transition", err)
	}
	pending, err := store.PendingSourceDriverStage(t.Context(), identity.Authority)
	if err != nil || pending == nil || !equalSourceDriverStageState(*pending, firstState) {
		t.Fatalf("pending after rejected predecessor = %+v, %v, want %+v", pending, err, firstState)
	}
	secondState, err := store.AppendSourceDriverStage(t.Context(), identity, second)
	if err != nil {
		t.Fatal(err)
	}
	afterComplete := sourceDriverPageForTest(secondState, SourceDriverStagePage{
		Digest: [sha256.Size]byte{6}, Complete: true,
	})
	if _, err := store.AppendSourceDriverStage(t.Context(), identity, afterComplete); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("page after terminal completion = %v, want invalid transition", err)
	}
	replayed, err := store.AppendSourceDriverStage(t.Context(), identity, first)
	if err != nil || !equalSourceDriverStageState(replayed, firstState) {
		t.Fatalf("first-page replay = %+v, %v, want %+v", replayed, err, firstState)
	}
	if secondState.Stage.Sequence != 2 || secondState.Cursor != nil {
		t.Fatalf("terminal state = %+v", secondState)
	}
}

func TestSourceDriverRejectsEngineOnlyOnDemandCauseBeforePersistence(t *testing.T) {
	_, _, declaration, targets := newSourceDriverCatalog(t, "driver-a")
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 32,
	)
	identity.Cause = causal.CauseOnDemand
	identity.Origin = "domain-a"
	identity.OriginGeneration = 1
	if _, err := validateSourceDriverStageIdentity(identity); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("validateSourceDriverStageIdentity(on-demand) = %v, want invalid object", err)
	}
}

func TestSourceDriverHundredTargetCatalogCommit(t *testing.T) {
	names := make([]string, 100)
	for index := range names {
		names[index] = fmt.Sprintf("driver-%03d", index)
	}
	store, _, declaration, targets := newSourceDriverCatalog(t, names...)
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 41,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	staged, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{Digest: [sha256.Size]byte{41}, Complete: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, identity)
	result, err := store.CommitSourceDriverStage(t.Context(), staged)
	if err != nil {
		t.Fatal(err)
	}
	resultTargets := sourceDriverResultTargets(t, store, result)
	if len(resultTargets) != len(targets) || result.Checkpoint.TargetCount != uint64(len(targets)) {
		t.Fatalf("100-target result has %d targets and checkpoint count %d", len(resultTargets), result.Checkpoint.TargetCount)
	}
	var persisted int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_driver_publication_targets
WHERE source_authority = ? AND publication_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&persisted); err != nil {
		t.Fatal(err)
	}
	if persisted != len(targets) {
		t.Fatalf("persisted target count = %d, want %d", persisted, len(targets))
	}
}

func TestSourceDriverNormalizationCommitsBoundedRecoverableBatches(t *testing.T) {
	store, provisions, declaration, targets := newSourceDriverCatalog(t, "driver-a")
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 42,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	entries := make([]SourceDriverStageEntry, 300)
	for index := range entries {
		key := SourceObjectKey(fmt.Sprintf("object-%03d", index))
		entries[index] = SourceDriverStageEntry{
			Tenant: provisions[0].Tenant, Generation: provisions[0].Generation, Key: key,
			Object: &SourceObject{
				Key: key, Name: string(key), Kind: KindDirectory,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}
	}
	first, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: [sha256.Size]byte{42}, Cursor: []byte("next"), Entries: entries[:256],
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	final := sourceDriverPageForTest(first, SourceDriverStagePage{
		Digest: [sha256.Size]byte{43}, Complete: true, Entries: entries[256:],
	})
	terminal, err := store.AppendSourceDriverStage(t.Context(), identity, final)
	if err != nil {
		t.Fatalf("terminal append: %v", err)
	}
	boom := errors.New("normalization interrupted")
	batches := 0
	store.failpoint = func(point string) error {
		if point != sourceDriverPreparationAfterBatchPoint {
			return nil
		}
		batches++
		var affected int
		if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_publication_stage_affected
WHERE source_authority = ? AND stage_operation_id = ?`,
			string(identity.Authority), identity.Operation[:]).Scan(&affected); err != nil {
			return err
		}
		if affected > 0 && affected < len(entries) {
			return boom
		}
		return nil
	}
	for step := 0; ; step++ {
		if step == 128 {
			t.Fatal("normalization did not reach a partial bounded batch")
		}
		if _, err := store.PrepareSourceDriverPublicationBatch(t.Context(), identity); errors.Is(err, boom) {
			break
		} else if err != nil {
			t.Fatalf("prepare interrupted normalization: %v", err)
		}
	}
	var affected, revisionComplete int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT
  (SELECT COUNT(*) FROM source_publication_stage_affected
   WHERE source_authority = ? AND stage_operation_id = ?),
  (SELECT complete FROM source_publication_stage_revisions
   WHERE source_authority = ? AND stage_operation_id = ? AND source_revision = 1)`,
		string(identity.Authority), identity.Operation[:], string(identity.Authority), identity.Operation[:]).Scan(
		&affected, &revisionComplete,
	); err != nil {
		t.Fatal(err)
	}
	if batches == 0 || affected == 0 || affected >= len(entries) || revisionComplete != 0 {
		t.Fatalf("interrupted normalization batches=%d affected=%d complete=%d",
			batches, affected, revisionComplete)
	}
	store.failpoint = nil
	prepareSourceDriverPublicationForTest(t, store, identity)
	result, err := store.CommitSourceDriverStage(t.Context(), terminal)
	if err != nil {
		t.Fatal(err)
	}
	resultTargets := sourceDriverResultTargets(t, store, result)
	if len(resultTargets) != 1 || resultTargets[0].CatalogRevision != 2 {
		t.Fatalf("normalized result = %+v", result)
	}
}

func TestSourceDriverDeltaStreamsOnlyLatestEntryPerTargetKey(t *testing.T) {
	store, provisions, declaration, targets := newSourceDriverCatalog(t, "driver-a")
	snapshot := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 42,
	)
	if err := store.BeginSourceDriverStage(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	staged, err := store.AppendSourceDriverStage(t.Context(), snapshot, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{Digest: [sha256.Size]byte{42}, Complete: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, snapshot)
	if _, err := store.CommitSourceDriverStage(t.Context(), staged); err != nil {
		t.Fatal(err)
	}
	delta := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverDelta, 0,
		"snapshot-token", "delta-token", 1, 43,
	)
	if err := store.BeginSourceDriverStage(t.Context(), delta); err != nil {
		t.Fatal(err)
	}
	provision := provisions[0]
	entries := []SourceDriverStageEntry{
		{
			Tenant: provision.Tenant, Generation: provision.Generation, ChangeSequence: 1, Key: "item",
			Object: &SourceObject{Key: "item", Name: "old", Kind: KindDirectory, Visibility: Visibility{Mount: true}},
		},
		{
			Tenant: provision.Tenant, Generation: provision.Generation, ChangeSequence: 2, Key: "item",
			Object: &SourceObject{Key: "item", Name: "new", Kind: KindDirectory, Visibility: Visibility{Mount: true}},
		},
	}
	deltaStage, err := store.AppendSourceDriverStage(t.Context(), delta, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: [sha256.Size]byte{43}, Complete: true, Entries: entries,
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, delta)
	if _, err := store.CommitSourceDriverStage(t.Context(), deltaStage); err != nil {
		t.Fatal(err)
	}
	if _, err := store.LookupName(t.Context(), provision.Tenant, PresentationMount, provision.Root, "new"); err != nil {
		t.Fatalf("latest projection missing: %v", err)
	}
	if _, err := store.LookupName(t.Context(), provision.Tenant, PresentationMount, provision.Root, "old"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("superseded projection lookup = %v, want not found", err)
	}
}

func TestSourceDriverCrossTenantCommitRollsBackAllTargets(t *testing.T) {
	store, provisions, declaration, targets := newSourceDriverCatalog(t, "driver-a", "driver-b")
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "snapshot-token", 0, 40,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	entries := make([]SourceDriverStageEntry, 0, len(provisions))
	for _, provision := range provisions {
		entries = append(entries, SourceDriverStageEntry{
			Tenant: provision.Tenant, Generation: provision.Generation, Key: "directory",
			Object: &SourceObject{
				Key: "directory", Name: "directory", Kind: KindDirectory,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		})
	}
	staged, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{40}, Complete: true, Entries: entries,
	}))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, identity)
	crash := errors.New("source driver crash")
	store.failpoint = func(point string) error {
		if point == sourceDriverAfterVisibilityCASPoint {
			return crash
		}
		return nil
	}
	if _, err := store.CommitSourceDriverStage(t.Context(), staged); !errors.Is(err, crash) {
		t.Fatalf("CommitSourceDriverStage = %v, want crash", err)
	}
	store.failpoint = nil
	for _, provision := range provisions {
		head, err := store.Head(t.Context(), provision.Tenant)
		if err != nil || head != 1 {
			t.Fatalf("head after rolled-back commit = %d, %v", head, err)
		}
	}
	if _, err := store.SourceDriverCheckpoint(t.Context(), identity.Authority); !errors.Is(err, ErrNotFound) {
		t.Fatalf("checkpoint after rolled-back commit = %v", err)
	}
	result, err := store.CommitSourceDriverStage(t.Context(), staged)
	if err != nil {
		t.Fatalf("retry CommitSourceDriverStage: %v", err)
	}
	for _, target := range sourceDriverResultTargets(t, store, result) {
		if target.CatalogRevision != 2 || target.SourceRevision != 1 {
			t.Fatalf("target after retry = %+v", target)
		}
	}
}

func TestSourceDriverReceiptCompactionQueryMatchesAuthorityWideSchema(t *testing.T) {
	store := newTestCatalog(t)
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := compactSettledSourceHistory(t.Context(), tx, 1024); err != nil {
		t.Fatalf("compactSettledSourceHistory: %v", err)
	}
}

func TestPendingSourceDriverReceiptAuthoritiesPagesDistinctAuthorities(t *testing.T) {
	store := newTestCatalog(t)
	insert := func(authority string, operation byte) {
		t.Helper()
		if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO source_driver_stage_receipts (
    source_authority, stage_operation_id, mode, from_token, to_token,
    source_revision, target_count, targets_digest, stage_sequence,
    stage_item_count, stage_byte_count, stage_digest, identity_digest,
    result_json, result_digest
) VALUES (?, ?, 1, '', 'head', 1, 1, ?, 1, 1, 1, ?, ?, ?, ?)`,
			authority, []byte{operation, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			make([]byte, sha256.Size), make([]byte, sha256.Size), make([]byte, sha256.Size), []byte("{}"), make([]byte, sha256.Size),
		); err != nil {
			t.Fatalf("insert receipt %q: %v", authority, err)
		}
	}
	insert("alpha", 1)
	insert("alpha", 2)
	insert("beta", 3)

	first, err := store.PendingSourceDriverReceiptAuthorities(t.Context(), "", 1)
	if err != nil || len(first.Authorities) != 1 || first.Authorities[0] != "alpha" || first.Next != "alpha" {
		t.Fatalf("first authority page = %+v, %v", first, err)
	}
	second, err := store.PendingSourceDriverReceiptAuthorities(t.Context(), first.Next, 1)
	if err != nil || len(second.Authorities) != 1 || second.Authorities[0] != "beta" || second.Next != "" {
		t.Fatalf("second authority page = %+v, %v", second, err)
	}
}

func prepareSourceDriverPublicationForTest(
	t *testing.T,
	store *Catalog,
	identity SourceDriverStageIdentity,
) SourceDriverPreparationState {
	t.Helper()
	for step := 0; step < SourceDriverTargetLimit*16+128; step++ {
		state, err := store.PrepareSourceDriverPublicationBatch(t.Context(), identity)
		if err != nil {
			t.Fatalf("PrepareSourceDriverPublicationBatch step %d: %v", step, err)
		}
		if state.Prepared {
			return state
		}
	}
	t.Fatal("source driver publication preparation did not converge")
	return SourceDriverPreparationState{}
}

func prepareSourceDriverTargetDeclarationForTest(
	t *testing.T,
	store *Catalog,
	identity SourceDriverStageIdentity,
) SourceDriverTargetDeclarationState {
	t.Helper()
	for step := 0; step < SourceDriverTargetLimit/sourceDriverTargetBatchSize+2; step++ {
		state, err := store.PrepareSourceDriverTargetDeclarationBatch(t.Context(), identity)
		if err != nil {
			t.Fatalf("PrepareSourceDriverTargetDeclarationBatch step %d: %v", step, err)
		}
		if state.Prepared {
			return state
		}
	}
	t.Fatal("source driver target declaration did not converge")
	return SourceDriverTargetDeclarationState{}
}

func newSourceDriverCatalog(
	t *testing.T,
	tenantNames ...string,
) (*Catalog, []TenantProvision, [sha256.Size]byte, []SourceDriverTarget) {
	t.Helper()
	store := newTestCatalog(t)
	provisions := make([]TenantProvision, 0, len(tenantNames))
	for _, name := range tenantNames {
		provision := testTenantProvision(t, name, 1)
		provision.ContentSourceID = "driver-authority"
		persisted, err := provisionTenantForTest(t, store, t.Context(), provision)
		if err != nil {
			t.Fatalf("ProvisionTenant(%s): %v", name, err)
		}
		provisions = append(provisions, persisted)
	}
	fleet := reconcileSourceAuthorityFleetForTest(t, store, "driver-owner", 0, 1, "driver-authority")
	acknowledgeSourceAuthorityFleetForTest(t, store, fleet)
	declaration := sourceAuthorityDeclarationsForTest("driver-authority")[0].DeclarationDigest
	return store, provisions, declaration, sourceDriverTargetsForProvisions(t, provisions...)
}

func sourceDriverTargetsForProvisions(t *testing.T, provisions ...TenantProvision) []SourceDriverTarget {
	t.Helper()
	targets := make([]SourceDriverTarget, 0, len(provisions))
	for _, provision := range provisions {
		targets = append(targets, SourceDriverTarget{
			Tenant: provision.Tenant, Generation: provision.Generation,
		})
	}
	return targets
}

func sourceDriverPageForTest(state SourceDriverStageState, page SourceDriverStagePage) SourceDriverStagePage {
	page.Sequence = state.Stage.Sequence
	page.PredecessorDigest = SourceDriverPagePredecessorDigest(state.Cursor, state.PageDigest)
	return page
}

func sourceDriverResultTargets(
	t *testing.T,
	store *Catalog,
	result SourceDriverStageResult,
) []SourceDriverTargetCheckpoint {
	t.Helper()
	var targets []SourceDriverTargetCheckpoint
	var after TenantID
	for {
		page, err := store.SourceDriverCommittedTargetCheckpoints(
			t.Context(), result.Identity.Authority, result.Identity.Operation,
			after, SourceDriverTargetCheckpointPageLimit,
		)
		if err != nil {
			t.Fatalf("SourceDriverCommittedTargetCheckpoints: %v", err)
		}
		targets = append(targets, page.Targets...)
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	if uint64(len(targets)) != result.Identity.TargetCount {
		t.Fatalf("committed target count = %d, want %d", len(targets), result.Identity.TargetCount)
	}
	return targets
}

func sourceDriverIdentityForTest(
	declaration [sha256.Size]byte,
	targets []SourceDriverTarget,
	mode SourceDriverMode,
	reason SourceDriverSnapshotReason,
	from, to string,
	predecessor causal.Revision,
	operationByte byte,
) SourceDriverStageIdentity {
	digest, err := SourceDriverTargetsDigest(targets)
	if err != nil {
		panic(err)
	}
	var operation, source causal.OperationID
	var change causal.ChangeID
	operation[0], source[0], change[0] = operationByte, operationByte+64, operationByte+128
	return SourceDriverStageIdentity{
		Authority: "driver-authority", FleetOwner: "driver-owner", AuthorityGeneration: 1,
		DeclarationDigest: declaration, TargetCount: uint64(len(targets)), TargetsDigest: digest,
		Operation: operation, SourceOperation: source, ChangeID: change,
		Cause: causal.CauseExternalUnattributed, Mode: mode, SnapshotReason: reason,
		FromToken: from, ToToken: to, Predecessor: predecessor,
	}
}
