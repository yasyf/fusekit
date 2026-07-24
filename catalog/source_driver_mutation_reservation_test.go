package catalog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceDriverMutationReservationReplayBindingAndRestart(t *testing.T) {
	store, provisions, identity, request := newSourceDriverMutationReservationFixture(t, 3)
	reservation, err := store.ReserveSourceDriverMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.ReserveSourceDriverMutation(t.Context(), request)
	if err != nil || replayed != reservation {
		t.Fatalf("reserve replay = %+v, %v; want %+v", replayed, err, reservation)
	}
	active, err := store.ActiveSourceDriverMutationReservation(t.Context(), request.Authority)
	if err != nil || active == nil || !reflect.DeepEqual(*active, reservation) {
		t.Fatalf("active reservation = %+v, %v; want %+v", active, err, reservation)
	}
	if _, err := store.BindSourceDriverMutationRequest(
		t.Context(), request.Mutation, request.Claim, identity.MutationRequestDigest,
	); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("bind before targets = %v, want ErrInvalidTransition", err)
	}
	for !reservation.TargetsPrepared {
		reservation, err = store.PrepareSourceDriverMutationReservationBatch(
			t.Context(), request.Mutation, request.Claim,
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	first, err := store.SourceDriverMutationReservationTargets(t.Context(), request.Mutation, "", 2)
	if err != nil || len(first.Targets) != 2 || first.Next != first.Targets[1].Tenant {
		t.Fatalf("first target page = %+v, %v", first, err)
	}
	second, err := store.SourceDriverMutationReservationTargets(t.Context(), request.Mutation, first.Next, 2)
	if err != nil || len(second.Targets) != 1 || second.Next != "" || second.Targets[0].Tenant != provisions[2].Tenant {
		t.Fatalf("second target page = %+v, %v", second, err)
	}
	bound, err := store.BindSourceDriverMutationRequest(
		t.Context(), request.Mutation, request.Claim, identity.MutationRequestDigest,
	)
	if err != nil || !bound.RequestBound || bound.RequestDigest != identity.MutationRequestDigest {
		t.Fatalf("bound reservation = %+v, %v", bound, err)
	}
	replayed, err = store.BindSourceDriverMutationRequest(
		t.Context(), request.Mutation, request.Claim, identity.MutationRequestDigest,
	)
	if err != nil || replayed != bound {
		t.Fatalf("bind replay = %+v, %v; want %+v", replayed, err, bound)
	}
	if _, err := store.BindSourceDriverMutationRequest(
		t.Context(), request.Mutation, request.Claim, [sha256.Size]byte{9},
	); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("different bind = %v, want ErrMutationConflict", err)
	}
	proof := SourceDriverMutationReceiptProof{
		ToToken: identity.ToToken, Result: identity.MutationResult, Digest: identity.MutationReceiptDigest,
	}
	recorded, err := store.RecordSourceDriverMutationReceipt(t.Context(), request.Mutation, request.Claim, proof)
	if err != nil || recorded.Receipt == nil || *recorded.Receipt != proof {
		t.Fatalf("recorded receipt = %+v, %v", recorded, err)
	}

	path := catalogDatabasePathForTest(t, store)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	recovered, err := reopened.SourceDriverMutationReservation(t.Context(), request.Mutation)
	if err != nil || !reflect.DeepEqual(recovered, recorded) {
		t.Fatalf("recovered reservation = %+v, %v; want %+v", recovered, err, recorded)
	}
	active, err = reopened.ActiveSourceDriverMutationReservation(t.Context(), request.Authority)
	if err != nil || active == nil || !reflect.DeepEqual(*active, recorded) {
		t.Fatalf("recovered active reservation = %+v, %v; want %+v", active, err, recorded)
	}
}

func TestSourceDriverMutationReservationEpochChurnReleaseAndAuthorityFence(t *testing.T) {
	store, provisions, _, request := newSourceDriverMutationReservationFixture(t, sourceDriverTargetBatchSize+1)
	reservation, err := store.ReserveSourceDriverMutation(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	reservation, err = store.PrepareSourceDriverMutationReservationBatch(
		t.Context(), request.Mutation, request.Claim,
	)
	if err != nil || reservation.TargetsPrepared || reservation.DeclaredTargetCount != sourceDriverTargetBatchSize {
		t.Fatalf("partial reservation = %+v, %v", reservation, err)
	}

	blocked := provisions[0]
	blocked.Generation++
	if _, err := replaceTenantForTest(t, store, t.Context(), provisions[0].Generation, blocked); !errors.Is(err, ErrMutationActive) {
		t.Fatalf("replace mutation tenant = %v, want ErrMutationActive", err)
	}
	if err := removeTenantForTest(t, store,
		t.Context(), provisions[0].Tenant, provisions[0].Generation,
	); !errors.Is(err, ErrMutationActive) {
		t.Fatalf("remove mutation tenant = %v, want ErrMutationActive", err)
	}

	other := provisions[len(provisions)-1]
	away := other
	away.Generation++
	away.ContentSourceID = "other-authority"
	away, err = replaceTenantForTest(t, store, t.Context(), other.Generation, away)
	if err != nil {
		t.Fatal(err)
	}
	back := away
	back.Generation++
	back.ContentSourceID = string(request.Authority)
	if _, err = replaceTenantForTest(t, store, t.Context(), away.Generation, back); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PrepareSourceDriverMutationReservationBatch(
		t.Context(), request.Mutation, request.Claim,
	); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("prepare after A-B-A = %v, want ErrMutationConflict", err)
	}
	if err := store.ReleaseUnboundSourceDriverMutationReservation(
		t.Context(), request.Mutation, request.Claim, reservation.TargetEpoch+1,
	); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("release wrong epoch = %v, want ErrMutationConflict", err)
	}
	if err := store.ReleaseUnboundSourceDriverMutationReservation(
		t.Context(), request.Mutation, request.Claim, reservation.TargetEpoch,
	); err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseUnboundSourceDriverMutationReservation(
		t.Context(), request.Mutation, request.Claim, reservation.TargetEpoch,
	); err != nil {
		t.Fatalf("release replay: %v", err)
	}
	if _, err := store.SourceDriverMutationReservation(t.Context(), request.Mutation); !errors.Is(err, ErrNotFound) {
		t.Fatalf("released reservation = %v, want ErrNotFound", err)
	}
	replaced, err := store.ReserveSourceDriverMutation(t.Context(), request)
	if err != nil || replaced.TargetEpoch <= reservation.TargetEpoch {
		t.Fatalf("replacement reservation = %+v, %v; old epoch %d", replaced, err, reservation.TargetEpoch)
	}
}

func TestSourceDriverMutationReservationAcceptsCurrentTopologyResetPublication(t *testing.T) {
	store, provisions, identity, _ := newSourceDriverMutationReservationFixture(t, 2)
	old := reserveSourceDriverMutationForTest(t, store, identity)

	other := provisions[1]
	away := other
	away.Generation++
	away.ContentSourceID = "other-authority"
	away, err := replaceTenantForTest(t, store, t.Context(), other.Generation, away)
	if err != nil {
		t.Fatal(err)
	}
	back := away
	back.Generation++
	back.ContentSourceID = string(identity.Authority)
	back, err = replaceTenantForTest(t, store, t.Context(), away.Generation, back)
	if err != nil {
		t.Fatal(err)
	}
	currentTargets := []SourceDriverTarget{
		{Tenant: provisions[0].Tenant, Generation: provisions[0].Generation},
		{Tenant: back.Tenant, Generation: back.Generation},
	}
	currentDigest, err := SourceDriverTargetsDigest(currentTargets)
	if err != nil {
		t.Fatal(err)
	}
	current := identity
	current.SnapshotReason = SourceDriverSnapshotReset
	current.TargetCount = uint64(len(currentTargets))
	current.TargetsDigest = currentDigest
	if err := store.BeginSourceDriverStage(t.Context(), current); err != nil {
		t.Fatalf("BeginSourceDriverStage current topology reset: %v", err)
	}
	var declaration SourceDriverTargetDeclarationState
	for !declaration.Prepared {
		declaration, err = store.PrepareSourceDriverTargetDeclarationBatch(t.Context(), current)
		if err != nil {
			t.Fatalf("PrepareSourceDriverTargetDeclarationBatch: %v", err)
		}
	}
	checkpoint, err := store.SourceDriverCheckpoint(t.Context(), current.Authority)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.TargetEpoch != declaration.TargetEpoch || checkpoint.TargetEpoch <= old.TargetEpoch ||
		checkpoint.TargetCount != current.TargetCount || checkpoint.TargetsDigest != current.TargetsDigest ||
		checkpoint.SnapshotRequired != SourceDriverSnapshotReset {
		t.Fatalf("current topology checkpoint = %+v, declaration = %+v, old reservation = %+v",
			checkpoint, declaration, old)
	}
	recovered, err := store.SourceDriverMutationReservation(t.Context(), identity.Mutation)
	if err != nil || recovered.TargetEpoch != old.TargetEpoch || recovered.TargetsDigest != old.TargetsDigest ||
		recovered.TargetsDigest == current.TargetsDigest {
		t.Fatalf("old external reservation changed = %+v, %v; want %+v", recovered, err, old)
	}
	if err := store.AbortSourceDriverStage(t.Context(), current); err != nil {
		t.Fatalf("abort stale mutation stage: %v", err)
	}
	if pending, err := store.PendingSourceDriverStage(t.Context(), current.Authority); err != nil || pending != nil {
		t.Fatalf("pending stage after abort = %+v, %v", pending, err)
	}
	recovered, err = store.SourceDriverMutationReservation(t.Context(), identity.Mutation)
	if err != nil || !reflect.DeepEqual(recovered, old) {
		t.Fatalf("reservation after stage abort = %+v, %v; want %+v", recovered, err, old)
	}
	active, err := store.ActiveSourceDriverMutationReservation(t.Context(), current.Authority)
	if err != nil || active == nil || !reflect.DeepEqual(*active, old) {
		t.Fatalf("active reservation after stage abort = %+v, %v; want %+v", active, err, old)
	}
	if err := store.BeginSourceDriverStage(t.Context(), current); err != nil {
		t.Fatalf("rebuild current mutation stage with retained IDs: %v", err)
	}
}

func newSourceDriverMutationReservationFixture(
	t *testing.T,
	targetCount int,
) (*Catalog, []TenantProvision, SourceDriverStageIdentity, SourceDriverMutationReservationRequest) {
	t.Helper()
	names := make([]string, targetCount)
	for index := range names {
		names[index] = fmt.Sprintf("reservation-%03d", index)
	}
	store, provisions, declaration, targets := newSourceDriverCatalog(t, names...)
	snapshot := sourceDriverIdentityAtHeadForTest(
		t, store, declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotReset,
		"", "reservation-snapshot", 50,
	)
	if err := store.BeginSourceDriverStage(t.Context(), snapshot); err != nil {
		t.Fatal(err)
	}
	stage, err := store.AppendSourceDriverStage(t.Context(), snapshot, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{Digest: [sha256.Size]byte{50}, Complete: true},
	))
	if err != nil {
		t.Fatal(err)
	}
	prepareSourceDriverPublicationForTest(t, store, snapshot)
	result, err := store.CommitSourceDriverStage(t.Context(), stage)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AcknowledgeSourceDriverCommittedReceipt(t.Context(), result); err != nil {
		t.Fatal(err)
	}
	if err := store.ForgetSourceDriverCommittedReceipt(t.Context(), result); err != nil {
		t.Fatal(err)
	}

	provision := provisions[0]
	head, err := store.Head(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.BeginMutation(t.Context(), provision.Tenant, head, MutationIntent{
		SourceID: "driver", Origin: CausalOrigin{Cause: causal.CauseDaemonWrite},
		Disposition: MutationDispositionNamespace,
		Create: &CreateMutation{Spec: CreateSpec{
			Parent: provision.Root, Name: "reserved", Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	prepared, err = store.ClaimMutation(t.Context(), prepared.OperationID, mustMutationOwner(t))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err = store.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err = store.SetMutationSourceResult(t.Context(), prepared.OperationID, *prepared.Claim, SourceLocator{
		SourceAuthority: "driver-authority", SourceRevision: prepared.Source.Parent.SourceRevision,
		SourceKey: "reserved",
	})
	if err != nil {
		t.Fatal(err)
	}
	identity := sourceDriverIdentityAtHeadForTest(
		t, store, declaration, targets, SourceDriverMutation, 0,
		"reservation-snapshot", "reservation-result", 51,
	)
	identity.Cause = causal.CauseDaemonWrite
	identity.Mutation = prepared.OperationID
	identity.MutationTenant = provision.Tenant
	identity.MutationGeneration = provision.Generation
	identity.MutationResult = "reserved"
	identity.MutationRequestDigest = [sha256.Size]byte{1}
	identity.MutationReceiptDigest = [sha256.Size]byte{2}
	identity.Claim = *prepared.Claim
	request := sourceDriverMutationReservationRequestForIdentity(identity)
	return store, provisions, identity, request
}

func catalogDatabasePathForTest(t *testing.T, store *Catalog) string {
	t.Helper()
	rows, err := store.readDB.QueryContext(t.Context(), "PRAGMA database_list")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var sequence int
		var name, path string
		if err := rows.Scan(&sequence, &name, &path); err != nil {
			t.Fatal(err)
		}
		if name == "main" {
			return path
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Fatal("catalog database path not found")
	return ""
}
