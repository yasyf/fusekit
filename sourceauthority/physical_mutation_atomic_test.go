package sourceauthority

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

type physicalMutationReservationStore struct {
	Store

	prepared         catalog.PreparedMutation
	checkpoint       catalog.SourceDriverCheckpoint
	targetCheckpoint catalog.SourceDriverTargetCheckpoint
	reservation      *catalog.SourceDriverMutationReservation
	reserved         catalog.SourceDriverMutationReservationRequest
	prepareCalls     int
	boundDigest      [sha256.Size]byte
}

func (s *physicalMutationReservationStore) PreparedMutation(
	context.Context,
	catalog.TenantID,
	catalog.MutationID,
) (catalog.PreparedMutation, error) {
	return s.prepared, nil
}

func (s *physicalMutationReservationStore) SourceDriverCheckpoint(
	context.Context,
	causal.SourceAuthorityID,
) (catalog.SourceDriverCheckpoint, error) {
	return s.checkpoint, nil
}

func (s *physicalMutationReservationStore) SourceDriverTargetCheckpoint(
	context.Context,
	causal.SourceAuthorityID,
	catalog.TenantID,
	catalog.Generation,
) (catalog.SourceDriverTargetCheckpoint, error) {
	return s.targetCheckpoint, nil
}

func (s *physicalMutationReservationStore) SourceDriverMutationReservation(
	context.Context,
	catalog.MutationID,
) (catalog.SourceDriverMutationReservation, error) {
	if s.reservation == nil {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrNotFound
	}
	return *s.reservation, nil
}

func (s *physicalMutationReservationStore) ReserveSourceDriverMutation(
	_ context.Context,
	request catalog.SourceDriverMutationReservationRequest,
) (catalog.SourceDriverMutationReservation, error) {
	s.reserved = request
	reservation := catalog.SourceDriverMutationReservation{SourceDriverMutationReservationRequest: request}
	s.reservation = &reservation
	return reservation, nil
}

func (s *physicalMutationReservationStore) PrepareSourceDriverMutationReservationBatch(
	_ context.Context,
	_ catalog.MutationID,
	_ catalog.MutationClaim,
) (catalog.SourceDriverMutationReservation, error) {
	s.prepareCalls++
	reservation := *s.reservation
	reservation.TargetsPrepared = true
	s.reservation = &reservation
	return reservation, nil
}

func (s *physicalMutationReservationStore) BindSourceDriverMutationRequest(
	_ context.Context,
	_ catalog.MutationID,
	_ catalog.MutationClaim,
	digest [sha256.Size]byte,
) (catalog.SourceDriverMutationReservation, error) {
	s.boundDigest = digest
	reservation := *s.reservation
	reservation.RequestBound = true
	reservation.RequestDigest = digest
	s.reservation = &reservation
	return reservation, nil
}

func TestPhysicalMutationReservesExactDriverAttemptBeforeIO(t *testing.T) {
	t.Parallel()

	authority := causal.SourceAuthorityID("authority")
	fleetOwner := catalog.SourceAuthorityFleetOwnerID("owner")
	mutation := catalog.MutationID{1}
	claim := catalog.MutationClaim{Owner: catalog.MutationOwnerID{2}, Epoch: 3}
	target := catalog.SourceDriverTarget{Tenant: "tenant-b", Generation: 7}
	targets := []catalog.SourceDriverTarget{
		{Tenant: "tenant-a", Generation: 5},
		target,
	}
	targetsDigest, err := catalog.SourceDriverTargetsDigest(targets)
	if err != nil {
		t.Fatal(err)
	}
	declarationDigest := sha256.Sum256([]byte("declaration"))
	requestDigest := sha256.Sum256([]byte("request"))
	store := &physicalMutationReservationStore{
		prepared: catalog.PreparedMutation{
			OperationID: mutation,
			Tenant:      target.Tenant,
			State:       catalog.MutationApplying,
			Source:      &catalog.SourceMutationContext{},
			Claim:       &claim,
		},
		checkpoint: catalog.SourceDriverCheckpoint{
			Authority: authority, FleetOwner: fleetOwner, AuthorityGeneration: 11,
			DeclarationDigest: declarationDigest, TargetCount: uint64(len(targets)),
			TargetsDigest: targetsDigest, Token: "source-41", SourceRevision: 41,
		},
		targetCheckpoint: catalog.SourceDriverTargetCheckpoint{
			SourceDriverTarget: target,
			CatalogRevision:    19,
		},
	}
	runtime := &Runtime{
		catalog: store, authority: authority, fleetOwner: fleetOwner, fleetGeneration: 11,
		declarationDigest: declarationDigest,
		tenants: []tenant.TenantSpec{
			{ID: "unrelated", Generation: 1, Content: tenant.ContentSource{ID: "other"}},
			{ID: "tenant-b", Generation: 7, Content: tenant.ContentSource{ID: string(authority)}},
			{ID: "tenant-a", Generation: 5, Content: tenant.ContentSource{ID: string(authority)}},
		},
	}
	step := tenant.SourceMutationStep{
		TenantID: target.Tenant, Generation: target.Generation, OperationID: mutation,
		SourceID: string(authority), ExpectedHead: 19,
	}

	reservation, err := runtime.reservePhysicalMutation(context.Background(), step,
		catalog.SourceMutationExpectationRecord{Digest: requestDigest})
	if err != nil {
		t.Fatal(err)
	}
	if !reservation.TargetsPrepared || !reservation.RequestBound || reservation.RequestDigest != requestDigest {
		t.Fatalf("reservation is incomplete: %+v", reservation)
	}
	if store.prepareCalls != 1 || store.boundDigest != requestDigest {
		t.Fatalf("prepare calls = %d, bound digest = %x", store.prepareCalls, store.boundDigest)
	}
	wantRequest := catalog.SourceDriverMutationReservationRequest{
		Mutation: mutation, Claim: claim, Authority: authority, FleetOwner: fleetOwner,
		AuthorityGeneration: 11, DeclarationDigest: declarationDigest,
		TargetCount: uint64(len(targets)), TargetsDigest: targetsDigest, Target: target,
		FromToken: "source-41", Predecessor: 41,
		Operation: store.reserved.Operation, SourceOperation: store.reserved.SourceOperation,
		ChangeID: store.reserved.ChangeID,
	}
	if store.reserved.Operation == (causal.OperationID{}) ||
		store.reserved.SourceOperation == (causal.OperationID{}) || store.reserved.ChangeID == (causal.ChangeID{}) {
		t.Fatalf("reservation causal identity is incomplete: %+v", store.reserved)
	}
	if !reflect.DeepEqual(store.reserved, wantRequest) {
		t.Fatalf("reserved request = %+v, want %+v", store.reserved, wantRequest)
	}
}

type physicalMutationSettlementStore struct {
	Store
	calls *[]string
}

func (s *physicalMutationSettlementStore) AcknowledgeSourceDriverCommittedReceipt(
	context.Context,
	catalog.SourceDriverStageResult,
) error {
	*s.calls = append(*s.calls, "catalog-ack")
	return nil
}

func (s *physicalMutationSettlementStore) ForgetSourceDriverCommittedReceipt(
	context.Context,
	catalog.SourceDriverStageResult,
) error {
	*s.calls = append(*s.calls, "catalog-forget")
	return nil
}

type physicalMutationSettlementExecutor struct {
	Executor
	calls *[]string
	err   error
}

func (e *physicalMutationSettlementExecutor) AcknowledgeMutation(
	context.Context,
	causal.SourceAuthorityID,
	causal.Generation,
	catalog.MutationID,
	Fingerprint,
) error {
	*e.calls = append(*e.calls, "worker-ack")
	return e.err
}

func TestPhysicalMutationSettlesWorkerBeforeCatalogReceipt(t *testing.T) {
	t.Parallel()

	authority := causal.SourceAuthorityID("authority")
	mutation := catalog.MutationID{1}
	digest := sha256.Sum256([]byte("receipt"))
	receipt := catalog.SourceDriverCommittedReceipt{Result: catalog.SourceDriverStageResult{
		Identity: catalog.SourceDriverStageIdentity{
			Mode: catalog.SourceDriverMutation, Authority: authority, AuthorityGeneration: 4,
			Mutation: mutation, MutationReceiptDigest: digest,
		},
	}}

	t.Run("success", func(t *testing.T) {
		var calls []string
		runtime := &Runtime{
			catalog:   &physicalMutationSettlementStore{calls: &calls},
			executor:  &physicalMutationSettlementExecutor{calls: &calls},
			authority: authority, fleetGeneration: 4,
		}
		if err := runtime.settleCommittedPhysicalMutation(context.Background(), receipt); err != nil {
			t.Fatal(err)
		}
		want := []string{"worker-ack", "catalog-ack", "catalog-forget"}
		if !reflect.DeepEqual(calls, want) {
			t.Fatalf("settlement order = %v, want %v", calls, want)
		}
	})

	t.Run("worker failure retains catalog receipt", func(t *testing.T) {
		var calls []string
		workerErr := errors.New("worker unavailable")
		runtime := &Runtime{
			catalog:   &physicalMutationSettlementStore{calls: &calls},
			executor:  &physicalMutationSettlementExecutor{calls: &calls, err: workerErr},
			authority: authority, fleetGeneration: 4,
		}
		if err := runtime.settleCommittedPhysicalMutation(context.Background(), receipt); !errors.Is(err, workerErr) {
			t.Fatalf("settlement error = %v, want %v", err, workerErr)
		}
		if want := []string{"worker-ack"}; !reflect.DeepEqual(calls, want) {
			t.Fatalf("settlement order = %v, want %v", calls, want)
		}
	})
}

func TestMergedPhysicalPublicationAdvancesToTerminalRevision(t *testing.T) {
	t.Parallel()

	runtime := &Runtime{authority: "authority"}
	publications := []sourcePublication{
		{
			Mode: catalog.SourceDelta, Predecessor: 10,
			Change: causal.ChangeSet{
				SourceAuthority: "authority", SourceRevision: 11,
				ChangeID: causal.ChangeID{1}, OperationID: causal.OperationID{1},
				Cause: causal.CauseExternalUnattributed, AffectedKeys: []causal.LogicalKey{"a"},
			},
			Tenants: []catalog.SourceTenant{{
				Tenant: "tenant", Generation: 1, RootKey: "root",
				Objects: []catalog.SourceObject{{Key: "object", Kind: catalog.KindFile, ContentRevision: 11}},
			}},
		},
		{
			Mode: catalog.SourceDelta, Predecessor: 11,
			Change: causal.ChangeSet{
				SourceAuthority: "authority", SourceRevision: 12,
				ChangeID: causal.ChangeID{2}, OperationID: causal.OperationID{2},
				Cause: causal.CauseExternalUnattributed, AffectedKeys: []causal.LogicalKey{"b"},
			},
			Tenants: []catalog.SourceTenant{{
				Tenant: "tenant", Generation: 1, RootKey: "root",
				Objects: []catalog.SourceObject{{Key: "object", Kind: catalog.KindFile, ContentRevision: 12}},
			}},
		},
	}

	merged, err := runtime.mergeSourcePublications(context.Background(), publications)
	if err != nil {
		t.Fatal(err)
	}
	if merged.Change.SourceRevision != 12 {
		t.Fatalf("merged revision = %d, want 12", merged.Change.SourceRevision)
	}
	if len(merged.Tenants) != 1 || len(merged.Tenants[0].Objects) != 1 ||
		merged.Tenants[0].Objects[0].ContentRevision != 12 {
		t.Fatalf("merged tenant projection = %+v", merged.Tenants)
	}
}
