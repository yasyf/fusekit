package sourceauthority

import (
	"context"
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

type physicalStageRecoveryStore struct {
	Store
	pending *catalog.SourceDriverStageState
	receipt *catalog.SourceDriverCommittedReceipt
	calls   []string
	aborted catalog.SourceDriverStageIdentity
}

func (s *physicalStageRecoveryStore) PendingSourceDriverStage(
	context.Context,
	causal.SourceAuthorityID,
) (*catalog.SourceDriverStageState, error) {
	s.calls = append(s.calls, "pending-stage")
	if s.pending == nil {
		return nil, nil
	}
	copy := *s.pending
	return &copy, nil
}

func (s *physicalStageRecoveryStore) AbortSourceDriverStage(
	_ context.Context,
	identity catalog.SourceDriverStageIdentity,
) error {
	s.calls = append(s.calls, "abort-release")
	s.aborted = identity
	s.pending = nil
	return nil
}

func (s *physicalStageRecoveryStore) PrepareSourceDriverPublicationBatch(
	context.Context,
	catalog.SourceDriverStageIdentity,
) (catalog.SourceDriverPreparationState, error) {
	s.calls = append(s.calls, "prepare")
	return catalog.SourceDriverPreparationState{Prepared: true}, nil
}

func (s *physicalStageRecoveryStore) CommitSourceDriverStage(
	_ context.Context,
	state catalog.SourceDriverStageState,
) (catalog.SourceDriverStageResult, error) {
	s.calls = append(s.calls, "commit")
	s.pending = nil
	return catalog.SourceDriverStageResult{Identity: state.Identity, Proof: state.Stage}, nil
}

func (s *physicalStageRecoveryStore) PendingSourceDriverCommittedReceipt(
	context.Context,
	causal.SourceAuthorityID,
) (*catalog.SourceDriverCommittedReceipt, error) {
	s.calls = append(s.calls, "pending-receipt")
	if s.receipt == nil {
		return nil, nil
	}
	copy := *s.receipt
	return &copy, nil
}

func (s *physicalStageRecoveryStore) AcknowledgeSourceDriverCommittedReceipt(
	_ context.Context,
	result catalog.SourceDriverStageResult,
) error {
	s.calls = append(s.calls, "catalog-ack")
	if s.receipt != nil && s.receipt.Result.Identity.Operation == result.Identity.Operation {
		s.receipt.Acknowledged = true
	}
	return nil
}

func (s *physicalStageRecoveryStore) ForgetSourceDriverCommittedReceipt(
	_ context.Context,
	result catalog.SourceDriverStageResult,
) error {
	s.calls = append(s.calls, "catalog-forget")
	if s.receipt != nil && s.receipt.Result.Identity.Operation == result.Identity.Operation {
		s.receipt = nil
	}
	return nil
}

func TestRecoverPendingPhysicalDriverStageCrashBoundaries(t *testing.T) {
	t.Parallel()
	authority := causal.SourceAuthorityID("authority")
	identity := catalog.SourceDriverStageIdentity{
		Authority: authority, Operation: causal.OperationID{1}, Mode: catalog.SourceDriverDelta,
	}

	t.Run("partial append aborts through content release boundary", func(t *testing.T) {
		state := catalog.SourceDriverStageState{
			Identity: identity,
			Stage:    catalog.SourcePublicationStageRef{Sequence: 1},
			Cursor:   []byte("next-page"),
		}
		store := &physicalStageRecoveryStore{pending: &state}
		runtime := &Runtime{catalog: store, authority: authority}
		if err := runtime.recoverPendingPhysicalDriverStage(context.Background()); err != nil {
			t.Fatal(err)
		}
		if store.aborted != identity {
			t.Fatalf("aborted identity = %+v, want %+v", store.aborted, identity)
		}
		if want := []string{"pending-stage", "abort-release"}; !reflect.DeepEqual(store.calls, want) {
			t.Fatalf("recovery calls = %v, want %v", store.calls, want)
		}
	})

	t.Run("complete append finishes before receipt recovery", func(t *testing.T) {
		state := catalog.SourceDriverStageState{
			Identity: identity,
			Stage:    catalog.SourcePublicationStageRef{Sequence: 1},
		}
		store := &physicalStageRecoveryStore{pending: &state}
		runtime := &Runtime{catalog: store, authority: authority}
		if err := runtime.recoverPendingPhysicalDriverStage(context.Background()); err != nil {
			t.Fatal(err)
		}
		want := []string{
			"pending-stage", "prepare", "commit", "catalog-ack", "catalog-forget", "pending-receipt",
		}
		if !reflect.DeepEqual(store.calls, want) {
			t.Fatalf("recovery calls = %v, want %v", store.calls, want)
		}
	})
}

func TestRecoverCommittedPhysicalDriverReceiptAfterCommitBeforeAck(t *testing.T) {
	t.Parallel()
	authority := causal.SourceAuthorityID("authority")
	digest := sha256.Sum256([]byte("worker receipt"))
	result := catalog.SourceDriverStageResult{Identity: catalog.SourceDriverStageIdentity{
		Authority: authority, AuthorityGeneration: 7, Operation: causal.OperationID{2},
		Mode: catalog.SourceDriverMutation, Mutation: catalog.MutationID{3},
		MutationReceiptDigest: digest,
	}}
	store := &physicalStageRecoveryStore{
		receipt: &catalog.SourceDriverCommittedReceipt{Result: result},
	}
	runtime := &Runtime{
		catalog: store, authority: authority, fleetGeneration: 7,
		executor: &physicalMutationSettlementExecutor{calls: &store.calls},
	}
	if err := runtime.recoverCommittedSourceDriverReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"pending-receipt", "worker-ack", "catalog-ack", "catalog-forget", "pending-receipt",
	}
	if !reflect.DeepEqual(store.calls, want) {
		t.Fatalf("recovery calls = %v, want %v", store.calls, want)
	}
}
