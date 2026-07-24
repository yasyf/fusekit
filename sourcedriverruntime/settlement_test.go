package sourcedriverruntime

import (
	"errors"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourcedriver"
)

func TestMutationReceiptFromStageAcceptsExactResultArms(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		result *catalog.SourceDriverMutationResult
	}{
		{
			name: "private",
			result: &catalog.SourceDriverMutationResult{
				Kind: catalog.SourceDriverMutationPrivate,
				Private: &catalog.PrivateMutationResult{
					Mutation: catalog.MutationID{1}, Tenant: "tenant", Generation: 4,
					ObjectID: catalog.ObjectID{3}, SourceAuthority: "authority", SourceKey: "item",
					SourceOperation: causal.OperationID{2}, SourceRevision: 9,
				},
			},
		},
		{
			name: "namespace",
			result: &catalog.SourceDriverMutationResult{
				Kind: catalog.SourceDriverMutationNamespace,
				Namespace: &catalog.NamespaceMutationResult{Mutation: catalog.MutationRecord{
					ID: catalog.MutationID{1}, Tenant: "tenant",
				}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stage := validMutationStageResult(t, test.result)
			receipt, err := mutationReceiptFromStage(stage)
			if err != nil {
				t.Fatal(err)
			}
			if receipt.Result != "item" || receipt.OperationID != (catalog.MutationID{1}) ||
				receipt.Digest != stage.Identity.MutationReceiptDigest {
				t.Fatalf("mutation receipt = %+v", receipt)
			}
		})
	}
}

func TestMutationReceiptFromStageRejectsMalformedResultArms(t *testing.T) {
	t.Parallel()

	validPrivate := &catalog.PrivateMutationResult{
		Mutation: catalog.MutationID{1}, Tenant: "tenant", Generation: 4,
		ObjectID: catalog.ObjectID{3}, SourceAuthority: "authority", SourceKey: "item",
		SourceOperation: causal.OperationID{2}, SourceRevision: 9,
	}
	validNamespace := &catalog.NamespaceMutationResult{Mutation: catalog.MutationRecord{
		ID: catalog.MutationID{1}, Tenant: "tenant",
	}}
	for _, test := range []struct {
		name   string
		result *catalog.SourceDriverMutationResult
		want   error
	}{
		{name: "missing", want: catalog.ErrInvalidTransition},
		{name: "both arms", result: &catalog.SourceDriverMutationResult{
			Kind: catalog.SourceDriverMutationPrivate, Private: validPrivate, Namespace: validNamespace,
		}, want: catalog.ErrInvalidTransition},
		{name: "wrong private tag", result: &catalog.SourceDriverMutationResult{
			Kind: catalog.SourceDriverMutationNamespace, Private: validPrivate,
		}, want: catalog.ErrInvalidTransition},
		{name: "wrong private identity", result: &catalog.SourceDriverMutationResult{
			Kind: catalog.SourceDriverMutationPrivate, Private: &catalog.PrivateMutationResult{
				Mutation: catalog.MutationID{1}, Tenant: "tenant", Generation: 4,
				ObjectID: catalog.ObjectID{3}, SourceAuthority: "authority", SourceKey: "other",
				SourceOperation: causal.OperationID{2}, SourceRevision: 9,
			},
		}, want: catalog.ErrMutationConflict},
		{name: "wrong namespace identity", result: &catalog.SourceDriverMutationResult{
			Kind: catalog.SourceDriverMutationNamespace,
			Namespace: &catalog.NamespaceMutationResult{Mutation: catalog.MutationRecord{
				ID: catalog.MutationID{9}, Tenant: "tenant",
			}},
		}, want: catalog.ErrMutationConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			stage := validMutationStageResult(t, test.result)
			if _, err := mutationReceiptFromStage(stage); !errors.Is(err, test.want) {
				t.Fatalf("mutationReceiptFromStage = %v, want %v", err, test.want)
			}
		})
	}
}

func validMutationStageResult(t *testing.T, result *catalog.SourceDriverMutationResult) catalog.SourceDriverStageResult {
	t.Helper()
	identity := catalog.SourceDriverStageIdentity{
		Mode: catalog.SourceDriverMutation, Authority: "authority",
		Mutation: catalog.MutationID{1}, MutationTenant: "tenant", MutationGeneration: 4,
		MutationResult: "item", SourceOperation: causal.OperationID{2},
		FromToken: "head-1", ToToken: "head-2", MutationRequestDigest: [32]byte{4},
	}
	receipt := sourcedriver.MutationReceipt{
		OperationID: identity.Mutation, State: sourcedriver.MutationApplied,
		RequestDigest: identity.MutationRequestDigest, Expected: "head-1", Committed: "head-2", Result: "item",
	}
	digest, err := sourcedriver.MutationReceiptDigest(receipt)
	if err != nil {
		t.Fatal(err)
	}
	identity.MutationReceiptDigest = digest
	return catalog.SourceDriverStageResult{
		Identity: identity, Checkpoint: catalog.SourceDriverCheckpoint{SourceRevision: 9}, MutationResult: result,
	}
}
