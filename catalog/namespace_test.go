package catalog

import (
	"context"
	"errors"

	"github.com/yasyf/fusekit/causal"
)

func (c *Catalog) Create(ctx context.Context, mutation MutationID, tenant TenantID, spec CreateSpec) (Object, error) {
	result, err := c.testNamespaceMutation(ctx, mutation, tenant, MutationIntent{
		SourceID: "test", Create: &CreateMutation{Spec: spec},
	})
	return result.Primary, err
}

func (c *Catalog) Revise(ctx context.Context, mutation MutationID, tenant TenantID, id ObjectID, spec RevisionSpec) (Object, error) {
	result, err := c.testNamespaceMutation(ctx, mutation, tenant, MutationIntent{
		SourceID: "test", Revise: &ReviseMutation{Object: id, Spec: spec},
	})
	return result.Primary, err
}

func (c *Catalog) Delete(ctx context.Context, mutation MutationID, tenant TenantID, id ObjectID) (Object, error) {
	result, err := c.testNamespaceMutation(ctx, mutation, tenant, MutationIntent{
		SourceID: "test", Delete: &DeleteMutation{Object: id},
	})
	return result.Primary, err
}

func (c *Catalog) Replace(ctx context.Context, mutation MutationID, tenant TenantID, source, target ObjectID) (ReplaceResult, error) {
	result, err := c.testNamespaceMutation(ctx, mutation, tenant, MutationIntent{
		SourceID: "test", Replace: &ReplaceMutation{Source: source, Target: target},
	})
	if err != nil {
		return ReplaceResult{}, err
	}
	return ReplaceResult{
		Revision: result.Mutation.Revision, Source: result.Primary, Target: *result.Secondary,
	}, nil
}

func (c *Catalog) testNamespaceMutation(
	ctx context.Context,
	mutation MutationID,
	tenant TenantID,
	intent MutationIntent,
) (NamespaceMutationResult, error) {
	if intent.Origin.Cause == "" && intent.Origin.Change == nil {
		intent.Origin = testCausalOrigin()
	}
	head, err := c.Head(ctx, tenant)
	if existing, existingErr := c.PreparedMutation(ctx, mutation); existingErr == nil {
		head = existing.ExpectedHead
	} else if !errors.Is(existingErr, ErrNotFound) {
		return NamespaceMutationResult{}, existingErr
	}
	for {
		if err != nil {
			return NamespaceMutationResult{}, err
		}
		prepared, beginErr := c.BeginMutation(ctx, mutation, tenant, head, intent)
		if !errors.Is(beginErr, errMutationHeadChanged) {
			if beginErr != nil {
				return NamespaceMutationResult{}, beginErr
			}
			return c.finishTestNamespaceMutation(ctx, mutation, prepared)
		}
		head, err = c.Head(ctx, tenant)
	}
}

func testCausalOrigin() CausalOrigin {
	return CausalOrigin{Cause: causal.CauseDaemonWrite}
}

func (c *Catalog) finishTestNamespaceMutation(ctx context.Context, mutation MutationID, prepared PreparedMutation) (NamespaceMutationResult, error) {
	switch prepared.State {
	case MutationApplied, MutationCommitted:
		return c.CommitMutation(ctx, mutation)
	case MutationPrepared:
	default:
		return NamespaceMutationResult{}, ErrInvalidTransition
	}
	owner, err := NewMutationOwnerID()
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	claimed, err := c.ClaimMutation(ctx, mutation, owner)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	if _, err := c.MarkMutationApplied(ctx, mutation, *claimed.Claim); err != nil {
		return NamespaceMutationResult{}, err
	}
	return c.CommitMutation(ctx, mutation)
}
