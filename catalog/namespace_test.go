package catalog

import (
	"context"

	"github.com/yasyf/fusekit/causal"
)

func (c *Catalog) Create(ctx context.Context, tenant TenantID, spec CreateSpec) (Object, error) {
	result, err := c.testNamespaceMutation(ctx, tenant, MutationIntent{
		SourceID: "test", Create: &CreateMutation{Spec: spec},
	})
	return result.Primary, err
}

func (c *Catalog) Revise(ctx context.Context, tenant TenantID, id ObjectID, spec RevisionSpec) (Object, error) {
	result, err := c.testNamespaceMutation(ctx, tenant, MutationIntent{
		SourceID: "test", Revise: &ReviseMutation{Object: id, Spec: spec},
	})
	return result.Primary, err
}

func (c *Catalog) Delete(ctx context.Context, tenant TenantID, id ObjectID) (Object, error) {
	result, err := c.testNamespaceMutation(ctx, tenant, MutationIntent{
		SourceID: "test", Delete: &DeleteMutation{Object: id},
	})
	return result.Primary, err
}

func (c *Catalog) Replace(ctx context.Context, tenant TenantID, source, target ObjectID) (ReplaceResult, error) {
	result, err := c.testNamespaceMutation(ctx, tenant, MutationIntent{
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
	tenant TenantID,
	intent MutationIntent,
) (NamespaceMutationResult, error) {
	if intent.Origin.Cause == "" {
		intent.Origin = testCausalOrigin()
	}
	head, err := c.Head(ctx, tenant)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	kind, err := validateMutationIntent(intent)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	_, digest, err := encodeMutationIntent(tenant, head, kind, intent)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	binding := MutationBinding{
		Tenant: tenant, Target: head + 1, Issuer: intent.SourceID,
		RequestDigest: MutationRequestDigest(digest),
	}
	id, err := deriveMutationID(binding)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	if err := c.claimIntentContent(ctx, tx, id, tenant, intent); err != nil {
		_ = tx.Rollback()
		return NamespaceMutationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return NamespaceMutationResult{}, err
	}
	var primary Object
	var secondary *Object
	switch kind {
	case MutationCreate:
		primary, err = c.create(ctx, id, binding, tenant, head, intent.Create.Spec, intent.Origin)
	case MutationRevise:
		primary, err = c.revise(ctx, id, binding, tenant, head, intent.Revise.Object, intent.Revise.Spec, intent.Origin)
	case MutationDelete:
		primary, err = c.delete(ctx, id, binding, tenant, head, intent.Delete.Object, intent.Origin)
	case MutationReplace:
		var replaced ReplaceResult
		replaced, err = c.replace(ctx, id, binding, tenant, head, *intent.Replace, intent.Origin)
		primary, secondary = replaced.Source, &replaced.Target
	default:
		return NamespaceMutationResult{}, ErrInvalidTransition
	}
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	mutation, err := c.Mutation(ctx, tenant, id)
	return NamespaceMutationResult{Mutation: mutation, Primary: primary, Secondary: secondary}, err
}

func testCausalOrigin() CausalOrigin {
	return CausalOrigin{Cause: causal.CauseDaemonWrite}
}

func (c *Catalog) finishTestNamespaceMutation(ctx context.Context, prepared PreparedMutation) (NamespaceMutationResult, error) {
	if prepared.State == MutationCommitted {
		mutation, err := c.Mutation(ctx, prepared.Tenant, prepared.OperationID)
		if err != nil {
			return NamespaceMutationResult{}, err
		}
		primary, err := c.objectAt(ctx, prepared.Tenant, ObjectID(mutation.Primary), mutation.Revision)
		if err != nil {
			return NamespaceMutationResult{}, err
		}
		result := NamespaceMutationResult{Mutation: mutation, Primary: primary}
		if mutation.Secondary != (ObjectID{}) {
			secondary, err := c.objectAt(ctx, prepared.Tenant, ObjectID(mutation.Secondary), mutation.Revision)
			if err != nil {
				return NamespaceMutationResult{}, err
			}
			result.Secondary = &secondary
		}
		return result, nil
	}
	if prepared.State == MutationPrepared {
		owner, err := NewMutationOwnerID()
		if err != nil {
			return NamespaceMutationResult{}, err
		}
		prepared, err = c.ClaimMutation(ctx, prepared.OperationID, owner)
		if err != nil {
			return NamespaceMutationResult{}, err
		}
	}
	if prepared.State != MutationApplying {
		return NamespaceMutationResult{}, ErrInvalidTransition
	}
	_, digest, err := encodeMutationIntent(prepared.Tenant, prepared.ExpectedHead, prepared.Kind, prepared.Intent)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	binding := MutationBinding{
		Tenant: prepared.Tenant, Target: prepared.ExpectedHead + 1, Issuer: prepared.Intent.SourceID,
		RequestDigest: MutationRequestDigest(digest),
	}
	var primary Object
	var secondary *Object
	switch prepared.Kind {
	case MutationCreate:
		primary, err = c.create(ctx, prepared.OperationID, binding, prepared.Tenant, prepared.ExpectedHead, prepared.Intent.Create.Spec, prepared.Intent.Origin)
	case MutationRevise:
		primary, err = c.revise(ctx, prepared.OperationID, binding, prepared.Tenant, prepared.ExpectedHead, prepared.Intent.Revise.Object, prepared.Intent.Revise.Spec, prepared.Intent.Origin)
	case MutationDelete:
		primary, err = c.delete(ctx, prepared.OperationID, binding, prepared.Tenant, prepared.ExpectedHead, prepared.Intent.Delete.Object, prepared.Intent.Origin)
	case MutationReplace:
		var replaced ReplaceResult
		replaced, err = c.replace(ctx, prepared.OperationID, binding, prepared.Tenant, prepared.ExpectedHead, *prepared.Intent.Replace, prepared.Intent.Origin)
		primary, secondary = replaced.Source, &replaced.Target
	default:
		return NamespaceMutationResult{}, ErrInvalidTransition
	}
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	if _, err := c.db.ExecContext(ctx, `UPDATE prepared_mutations SET state = ? WHERE mutation_id = ? AND state = ?`,
		uint8(MutationCommitted), prepared.OperationID[:], uint8(MutationApplying)); err != nil {
		return NamespaceMutationResult{}, err
	}
	mutation, err := c.Mutation(ctx, prepared.Tenant, prepared.OperationID)
	return NamespaceMutationResult{Mutation: mutation, Primary: primary, Secondary: secondary}, err
}
