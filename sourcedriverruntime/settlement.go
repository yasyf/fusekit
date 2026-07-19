package sourcedriverruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/sourcedriver"
)

func (r *Runtime) recoverCommittedReceipts(ctx context.Context) error {
	for {
		receipt, err := r.config.Store.PendingSourceDriverCommittedReceipt(ctx, r.config.Authority)
		if err != nil {
			return err
		}
		if receipt == nil {
			return nil
		}
		if err := r.settleCommittedReceipt(ctx, *receipt); err != nil {
			return err
		}
	}
}

func (r *Runtime) settleCommittedMutation(
	ctx context.Context,
	mutation catalog.MutationID,
) (MutationResult, error) {
	receipt, err := r.config.Store.CommittedSourceDriverMutation(ctx, r.config.Authority, mutation)
	if err != nil {
		return MutationResult{}, err
	}
	if receipt == nil {
		return MutationResult{}, catalog.ErrNotFound
	}
	if receipt.Result.Identity.Mode != catalog.SourceDriverMutation ||
		receipt.Result.Identity.Mutation != mutation || receipt.Result.MutationResult == nil {
		return MutationResult{}, catalog.ErrIntegrity
	}
	if !receipt.Acknowledged || !receipt.Forgotten {
		if err := r.settleCommittedReceipt(ctx, *receipt); err != nil {
			return MutationResult{}, err
		}
	}
	sourceReceipt, err := mutationReceiptFromStage(receipt.Result)
	if err != nil {
		return MutationResult{}, err
	}
	return MutationResult{Receipt: sourceReceipt, Stage: receipt.Result}, nil
}

func (r *Runtime) settleCommittedResult(ctx context.Context, result catalog.SourceDriverStageResult) error {
	return r.settleCommittedReceipt(ctx, catalog.SourceDriverCommittedReceipt{Result: result})
}

func (r *Runtime) settleCommittedReceipt(ctx context.Context, receipt catalog.SourceDriverCommittedReceipt) error {
	result := receipt.Result
	if result.Identity.Authority != r.config.Authority {
		return catalog.ErrGenerationMismatch
	}
	var targetSet sourcedriver.TargetSetRef
	if result.Identity.Mode == catalog.SourceDriverMutation && (!receipt.Acknowledged || !receipt.Forgotten) {
		reservation, err := r.config.Store.SourceDriverMutationReservation(ctx, result.Identity.Mutation)
		if err != nil {
			return err
		}
		if err := validateSettlementReservation(result.Identity, reservation); err != nil {
			return err
		}
		targetSet, err = mutationTargetSetRef(reservation)
		if err != nil {
			return err
		}
	}
	if result.Identity.Mode == catalog.SourceDriverMutation && !receipt.Acknowledged {
		settlement := sourcedriver.MutationSettlement{
			TargetSet: targetSet, OperationID: result.Identity.Mutation,
			RequestDigest: result.Identity.MutationRequestDigest,
			ReceiptDigest: result.Identity.MutationReceiptDigest,
			Kind:          sourcedriver.MutationSettlementAcknowledge,
		}
		if err := r.config.Driver.SettleMutation(ctx, r.config.Authority, settlement); err != nil {
			return err
		}
	}
	if !receipt.Acknowledged {
		if err := r.config.Store.AcknowledgeSourceDriverCommittedReceipt(ctx, result); err != nil {
			return err
		}
	}
	if result.Identity.Mode == catalog.SourceDriverMutation && !receipt.Forgotten {
		settlement := sourcedriver.MutationSettlement{
			TargetSet: targetSet, OperationID: result.Identity.Mutation,
			RequestDigest: result.Identity.MutationRequestDigest,
			ReceiptDigest: result.Identity.MutationReceiptDigest,
			Kind:          sourcedriver.MutationSettlementForget,
		}
		if err := r.config.Driver.SettleMutation(ctx, r.config.Authority, settlement); err != nil {
			return err
		}
	}
	if !receipt.Forgotten {
		return r.config.Store.ForgetSourceDriverCommittedReceipt(ctx, result)
	}
	return nil
}

func validateSettlementReservation(
	identity catalog.SourceDriverStageIdentity,
	reservation catalog.SourceDriverMutationReservation,
) error {
	if reservation.Receipt == nil || !reservation.RequestBound ||
		reservation.Mutation != identity.Mutation || reservation.Claim != identity.Claim ||
		reservation.Authority != identity.Authority ||
		reservation.Target != (catalog.SourceDriverTarget{
			Tenant: identity.MutationTenant, Generation: identity.MutationGeneration,
		}) || reservation.FromToken != identity.FromToken ||
		reservation.Operation != identity.Operation || reservation.SourceOperation != identity.SourceOperation ||
		reservation.ChangeID != identity.ChangeID || reservation.RequestDigest != identity.MutationRequestDigest ||
		reservation.Receipt.ToToken != identity.ToToken || reservation.Receipt.Result != identity.MutationResult ||
		reservation.Receipt.Digest != identity.MutationReceiptDigest {
		return catalog.ErrMutationConflict
	}
	return nil
}

func mutationReceiptFromStage(result catalog.SourceDriverStageResult) (sourcedriver.MutationReceipt, error) {
	identity := result.Identity
	if identity.Mode != catalog.SourceDriverMutation || result.MutationResult == nil {
		return sourcedriver.MutationReceipt{}, catalog.ErrInvalidTransition
	}
	receipt := sourcedriver.MutationReceipt{
		OperationID: identity.Mutation, State: sourcedriver.MutationApplied,
		RequestDigest: identity.MutationRequestDigest,
		Expected:      sourcedriver.RevisionToken(identity.FromToken),
		Committed:     sourcedriver.RevisionToken(identity.ToToken),
		Result:        sourcedriver.LogicalID(identity.MutationResult),
		Digest:        identity.MutationReceiptDigest,
	}
	if err := sourcedriver.ValidateMutationReceipt(receipt); err != nil {
		return sourcedriver.MutationReceipt{}, errors.Join(
			fmt.Errorf("source driver runtime: committed receipt is invalid"), err,
		)
	}
	return receipt, nil
}
