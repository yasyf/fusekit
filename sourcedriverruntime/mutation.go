package sourcedriverruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
)

const mutationInspectTimeout = 5 * time.Second

var errMutationAlreadyApplied = errors.New("source driver runtime: mutation already applied")

func (r *Runtime) applyPreparedMutation(
	ctx context.Context,
	prepared catalog.PreparedMutation,
	content contentstream.Source,
) (MutationResult, error) {
	committed, err := r.config.Store.CommittedSourceDriverMutation(ctx, r.config.Authority, prepared.OperationID)
	if err != nil {
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	if committed != nil {
		identity := committed.Result.Identity
		if identity.MutationTenant != prepared.Tenant || committed.Result.MutationResult == nil {
			err := catalog.ErrMutationConflict
			return MutationResult{}, errors.Join(err, settleUnused(content, err))
		}
		if err := settleUnused(content, errMutationAlreadyApplied); err != nil {
			return MutationResult{}, err
		}
		if !committed.Acknowledged || !committed.Forgotten {
			if err := r.settleCommittedReceipt(ctx, *committed); err != nil {
				return MutationResult{}, err
			}
		}
		receipt, err := mutationReceiptFromStage(committed.Result)
		if err != nil {
			return MutationResult{}, err
		}
		return MutationResult{Receipt: receipt, Stage: committed.Result}, nil
	}
	recovered, err := r.recoverPendingForPreparedMutation(ctx, &prepared.OperationID, &prepared)
	if err != nil {
		if errors.Is(err, errProgressPending) {
			return MutationResult{}, err
		}
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	if recovered != nil {
		receipt, receiptErr := mutationReceiptFromStage(*recovered)
		if receiptErr != nil {
			return MutationResult{}, receiptErr
		}
		return MutationResult{Receipt: receipt, Stage: *recovered}, nil
	}
	if err := r.validatePreparedMutation(prepared); err != nil {
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	request, hasContent, err := mutationRequestFor(prepared)
	if err != nil {
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	if hasContent != (content != nil) {
		err := fmt.Errorf("%w: mutation content ownership differs", sourcedriver.ErrInvalidValue)
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}

	reservation, err := r.reservePreparedMutation(ctx, prepared)
	if err != nil {
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	if !reservation.TargetsPrepared {
		_, err = r.config.Store.PrepareSourceDriverMutationReservationBatch(
			ctx, reservation.Mutation, reservation.Claim,
		)
		if err == nil {
			return MutationResult{}, errProgressPending
		}
		if !reservation.RequestBound && reservation.Receipt == nil &&
			errors.Is(err, catalog.ErrGenerationMismatch) {
			if releaseErr := r.releaseUnboundReservation(ctx, reservation); releaseErr != nil {
				return MutationResult{}, errors.Join(err, releaseErr, settleUnused(content, err))
			}
			if _, reserveErr := r.reservePreparedMutation(ctx, prepared); reserveErr != nil {
				return MutationResult{}, errors.Join(reserveErr, settleUnused(content, reserveErr))
			}
			return MutationResult{}, errProgressPending
		}
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	request, err = mutationRequestFromReservation(prepared, request, hasContent, reservation)
	if err != nil {
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	wasBound := reservation.RequestBound
	reservation, err = r.bindMutationRequest(ctx, reservation, requestDigest)
	if err != nil {
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	if !wasBound {
		return MutationResult{}, errProgressPending
	}
	_, targetSetComplete, err := r.ensureMutationTargetSet(ctx, reservation)
	if err != nil {
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	if !targetSetComplete {
		return MutationResult{}, errProgressPending
	}

	var receipt sourcedriver.MutationReceipt
	if reservation.Receipt != nil {
		if err := abortUnused(content, errMutationAlreadyApplied); err != nil {
			return MutationResult{}, err
		}
		receipt, err = mutationReceiptFromReservation(reservation)
	} else {
		receipt, err = r.inspectOrApply(ctx, request, content)
		if err == nil {
			reservation, err = r.recordMutationReceipt(ctx, prepared, reservation, receipt)
		}
	}
	if err != nil {
		return MutationResult{}, err
	}
	identity, err := r.mutationStageIdentity(ctx, prepared, reservation)
	if err != nil {
		return MutationResult{}, err
	}
	stage, err := r.publishStage(ctx, identity)
	if err != nil {
		return MutationResult{}, err
	}
	if stage.MutationResult == nil {
		return MutationResult{}, fmt.Errorf("%w: terminal mutation stage has no namespace result", catalog.ErrIntegrity)
	}
	return MutationResult{Receipt: receipt, Stage: stage}, nil
}

func (r *Runtime) reservePreparedMutation(
	ctx context.Context,
	prepared catalog.PreparedMutation,
) (catalog.SourceDriverMutationReservation, error) {
	reservation, err := r.config.Store.SourceDriverMutationReservation(ctx, prepared.OperationID)
	if err == nil {
		reservation, err = validateMutationReservation(prepared, r.config.Authority, reservation)
		if err == nil {
			err = r.validateMutationReservationGeneration(reservation)
		}
		if err == nil {
			err = validatePreparedMutationReservation(prepared, r.config.Authority, reservation)
		}
		return reservation, err
	}
	if !errors.Is(err, catalog.ErrNotFound) {
		return catalog.SourceDriverMutationReservation{}, err
	}
	checkpoint, err := r.alignCheckpoint(ctx)
	if err != nil {
		return catalog.SourceDriverMutationReservation{}, err
	}
	if err := r.validateCheckpoint(checkpoint); err != nil {
		return catalog.SourceDriverMutationReservation{}, err
	}
	if checkpoint.SnapshotRequired != 0 {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrSourceRequiresSnapshot
	}
	target, found := r.config.target(prepared.Tenant)
	if !found {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrGenerationMismatch
	}
	targetCheckpoint, err := r.config.Store.SourceDriverTargetCheckpoint(
		ctx, r.config.Authority, target.Tenant, target.Generation,
	)
	if err != nil {
		return catalog.SourceDriverMutationReservation{}, err
	}
	if prepared.ExpectedHead != targetCheckpoint.CatalogRevision {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrSourcePredecessor
	}
	operation, sourceOperation, change, err := newCausalIDs()
	if err != nil {
		return catalog.SourceDriverMutationReservation{}, err
	}
	request := catalog.SourceDriverMutationReservationRequest{
		Mutation: prepared.OperationID, Claim: *prepared.Claim,
		Authority: r.config.Authority, FleetOwner: r.config.FleetOwner,
		AuthorityGeneration: r.config.AuthorityGeneration, DeclarationDigest: r.config.DeclarationDigest,
		TargetCount: uint64(len(r.config.Targets)), TargetsDigest: r.config.targetsDigest,
		Target: target, FromToken: checkpoint.Token, Predecessor: checkpoint.SourceRevision,
		Operation: operation, SourceOperation: sourceOperation, ChangeID: change,
	}
	reservation, err = r.config.Store.ReserveSourceDriverMutation(ctx, request)
	if err != nil {
		recovered, recoverErr := r.config.Store.SourceDriverMutationReservation(
			context.WithoutCancel(ctx), prepared.OperationID,
		)
		if recoverErr != nil {
			return catalog.SourceDriverMutationReservation{}, errors.Join(err, recoverErr)
		}
		reservation = recovered
	}
	reservation, err = validateMutationReservation(prepared, r.config.Authority, reservation)
	if err == nil {
		err = r.validateMutationReservationGeneration(reservation)
	}
	if err == nil {
		err = validatePreparedMutationReservation(prepared, r.config.Authority, reservation)
	}
	return reservation, err
}

func (r *Runtime) validateMutationReservationGeneration(
	reservation catalog.SourceDriverMutationReservation,
) error {
	if reservation.FleetOwner != r.config.FleetOwner ||
		reservation.AuthorityGeneration != r.config.AuthorityGeneration ||
		reservation.DeclarationDigest != r.config.DeclarationDigest {
		return catalog.ErrGenerationMismatch
	}
	return nil
}

func validateMutationReservation(
	prepared catalog.PreparedMutation,
	authority causal.SourceAuthorityID,
	reservation catalog.SourceDriverMutationReservation,
) (catalog.SourceDriverMutationReservation, error) {
	if prepared.Claim == nil || reservation.Mutation != prepared.OperationID ||
		reservation.Claim != *prepared.Claim || reservation.Authority != authority ||
		reservation.Target.Tenant != prepared.Tenant {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrMutationConflict
	}
	return reservation, nil
}

func (r *Runtime) releaseUnboundReservation(
	ctx context.Context,
	reservation catalog.SourceDriverMutationReservation,
) error {
	err := r.config.Store.ReleaseUnboundSourceDriverMutationReservation(
		context.WithoutCancel(ctx), reservation.Mutation, reservation.Claim, reservation.TargetEpoch,
	)
	if err != nil && !errors.Is(err, catalog.ErrNotFound) {
		return err
	}
	surviving, lookupErr := r.config.Store.SourceDriverMutationReservation(
		context.WithoutCancel(ctx), reservation.Mutation,
	)
	if errors.Is(lookupErr, catalog.ErrNotFound) {
		return nil
	}
	if lookupErr != nil {
		return lookupErr
	}
	if surviving.TargetEpoch != reservation.TargetEpoch || surviving.RequestBound || surviving.Receipt != nil {
		return catalog.ErrMutationConflict
	}
	return catalog.ErrMutationConflict
}

func mutationRequestFromReservation(
	prepared catalog.PreparedMutation,
	request sourcedriver.MutationRequest,
	hasContent bool,
	reservation catalog.SourceDriverMutationReservation,
) (sourcedriver.MutationRequest, error) {
	targetSet, err := mutationTargetSetRef(reservation)
	if err != nil {
		return sourcedriver.MutationRequest{}, err
	}
	request.TargetSet = targetSet
	request.Tenant = reservation.Target.Tenant
	request.Generation = causal.Generation(reservation.Target.Generation)
	request.OperationID = reservation.Mutation
	request.Expected = sourcedriver.RevisionToken(reservation.FromToken)
	request.Context = *prepared.Source
	request.HasContent = hasContent
	if err := sourcedriver.ValidateMutationRequest(request); err != nil {
		return sourcedriver.MutationRequest{}, err
	}
	return request, nil
}

func (r *Runtime) bindMutationRequest(
	ctx context.Context,
	reservation catalog.SourceDriverMutationReservation,
	digest [32]byte,
) (catalog.SourceDriverMutationReservation, error) {
	if reservation.RequestBound {
		if reservation.RequestDigest != digest {
			return catalog.SourceDriverMutationReservation{}, catalog.ErrMutationConflict
		}
		return reservation, nil
	}
	bound, err := r.config.Store.BindSourceDriverMutationRequest(
		ctx, reservation.Mutation, reservation.Claim, digest,
	)
	if err != nil {
		recovered, recoverErr := r.config.Store.SourceDriverMutationReservation(
			context.WithoutCancel(ctx), reservation.Mutation,
		)
		if recoverErr != nil || !recovered.RequestBound || recovered.RequestDigest != digest {
			return catalog.SourceDriverMutationReservation{}, errors.Join(err, recoverErr)
		}
		bound = recovered
	}
	if !bound.RequestBound || bound.RequestDigest != digest {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrIntegrity
	}
	return bound, nil
}

func (r *Runtime) recordMutationReceipt(
	ctx context.Context,
	prepared catalog.PreparedMutation,
	reservation catalog.SourceDriverMutationReservation,
	receipt sourcedriver.MutationReceipt,
) (catalog.SourceDriverMutationReservation, error) {
	result := catalog.SourceObjectKey(receipt.Result)
	if (prepared.Kind == catalog.MutationCreate) != (result != "") {
		return catalog.SourceDriverMutationReservation{}, sourcedriver.ErrIntegrity
	}
	proof := catalog.SourceDriverMutationReceiptProof{
		ToToken: string(receipt.Committed), Result: result, Digest: receipt.Digest,
	}
	recorded, err := r.config.Store.RecordSourceDriverMutationReceipt(
		ctx, reservation.Mutation, reservation.Claim, proof,
	)
	if err != nil {
		recovered, recoverErr := r.config.Store.SourceDriverMutationReservation(
			context.WithoutCancel(ctx), reservation.Mutation,
		)
		if recoverErr != nil || recovered.Receipt == nil || *recovered.Receipt != proof {
			return catalog.SourceDriverMutationReservation{}, errors.Join(err, recoverErr)
		}
		recorded = recovered
	}
	if recorded.Receipt == nil || *recorded.Receipt != proof {
		return catalog.SourceDriverMutationReservation{}, catalog.ErrIntegrity
	}
	return recorded, nil
}

func mutationReceiptFromReservation(
	reservation catalog.SourceDriverMutationReservation,
) (sourcedriver.MutationReceipt, error) {
	if !reservation.RequestBound || reservation.Receipt == nil {
		return sourcedriver.MutationReceipt{}, catalog.ErrInvalidTransition
	}
	receipt := sourcedriver.MutationReceipt{
		OperationID: reservation.Mutation, State: sourcedriver.MutationApplied,
		RequestDigest: reservation.RequestDigest,
		Expected:      sourcedriver.RevisionToken(reservation.FromToken),
		Committed:     sourcedriver.RevisionToken(reservation.Receipt.ToToken),
		Result:        sourcedriver.LogicalID(reservation.Receipt.Result), Digest: reservation.Receipt.Digest,
	}
	if err := sourcedriver.ValidateMutationReceipt(receipt); err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	return receipt, nil
}

func (r *Runtime) mutationStageIdentity(
	ctx context.Context,
	prepared catalog.PreparedMutation,
	reservation catalog.SourceDriverMutationReservation,
) (catalog.SourceDriverStageIdentity, error) {
	if reservation.Receipt == nil || !reservation.RequestBound {
		return catalog.SourceDriverStageIdentity{}, catalog.ErrInvalidTransition
	}
	checkpoint, err := r.alignCheckpoint(ctx)
	if err != nil {
		return catalog.SourceDriverStageIdentity{}, err
	}
	targetEpoch, err := r.config.Store.SourceDriverTargetEpoch(ctx, r.config.Authority)
	if err != nil {
		return catalog.SourceDriverStageIdentity{}, err
	}
	currentMutationTarget, found := r.config.target(reservation.Target.Tenant)
	if checkpoint.Authority != r.config.Authority || checkpoint.FleetOwner != r.config.FleetOwner ||
		checkpoint.AuthorityGeneration != r.config.AuthorityGeneration ||
		checkpoint.DeclarationDigest != r.config.DeclarationDigest || checkpoint.TargetEpoch > targetEpoch ||
		checkpoint.Token != reservation.FromToken || checkpoint.SourceRevision != reservation.Predecessor ||
		!found || currentMutationTarget != reservation.Target {
		return catalog.SourceDriverStageIdentity{}, catalog.ErrGenerationMismatch
	}
	if checkpoint.SnapshotRequired != 0 && checkpoint.SnapshotRequired != catalog.SourceDriverSnapshotReset {
		return catalog.SourceDriverStageIdentity{}, catalog.ErrSourceRequiresSnapshot
	}
	reason := catalog.SourceDriverSnapshotReason(0)
	if checkpoint.SnapshotRequired == catalog.SourceDriverSnapshotReset || checkpoint.TargetEpoch < targetEpoch ||
		checkpoint.TargetCount != uint64(len(r.config.Targets)) || checkpoint.TargetsDigest != r.config.targetsDigest {
		reason = catalog.SourceDriverSnapshotReset
	}
	return catalog.SourceDriverStageIdentity{
		Authority: r.config.Authority, FleetOwner: r.config.FleetOwner,
		AuthorityGeneration: r.config.AuthorityGeneration, DeclarationDigest: r.config.DeclarationDigest,
		TargetCount: uint64(len(r.config.Targets)), TargetsDigest: r.config.targetsDigest,
		Operation: reservation.Operation, SourceOperation: reservation.SourceOperation,
		ChangeID: reservation.ChangeID, Cause: prepared.Intent.Origin.Cause,
		Origin: prepared.Intent.Origin.Domain, OriginGeneration: prepared.Intent.Origin.Generation,
		Mode: catalog.SourceDriverMutation, SnapshotReason: reason, FromToken: reservation.FromToken,
		ToToken: reservation.Receipt.ToToken, Predecessor: checkpoint.SourceRevision,
		Mutation: reservation.Mutation, MutationTenant: reservation.Target.Tenant,
		MutationGeneration: reservation.Target.Generation, MutationResult: reservation.Receipt.Result,
		MutationRequestDigest: reservation.RequestDigest, MutationReceiptDigest: reservation.Receipt.Digest,
		Claim: reservation.Claim,
	}, nil
}

func (r *Runtime) inspectOrApply(
	ctx context.Context,
	request sourcedriver.MutationRequest,
	content contentstream.Source,
) (sourcedriver.MutationReceipt, error) {
	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		return sourcedriver.MutationReceipt{}, errors.Join(err, settleUnused(content, err))
	}
	receipt, err := r.config.Driver.InspectMutation(ctx, r.config.Authority, request.OperationID, requestDigest)
	if err != nil {
		return sourcedriver.MutationReceipt{}, errors.Join(err, settleUnused(content, err))
	}
	if err := validateReceiptForRequest(request, receipt); err != nil {
		return sourcedriver.MutationReceipt{}, errors.Join(err, settleUnused(content, err))
	}
	if receipt.State == sourcedriver.MutationApplied {
		if settleErr := abortUnused(content, errMutationAlreadyApplied); settleErr != nil {
			return sourcedriver.MutationReceipt{}, settleErr
		}
		return receipt, nil
	}
	receipt, applyErr := r.config.Driver.ApplyMutation(ctx, r.config.Authority, request, content)
	settleErr := settleTransferred(content, applyErr)
	if applyErr == nil {
		if settleErr != nil {
			return sourcedriver.MutationReceipt{}, settleErr
		}
		if err := validateAppliedReceipt(request, receipt); err != nil {
			return sourcedriver.MutationReceipt{}, err
		}
		return receipt, nil
	}
	inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationInspectTimeout)
	defer cancel()
	recovered, inspectErr := r.config.Driver.InspectMutation(
		inspectCtx, r.config.Authority, request.OperationID, requestDigest,
	)
	if inspectErr != nil {
		return sourcedriver.MutationReceipt{}, errors.Join(applyErr, settleErr, inspectErr)
	}
	if err := validateReceiptForRequest(request, recovered); err != nil {
		return sourcedriver.MutationReceipt{}, errors.Join(applyErr, settleErr, err)
	}
	if recovered.State != sourcedriver.MutationApplied {
		return sourcedriver.MutationReceipt{}, errors.Join(applyErr, settleErr)
	}
	if settleErr != nil {
		return sourcedriver.MutationReceipt{}, errors.Join(applyErr, settleErr)
	}
	return recovered, nil
}

func validateReceiptForRequest(request sourcedriver.MutationRequest, receipt sourcedriver.MutationReceipt) error {
	if err := sourcedriver.ValidateMutationReceipt(receipt); err != nil {
		return err
	}
	if receipt.OperationID != request.OperationID {
		return fmt.Errorf("%w: mutation receipt operation differs", sourcedriver.ErrIntegrity)
	}
	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		return err
	}
	if receipt.State != sourcedriver.MutationNotFound &&
		(receipt.RequestDigest != requestDigest || receipt.Expected != request.Expected) {
		return fmt.Errorf("%w: mutation receipt request proof differs", sourcedriver.ErrIntegrity)
	}
	return nil
}

func validateAppliedReceipt(request sourcedriver.MutationRequest, receipt sourcedriver.MutationReceipt) error {
	if err := validateReceiptForRequest(request, receipt); err != nil {
		return err
	}
	if receipt.State != sourcedriver.MutationApplied {
		return fmt.Errorf("%w: mutation apply returned no terminal receipt", sourcedriver.ErrIntegrity)
	}
	return nil
}

func (r *Runtime) validatePreparedMutation(prepared catalog.PreparedMutation) error {
	if prepared.OperationID == (catalog.MutationID{}) ||
		prepared.State != catalog.MutationApplying || prepared.Claim == nil || prepared.Source == nil {
		return fmt.Errorf("%w: prepared mutation is not an owned applying operation", catalog.ErrInvalidTransition)
	}
	return catalog.ValidateSourceMutationContext(*prepared.Source)
}

func validatePreparedMutationReservation(
	prepared catalog.PreparedMutation,
	authority causal.SourceAuthorityID,
	reservation catalog.SourceDriverMutationReservation,
) error {
	locators := []*catalog.SourceLocator{
		prepared.Source.Object,
		prepared.Source.Parent,
		prepared.Source.Target,
		prepared.SourceResult,
	}
	for _, locator := range locators {
		if locator != nil && (locator.SourceAuthority != authority ||
			locator.SourceRevision != reservation.Predecessor) {
			return catalog.ErrSourcePredecessor
		}
	}
	return nil
}

func mutationRequestFor(prepared catalog.PreparedMutation) (sourcedriver.MutationRequest, bool, error) {
	request := sourcedriver.MutationRequest{}
	var ref catalog.ContentRef
	var hasContent bool
	switch {
	case prepared.Intent.Create != nil:
		if prepared.Intent.Create.Spec.Kind == catalog.KindFile {
			ref, hasContent = prepared.Intent.Create.Spec.Content, true
		}
	case prepared.Intent.Revise != nil:
		if prepared.Intent.Revise.Spec.Content != nil {
			ref, hasContent = prepared.Intent.Revise.Spec.Content.Ref, true
		}
	case prepared.Intent.Replace != nil:
		if prepared.Intent.Replace.Content != nil {
			ref, hasContent = prepared.Intent.Replace.Content.Ref, true
		}
	case prepared.Intent.Delete != nil:
	default:
		return sourcedriver.MutationRequest{}, false, fmt.Errorf("%w: mutation intent is not closed", catalog.ErrInvalidTransition)
	}
	if hasContent {
		if ref.Stage == (catalog.StageID{}) || ref.Size < 0 || ref.Hash == (catalog.ContentHash{}) {
			return sourcedriver.MutationRequest{}, false, fmt.Errorf("%w: mutation content reference is incomplete", catalog.ErrIntegrity)
		}
		request.ContentSize, request.ContentHash = ref.Size, ref.Hash
	}
	return request, hasContent, nil
}
