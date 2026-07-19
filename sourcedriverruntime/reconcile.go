package sourcedriverruntime

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourcedriver"
)

func (r *Runtime) reconcile(ctx context.Context) (ReconcileResult, error) {
	if err := r.recoverCommittedReceipts(ctx); err != nil {
		return ReconcileResult{}, err
	}
	if recovered, err := r.recoverPending(ctx); err != nil {
		return ReconcileResult{}, err
	} else if recovered != nil {
		return ReconcileResult{Changed: true, Checkpoint: recovered.Checkpoint, Stage: recovered}, nil
	}
	checkpoint, err := r.alignCheckpoint(ctx)
	if err != nil {
		return ReconcileResult{}, err
	}
	targetEpoch, err := r.config.Store.SourceDriverTargetEpoch(ctx, r.config.Authority)
	if err != nil {
		return ReconcileResult{}, err
	}
	if checkpoint.Authority != "" && checkpoint.TargetEpoch > targetEpoch {
		return ReconcileResult{}, catalog.ErrGenerationMismatch
	}
	head, err := r.config.Driver.Refresh(ctx, r.config.Authority)
	if err != nil {
		return ReconcileResult{}, err
	}
	if err := sourcedriver.ValidateHead(head); err != nil {
		return ReconcileResult{}, err
	}
	checkpointErr := error(nil)
	if checkpoint.Authority == "" {
		checkpointErr = catalog.ErrNotFound
	} else if checkpoint.Token == string(head.Revision) && checkpoint.SnapshotRequired == 0 &&
		checkpoint.TargetEpoch == targetEpoch &&
		checkpoint.TargetCount == uint64(len(r.config.Targets)) && checkpoint.TargetsDigest == r.config.targetsDigest {
		return ReconcileResult{Checkpoint: checkpoint}, nil
	}
	identity, err := r.reconcileIdentity(head.Revision, targetEpoch, checkpoint, checkpointErr)
	if err != nil {
		return ReconcileResult{}, err
	}
	result, err := r.publishStage(ctx, identity)
	if err != nil {
		var required *sourcedriver.SnapshotRequiredError
		if identity.Mode != catalog.SourceDriverDelta || !errors.As(err, &required) {
			return ReconcileResult{}, err
		}
		if string(required.From) != identity.FromToken || string(required.Head) != identity.ToToken {
			return ReconcileResult{}, fmt.Errorf("%w: snapshot-required fence differs", sourcedriver.ErrIntegrity)
		}
		recoveryCtx := context.WithoutCancel(ctx)
		if abortErr := r.config.Store.AbortSourceDriverStage(recoveryCtx, identity); abortErr != nil {
			return ReconcileResult{}, errors.Join(err, abortErr)
		}
		checkpoint, markErr := r.config.Store.RequireSourceDriverSnapshot(
			recoveryCtx, r.config.Authority, identity.FromToken, catalog.SourceDriverSnapshotExpiredFloor,
		)
		if markErr != nil {
			return ReconcileResult{}, errors.Join(err, markErr)
		}
		identity, err = r.newStageIdentity(
			catalog.SourceDriverSnapshot, catalog.SourceDriverSnapshotExpiredFloor,
			"", identity.ToToken, checkpoint,
		)
		if err != nil {
			return ReconcileResult{}, err
		}
		if err := r.beginStage(ctx, identity); err != nil {
			return ReconcileResult{}, err
		}
		return ReconcileResult{}, errProgressPending
	}
	return ReconcileResult{Changed: true, Checkpoint: result.Checkpoint, Stage: &result}, nil
}

func (r *Runtime) alignCheckpoint(ctx context.Context) (catalog.SourceDriverCheckpoint, error) {
	checkpoint, err := r.config.Store.SourceDriverCheckpoint(ctx, r.config.Authority)
	if errors.Is(err, catalog.ErrNotFound) {
		return catalog.SourceDriverCheckpoint{}, nil
	}
	if err != nil {
		return catalog.SourceDriverCheckpoint{}, err
	}
	currentTargetEpoch, err := r.config.Store.SourceDriverTargetEpoch(ctx, r.config.Authority)
	if err != nil {
		return catalog.SourceDriverCheckpoint{}, err
	}
	if checkpoint.TargetEpoch > currentTargetEpoch {
		return catalog.SourceDriverCheckpoint{}, catalog.ErrGenerationMismatch
	}
	if err := r.validateCheckpoint(checkpoint); err == nil {
		if checkpoint.TargetEpoch == currentTargetEpoch {
			return checkpoint, nil
		}
	}
	if checkpoint.Authority != r.config.Authority || checkpoint.FleetOwner != r.config.FleetOwner ||
		checkpoint.AuthorityGeneration > r.config.AuthorityGeneration ||
		checkpoint.AuthorityGeneration == r.config.AuthorityGeneration &&
			checkpoint.DeclarationDigest != r.config.DeclarationDigest {
		return catalog.SourceDriverCheckpoint{}, catalog.ErrGenerationMismatch
	}
	if checkpoint.AuthorityGeneration == r.config.AuthorityGeneration {
		return checkpoint, nil
	}
	return r.config.Store.RebindSourceDriverCheckpoint(ctx, catalog.SourceDriverCheckpointRebind{
		Expected: checkpoint, AuthorityGeneration: r.config.AuthorityGeneration,
		DeclarationDigest: r.config.DeclarationDigest,
	})
}

func (r *Runtime) reconcileIdentity(
	head sourcedriver.RevisionToken,
	targetEpoch uint64,
	checkpoint catalog.SourceDriverCheckpoint,
	checkpointErr error,
) (catalog.SourceDriverStageIdentity, error) {
	if errors.Is(checkpointErr, catalog.ErrNotFound) {
		return r.newStageIdentity(
			catalog.SourceDriverSnapshot, catalog.SourceDriverSnapshotInitial,
			"", string(head), checkpoint,
		)
	}
	if checkpoint.SnapshotRequired != 0 {
		return r.newStageIdentity(
			catalog.SourceDriverSnapshot, checkpoint.SnapshotRequired,
			"", string(head), checkpoint,
		)
	}
	if checkpoint.TargetEpoch < targetEpoch || checkpoint.TargetCount != uint64(len(r.config.Targets)) || checkpoint.TargetsDigest != r.config.targetsDigest {
		return r.newStageIdentity(
			catalog.SourceDriverSnapshot, catalog.SourceDriverSnapshotReset,
			"", string(head), checkpoint,
		)
	}
	return r.newStageIdentity(catalog.SourceDriverDelta, 0, checkpoint.Token, string(head), checkpoint)
}

func (r *Runtime) recoverPending(ctx context.Context) (*catalog.SourceDriverStageResult, error) {
	return r.recoverPendingForMutation(ctx, nil)
}

func (r *Runtime) recoverPendingForMutation(
	ctx context.Context,
	admitted *catalog.MutationID,
) (*catalog.SourceDriverStageResult, error) {
	return r.recoverPendingForPreparedMutation(ctx, admitted, nil)
}

func (r *Runtime) recoverPendingForPreparedMutation(
	ctx context.Context,
	admitted *catalog.MutationID,
	admittedPrepared *catalog.PreparedMutation,
) (*catalog.SourceDriverStageResult, error) {
	pending, err := r.config.Store.PendingSourceDriverStage(ctx, r.config.Authority)
	if err != nil {
		return nil, err
	}
	if pending == nil {
		if admitted != nil {
			reservation, lookupErr := r.config.Store.ActiveSourceDriverMutationReservation(ctx, r.config.Authority)
			if lookupErr != nil {
				return nil, lookupErr
			}
			if reservation != nil && reservation.Mutation == *admitted {
				return nil, nil
			}
		}
		return r.recoverRetainedMutation(ctx, admittedPrepared)
	}
	if pending.Identity.Mode == catalog.SourceDriverMutation {
		reservation, lookupErr := r.config.Store.SourceDriverMutationReservation(
			ctx, pending.Identity.Mutation,
		)
		if lookupErr != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, lookupErr)
		}
		if err := validateSettlementReservation(pending.Identity, reservation); err != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, catalog.ErrMutationConflict, err)
		}
		if err := r.validateMutationReservationGeneration(reservation); err != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, err)
		}
		var prepared catalog.PreparedMutation
		var preparedErr error
		if admittedPrepared != nil && admittedPrepared.OperationID == reservation.Mutation &&
			admittedPrepared.Tenant == reservation.Target.Tenant {
			prepared = *admittedPrepared
		} else {
			prepared, preparedErr = r.config.Store.PreparedMutation(
				ctx, reservation.Target.Tenant, reservation.Mutation,
			)
		}
		if preparedErr != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, preparedErr)
		}
		if preparedErr = r.validatePreparedMutation(prepared); preparedErr != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, preparedErr)
		}
		if preparedErr = validatePreparedMutationReservation(
			prepared, r.config.Authority, reservation,
		); preparedErr != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, preparedErr)
		}
		current, currentErr := r.pendingStageIsCurrent(ctx, *pending)
		if currentErr != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, currentErr)
		}
		if !current {
			if abortErr := r.config.Store.AbortSourceDriverStage(
				context.WithoutCancel(ctx), pending.Identity,
			); abortErr != nil {
				return nil, errors.Join(ErrRetainedMutationLiability, abortErr)
			}
			return r.recoverRetainedMutation(ctx, admittedPrepared)
		}
	} else {
		current, currentErr := r.pendingStageIsCurrent(ctx, *pending)
		if currentErr != nil {
			return nil, currentErr
		}
		if !current {
			if abortErr := r.config.Store.AbortSourceDriverStage(
				context.WithoutCancel(ctx), pending.Identity,
			); abortErr != nil {
				return nil, abortErr
			}
			return nil, nil
		}
	}
	result, err := r.resumeStage(ctx, *pending)
	if err != nil {
		var required *sourcedriver.SnapshotRequiredError
		if errors.As(err, &required) {
			if string(required.From) != pending.Identity.FromToken || string(required.Head) != pending.Identity.ToToken {
				return nil, errors.Join(sourcedriver.ErrIntegrity, err)
			}
			recoveryCtx := context.WithoutCancel(ctx)
			if abortErr := r.config.Store.AbortSourceDriverStage(recoveryCtx, pending.Identity); abortErr != nil {
				if pending.Identity.Mode == catalog.SourceDriverMutation {
					return nil, errors.Join(ErrRetainedMutationLiability, err, abortErr)
				}
				return nil, errors.Join(err, abortErr)
			}
			reason := catalog.SourceDriverSnapshotExpiredFloor
			if pending.Identity.Mode == catalog.SourceDriverMutation {
				reason = catalog.SourceDriverSnapshotReset
			}
			checkpoint, markErr := r.config.Store.RequireSourceDriverSnapshot(
				recoveryCtx, r.config.Authority, pending.Identity.FromToken, reason,
			)
			if markErr != nil {
				if pending.Identity.Mode == catalog.SourceDriverMutation {
					return nil, errors.Join(ErrRetainedMutationLiability, err, markErr)
				}
				return nil, errors.Join(err, markErr)
			}
			if pending.Identity.Mode == catalog.SourceDriverMutation {
				return r.recoverRetainedMutation(ctx, admittedPrepared)
			}
			identity, identityErr := r.newStageIdentity(
				catalog.SourceDriverSnapshot, reason, "", pending.Identity.ToToken, checkpoint,
			)
			if identityErr != nil {
				return nil, identityErr
			}
			if beginErr := r.beginStage(ctx, identity); beginErr != nil {
				return nil, beginErr
			}
			return nil, errProgressPending
		}
		current, topologyErr := r.pendingStageIsCurrent(ctx, *pending)
		if topologyErr == nil && !current {
			if abortErr := r.config.Store.AbortSourceDriverStage(
				context.WithoutCancel(ctx), pending.Identity,
			); abortErr != nil {
				if pending.Identity.Mode == catalog.SourceDriverMutation {
					return nil, errors.Join(ErrRetainedMutationLiability, err, abortErr)
				}
				return nil, errors.Join(err, abortErr)
			}
			if pending.Identity.Mode == catalog.SourceDriverMutation {
				return r.recoverRetainedMutation(ctx, admittedPrepared)
			}
			return nil, nil
		}
		if pending.Identity.Mode == catalog.SourceDriverMutation {
			return nil, errors.Join(ErrRetainedMutationLiability, err, topologyErr)
		}
		return nil, errors.Join(err, topologyErr)
	}
	return &result, nil
}

func (r *Runtime) pendingStageIsCurrent(
	ctx context.Context,
	state catalog.SourceDriverStageState,
) (bool, error) {
	identity := state.Identity
	if identity.Authority != r.config.Authority || identity.FleetOwner != r.config.FleetOwner ||
		identity.AuthorityGeneration > r.config.AuthorityGeneration ||
		identity.AuthorityGeneration == r.config.AuthorityGeneration &&
			identity.DeclarationDigest != r.config.DeclarationDigest {
		return false, catalog.ErrGenerationMismatch
	}
	currentEpoch, err := r.config.Store.SourceDriverTargetEpoch(ctx, r.config.Authority)
	if err != nil {
		return false, err
	}
	if state.TargetEpoch > currentEpoch {
		return false, catalog.ErrGenerationMismatch
	}
	if identity.AuthorityGeneration < r.config.AuthorityGeneration || state.TargetEpoch < currentEpoch {
		return false, nil
	}
	if identity.TargetCount != uint64(len(r.config.Targets)) || identity.TargetsDigest != r.config.targetsDigest {
		return false, catalog.ErrGenerationMismatch
	}
	return true, nil
}

func (r *Runtime) recoverRetainedMutation(
	ctx context.Context,
	admittedPrepared *catalog.PreparedMutation,
) (*catalog.SourceDriverStageResult, error) {
	reservation, err := r.config.Store.ActiveSourceDriverMutationReservation(ctx, r.config.Authority)
	if err != nil || reservation == nil {
		return nil, err
	}
	if err := r.validateMutationReservationGeneration(*reservation); err != nil {
		return nil, errors.Join(ErrRetainedMutationLiability, err)
	}
	if !reservation.RequestBound {
		if err := r.releaseUnboundReservation(ctx, *reservation); err != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, err)
		}
		return nil, nil
	}
	var prepared catalog.PreparedMutation
	if admittedPrepared != nil && admittedPrepared.OperationID == reservation.Mutation &&
		admittedPrepared.Tenant == reservation.Target.Tenant {
		prepared = *admittedPrepared
	} else {
		prepared, err = r.config.Store.PreparedMutation(ctx, reservation.Target.Tenant, reservation.Mutation)
		if err != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, err)
		}
	}
	if _, err := validateMutationReservation(prepared, r.config.Authority, *reservation); err != nil {
		return nil, errors.Join(ErrRetainedMutationLiability, err)
	}
	if err := r.validatePreparedMutation(prepared); err != nil {
		return nil, errors.Join(ErrRetainedMutationLiability, err)
	}
	if err := validatePreparedMutationReservation(prepared, r.config.Authority, *reservation); err != nil {
		return nil, errors.Join(ErrRetainedMutationLiability, err)
	}
	if reservation.Receipt == nil {
		request, hasContent, requestErr := mutationRequestFor(prepared)
		if requestErr != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, requestErr)
		}
		request, requestErr = mutationRequestFromReservation(prepared, request, hasContent, *reservation)
		if requestErr != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, requestErr)
		}
		receipt, inspectErr := r.config.Driver.InspectMutation(
			ctx, r.config.Authority, reservation.Mutation, reservation.RequestDigest,
		)
		if inspectErr != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, inspectErr)
		}
		if validateErr := validateReceiptForRequest(request, receipt); validateErr != nil ||
			receipt.State != sourcedriver.MutationApplied {
			if validateErr != nil {
				return nil, errors.Join(ErrRetainedMutationLiability, validateErr)
			}
			return nil, &retainedMutationPendingError{Mutation: reservation.Mutation}
		}
		recorded, recordErr := r.recordMutationReceipt(ctx, prepared, *reservation, receipt)
		if recordErr != nil {
			return nil, errors.Join(ErrRetainedMutationLiability, recordErr)
		}
		reservation = &recorded
	}
	identity, err := r.mutationStageIdentity(ctx, prepared, *reservation)
	if err != nil {
		return nil, errors.Join(ErrRetainedMutationLiability, err)
	}
	result, err := r.publishStage(ctx, identity)
	if err != nil {
		return nil, errors.Join(ErrRetainedMutationLiability, err)
	}
	return &result, nil
}

func (r *Runtime) publishStage(
	ctx context.Context,
	identity catalog.SourceDriverStageIdentity,
) (catalog.SourceDriverStageResult, error) {
	if err := r.beginStage(ctx, identity); err != nil {
		return catalog.SourceDriverStageResult{}, err
	}
	pending, err := r.config.Store.PendingSourceDriverStage(ctx, r.config.Authority)
	if err != nil {
		return catalog.SourceDriverStageResult{}, err
	}
	return r.resumeStage(ctx, *pending)
}

func (r *Runtime) beginStage(ctx context.Context, identity catalog.SourceDriverStageIdentity) error {
	if err := r.config.Store.BeginSourceDriverStage(ctx, identity); err != nil {
		return err
	}
	pending, err := r.config.Store.PendingSourceDriverStage(ctx, r.config.Authority)
	if err != nil {
		return err
	}
	if pending == nil || pending.Identity != identity {
		return fmt.Errorf("%w: begun source driver stage disappeared", catalog.ErrIntegrity)
	}
	return nil
}

func (r *Runtime) resumeStage(
	ctx context.Context,
	state catalog.SourceDriverStageState,
) (catalog.SourceDriverStageResult, error) {
	targetSet, err := sourceDriverStageTargetSet(state)
	if err != nil {
		return catalog.SourceDriverStageResult{}, err
	}
	if state.Stage.Sequence == 0 || len(state.Cursor) != 0 {
		prepared, err := r.prepareTargetDeclaration(ctx, state)
		if err != nil {
			return catalog.SourceDriverStageResult{}, err
		}
		if !prepared {
			return catalog.SourceDriverStageResult{}, errProgressPending
		}
		var complete bool
		targetSet, complete, err = r.ensureTargetSet(ctx, state)
		if err != nil {
			return catalog.SourceDriverStageResult{}, err
		}
		if !complete {
			return catalog.SourceDriverStageResult{}, errProgressPending
		}
	}
	if state.Stage.Sequence != 0 && len(state.Cursor) == 0 {
		preparation, err := r.config.Store.PrepareSourceDriverPublicationBatch(ctx, state.Identity)
		if err != nil {
			return catalog.SourceDriverStageResult{}, err
		}
		if !preparation.Prepared {
			return catalog.SourceDriverStageResult{}, errProgressPending
		}
		var result catalog.SourceDriverStageResult
		if state.Identity.Mode == catalog.SourceDriverMutation {
			result, err = r.config.Store.CommitSourceDriverMutation(ctx, state)
		} else {
			result, err = r.config.Store.CommitSourceDriverStage(ctx, state)
		}
		if err != nil {
			return catalog.SourceDriverStageResult{}, err
		}
		if err := r.settleCommittedResult(ctx, result); err != nil {
			return catalog.SourceDriverStageResult{}, err
		}
		return result, nil
	}
	cursor, err := decodeCursor(state.Cursor)
	if err != nil {
		return catalog.SourceDriverStageResult{}, err
	}
	page, err := r.nextStagePage(ctx, state, targetSet, cursor)
	if err != nil {
		return catalog.SourceDriverStageResult{}, err
	}
	if _, err := r.appendStagePage(ctx, state, page); err != nil {
		return catalog.SourceDriverStageResult{}, err
	}
	return catalog.SourceDriverStageResult{}, errProgressPending
}

func sourceDriverStageTargetSet(state catalog.SourceDriverStageState) (sourcedriver.TargetSetRef, error) {
	identity := state.Identity
	return sourcedriver.NewTargetSetRefForDigest(
		identity.Authority,
		identity.AuthorityGeneration,
		state.TargetEpoch,
		identity.DeclarationDigest,
		identity.TargetCount,
		identity.TargetsDigest,
	)
}

func (r *Runtime) nextStagePage(
	ctx context.Context,
	state catalog.SourceDriverStageState,
	targetSet sourcedriver.TargetSetRef,
	cursor *sourcedriver.PageCursor,
) (catalog.SourceDriverStagePage, error) {
	identity := state.Identity
	switch identity.Mode {
	case catalog.SourceDriverSnapshot:
		request := sourcedriver.SnapshotRequest{
			TargetSet: targetSet,
			Revision:  sourcedriver.RevisionToken(identity.ToToken), Cursor: cursor, Limit: r.config.PageLimit,
		}
		if err := sourcedriver.ValidateSnapshotRequest(request); err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		if err := r.config.Store.ValidateSourceDriverTargetEpoch(ctx, r.config.Authority, targetSet.TargetEpoch); err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		page, err := r.config.Driver.Snapshot(ctx, r.config.Authority, request)
		if err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		if err := sourcedriver.ValidateSnapshotPage(request, page); err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		entries, refs, err := r.stageProjections(ctx, identity, page.Objects)
		if err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		return r.finishStagePage(state, page.Next, page.Digest, entries, refs)
	case catalog.SourceDriverMutation:
		if identity.SnapshotReason == catalog.SourceDriverSnapshotReset {
			request := sourcedriver.SnapshotRequest{
				TargetSet: targetSet, Revision: sourcedriver.RevisionToken(identity.ToToken),
				Cursor: cursor, Limit: r.config.PageLimit,
			}
			if err := sourcedriver.ValidateSnapshotRequest(request); err != nil {
				return catalog.SourceDriverStagePage{}, err
			}
			if err := r.config.Store.ValidateSourceDriverTargetEpoch(
				ctx, r.config.Authority, targetSet.TargetEpoch,
			); err != nil {
				return catalog.SourceDriverStagePage{}, err
			}
			page, err := r.config.Driver.Snapshot(ctx, r.config.Authority, request)
			if err != nil {
				return catalog.SourceDriverStagePage{}, err
			}
			if err := sourcedriver.ValidateSnapshotPage(request, page); err != nil {
				return catalog.SourceDriverStagePage{}, err
			}
			entries, refs, err := r.stageProjections(ctx, identity, page.Objects)
			if err != nil {
				return catalog.SourceDriverStagePage{}, err
			}
			return r.finishStagePage(state, page.Next, page.Digest, entries, refs)
		}
		fallthrough
	case catalog.SourceDriverDelta:
		request := sourcedriver.ChangesRequest{
			TargetSet: targetSet,
			From:      sourcedriver.RevisionToken(identity.FromToken), To: sourcedriver.RevisionToken(identity.ToToken),
			Cursor: cursor, Limit: r.config.PageLimit,
		}
		if err := sourcedriver.ValidateChangesRequest(request); err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		if err := r.config.Store.ValidateSourceDriverTargetEpoch(ctx, r.config.Authority, targetSet.TargetEpoch); err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		page, err := r.config.Driver.ChangesSince(ctx, r.config.Authority, request)
		if err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		if err := sourcedriver.ValidateChangePage(request, page); err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		entries, refs, err := r.stageChanges(ctx, identity, page.Changes)
		if err != nil {
			return catalog.SourceDriverStagePage{}, err
		}
		return r.finishStagePage(state, page.Next, page.Digest, entries, refs)
	default:
		return catalog.SourceDriverStagePage{}, fmt.Errorf("%w: invalid pending source driver mode", catalog.ErrIntegrity)
	}
}

func (r *Runtime) finishStagePage(
	state catalog.SourceDriverStageState,
	next *sourcedriver.PageCursor,
	digest [sha256.Size]byte,
	entries []catalog.SourceDriverStageEntry,
	refs []catalog.ContentRef,
) (catalog.SourceDriverStagePage, error) {
	encoded, err := encodeCursor(next)
	if err != nil {
		_ = r.config.Store.ReleaseUnclaimedContent(context.Background(), refs)
		return catalog.SourceDriverStagePage{}, err
	}
	return catalog.SourceDriverStagePage{
		Sequence: state.Stage.Sequence, Cursor: encoded, Digest: digest,
		PredecessorDigest: catalog.SourceDriverPagePredecessorDigest(state.Cursor, state.PageDigest),
		Entries:           entries, Complete: next == nil,
	}, nil
}

func (r *Runtime) appendStagePage(
	ctx context.Context,
	before catalog.SourceDriverStageState,
	page catalog.SourceDriverStagePage,
) (catalog.SourceDriverStageState, error) {
	refs := contentRefs(page.Entries)
	after, err := r.config.Store.AppendSourceDriverStage(ctx, before.Identity, page)
	if err == nil {
		return after, nil
	}
	recoveryCtx := context.WithoutCancel(ctx)
	pending, inspectErr := r.config.Store.PendingSourceDriverStage(recoveryCtx, r.config.Authority)
	if inspectErr == nil && pending != nil && pending.Identity == before.Identity &&
		pending.Stage.Sequence == before.Stage.Sequence+1 &&
		bytes.Equal(pending.Cursor, page.Cursor) && pending.PageDigest == page.Digest {
		return *pending, nil
	}
	if inspectErr == nil && pending != nil && pending.Identity == before.Identity &&
		pending.Stage.Sequence == before.Stage.Sequence {
		releaseErr := r.config.Store.ReleaseUnclaimedContent(recoveryCtx, refs)
		return catalog.SourceDriverStageState{}, errors.Join(err, releaseErr)
	}
	return catalog.SourceDriverStageState{}, errors.Join(err, inspectErr)
}

func (r *Runtime) stageProjections(
	ctx context.Context,
	identity catalog.SourceDriverStageIdentity,
	values []sourcedriver.Projection,
) ([]catalog.SourceDriverStageEntry, []catalog.ContentRef, error) {
	entries := make([]catalog.SourceDriverStageEntry, 0, len(values))
	refs := make([]catalog.ContentRef, 0, len(values))
	for _, value := range values {
		entry, ref, err := r.stageProjection(ctx, identity, value, 0)
		if err != nil {
			releaseErr := r.config.Store.ReleaseUnclaimedContent(context.WithoutCancel(ctx), refs)
			return nil, nil, errors.Join(err, releaseErr)
		}
		entries = append(entries, entry)
		if ref != (catalog.ContentRef{}) {
			refs = append(refs, ref)
		}
	}
	return entries, refs, nil
}

func (r *Runtime) stageChanges(
	ctx context.Context,
	identity catalog.SourceDriverStageIdentity,
	values []sourcedriver.Change,
) ([]catalog.SourceDriverStageEntry, []catalog.ContentRef, error) {
	entries := make([]catalog.SourceDriverStageEntry, 0, len(values))
	refs := make([]catalog.ContentRef, 0, len(values))
	for _, change := range values {
		target, found := r.config.target(change.Tenant)
		if !found || causal.Generation(target.Generation) != change.Generation {
			return nil, nil, errors.Join(
				fmt.Errorf("%w: change generation differs", sourcedriver.ErrIntegrity),
				r.config.Store.ReleaseUnclaimedContent(context.WithoutCancel(ctx), refs),
			)
		}
		root, err := catalog.DeriveSourceDriverRootKey(r.config.Authority, change.Tenant)
		if err != nil {
			return nil, nil, err
		}
		key := catalog.SourceObjectKey(change.ID)
		if key == root {
			return nil, nil, errors.Join(
				fmt.Errorf("%w: driver attempted to mutate the catalog-owned root", sourcedriver.ErrInvalidValue),
				r.config.Store.ReleaseUnclaimedContent(context.WithoutCancel(ctx), refs),
			)
		}
		if change.Kind == sourcedriver.ChangeDelete {
			entries = append(entries, catalog.SourceDriverStageEntry{
				Tenant: change.Tenant, Generation: catalog.Generation(change.Generation),
				ChangeSequence: change.Sequence, Key: key,
			})
			continue
		}
		entry, ref, err := r.stageProjection(ctx, identity, *change.Object, change.Sequence)
		if err != nil {
			return nil, nil, errors.Join(err, r.config.Store.ReleaseUnclaimedContent(context.WithoutCancel(ctx), refs))
		}
		entries = append(entries, entry)
		if ref != (catalog.ContentRef{}) {
			refs = append(refs, ref)
		}
	}
	return entries, refs, nil
}

func (r *Runtime) stageProjection(
	ctx context.Context,
	identity catalog.SourceDriverStageIdentity,
	value sourcedriver.Projection,
	sequence uint64,
) (catalog.SourceDriverStageEntry, catalog.ContentRef, error) {
	target, found := r.config.target(value.Tenant)
	if !found || causal.Generation(target.Generation) != value.Generation {
		return catalog.SourceDriverStageEntry{}, catalog.ContentRef{}, fmt.Errorf("%w: projection generation differs", sourcedriver.ErrIntegrity)
	}
	root, err := catalog.DeriveSourceDriverRootKey(r.config.Authority, value.Tenant)
	if err != nil {
		return catalog.SourceDriverStageEntry{}, catalog.ContentRef{}, err
	}
	key := catalog.SourceObjectKey(value.ID)
	if key == root {
		return catalog.SourceDriverStageEntry{}, catalog.ContentRef{}, fmt.Errorf("%w: driver projected the catalog-owned root", sourcedriver.ErrInvalidValue)
	}
	object := catalog.SourceObject{
		Key: key, Parent: catalog.SourceObjectKey(value.Parent), Name: value.Name,
		Kind: value.Kind, Mode: value.Mode, LinkTarget: value.LinkTarget, Visibility: value.Visibility,
	}
	entry := catalog.SourceDriverStageEntry{
		Tenant: value.Tenant, Generation: target.Generation, ChangeSequence: sequence, Key: key, Object: &object,
	}
	if value.Kind == catalog.KindDirectory {
		return entry, catalog.ContentRef{}, nil
	}
	object.ContentRevision = catalog.Revision(identity.Predecessor + 1)
	if value.Kind == catalog.KindSymlink {
		return entry, catalog.ContentRef{}, nil
	}
	stream, err := r.config.Driver.OpenContent(ctx, r.config.Authority, *value.Content)
	if err != nil {
		return catalog.SourceDriverStageEntry{}, catalog.ContentRef{}, err
	}
	ref, err := r.config.Store.StageOwnedContent(ctx, stream)
	if err != nil {
		return catalog.SourceDriverStageEntry{}, catalog.ContentRef{}, err
	}
	if ref.Size != value.Size || ref.Hash != value.Hash {
		releaseErr := r.config.Store.ReleaseUnclaimedContent(context.WithoutCancel(ctx), []catalog.ContentRef{ref})
		return catalog.SourceDriverStageEntry{}, catalog.ContentRef{}, errors.Join(
			fmt.Errorf("%w: staged content differs from projection", sourcedriver.ErrIntegrity), releaseErr,
		)
	}
	object.Content = ref
	entry.Object = &object
	return entry, ref, nil
}

func (r *Runtime) newStageIdentity(
	mode catalog.SourceDriverMode,
	reason catalog.SourceDriverSnapshotReason,
	from, to string,
	checkpoint catalog.SourceDriverCheckpoint,
) (catalog.SourceDriverStageIdentity, error) {
	operation, sourceOperation, change, err := newCausalIDs()
	if err != nil {
		return catalog.SourceDriverStageIdentity{}, err
	}
	identity := catalog.SourceDriverStageIdentity{
		Authority: r.config.Authority, FleetOwner: r.config.FleetOwner,
		AuthorityGeneration: r.config.AuthorityGeneration, DeclarationDigest: r.config.DeclarationDigest,
		TargetCount: uint64(len(r.config.Targets)), TargetsDigest: r.config.targetsDigest,
		Operation: operation, SourceOperation: sourceOperation, ChangeID: change,
		Cause: causal.CauseExternalUnattributed, Mode: mode, SnapshotReason: reason,
		FromToken: from, ToToken: to, Predecessor: checkpoint.SourceRevision,
	}
	return identity, nil
}

func (r *Runtime) validateCheckpoint(checkpoint catalog.SourceDriverCheckpoint) error {
	if checkpoint.Authority != r.config.Authority || checkpoint.FleetOwner != r.config.FleetOwner ||
		checkpoint.AuthorityGeneration != r.config.AuthorityGeneration ||
		checkpoint.DeclarationDigest != r.config.DeclarationDigest ||
		checkpoint.TargetCount != uint64(len(r.config.Targets)) || checkpoint.TargetsDigest != r.config.targetsDigest {
		return catalog.ErrGenerationMismatch
	}
	return nil
}

func encodeCursor(cursor *sourcedriver.PageCursor) ([]byte, error) {
	if cursor == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return nil, err
	}
	if len(encoded) > catalog.SourceDriverCursorMaxBytes {
		return nil, fmt.Errorf("%w: encoded source cursor is too large", sourcedriver.ErrInvalidValue)
	}
	return encoded, nil
}

func decodeCursor(encoded []byte) (*sourcedriver.PageCursor, error) {
	if len(encoded) == 0 {
		return nil, nil
	}
	if len(encoded) > catalog.SourceDriverCursorMaxBytes {
		return nil, fmt.Errorf("%w: encoded source cursor is too large", sourcedriver.ErrInvalidValue)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var cursor sourcedriver.PageCursor
	if err := decoder.Decode(&cursor); err != nil {
		return nil, fmt.Errorf("%w: decode source cursor: %v", catalog.ErrIntegrity, err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%w: source cursor has trailing data", catalog.ErrIntegrity)
	}
	return &cursor, nil
}

func newCausalIDs() (causal.OperationID, causal.OperationID, causal.ChangeID, error) {
	var operation causal.OperationID
	var sourceOperation causal.OperationID
	var change causal.ChangeID
	if _, err := rand.Read(operation[:]); err != nil {
		return operation, sourceOperation, change, err
	}
	if _, err := rand.Read(sourceOperation[:]); err != nil {
		return operation, sourceOperation, change, err
	}
	if _, err := rand.Read(change[:]); err != nil {
		return operation, sourceOperation, change, err
	}
	return operation, sourceOperation, change, nil
}

func contentRefs(entries []catalog.SourceDriverStageEntry) []catalog.ContentRef {
	refs := make([]catalog.ContentRef, 0, len(entries))
	for _, entry := range entries {
		if entry.Object != nil && entry.Object.Kind == catalog.KindFile {
			refs = append(refs, entry.Object.Content)
		}
	}
	return refs
}
