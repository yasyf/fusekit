package sourcedriverruntime

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourcedriver"
)

const targetSetInspectTimeout = 5 * time.Second

func (r *Runtime) prepareTargetDeclaration(
	ctx context.Context,
	stage catalog.SourceDriverStageState,
) (bool, error) {
	state, err := r.config.Store.PrepareSourceDriverTargetDeclarationBatch(ctx, stage.Identity)
	if err != nil {
		return false, err
	}
	if state.TargetEpoch != stage.TargetEpoch || state.TargetCount != stage.Identity.TargetCount ||
		state.DeclaredCount > state.TargetCount || state.Prepared &&
		(state.DeclaredCount != state.TargetCount || state.Digest != stage.Identity.TargetsDigest) {
		return false, catalog.ErrIntegrity
	}
	return state.Prepared, nil
}

func (r *Runtime) ensureTargetSet(
	ctx context.Context,
	stage catalog.SourceDriverStageState,
) (sourcedriver.TargetSetRef, bool, error) {
	ref, err := r.config.targetSetRef(stage.TargetEpoch)
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	state, err := r.config.Driver.InspectTargetSet(ctx, r.config.Authority, ref)
	if errors.Is(err, sourcedriver.ErrNotFound) {
		state, err = sourcedriver.NewTargetSetState(r.config.Authority, ref)
	}
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	if err := r.validateTargetSetState(ref, state); err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	if state.Complete {
		return ref, true, nil
	}
	last := min(state.DeclaredCount+uint64(sourcedriver.MaxTargetPageItems), ref.TargetCount)
	targets, err := r.config.Store.SourceDriverStageTargets(
		ctx, r.config.Authority, stage.Identity.Operation, state.After.Tenant, int(last-state.DeclaredCount),
	)
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	if uint64(len(targets)) != last-state.DeclaredCount {
		return sourcedriver.TargetSetRef{}, false, catalog.ErrIntegrity
	}
	page, err := sourcedriver.NewTargetSetPage(state, driverTargetDeclarations(targets))
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	expected, err := sourcedriver.ApplyTargetSetPage(state, page)
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	if err := r.config.Store.ValidateSourceDriverTargetEpoch(ctx, r.config.Authority, ref.TargetEpoch); err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	advanced, declareErr := r.config.Driver.DeclareTargetSet(ctx, r.config.Authority, page)
	if declareErr != nil {
		inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), targetSetInspectTimeout)
		advanced, err = r.config.Driver.InspectTargetSet(inspectCtx, r.config.Authority, ref)
		cancel()
		if err != nil || advanced != expected {
			return sourcedriver.TargetSetRef{}, false, errors.Join(declareErr, err)
		}
	}
	if err := r.validateTargetSetState(ref, advanced); err != nil || advanced != expected {
		return sourcedriver.TargetSetRef{}, false, errors.Join(
			sourcedriver.ErrIntegrity, err,
			fmt.Errorf("source driver runtime: target declaration result differs"),
		)
	}
	return ref, false, nil
}

func (r *Runtime) ensureMutationTargetSet(
	ctx context.Context,
	reservation catalog.SourceDriverMutationReservation,
) (sourcedriver.TargetSetRef, bool, error) {
	ref, err := mutationTargetSetRef(reservation)
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	state, err := r.config.Driver.InspectTargetSet(ctx, reservation.Authority, ref)
	if errors.Is(err, sourcedriver.ErrNotFound) {
		state, err = sourcedriver.NewTargetSetState(reservation.Authority, ref)
	}
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	if err := r.validateTargetSetState(ref, state); err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	if state.Complete {
		return ref, true, nil
	}
	last := min(state.DeclaredCount+uint64(sourcedriver.MaxTargetPageItems), ref.TargetCount)
	pageTargets, err := r.config.Store.SourceDriverMutationReservationTargets(
		ctx, reservation.Mutation, state.After.Tenant, int(last-state.DeclaredCount),
	)
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	if uint64(len(pageTargets.Targets)) != last-state.DeclaredCount {
		return sourcedriver.TargetSetRef{}, false, catalog.ErrIntegrity
	}
	wantNext := last < ref.TargetCount
	if (pageTargets.Next != "") != wantNext ||
		(wantNext && pageTargets.Next != pageTargets.Targets[len(pageTargets.Targets)-1].Tenant) {
		return sourcedriver.TargetSetRef{}, false, catalog.ErrIntegrity
	}
	page, err := sourcedriver.NewTargetSetPage(state, driverTargetDeclarations(pageTargets.Targets))
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	expected, err := sourcedriver.ApplyTargetSetPage(state, page)
	if err != nil {
		return sourcedriver.TargetSetRef{}, false, err
	}
	advanced, declareErr := r.config.Driver.DeclareTargetSet(ctx, reservation.Authority, page)
	if declareErr != nil {
		inspectCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), targetSetInspectTimeout)
		advanced, err = r.config.Driver.InspectTargetSet(inspectCtx, reservation.Authority, ref)
		cancel()
		if err != nil || advanced != expected {
			return sourcedriver.TargetSetRef{}, false, errors.Join(declareErr, err)
		}
	}
	if err := r.validateTargetSetState(ref, advanced); err != nil || advanced != expected {
		return sourcedriver.TargetSetRef{}, false, errors.Join(
			sourcedriver.ErrIntegrity, err,
			fmt.Errorf("source driver runtime: mutation target declaration result differs"),
		)
	}
	return ref, false, nil
}

func mutationTargetSetRef(
	reservation catalog.SourceDriverMutationReservation,
) (sourcedriver.TargetSetRef, error) {
	return sourcedriver.NewTargetSetRefForDigest(
		reservation.Authority,
		reservation.AuthorityGeneration,
		reservation.TargetEpoch,
		reservation.DeclarationDigest,
		reservation.TargetCount,
		reservation.TargetsDigest,
	)
}

func driverTargetDeclarations(targets []catalog.SourceDriverTarget) []sourcedriver.TargetDeclaration {
	values := make([]sourcedriver.TargetDeclaration, len(targets))
	for index, target := range targets {
		values[index] = sourcedriver.TargetDeclaration{
			Tenant: target.Tenant, Generation: causal.Generation(target.Generation),
		}
	}
	return values
}

func (r *Runtime) validateTargetSetState(
	ref sourcedriver.TargetSetRef,
	state sourcedriver.TargetSetState,
) error {
	if err := sourcedriver.ValidateTargetSetState(r.config.Authority, state); err != nil {
		return err
	}
	if state.Ref != ref {
		return fmt.Errorf("%w: target declaration reference differs", sourcedriver.ErrIntegrity)
	}
	return nil
}
