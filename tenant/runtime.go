package tenant

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
)

var (
	// ErrRecoveryActive means state recovery was requested while tenant work was active.
	ErrRecoveryActive = errors.New("tenant runtime: recovery requires idle actors")
	// ErrRecovering means the runtime is temporarily closed to preparation while
	// durable mutation and quarantine state is recovered.
	ErrRecovering = errors.New("tenant runtime: state recovery in progress")
)

// TenantRuntime owns one serialized convergence actor per tenant specification.
type TenantRuntime struct {
	store   Store
	planner Planner
	fleets  FleetTransitionHook
	owner   catalog.MutationOwnerID

	transitionMu    sync.Mutex
	lifecycleCtx    context.Context
	lifecycleCancel context.CancelFunc

	mu             sync.Mutex
	tenants        map[catalog.TenantID]*tenantSlot
	nextWaiter     uint64
	transitions    int
	closed         bool
	canceled       bool
	recovering     bool
	recoveryDone   chan struct{}
	recoveryCancel context.CancelFunc

	closeOnce          sync.Once
	cancelOnce         sync.Once
	actorLifecycleOnce sync.Once
	actorsClosed       chan struct{}
	cancellationDone   chan struct{}
}

type tenantSlot struct {
	spec          TenantSpec
	actor         *tenantActor
	admissions    int
	transitioning bool
	drained       chan struct{}
	pending       *pendingFleetTransition
}

type pendingFleetTransition struct {
	transition         FleetTransition
	next               TenantSpec
	expectedGeneration catalog.Generation
}

type actorShutdown struct {
	actor   *tenantActor
	drained <-chan struct{}
}

// GenerationLease fences one exact tenant actor generation across durable work.
type GenerationLease struct {
	runtime *TenantRuntime
	slot    *tenantSlot
	actor   *tenantActor
	tenant  catalog.TenantID
	once    sync.Once
}

// NewRuntime realizes one revision-fenced desired tenant snapshot.
func NewRuntime(
	ctx context.Context,
	store Store,
	planner Planner,
	fleets FleetTransitionHook,
	desired []catalog.TenantProvision,
) (*TenantRuntime, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("tenant runtime: initialize: %w", err)
	}
	if store == nil {
		return nil, errors.New("tenant runtime: store is required")
	}
	if planner == nil {
		return nil, errors.New("tenant runtime: planner is required")
	}
	if fleets == nil {
		return nil, errors.New("tenant runtime: fleet transition hook is required")
	}
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		return nil, fmt.Errorf("tenant runtime: generate mutation owner: %w", err)
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	r := &TenantRuntime{
		store:            store,
		planner:          planner,
		fleets:           fleets,
		owner:            owner,
		lifecycleCtx:     lifecycleCtx,
		lifecycleCancel:  lifecycleCancel,
		tenants:          make(map[catalog.TenantID]*tenantSlot),
		actorsClosed:     make(chan struct{}),
		cancellationDone: make(chan struct{}),
	}
	for index, provision := range desired {
		spec := provisionSpec(provision)
		if err := spec.validate(); err != nil {
			r.cancelRecoveredActors()
			return nil, fmt.Errorf("tenant runtime: load desired tenant %q: %w", spec.ID, err)
		}
		if index > 0 && desired[index-1].Tenant >= provision.Tenant {
			r.cancelRecoveredActors()
			return nil, fmt.Errorf("%w: desired tenant snapshot is not exact and ordered", catalog.ErrIntegrity)
		}
		actor := newTenantActor(r.store, r.planner, r.owner, spec)
		r.tenants[spec.ID] = &tenantSlot{spec: spec, actor: actor}
		<-actor.ready
		if actor.loadErr != nil {
			r.cancelRecoveredActors()
			return nil, actor.loadErr
		}
	}
	return r, nil
}

// ProvisionTenant durably creates one exact tenant definition before realizing
// its actor.
func (r *TenantRuntime) ProvisionTenant(ctx context.Context, spec TenantSpec) error {
	r.transitionMu.Lock()
	defer r.transitionMu.Unlock()
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("tenant runtime: provision tenant: %w", err)
	}
	if err := spec.validate(); err != nil {
		return err
	}
	r.mu.Lock()
	if err := r.admissionErrorLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	if r.recovering {
		r.mu.Unlock()
		return ErrRecovering
	}
	if slot, found := r.tenants[spec.ID]; found {
		if slot.transitioning {
			if slot.pending != nil && slot.pending.transition.Kind == FleetProvision && slot.pending.next == spec {
				pending := slot.pending
				r.mu.Unlock()
				return r.finishProvision(spec.ID, slot, pending)
			}
			r.mu.Unlock()
			return ErrTenantChanging
		}
		if slot.spec == spec {
			ready := slot.actor.ready
			r.mu.Unlock()
			<-ready
			return nil
		}
		r.mu.Unlock()
		return ErrTenantConflict
	}
	before := r.specsLocked()
	r.mu.Unlock()
	persisted, err := r.store.ProvisionTenant(context.WithoutCancel(ctx), tenantProvision(spec))
	if errors.Is(err, catalog.ErrTenantProvisionConflict) {
		return fmt.Errorf("%w: %v", ErrTenantConflict, err)
	}
	if err != nil {
		return fmt.Errorf("tenant runtime: persist tenant %q: %w", spec.ID, err)
	}
	spec = provisionSpec(persisted)
	r.mu.Lock()
	actor := newTenantActor(r.store, r.planner, r.owner, spec)
	drained := make(chan struct{})
	close(drained)
	slot := &tenantSlot{spec: spec, actor: actor, transitioning: true, drained: drained}
	r.tenants[spec.ID] = slot
	r.transitions++
	r.mu.Unlock()
	<-actor.ready
	if actor.loadErr != nil {
		r.mu.Lock()
		if current, ok := r.tenants[spec.ID]; ok && current.actor == actor {
			delete(r.tenants, spec.ID)
			r.transitions--
		}
		r.mu.Unlock()
		actor.cancel()
		<-actor.done
		return actor.loadErr
	}
	transition := fleetTransition(FleetProvision, before, spec.ID, &spec)
	r.mu.Lock()
	pending := &pendingFleetTransition{transition: transition, next: spec}
	slot.pending = pending
	r.mu.Unlock()
	return r.finishProvision(spec.ID, slot, pending)
}

// ReplaceTenant drains one generation and atomically registers its successor.
func (r *TenantRuntime) ReplaceTenant(ctx context.Context, expectedGeneration catalog.Generation, next TenantSpec) error {
	r.transitionMu.Lock()
	defer r.transitionMu.Unlock()
	if err := next.validate(); err != nil {
		return err
	}
	if expectedGeneration == 0 {
		return fmt.Errorf("%w: expected generation is required", ErrInvalidSpec)
	}
	r.mu.Lock()
	if err := r.admissionErrorLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	if r.recovering {
		r.mu.Unlock()
		return ErrRecovering
	}
	slot, found := r.tenants[next.ID]
	if !found {
		r.mu.Unlock()
		return ErrTenantNotFound
	}
	if slot.transitioning {
		if slot.pending != nil && slot.pending.transition.Kind == FleetReplace &&
			slot.pending.expectedGeneration == expectedGeneration && slot.pending.next == next {
			pending := slot.pending
			r.mu.Unlock()
			return r.finishReplace(next.ID, slot, pending)
		}
		r.mu.Unlock()
		return ErrTenantChanging
	}
	if slot.spec.Generation != expectedGeneration {
		r.mu.Unlock()
		return ErrGenerationConflict
	}
	if next.Generation <= expectedGeneration {
		r.mu.Unlock()
		return fmt.Errorf("%w: replacement generation %d must exceed %d", ErrInvalidSpec, next.Generation, expectedGeneration)
	}
	transition := fleetTransition(FleetReplace, r.specsLocked(), next.ID, &next)
	drained := r.beginTransitionLocked(slot)
	r.mu.Unlock()
	if err := r.awaitTransitionDrain(ctx, next.ID, slot, drained); err != nil {
		return err
	}
	if err := r.prepareFleet(ctx, transition); err != nil {
		r.abortTransition(next.ID, slot, drained)
		return fmt.Errorf("tenant runtime: drain authority fleet for tenant %q: %w", next.ID, err)
	}
	persisted, err := r.store.ReplaceTenantProvision(context.WithoutCancel(ctx), expectedGeneration, tenantProvision(next))
	if errors.Is(err, catalog.ErrTenantProvisionConflict) {
		if restoreErr := r.fleets.Abort(r.lifecycleCtx, transition); restoreErr != nil {
			return errors.Join(ErrGenerationConflict, fmt.Errorf("tenant runtime: restore authority fleet: %w", restoreErr))
		}
		r.abortTransition(next.ID, slot, drained)
		return ErrGenerationConflict
	}
	if err != nil {
		if restoreErr := r.fleets.Abort(r.lifecycleCtx, transition); restoreErr != nil {
			return errors.Join(fmt.Errorf("tenant runtime: persist replacement for tenant %q: %w", next.ID, err),
				fmt.Errorf("tenant runtime: restore authority fleet: %w", restoreErr))
		}
		r.abortTransition(next.ID, slot, drained)
		return fmt.Errorf("tenant runtime: persist replacement for tenant %q: %w", next.ID, err)
	}
	next = provisionSpec(persisted)
	transition.Committed = replaceFleetSpec(transition.Committed, next)
	r.mu.Lock()
	pending := &pendingFleetTransition{transition: transition, next: next, expectedGeneration: expectedGeneration}
	slot.pending = pending
	r.mu.Unlock()
	return r.finishReplace(next.ID, slot, pending)
}

// RemoveTenant drains and forgets one generation without deleting its durable data.
func (r *TenantRuntime) RemoveTenant(ctx context.Context, tenant catalog.TenantID, expectedGeneration catalog.Generation) error {
	r.transitionMu.Lock()
	defer r.transitionMu.Unlock()
	if expectedGeneration == 0 {
		return fmt.Errorf("%w: expected generation is required", ErrInvalidSpec)
	}
	r.mu.Lock()
	if err := r.admissionErrorLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	if r.recovering {
		r.mu.Unlock()
		return ErrRecovering
	}
	slot, found := r.tenants[tenant]
	if !found {
		r.mu.Unlock()
		return ErrTenantNotFound
	}
	if slot.transitioning {
		if slot.pending != nil && slot.pending.transition.Kind == FleetRemove &&
			slot.pending.expectedGeneration == expectedGeneration {
			pending := slot.pending
			r.mu.Unlock()
			return r.finishRemove(tenant, slot, pending)
		}
		r.mu.Unlock()
		return ErrTenantChanging
	}
	if slot.spec.Generation != expectedGeneration {
		r.mu.Unlock()
		return ErrGenerationConflict
	}
	transition := fleetTransition(FleetRemove, r.specsLocked(), tenant, nil)
	drained := r.beginTransitionLocked(slot)
	r.mu.Unlock()
	if err := r.awaitTransitionDrain(ctx, tenant, slot, drained); err != nil {
		return err
	}
	if err := r.prepareFleet(ctx, transition); err != nil {
		r.abortTransition(tenant, slot, drained)
		return fmt.Errorf("tenant runtime: drain authority fleet for tenant %q: %w", tenant, err)
	}
	if err := r.store.RemoveTenantProvision(context.WithoutCancel(ctx), tenant, expectedGeneration); err != nil {
		if restoreErr := r.fleets.Abort(r.lifecycleCtx, transition); restoreErr != nil {
			return errors.Join(fmt.Errorf("tenant runtime: persist removal for tenant %q: %w", tenant, err),
				fmt.Errorf("tenant runtime: restore authority fleet: %w", restoreErr))
		}
		r.abortTransition(tenant, slot, drained)
		if errors.Is(err, catalog.ErrNotFound) {
			return ErrGenerationConflict
		}
		return fmt.Errorf("tenant runtime: persist removal for tenant %q: %w", tenant, err)
	}
	r.mu.Lock()
	pending := &pendingFleetTransition{transition: transition, expectedGeneration: expectedGeneration}
	slot.pending = pending
	r.mu.Unlock()
	return r.finishRemove(tenant, slot, pending)
}

func (r *TenantRuntime) finishProvision(id catalog.TenantID, slot *tenantSlot, pending *pendingFleetTransition) error {
	if err := r.fleets.Commit(r.lifecycleCtx, pending.transition); err != nil {
		return fmt.Errorf("tenant runtime: commit authority fleet for provisioned tenant %q: %w", id, err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.tenants[id]; !ok || current != slot || slot.pending != pending {
		return fmt.Errorf("%w: provision slot changed", catalog.ErrIntegrity)
	}
	slot.pending = nil
	slot.transitioning = false
	slot.drained = nil
	r.transitions--
	return nil
}

func (r *TenantRuntime) prepareFleet(ctx context.Context, transition FleetTransition) error {
	prepareCtx, cancel := context.WithCancel(ctx)
	stopLifecycle := context.AfterFunc(r.lifecycleCtx, cancel)
	defer func() {
		stopLifecycle()
		cancel()
	}()
	return r.fleets.Prepare(prepareCtx, transition)
}

func (r *TenantRuntime) finishReplace(id catalog.TenantID, slot *tenantSlot, pending *pendingFleetTransition) error {
	if err := r.fleets.Commit(r.lifecycleCtx, pending.transition); err != nil {
		return fmt.Errorf("tenant runtime: commit authority fleet for tenant %q: %w", id, err)
	}
	slot.actor.close()
	<-slot.actor.done

	r.mu.Lock()
	if current, ok := r.tenants[id]; !ok || current != slot || slot.pending != pending {
		r.mu.Unlock()
		return fmt.Errorf("%w: replacement slot changed", catalog.ErrIntegrity)
	}
	r.transitions--
	slot.pending = nil
	if r.canceled {
		delete(r.tenants, id)
		r.mu.Unlock()
		return ErrCanceled
	}
	if r.closed {
		delete(r.tenants, id)
		r.mu.Unlock()
		return ErrClosed
	}
	actor := newTenantActor(r.store, r.planner, r.owner, pending.next)
	slot.spec = pending.next
	slot.actor = actor
	slot.transitioning = false
	slot.drained = nil
	r.mu.Unlock()
	<-actor.ready
	if actor.loadErr == nil {
		return nil
	}
	r.mu.Lock()
	if current, ok := r.tenants[id]; ok && current.actor == actor {
		delete(r.tenants, id)
	}
	r.mu.Unlock()
	actor.cancel()
	<-actor.done
	return actor.loadErr
}

func (r *TenantRuntime) finishRemove(id catalog.TenantID, slot *tenantSlot, pending *pendingFleetTransition) error {
	if err := r.fleets.Commit(r.lifecycleCtx, pending.transition); err != nil {
		return fmt.Errorf("tenant runtime: commit authority fleet after removing tenant %q: %w", id, err)
	}
	slot.actor.close()
	<-slot.actor.done

	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.tenants[id]; !ok || current != slot || slot.pending != pending {
		return fmt.Errorf("%w: removal slot changed", catalog.ErrIntegrity)
	}
	r.transitions--
	delete(r.tenants, id)
	return nil
}

// Specs returns a deterministic linearizable snapshot of fleet specifications.
func (r *TenantRuntime) Specs() []TenantSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.specsLocked()
}

func (r *TenantRuntime) specsLocked() []TenantSpec {
	specs := make([]TenantSpec, 0, len(r.tenants))
	for _, slot := range r.tenants {
		specs = append(specs, slot.spec)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].ID < specs[j].ID })
	return specs
}

func fleetTransition(kind FleetTransitionKind, before []TenantSpec, id catalog.TenantID, committed *TenantSpec) FleetTransition {
	transition := FleetTransition{Kind: kind, Before: append([]TenantSpec(nil), before...)}
	for _, spec := range before {
		if spec.ID != id {
			transition.Drained = append(transition.Drained, spec)
		}
	}
	transition.Committed = append([]TenantSpec(nil), transition.Drained...)
	if committed != nil {
		transition.Committed = append(transition.Committed, *committed)
		sort.Slice(transition.Committed, func(i, j int) bool { return transition.Committed[i].ID < transition.Committed[j].ID })
	}
	return transition
}

func replaceFleetSpec(fleet []TenantSpec, spec TenantSpec) []TenantSpec {
	result := append([]TenantSpec(nil), fleet...)
	for index := range result {
		if result[index].ID == spec.ID {
			result[index] = spec
			sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
			return result
		}
	}
	result = append(result, spec)
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result
}

// PrepareTenant converges one tenant to at least revision. Calls for the same
// or older revision share the latest required work.
func (r *TenantRuntime) PrepareTenant(ctx context.Context, tenant catalog.TenantID, revision catalog.Revision) (TenantState, error) {
	if revision == 0 {
		return TenantState{}, errors.New("tenant runtime: revision is required")
	}
	r.mu.Lock()
	if err := r.admissionErrorLocked(); err != nil {
		r.mu.Unlock()
		return TenantState{}, err
	}
	if r.recovering {
		r.mu.Unlock()
		return TenantState{}, ErrRecovering
	}
	slot, ok := r.tenants[tenant]
	if !ok {
		r.mu.Unlock()
		return TenantState{}, ErrTenantNotFound
	}
	if slot.transitioning {
		r.mu.Unlock()
		return TenantState{}, ErrTenantChanging
	}
	slot.admissions++
	r.nextWaiter++
	waiterID := r.nextWaiter
	r.mu.Unlock()
	defer r.releaseAdmission(slot)

	response := make(chan prepareResult, 1)
	request := prepareRequest{id: waiterID, revision: revision, response: response}
	if err := slot.actor.send(ctx, request); err != nil {
		return TenantState{}, err
	}
	select {
	case result := <-response:
		return result.state, result.err
	case <-ctx.Done():
		slot.actor.cancelWaiter(waiterID)
		return TenantState{}, fmt.Errorf("tenant runtime: prepare tenant %q: %w", tenant, ctx.Err())
	}
}

// AcquireGeneration admits durable work only for one exact live generation.
func (r *TenantRuntime) AcquireGeneration(ctx context.Context, tenant catalog.TenantID, generation catalog.Generation) (*GenerationLease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if generation == 0 {
		return nil, ErrGenerationConflict
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.admissionErrorLocked(); err != nil {
		return nil, err
	}
	if r.recovering {
		return nil, ErrRecovering
	}
	slot, ok := r.tenants[tenant]
	if !ok {
		return nil, ErrTenantNotFound
	}
	if slot.transitioning {
		return nil, ErrTenantChanging
	}
	if slot.spec.Generation != generation {
		return nil, ErrGenerationConflict
	}
	slot.admissions++
	return &GenerationLease{runtime: r, slot: slot, actor: slot.actor, tenant: tenant}, nil
}

// Prepare converges the actor held by this exact-generation lease.
func (l *GenerationLease) Prepare(ctx context.Context, revision catalog.Revision) (TenantState, error) {
	if l == nil || l.runtime == nil || l.slot == nil || l.actor == nil {
		return TenantState{}, ErrGenerationConflict
	}
	if revision == 0 {
		return TenantState{}, errors.New("tenant runtime: revision is required")
	}
	l.runtime.mu.Lock()
	l.runtime.nextWaiter++
	waiterID := l.runtime.nextWaiter
	l.runtime.mu.Unlock()
	response := make(chan prepareResult, 1)
	request := prepareRequest{id: waiterID, revision: revision, response: response}
	if err := l.actor.send(ctx, request); err != nil {
		return TenantState{}, err
	}
	select {
	case result := <-response:
		return result.state, result.err
	case <-ctx.Done():
		l.actor.cancelWaiter(waiterID)
		return TenantState{}, fmt.Errorf("tenant runtime: prepare tenant %q: %w", l.tenant, ctx.Err())
	}
}

// Spec returns the immutable tenant definition fenced by this generation lease.
func (l *GenerationLease) Spec() (TenantSpec, error) {
	if l == nil || l.runtime == nil || l.slot == nil || l.actor == nil {
		return TenantSpec{}, ErrGenerationConflict
	}
	return l.slot.spec, nil
}

// Release retires the generation admission. It is idempotent.
func (l *GenerationLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.runtime.releaseAdmission(l.slot)
	})
}

// State returns one owner-fenced durable lifecycle snapshot.
func (r *TenantRuntime) State(ctx context.Context, owner OwnerID, tenant catalog.TenantID) (TenantStatus, error) {
	if owner == "" {
		return TenantStatus{}, fmt.Errorf("%w: owner is required", ErrInvalidSpec)
	}
	r.mu.Lock()
	if err := r.admissionErrorLocked(); err != nil {
		r.mu.Unlock()
		return TenantStatus{}, err
	}
	if r.recovering {
		r.mu.Unlock()
		return TenantStatus{}, ErrRecovering
	}
	slot, ok := r.tenants[tenant]
	if !ok {
		r.mu.Unlock()
		return TenantStatus{}, ErrTenantNotFound
	}
	if slot.spec.OwnerID != owner {
		r.mu.Unlock()
		return TenantStatus{}, ErrTenantOwnerMismatch
	}
	if slot.transitioning {
		r.mu.Unlock()
		return TenantStatus{}, ErrTenantChanging
	}
	slot.admissions++
	r.mu.Unlock()
	defer r.releaseAdmission(slot)
	response := make(chan prepareResult, 1)
	if err := slot.actor.send(ctx, stateRequest{response: response}); err != nil {
		return TenantStatus{}, err
	}
	select {
	case result := <-response:
		if result.err != nil {
			return TenantStatus{}, result.err
		}
		return TenantStatus{Owner: owner, State: result.state, ReplacementEligible: true}, nil
	case <-ctx.Done():
		return TenantStatus{}, fmt.Errorf("tenant runtime: read tenant %q state: %w", tenant, ctx.Err())
	}
}

// Recover replays exact mutation recovery and releases only unsettled quarantine records.
func (r *TenantRuntime) Recover(ctx context.Context) error {
	r.mu.Lock()
	if err := r.admissionErrorLocked(); err != nil {
		r.mu.Unlock()
		return err
	}
	if r.recovering {
		r.mu.Unlock()
		return ErrRecovering
	}
	if r.transitions != 0 || r.hasAdmissionsLocked() {
		r.mu.Unlock()
		return ErrRecoveryActive
	}
	recoveryCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	r.recovering = true
	r.recoveryDone = done
	r.recoveryCancel = cancel
	actors := r.actorSliceLocked()
	r.mu.Unlock()
	paused := make([]*tenantActor, 0, len(actors))
	defer func() {
		for _, actor := range paused {
			actor.resumeRecovery()
		}
		r.mu.Lock()
		r.recovering = false
		r.recoveryDone = nil
		r.recoveryCancel = nil
		close(done)
		r.mu.Unlock()
		cancel()
	}()

	for _, actor := range actors {
		active, err := actor.pauseRecovery(recoveryCtx)
		if err != nil {
			return err
		}
		paused = append(paused, actor)
		if active {
			return ErrRecoveryActive
		}
	}
	for _, actor := range actors {
		if err := actor.recover(recoveryCtx); err != nil {
			return err
		}
	}
	return nil
}

// Close rejects new preparation and lets every admitted actor operation settle.
func (r *TenantRuntime) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		recoveryCancel := r.recoveryCancel
		r.mu.Unlock()
		if recoveryCancel != nil {
			recoveryCancel()
		}
		r.closeActorsAfterTransitions()
	})
}

// Cancel rejects new preparation and cancels every active tenant operation.
func (r *TenantRuntime) Cancel() {
	r.cancelOnce.Do(func() {
		r.lifecycleCancel()
		r.mu.Lock()
		r.closed = true
		r.canceled = true
		recoveryCancel := r.recoveryCancel
		actors := r.actorSliceLocked()
		r.mu.Unlock()
		if recoveryCancel != nil {
			recoveryCancel()
		}
		for _, actor := range actors {
			actor.cancel()
		}
		go func() {
			defer close(r.cancellationDone)
			<-r.actorsClosed
		}()
		r.closeActorsAfterTransitions()
	})
}

func (r *TenantRuntime) closeActorsAfterTransitions() {
	r.actorLifecycleOnce.Do(func() {
		go func() {
			r.transitionMu.Lock()
			defer r.transitionMu.Unlock()
			r.mu.Lock()
			shutdowns := r.shutdownsLocked()
			recoveryDone := r.recoveryDone
			r.mu.Unlock()
			waitSignal(recoveryDone)
			for _, shutdown := range shutdowns {
				<-shutdown.drained
				r.mu.Lock()
				canceled := r.canceled
				r.mu.Unlock()
				if canceled {
					shutdown.actor.cancel()
				} else {
					shutdown.actor.close()
				}
				<-shutdown.actor.done
			}
			close(r.actorsClosed)
		}()
	})
}

// Wait joins every tenant actor. A caller context error is returned only after
// every admitted actor operation has settled.
func (r *TenantRuntime) Wait(ctx context.Context) error {
	done := ctx.Done()
	var ctxErr error
	select {
	case <-r.actorsClosed:
	case <-done:
		ctxErr = ctx.Err()
		r.Cancel()
		<-r.actorsClosed
	}
	r.mu.Lock()
	canceled := r.canceled
	r.mu.Unlock()
	if canceled {
		<-r.cancellationDone
	}
	if ctxErr != nil {
		return fmt.Errorf("tenant runtime: wait: %w", ctxErr)
	}
	return nil
}

func (r *TenantRuntime) actorSliceLocked() []*tenantActor {
	actors := make([]*tenantActor, 0, len(r.tenants))
	for _, slot := range r.tenants {
		actors = append(actors, slot.actor)
	}
	return actors
}

func (r *TenantRuntime) admissionErrorLocked() error {
	if r.canceled {
		return ErrCanceled
	}
	if r.closed {
		return ErrClosed
	}
	return nil
}

func (r *TenantRuntime) hasAdmissionsLocked() bool {
	for _, slot := range r.tenants {
		if slot.admissions != 0 {
			return true
		}
	}
	return false
}

func (r *TenantRuntime) releaseAdmission(slot *tenantSlot) {
	r.mu.Lock()
	slot.admissions--
	if slot.admissions < 0 {
		r.mu.Unlock()
		panic("tenant runtime: negative admission count")
	}
	if slot.transitioning && slot.admissions == 0 {
		close(slot.drained)
	}
	r.mu.Unlock()
}

func (r *TenantRuntime) beginTransitionLocked(slot *tenantSlot) <-chan struct{} {
	slot.transitioning = true
	slot.drained = make(chan struct{})
	r.transitions++
	if slot.admissions == 0 {
		close(slot.drained)
	}
	return slot.drained
}

func (r *TenantRuntime) awaitTransitionDrain(
	ctx context.Context,
	tenant catalog.TenantID,
	slot *tenantSlot,
	drained <-chan struct{},
) error {
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
	}
	r.mu.Lock()
	current, found := r.tenants[tenant]
	if !r.closed && !r.canceled && found && current == slot && slot.transitioning && slot.drained == drained {
		slot.transitioning = false
		slot.drained = nil
		r.transitions--
		r.mu.Unlock()
		return fmt.Errorf("tenant runtime: change tenant %q: %w", tenant, ctx.Err())
	}
	r.mu.Unlock()
	<-drained
	return nil
}

func (r *TenantRuntime) abortTransition(id catalog.TenantID, slot *tenantSlot, drained <-chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, found := r.tenants[id]
	if !found || current != slot || !slot.transitioning || slot.drained != drained {
		panic("tenant runtime: durable transition lost its slot")
	}
	slot.transitioning = false
	slot.drained = nil
	r.transitions--
}

func (r *TenantRuntime) cancelRecoveredActors() {
	for _, slot := range r.tenants {
		slot.actor.cancel()
	}
	for _, slot := range r.tenants {
		<-slot.actor.done
	}
	r.tenants = make(map[catalog.TenantID]*tenantSlot)
}

func (r *TenantRuntime) shutdownsLocked() []actorShutdown {
	shutdowns := make([]actorShutdown, 0, len(r.tenants))
	for _, slot := range r.tenants {
		if !slot.transitioning {
			slot.transitioning = true
			slot.drained = make(chan struct{})
			if slot.admissions == 0 {
				close(slot.drained)
			}
		}
		shutdowns = append(shutdowns, actorShutdown{actor: slot.actor, drained: slot.drained})
	}
	return shutdowns
}

func waitSignal(signal <-chan struct{}) {
	if signal != nil {
		<-signal
	}
}

type prepareRequest struct {
	id       uint64
	revision catalog.Revision
	response chan<- prepareResult
}

type cancelWaiterRequest struct{ id uint64 }

type stateRequest struct{ response chan<- prepareResult }

type pauseRecoveryRequest struct{ response chan<- bool }

type resumeRecoveryRequest struct{ ack chan<- struct{} }

type recoverRequest struct{ response chan<- error }

type closeRequest struct{ ack chan<- struct{} }

type cancelRequest struct{ ack chan<- struct{} }

type prepareResult struct {
	state TenantState
	err   error
}

type waiter struct {
	revision catalog.Revision
	response chan<- prepareResult
}

type activeRevision struct {
	revision  catalog.Revision
	cancel    context.CancelFunc
	abandoned bool
}

type progress struct {
	revision catalog.Revision
	lane     Lane
	ack      chan<- error
}

type executionResult struct {
	revision catalog.Revision
	lane     Lane
	err      error
}

type tenantActor struct {
	store   Store
	planner Planner
	owner   catalog.MutationOwnerID
	spec    TenantSpec

	ctx       context.Context
	cancelCtx context.CancelFunc
	in        chan any
	progress  chan progress
	executed  chan executionResult
	done      chan struct{}
	ready     chan struct{}

	record     catalog.TenantStateRecord
	loadErr    error
	waiters    map[uint64]waiter
	active     *activeRevision
	closing    bool
	canceled   bool
	paused     bool
	closeOnce  sync.Once
	cancelOnce sync.Once
}

func newTenantActor(store Store, planner Planner, owner catalog.MutationOwnerID, spec TenantSpec) *tenantActor {
	ctx, cancel := context.WithCancel(context.Background())
	a := &tenantActor{
		store:     store,
		planner:   planner,
		owner:     owner,
		spec:      spec,
		ctx:       ctx,
		cancelCtx: cancel,
		in:        make(chan any, 128),
		progress:  make(chan progress),
		executed:  make(chan executionResult, 1),
		done:      make(chan struct{}),
		ready:     make(chan struct{}),
		waiters:   make(map[uint64]waiter),
	}
	go a.run()
	return a
}

func (a *tenantActor) send(ctx context.Context, request any) error {
	select {
	case a.in <- request:
		return nil
	case <-a.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *tenantActor) cancelWaiter(id uint64) {
	select {
	case a.in <- cancelWaiterRequest{id: id}:
	case <-a.done:
	}
}

func (a *tenantActor) pauseRecovery(ctx context.Context) (bool, error) {
	response := make(chan bool, 1)
	if err := a.send(ctx, pauseRecoveryRequest{response: response}); err != nil {
		return false, err
	}
	select {
	case active := <-response:
		return active, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (a *tenantActor) resumeRecovery() {
	ack := make(chan struct{}, 1)
	select {
	case a.in <- resumeRecoveryRequest{ack: ack}:
	case <-a.done:
		return
	}
	select {
	case <-ack:
	case <-a.done:
	}
}

func (a *tenantActor) recover(ctx context.Context) error {
	response := make(chan error, 1)
	if err := a.send(ctx, recoverRequest{response: response}); err != nil {
		return err
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *tenantActor) close() {
	a.closeOnce.Do(func() {
		ack := make(chan struct{}, 1)
		select {
		case a.in <- closeRequest{ack: ack}:
		case <-a.done:
			return
		}
		select {
		case <-ack:
		case <-a.done:
		}
	})
}

func (a *tenantActor) cancel() {
	a.cancelOnce.Do(func() {
		ack := make(chan struct{}, 1)
		select {
		case a.in <- cancelRequest{ack: ack}:
		case <-a.done:
			return
		}
		select {
		case <-ack:
		case <-a.done:
		}
	})
}

func (a *tenantActor) run() {
	defer close(a.done)
	a.load()
	close(a.ready)
	for {
		a.maybeStart()
		if a.shouldExit() {
			return
		}
		select {
		case request := <-a.in:
			a.handleRequest(request)
		case update := <-a.progress:
			a.handleProgress(update)
		case result := <-a.executed:
			a.handleExecution(result)
		}
	}
}

func (a *tenantActor) load() {
	metadata, err := a.store.Tenant(a.ctx, a.spec.ID)
	if err != nil {
		a.loadErr = fmt.Errorf("tenant runtime: load tenant %q metadata: %w", a.spec.ID, err)
		return
	}
	if metadata.CasePolicy != a.spec.Traits.CaseSensitivity || metadata.Presentations != a.spec.Traits.Presentations {
		a.loadErr = fmt.Errorf("%w: catalog metadata does not match tenant %q traits", ErrTenantConflict, a.spec.ID)
		return
	}
	record, err := a.store.LoadTenantState(a.ctx, a.spec.ID)
	if errors.Is(err, catalog.ErrStateNotFound) {
		a.record = catalog.TenantStateRecord{Tenant: a.spec.ID, Generation: a.spec.Generation}
	} else if err != nil {
		a.loadErr = fmt.Errorf("tenant runtime: load tenant %q state: %w", a.spec.ID, err)
		return
	} else {
		a.record = record
		if record.Generation != a.spec.Generation &&
			(record.Quarantine == nil || record.Quarantine.Cause != catalog.QuarantineCauseUnsettled) {
			a.resetGeneration()
		}
	}
	if a.loadErr != nil {
		return
	}
	pending, err := a.store.PendingMutation(a.ctx, a.spec.ID)
	if err != nil {
		a.loadErr = fmt.Errorf("tenant runtime: load pending mutation for tenant %q: %w", a.spec.ID, err)
		return
	}
	if pending != nil {
		mutation := *pending
		var cause catalog.QuarantineCause
		var detail string
		quarantine := true
		switch mutation.State {
		case catalog.MutationApplying:
			cause = catalog.QuarantineCauseUnsettled
			detail = catalog.ErrMutationClaimed.Error()
		case catalog.MutationRecoveryRequired:
			cause = catalog.QuarantineCauseConflict
			detail = catalog.ErrMutationRecoveryRequired.Error()
		default:
			quarantine = false
		}
		if quarantine {
			if a.record.Quarantine != nil {
				return
			}
			revision := mutation.ExpectedHead
			if revision == 0 {
				revision = 1
			}
			next := a.record
			next.Quarantine = &catalog.Quarantine{
				Lane: LaneCatalogMutation, Revision: revision,
				Cause: cause, Detail: detail,
				Since: time.Now().UTC(),
			}
			if err := a.save(next); err != nil {
				a.loadErr = err
			}
			return
		}
	}
	if a.record.Version == 0 {
		if err := a.save(a.record); err != nil {
			a.loadErr = err
			return
		}
	}
	if err := a.ensureGenerationActivation(a.ctx); err != nil {
		a.loadErr = fmt.Errorf("tenant runtime: activate tenant %q presentation: %w", a.spec.ID, err)
	}
}

func (a *tenantActor) resetGeneration() {
	next := catalog.TenantStateRecord{Tenant: a.spec.ID, Generation: a.spec.Generation, Version: a.record.Version}
	saved, err := a.store.SaveTenantState(context.WithoutCancel(a.ctx), a.record.Version, next)
	if err != nil {
		a.loadErr = fmt.Errorf("tenant runtime: reset tenant %q generation: %w", a.spec.ID, err)
		return
	}
	a.record = saved
}

func (a *tenantActor) handleRequest(request any) {
	switch request := request.(type) {
	case prepareRequest:
		a.handlePrepare(request)
	case cancelWaiterRequest:
		if _, exists := a.waiters[request.id]; exists {
			delete(a.waiters, request.id)
			if len(a.waiters) == 0 && a.active != nil {
				a.active.abandoned = true
				a.active.cancel()
			}
		}
	case stateRequest:
		request.response <- prepareResult{state: stateFor(0, a.record), err: a.loadErr}
	case pauseRecoveryRequest:
		a.paused = true
		request.response <- a.active != nil
	case resumeRecoveryRequest:
		a.paused = false
		request.ack <- struct{}{}
	case recoverRequest:
		request.response <- a.handleRecover()
	case closeRequest:
		a.closing = true
		request.ack <- struct{}{}
	case cancelRequest:
		a.closing = true
		a.canceled = true
		a.completeAll(ErrCanceled)
		a.cancelCtx()
		request.ack <- struct{}{}
	default:
		panic(fmt.Sprintf("tenant runtime: unknown actor request %T", request))
	}
}

func (a *tenantActor) handlePrepare(request prepareRequest) {
	if a.loadErr != nil {
		request.response <- prepareResult{err: a.loadErr}
		return
	}
	if a.closing {
		request.response <- prepareResult{err: ErrClosed}
		return
	}
	if a.record.Generation != a.spec.Generation {
		state := stateFor(request.revision, a.record)
		request.response <- prepareResult{state: state, err: &QuarantinedError{State: state}}
		return
	}
	if a.record.Quarantine != nil {
		if a.record.Quarantine.Cause == catalog.QuarantineCauseUnsettled {
			state := stateFor(request.revision, a.record)
			request.response <- prepareResult{state: state, err: &QuarantinedError{State: state}}
			return
		}
		if a.record.Quarantine.Lane == LaneCatalogMutation && a.record.Quarantine.Cause == catalog.QuarantineCauseConflict {
			blocked, err := a.recoveryRequiredPending(a.ctx)
			if err != nil {
				request.response <- prepareResult{err: err}
				return
			}
			if blocked {
				state := stateFor(request.revision, a.record)
				request.response <- prepareResult{state: state, err: &QuarantinedError{State: state}}
				return
			}
		}
		if err := a.clearQuarantine(); err != nil {
			request.response <- prepareResult{err: err}
			return
		}
	}
	if state := stateFor(request.revision, a.record); state.Prepared() {
		request.response <- prepareResult{state: state}
		return
	}
	if request.revision > a.record.Desired {
		next := a.record
		next.Desired = request.revision
		if err := a.save(next); err != nil {
			request.response <- prepareResult{err: err}
			return
		}
		if a.active != nil && a.active.revision < request.revision {
			a.active.cancel()
		}
	}
	a.waiters[request.id] = waiter{revision: request.revision, response: request.response}
}

func (a *tenantActor) handleProgress(update progress) {
	if a.active == nil || update.revision != a.active.revision || update.revision < a.record.Desired {
		update.ack <- context.Canceled
		return
	}
	next := a.record
	switch update.lane {
	case LaneCatalogMutation:
		next.Observed = update.revision
	case LaneMaterialization:
		next.Verified = update.revision
		next.Applied = update.revision
	default:
		panic(fmt.Sprintf("tenant runtime: unknown lane %d", update.lane))
	}
	if next == a.record {
		update.ack <- nil
		return
	}
	update.ack <- a.save(next)
}

func (a *tenantActor) handleExecution(result executionResult) {
	if a.active == nil || result.revision != a.active.revision {
		return
	}
	abandoned := a.active.abandoned
	a.active.cancel()
	a.active = nil
	if result.err == nil {
		a.completePrepared()
		return
	}
	if errors.Is(result.err, context.Canceled) && result.revision < a.record.Desired {
		return
	}
	if errors.Is(result.err, context.Canceled) && abandoned {
		return
	}
	if a.canceled && errors.Is(result.err, context.Canceled) {
		return
	}
	a.quarantine(result, quarantineCause(result.err))
}

func (a *tenantActor) handleRecover() error {
	if a.active != nil {
		return ErrRecoveryActive
	}
	if a.record.Quarantine == nil || a.record.Quarantine.Cause != catalog.QuarantineCauseUnsettled {
		return nil
	}
	if a.record.Generation != a.spec.Generation {
		a.resetGeneration()
		return a.loadErr
	}
	if err := a.reclaimPendingMutations(a.ctx); err != nil {
		return err
	}
	return a.clearQuarantine()
}

func (a *tenantActor) clearQuarantine() error {
	next := a.record
	next.Quarantine = nil
	return a.save(next)
}

func (a *tenantActor) quarantine(result executionResult, cause catalog.QuarantineCause) {
	next := a.record
	next.Quarantine = &catalog.Quarantine{
		Lane:     result.lane,
		Revision: result.revision,
		Cause:    cause,
		Detail:   result.err.Error(),
		Since:    time.Now().UTC(),
	}
	err := a.save(next)
	if err != nil {
		a.completeAll(err)
		return
	}
	state := stateFor(result.revision, a.record)
	a.completeAll(&QuarantinedError{State: state})
}

func (a *tenantActor) save(next catalog.TenantStateRecord) error {
	saved, err := a.store.SaveTenantState(context.WithoutCancel(a.ctx), a.record.Version, next)
	if err != nil {
		return fmt.Errorf("tenant runtime: save tenant %q state: %w", a.spec.ID, err)
	}
	a.record = saved
	return nil
}

func (a *tenantActor) maybeStart() {
	if a.active != nil || a.canceled || a.paused || len(a.waiters) == 0 ||
		a.loadErr != nil || a.record.Quarantine != nil ||
		stateFor(a.record.Desired, a.record).Prepared() {
		return
	}
	revision := a.record.Desired
	ctx, cancel := context.WithCancel(a.ctx)
	a.active = &activeRevision{revision: revision, cancel: cancel}
	go func() {
		a.executed <- a.execute(ctx, revision)
	}()
}

func (a *tenantActor) execute(ctx context.Context, revision catalog.Revision) executionResult {
	if err := a.replayPendingMutations(ctx); err != nil {
		return laneFailure(revision, LaneCatalogMutation, "replay pending mutations", err)
	}
	head, err := a.store.Head(ctx, a.spec.ID)
	if err != nil {
		return laneFailure(revision, LaneCatalogMutation, "read catalog head", err)
	}
	if head < revision {
		return laneFailure(revision, LaneCatalogMutation, "prove catalog revision", fmt.Errorf("%w: catalog head %d is behind requested revision %d", catalog.ErrIntegrity, head, revision))
	}
	record, err := a.store.LoadTenantState(ctx, a.spec.ID)
	if err != nil {
		return laneFailure(revision, LaneCatalogMutation, "load exact tenant state", err)
	}
	if record.Tenant != a.spec.ID || record.Generation != a.spec.Generation || record.ActivatedGeneration != a.spec.Generation || record.Desired < revision {
		return laneFailure(revision, LaneCatalogMutation, "prove exact tenant state", fmt.Errorf("%w: catalog tenant state does not match requested generation", catalog.ErrIntegrity))
	}
	if err := a.recordProgress(ctx, revision, LaneCatalogMutation); err != nil {
		return executionResult{revision: revision, lane: LaneCatalogMutation, err: err}
	}

	if a.record.Verified < revision {
		if err := a.store.VerifyMaterialization(ctx, a.spec.ID, a.spec.Generation, revision); err != nil {
			return laneFailure(revision, LaneMaterialization, "verify materialization", err)
		}
	}
	if err := a.recordProgress(ctx, revision, LaneMaterialization); err != nil {
		return executionResult{revision: revision, lane: LaneMaterialization, err: err}
	}
	return executionResult{revision: revision, lane: LaneMaterialization}
}

func (a *tenantActor) ensureGenerationActivation(ctx context.Context) error {
	if a.record.ActivatedGeneration == a.spec.Generation {
		return nil
	}
	next := a.record
	next.ActivatedGeneration = a.spec.Generation
	return a.save(next)
}

func (a *tenantActor) replayPendingMutations(ctx context.Context) error {
	for {
		pending, err := a.store.PendingMutation(ctx, a.spec.ID)
		if err != nil {
			return err
		}
		if pending == nil {
			return nil
		}
		mutation := *pending
		if mutation.Tenant != a.spec.ID || mutation.OperationID == (catalog.MutationID{}) {
			return fmt.Errorf("%w: pending mutation identity mismatch", catalog.ErrIntegrity)
		}
		switch mutation.State {
		case catalog.MutationPrepared:
			mutation, err = a.store.ClaimMutation(ctx, mutation.OperationID, a.owner)
			if err != nil {
				return err
			}
			if err := a.applyClaimedMutation(ctx, mutation); err != nil {
				return err
			}
		case catalog.MutationApplying:
			if mutation.Claim == nil || mutation.Claim.Owner != a.owner {
				return catalog.ErrMutationClaimed
			}
			if err := a.applyClaimedMutation(ctx, mutation); err != nil {
				return err
			}
		case catalog.MutationApplied:
		case catalog.MutationRecoveryRequired:
			return catalog.ErrMutationRecoveryRequired
		default:
			return fmt.Errorf("%w: pending mutation %s has state %d", catalog.ErrIntegrity, mutation.OperationID, mutation.State)
		}
		committed, err := a.store.PreparedMutation(ctx, a.spec.ID, mutation.OperationID)
		if err != nil {
			return err
		}
		switch committed.State {
		case catalog.MutationApplied:
			if _, err := a.store.CommitMutation(ctx, a.spec.ID, mutation.OperationID); err != nil {
				return err
			}
		case catalog.MutationCommitted:
		default:
			return fmt.Errorf(
				"%w: source mutation %s remained in state %d after apply",
				catalog.ErrIntegrity, mutation.OperationID, committed.State,
			)
		}
		if err := a.planner.SourceMutationCommitted(ctx, SourceMutationCommit{
			OperationID: committed.OperationID, SourceID: committed.Intent.SourceID,
		}); err != nil {
			return err
		}
	}
}

func (a *tenantActor) applyClaimedMutation(ctx context.Context, mutation catalog.PreparedMutation) error {
	if mutation.OperationID == (catalog.MutationID{}) || mutation.Tenant != a.spec.ID || mutation.State != catalog.MutationApplying || mutation.Claim == nil {
		return fmt.Errorf("%w: claimed mutation identity mismatch", catalog.ErrIntegrity)
	}
	mutation, err := a.store.PrepareMutationSource(ctx, mutation.OperationID, *mutation.Claim)
	if err != nil {
		return err
	}
	if mutation.Source == nil {
		return fmt.Errorf("%w: claimed mutation has no source context", catalog.ErrIntegrity)
	}
	step := SourceMutationStep{
		TenantID: a.spec.ID, Generation: a.spec.Generation, OperationID: mutation.OperationID,
		SourceID: mutation.Intent.SourceID, SourceMetadata: mutation.Intent.SourceMetadata,
		Kind: mutation.Kind, ExpectedHead: mutation.ExpectedHead, Origin: mutation.Intent.Origin,
		Source: *mutation.Source,
	}
	operation, err := a.planner.PrepareSourceMutation(ctx, step)
	if err != nil {
		return err
	}
	if operation.OperationID != step.OperationID || operation.SourceID != step.SourceID || operation.SourceMetadata != step.SourceMetadata {
		return fmt.Errorf("%w: source operation identity does not match persisted operation", catalog.ErrIntegrity)
	}
	if mutation.Kind == catalog.MutationCreate {
		if operation.SourceResult == nil {
			return fmt.Errorf("%w: create source operation has no authority result", catalog.ErrIntegrity)
		}
		mutation, err = a.store.SetMutationSourceResult(ctx, mutation.OperationID, *mutation.Claim, *operation.SourceResult)
		if err != nil {
			return err
		}
		if mutation.SourceResult == nil || *mutation.SourceResult != *operation.SourceResult {
			return fmt.Errorf("%w: source result reservation changed", catalog.ErrIntegrity)
		}
	} else if operation.SourceResult != nil {
		return fmt.Errorf("%w: non-create source operation returned an authority result", catalog.ErrIntegrity)
	}
	var content SourceMutationContent
	if mutationCarriesFileContent(mutation.Intent) {
		content = catalogMutationContent{store: a.store, tenant: mutation.Tenant, operation: mutation.OperationID}
	}
	apply, err := a.planner.ApplySourceMutation(ctx, step, operation, content)
	if err != nil {
		return err
	}
	if apply.Settlement != operation.ExpectedSettlement {
		return fmt.Errorf("%w: source mutation settlement differs from its prepared contract", catalog.ErrIntegrity)
	}
	switch apply.Settlement {
	case SourceMutationExternalApplied:
	case SourceMutationCatalogCommitted:
		committed, err := a.store.PreparedMutation(ctx, mutation.Tenant, mutation.OperationID)
		if err != nil {
			return err
		}
		if committed.State != catalog.MutationCommitted {
			return fmt.Errorf("%w: atomic source mutation did not commit", catalog.ErrIntegrity)
		}
		return nil
	default:
		return fmt.Errorf("%w: source mutation returned no settlement proof", catalog.ErrIntegrity)
	}
	if _, err := a.store.MarkMutationApplied(ctx, mutation.OperationID, *mutation.Claim); err != nil {
		return err
	}
	return nil
}

type catalogMutationContent struct {
	store     Catalog
	tenant    catalog.TenantID
	operation catalog.MutationID
}

func (c catalogMutationContent) Open(ctx context.Context) (contentstream.Source, error) {
	return c.store.OpenMutationContent(ctx, c.tenant, c.operation)
}

func (catalogMutationContent) Close() error { return nil }

func mutationCarriesFileContent(intent catalog.MutationIntent) bool {
	switch {
	case intent.Create != nil:
		return intent.Create.Spec.Kind == catalog.KindFile
	case intent.Revise != nil:
		return intent.Revise.Spec.Content != nil
	case intent.Replace != nil:
		return intent.Replace.Content != nil
	default:
		return false
	}
}

func (a *tenantActor) reclaimPendingMutations(ctx context.Context) error {
	pending, err := a.store.PendingMutation(ctx, a.spec.ID)
	if err != nil {
		return err
	}
	if pending == nil || pending.State != catalog.MutationApplying {
		return nil
	}
	if pending.Claim == nil {
		return fmt.Errorf("%w: applying mutation has no claim", catalog.ErrIntegrity)
	}
	if _, err := a.store.ReclaimMutation(ctx, pending.OperationID, *pending.Claim, a.owner); err != nil {
		return err
	}
	return nil
}

func (a *tenantActor) recoveryRequiredPending(ctx context.Context) (bool, error) {
	pending, err := a.store.PendingMutation(ctx, a.spec.ID)
	if err != nil {
		return false, fmt.Errorf("tenant runtime: load pending mutation for tenant %q: %w", a.spec.ID, err)
	}
	return pending != nil && pending.State == catalog.MutationRecoveryRequired, nil
}

func (a *tenantActor) recordProgress(ctx context.Context, revision catalog.Revision, lane Lane) error {
	ack := make(chan error, 1)
	select {
	case a.progress <- progress{revision: revision, lane: lane, ack: ack}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func laneFailure(revision catalog.Revision, lane Lane, operation string, err error) executionResult {
	return executionResult{revision: revision, lane: lane, err: fmt.Errorf("tenant runtime: %s: %w", operation, err)}
}

func quarantineCause(err error) catalog.QuarantineCause {
	switch {
	case errors.Is(err, catalog.ErrConflict),
		errors.Is(err, catalog.ErrMutationConflict),
		errors.Is(err, catalog.ErrMutationActive),
		errors.Is(err, catalog.ErrMutationRecoveryRequired),
		errors.Is(err, catalog.ErrStateConflict):
		return catalog.QuarantineCauseConflict
	case errors.Is(err, catalog.ErrIntegrity), errors.Is(err, catalog.ErrInvalidTransition):
		return catalog.QuarantineCauseIntegrity
	case errors.Is(err, catalog.ErrMutationClaimed):
		return catalog.QuarantineCauseUnsettled
	default:
		return catalog.QuarantineCauseUnavailable
	}
}

func (a *tenantActor) completePrepared() {
	for id, waiter := range a.waiters {
		state := stateFor(waiter.revision, a.record)
		if !state.Prepared() {
			continue
		}
		waiter.response <- prepareResult{state: state}
		delete(a.waiters, id)
	}
}

func (a *tenantActor) completeAll(err error) {
	for id, waiter := range a.waiters {
		waiter.response <- prepareResult{state: stateFor(waiter.revision, a.record), err: err}
		delete(a.waiters, id)
	}
}

func (a *tenantActor) shouldExit() bool {
	return a.closing && a.active == nil && len(a.waiters) == 0
}
