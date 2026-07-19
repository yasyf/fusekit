package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/fusekit/catalog"
)

var (
	// ErrRecoveryActive means worker recovery was requested while tenant work was active.
	ErrRecoveryActive = errors.New("tenant runtime: recovery requires idle actors")
	// ErrRecovering means the runtime is temporarily closed to preparation while
	// prior-generation workers are recovered.
	ErrRecovering = errors.New("tenant runtime: worker recovery in progress")
)

const maxWorkerProofBytes = 4 << 10

type workerProof struct {
	Tenant     catalog.TenantID   `json:"tenant"`
	Generation catalog.Generation `json:"generation"`
	Revision   catalog.Revision   `json:"revision"`
	Lane       Lane               `json:"lane"`
}

type boundedProofSink struct {
	file     *os.File
	written  int
	overflow bool
}

type copyReader struct{ io.Reader }

type copyWriter struct{ io.Writer }

func (s *boundedProofSink) Write(p []byte) (int, error) {
	remaining := maxWorkerProofBytes + 1 - s.written
	if remaining > 0 {
		chunk := p
		if len(chunk) > remaining {
			chunk = chunk[:remaining]
		}
		n, err := s.file.Write(chunk)
		s.written += n
		if err != nil {
			return n, err
		}
		if n != len(chunk) {
			return n, io.ErrShortWrite
		}
	}
	if len(p) > remaining {
		s.overflow = true
	}
	return len(p), nil
}

// TenantRuntime owns one serialized convergence actor per tenant specification.
type TenantRuntime struct {
	store   Store
	workers WorkerPool
	planner Planner
	owner   catalog.MutationOwnerID

	mu             sync.Mutex
	tenants        map[catalog.TenantID]*tenantSlot
	nextWaiter     uint64
	transitions    int
	closed         bool
	canceled       bool
	recovering     bool
	recoveryDone   chan struct{}
	recoveryCancel context.CancelFunc

	closeOnce           sync.Once
	cancelOnce          sync.Once
	workerCloseOnce     sync.Once
	workerLifecycleOnce sync.Once
	workersClosed       chan struct{}
}

type tenantSlot struct {
	spec          TenantSpec
	actor         *tenantActor
	admissions    int
	transitioning bool
	drained       chan struct{}
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

// NewRuntime builds an empty dynamic tenant fleet.
func NewRuntime(store Store, workers WorkerPool, planner Planner) (*TenantRuntime, error) {
	if store == nil {
		return nil, errors.New("tenant runtime: store is required")
	}
	if workers == nil {
		return nil, errors.New("tenant runtime: worker pool is required")
	}
	if planner == nil {
		return nil, errors.New("tenant runtime: planner is required")
	}
	owner, err := catalog.NewMutationOwnerID()
	if err != nil {
		return nil, fmt.Errorf("tenant runtime: generate mutation owner: %w", err)
	}
	r := &TenantRuntime{
		store:         store,
		workers:       workers,
		planner:       planner,
		owner:         owner,
		tenants:       make(map[catalog.TenantID]*tenantSlot),
		workersClosed: make(chan struct{}),
	}
	return r, nil
}

// RegisterTenant adds one immutable tenant specification to the live fleet.
func (r *TenantRuntime) RegisterTenant(ctx context.Context, spec TenantSpec) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("tenant runtime: register tenant: %w", err)
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
	actor := newTenantActor(r.store, r.workers, r.planner, r.owner, spec)
	r.tenants[spec.ID] = &tenantSlot{spec: spec, actor: actor}
	r.mu.Unlock()
	<-actor.ready
	if actor.loadErr == nil {
		return nil
	}
	r.mu.Lock()
	if current, ok := r.tenants[spec.ID]; ok && current.actor == actor {
		delete(r.tenants, spec.ID)
	}
	r.mu.Unlock()
	actor.cancel()
	<-actor.done
	return actor.loadErr
}

// ReplaceTenant drains one generation and atomically registers its successor.
func (r *TenantRuntime) ReplaceTenant(ctx context.Context, expectedGeneration catalog.Generation, next TenantSpec) error {
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
	drained := r.beginTransitionLocked(slot)
	r.mu.Unlock()
	if err := r.awaitTransitionDrain(ctx, next.ID, slot, drained); err != nil {
		return err
	}
	slot.actor.close()
	<-slot.actor.done

	r.mu.Lock()
	r.transitions--
	if current, ok := r.tenants[next.ID]; !ok || current != slot {
		r.mu.Unlock()
		return fmt.Errorf("%w: replacement slot changed", catalog.ErrIntegrity)
	}
	if r.canceled {
		delete(r.tenants, next.ID)
		r.mu.Unlock()
		return ErrCanceled
	}
	if r.closed {
		delete(r.tenants, next.ID)
		r.mu.Unlock()
		return ErrClosed
	}
	actor := newTenantActor(r.store, r.workers, r.planner, r.owner, next)
	slot.spec = next
	slot.actor = actor
	slot.transitioning = false
	slot.drained = nil
	r.mu.Unlock()
	<-actor.ready
	if actor.loadErr == nil {
		return nil
	}
	r.mu.Lock()
	if current, ok := r.tenants[next.ID]; ok && current.actor == actor {
		delete(r.tenants, next.ID)
	}
	r.mu.Unlock()
	actor.cancel()
	<-actor.done
	return actor.loadErr
}

// RemoveTenant drains and forgets one generation without deleting its durable data.
func (r *TenantRuntime) RemoveTenant(ctx context.Context, tenant catalog.TenantID, expectedGeneration catalog.Generation) error {
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
		r.mu.Unlock()
		return ErrTenantChanging
	}
	if slot.spec.Generation != expectedGeneration {
		r.mu.Unlock()
		return ErrGenerationConflict
	}
	drained := r.beginTransitionLocked(slot)
	r.mu.Unlock()
	if err := r.awaitTransitionDrain(ctx, tenant, slot, drained); err != nil {
		return err
	}
	slot.actor.close()
	<-slot.actor.done

	r.mu.Lock()
	defer r.mu.Unlock()
	r.transitions--
	if current, ok := r.tenants[tenant]; ok && current == slot {
		delete(r.tenants, tenant)
	}
	return nil
}

// Specs returns a deterministic linearizable snapshot of fleet specifications.
func (r *TenantRuntime) Specs() []TenantSpec {
	r.mu.Lock()
	specs := make([]TenantSpec, 0, len(r.tenants))
	for _, slot := range r.tenants {
		specs = append(specs, slot.spec)
	}
	r.mu.Unlock()
	sort.Slice(specs, func(i, j int) bool { return specs[i].ID < specs[j].ID })
	return specs
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

// Release retires the generation admission. It is idempotent.
func (l *GenerationLease) Release() {
	if l == nil {
		return
	}
	l.once.Do(func() {
		l.runtime.releaseAdmission(l.slot)
	})
}

// State returns the actor's current durable convergence state.
func (r *TenantRuntime) State(ctx context.Context, tenant catalog.TenantID) (TenantState, error) {
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
	r.mu.Unlock()
	defer r.releaseAdmission(slot)
	response := make(chan prepareResult, 1)
	if err := slot.actor.send(ctx, stateRequest{response: response}); err != nil {
		return TenantState{}, err
	}
	select {
	case result := <-response:
		return result.state, result.err
	case <-ctx.Done():
		return TenantState{}, fmt.Errorf("tenant runtime: read tenant %q state: %w", tenant, ctx.Err())
	}
}

// Recover settles prior-generation worker groups, then releases only quarantine
// records whose cause was an unsettled group.
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
	if err := r.workers.Recover(recoveryCtx); err != nil {
		return fmt.Errorf("tenant runtime: recover workers: %w", err)
	}
	for _, actor := range actors {
		if err := actor.recover(recoveryCtx); err != nil {
			return err
		}
	}
	return nil
}

// Close rejects new preparation, lets admitted work settle, and then closes the
// owned worker pool.
func (r *TenantRuntime) Close() {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		shutdowns := r.shutdownsLocked()
		recoveryDone := r.recoveryDone
		recoveryCancel := r.recoveryCancel
		r.mu.Unlock()
		if recoveryCancel != nil {
			recoveryCancel()
		}
		r.closeWorkersAfterActors(shutdowns, recoveryDone)
	})
}

// Cancel rejects new preparation and cancels every active tenant operation.
func (r *TenantRuntime) Cancel() {
	r.cancelOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.canceled = true
		shutdowns := r.shutdownsLocked()
		recoveryDone := r.recoveryDone
		recoveryCancel := r.recoveryCancel
		r.mu.Unlock()
		if recoveryCancel != nil {
			recoveryCancel()
		}
		go func() {
			waitSignal(recoveryDone)
			for _, shutdown := range shutdowns {
				shutdown.actor.cancel()
			}
			r.workerCloseOnce.Do(r.workers.Close)
			r.workers.Cancel()
		}()
		r.closeWorkersAfterActors(shutdowns, recoveryDone)
	})
}

func (r *TenantRuntime) closeWorkersAfterActors(shutdowns []actorShutdown, recoveryDone <-chan struct{}) {
	r.workerLifecycleOnce.Do(func() {
		go func() {
			waitSignal(recoveryDone)
			for _, shutdown := range shutdowns {
				<-shutdown.drained
				shutdown.actor.close()
				<-shutdown.actor.done
			}
			r.workerCloseOnce.Do(r.workers.Close)
			close(r.workersClosed)
		}()
	})
}

// Wait joins every tenant actor. A caller context error is returned only after
// all actor and worker-operation goroutines have settled.
func (r *TenantRuntime) Wait(ctx context.Context) error {
	done := ctx.Done()
	var ctxErr error
	select {
	case <-r.workersClosed:
	case <-done:
		ctxErr = ctx.Err()
		<-r.workersClosed
	}
	if ctxErr != nil {
		ctx = context.WithoutCancel(ctx)
	}
	workerErr := r.workers.Wait(ctx)
	if ctxErr != nil {
		return errors.Join(fmt.Errorf("tenant runtime: wait: %w", ctxErr), workerErr)
	}
	if workerErr != nil {
		return fmt.Errorf("tenant runtime: wait workers: %w", workerErr)
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
	workers WorkerPool
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

func newTenantActor(store Store, workers WorkerPool, planner Planner, owner catalog.MutationOwnerID, spec TenantSpec) *tenantActor {
	ctx, cancel := context.WithCancel(context.Background())
	a := &tenantActor{
		store:     store,
		workers:   workers,
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
	pending, err := a.store.PendingMutations(a.ctx, a.spec.ID)
	if err != nil {
		a.loadErr = fmt.Errorf("tenant runtime: enumerate pending mutations for tenant %q: %w", a.spec.ID, err)
		return
	}
	for _, mutation := range pending {
		var cause catalog.QuarantineCause
		var detail string
		switch mutation.State {
		case catalog.MutationApplying:
			cause = catalog.QuarantineCauseUnsettled
			detail = catalog.ErrMutationClaimed.Error()
		case catalog.MutationRecoveryRequired:
			cause = catalog.QuarantineCauseConflict
			detail = catalog.ErrMutationRecoveryRequired.Error()
		default:
			continue
		}
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
	if a.record.Version == 0 {
		if err := a.save(a.record); err != nil {
			a.loadErr = err
			return
		}
	}
	if err := a.ensureMountLifecycle(a.ctx); err != nil {
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
	if a.record.Applied >= request.revision {
		request.response <- prepareResult{state: stateFor(request.revision, a.record)}
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
	if a.active != nil || a.canceled || a.paused || len(a.waiters) == 0 || a.loadErr != nil || a.record.Quarantine != nil || a.record.Desired <= a.record.Applied {
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

	demanded, err := a.store.HasMaterializationDemand(ctx, a.spec.ID)
	if err != nil {
		return laneFailure(revision, LaneMaterialization, "inspect materialization demand", err)
	}
	if a.record.Verified < revision && demanded {
		materializationWorker, err := a.planner.PrepareMaterialization(ctx, a.store, MaterializationStep{Tenant: a.spec, Revision: revision})
		if err != nil {
			return laneFailure(revision, LaneMaterialization, "prepare materialization", err)
		}
		if err := a.runProofWorker(ctx, LaneMaterialization, revision, materializationWorker, nil); err != nil {
			return laneFailure(revision, LaneMaterialization, "materialize", err)
		}
	}
	if err := a.recordProgress(ctx, revision, LaneMaterialization); err != nil {
		return executionResult{revision: revision, lane: LaneMaterialization, err: err}
	}
	return executionResult{revision: revision, lane: LaneMaterialization}
}

func (a *tenantActor) ensureMountLifecycle(ctx context.Context) error {
	if a.record.ActivatedGeneration == a.spec.Generation {
		return nil
	}
	head, err := a.store.Head(ctx, a.spec.ID)
	if err != nil {
		return err
	}
	if a.spec.Traits.Presentations.Has(catalog.PresentationMount) {
		worker, err := a.planner.PrepareMountLifecycle(ctx, a.store, MountLifecycleStep{Tenant: a.spec, Revision: head})
		if err != nil {
			return err
		}
		if worker != nil {
			if err := a.runProofWorker(ctx, LaneMountLifecycle, head, *worker, nil); err != nil {
				return err
			}
		}
	}
	next := a.record
	next.ActivatedGeneration = a.spec.Generation
	return a.save(next)
}

func (a *tenantActor) runProofWorker(ctx context.Context, lane Lane, revision catalog.Revision, spec WorkerSpec, input io.Reader) error {
	if spec.Path == "" || !filepath.IsAbs(spec.Path) {
		return fmt.Errorf("%w: worker path must be absolute", catalog.ErrIntegrity)
	}
	if input != nil && len(spec.Input) != 0 {
		return fmt.Errorf("%w: worker has multiple stdin sources", catalog.ErrIntegrity)
	}
	stdin, err := workerInput(spec.Input, input)
	if err != nil {
		return err
	}
	proofFile, err := privateTemp("fusekit-worker-proof-")
	if err != nil {
		if stdin != nil {
			_ = stdin.Close()
		}
		return err
	}
	defer func() { _ = proofFile.Close() }()
	sink := &boundedProofSink{file: proofFile}
	task := supervise.Task{
		Path:   spec.Path,
		Args:   append([]string(nil), spec.Args...),
		Dir:    spec.Dir,
		Env:    append([]string(nil), spec.Env...),
		Stdin:  stdin,
		Stdout: sink,
	}
	if err := a.workers.Run(ctx, task); err != nil {
		return err
	}
	if sink.overflow || sink.written > maxWorkerProofBytes {
		return fmt.Errorf("%w: worker proof exceeds %d bytes", catalog.ErrIntegrity, maxWorkerProofBytes)
	}
	if _, err := proofFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("tenant runtime: seek worker proof: %w", err)
	}
	proofBytes, err := io.ReadAll(io.LimitReader(proofFile, maxWorkerProofBytes+1))
	if err != nil {
		return fmt.Errorf("tenant runtime: read worker proof: %w", err)
	}
	proof, err := decodeWorkerProof(proofBytes)
	if err != nil {
		return err
	}
	if proof.Tenant != a.spec.ID || proof.Generation != a.spec.Generation || proof.Revision != revision || proof.Lane != lane {
		return fmt.Errorf("%w: worker proof identity mismatch", catalog.ErrIntegrity)
	}
	record, err := a.store.LoadTenantState(ctx, a.spec.ID)
	if err != nil {
		return err
	}
	if record.Tenant != a.spec.ID || record.Generation != a.spec.Generation {
		return fmt.Errorf("%w: worker proof does not match catalog tenant state", catalog.ErrIntegrity)
	}
	if lane == LaneMaterialization && record.Desired < revision {
		return fmt.Errorf("%w: materialization proof exceeds desired revision", catalog.ErrIntegrity)
	}
	head, err := a.store.Head(ctx, a.spec.ID)
	if err != nil {
		return err
	}
	if head < revision {
		return fmt.Errorf("%w: worker proof revision %d exceeds catalog head %d", catalog.ErrIntegrity, revision, head)
	}
	return nil
}

func decodeWorkerProof(data []byte) (workerProof, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var proof workerProof
	if err := decoder.Decode(&proof); err != nil {
		return workerProof{}, fmt.Errorf("%w: decode worker proof: %v", catalog.ErrIntegrity, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return workerProof{}, fmt.Errorf("%w: worker proof has trailing data", catalog.ErrIntegrity)
	}
	return proof, nil
}

func workerInput(data []byte, source io.Reader) (*os.File, error) {
	if len(data) == 0 && source == nil {
		return nil, nil
	}
	file, err := privateTemp("fusekit-worker-input-")
	if err != nil {
		return nil, err
	}
	reader := source
	if reader == nil {
		reader = bytes.NewReader(data)
	}
	if _, err := io.Copy(copyWriter{file}, copyReader{reader}); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("tenant runtime: stage worker input: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("tenant runtime: seek worker input: %w", err)
	}
	return file, nil
}

func privateTemp(pattern string) (*os.File, error) {
	file, err := os.CreateTemp("", pattern)
	if err != nil {
		return nil, fmt.Errorf("tenant runtime: create private temp: %w", err)
	}
	name := file.Name()
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("tenant runtime: secure private temp: %w", err)
	}
	if err := os.Remove(name); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("tenant runtime: unlink private temp: %w", err)
	}
	return file, nil
}

func (a *tenantActor) replayPendingMutations(ctx context.Context) error {
	for {
		pending, err := a.store.PendingMutations(ctx, a.spec.ID)
		if err != nil {
			return err
		}
		if len(pending) == 0 {
			return nil
		}
		mutation := pending[0]
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
		if _, err := a.store.CommitMutation(ctx, mutation.OperationID); err != nil {
			return err
		}
	}
}

func (a *tenantActor) applyClaimedMutation(ctx context.Context, mutation catalog.PreparedMutation) error {
	if mutation.OperationID == (catalog.MutationID{}) || mutation.Tenant != a.spec.ID || mutation.State != catalog.MutationApplying || mutation.Claim == nil {
		return fmt.Errorf("%w: claimed mutation identity mismatch", catalog.ErrIntegrity)
	}
	step := SourceMutationStep{
		Tenant: a.spec, OperationID: mutation.OperationID,
		SourceID: mutation.Intent.SourceID, SourceMetadata: mutation.Intent.SourceMetadata,
		Kind: mutation.Kind, ExpectedHead: mutation.ExpectedHead, Intent: mutation.Intent,
	}
	worker, err := a.planner.PrepareSourceMutation(ctx, step)
	if err != nil {
		return err
	}
	if worker.OperationID != step.OperationID || worker.SourceID != step.SourceID || worker.SourceMetadata != step.SourceMetadata {
		return fmt.Errorf("%w: source worker identity does not match persisted operation", catalog.ErrIntegrity)
	}
	if len(worker.Spec.Input) != 0 {
		return fmt.Errorf("%w: source planner supplied worker stdin", catalog.ErrIntegrity)
	}
	var content *os.File
	var input io.Reader
	if mutationCarriesFileContent(mutation.Intent) {
		content, err = a.store.OpenMutationContent(ctx, mutation.OperationID)
		if err != nil {
			return err
		}
		defer func() { _ = content.Close() }()
		input = content
	}
	if err := a.runProofWorker(ctx, LaneCatalogMutation, step.ExpectedHead, worker.Spec, input); err != nil {
		return err
	}
	if _, err := a.store.MarkMutationApplied(ctx, mutation.OperationID, *mutation.Claim); err != nil {
		return err
	}
	return nil
}

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
	pending, err := a.store.PendingMutations(ctx, a.spec.ID)
	if err != nil {
		return err
	}
	for _, mutation := range pending {
		if mutation.State != catalog.MutationApplying {
			continue
		}
		if mutation.Claim == nil {
			return fmt.Errorf("%w: applying mutation has no claim", catalog.ErrIntegrity)
		}
		if _, err := a.store.ReclaimMutation(ctx, mutation.OperationID, *mutation.Claim, a.owner); err != nil {
			return err
		}
	}
	return nil
}

func (a *tenantActor) recoveryRequiredPending(ctx context.Context) (bool, error) {
	pending, err := a.store.PendingMutations(ctx, a.spec.ID)
	if err != nil {
		return false, fmt.Errorf("tenant runtime: enumerate pending mutations for tenant %q: %w", a.spec.ID, err)
	}
	for _, mutation := range pending {
		if mutation.State == catalog.MutationRecoveryRequired {
			return true, nil
		}
	}
	return false, nil
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
	case errors.Is(err, catalog.ErrMutationClaimed), errors.Is(err, supervise.ErrUnsettledGroup):
		return catalog.QuarantineCauseUnsettled
	default:
		return catalog.QuarantineCauseUnavailable
	}
}

func (a *tenantActor) completePrepared() {
	for id, waiter := range a.waiters {
		if waiter.revision > a.record.Applied {
			continue
		}
		waiter.response <- prepareResult{state: stateFor(waiter.revision, a.record)}
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
