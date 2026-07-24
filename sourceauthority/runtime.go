package sourceauthority

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
)

const defaultSnapshotAttempts = 3

const maxEventBatchEvents = 4096

const (
	initialRetryDelay = 25 * time.Millisecond
	maximumRetryDelay = 2 * time.Second
)

type wallRetryClock struct{}

func (wallRetryClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

// Config is one single-writer source-authority runtime definition.
type Config struct {
	Store             Store
	Authority         causal.SourceAuthorityID
	FleetOwner        catalog.SourceAuthorityFleetOwnerID
	FleetGeneration   causal.Generation
	DriverID          string
	DeclarationDigest [32]byte
	RuntimeEpoch      [16]byte
	RuntimeProcess    proc.Record
	Policy            Policy
	Executor          Executor
	Tenants           []tenant.TenantSpec
	SnapshotAttempts  int
	RetryClock        RetryClock
}

// Runtime owns durable observation, publication, barriers, and mutation locators for one authority.
type Runtime struct {
	catalog           Store
	authority         causal.SourceAuthorityID
	fleetOwner        catalog.SourceAuthorityFleetOwnerID
	fleetGeneration   causal.Generation
	driverID          string
	declarationDigest [32]byte
	runtimeEpoch      [16]byte
	runtimeProcess    proc.Record
	policy            Policy
	executor          Executor
	attempts          int
	clock             RetryClock
	mutationFence     func(context.Context) (bool, error)

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	mu                sync.RWMutex
	roots             []RootSpec
	tenants           []tenant.TenantSpec
	tenantGenerations map[catalog.TenantID]catalog.Generation
	stream            EventStream
	stopErr           error

	wake         chan struct{}
	barriers     chan barrierRequest
	drains       chan drainRequest
	reconfigure  chan reconfigureRequest
	preparations chan mutationPreparationRequest
	applications chan mutationApplicationRequest

	cancelOnce sync.Once
	closeOnce  sync.Once
	closeDone  chan struct{}
	closeErr   error
}

type barrierRequest struct {
	tenant     catalog.TenantID
	generation catalog.Generation
	streams    []StreamCheckpoint
	through    InboxSequence
	result     chan barrierResponse
}

type barrierResponse struct {
	result BarrierResult
	err    error
}

type drainRequest struct {
	streams  []StreamCheckpoint
	through  InboxSequence
	result   chan error
	shutdown bool
}

type reconfigureRequest struct {
	tenants []tenant.TenantSpec
	result  chan error
}

type mutationApplicationRequest struct {
	step      tenant.SourceMutationStep
	operation tenant.SourceMutationOperation
	content   tenant.SourceMutationContent
	result    chan error
}

type mutationPreparationRequest struct {
	ctx    context.Context
	step   tenant.SourceMutationStep
	result chan mutationPreparationResponse
}

type mutationPreparationResponse struct {
	operation tenant.SourceMutationOperation
	err       error
}

type observedRoot struct {
	Spec     RootSpec
	Identity FileIdentity
}

// NewRuntime recovers one authority, opens its exact event streams, and resumes durable work.
func NewRuntime(ctx context.Context, config Config) (*Runtime, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	attempts := config.SnapshotAttempts
	if attempts == 0 {
		attempts = defaultSnapshotAttempts
	}
	r := &Runtime{
		catalog: config.Store, authority: config.Authority,
		fleetOwner: config.FleetOwner, fleetGeneration: config.FleetGeneration, driverID: config.DriverID,
		declarationDigest: config.DeclarationDigest, runtimeEpoch: config.RuntimeEpoch,
		runtimeProcess: config.RuntimeProcess,
		policy:         config.Policy,
		executor:       config.Executor, attempts: attempts, clock: config.RetryClock,
		ctx: runtimeCtx, cancel: cancel, done: make(chan struct{}),
		closeDone: make(chan struct{}),
		wake:      make(chan struct{}, 1), barriers: make(chan barrierRequest), drains: make(chan drainRequest),
		reconfigure: make(chan reconfigureRequest), preparations: make(chan mutationPreparationRequest),
		applications:      make(chan mutationApplicationRequest),
		tenantGenerations: make(map[catalog.TenantID]catalog.Generation),
	}
	ref := catalog.SourceAuthorityRuntimeRef{
		Owner: config.FleetOwner, Generation: config.FleetGeneration,
		Authority: config.Authority,
	}
	runtimeFence := catalog.SourceAuthorityRuntimeFence{
		Owner: ref.Owner, Generation: ref.Generation, Authority: ref.Authority,
		Epoch: config.RuntimeEpoch,
	}
	state, err := config.Store.SourceAuthorityRuntimeStatus(ctx, ref)
	if err != nil {
		closeErr := config.Store.CloseSourceAuthorityRuntime(context.Background(), runtimeFence)
		cancel()
		return nil, errors.Join(err, closeErr, config.Executor.Close())
	}
	if state.DeclarationDigest != config.DeclarationDigest ||
		state.Epoch != config.RuntimeEpoch || state.Process != config.RuntimeProcess || state.Closed {
		closeErr := config.Store.CloseSourceAuthorityRuntime(context.Background(), runtimeFence)
		cancel()
		return nil, errors.Join(
			fmt.Errorf("%w: source authority declaration, runtime epoch, or owner mismatch", catalog.ErrMutationConflict),
			closeErr,
			config.Executor.Close(),
		)
	}
	if err := config.Store.OpenSourceAuthorityRuntime(ctx, runtimeFence); err != nil {
		closeErr := config.Store.CloseSourceAuthorityRuntime(context.Background(), runtimeFence)
		cancel()
		return nil, errors.Join(err, closeErr, config.Executor.Close())
	}
	if r.clock == nil {
		r.clock = wallRetryClock{}
	}
	r.mutationFence = r.hasUnsettledSourceMutations
	pending, err := config.Store.PendingSourcePublicationStage(ctx, config.Authority)
	if err != nil {
		closeErr := config.Store.CloseSourceAuthorityRuntime(context.Background(), runtimeFence)
		cancel()
		return nil, errors.Join(err, closeErr, config.Executor.Close())
	}
	if pending != nil {
		if err := r.recoverPendingPublicationStage(ctx, *pending); err != nil {
			closeErr := config.Store.CloseSourceAuthorityRuntime(context.Background(), runtimeFence)
			cancel()
			return nil, errors.Join(err, closeErr, config.Executor.Close())
		}
	}
	if err := r.configure(ctx, config.Tenants); err != nil {
		closeErr := config.Store.CloseSourceAuthorityRuntime(context.Background(), runtimeFence)
		cancel()
		return nil, errors.Join(err, closeErr, config.Executor.Close())
	}
	go r.run()
	r.signal()
	return r, nil
}

// Reconfigure durably fences a new desired authority fleet before new callbacks begin.
func (r *Runtime) Reconfigure(ctx context.Context, specs []tenant.TenantSpec) error {
	response := make(chan error, 1)
	request := reconfigureRequest{tenants: append([]tenant.TenantSpec(nil), specs...), result: response}
	select {
	case r.reconfigure <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return ErrClosed
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return ErrClosed
	}
}

// Barrier flushes every backend stream and waits for its durable inbox sequence to settle.
func (r *Runtime) Barrier(ctx context.Context, id catalog.TenantID, generation catalog.Generation) (BarrierResult, error) {
	if id == "" || generation == 0 {
		return BarrierResult{}, fmt.Errorf("%w: incomplete tenant generation", ErrInvalidPlan)
	}
	response := make(chan barrierResponse, 1)
	request := barrierRequest{tenant: id, generation: generation, result: response}
	select {
	case r.barriers <- request:
	case <-ctx.Done():
		return BarrierResult{}, ctx.Err()
	case <-r.done:
		return BarrierResult{}, ErrClosed
	}
	select {
	case result := <-response:
		return result.result, result.err
	case <-ctx.Done():
		return BarrierResult{}, ctx.Err()
	case <-r.done:
		return BarrierResult{}, ErrClosed
	}
}

// PrepareSourceMutation implements tenant.SourceMutationPlanner through the same durable index.
func (r *Runtime) PrepareSourceMutation(ctx context.Context, step tenant.SourceMutationStep) (tenant.SourceMutationOperation, error) {
	response := make(chan mutationPreparationResponse, 1)
	request := mutationPreparationRequest{ctx: ctx, step: step, result: response}
	select {
	case r.preparations <- request:
	case <-ctx.Done():
		return tenant.SourceMutationOperation{}, ctx.Err()
	case <-r.done:
		return tenant.SourceMutationOperation{}, ErrClosed
	}
	select {
	case result := <-response:
		return result.operation, result.err
	case <-ctx.Done():
		return tenant.SourceMutationOperation{}, ctx.Err()
	case <-r.done:
		return tenant.SourceMutationOperation{}, ErrClosed
	}
}

// ApplySourceMutation executes and durably records one typed semantic source operation.
func (r *Runtime) ApplySourceMutation(ctx context.Context, step tenant.SourceMutationStep, operation tenant.SourceMutationOperation, content tenant.SourceMutationContent) error {
	owned := content != nil
	defer func() {
		if owned {
			_ = content.Close()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	response := make(chan error, 1)
	request := mutationApplicationRequest{step: step, operation: operation, content: content, result: response}
	select {
	case r.applications <- request:
		owned = false
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return ErrClosed
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return ErrClosed
	}
}

// SourceMutationCommitted makes an atomically armed catalog mutation eligible for causal echo settlement.
func (r *Runtime) SourceMutationCommitted(ctx context.Context, commit tenant.SourceMutationCommit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if commit.OperationID == (catalog.MutationID{}) || commit.SourceID != string(r.authority) {
		return catalog.ErrIntegrity
	}
	r.signal()
	return nil
}

// Close stops intake only after every flushed inbox sequence and staged publication settles.
func (r *Runtime) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.closeOnce.Do(func() { go r.closeGracefully() })
	select {
	case <-r.closeDone:
	case <-ctx.Done():
		r.Cancel()
		<-r.closeDone
	}
	r.mu.RLock()
	err := r.closeErr
	r.mu.RUnlock()
	return errors.Join(err, closeContextError("close", ctx.Err()))
}

func (r *Runtime) closeGracefully() {
	response := make(chan error, 1)
	sent := false
	select {
	case r.drains <- drainRequest{result: response, shutdown: true}:
		sent = true
	case <-r.done:
	}
	var drainErr error
	if sent {
		select {
		case drainErr = <-response:
		case <-r.done:
			select {
			case drainErr = <-response:
			default:
			}
		}
	}
	<-r.done
	r.mu.Lock()
	r.closeErr = errors.Join(drainErr, r.stopErr)
	r.mu.Unlock()
	close(r.closeDone)
}

// Cancel immediately aborts runtime-owned work without settling it.
func (r *Runtime) Cancel() {
	r.cancelOnce.Do(r.cancel)
}

// Wait joins the authority actor and event receivers.
func (r *Runtime) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-r.done:
	case <-ctx.Done():
		r.Cancel()
		<-r.done
	}
	r.mu.RLock()
	err := r.stopErr
	r.mu.RUnlock()
	return errors.Join(err, closeContextError("wait", ctx.Err()))
}

func closeContextError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("sourceauthority: %s: %w", operation, err)
}

func (r *Runtime) run() {
	defer func() {
		stopErr := errors.Join(
			r.closeCurrentStream(),
			r.executor.Close(),
			r.closeRuntimeFence(),
		)
		r.mu.Lock()
		r.stopErr = errors.Join(r.stopErr, stopErr)
		r.mu.Unlock()
		close(r.done)
	}()
	var pendingBarriers []barrierRequest
	var pendingDrains []drainRequest
	var pendingReconfigures []reconfigureRequest
	var pendingPreparations []mutationPreparationRequest
	var reconcileErr error
	var retry <-chan time.Time
	closing := false
	retryDelay := initialRetryDelay
	scheduleRetry := func(err error) {
		if retryableReconcileError(err) && retry == nil {
			retry = r.clock.After(retryDelay)
			retryDelay = min(retryDelay*2, maximumRetryDelay)
		}
	}
	reconcile := func() {
		reconcileErr = r.reconcile(r.ctx)
		if reconcileErr == nil {
			retryDelay = initialRetryDelay
			return
		}
		scheduleRetry(reconcileErr)
	}
	advanceReconfigures := func() {
		for len(pendingReconfigures) > 0 {
			blocked, err := r.mutationFence(r.ctx)
			if err != nil {
				if retryableReconcileError(err) {
					scheduleRetry(err)
					return
				}
				for _, request := range pendingReconfigures {
					request.result <- err
				}
				pendingReconfigures = nil
				return
			}
			if blocked {
				return
			}
			request := pendingReconfigures[0]
			pendingReconfigures = pendingReconfigures[1:]
			request.result <- r.configure(r.ctx, request.tenants)
		}
	}
	advancePreparations := func() {
		for len(pendingPreparations) > 0 {
			request := pendingPreparations[0]
			if err := request.ctx.Err(); err != nil {
				pendingPreparations = pendingPreparations[1:]
				request.result <- mutationPreparationResponse{err: err}
				continue
			}
			blocked, err := r.mutationPreparationBlocked(r.ctx, request.step.OperationID)
			if err != nil {
				pendingPreparations = pendingPreparations[1:]
				request.result <- mutationPreparationResponse{err: err}
				continue
			}
			if blocked {
				return
			}
			pendingPreparations = pendingPreparations[1:]
			operation, err := r.prepareSourceMutation(request.ctx, request.step)
			request.result <- mutationPreparationResponse{operation: operation, err: err}
		}
	}
	for {
		select {
		case <-r.ctx.Done():
			for _, request := range pendingBarriers {
				request.result <- barrierResponse{err: ErrClosed}
			}
			for _, request := range pendingDrains {
				request.result <- ErrClosed
			}
			for _, request := range pendingReconfigures {
				request.result <- ErrClosed
			}
			for _, request := range pendingPreparations {
				request.result <- mutationPreparationResponse{err: ErrClosed}
			}
			return
		case request := <-r.barriers:
			if closing {
				request.result <- barrierResponse{err: ErrClosed}
				break
			}
			checkpoints, through, err := r.captureStreamFence(r.ctx)
			if err != nil {
				request.result <- barrierResponse{err: err}
				break
			}
			request.streams, request.through = checkpoints, through
			pendingBarriers = append(pendingBarriers, request)
		case request := <-r.drains:
			if closing {
				request.result <- ErrClosed
				break
			}
			if request.shutdown && r.currentStream() == nil {
				request.result <- nil
				return
			}
			checkpoints, through, err := r.captureStreamFence(r.ctx)
			if err != nil {
				request.result <- err
				break
			}
			request.streams, request.through = checkpoints, through
			pendingDrains = append(pendingDrains, request)
			closing = request.shutdown
		case request := <-r.reconfigure:
			if closing {
				request.result <- ErrClosed
			} else {
				pendingReconfigures = append(pendingReconfigures, request)
			}
		case request := <-r.preparations:
			if closing {
				request.result <- mutationPreparationResponse{err: ErrClosed}
			} else {
				pendingPreparations = append(pendingPreparations, request)
			}
		case request := <-r.applications:
			if closing {
				closeErr := error(nil)
				if request.content != nil {
					closeErr = request.content.Close()
				}
				request.result <- errors.Join(ErrClosed, closeErr)
			} else {
				request.result <- r.applySourceMutation(r.ctx, request.step, request.operation, request.content)
			}
		case <-r.wake:
			reconcile()
		case <-retry:
			retry = nil
			reconcile()
		}
		if !closing {
			advancePreparations()
			advanceReconfigures()
		}
		reportedErr := reconcileErr
		if retryableReconcileError(reportedErr) {
			reportedErr = nil
		}
		pendingBarriers = r.settleBarrierRequests(pendingBarriers, reportedErr)
		var shutdown bool
		pendingDrains, shutdown = r.settleDrainRequests(pendingDrains, reportedErr)
		if shutdown {
			return
		}
		closing = hasShutdownDrain(pendingDrains)
		if reportedErr != nil && !retryableReconcileError(reportedErr) {
			reconcileErr = nil
		}
		if len(pendingBarriers) > 0 || len(pendingDrains) > 0 {
			if retry == nil {
				reconcile()
			}
			reportedErr = reconcileErr
			if retryableReconcileError(reportedErr) {
				reportedErr = nil
			}
			pendingBarriers = r.settleBarrierRequests(pendingBarriers, reportedErr)
			pendingDrains, shutdown = r.settleDrainRequests(pendingDrains, reportedErr)
			if shutdown {
				return
			}
			closing = hasShutdownDrain(pendingDrains)
			if reportedErr != nil && !retryableReconcileError(reportedErr) {
				reconcileErr = nil
			}
		}
	}
}

func (r *Runtime) closeRuntimeFence() error {
	ref := catalog.SourceAuthorityRuntimeRef{
		Owner: r.fleetOwner, Generation: r.fleetGeneration, Authority: r.authority,
	}
	fence := catalog.SourceAuthorityRuntimeFence{
		Owner: ref.Owner, Generation: ref.Generation, Authority: ref.Authority,
		Epoch: r.runtimeEpoch,
	}
	delay := initialRetryDelay
	for {
		closeErr := r.catalog.CloseSourceAuthorityRuntime(context.Background(), fence)
		if closeErr == nil {
			return nil
		}
		state, statusErr := r.catalog.SourceAuthorityRuntimeStatus(context.Background(), ref)
		if statusErr == nil {
			if state.Epoch != r.runtimeEpoch {
				return errors.Join(
					closeErr,
					fmt.Errorf("%w: source authority runtime epoch changed during close", catalog.ErrMutationConflict),
				)
			}
			if state.Closed {
				return nil
			}
		}
		if !retryableReconcileError(closeErr) || (statusErr != nil && !retryableReconcileError(statusErr)) {
			return errors.Join(closeErr, statusErr)
		}
		<-r.clock.After(delay)
		delay *= 2
		if delay > maximumRetryDelay {
			delay = maximumRetryDelay
		}
	}
}

func retryableReconcileError(err error) bool {
	return err != nil && !errors.Is(err, ErrQuarantined) && !errors.Is(err, ErrInvalidPlan) &&
		!errors.Is(err, catalog.ErrIntegrity) && !errors.Is(err, catalog.ErrSourceObserverConflict) &&
		!errors.Is(err, catalog.ErrInvalidObject) && !errors.Is(err, catalog.ErrInvalidTransition) &&
		!errors.Is(err, catalog.ErrMutationConflict) && !errors.Is(err, catalog.ErrGenerationMismatch) &&
		!errors.Is(err, catalog.ErrTenantOwnerMismatch) && !errors.Is(err, catalog.ErrSchemaMismatch) &&
		!errors.Is(err, catalog.ErrConflict)
}

func (r *Runtime) configure(ctx context.Context, specs []tenant.TenantSpec) error {
	tenants := normalizedTenants(r.authority, specs)
	if len(tenants) == 0 {
		return r.configureEmpty(ctx)
	}
	roots, err := r.policy.Roots(ctx, append([]tenant.TenantSpec(nil), tenants...))
	if err != nil {
		return fmt.Errorf("sourceauthority: declare roots: %w", err)
	}
	if err := validateRoots(r.authority, roots); err != nil {
		return err
	}
	slices.SortFunc(roots, func(left, right RootSpec) int { return compareString(string(left.ID), string(right.ID)) })
	observed := make([]observedRoot, len(roots))
	for index, root := range roots {
		declared := root
		identity, err := r.executor.RootIdentity(ctx, root)
		if err != nil {
			return fmt.Errorf("sourceauthority: resolve root identity %q: %w", root.ID, err)
		}
		if err := validateFileIdentity(identity); err != nil {
			return fmt.Errorf("%w: root %q: %v", ErrInvalidPlan, root.ID, err)
		}
		root.ExpectedIdentity = identity
		roots[index] = root
		observed[index] = observedRoot{Spec: declared, Identity: identity}
	}
	rootDigest, err := digestJSON(observed)
	if err != nil {
		return err
	}
	fleetDigest, err := digestJSON(tenants)
	if err != nil {
		return err
	}

	if prior := r.currentStream(); prior != nil {
		if err := r.drainStream(ctx, prior); err != nil {
			return err
		}
		r.mu.Lock()
		if r.stream == prior {
			r.stream = nil
		}
		r.mu.Unlock()
		if err := prior.Close(); err != nil {
			return fmt.Errorf("sourceauthority: close prior stream before reconfigure: %w", err)
		}
	}

	var resume []StreamCheckpoint
	state, stateErr := r.loadSourceObserverFence(ctx, r.authority)
	if stateErr == nil {
		resume = checkpointsFromCatalog(state.Checkpoints)
	} else if !errors.Is(stateErr, catalog.ErrNotFound) {
		return fmt.Errorf("sourceauthority: load durable observer: %w", stateErr)
	}
	rootFence := append([]RootSpec(nil), roots...)
	stream, err := r.executor.Open(ctx, rootFence, resume, func(sinkCtx context.Context, batch EventBatch) error {
		return r.ingest(sinkCtx, rootFence, batch)
	})
	if err != nil {
		return fmt.Errorf("sourceauthority: open event stream: %w", err)
	}
	checkpoints := stream.Checkpoints()
	if err := validateCheckpoints(checkpoints); err != nil {
		_ = stream.Close()
		return err
	}
	streamIdentity, streamEpoch, err := checkpointSetIdentity(checkpoints)
	if err != nil {
		_ = stream.Close()
		return err
	}
	configuration := observerConfiguration{
		Authority: r.authority, FleetOwner: r.fleetOwner, FleetGeneration: r.fleetGeneration,
		Stream: streamIdentity, RootEpoch: streamEpoch,
		RootDigest: rootDigest, FleetDigest: fleetDigest,
		Roots: rootsToCatalog(observed), Checkpoints: checkpointsToCatalog(checkpoints),
	}
	if err := r.configureObserver(ctx, configuration); err != nil {
		_ = stream.Close()
		return fmt.Errorf("sourceauthority: configure durable observer: %w", err)
	}
	if err := stream.Activate(ctx); err != nil {
		_ = stream.Close()
		_ = r.catalog.QuarantineSourceObserver(context.WithoutCancel(ctx), r.authority, "event stream activation failed: "+err.Error())
		return fmt.Errorf("sourceauthority: activate event stream: %w", err)
	}
	r.mu.Lock()
	r.roots = append([]RootSpec(nil), roots...)
	r.tenants = append([]tenant.TenantSpec(nil), tenants...)
	r.tenantGenerations = make(map[catalog.TenantID]catalog.Generation, len(tenants))
	for _, spec := range tenants {
		r.tenantGenerations[spec.ID] = spec.Generation
	}
	r.stream = stream
	r.mu.Unlock()
	r.signal()
	return nil
}

func (r *Runtime) drainStream(ctx context.Context, stream EventStream) error {
	checkpoints, err := stream.Flush(ctx)
	if err != nil {
		return fmt.Errorf("sourceauthority: flush prior stream before reconfigure: %w", err)
	}
	through, err := r.receivedThrough(ctx, checkpoints)
	if err != nil {
		return err
	}
	delay := initialRetryDelay
	for {
		_, ready, err := r.settledFence(ctx, checkpoints, through)
		if err != nil {
			return err
		}
		if ready {
			return nil
		}
		if err := r.reconcile(ctx); err != nil {
			if !retryableReconcileError(err) {
				return fmt.Errorf("sourceauthority: settle prior stream before reconfigure: %w", err)
			}
			select {
			case <-r.clock.After(delay):
				delay = min(delay*2, maximumRetryDelay)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (r *Runtime) configureEmpty(ctx context.Context) error {
	state, err := r.loadSourceObserverFence(ctx, r.authority)
	if errors.Is(err, catalog.ErrNotFound) {
		if stream := r.currentStream(); stream != nil {
			if err := r.drainStream(ctx, stream); err != nil {
				return err
			}
			r.mu.Lock()
			if r.stream == stream {
				r.stream = nil
			}
			r.mu.Unlock()
			if err := stream.Close(); err != nil {
				return err
			}
		}
		r.mu.Lock()
		r.roots, r.tenants, r.stream = nil, nil, nil
		r.mu.Unlock()
		return nil
	}
	if err != nil {
		return err
	}
	roots, observed, err := rootsFromCatalog(r.authority, state.Roots)
	if err != nil {
		return err
	}
	stream := r.currentStream()
	if stream == nil {
		rootFence := append([]RootSpec(nil), roots...)
		stream, err = r.executor.Open(ctx, rootFence, checkpointsFromCatalog(state.Checkpoints), func(sinkCtx context.Context, batch EventBatch) error {
			return r.ingest(sinkCtx, rootFence, batch)
		})
		if err != nil {
			return fmt.Errorf("sourceauthority: open stream for empty fleet: %w", err)
		}
		if err := stream.Activate(ctx); err != nil {
			_ = stream.Close()
			return fmt.Errorf("sourceauthority: activate stream for empty fleet: %w", err)
		}
		r.mu.Lock()
		r.roots, r.stream = roots, stream
		r.mu.Unlock()
	}
	if err := r.drainStream(ctx, stream); err != nil {
		return err
	}
	checkpoints := stream.Checkpoints()
	r.mu.Lock()
	if r.stream == stream {
		r.stream = nil
	}
	r.mu.Unlock()
	if err := stream.Close(); err != nil {
		return err
	}
	fleetDigest, err := digestJSON([]tenant.TenantSpec{})
	if err != nil {
		return err
	}
	streamIdentity, streamEpoch, err := checkpointSetIdentity(checkpoints)
	if err != nil {
		return err
	}
	configuration := observerConfiguration{
		Authority: r.authority, FleetOwner: r.fleetOwner, FleetGeneration: r.fleetGeneration,
		Stream: streamIdentity, RootEpoch: streamEpoch,
		RootDigest: state.Stream.RootDigest, FleetDigest: fleetDigest,
		Roots: rootsToCatalog(observed), Checkpoints: checkpointsToCatalog(checkpoints),
	}
	if err := r.configureObserver(ctx, configuration); err != nil {
		return err
	}
	if err := r.publishEmptyFleet(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	r.roots, r.tenants, r.stream = nil, nil, nil
	r.mu.Unlock()
	return nil
}

func (r *Runtime) refreshDiscontinuousStream(ctx context.Context) error {
	tenants := r.currentTenants()
	if len(tenants) == 0 {
		return r.configureEmpty(ctx)
	}
	roots, err := r.policy.Roots(ctx, append([]tenant.TenantSpec(nil), tenants...))
	if err != nil {
		return err
	}
	if err := validateRoots(r.authority, roots); err != nil {
		return err
	}
	slices.SortFunc(roots, func(left, right RootSpec) int { return compareString(string(left.ID), string(right.ID)) })
	observed := make([]observedRoot, len(roots))
	for index, root := range roots {
		declared := root
		identity, err := r.executor.RootIdentity(ctx, root)
		if err != nil {
			return err
		}
		if err := validateFileIdentity(identity); err != nil {
			return err
		}
		root.ExpectedIdentity = identity
		roots[index] = root
		observed[index] = observedRoot{Spec: declared, Identity: identity}
	}
	if prior := r.currentStream(); prior != nil {
		r.mu.Lock()
		if r.stream == prior {
			r.stream = nil
		}
		r.mu.Unlock()
		if err := prior.Close(); err != nil {
			return err
		}
	}
	rootFence := append([]RootSpec(nil), roots...)
	stream, err := r.executor.Open(ctx, rootFence, nil, func(sinkCtx context.Context, batch EventBatch) error {
		return r.ingest(sinkCtx, rootFence, batch)
	})
	if err != nil {
		return err
	}
	checkpoints := stream.Checkpoints()
	if err := validateCheckpoints(checkpoints); err != nil {
		_ = stream.Close()
		return err
	}
	rootDigest, err := digestJSON(observed)
	if err != nil {
		_ = stream.Close()
		return err
	}
	fleetDigest, err := digestJSON(tenants)
	if err != nil {
		_ = stream.Close()
		return err
	}
	streamIdentity, streamEpoch, err := checkpointSetIdentity(checkpoints)
	if err != nil {
		_ = stream.Close()
		return err
	}
	if err := r.configureObserver(ctx, observerConfiguration{
		Authority: r.authority, FleetOwner: r.fleetOwner, FleetGeneration: r.fleetGeneration,
		Stream: streamIdentity, RootEpoch: streamEpoch,
		RootDigest: rootDigest, FleetDigest: fleetDigest, Roots: rootsToCatalog(observed),
		Checkpoints: checkpointsToCatalog(checkpoints), Reset: true,
	}); err != nil {
		_ = stream.Close()
		return err
	}
	if err := stream.Activate(ctx); err != nil {
		_ = stream.Close()
		return err
	}
	r.mu.Lock()
	r.roots, r.stream = roots, stream
	r.mu.Unlock()
	return nil
}

func (r *Runtime) configureObserver(ctx context.Context, configuration observerConfiguration) error {
	_, err := r.configureSourceObserver(ctx, configuration)
	if !errors.Is(err, catalog.ErrSourceObserverConflict) {
		return err
	}
	detail := "publication stage crossed an observer configuration fence"
	quarantineErr := r.catalog.QuarantineSourceObserver(context.WithoutCancel(ctx), r.authority, detail)
	return errors.Join(fmt.Errorf("%w: %s", ErrQuarantined, detail), err, quarantineErr)
}

func (r *Runtime) publishEmptyFleet(ctx context.Context) (resultErr error) {
	state, err := r.loadSourceObserverFence(ctx, r.authority)
	if err != nil {
		return err
	}
	emptyDigest, err := digestJSON([]tenant.TenantSpec{})
	if err != nil {
		return err
	}
	if state.Stream.Mode == catalog.SourceObserverIncremental && state.Stream.FleetDigest == emptyDigest {
		return nil
	}
	snapshot, err := newSnapshotID()
	if err != nil {
		return err
	}
	if err := r.catalog.BeginSourceSnapshotStage(ctx, r.authority, snapshot); err != nil {
		return err
	}
	snapshotOwned := true
	defer func() {
		if snapshotOwned {
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), terminalStageCleanupTimeout)
			defer cancel()
			resultErr = errors.Join(resultErr, r.catalog.AbortSourceSnapshotStage(cleanupCtx, r.authority, snapshot))
		}
	}()
	watermark, err := r.catalog.SourceWatermark(ctx, r.authority)
	if err != nil {
		return err
	}
	change, operation, err := newCausalIDs()
	if err != nil {
		return err
	}
	fence := r.fence(state, checkpointsFromCatalog(state.Checkpoints))
	fenceDigest, err := digestJSON(fence)
	if err != nil {
		return err
	}
	identity := catalog.SourceSnapshotIdentity{
		Authority: r.authority, AuthorityGeneration: r.fleetGeneration,
		Snapshot: snapshot, FenceDigest: fenceDigest,
		Change: causal.ChangeSet{
			SourceAuthority: r.authority, SourceRevision: watermark + 1,
			ChangeID: change, OperationID: operation, Cause: causal.CauseBootstrap,
		},
	}
	if err := r.catalog.BeginSourceSnapshotPublication(ctx, identity); err != nil {
		return err
	}
	ref, err := r.catalog.AppendSourceSnapshotPublication(ctx, identity, catalog.SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"fusekit.authority-fleet"},
	})
	if err != nil {
		return err
	}
	settlement := catalog.SourceSnapshotSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: r.authority, Stream: state.Stream.Stream, RootEpoch: state.Stream.RootEpoch,
			Through: state.Stream.LastReceived, Operation: operation,
		},
		Snapshot: ref,
	}
	return r.promoteSnapshotWithHandoff(ctx, ref, settlement, func() {
		snapshotOwned = false
	})
}

func rootsFromCatalog(authority causal.SourceAuthorityID, records []catalog.SourceObserverRootRecord) ([]RootSpec, []observedRoot, error) {
	roots := make([]RootSpec, len(records))
	observed := make([]observedRoot, len(records))
	for index, record := range records {
		identity := FileIdentity{
			VolumeUUID: record.VolumeUUID, Inode: record.Inode,
			BirthtimeSec: record.BirthSec, BirthtimeNsec: record.BirthNsec,
		}
		if err := validateFileIdentity(identity); err != nil {
			return nil, nil, fmt.Errorf("%w: corrupt durable root identity", ErrQuarantined)
		}
		declared := RootSpec{Authority: authority, ID: RootID(record.ID), Path: record.Path, Kind: RootKind(record.Kind), Generation: record.Generation}
		root := declared
		root.ExpectedIdentity = identity
		roots[index] = root
		observed[index] = observedRoot{Spec: declared, Identity: identity}
	}
	return roots, observed, nil
}

func (r *Runtime) ingest(ctx context.Context, roots []RootSpec, batch EventBatch) error {
	batch.Events = append([]PathEvent(nil), batch.Events...)
	if err := validateBatch(roots, batch); err != nil {
		_ = r.catalog.RequireSourceObserverSnapshot(context.WithoutCancel(ctx), r.authority)
		return err
	}
	payload, err := json.Marshal(batch)
	if err != nil {
		return fmt.Errorf("sourceauthority: encode event batch: %w", err)
	}
	record := catalog.SourceObserverInboxRecord{
		Authority: r.authority, Stream: string(batch.Stream), RootEpoch: string(batch.RootEpoch),
		NativePredecessor: uint64(batch.Predecessor), NativeCursor: uint64(batch.Cursor),
		EventCount: uint64(len(batch.Events)), Digest: sha256.Sum256(payload), Payload: payload,
	}
	if _, err := r.catalog.AppendSourceObserverInbox(ctx, record); err != nil {
		r.signal()
		if errors.Is(err, catalog.ErrSourceObserverInboxCoalesced) {
			return nil
		}
		return fmt.Errorf("sourceauthority: append durable inbox: %w", err)
	}
	for _, event := range batch.Events {
		if event.Flags.RequiresSnapshot() {
			if err := r.catalog.RequireSourceObserverSnapshot(ctx, r.authority); err != nil {
				return err
			}
			r.signal()
			if batch.Cursor == 0 {
				return ErrSnapshotRequired
			}
			break
		}
	}
	r.signal()
	return nil
}

func validateBatch(rootSpecs []RootSpec, batch EventBatch) error {
	discontinuity := batch.Predecessor == 0 && batch.Cursor == 0
	if batch.Stream == "" || batch.RootEpoch == "" || (!discontinuity && batch.Predecessor >= batch.Cursor) ||
		len(batch.Events) == 0 || len(batch.Events) > maxEventBatchEvents {
		return fmt.Errorf("%w: incomplete event batch", ErrInvalidEvent)
	}
	roots := make(map[RootID]RootKind)
	for _, root := range rootSpecs {
		roots[root.ID] = root.Kind
	}
	var priorID EventID
	var priorOrdinal EventOrdinal
	for index, event := range batch.Events {
		rootKind, ok := roots[event.Root]
		rootChanged := discontinuity && event.Flags&FlagRootChanged != 0 && event.ID == 0
		validPath := validRelative(event.Relative) || (event.Relative == "." && (rootChanged || rootKind == RootFile))
		if !ok || !validPath ||
			event.Kind < EventCreated || event.Kind > EventMetadata ||
			(!rootChanged && (event.ID <= batch.Predecessor || event.ID > batch.Cursor)) {
			return fmt.Errorf("%w: event lies outside its root or native cursor fence", ErrInvalidEvent)
		}
		if index > 0 && (event.ID < priorID || (event.ID == priorID && event.Ordinal <= priorOrdinal)) {
			return fmt.Errorf("%w: event id and ordinal are not strictly ordered", ErrInvalidEvent)
		}
		priorID, priorOrdinal = event.ID, event.Ordinal
	}
	if discontinuity {
		for _, event := range batch.Events {
			if event.Flags&FlagRootChanged == 0 || event.ID != 0 {
				return fmt.Errorf("%w: zero cursor is reserved for root discontinuity", ErrInvalidEvent)
			}
		}
	}
	return nil
}

func (r *Runtime) settleBarrierRequests(requests []barrierRequest, reconcileErr error) []barrierRequest {
	remaining := requests[:0]
	for _, request := range requests {
		result, ready, err := r.barrierResult(request.tenant, request.generation, request.streams, request.through)
		if err != nil {
			request.result <- barrierResponse{err: err}
			continue
		}
		if !ready && reconcileErr == nil {
			remaining = append(remaining, request)
			continue
		}
		if err == nil && !ready {
			err = reconcileErr
		}
		request.result <- barrierResponse{result: result, err: err}
	}
	return remaining
}

func (r *Runtime) settleDrainRequests(requests []drainRequest, reconcileErr error) ([]drainRequest, bool) {
	remaining := requests[:0]
	for _, request := range requests {
		_, ready, err := r.settledFence(r.ctx, request.streams, request.through)
		if err != nil {
			request.result <- err
			continue
		}
		if !ready && reconcileErr == nil {
			remaining = append(remaining, request)
			continue
		}
		if err == nil && !ready {
			err = reconcileErr
		}
		if err == nil && request.shutdown {
			err = r.closeCurrentStream()
			request.result <- err
			return remaining, true
		}
		request.result <- err
	}
	return remaining, false
}

func hasShutdownDrain(requests []drainRequest) bool {
	for _, request := range requests {
		if request.shutdown {
			return true
		}
	}
	return false
}

func (r *Runtime) barrierResult(id catalog.TenantID, generation catalog.Generation, checkpoints []StreamCheckpoint, through InboxSequence) (BarrierResult, bool, error) {
	fence, ready, err := r.settledFence(r.ctx, checkpoints, through)
	if err != nil || !ready {
		return BarrierResult{}, ready, err
	}
	r.mu.RLock()
	configuredGeneration, found := r.tenantGenerations[id]
	r.mu.RUnlock()
	if !found || configuredGeneration != generation {
		return BarrierResult{}, true, fmt.Errorf("%w: tenant generation is not in the authority fleet", ErrInvalidPlan)
	}
	target, err := r.catalog.SourceDriverTargetCheckpoint(r.ctx, r.authority, id, generation)
	if err != nil {
		return BarrierResult{}, true, fmt.Errorf("sourceauthority: read tenant source checkpoint: %w", err)
	}
	source, err := r.catalog.SourceDriverCheckpoint(r.ctx, r.authority)
	if err != nil {
		return BarrierResult{}, true, fmt.Errorf("sourceauthority: read source checkpoint: %w", err)
	}
	if target.SourceRevision != source.SourceRevision {
		return BarrierResult{}, true, fmt.Errorf("%w: source and tenant checkpoints disagree", catalog.ErrIntegrity)
	}
	return BarrierResult{Fence: fence, Target: target, Source: source}, true, nil
}

func (r *Runtime) settledFence(ctx context.Context, checkpoints []StreamCheckpoint, through InboxSequence) (EventFence, bool, error) {
	state, err := r.loadSourceObserverFence(ctx, r.authority)
	if err != nil {
		return EventFence{}, false, err
	}
	if state.Stream.Mode == catalog.SourceObserverQuarantined {
		return EventFence{}, false, fmt.Errorf("%w: %s", ErrQuarantined, state.Stream.Quarantine)
	}
	if state.Stream.Mode != catalog.SourceObserverIncremental {
		return EventFence{}, false, nil
	}
	if !checkpointIdentitiesEqual(state.Checkpoints, checkpoints) {
		return EventFence{}, false, fmt.Errorf("%w: observer stream changed before fence settlement", ErrSourceChanged)
	}
	pending, err := r.catalog.PendingSourcePublicationStage(ctx, r.authority)
	if err != nil {
		return EventFence{}, false, err
	}
	unsettledMutation, err := r.hasUnsettledSourceMutations(ctx)
	if err != nil {
		return EventFence{}, false, err
	}
	if !catalogCheckpointsCover(state.Checkpoints, checkpoints) || state.Stream.LastApplied < uint64(through) ||
		pending != nil || unsettledMutation {
		return EventFence{}, false, nil
	}
	return EventFence{Streams: cloneCheckpoints(checkpoints), Inbox: through}, true, nil
}

func (r *Runtime) receivedThrough(ctx context.Context, checkpoints []StreamCheckpoint) (InboxSequence, error) {
	state, err := r.loadSourceObserverFence(ctx, r.authority)
	if err != nil {
		return 0, err
	}
	if !catalogCheckpointsCover(state.Checkpoints, checkpoints) {
		return 0, ErrSourceChanged
	}
	return InboxSequence(state.Stream.LastReceived), nil
}

func checkpointIdentitiesEqual(stored []catalog.SourceObserverCheckpointRecord, required []StreamCheckpoint) bool {
	if len(stored) != len(required) {
		return false
	}
	for index := range stored {
		if stored[index].Stream != string(required[index].Identity) || stored[index].RootEpoch != string(required[index].RootEpoch) {
			return false
		}
	}
	return true
}

func (r *Runtime) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *Runtime) currentRoots() []RootSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]RootSpec(nil), r.roots...)
}

func (r *Runtime) currentTenants() []tenant.TenantSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]tenant.TenantSpec(nil), r.tenants...)
}

func (r *Runtime) currentStream() EventStream {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.stream
}

func (r *Runtime) captureStreamFence(ctx context.Context) ([]StreamCheckpoint, InboxSequence, error) {
	stream := r.currentStream()
	if stream == nil {
		return nil, 0, ErrClosed
	}
	checkpoints, err := stream.Flush(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("sourceauthority: flush event streams: %w", err)
	}
	if err := validateCheckpoints(checkpoints); err != nil {
		return nil, 0, err
	}
	through, err := r.receivedThrough(ctx, checkpoints)
	if err != nil {
		return nil, 0, err
	}
	return cloneCheckpoints(checkpoints), through, nil
}

func (r *Runtime) closeCurrentStream() error {
	r.mu.Lock()
	stream := r.stream
	r.stream = nil
	r.mu.Unlock()
	if stream == nil {
		return nil
	}
	return stream.Close()
}

func validateConfig(config Config) error {
	switch {
	case config.Store == nil:
		return errors.New("sourceauthority: catalog is required")
	case config.Authority == "":
		return errors.New("sourceauthority: authority is required")
	case config.FleetOwner == "":
		return errors.New("sourceauthority: fleet owner is required")
	case config.FleetGeneration == 0:
		return errors.New("sourceauthority: fleet generation is required")
	case catalog.ValidateSourceDriverID(config.DriverID) != nil:
		return errors.New("sourceauthority: source DriverID is invalid")
	case config.DeclarationDigest == ([32]byte{}):
		return errors.New("sourceauthority: declaration digest is required")
	case config.RuntimeEpoch == ([16]byte{}):
		return errors.New("sourceauthority: runtime epoch is required")
	case config.RuntimeProcess.Validate() != nil:
		return errors.New("sourceauthority: runtime process is invalid")
	case config.Policy == nil:
		return errors.New("sourceauthority: policy is required")
	case config.Executor == nil:
		return errors.New("sourceauthority: executor is required")
	case config.SnapshotAttempts < 0:
		return errors.New("sourceauthority: snapshot attempts cannot be negative")
	default:
		return nil
	}
}

func normalizedTenants(authority causal.SourceAuthorityID, specs []tenant.TenantSpec) []tenant.TenantSpec {
	result := make([]tenant.TenantSpec, 0, len(specs))
	for _, spec := range specs {
		if spec.Content.ID == string(authority) {
			result = append(result, spec)
		}
	}
	slices.SortFunc(result, func(left, right tenant.TenantSpec) int { return compareString(string(left.ID), string(right.ID)) })
	return result
}

func validateRoots(authority causal.SourceAuthorityID, roots []RootSpec) error {
	if len(roots) == 0 {
		return fmt.Errorf("%w: policy declared no roots", ErrInvalidPlan)
	}
	seenID := make(map[RootID]struct{}, len(roots))
	paths := make([]string, 0, len(roots))
	for _, root := range roots {
		if root.Authority != authority || root.ID == "" || root.Generation == 0 || root.ExpectedIdentity != (FileIdentity{}) ||
			(root.Kind != RootFile && root.Kind != RootDirectory) || !filepath.IsAbs(root.Path) ||
			filepath.Clean(root.Path) != root.Path || strings.ContainsRune(root.Path, 0) {
			return fmt.Errorf("%w: invalid root declaration", ErrInvalidPlan)
		}
		if _, duplicate := seenID[root.ID]; duplicate {
			return fmt.Errorf("%w: duplicate root id %q", ErrInvalidPlan, root.ID)
		}
		seenID[root.ID] = struct{}{}
		paths = append(paths, root.Path)
	}
	slices.Sort(paths)
	for index := 1; index < len(paths); index++ {
		previous, current := paths[index-1], paths[index]
		prefix := previous
		if previous != string(filepath.Separator) {
			prefix += string(filepath.Separator)
		}
		if current == previous || strings.HasPrefix(current, prefix) {
			return fmt.Errorf("%w: overlapping root paths %q and %q", ErrInvalidPlan, previous, current)
		}
	}
	return nil
}

func validateCheckpoints(checkpoints []StreamCheckpoint) error {
	if len(checkpoints) == 0 {
		return fmt.Errorf("%w: backend returned no stream checkpoints", ErrInvalidEvent)
	}
	for index, checkpoint := range checkpoints {
		if checkpoint.Identity == "" || checkpoint.RootEpoch == "" ||
			(index > 0 && checkpoints[index-1].Identity >= checkpoint.Identity) {
			return fmt.Errorf("%w: stream checkpoints are incomplete or unordered", ErrInvalidEvent)
		}
	}
	return nil
}

func validRelative(relative string) bool {
	return relative != "" && !filepath.IsAbs(relative) && filepath.Clean(relative) == relative && relative != "." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !strings.ContainsRune(relative, 0)
}

func checkpointSetIdentity(checkpoints []StreamCheckpoint) (string, string, error) {
	type identity struct {
		Stream StreamIdentity
		Epoch  RootEpoch
	}
	values := make([]identity, len(checkpoints))
	for index, checkpoint := range checkpoints {
		values[index] = identity{Stream: checkpoint.Identity, Epoch: checkpoint.RootEpoch}
	}
	digest, err := digestJSON(values)
	if err != nil {
		return "", "", err
	}
	encoded := hex.EncodeToString(digest[:])
	return "set:" + encoded, "epochs:" + encoded, nil
}

func digestJSON(value any) ([32]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return [32]byte{}, fmt.Errorf("sourceauthority: encode durable fence: %w", err)
	}
	return sha256.Sum256(payload), nil
}

func rootsToCatalog(roots []observedRoot) []catalog.SourceObserverRootRecord {
	result := make([]catalog.SourceObserverRootRecord, len(roots))
	for index, root := range roots {
		result[index] = catalog.SourceObserverRootRecord{
			ID: string(root.Spec.ID), Generation: root.Spec.Generation, Path: root.Spec.Path,
			VolumeUUID: root.Identity.VolumeUUID, Inode: root.Identity.Inode,
			BirthSec: root.Identity.BirthtimeSec, BirthNsec: root.Identity.BirthtimeNsec,
			Kind: uint8(root.Spec.Kind),
		}
	}
	return result
}

func validateFileIdentity(identity FileIdentity) error {
	if identity.VolumeUUID == "" || identity.Inode == 0 || identity.BirthtimeNsec < 0 || identity.BirthtimeNsec >= 1_000_000_000 {
		return errors.New("incomplete physical identity")
	}
	return nil
}

func checkpointsToCatalog(checkpoints []StreamCheckpoint) []catalog.SourceObserverCheckpointRecord {
	result := make([]catalog.SourceObserverCheckpointRecord, len(checkpoints))
	for index, checkpoint := range checkpoints {
		result[index] = catalog.SourceObserverCheckpointRecord{
			Stream: string(checkpoint.Identity), RootEpoch: string(checkpoint.RootEpoch), EventID: uint64(checkpoint.Cursor),
		}
	}
	return result
}

func checkpointsFromCatalog(checkpoints []catalog.SourceObserverCheckpointRecord) []StreamCheckpoint {
	result := make([]StreamCheckpoint, len(checkpoints))
	for index, checkpoint := range checkpoints {
		result[index] = StreamCheckpoint{Identity: StreamIdentity(checkpoint.Stream), RootEpoch: RootEpoch(checkpoint.RootEpoch), Cursor: EventID(checkpoint.EventID)}
	}
	return result
}

func cloneCheckpoints(checkpoints []StreamCheckpoint) []StreamCheckpoint {
	return append([]StreamCheckpoint(nil), checkpoints...)
}

func catalogCheckpointsCover(stored []catalog.SourceObserverCheckpointRecord, required []StreamCheckpoint) bool {
	if len(stored) != len(required) {
		return false
	}
	for index := range stored {
		if stored[index].Stream != string(required[index].Identity) || stored[index].RootEpoch != string(required[index].RootEpoch) ||
			stored[index].EventID < uint64(required[index].Cursor) {
			return false
		}
	}
	return true
}

var (
	_ Barrier                      = (*Runtime)(nil)
	_ tenant.SourceMutationPlanner = (*Runtime)(nil)
)
