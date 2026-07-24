package holder

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
)

const (
	authorityInitialRetryDelay = 25 * time.Millisecond
	authorityMaximumRetryDelay = 2 * time.Second
)

type authorityRetryClock interface {
	After(time.Duration) <-chan time.Time
}

type wallAuthorityRetryClock struct{}

func (wallAuthorityRetryClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

// SourceAuthoritySpec is one sealed physical or semantic source declaration.
type SourceAuthoritySpec interface {
	sourceAuthoritySpec()
}

// PhysicalSourceSpec declares one filesystem-observed source authority.
type PhysicalSourceSpec struct {
	Authority         causal.SourceAuthorityID
	DeclarationDigest [32]byte
	DriverID          string
	DriverConfig      []byte
	Policy            sourceauthority.AuthorityPolicy
}

func (PhysicalSourceSpec) sourceAuthoritySpec() {}

// SemanticDriverSpec declares one child-dispatched semantic source authority.
type SemanticDriverSpec struct {
	Authority         causal.SourceAuthorityID
	DeclarationDigest [32]byte
	DriverID          string
	DriverConfig      []byte
}

func (SemanticDriverSpec) sourceAuthoritySpec() {}

func sourceAuthorityIdentity(spec SourceAuthoritySpec) (causal.SourceAuthorityID, [32]byte) {
	switch source := spec.(type) {
	case PhysicalSourceSpec:
		return source.Authority, source.DeclarationDigest
	case SemanticDriverSpec:
		return source.Authority, source.DeclarationDigest
	default:
		return "", [32]byte{}
	}
}

func sourceAuthorityDriverID(spec SourceAuthoritySpec) string {
	switch source := spec.(type) {
	case PhysicalSourceSpec:
		return source.DriverID
	case SemanticDriverSpec:
		return source.DriverID
	default:
		return ""
	}
}

func sourceAuthorityDriverConfig(spec SourceAuthoritySpec) []byte {
	switch source := spec.(type) {
	case PhysicalSourceSpec:
		return source.DriverConfig
	case SemanticDriverSpec:
		return source.DriverConfig
	default:
		return nil
	}
}

// SourceAuthorityFleet declares one complete owner-generation authority set.
type SourceAuthorityFleet struct {
	Owner       catalog.SourceAuthorityFleetOwnerID
	Generation  causal.Generation
	Authorities []SourceAuthoritySpec
}

type managedAuthority interface {
	sourceauthority.Barrier
	tenant.SourceMutationPlanner
	Reconfigure(context.Context, []tenant.TenantSpec) error
	Close(context.Context) error
	Cancel()
	Wait(context.Context) error
}

type authorityRuntimeFactory func(context.Context, sourceauthority.Config) (managedAuthority, error)
type authorityExecutorFactory func(SourceAuthoritySpec) (sourceauthority.Executor, error)

type authorityDeclaration struct {
	spec    SourceAuthoritySpec
	runtime managedAuthority
	closed  bool
}

func (d *authorityDeclaration) authority() causal.SourceAuthorityID {
	authority, _ := sourceAuthorityIdentity(d.spec)
	return authority
}

func (d *authorityDeclaration) declarationDigest() [32]byte {
	_, digest := sourceAuthorityIdentity(d.spec)
	return digest
}

type authorityRegistry struct {
	catalog         sourceauthority.Store
	factory         authorityRuntimeFactory
	executors       authorityExecutorFactory
	semantic        semanticAuthorityFactory
	closeTimeout    time.Duration
	retryClock      authorityRetryClock
	fleetOwner      catalog.SourceAuthorityFleetOwnerID
	fleetGeneration causal.Generation
	runtimeEpoch    [16]byte
	runtimeProcess  proc.Record

	ctx    context.Context
	cancel context.CancelFunc

	lifecycle sync.Mutex
	mu        sync.RWMutex
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
	ordered   []*authorityDeclaration
	bySource  map[string]*authorityDeclaration
	current   []tenant.TenantSpec
	started   bool
	closing   bool
}

func newAuthorityRegistry(
	store sourceauthority.Store,
	fleet SourceAuthorityFleet,
	factory authorityRuntimeFactory,
	executors authorityExecutorFactory,
	semantic semanticAuthorityFactory,
	runtimeProcess proc.Record,
	closeTimeout time.Duration,
) (*authorityRegistry, error) {
	if store == nil {
		return nil, errors.New("FuseKit runtime: source authority catalog is required")
	}
	if fleet.Owner == "" {
		return nil, errors.New("FuseKit runtime: source authority fleet owner is required")
	}
	if fleet.Generation == 0 {
		return nil, errors.New("FuseKit runtime: source authority fleet generation is required")
	}
	if err := runtimeProcess.Validate(); err != nil {
		return nil, errors.New("FuseKit runtime: source authority runtime process is invalid")
	}
	if runtimeProcess.RecoveryID != recoveryid.SourceOwner || runtimeProcess.ProcessGroup {
		return nil, errors.New("FuseKit runtime: source authority runtime process has the wrong recovery class")
	}
	var runtimeEpoch [16]byte
	if _, err := rand.Read(runtimeEpoch[:]); err != nil {
		return nil, fmt.Errorf("FuseKit runtime: create source authority runtime epoch: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	registry := &authorityRegistry{
		catalog: store, factory: factory, executors: executors, semantic: semantic,
		closeTimeout: closeTimeout,
		retryClock:   wallAuthorityRetryClock{},
		fleetOwner:   fleet.Owner, fleetGeneration: fleet.Generation, runtimeEpoch: runtimeEpoch,
		runtimeProcess: runtimeProcess,
		ctx:            ctx, cancel: cancel,
		closeDone: make(chan struct{}),
		bySource:  make(map[string]*authorityDeclaration, len(fleet.Authorities)),
	}
	for _, spec := range fleet.Authorities {
		authority, declarationDigest := sourceAuthorityIdentity(spec)
		switch {
		case causal.ValidateSourceAuthorityID(authority) != nil:
			cancel()
			return nil, errors.New("FuseKit runtime: source authority id is invalid")
		case declarationDigest == ([32]byte{}):
			cancel()
			return nil, fmt.Errorf("FuseKit runtime: source authority declaration digest is required for %q", authority)
		case !validSourceAuthoritySpec(spec):
			cancel()
			return nil, fmt.Errorf("FuseKit runtime: source authority declaration is invalid for %q", authority)
		case isSemanticSourceAuthority(spec) && semantic == nil:
			cancel()
			return nil, fmt.Errorf("FuseKit runtime: semantic source authority factory is required for %q", authority)
		case !isSemanticSourceAuthority(spec) && factory == nil:
			cancel()
			return nil, fmt.Errorf("FuseKit runtime: physical source authority runtime factory is required for %q", authority)
		case !isSemanticSourceAuthority(spec) && executors == nil:
			cancel()
			return nil, fmt.Errorf("FuseKit runtime: physical source authority executor factory is required for %q", authority)
		}
		sourceID := string(authority)
		if _, exists := registry.bySource[sourceID]; exists {
			cancel()
			return nil, fmt.Errorf("FuseKit runtime: duplicate source authority %q", authority)
		}
		declaration := &authorityDeclaration{spec: spec}
		registry.bySource[sourceID] = declaration
		registry.ordered = append(registry.ordered, declaration)
	}
	slices.SortFunc(registry.ordered, func(a, b *authorityDeclaration) int {
		authorityA, _ := sourceAuthorityIdentity(a.spec)
		authorityB, _ := sourceAuthorityIdentity(b.spec)
		return strings.Compare(string(authorityA), string(authorityB))
	})
	return registry, nil
}

func isSemanticSourceAuthority(spec SourceAuthoritySpec) bool {
	_, ok := spec.(SemanticDriverSpec)
	return ok
}

func validSourceAuthoritySpec(spec SourceAuthoritySpec) bool {
	switch source := spec.(type) {
	case PhysicalSourceSpec:
		return !nilManagedValue(source.Policy) && catalog.ValidateSourceDriverID(source.DriverID) == nil &&
			len(source.DriverConfig) <= catalog.SourceDriverConfigMaxBytes
	case SemanticDriverSpec:
		return source.DeclarationDigest != ([32]byte{}) && catalog.ValidateSourceDriverID(source.DriverID) == nil &&
			len(source.DriverConfig) <= catalog.SourceDriverConfigMaxBytes
	default:
		return false
	}
}

func (r *authorityRegistry) start(ctx context.Context, fleet []tenant.TenantSpec) (resultErr error) {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.mu.Lock()
	if r.started || r.closing {
		r.mu.Unlock()
		return errors.New("FuseKit runtime: source authority registry cannot be started")
	}
	r.mu.Unlock()
	groups, err := r.groupFleet(fleet)
	if err != nil {
		return err
	}
	startCtx, cancelStart := context.WithCancel(r.ctx)
	stopCaller := context.AfterFunc(ctx, cancelStart)
	defer func() {
		stopCaller()
		cancelStart()
	}()
	if err := r.reconcileAuthorityFleet(startCtx); err != nil {
		return fmt.Errorf("FuseKit runtime: reconcile source authority fleet: %w", err)
	}
	published := false
	defer func() {
		if !published {
			resultErr = errors.Join(resultErr, r.closeUnpublishedRuntimeFences())
		}
	}()
	var started []*authorityDeclaration
	for _, declaration := range r.ordered {
		runtime, cleanupErr, createErr := r.createAuthorityRuntime(
			startCtx, declaration, groups[string(declaration.authority())],
		)
		if createErr != nil {
			settlementErr := cleanupErr
			if runtime != nil {
				runtime.Cancel()
				if err := runtime.Wait(context.Background()); err != nil {
					settlementErr = errors.Join(settlementErr, fmt.Errorf(
						"FuseKit runtime: settle unpublished source authority %q: %w",
						declaration.authority(),
						err,
					))
				}
			}
			r.mu.Lock()
			r.closeErr = errors.Join(r.closeErr, settlementErr)
			r.mu.Unlock()
			return errors.Join(
				fmt.Errorf("FuseKit runtime: start source authority %q: %w", declaration.authority(), createErr),
				settlementErr,
				r.cancelStarted(started),
			)
		}
		if runtime == nil {
			return errors.Join(
				fmt.Errorf("FuseKit runtime: start source authority %q: runtime is nil", declaration.authority()),
				r.cancelStarted(started),
			)
		}
		runtimeFence := catalog.SourceAuthorityRuntimeFence{
			Owner: r.fleetOwner, Generation: r.fleetGeneration,
			Authority: declaration.authority(), Epoch: r.runtimeEpoch,
		}
		r.mu.RLock()
		closing := r.closing
		r.mu.RUnlock()
		if closing {
			runtime.Cancel()
			return errors.Join(
				sourceauthority.ErrClosed, runtime.Wait(context.Background()),
				r.closeDeclarationRuntimeFence(context.Background(), declaration),
				r.cancelStarted(started),
			)
		}
		if err := r.catalog.OpenSourceAuthorityRuntime(startCtx, runtimeFence); err != nil {
			runtime.Cancel()
			if errors.Is(err, context.Canceled) {
				r.mu.RLock()
				closing = r.closing
				r.mu.RUnlock()
				if closing {
					err = errors.Join(sourceauthority.ErrClosed, err)
				}
			}
			return errors.Join(
				fmt.Errorf("FuseKit runtime: open source authority %q runtime fence: %w", declaration.authority(), err),
				runtime.Wait(context.Background()),
				r.catalog.CloseSourceAuthorityRuntime(context.Background(), runtimeFence),
				r.cancelStarted(started),
			)
		}
		r.mu.Lock()
		if r.closing {
			r.mu.Unlock()
			runtime.Cancel()
			return errors.Join(
				sourceauthority.ErrClosed, runtime.Wait(context.Background()),
				r.closeDeclarationRuntimeFence(context.Background(), declaration),
				r.cancelStarted(started),
			)
		}
		declaration.runtime = runtime
		declaration.closed = false
		r.mu.Unlock()
		started = append(started, declaration)
	}
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		return errors.Join(sourceauthority.ErrClosed, r.cancelStarted(started))
	}
	r.current = canonicalFleet(fleet)
	r.started = true
	r.mu.Unlock()
	published = true
	return nil
}

func (r *authorityRegistry) createAuthorityRuntime(
	ctx context.Context,
	declaration *authorityDeclaration,
	tenants []tenant.TenantSpec,
) (managedAuthority, error, error) {
	switch spec := declaration.spec.(type) {
	case PhysicalSourceSpec:
		executor, err := r.executors(spec)
		if err != nil {
			var cleanupErr error
			if executor != nil {
				cleanupErr = executor.Close()
			}
			return nil, cleanupErr, fmt.Errorf(
				"FuseKit runtime: create source authority %q executor: %w",
				declaration.authority(), err,
			)
		}
		runtime, createErr := r.factory(ctx, sourceauthority.Config{
			Store: r.catalog, Authority: declaration.authority(),
			FleetOwner: r.fleetOwner, FleetGeneration: r.fleetGeneration,
			DriverID:          sourceAuthorityDriverID(spec),
			DeclarationDigest: declaration.declarationDigest(),
			RuntimeEpoch:      r.runtimeEpoch, RuntimeProcess: r.runtimeProcess,
			Policy: spec.Policy, Executor: executor, Tenants: tenants,
		})
		if createErr != nil || runtime == nil {
			var cleanupErr error
			if runtime == nil {
				cleanupErr = executor.Close()
			}
			if createErr == nil {
				createErr = errors.New("runtime is nil")
			}
			return runtime, cleanupErr, createErr
		}
		return runtime, nil, nil
	case SemanticDriverSpec:
		runtime, err := r.semantic(ctx, spec, tenants)
		if err == nil && runtime == nil {
			err = errors.New("runtime is nil")
		}
		return runtime, nil, err
	default:
		return nil, nil, errors.New("FuseKit runtime: unknown source authority declaration")
	}
}

func (r *authorityRegistry) closeUnpublishedRuntimeFences() error {
	var result error
	for _, declaration := range r.ordered {
		err := r.catalog.CloseSourceAuthorityRuntime(
			context.Background(),
			catalog.SourceAuthorityRuntimeFence{
				Owner: r.fleetOwner, Generation: r.fleetGeneration,
				Authority: declaration.authority(), Epoch: r.runtimeEpoch,
			},
		)
		if err != nil {
			result = errors.Join(result, fmt.Errorf(
				"FuseKit runtime: close unpublished source authority %q runtime fence: %w",
				declaration.authority(), err,
			))
		}
	}
	return result
}

func (r *authorityRegistry) reconcileAuthorityFleet(ctx context.Context) error {
	declarations := make([]catalog.SourceAuthorityDeclaration, len(r.ordered))
	authorities := make([]causal.SourceAuthorityID, len(r.ordered))
	for index, declaration := range r.ordered {
		authorities[index] = declaration.authority()
		declarations[index] = catalog.SourceAuthorityDeclaration{
			Authority:         declaration.authority(),
			DriverID:          sourceAuthorityDriverID(declaration.spec),
			DriverConfig:      append([]byte(nil), sourceAuthorityDriverConfig(declaration.spec)...),
			DeclarationDigest: declaration.declarationDigest(),
		}
	}
	digest, err := catalog.SourceAuthorityFleetDigest(authorities)
	if err != nil {
		return err
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		return err
	}
	status, err := r.catalog.SourceAuthorityFleetHead(ctx, r.fleetOwner)
	if errors.Is(err, catalog.ErrNotFound) {
		status = catalog.SourceAuthorityFleetStatus{}
	} else if err != nil {
		return err
	}
	var expected causal.Generation
	if status.Current != nil {
		expected = status.Current.Generation
		if status.Current.Generation == r.fleetGeneration {
			if status.Current.AuthorityCount != uint64(len(declarations)) ||
				status.Current.AuthoritiesDigest != digest ||
				status.Current.DeclarationsDigest != declarationsDigest {
				return catalog.ErrMutationConflict
			}
			if status.Pending != nil {
				if err := r.abortSourceAuthorityFleet(
					ctx, catalog.SourceAuthorityFleetAbortRequest{
						Owner:              r.fleetOwner,
						ExpectedGeneration: status.Pending.ExpectedGeneration,
						Generation:         status.Pending.Generation,
						StageDigest:        status.Pending.StageDigest,
					},
				); err != nil {
					return err
				}
			}
			return r.takeoverSourceAuthorityRuntimes(ctx, declarations, r.fleetGeneration)
		}
		if status.Current.Generation > r.fleetGeneration {
			return catalog.ErrGenerationMismatch
		}
	}
	if r.fleetGeneration <= expected {
		return catalog.ErrGenerationMismatch
	}
	var pending catalog.SourceAuthorityFleetReconcileState
	if status.Pending != nil {
		pending = *status.Pending
		if pending.Owner != r.fleetOwner || pending.ExpectedGeneration != expected ||
			pending.Generation != r.fleetGeneration ||
			pending.AuthorityCount != uint64(len(declarations)) ||
			pending.AuthoritiesDigest != digest ||
			pending.DeclarationsDigest != declarationsDigest {
			if r.fleetGeneration <= pending.Generation {
				return catalog.ErrMutationConflict
			}
			if err := r.abortSourceAuthorityFleet(
				ctx, catalog.SourceAuthorityFleetAbortRequest{
					Owner: r.fleetOwner, ExpectedGeneration: pending.ExpectedGeneration,
					Generation: pending.Generation, StageDigest: pending.StageDigest,
				},
			); err != nil {
				return err
			}
			pending = catalog.SourceAuthorityFleetReconcileState{}
		}
	}
	if !pending.Complete {
		offset := int(pending.ReceivedCount)
		sequence := pending.NextSequence
		if offset > len(declarations) {
			return catalog.ErrIntegrity
		}
		if len(declarations) == 0 {
			pending, err = r.catalog.ReconcileSourceAuthorityFleet(
				ctx,
				catalog.SourceAuthorityFleetReconcileRequest{
					Owner: r.fleetOwner, ExpectedGeneration: expected,
					Generation: r.fleetGeneration, Sequence: sequence,
					Complete: true, AuthoritiesDigest: digest,
					DeclarationsDigest: declarationsDigest,
				},
			)
			if err != nil {
				return err
			}
		}
		for offset < len(declarations) {
			end := min(offset+catalog.SourceAuthorityFleetPageLimit, len(declarations))
			pending, err = r.catalog.ReconcileSourceAuthorityFleet(
				ctx,
				catalog.SourceAuthorityFleetReconcileRequest{
					Owner: r.fleetOwner, ExpectedGeneration: expected,
					Generation: r.fleetGeneration, Sequence: sequence,
					Declarations: declarations[offset:end], Complete: end == len(declarations),
					AuthorityCount: uint64(len(declarations)), AuthoritiesDigest: digest,
					DeclarationsDigest: declarationsDigest,
				},
			)
			if err != nil {
				return err
			}
			offset = end
			sequence++
		}
	}
	if !pending.Complete {
		return catalog.ErrIntegrity
	}
	desired := make(map[causal.SourceAuthorityID]struct{}, len(authorities))
	for _, authority := range authorities {
		desired[authority] = struct{}{}
	}
	if status.Current != nil {
		var after causal.SourceAuthorityID
		for {
			page, err := r.catalog.SourceAuthorityFleetPage(
				ctx,
				catalog.SourceAuthorityFleetPageRequest{
					Owner: r.fleetOwner, Generation: expected, After: after,
					Limit: catalog.SourceAuthorityFleetPageLimit,
				},
			)
			if err != nil {
				return err
			}
			for _, declaration := range page.Declarations {
				authority := declaration.Authority
				if err := r.settlePriorSourceAuthorityRuntime(
					ctx, r.fleetOwner, expected, authority,
				); err != nil {
					return err
				}
				if _, retained := desired[authority]; retained {
					continue
				}
				if _, err := r.catalog.RetireSourceAuthority(
					ctx,
					catalog.SourceAuthorityRetireRequest{
						Owner: r.fleetOwner, ExpectedGeneration: expected,
						Generation: r.fleetGeneration, Authority: authority,
						StageDigest: pending.StageDigest,
					},
				); err != nil {
					return err
				}
			}
			if page.Next == "" {
				break
			}
			if page.Next <= after {
				return catalog.ErrIntegrity
			}
			after = page.Next
		}
	}
	_, err = r.catalog.AcknowledgeSourceAuthorityFleet(
		ctx,
		catalog.SourceAuthorityFleetAcknowledgement{
			Owner: r.fleetOwner, ExpectedGeneration: expected,
			Generation: r.fleetGeneration, AuthorityCount: uint64(len(authorities)),
			AuthoritiesDigest: digest, DeclarationsDigest: declarationsDigest,
			StageDigest: pending.StageDigest,
		},
	)
	if err != nil {
		return err
	}
	return r.takeoverSourceAuthorityRuntimes(ctx, declarations, r.fleetGeneration)
}

func (r *authorityRegistry) abortSourceAuthorityFleet(
	ctx context.Context,
	request catalog.SourceAuthorityFleetAbortRequest,
) error {
	receipt, err := r.catalog.AbortSourceAuthorityFleet(ctx, request)
	if err != nil {
		return err
	}
	return receipt.Validate(request)
}

func (r *authorityRegistry) takeoverSourceAuthorityRuntimes(
	ctx context.Context,
	declarations []catalog.SourceAuthorityDeclaration,
	generation causal.Generation,
) error {
	for _, declaration := range declarations {
		ref := catalog.SourceAuthorityRuntimeRef{
			Owner: r.fleetOwner, Generation: generation, Authority: declaration.Authority,
		}
		state, err := r.catalog.SourceAuthorityRuntimeStatus(ctx, ref)
		if err != nil {
			return err
		}
		if state.DeclarationDigest != declaration.DeclarationDigest {
			return fmt.Errorf(
				"%w: source authority declaration changed before runtime startup",
				catalog.ErrMutationConflict,
			)
		}
		if !state.Closed {
			if state.Epoch == r.runtimeEpoch &&
				state.Process != nil && *state.Process == r.runtimeProcess {
				continue
			}
			return fmt.Errorf(
				"%w: source authority runtime remained open after global receipt recovery",
				catalog.ErrMutationConflict,
			)
		}
		if err := r.catalog.TakeoverSourceAuthorityRuntime(
			ctx,
			catalog.SourceAuthorityRuntimeTakeover{
				Ref: ref, ExpectedEpoch: state.Epoch, Epoch: r.runtimeEpoch,
				Process: r.runtimeProcess,
			},
		); err != nil {
			return err
		}
	}
	return nil
}

func (r *authorityRegistry) settlePriorSourceAuthorityRuntime(
	ctx context.Context,
	owner catalog.SourceAuthorityFleetOwnerID,
	generation causal.Generation,
	authority causal.SourceAuthorityID,
) error {
	ref := catalog.SourceAuthorityRuntimeRef{
		Owner: owner, Generation: generation, Authority: authority,
	}
	state, err := r.catalog.SourceAuthorityRuntimeStatus(ctx, ref)
	if err != nil {
		return err
	}
	if state.Closed {
		return nil
	}
	return fmt.Errorf(
		"%w: prior source authority runtime remained open after global receipt recovery",
		catalog.ErrMutationConflict,
	)
}

func (r *authorityRegistry) cancelStarted(started []*authorityDeclaration) error {
	for _, declaration := range started {
		declaration.runtime.Cancel()
	}
	var result error
	for _, declaration := range started {
		if err := declaration.runtime.Wait(context.Background()); err != nil {
			result = errors.Join(result, fmt.Errorf(
				"FuseKit runtime: settle started source authority %q: %w",
				declaration.authority(),
				err,
			))
		}
		result = errors.Join(result, r.closeDeclarationRuntimeFence(context.Background(), declaration))
		r.mu.Lock()
		declaration.closed = true
		r.mu.Unlock()
	}
	r.mu.Lock()
	r.closeErr = errors.Join(r.closeErr, result)
	r.mu.Unlock()
	return result
}

func (r *authorityRegistry) closeDeclarationRuntimeFence(
	ctx context.Context,
	declaration *authorityDeclaration,
) error {
	err := r.catalog.CloseSourceAuthorityRuntime(
		ctx,
		catalog.SourceAuthorityRuntimeFence{
			Owner: r.fleetOwner, Generation: r.fleetGeneration,
			Authority: declaration.authority(), Epoch: r.runtimeEpoch,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"FuseKit runtime: close source authority %q runtime fence: %w",
			declaration.authority(), err,
		)
	}
	return nil
}

func (r *authorityRegistry) groupFleet(fleet []tenant.TenantSpec) (map[string][]tenant.TenantSpec, error) {
	groups := make(map[string][]tenant.TenantSpec, len(r.bySource))
	for _, spec := range fleet {
		declaration, found := r.bySource[spec.Content.ID]
		if !found {
			return nil, fmt.Errorf("FuseKit runtime: no source authority configured for content source %q used by tenant %q", spec.Content.ID, spec.ID)
		}
		groups[string(declaration.authority())] = append(groups[string(declaration.authority())], spec)
	}
	for sourceID := range groups {
		slices.SortFunc(groups[sourceID], func(a, b tenant.TenantSpec) int {
			return strings.Compare(string(a.ID), string(b.ID))
		})
	}
	return groups, nil
}

func (r *authorityRegistry) requireSource(sourceID string) error {
	r.mu.RLock()
	declaration, found := r.bySource[sourceID]
	started := r.started
	closing := r.closing
	r.mu.RUnlock()
	if !found {
		return fmt.Errorf("FuseKit runtime: no source authority configured for content source %q", sourceID)
	}
	if !started || declaration.runtime == nil {
		return errors.New("FuseKit runtime: source authority registry is not started")
	}
	if closing {
		return sourceauthority.ErrClosed
	}
	return nil
}

func (r *authorityRegistry) Prepare(ctx context.Context, transition tenant.FleetTransition) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	before := canonicalFleet(transition.Before)
	drained := canonicalFleet(transition.Drained)
	if _, err := r.groupFleet(before); err != nil {
		return err
	}
	groups, err := r.groupFleet(drained)
	if err != nil {
		return err
	}
	r.mu.RLock()
	if !r.started || r.closing {
		r.mu.RUnlock()
		return sourceauthority.ErrClosed
	}
	current := append([]tenant.TenantSpec(nil), r.current...)
	r.mu.RUnlock()
	if slices.Equal(current, drained) {
		return nil
	}
	if !slices.Equal(current, before) {
		return fmt.Errorf("FuseKit runtime: source authority fleet does not match transition before state")
	}
	rollback, err := r.groupFleet(before)
	if err != nil {
		return err
	}
	opCtx, cancel := authorityOperationContext(r.ctx, ctx)
	defer cancel()
	var attempted []*authorityDeclaration
	for _, declaration := range r.ordered {
		attempted = append(attempted, declaration)
		reconfigureErr := declaration.runtime.Reconfigure(opCtx, groups[string(declaration.authority())])
		if reconfigureErr == nil {
			if requestErr := ctx.Err(); requestErr != nil {
				reconfigureErr = requestErr
			} else if operationErr := opCtx.Err(); operationErr != nil {
				reconfigureErr = operationErr
			}
		}
		if reconfigureErr != nil {
			result := fmt.Errorf("FuseKit runtime: prepare source authority %q fleet: %w", declaration.authority(), reconfigureErr)
			rollbackErr := r.rollbackPrepared(attempted, rollback)
			if rollbackErr == nil {
				r.mu.Lock()
				r.current = before
				r.mu.Unlock()
				return result
			}
			return errors.Join(result, rollbackErr, r.failClosedLocked())
		}
	}
	r.mu.Lock()
	r.current = drained
	r.mu.Unlock()
	return nil
}

func (r *authorityRegistry) rollbackPrepared(
	attempted []*authorityDeclaration,
	rollback map[string][]tenant.TenantSpec,
) error {
	ctx, cancel := context.WithTimeout(r.ctx, r.closeTimeout)
	defer cancel()
	var result error
	for index := len(attempted) - 1; index >= 0; index-- {
		declaration := attempted[index]
		delay := authorityInitialRetryDelay
		for {
			err := declaration.runtime.Reconfigure(ctx, rollback[string(declaration.authority())])
			if err == nil {
				break
			}
			if !sourceauthority.IsTransient(err) {
				result = errors.Join(result, fmt.Errorf("FuseKit runtime: restore source authority %q fleet: %w", declaration.authority(), err))
				break
			}
			select {
			case <-ctx.Done():
				result = errors.Join(result, fmt.Errorf(
					"FuseKit runtime: restore source authority %q fleet: %w",
					declaration.authority(), errors.Join(err, ctx.Err()),
				))
			case <-r.retryClock.After(delay):
				delay = min(delay*2, authorityMaximumRetryDelay)
				continue
			}
			break
		}
	}
	return result
}

func (r *authorityRegistry) Commit(_ context.Context, transition tenant.FleetTransition) error {
	return r.settleFleet(transition.Committed)
}

func (r *authorityRegistry) Abort(_ context.Context, transition tenant.FleetTransition) error {
	return r.settleFleet(transition.Before)
}

func (r *authorityRegistry) settleFleet(fleet []tenant.TenantSpec) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	target := canonicalFleet(fleet)
	groups, err := r.groupFleet(target)
	if err != nil {
		return err
	}
	delay := authorityInitialRetryDelay
	for {
		r.mu.RLock()
		if !r.started || r.closing {
			r.mu.RUnlock()
			return sourceauthority.ErrClosed
		}
		if slices.Equal(r.current, target) {
			r.mu.RUnlock()
			return nil
		}
		r.mu.RUnlock()
		var result error
		for _, declaration := range r.ordered {
			if reconfigureErr := declaration.runtime.Reconfigure(r.ctx, groups[string(declaration.authority())]); reconfigureErr != nil {
				result = errors.Join(result, fmt.Errorf("FuseKit runtime: settle source authority %q fleet: %w", declaration.authority(), reconfigureErr))
			}
		}
		if result == nil {
			r.mu.Lock()
			r.current = target
			r.mu.Unlock()
			return nil
		}
		if !sourceauthority.IsTransient(result) {
			return errors.Join(result, r.failClosedLocked())
		}
		select {
		case <-r.ctx.Done():
			return errors.Join(result, r.ctx.Err(), r.failClosedLocked())
		case <-r.retryClock.After(delay):
			delay = min(delay*2, authorityMaximumRetryDelay)
		}
	}
}

func (r *authorityRegistry) failClosedLocked() error {
	r.mu.Lock()
	r.closing = true
	r.current = nil
	var declarations []*authorityDeclaration
	for _, declaration := range r.ordered {
		if declaration.runtime != nil && !declaration.closed {
			declarations = append(declarations, declaration)
		}
	}
	r.mu.Unlock()
	r.cancel()
	for _, declaration := range declarations {
		declaration.runtime.Cancel()
	}
	var result error
	for _, declaration := range declarations {
		if err := declaration.runtime.Wait(context.Background()); err != nil {
			result = errors.Join(result, fmt.Errorf(
				"FuseKit runtime: settle failed source authority %q: %w",
				declaration.authority(),
				err,
			))
		}
		result = errors.Join(result, r.closeDeclarationRuntimeFence(context.Background(), declaration))
		r.mu.Lock()
		declaration.closed = true
		r.mu.Unlock()
	}
	r.mu.Lock()
	r.closeErr = errors.Join(r.closeErr, result)
	r.mu.Unlock()
	return result
}

func authorityOperationContext(lifecycle, request context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(lifecycle)
	stop := context.AfterFunc(request, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func canonicalFleet(fleet []tenant.TenantSpec) []tenant.TenantSpec {
	result := append([]tenant.TenantSpec(nil), fleet...)
	slices.SortFunc(result, func(a, b tenant.TenantSpec) int {
		return strings.Compare(string(a.ID), string(b.ID))
	})
	return result
}

func (r *authorityRegistry) runtimeForSource(sourceID string) (managedAuthority, error) {
	if err := r.requireSource(sourceID); err != nil {
		return nil, err
	}
	r.mu.RLock()
	runtime := r.bySource[sourceID].runtime
	r.mu.RUnlock()
	return runtime, nil
}

func (r *authorityRegistry) PrepareSourceMutation(ctx context.Context, step tenant.SourceMutationStep) (tenant.SourceMutationOperation, error) {
	runtime, err := r.runtimeForSource(step.SourceID)
	if err != nil {
		return tenant.SourceMutationOperation{}, err
	}
	return runtime.PrepareSourceMutation(ctx, step)
}

func (r *authorityRegistry) ApplySourceMutation(
	ctx context.Context,
	step tenant.SourceMutationStep,
	operation tenant.SourceMutationOperation,
	content tenant.SourceMutationContent,
) error {
	runtime, err := r.runtimeForSource(step.SourceID)
	if err != nil {
		return err
	}
	return runtime.ApplySourceMutation(ctx, step, operation, content)
}

func (r *authorityRegistry) SourceMutationCommitted(ctx context.Context, commit tenant.SourceMutationCommit) error {
	r.mu.RLock()
	declaration := r.bySource[commit.SourceID]
	var runtime managedAuthority
	if declaration != nil {
		runtime = declaration.runtime
	}
	r.mu.RUnlock()
	if runtime != nil {
		return runtime.SourceMutationCommitted(ctx, commit)
	}
	return fmt.Errorf("FuseKit runtime: source authority %q is not active", commit.SourceID)
}

func (r *authorityRegistry) recoverSemanticReceipts(ctx context.Context) error {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.mu.RLock()
	if !r.started || r.closing {
		r.mu.RUnlock()
		return sourceauthority.ErrClosed
	}
	r.mu.RUnlock()
	pager, ok := r.catalog.(interface {
		PendingSourceDriverReceiptAuthorities(context.Context, causal.SourceAuthorityID, int) (catalog.SourceDriverReceiptAuthorityPage, error)
	})
	if !ok {
		return errors.New("FuseKit runtime: source-driver receipt authority discovery is unavailable")
	}
	after := causal.SourceAuthorityID("")
	for {
		page, err := pager.PendingSourceDriverReceiptAuthorities(
			ctx, after, catalog.SourceDriverReceiptAuthorityPageLimit,
		)
		if err != nil {
			return err
		}
		for _, authority := range page.Authorities {
			r.mu.RLock()
			declaration := r.bySource[string(authority)]
			var runtime managedAuthority
			if declaration != nil && isSemanticSourceAuthority(declaration.spec) && !declaration.closed {
				runtime = declaration.runtime
			}
			r.mu.RUnlock()
			recovery, ok := runtime.(interface{ RecoverCommittedReceipts(context.Context) error })
			if !ok {
				return fmt.Errorf(
					"%w: no active semantic authority owns pending receipts for %q",
					catalog.ErrIntegrity, authority,
				)
			}
			if err := recovery.RecoverCommittedReceipts(ctx); err != nil {
				return err
			}
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	page, err := pager.PendingSourceDriverReceiptAuthorities(ctx, "", 1)
	if err != nil {
		return err
	}
	if len(page.Authorities) != 0 {
		return fmt.Errorf("%w: source-driver receipts appeared during recovery", catalog.ErrIntegrity)
	}
	return nil
}

func (r *authorityRegistry) barrierFor(ctx context.Context, spec tenant.TenantSpec) (sourceauthority.BarrierResult, error) {
	runtime, err := r.runtimeForSource(spec.Content.ID)
	if err != nil {
		return sourceauthority.BarrierResult{}, err
	}
	return runtime.Barrier(ctx, spec.ID, spec.Generation)
}

func (r *authorityRegistry) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closing = true
		r.mu.Unlock()
		go r.closeAll()
	})
	select {
	case <-r.closeDone:
	case <-ctx.Done():
		r.Cancel()
		<-r.closeDone
	}
	r.mu.RLock()
	result := r.closeErr
	r.mu.RUnlock()
	return errors.Join(result, ctx.Err())
}

func (r *authorityRegistry) closeAll() {
	r.lifecycle.Lock()
	defer r.lifecycle.Unlock()
	r.mu.RLock()
	declarations := append([]*authorityDeclaration(nil), r.ordered...)
	result := r.closeErr
	r.mu.RUnlock()
	for _, declaration := range declarations {
		r.mu.RLock()
		runtime, closed := declaration.runtime, declaration.closed
		r.mu.RUnlock()
		if runtime == nil || closed {
			continue
		}
		if err := runtime.Close(context.Background()); err != nil {
			result = errors.Join(result, fmt.Errorf("FuseKit runtime: close source authority %q: %w", declaration.authority(), err))
		}
		if err := runtime.Wait(context.Background()); err != nil {
			result = errors.Join(result, fmt.Errorf("FuseKit runtime: wait source authority %q: %w", declaration.authority(), err))
		}
		result = errors.Join(result, r.closeDeclarationRuntimeFence(context.Background(), declaration))
		r.mu.Lock()
		declaration.closed = true
		r.mu.Unlock()
	}
	r.cancel()
	r.mu.Lock()
	r.closeErr = result
	r.mu.Unlock()
	close(r.closeDone)
}

func (r *authorityRegistry) Cancel() {
	r.cancel()
	r.mu.Lock()
	r.closing = true
	var runtimes []managedAuthority
	for _, declaration := range r.ordered {
		if declaration.runtime != nil && !declaration.closed {
			runtimes = append(runtimes, declaration.runtime)
		}
	}
	r.mu.Unlock()
	for _, runtime := range runtimes {
		runtime.Cancel()
	}
}

func (r *authorityRegistry) Wait(ctx context.Context) error {
	return r.Close(ctx)
}

type authorityRouter struct {
	mu       sync.RWMutex
	current  *authorityRegistry
	changing bool
	closed   bool
}

func (r *authorityRouter) installInitial(current *authorityRegistry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.changing || r.current != nil {
		return errors.New("FuseKit runtime: source authority router was already initialized")
	}
	r.current = current
	return nil
}

func (r *authorityRouter) replace(
	ctx context.Context,
	next *authorityRegistry,
	tenants []tenant.TenantSpec,
) (resultErr error) {
	r.mu.Lock()
	if r.closed || r.changing {
		r.mu.Unlock()
		return sourceauthority.ErrClosed
	}
	r.changing = true
	prior := r.current
	r.current = nil
	r.mu.Unlock()
	defer func() {
		if resultErr == nil {
			return
		}
		if next != nil {
			next.Cancel()
			resultErr = errors.Join(resultErr, next.Wait(context.Background()))
		}
	}()
	if prior != nil {
		if err := errors.Join(prior.Close(ctx), prior.Wait(ctx)); err != nil {
			return fmt.Errorf("FuseKit runtime: settle prior source authority fleet: %w", err)
		}
	}
	if next != nil {
		if err := next.start(ctx, tenants); err != nil {
			return fmt.Errorf("FuseKit runtime: start desired source authority fleet: %w", err)
		}
		if err := next.recoverSemanticReceipts(ctx); err != nil {
			return fmt.Errorf("FuseKit runtime: recover desired semantic source receipts: %w", err)
		}
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return sourceauthority.ErrClosed
	}
	r.current = next
	r.changing = false
	r.mu.Unlock()
	return nil
}

func (r *authorityRouter) lockCurrent() (*authorityRegistry, error) {
	r.mu.RLock()
	if r.closed || r.changing {
		r.mu.RUnlock()
		return nil, sourceauthority.ErrClosed
	}
	if r.current == nil {
		r.mu.RUnlock()
		return nil, errors.New("FuseKit runtime: source authority fleet is not configured")
	}
	return r.current, nil
}

func (r *authorityRouter) unlockCurrent() { r.mu.RUnlock() }

func (r *authorityRouter) requireSource(sourceID string) error {
	current, err := r.lockCurrent()
	if err != nil {
		return err
	}
	defer r.unlockCurrent()
	return current.requireSource(sourceID)
}

func (r *authorityRouter) Prepare(ctx context.Context, transition tenant.FleetTransition) error {
	current, err := r.lockCurrent()
	if err != nil {
		return err
	}
	defer r.unlockCurrent()
	return current.Prepare(ctx, transition)
}

func (r *authorityRouter) Commit(ctx context.Context, transition tenant.FleetTransition) error {
	current, err := r.lockCurrent()
	if err != nil {
		return err
	}
	defer r.unlockCurrent()
	return current.Commit(ctx, transition)
}

func (r *authorityRouter) Abort(ctx context.Context, transition tenant.FleetTransition) error {
	current, err := r.lockCurrent()
	if err != nil {
		return err
	}
	defer r.unlockCurrent()
	return current.Abort(ctx, transition)
}

func (r *authorityRouter) PrepareSourceMutation(
	ctx context.Context,
	step tenant.SourceMutationStep,
) (tenant.SourceMutationOperation, error) {
	current, err := r.lockCurrent()
	if err != nil {
		return tenant.SourceMutationOperation{}, err
	}
	defer r.unlockCurrent()
	return current.PrepareSourceMutation(ctx, step)
}

func (r *authorityRouter) ApplySourceMutation(
	ctx context.Context,
	step tenant.SourceMutationStep,
	operation tenant.SourceMutationOperation,
	content tenant.SourceMutationContent,
) error {
	current, err := r.lockCurrent()
	if err != nil {
		return err
	}
	defer r.unlockCurrent()
	return current.ApplySourceMutation(ctx, step, operation, content)
}

func (r *authorityRouter) SourceMutationCommitted(ctx context.Context, commit tenant.SourceMutationCommit) error {
	current, err := r.lockCurrent()
	if err != nil {
		return err
	}
	defer r.unlockCurrent()
	return current.SourceMutationCommitted(ctx, commit)
}

func (r *authorityRouter) barrierFor(ctx context.Context, spec tenant.TenantSpec) (sourceauthority.BarrierResult, error) {
	current, err := r.lockCurrent()
	if err != nil {
		return sourceauthority.BarrierResult{}, err
	}
	defer r.unlockCurrent()
	return current.barrierFor(ctx, spec)
}

func (r *authorityRouter) Cancel() {
	r.mu.RLock()
	current := r.current
	r.mu.RUnlock()
	if current != nil {
		current.Cancel()
	}
}

func (r *authorityRouter) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.changing = true
	current := r.current
	r.current = nil
	r.mu.Unlock()
	if current == nil {
		return nil
	}
	return current.Close(ctx)
}

func (r *authorityRouter) Wait(ctx context.Context) error { return r.Close(ctx) }

type authorityTenantController struct {
	tenants     *tenant.TenantRuntime
	authorities interface{ requireSource(string) error }
}

func (c authorityTenantController) ProvisionTenant(ctx context.Context, spec tenant.TenantSpec) error {
	if err := c.authorities.requireSource(spec.Content.ID); err != nil {
		return err
	}
	return c.tenants.ProvisionTenant(ctx, spec)
}

func (c authorityTenantController) ReplaceTenant(ctx context.Context, expected catalog.Generation, spec tenant.TenantSpec) error {
	if err := c.authorities.requireSource(spec.Content.ID); err != nil {
		return err
	}
	return c.tenants.ReplaceTenant(ctx, expected, spec)
}

func (c authorityTenantController) RemoveTenant(ctx context.Context, id catalog.TenantID, generation catalog.Generation) error {
	return c.tenants.RemoveTenant(ctx, id, generation)
}

func (c authorityTenantController) AcquireGeneration(ctx context.Context, id catalog.TenantID, generation catalog.Generation) (mountmux.GenerationPin, error) {
	return c.tenants.AcquireGeneration(ctx, id, generation)
}

func (c authorityTenantController) State(ctx context.Context, owner tenant.OwnerID, id catalog.TenantID) (tenant.TenantStatus, error) {
	return c.tenants.State(ctx, owner, id)
}

func (c authorityTenantController) Specs() []tenant.TenantSpec { return c.tenants.Specs() }

type preparationBarrier struct {
	tenants     *tenant.TenantRuntime
	authorities interface {
		barrierFor(context.Context, tenant.TenantSpec) (sourceauthority.BarrierResult, error)
	}
}

func (b preparationBarrier) Barrier(ctx context.Context, id catalog.TenantID, generation catalog.Generation) (sourceauthority.BarrierResult, error) {
	for _, spec := range b.tenants.Specs() {
		if spec.ID != id {
			continue
		}
		if spec.Generation != generation {
			return sourceauthority.BarrierResult{}, tenant.ErrGenerationConflict
		}
		return b.authorities.barrierFor(ctx, spec)
	}
	return sourceauthority.BarrierResult{}, tenant.ErrTenantNotFound
}

var (
	_ tenant.SourceMutationPlanner = (*authorityRegistry)(nil)
	_ tenant.FleetTransitionHook   = (*authorityRegistry)(nil)
	_ tenant.SourceMutationPlanner = (*authorityRouter)(nil)
	_ tenant.FleetTransitionHook   = (*authorityRouter)(nil)
	_ mountmux.TenantController    = authorityTenantController{}
	_ sourceauthority.Barrier      = preparationBarrier{}
)
