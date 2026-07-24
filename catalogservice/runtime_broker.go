package catalogservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
)

const (
	brokerCommandBuffer  = 32
	brokerCommandTimeout = 30 * time.Second
)

var (
	errBrokerSessionLost     = errors.New("catalog service: broker session lost")
	errBrokerDeliveryUnknown = errors.New("catalog service: broker delivery unknown")
)

type brokerPending struct {
	command        catalogproto.BrokerCommand
	reconcileEpoch uint64
	done           chan brokerOutcome
	removal        *catalog.FileProviderDomainRemoval
	managedDomain  *catalogproto.DomainID
	paths          []catalogproto.CriticalMaterializationPath
	attempt        catalog.BrokerCommandAttempt
	settled        chan struct{}
}

type brokerOutcome struct {
	delivery convergence.Delivery
	err      error
	paths    []catalogproto.CriticalMaterializationPath
}

// RuntimeBrokerPhase is the authenticated broker session readiness observed by the holder.
type RuntimeBrokerPhase uint8

const (
	// RuntimeBrokerStarting means the configured broker has no authenticated active session.
	RuntimeBrokerStarting RuntimeBrokerPhase = iota
	// RuntimeBrokerLive means the exact launched broker owns the active authenticated
	// session and its observed domain set is at a reconciliation fixed point.
	RuntimeBrokerLive
	// RuntimeBrokerFailed means the broker runtime is terminally closed.
	RuntimeBrokerFailed
)

// RuntimeBrokerStore is the catalog-worker-owned persistence used by the
// signed broker runtime. The holder must not satisfy it with direct SQLite.
type RuntimeBrokerStore interface {
	BeginFileProviderDomainRemoval(context.Context, string, catalog.TenantID, catalog.Generation) (catalog.FileProviderDomainRemoval, error)
	FileProviderDomainRemovalState(context.Context, string, catalog.TenantID, catalog.Generation) (catalog.FileProviderDomainRemoval, error)
	ActivationPresentationTarget(context.Context, causal.ActivationKey) (catalog.TenantPresentationTarget, error)
	NextBrokerCommandID(context.Context) (uint64, error)
	ConfirmFileProviderDomain(context.Context, catalog.FileProviderDomain) error
	InvalidateFileProviderDomain(context.Context, catalog.TenantID, catalog.Generation) error
	ConfirmFileProviderDomainAbsent(context.Context, causal.DomainID) error
	PageFileProviderDomains(context.Context, catalog.TenantID, int) (catalog.FileProviderDomainPage, error)
	FileProviderDomainForTenant(context.Context, catalog.TenantID) (catalog.FileProviderDomain, bool, error)
	PageFileProviderDomainRemovals(context.Context, catalog.TenantID, int) (catalog.FileProviderDomainRemovalPage, error)
	ConfirmFileProviderDomainRemoval(context.Context, catalog.FileProviderDomainRemoval) error
	BeginBrokerCommandAttempt(context.Context, catalog.BrokerCommandAttempt) (catalog.BrokerCommandAttempt, bool, error)
	TransitionBrokerCommandAttempt(context.Context, catalog.BrokerCommandAttempt, catalog.BrokerCommandAttemptState) (catalog.BrokerCommandAttempt, error)
	AbandonBrokerCommandAttempt(context.Context, catalog.BrokerCommandAttempt) error
	RecoverReapedBrokerCommandAttempts(context.Context, catalog.BrokerProcessIdentity) error
	RecoverBrokerCommandAttempts(context.Context) error
}

// BrokerProcessOwner binds authenticated sessions to daemonkit process records
// and settles an exact poisoned generation before starting its replacement.
type BrokerProcessOwner interface {
	BindBroker(context.Context, wire.Peer) (catalog.BrokerProcessIdentity, error)
	RetireBroker(context.Context, catalog.BrokerProcessIdentity) error
	StartBroker(context.Context) error
}

// RuntimeBroker owns actual-domain reconciliation and convergence delivery over one broker stream.
type RuntimeBroker struct {
	catalog              RuntimeBrokerStore
	identity             BrokerIdentity
	activationGeneration string
	owner                BrokerProcessOwner
	ctx                  context.Context
	cancel               context.CancelFunc

	mu                 sync.Mutex
	active             *runtimeBrokerSession
	binding            bool
	recovering         bool
	recovered          bool
	closed             bool
	pending            map[uint64]brokerPending
	reconciling        *runtimeBrokerSession
	reconcileRequested bool
	ready              func()
	changed            chan struct{}
	commandTimeout     time.Duration
}

// ReadinessPhase returns authenticated broker and domain reconciliation readiness.
func (b *RuntimeBroker) ReadinessPhase() RuntimeBrokerPhase {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.closed:
		return RuntimeBrokerFailed
	case b.active != nil && b.active.reconcileEpoch != 0 &&
		b.active.settledEpoch == b.active.reconcileEpoch:
		return RuntimeBrokerLive
	default:
		return RuntimeBrokerStarting
	}
}

// SetReady installs the non-blocking convergence retry triggered after domain reconciliation.
func (b *RuntimeBroker) SetReady(ready func()) {
	b.mu.Lock()
	b.ready = ready
	b.mu.Unlock()
}

// Start launches the fixed signed broker and waits for its authenticated
// session to reconcile the complete domain set.
func (b *RuntimeBroker) Start(ctx context.Context) error {
	b.mu.Lock()
	closed := b.closed
	recovered := b.recovered
	active := b.active != nil
	b.mu.Unlock()
	if closed {
		return errors.New("catalog service: broker runtime is closed")
	}
	if !recovered {
		return errors.New("catalog service: broker runtime is not recovered")
	}
	if !active {
		if err := b.owner.StartBroker(ctx); err != nil {
			return fmt.Errorf("catalog service: start signed broker: %w", err)
		}
	}
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return errors.New("catalog service: broker runtime closed during readiness")
		}
		active := b.active
		if active != nil && active.reconcileEpoch != 0 && active.settledEpoch == active.reconcileEpoch {
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("catalog service: await broker reconciliation: %w", ctx.Err())
		}
	}
}

// NewRuntimeBroker creates an unconnected broker runtime over durable catalog state.
func NewRuntimeBroker(
	ctx context.Context,
	store RuntimeBrokerStore,
	identity BrokerIdentity,
	activationGeneration string,
	owner BrokerProcessOwner,
) (*RuntimeBroker, error) {
	if ctx == nil {
		return nil, errors.New("catalog service: broker lifecycle context is required")
	}
	if store == nil {
		return nil, errors.New("catalog service: broker catalog is required")
	}
	if owner == nil {
		return nil, errors.New("catalog service: broker process owner is required")
	}
	if identity.ProductBuild == "" || identity.Executable == "" || identity.DesignatedRequirement == "" ||
		identity.EntitlementValidationDigest == ([32]byte{}) {
		return nil, errors.New("catalog service: fixed broker identity is incomplete")
	}
	if activationGeneration == "" || strings.ContainsRune(activationGeneration, 0) {
		return nil, errors.New("catalog service: broker activation generation is incomplete")
	}
	lifecycle, cancel := context.WithCancel(ctx)
	return &RuntimeBroker{
		catalog: store, identity: identity, activationGeneration: activationGeneration,
		owner: owner, ctx: lifecycle, cancel: cancel,
		pending: make(map[uint64]brokerPending), changed: make(chan struct{}),
		commandTimeout: brokerCommandTimeout,
	}, nil
}

// PrepareFileProviderPresentation invalidates any prior observation, asks the
// current signed broker to re-observe the OS domain, and returns only the exact
// proof confirmed for this holder activation.
func (b *RuntimeBroker) PrepareFileProviderPresentation(
	ctx context.Context,
	tenantID catalog.TenantID,
	generation catalog.Generation,
) (catalog.FileProviderDomain, error) {
	desired, found, err := b.catalog.FileProviderDomainForTenant(ctx, tenantID)
	if err != nil {
		return catalog.FileProviderDomain{}, err
	}
	if !found {
		return catalog.FileProviderDomain{}, catalog.ErrNotFound
	}
	if desired.Generation != generation {
		return catalog.FileProviderDomain{}, catalog.ErrGenerationMismatch
	}
	registration, err := protocolDomainRegistration(desired)
	if err != nil {
		return catalog.FileProviderDomain{}, err
	}
	b.mu.Lock()
	session := b.active
	closed := b.closed
	b.mu.Unlock()
	if closed || session == nil {
		return catalog.FileProviderDomain{}, errBrokerSessionLost
	}
	if err := b.catalog.InvalidateFileProviderDomain(ctx, tenantID, generation); err != nil {
		return catalog.FileProviderDomain{}, err
	}
	done := make(chan brokerOutcome, 1)
	if err := b.enqueue(ctx, session, catalogproto.BrokerCommand{
		Kind: catalogproto.BrokerCommandKindRegisterDomain, Registration: &registration,
	}, done); err != nil {
		return catalog.FileProviderDomain{}, err
	}
	select {
	case outcome := <-done:
		if outcome.err != nil {
			return catalog.FileProviderDomain{}, outcome.err
		}
		if outcome.delivery != convergence.DeliverySent {
			return catalog.FileProviderDomain{}, errBrokerDeliveryUnknown
		}
	case <-ctx.Done():
		return catalog.FileProviderDomain{}, ctx.Err()
	case <-session.done:
		return catalog.FileProviderDomain{}, errBrokerSessionLost
	}
	confirmed, found, err := b.catalog.FileProviderDomainForTenant(ctx, tenantID)
	if err != nil {
		return catalog.FileProviderDomain{}, err
	}
	if !found || !confirmed.Registered || confirmed.Generation != generation ||
		confirmed.ActivationGeneration != b.activationGeneration {
		return catalog.FileProviderDomain{}, fmt.Errorf("%w: File Provider presentation was not confirmed for the current activation", catalog.ErrIntegrity)
	}
	return confirmed, nil
}

// ScheduleCriticalMaterialization asks the signed broker to schedule every exact object download.
func (b *RuntimeBroker) ScheduleCriticalMaterialization(
	ctx context.Context,
	readiness catalogproto.CriticalReadinessProof,
) ([]catalogproto.CriticalMaterializationPath, error) {
	b.mu.Lock()
	session := b.active
	closed := b.closed
	b.mu.Unlock()
	if closed || session == nil {
		return nil, errBrokerSessionLost
	}
	done := make(chan brokerOutcome, 1)
	if err := b.enqueue(ctx, session, catalogproto.BrokerCommand{
		Kind: catalogproto.BrokerCommandKindMaterializeCritical, CriticalReadiness: &readiness,
	}, done); err != nil {
		return nil, err
	}
	select {
	case outcome := <-done:
		if outcome.err != nil || outcome.delivery != convergence.DeliverySent {
			return nil, errors.Join(errBrokerDeliveryUnknown, outcome.err)
		}
		if len(outcome.paths) != len(readiness.Objects) {
			return nil, fmt.Errorf("%w: broker materialization path count changed", catalog.ErrIntegrity)
		}
		expected := make(map[catalogproto.ObjectID]struct{}, len(readiness.Objects))
		for _, object := range readiness.Objects {
			expected[object.ObjectID] = struct{}{}
		}
		for _, path := range outcome.paths {
			if _, ok := expected[path.ObjectID]; !ok {
				return nil, fmt.Errorf("%w: broker materialization object changed", catalog.ErrIntegrity)
			}
			delete(expected, path.ObjectID)
		}
		if len(expected) != 0 {
			return nil, fmt.Errorf("%w: broker materialization path is incomplete", catalog.ErrIntegrity)
		}
		domain, found, err := b.catalog.FileProviderDomainForTenant(ctx, catalog.TenantID(readiness.Lease.TenantID))
		if err != nil {
			return nil, err
		}
		if !found || !domain.Registered || domain.DomainID != causal.DomainID(readiness.DomainID) ||
			domain.Generation != catalog.Generation(readiness.TenantGeneration) || domain.Root.String() != string(readiness.RootID) ||
			domain.PresentationInstance != string(readiness.PresentationInstanceID) ||
			domain.ActivationGeneration != readiness.ActivationGeneration || !filepath.IsAbs(domain.PublicPath) ||
			filepath.Clean(domain.PublicPath) != domain.PublicPath {
			return nil, fmt.Errorf("%w: broker materialization presentation changed", catalog.ErrIntegrity)
		}
		uniquePaths := make(map[string]struct{}, len(outcome.paths))
		for _, path := range outcome.paths {
			relative, relErr := filepath.Rel(domain.PublicPath, path.Path)
			if relErr != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
				return nil, fmt.Errorf("%w: broker materialization path escaped its presentation", catalog.ErrIntegrity)
			}
			if _, duplicate := uniquePaths[path.Path]; duplicate {
				return nil, fmt.Errorf("%w: broker materialization path is duplicated", catalog.ErrIntegrity)
			}
			uniquePaths[path.Path] = struct{}{}
		}
		return outcome.paths, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-session.done:
		return nil, errBrokerSessionLost
	}
}

// Recover settles durable command state after the owning process registry has
// reaped every prior generation. No broker process may bind before it succeeds.
func (b *RuntimeBroker) Recover(ctx context.Context) error {
	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return errors.New("catalog service: broker runtime is closed")
		}
		if b.recovered {
			b.mu.Unlock()
			return nil
		}
		if b.binding || b.active != nil {
			b.mu.Unlock()
			return errors.New("catalog service: broker recovery began after process admission")
		}
		if !b.recovering {
			b.recovering = true
			b.mu.Unlock()
			break
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("catalog service: await broker recovery: %w", ctx.Err())
		}
	}
	if err := b.catalog.RecoverBrokerCommandAttempts(ctx); err != nil {
		b.mu.Lock()
		b.recovering = false
		b.signalChangedLocked()
		b.mu.Unlock()
		return fmt.Errorf("catalog service: recover broker command attempts: %w", err)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.recovering = false
	if b.closed || b.binding || b.active != nil {
		b.signalChangedLocked()
		return errors.New("catalog service: broker lifecycle changed during recovery")
	}
	b.recovered = true
	b.signalChangedLocked()
	return nil
}

func (b *RuntimeBroker) RemoveTenantDomain(
	ctx context.Context,
	owner string,
	tenantID catalog.TenantID,
	generation catalog.Generation,
) error {
	removal, err := b.catalog.BeginFileProviderDomainRemoval(ctx, owner, tenantID, generation)
	if err != nil {
		return err
	}
	if removal.ConfirmedAbsent {
		return nil
	}
	b.requestReconcile()
	for {
		// Snapshot the edge before reading durable state so a concurrent
		// confirmation cannot land between the read and the wait.
		b.mu.Lock()
		changed := b.changed
		closed := b.closed
		b.mu.Unlock()
		if closed {
			return errors.New("catalog service: broker runtime closed during domain removal")
		}
		state, err := b.catalog.FileProviderDomainRemovalState(ctx, owner, tenantID, generation)
		if err != nil {
			return err
		}
		if state.ConfirmedAbsent {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// ProveTenantDomainRemoved validates a replay after tenant runtime state is gone.
func (b *RuntimeBroker) ProveTenantDomainRemoved(
	ctx context.Context,
	owner string,
	tenantID catalog.TenantID,
	generation catalog.Generation,
) error {
	removal, err := b.catalog.FileProviderDomainRemovalState(ctx, owner, tenantID, generation)
	if err != nil {
		return err
	}
	if !removal.ConfirmedAbsent {
		return errors.New("catalog service: File Provider domain removal is not settled")
	}
	return nil
}

// OpenBroker installs one authenticated signed-app broker session.
func (b *RuntimeBroker) OpenBroker(ctx context.Context, identity Identity, _ string) (BrokerSession, error) {
	if identity.Peer.Executable != b.identity.Executable {
		return nil, fmt.Errorf("catalog service: broker executable %q is not fixed %q", identity.Peer.Executable, b.identity.Executable)
	}
	for {
		b.mu.Lock()
		if !b.recovered {
			b.mu.Unlock()
			return nil, errors.New("catalog service: broker runtime is not recovered")
		}
		if b.closed {
			b.mu.Unlock()
			return nil, errors.New("catalog service: broker runtime closed")
		}
		if b.active != nil {
			active := b.active
			if active.identity.Peer.PID == identity.Peer.PID &&
				active.identity.Peer.StartTime == identity.Peer.StartTime &&
				active.identity.Peer.Boot == identity.Peer.Boot {
				b.mu.Unlock()
				return nil, errors.New("catalog service: duplicate broker process session")
			}
			b.mu.Unlock()
			select {
			case <-active.done:
				continue
			case <-ctx.Done():
				return nil, errors.Join(errBrokerSessionLost, ctx.Err())
			}
		}
		if b.binding {
			changed := b.changed
			b.mu.Unlock()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return nil, errors.Join(errBrokerSessionLost, ctx.Err())
			}
		}
		b.binding = true
		b.mu.Unlock()
		break
	}
	process, err := b.owner.BindBroker(ctx, identity.Peer)
	if err != nil {
		b.mu.Lock()
		b.binding = false
		b.signalChangedLocked()
		b.mu.Unlock()
		return nil, fmt.Errorf("catalog service: bind broker process: %w", err)
	}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		retireErr := b.retireBrokerProcess(process)
		b.mu.Lock()
		b.binding = false
		b.signalChangedLocked()
		b.mu.Unlock()
		return nil, errors.Join(errors.New("catalog service: broker runtime closed"), retireErr)
	}
	b.binding = false
	sessionCtx, cancel := context.WithCancel(ctx)
	session := &runtimeBrokerSession{
		hub: b, ctx: sessionCtx, cancel: cancel,
		commands: make(chan catalogproto.BrokerCommand, brokerCommandBuffer),
		done:     make(chan struct{}), transportDone: make(chan struct{}),
		ready: make(chan struct{}), identity: identity,
		slots: make(chan struct{}, brokerCommandBuffer), process: process,
	}
	for range brokerCommandBuffer {
		session.slots <- struct{}{}
	}
	b.active = session
	b.signalChangedLocked()
	b.mu.Unlock()
	if err := b.enqueueReconcile(sessionCtx, session); err != nil {
		close(session.ready)
		session.Close(err)
		return nil, err
	}
	close(session.ready)
	return session, nil
}

func (b *RuntimeBroker) Notify(ctx context.Context, notification causal.ActivationEvent) (convergence.Delivery, error) {
	if err := causal.ValidateActivationEvent(notification); err != nil || notification.Backend != causal.BackendFileProvider {
		return convergence.DeliveryNotSent, errors.Join(errors.New("catalog service: invalid File Provider activation"), err)
	}
	b.mu.Lock()
	session := b.active
	closed := b.closed
	b.mu.Unlock()
	if closed || session == nil {
		return convergence.DeliveryNotSent, errBrokerSessionLost
	}
	select {
	case <-session.transportDone:
		return convergence.DeliveryNotSent, errBrokerSessionLost
	default:
	}
	target, err := b.catalog.ActivationPresentationTarget(ctx, notification.Key())
	if err != nil {
		return convergence.DeliveryNotSent, err
	}
	if target.PresentationID != notification.PresentationID || target.Backend != notification.Backend {
		return convergence.DeliveryNotSent, errors.New("catalog service: activation target identity mismatch")
	}
	protocolTargets := make([]catalogproto.SignalTarget, 0, len(target.SignalTargets))
	for _, signal := range target.SignalTargets {
		if signal.WorkingSet {
			protocolTargets = append(protocolTargets, catalogproto.SignalTarget{Kind: catalogproto.SignalTargetKindWorkingSet})
			continue
		}
		parent := catalogproto.ObjectID(signal.Parent.String())
		protocolTargets = append(protocolTargets, catalogproto.SignalTarget{Kind: catalogproto.SignalTargetKindContainer, ParentID: &parent})
	}
	protocolCauses := make([]catalogproto.ActivationSourceCause, len(notification.Causes))
	for index, cause := range notification.Causes {
		protocolCauses[index] = catalogproto.ActivationSourceCause{
			PublicationID:      catalogproto.OperationID(hex.EncodeToString(cause.PublicationID[:])),
			ChangeID:           catalogproto.ChangeID(hex.EncodeToString(cause.ChangeID[:])),
			SourceRevision:     uint64(cause.SourceRevision),
			OperationID:        catalogproto.OperationID(hex.EncodeToString(cause.OperationID[:])),
			Cause:              catalogproto.ActivationCause(cause.Cause),
			AffectedKeysDigest: hex.EncodeToString(cause.AffectedKeysDigest[:]),
		}
	}
	command := catalogproto.BrokerCommand{
		Kind: catalogproto.BrokerCommandKindSignalDomain,
		Notification: &catalogproto.ActivationNotification{
			Protocol:           catalogproto.Version,
			ActivationChangeID: catalogproto.ActivationChangeID(hex.EncodeToString(notification.ActivationChangeID[:])),
			TenantID:           catalogproto.TenantID(notification.TenantID),
			DomainID:           catalogproto.DomainID(notification.PresentationID),
			Generation:         uint64(notification.TenantGeneration),
			ActivationRevision: uint64(notification.ActivationRevision),
			CatalogHead:        uint64(notification.CatalogHead), HeadDigest: hex.EncodeToString(notification.HeadDigest[:]),
			ProviderFingerprint: hex.EncodeToString(target.ProviderFingerprint[:]), Causes: protocolCauses,
			TargetCount: target.SignalTargetCount, TargetDigest: hex.EncodeToString(target.SignalTargetDigest[:]),
			TargetsCoalesced: target.SignalsCoalesced, Targets: protocolTargets,
		},
	}
	done := make(chan brokerOutcome, 1)
	if err := b.enqueue(ctx, session, command, done); err != nil {
		if errors.Is(err, errBrokerDeliveryUnknown) {
			return convergence.DeliveryUnknown, nil
		}
		return convergence.DeliveryNotSent, err
	}
	select {
	case outcome := <-done:
		if outcome.delivery == convergence.DeliveryUnknown {
			return convergence.DeliveryUnknown, nil
		}
		return outcome.delivery, outcome.err
	case <-ctx.Done():
		go b.poisonSession(session, true, ctx.Err())
		return convergence.DeliveryUnknown, nil
	case <-session.done:
		return convergence.DeliveryUnknown, nil
	}
}

// Close disconnects the broker and joins exact process retirement.
func (b *RuntimeBroker) Close(ctx context.Context) error {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.cancel()
		b.signalChangedLocked()
	}
	b.mu.Unlock()

	done := ctx.Done()
	var ctxErr error
	for {
		b.mu.Lock()
		session := b.active
		binding := b.binding
		recovering := b.recovering
		changed := b.changed
		b.mu.Unlock()
		if binding || recovering {
			select {
			case <-changed:
				continue
			case <-done:
				ctxErr = fmt.Errorf("catalog service: join broker startup: %w", ctx.Err())
				done = nil
				continue
			}
		}
		if session == nil {
			return ctxErr
		}
		go b.poisonSession(session, false, errors.New("catalog service: broker runtime closed"))
		select {
		case <-session.done:
			return errors.Join(ctxErr, session.retirementError())
		case <-done:
			ctxErr = fmt.Errorf("catalog service: join broker retirement: %w", ctx.Err())
			done = nil
		}
	}
}

func (b *RuntimeBroker) enqueue(
	ctx context.Context,
	session *runtimeBrokerSession,
	command catalogproto.BrokerCommand,
	done chan brokerOutcome,
) error {
	return b.enqueuePending(ctx, session, brokerPending{command: command, done: done})
}

func (b *RuntimeBroker) enqueueRemoval(
	ctx context.Context,
	session *runtimeBrokerSession,
	observedID catalogproto.ObservedDomainID,
	managedDomain *catalogproto.DomainID,
	removal *catalog.FileProviderDomainRemoval,
) error {
	return b.enqueuePending(ctx, session, brokerPending{
		command:       catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindRemoveDomain, ObservedID: &observedID},
		managedDomain: managedDomain,
		removal:       removal,
	})
}

func (b *RuntimeBroker) enqueuePending(
	ctx context.Context,
	session *runtimeBrokerSession,
	pending brokerPending,
) error {
	select {
	case <-session.transportDone:
		return errBrokerSessionLost
	default:
	}
	select {
	case <-session.slots:
	case <-ctx.Done():
		return ctx.Err()
	case <-session.transportDone:
		return errBrokerSessionLost
	case <-session.done:
		return errBrokerSessionLost
	}
	releaseSlot := true
	defer func() {
		if releaseSlot {
			session.slots <- struct{}{}
		}
	}()
	id, err := b.catalog.NextBrokerCommandID(ctx)
	if err != nil {
		return fmt.Errorf("catalog service: allocate broker command ID: %w", err)
	}
	pending.command.Protocol = catalogproto.Version
	pending.command.CommandID = id
	if err := catalogproto.Validate(pending.command); err != nil {
		return err
	}
	attemptID, err := catalog.NewBrokerCommandAttemptID()
	if err != nil {
		return fmt.Errorf("catalog service: allocate broker attempt identity: %w", err)
	}
	semantic := pending.command
	semantic.CommandID = 0
	payload, err := json.Marshal(semantic)
	if err != nil {
		return fmt.Errorf("catalog service: encode broker command attempt: %w", err)
	}
	pending.attempt = catalog.BrokerCommandAttempt{
		AttemptID: attemptID, CommandID: id, Process: session.process,
		Kind: string(pending.command.Kind), PayloadDigest: sha256.Sum256(payload),
	}
	if pending.command.Kind == catalogproto.BrokerCommandKindSignalDomain {
		if pending.command.Notification == nil {
			return errors.New("catalog service: signal command lacks notification")
		}
		pending.attempt.DomainID = string(pending.command.Notification.DomainID)
		pending.attempt.Revision = pending.command.Notification.ActivationRevision
	}
	planned, created, err := b.catalog.BeginBrokerCommandAttempt(ctx, pending.attempt)
	if err != nil {
		return fmt.Errorf("catalog service: begin broker command attempt: %w", err)
	}
	if !created {
		if pending.command.Kind != catalogproto.BrokerCommandKindSignalDomain {
			return errors.New("catalog service: non-signal broker attempt unexpectedly exists")
		}
		delivery := convergence.DeliveryUnknown
		if planned.State == catalog.BrokerCommandAccepted {
			delivery = convergence.DeliverySent
		}
		if pending.done != nil {
			pending.done <- brokerOutcome{delivery: delivery}
		}
		return nil
	}
	pending.attempt = planned
	pending.settled = make(chan struct{})
	b.mu.Lock()
	if b.closed || b.active != session {
		b.mu.Unlock()
		if err := b.catalog.AbandonBrokerCommandAttempt(context.WithoutCancel(ctx), pending.attempt); err != nil {
			return errors.Join(errBrokerSessionLost, err)
		}
		return errBrokerSessionLost
	}
	if pending.command.Kind == catalogproto.BrokerCommandKindListDomains {
		if pending.command.AfterObservedID != nil && b.reconciling != session {
			b.mu.Unlock()
			if err := b.catalog.AbandonBrokerCommandAttempt(context.WithoutCancel(ctx), pending.attempt); err != nil {
				return errors.Join(errors.New("catalog service: orphan broker domain page continuation"), err)
			}
			return errors.New("catalog service: orphan broker domain page continuation")
		}
		if pending.command.AfterObservedID == nil && b.reconciling == session {
			b.reconcileRequested = true
			b.mu.Unlock()
			if err := b.catalog.AbandonBrokerCommandAttempt(context.WithoutCancel(ctx), pending.attempt); err != nil {
				return err
			}
			return nil
		}
		for _, existing := range b.pending {
			if existing.command.Kind == catalogproto.BrokerCommandKindListDomains {
				b.reconcileRequested = true
				b.mu.Unlock()
				if err := b.catalog.AbandonBrokerCommandAttempt(context.WithoutCancel(ctx), pending.attempt); err != nil {
					return err
				}
				return nil
			}
		}
		if pending.command.AfterObservedID == nil {
			session.reconcileEpoch++
			pending.reconcileEpoch = session.reconcileEpoch
		}
	}
	if pending.command.Kind == catalogproto.BrokerCommandKindRemoveDomain && pending.command.ObservedID != nil {
		for existingID, existing := range b.pending {
			if existing.command.Kind != catalogproto.BrokerCommandKindRemoveDomain || existing.command.ObservedID == nil ||
				*pending.command.ObservedID != *existing.command.ObservedID {
				continue
			}
			if pending.removal != nil && existing.removal == nil {
				removal := *pending.removal
				existing.removal = &removal
				b.pending[existingID] = existing
			} else if pending.removal != nil && !sameDomainIdentity(existing.removal.Domain, pending.removal.Domain) {
				b.mu.Unlock()
				if err := b.catalog.AbandonBrokerCommandAttempt(context.WithoutCancel(ctx), pending.attempt); err != nil {
					return errors.Join(errors.New("catalog service: pending domain removal identity changed"), err)
				}
				return errors.New("catalog service: pending domain removal identity changed")
			}
			if pending.managedDomain != nil && existing.managedDomain == nil {
				managed := *pending.managedDomain
				existing.managedDomain = &managed
				b.pending[existingID] = existing
			} else if pending.managedDomain != nil && *existing.managedDomain != *pending.managedDomain {
				b.mu.Unlock()
				if err := b.catalog.AbandonBrokerCommandAttempt(context.WithoutCancel(ctx), pending.attempt); err != nil {
					return errors.Join(errors.New("catalog service: pending observed domain identity changed"), err)
				}
				return errors.New("catalog service: pending observed domain identity changed")
			}
			b.mu.Unlock()
			if err := b.catalog.AbandonBrokerCommandAttempt(context.WithoutCancel(ctx), pending.attempt); err != nil {
				return err
			}
			return nil
		}
	}
	b.pending[id] = pending
	if pending.command.Kind == catalogproto.BrokerCommandKindRegisterDomain ||
		pending.command.Kind == catalogproto.BrokerCommandKindRemoveDomain {
		session.reconcileEpoch++
	}
	b.mu.Unlock()
	sent, err := b.catalog.TransitionBrokerCommandAttempt(ctx, pending.attempt, catalog.BrokerCommandSent)
	if err != nil {
		var retryErr error
		sent, retryErr = b.catalog.TransitionBrokerCommandAttempt(
			context.WithoutCancel(b.ctx), pending.attempt, catalog.BrokerCommandSent,
		)
		if retryErr != nil {
			releaseSlot = false
			go b.poisonSession(session, true, errors.Join(err, retryErr))
			return errors.Join(errBrokerDeliveryUnknown, err, retryErr)
		}
	}
	b.mu.Lock()
	if current, ok := b.pending[id]; ok {
		current.attempt = sent
		b.pending[id] = current
	}
	b.mu.Unlock()
	select {
	case session.commands <- pending.command:
		releaseSlot = false
		go b.watchBrokerCommand(session, id)
		return nil
	case <-ctx.Done():
		releaseSlot = false
		go b.poisonSession(session, true, ctx.Err())
		return errors.Join(errBrokerDeliveryUnknown, ctx.Err())
	case <-session.transportDone:
		releaseSlot = false
		return errBrokerDeliveryUnknown
	case <-session.done:
		releaseSlot = false
		return errBrokerDeliveryUnknown
	}
}

func (b *RuntimeBroker) accept(ctx context.Context, session *runtimeBrokerSession, result catalogproto.BrokerResult) error {
	b.mu.Lock()
	if b.active != session {
		b.mu.Unlock()
		return errBrokerSessionLost
	}
	pending, ok := b.pending[result.CommandID]
	if !ok || pending.command.Kind != result.Kind {
		b.mu.Unlock()
		return errors.New("catalog service: unmatched runtime broker result")
	}
	if result.Kind == catalogproto.BrokerCommandKindListDomains {
		if pending.command.AfterObservedID == nil {
			b.reconciling = session
			session.reconcile = nil
		} else if b.reconciling != session {
			b.mu.Unlock()
			return errors.New("catalog service: broker domain page continuation lost its reconciliation")
		}
		if pending.reconcileEpoch == 0 {
			b.mu.Unlock()
			return errors.New("catalog service: broker reconciliation has no epoch")
		}
	}
	b.mu.Unlock()

	followup := false
	var continuation *catalogproto.ObservedDomainID
	reconcileComplete := false
	var reconcileEpoch uint64
	switch result.Kind {
	case catalogproto.BrokerCommandKindListDomains:
		if result.Code != catalogproto.ErrorCodeOk || result.Domains == nil {
			return fmt.Errorf("catalog service: broker list domains failed: %s", result.Message)
		}
		if result.NextAfterObservedID != nil && (len(*result.Domains) == 0 ||
			(*result.Domains)[len(*result.Domains)-1].ObservedID != *result.NextAfterObservedID) {
			return errors.New("catalog service: broker domain page continuation is not its last domain")
		}
		state := session.reconcile
		if state == nil {
			if pending.command.AfterObservedID != nil {
				return errors.New("catalog service: broker domain page continuation has no snapshot")
			}
			var err error
			state, err = b.newBrokerReconcileState(ctx, pending.reconcileEpoch)
			if err != nil {
				return err
			}
			session.reconcile = state
		} else if pending.reconcileEpoch != state.epoch {
			return errors.New("catalog service: broker domain page changed reconciliation epoch")
		} else if after := pending.command.AfterObservedID; after == nil || state.lastObservedID == nil || *after != *state.lastObservedID ||
			(len(*result.Domains) > 0 && (*result.Domains)[0].ObservedID <= *after) {
			return errors.New("catalog service: broker domain page did not advance")
		}
		restart, err := b.reconcileBrokerDomainPage(ctx, session, state, *result.Domains)
		if err != nil {
			return err
		}
		if !restart && result.NextAfterObservedID != nil {
			value := *result.NextAfterObservedID
			state.lastObservedID = &value
			continuation = &value
		} else {
			if !restart {
				restart, err = b.finishBrokerReconcile(ctx, session, state)
				if err != nil {
					return err
				}
			}
			session.reconcile = nil
			reconcileComplete = !restart
			reconcileEpoch = state.epoch
		}
	case catalogproto.BrokerCommandKindRegisterDomain:
		if result.Code != catalogproto.ErrorCodeOk || result.Registered == nil {
			return fmt.Errorf("catalog service: broker register domain failed: %s", result.Message)
		}
		domain, err := catalogDomain(*result.Registered)
		if err != nil {
			return err
		}
		domain.ActivationGeneration = b.activationGeneration
		if err := b.catalog.ConfirmFileProviderDomain(ctx, domain); err != nil {
			return err
		}
		followup = true
	case catalogproto.BrokerCommandKindRemoveDomain:
		if result.Code != catalogproto.ErrorCodeOk || result.ConfirmedAbsent == nil || !*result.ConfirmedAbsent || pending.command.ObservedID == nil {
			return fmt.Errorf("catalog service: broker remove domain failed: %s", result.Message)
		}
		if pending.managedDomain != nil {
			if err := b.catalog.ConfirmFileProviderDomainAbsent(ctx, causal.DomainID(*pending.managedDomain)); err != nil {
				return err
			}
		}
		followup = true
	case catalogproto.BrokerCommandKindSignalDomain:
		if result.Code != catalogproto.ErrorCodeOk || result.SignalAccepted == nil || !*result.SignalAccepted {
			return fmt.Errorf("catalog service: broker signal outcome is ambiguous: %s", result.Message)
		}
	case catalogproto.BrokerCommandKindMaterializeCritical:
		if result.Code != catalogproto.ErrorCodeOk || result.MaterializationScheduled == nil ||
			!*result.MaterializationScheduled || result.MaterializationPaths == nil {
			return fmt.Errorf("catalog service: broker materialization outcome is ambiguous: %s", result.Message)
		}
		pending.paths = append([]catalogproto.CriticalMaterializationPath(nil), (*result.MaterializationPaths)...)
	default:
		return errors.New("catalog service: unknown runtime broker result")
	}
	if err := b.completeBrokerCommand(ctx, session, result.CommandID, pending); err != nil {
		return err
	}
	if continuation != nil {
		return b.enqueuePending(ctx, session, brokerPending{
			command: catalogproto.BrokerCommand{
				Kind: catalogproto.BrokerCommandKindListDomains, AfterObservedID: continuation,
			},
			reconcileEpoch: pending.reconcileEpoch,
		})
	}
	if result.Kind == catalogproto.BrokerCommandKindListDomains {
		b.mu.Lock()
		ready := b.ready
		b.mu.Unlock()
		settled := b.finishReconcile(session, reconcileComplete, reconcileEpoch)
		if settled && ready != nil {
			go ready()
		}
	}
	if followup {
		return b.enqueueReconcile(ctx, session)
	}
	return nil
}

func (b *RuntimeBroker) newBrokerReconcileState(
	ctx context.Context,
	epoch uint64,
) (*brokerReconcileState, error) {
	state := &brokerReconcileState{
		epoch:                  epoch,
		actualIDs:              make(map[catalogproto.ObservedDomainID]struct{}),
		managedIDs:             make(map[catalogproto.DomainID]struct{}),
		removalsByTenant:       make(map[catalog.TenantID]catalog.FileProviderDomainRemoval),
		removalsByDomain:       make(map[catalogproto.DomainID]catalog.FileProviderDomainRemoval),
		removalsByPresentation: make(map[[2]string]catalog.FileProviderDomainRemoval),
		blockedRemovals:        make(map[catalog.TenantID]bool),
	}
	for after := catalog.TenantID(""); ; {
		page, err := b.catalog.PageFileProviderDomainRemovals(ctx, after, catalog.FileProviderDomainPageLimit)
		if err != nil {
			return nil, err
		}
		for _, removal := range page.Removals {
			state.removalsByTenant[removal.Domain.Tenant] = removal
			state.removalsByDomain[catalogproto.DomainID(removal.Domain.DomainID)] = removal
			state.removalsByPresentation[[2]string{removal.Domain.OwnerID, removal.Domain.PresentationInstance}] = removal
		}
		if page.Next == "" {
			return state, nil
		}
		if page.Next <= after {
			return nil, fmt.Errorf("%w: File Provider domain removal cursor did not advance", catalog.ErrIntegrity)
		}
		after = page.Next
	}
}

func (b *RuntimeBroker) reconcileBrokerDomainPage(
	ctx context.Context,
	session *runtimeBrokerSession,
	state *brokerReconcileState,
	actual []catalogproto.ObservedDomain,
) (bool, error) {
	actions := 0
	for _, observed := range actual {
		if _, exists := state.actualIDs[observed.ObservedID]; exists {
			return false, fmt.Errorf("%w: duplicate File Provider domain across pages", catalog.ErrIntegrity)
		}
		state.actualIDs[observed.ObservedID] = struct{}{}
		if observed.Managed == nil {
			if err := b.enqueueRemoval(ctx, session, observed.ObservedID, nil, nil); err != nil {
				return false, err
			}
			actions++
			if actions >= int(catalogproto.MaxBrokerDomainPageSize) {
				return true, nil
			}
			continue
		}
		domain := *observed.Managed
		state.managedIDs[domain.DomainID] = struct{}{}
		removal, removing := state.removalsByDomain[domain.DomainID]
		if !removing {
			removal, removing = state.removalsByTenant[catalog.TenantID(domain.TenantID)]
		}
		if !removing {
			removal, removing = state.removalsByPresentation[[2]string{string(domain.OwnerID), string(domain.PresentationInstanceID)}]
		}
		if removing {
			state.blockedRemovals[removal.Domain.Tenant] = true
			converted, convertErr := catalogDomain(domain)
			if convertErr == nil && sameDomainIdentity(removal.Domain, converted) {
				managed := domain.DomainID
				if err := b.enqueueRemoval(ctx, session, observed.ObservedID, &managed, &removal); err != nil {
					return false, err
				}
			} else {
				managed := domain.DomainID
				if err := b.enqueueRemoval(ctx, session, observed.ObservedID, &managed, nil); err != nil {
					return false, err
				}
			}
			actions++
		} else {
			desired, found, err := b.catalog.FileProviderDomainForTenant(ctx, catalog.TenantID(domain.TenantID))
			if err != nil {
				return false, err
			}
			converted, convertErr := catalogDomain(domain)
			converted.ActivationGeneration = b.activationGeneration
			if !found || convertErr != nil || catalogproto.DomainID(desired.DomainID) != domain.DomainID ||
				!sameDomainIdentity(desired, converted) {
				managed := domain.DomainID
				if err := b.enqueueRemoval(ctx, session, observed.ObservedID, &managed, nil); err != nil {
					return false, err
				}
				actions++
			} else if err := b.catalog.ConfirmFileProviderDomain(ctx, converted); err != nil {
				return false, err
			}
		}
		if actions >= int(catalogproto.MaxBrokerDomainPageSize) {
			return true, nil
		}
	}
	return actions > 0, nil
}

func (b *RuntimeBroker) finishBrokerReconcile(
	ctx context.Context,
	session *runtimeBrokerSession,
	state *brokerReconcileState,
) (bool, error) {
	for _, removal := range state.removalsByTenant {
		if state.blockedRemovals[removal.Domain.Tenant] {
			continue
		}
		if err := b.catalog.ConfirmFileProviderDomainRemoval(ctx, removal); err != nil {
			return false, err
		}
		b.signalChanged()
	}
	actions := 0
	for after := catalog.TenantID(""); ; {
		page, err := b.catalog.PageFileProviderDomains(ctx, after, catalog.FileProviderDomainPageLimit)
		if err != nil {
			return false, err
		}
		for _, domain := range page.Domains {
			id := catalogproto.DomainID(domain.DomainID)
			if _, removing := state.removalsByDomain[id]; removing {
				continue
			}
			if _, present := state.managedIDs[id]; present {
				continue
			}
			registration, err := protocolDomainRegistration(domain)
			if err != nil {
				return false, err
			}
			if err := b.enqueue(ctx, session, catalogproto.BrokerCommand{
				Kind: catalogproto.BrokerCommandKindRegisterDomain, Registration: &registration,
			}, nil); err != nil {
				return false, err
			}
			actions++
			if actions >= int(catalogproto.MaxBrokerDomainPageSize) {
				return true, nil
			}
		}
		if page.Next == "" {
			return actions > 0, nil
		}
		if page.Next <= after {
			return false, fmt.Errorf("%w: File Provider domain cursor did not advance", catalog.ErrIntegrity)
		}
		after = page.Next
	}
}

func (b *RuntimeBroker) requestReconcile() {
	b.mu.Lock()
	session := b.active
	b.mu.Unlock()
	if session != nil {
		if err := b.enqueueReconcile(session.ctx, session); err != nil &&
			!errors.Is(err, errBrokerSessionLost) {
			go b.poisonSession(session, true, err)
		}
	}
}

func (b *RuntimeBroker) enqueueReconcile(
	ctx context.Context,
	session *runtimeBrokerSession,
) error {
	b.mu.Lock()
	if b.closed || b.active != session {
		b.mu.Unlock()
		return errBrokerSessionLost
	}
	b.mu.Unlock()
	return b.enqueuePending(ctx, session, brokerPending{
		command: catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindListDomains},
	})
}

func (b *RuntimeBroker) finishReconcile(
	session *runtimeBrokerSession,
	complete bool,
	epoch uint64,
) bool {
	b.mu.Lock()
	if b.reconciling != session {
		b.mu.Unlock()
		return false
	}
	pendingMutation := false
	for _, pending := range b.pending {
		if pending.command.Kind == catalogproto.BrokerCommandKindRegisterDomain ||
			pending.command.Kind == catalogproto.BrokerCommandKindRemoveDomain {
			pendingMutation = true
			break
		}
	}
	settled := complete && !pendingMutation && !b.reconcileRequested && epoch != 0 &&
		session.reconcileEpoch == epoch && b.active == session && !b.closed
	if settled {
		session.settledEpoch = epoch
		b.signalChangedLocked()
	}
	retry := !pendingMutation && !settled && b.active == session && !b.closed &&
		(b.reconcileRequested || complete)
	b.reconciling = nil
	b.reconcileRequested = false
	session.reconcile = nil
	b.mu.Unlock()
	if retry {
		_ = b.enqueueReconcile(session.ctx, session)
	}
	return settled
}

func (b *RuntimeBroker) signalChanged() {
	b.mu.Lock()
	b.signalChangedLocked()
	b.mu.Unlock()
}

func (b *RuntimeBroker) signalChangedLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

func (b *RuntimeBroker) completeBrokerCommand(
	ctx context.Context,
	session *runtimeBrokerSession,
	id uint64,
	pending brokerPending,
) error {
	accepted, err := b.catalog.TransitionBrokerCommandAttempt(ctx, pending.attempt, catalog.BrokerCommandAccepted)
	if err != nil {
		return err
	}
	b.mu.Lock()
	current, ok := b.pending[id]
	if b.active != session || !ok || current.attempt.AttemptID != pending.attempt.AttemptID {
		b.mu.Unlock()
		return errBrokerSessionLost
	}
	delete(b.pending, id)
	close(current.settled)
	b.mu.Unlock()
	session.slots <- struct{}{}
	if current.done != nil {
		current.done <- brokerOutcome{
			delivery: convergence.DeliverySent,
			paths:    append([]catalogproto.CriticalMaterializationPath(nil), pending.paths...),
		}
	}
	_ = accepted
	return nil
}

func (b *RuntimeBroker) watchBrokerCommand(session *runtimeBrokerSession, id uint64) {
	b.mu.Lock()
	pending, ok := b.pending[id]
	timeout := b.commandTimeout
	b.mu.Unlock()
	if !ok {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-pending.settled:
	case <-session.done:
	case <-b.ctx.Done():
	case <-timer.C:
		go b.poisonSession(session, true, fmt.Errorf("catalog service: broker command %d deadline exceeded", id))
	}
}

func (b *RuntimeBroker) poisonSession(session *runtimeBrokerSession, relaunch bool, cause error) {
	session.poisonOnce.Do(func() {
		b.mu.Lock()
		if b.active != session {
			b.mu.Unlock()
			return
		}
		b.mu.Unlock()

		session.cancel()
		close(session.transportDone)

		session.setRetirementError(b.retireBrokerProcess(session.process))
		delay := 10 * time.Millisecond
		for {
			recoverErr := b.catalog.RecoverReapedBrokerCommandAttempts(
				context.WithoutCancel(b.ctx), session.process,
			)
			session.setRetirementError(recoverErr)
			if recoverErr == nil {
				break
			}
			time.Sleep(delay)
			if delay < time.Second {
				delay *= 2
				if delay > time.Second {
					delay = time.Second
				}
			}
		}

		b.mu.Lock()
		if b.active == session {
			b.active = nil
		}
		if b.reconciling == session {
			b.reconciling = nil
			b.reconcileRequested = false
			session.reconcile = nil
		}
		for id, value := range b.pending {
			if value.attempt.Process != session.process {
				continue
			}
			delete(b.pending, id)
			close(value.settled)
			session.slots <- struct{}{}
			if value.done != nil {
				value.done <- brokerOutcome{
					delivery: convergence.DeliveryUnknown,
					err:      errors.Join(errBrokerSessionLost, cause),
				}
			}
		}
		close(session.done)
		b.signalChangedLocked()
		closed := b.closed
		b.mu.Unlock()

		if !relaunch || closed {
			return
		}
		go b.relaunchBroker(cause)
	})
}

func (b *RuntimeBroker) retireBrokerProcess(process catalog.BrokerProcessIdentity) error {
	delay := 10 * time.Millisecond
	for {
		retireErr := b.owner.RetireBroker(context.WithoutCancel(b.ctx), process)
		if retireErr == nil {
			return nil
		}
		time.Sleep(delay)
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

func (b *RuntimeBroker) relaunchBroker(_ error) {
	delay := 10 * time.Millisecond
	for {
		if err := b.ctx.Err(); err != nil {
			return
		}
		if err := b.owner.StartBroker(b.ctx); err == nil {
			return
		}
		timer := time.NewTimer(delay)
		select {
		case <-b.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if delay < time.Second {
			delay *= 2
			if delay > time.Second {
				delay = time.Second
			}
		}
	}
}

type runtimeBrokerSession struct {
	hub            *RuntimeBroker
	ctx            context.Context
	cancel         context.CancelFunc
	commands       chan catalogproto.BrokerCommand
	done           chan struct{}
	transportDone  chan struct{}
	ready          chan struct{}
	slots          chan struct{}
	poisonOnce     sync.Once
	retirementMu   sync.Mutex
	retirementErr  error
	identity       Identity
	process        catalog.BrokerProcessIdentity
	reconcile      *brokerReconcileState
	reconcileEpoch uint64
	settledEpoch   uint64
}

type brokerReconcileState struct {
	epoch                  uint64
	actualIDs              map[catalogproto.ObservedDomainID]struct{}
	managedIDs             map[catalogproto.DomainID]struct{}
	removalsByTenant       map[catalog.TenantID]catalog.FileProviderDomainRemoval
	removalsByDomain       map[catalogproto.DomainID]catalog.FileProviderDomainRemoval
	removalsByPresentation map[[2]string]catalog.FileProviderDomainRemoval
	blockedRemovals        map[catalog.TenantID]bool
	lastObservedID         *catalogproto.ObservedDomainID
}

func (s *runtimeBrokerSession) Commands() <-chan catalogproto.BrokerCommand { return s.commands }

func (s *runtimeBrokerSession) Done() <-chan struct{} { return s.transportDone }

func (s *runtimeBrokerSession) AcceptResult(ctx context.Context, result catalogproto.BrokerResult) error {
	return s.hub.accept(ctx, s, result)
}

func (s *runtimeBrokerSession) Close(err error) {
	if err == nil {
		err = errBrokerSessionLost
	}
	go s.hub.poisonSession(s, true, err)
}

func (s *runtimeBrokerSession) setRetirementError(err error) {
	s.retirementMu.Lock()
	s.retirementErr = err
	s.retirementMu.Unlock()
}

func (s *runtimeBrokerSession) retirementError() error {
	s.retirementMu.Lock()
	defer s.retirementMu.Unlock()
	return s.retirementErr
}

func protocolDomainRegistration(domain catalog.FileProviderDomain) (catalogproto.DomainRegistration, error) {
	access, err := protocolTenantAccess(domain.Access)
	if err != nil {
		return catalogproto.DomainRegistration{}, err
	}
	return catalogproto.DomainRegistration{
		DomainID: catalogproto.DomainID(domain.DomainID), OwnerID: catalogproto.OwnerID(domain.OwnerID),
		TenantID: catalogproto.TenantID(domain.Tenant), Generation: uint64(domain.Generation),
		RootID: catalogproto.ObjectID(domain.Root.String()), AccessMode: access,
		PresentationInstanceID: catalogproto.PresentationInstanceID(domain.PresentationInstance),
		DisplayName:            domain.DisplayName,
	}, nil
}

func catalogDomain(domain catalogproto.RegisteredDomain) (catalog.FileProviderDomain, error) {
	root, err := catalog.ParseObjectID(string(domain.RootID))
	if err != nil {
		return catalog.FileProviderDomain{}, err
	}
	access, err := catalogTenantAccess(domain.AccessMode)
	if err != nil {
		return catalog.FileProviderDomain{}, err
	}
	return catalog.FileProviderDomain{
		DomainID: causal.DomainID(domain.DomainID), OwnerID: string(domain.OwnerID), Tenant: catalog.TenantID(domain.TenantID),
		Generation: catalog.Generation(domain.Generation), Root: root, Access: access,
		PresentationInstance: string(domain.PresentationInstanceID),
		DisplayName:          domain.DisplayName, PublicPath: domain.PublicPath, Registered: true,
	}, nil
}

func protocolTenantAccess(access catalog.TenantAccessMode) (catalogproto.TenantAccessMode, error) {
	switch access {
	case catalog.TenantReadOnly:
		return catalogproto.TenantAccessModeReadOnly, nil
	case catalog.TenantReadWrite:
		return catalogproto.TenantAccessModeReadWrite, nil
	default:
		return "", fmt.Errorf("catalog service: unknown tenant access mode %d", access)
	}
}

func catalogTenantAccess(access catalogproto.TenantAccessMode) (catalog.TenantAccessMode, error) {
	switch access {
	case catalogproto.TenantAccessModeReadOnly:
		return catalog.TenantReadOnly, nil
	case catalogproto.TenantAccessModeReadWrite:
		return catalog.TenantReadWrite, nil
	default:
		return 0, fmt.Errorf("catalog service: unknown tenant access mode %q", access)
	}
}

func sameDomainIdentity(left, right catalog.FileProviderDomain) bool {
	return left.DomainID == right.DomainID && left.OwnerID == right.OwnerID && left.Tenant == right.Tenant &&
		left.Generation == right.Generation && left.Root == right.Root && left.Access == right.Access &&
		left.PresentationInstance == right.PresentationInstance &&
		left.DisplayName == right.DisplayName
}

var _ BrokerService = (*RuntimeBroker)(nil)
var _ convergence.Notifier = (*RuntimeBroker)(nil)
