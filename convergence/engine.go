package convergence

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/fusekit/causal"
)

type realClock struct{}

func (realClock) Now() time.Time                             { return time.Now() }
func (realClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

type commandKind uint8

const (
	commandApply commandKind = iota + 1
	commandAck
	commandPrepare
	commandSnapshot
	commandTick
	commandAwait
	commandCancelWaiter
	commandDrain
	commandClose
)

type command struct {
	kind        commandKind
	ctx         context.Context
	change      ChangeSet
	ack         Ack
	requirement PreparationRequirement
	prep        Preparation
	waiter      uint64
	reply       chan result
}

type result struct {
	state       State
	preparation Preparation
	proof       ObservationProof
	wait        <-chan result
	err         error
}

type waiter struct {
	preparation Preparation
	reply       chan result
}

// Engine serializes fleet convergence through one durable actor.
type Engine struct {
	resolver Resolver
	notifier Notifier
	clock    Clock
	store    Persistence

	ctx      context.Context
	cancel   context.CancelFunc
	commands chan command
	done     chan struct{}

	mu          sync.Mutex
	terminalErr error
	cancelOnce  sync.Once
	waiterID    atomic.Uint64
}

// New loads durable state before accepting any work.
func New(ctx context.Context, config Config) (*Engine, error) {
	if config.Resolver == nil || config.Notifier == nil || config.Persistence == nil {
		return nil, errors.New("convergence: resolver, notifier, and persistence are required")
	}
	if config.Clock == nil {
		config.Clock = realClock{}
	}
	state, err := config.Persistence.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("convergence: load state: %w", err)
	}
	state = cloneState(state)
	if err := validateState(state); err != nil {
		return nil, err
	}
	engineCtx, cancel := context.WithCancel(context.Background())
	engine := &Engine{
		resolver: config.Resolver,
		notifier: config.Notifier,
		clock:    config.Clock,
		store:    config.Persistence,
		ctx:      engineCtx,
		cancel:   cancel,
		commands: make(chan command),
		done:     make(chan struct{}),
	}
	if err := engine.drainOutbox(ctx, &state); err != nil {
		cancel()
		return nil, fmt.Errorf("convergence: drain catalog outbox: %w", err)
	}
	if err := engine.pump(ctx, &state); err != nil {
		cancel()
		return nil, fmt.Errorf("convergence: resume durable state: %w", err)
	}
	go engine.run(state)
	return engine, nil
}

func (e *Engine) publishForTest(ctx context.Context, change ChangeSet) error {
	result := e.call(ctx, command{kind: commandApply, ctx: ctx, change: change})
	return result.err
}

// Acknowledge records external observation and immediately releases one fleet slot.
func (e *Engine) Acknowledge(ctx context.Context, ack Ack) error {
	result := e.call(ctx, command{kind: commandAck, ctx: ctx, ack: ack})
	return result.err
}

// RequestTenant targets one selected tenant from exact causal identity and derives its engine revision.
func (e *Engine) RequestTenant(ctx context.Context, requirement PreparationRequirement) (Preparation, error) {
	result := e.call(ctx, command{kind: commandPrepare, ctx: ctx, requirement: requirement})
	return result.preparation, result.err
}

// AwaitObserved waits without polling until an exact or newer requested revision is observed.
func (e *Engine) AwaitObserved(ctx context.Context, preparation Preparation) (ObservationProof, error) {
	id := e.waiterID.Add(1)
	registered := e.call(ctx, command{kind: commandAwait, ctx: ctx, prep: preparation, waiter: id})
	if registered.err != nil || registered.wait == nil {
		return registered.proof, registered.err
	}
	stop := context.AfterFunc(ctx, func() { e.cancelWaiter(id) })
	defer stop()
	select {
	case settled := <-registered.wait:
		return settled.proof, settled.err
	case <-ctx.Done():
		select {
		case settled := <-registered.wait:
			return settled.proof, settled.err
		default:
			return ObservationProof{}, ctx.Err()
		}
	case <-e.done:
		return ObservationProof{}, e.closedError()
	}
}

// PrepareTenant targets one selected tenant and returns only with observed-revision proof.
func (e *Engine) PrepareTenant(ctx context.Context, requirement PreparationRequirement) (ObservationProof, error) {
	preparation, err := e.RequestTenant(ctx, requirement)
	if err != nil {
		return ObservationProof{}, err
	}
	return e.AwaitObserved(ctx, preparation)
}

// Snapshot returns an isolated copy of durable convergence state.
func (e *Engine) Snapshot(ctx context.Context) (State, error) {
	result := e.call(ctx, command{kind: commandSnapshot, ctx: ctx})
	return result.state, result.err
}

// Tick deterministically processes acknowledgement deadlines at Clock.Now.
func (e *Engine) Tick(ctx context.Context) error {
	result := e.call(ctx, command{kind: commandTick, ctx: ctx})
	return result.err
}

// Drain applies and settles every catalog commit currently reserved in the durable outbox.
func (e *Engine) Drain(ctx context.Context) error {
	result := e.call(ctx, command{kind: commandDrain, ctx: ctx})
	return result.err
}

// Close durably stops the actor without changing outstanding acknowledgement state.
func (e *Engine) Close(ctx context.Context) error {
	result := e.call(ctx, command{kind: commandClose, ctx: ctx})
	return result.err
}

// Cancel aborts in-flight I/O. Any reserved notification remains awaiting acknowledgement.
func (e *Engine) Cancel() {
	e.cancelOnce.Do(e.cancel)
}

// Wait joins the engine actor.
func (e *Engine) Wait(ctx context.Context) error {
	select {
	case <-e.done:
		return e.endError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Engine) call(ctx context.Context, request command) result {
	if err := ctx.Err(); err != nil {
		return result{err: err}
	}
	request.reply = make(chan result, 1)
	select {
	case e.commands <- request:
	case <-ctx.Done():
		select {
		case response := <-request.reply:
			return response
		default:
			return result{err: ctx.Err()}
		}
	case <-e.done:
		select {
		case response := <-request.reply:
			return response
		default:
			return result{err: e.closedError()}
		}
	}
	select {
	case response := <-request.reply:
		return response
	case <-ctx.Done():
		select {
		case response := <-request.reply:
			return response
		default:
			return result{err: ctx.Err()}
		}
	case <-e.done:
		select {
		case response := <-request.reply:
			return response
		default:
			return result{err: e.closedError()}
		}
	}
}

func (e *Engine) cancelWaiter(id uint64) {
	select {
	case e.commands <- command{kind: commandCancelWaiter, ctx: context.Background(), waiter: id, reply: make(chan result, 1)}:
	case <-e.done:
	}
}

func (e *Engine) run(state State) {
	waiters := make(map[uint64]waiter)
	defer func() {
		for _, waiter := range waiters {
			waiter.reply <- result{err: ErrClosed}
		}
		close(e.done)
	}()
	for {
		timer := e.timer(state)
		select {
		case <-e.ctx.Done():
			e.setEndError(e.ctx.Err())
			return
		case <-timer:
			if err := e.onTimer(e.ctx, &state); err != nil {
				e.setEndError(err)
				return
			}
			settleWaiters(state, waiters)
		case request := <-e.commands:
			response, stop := e.handle(&state, waiters, request)
			request.reply <- response
			settleWaiters(state, waiters)
			if stop {
				return
			}
		}
	}
}

func (e *Engine) handle(state *State, waiters map[uint64]waiter, request command) (result, bool) {
	if err := request.ctx.Err(); err != nil {
		return result{err: err}, false
	}
	operationCtx, cancel := context.WithCancel(request.ctx)
	stop := context.AfterFunc(e.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()
	switch request.kind {
	case commandApply:
		return result{err: e.apply(operationCtx, state, request.change)}, false
	case commandAck:
		return result{err: e.acknowledge(operationCtx, state, request.ack)}, false
	case commandPrepare:
		preparation, err := e.prepare(operationCtx, state, request.requirement)
		return result{preparation: preparation, err: err}, false
	case commandSnapshot:
		return result{state: cloneState(*state)}, false
	case commandTick:
		return result{err: e.onTimer(operationCtx, state)}, false
	case commandAwait:
		return registerWaiter(*state, waiters, request), false
	case commandCancelWaiter:
		delete(waiters, request.waiter)
		return result{}, false
	case commandDrain:
		return result{err: e.drainOutbox(operationCtx, state)}, false
	case commandClose:
		return result{}, true
	default:
		return result{err: errors.New("convergence: unknown command")}, false
	}
}

func (e *Engine) apply(ctx context.Context, state *State, change ChangeSet) error {
	if change.Cause == CauseOnDemand {
		return fmt.Errorf("%w: on-demand changes are internal", ErrInvalidChange)
	}
	return e.applyChange(ctx, state, change, nil)
}

func (e *Engine) applyChange(ctx context.Context, state *State, change ChangeSet, outbox *causal.OutboxBatch) error {
	if err := validateChange(change, true); err != nil {
		return err
	}
	if applied, duplicate := state.Changes[change.ChangeID]; duplicate {
		if !equalChange(applied.Change, change) {
			return fmt.Errorf("%w: change id reused with different causal metadata", ErrInvalidChange)
		}
		return nil
	}
	if change.SourceRevision <= state.DedupFloors[change.SourceAuthority] {
		return nil
	}
	head := state.SourceHeads[change.SourceAuthority]
	if change.SourceRevision <= head {
		return fmt.Errorf("%w: source authority %q revision %d does not advance head %d", ErrInvalidChange, change.SourceAuthority, change.SourceRevision, head)
	}
	for _, applied := range state.Changes {
		if applied.Change.SourceAuthority == change.SourceAuthority && applied.Change.SourceRevision == change.SourceRevision {
			return fmt.Errorf("%w: source authority %q revision %d already has another change id", ErrInvalidChange, change.SourceAuthority, change.SourceRevision)
		}
	}
	resolutions, err := e.resolver.ResolveAffected(ctx, cloneChange(change))
	if err != nil {
		return fmt.Errorf("convergence: resolve change: %w", err)
	}
	if change.Cause == CauseOnDemand {
		resolutions, err = exactDomainCandidates(resolutions, change.Origin, change.OriginGeneration)
		if err != nil {
			return err
		}
	}
	resolved, err := resolveFleet(resolutions, change.SourceAuthority, change.SourceRevision)
	if err != nil {
		return err
	}
	if outbox != nil {
		matched := make(map[causal.TenantID]causal.CatalogRevision, len(resolved))
		for _, tenant := range resolved {
			matched[tenant.Tenant] = tenant.CatalogRevision
		}
		for _, commit := range outbox.Commits {
			if matched[commit.Tenant] != commit.CatalogRevision {
				return fmt.Errorf("%w: outbox tenant %q catalog revision %d was not resolved exactly", ErrInvalidResolution, commit.Tenant, commit.CatalogRevision)
			}
		}
	}
	before := cloneState(*state)
	changed := make([]resolvedTenant, 0, len(resolved))
	for _, tenant := range resolved {
		current, exists := state.Domains[tenant.Domain]
		if !exists || current.Generation != tenant.Generation || current.Fingerprint != tenant.Fingerprint {
			changed = append(changed, tenant)
		}
	}
	if len(changed) > 0 {
		state.Revision++
	}
	for _, tenant := range resolved {
		domain := state.Domains[tenant.Domain]
		generationChanged := domain.Generation != tenant.Generation
		if generationChanged {
			domain = DomainState{}
		}
		domain.Tenant = tenant.Tenant
		domain.Domain = tenant.Domain
		domain.Generation = tenant.Generation
		domain.Demanded = tenant.Demanded
		if change.Cause == CauseOnDemand {
			domain.Forced = true
		}
		domain.ResolvedSourceRevision = tenant.SourceRevision
		domain.CatalogRevision = tenant.CatalogRevision
		if generationChanged || domain.Fingerprint != tenant.Fingerprint {
			domain.Fingerprint = tenant.Fingerprint
			domain.Desired = state.Revision
			domain.DesiredChange = cloneChange(change)
			domain.Quarantine = nil
			if change.Cause == CauseProviderMutation && change.Origin == tenant.Domain && change.OriginGeneration == tenant.Generation {
				domain.Observed = state.Revision
				domain.ObservedCatalogRevision = tenant.CatalogRevision
				domain.ObservedChange = cloneChange(change)
				domain.Forced = false
			}
		}
		state.Domains[tenant.Domain] = domain
	}
	catalogs := make(map[TenantID]CatalogRevision, len(resolved))
	for _, tenant := range resolved {
		catalogs[tenant.Tenant] = tenant.CatalogRevision
	}
	state.Changes[change.ChangeID] = AppliedChange{Change: cloneChange(change), EngineRevision: state.Revision, CatalogRevisions: catalogs}
	if state.SourceHeads == nil {
		state.SourceHeads = make(map[SourceAuthorityID]Revision)
	}
	state.SourceHeads[change.SourceAuthority] = change.SourceRevision
	compactChanges(state)
	if err := e.save(ctx, *state); err != nil {
		*state = before
		return err
	}
	return e.pump(ctx, state)
}

func (e *Engine) drainOutbox(ctx context.Context, state *State) error {
	for {
		batch, err := e.store.ClaimOutbox(ctx)
		if err != nil {
			return fmt.Errorf("convergence: claim catalog outbox: %w", err)
		}
		if batch == nil {
			return nil
		}
		if len(batch.Commits) == 0 {
			return fmt.Errorf("%w: catalog outbox batch has no commits", ErrInvalidChange)
		}
		if err := e.applyChange(ctx, state, cloneChange(batch.Change), batch); err != nil {
			return err
		}
		if err := e.store.SettleOutbox(ctx, batch.Change.ChangeID); err != nil {
			return fmt.Errorf("convergence: settle catalog outbox: %w", err)
		}
	}
}

func (e *Engine) prepare(ctx context.Context, state *State, requirement PreparationRequirement) (Preparation, error) {
	if requirement.Tenant == "" || requirement.Domain == "" || requirement.Generation == 0 || requirement.SourceAuthority == "" ||
		requirement.SourceRevision == 0 || requirement.CatalogRevision == 0 || requirement.ChangeID == (ChangeID{}) || requirement.OperationID == (OperationID{}) {
		return Preparation{}, fmt.Errorf("%w: causal preparation requirement is incomplete", ErrInvalidResolution)
	}
	applied, ok := state.Changes[requirement.ChangeID]
	if !ok || applied.EngineRevision == 0 || applied.Change.SourceAuthority != requirement.SourceAuthority ||
		applied.Change.SourceRevision != requirement.SourceRevision || applied.Change.OperationID != requirement.OperationID ||
		applied.CatalogRevisions[requirement.Tenant] != requirement.CatalogRevision {
		return Preparation{}, fmt.Errorf("%w: causal catalog requirement is not published exactly", ErrInvalidResolution)
	}
	resolution, err := e.resolver.ResolveTenant(ctx, requirement.Tenant)
	if err != nil {
		return Preparation{}, fmt.Errorf("convergence: resolve tenant: %w", err)
	}
	resolved, err := resolveOne(resolution)
	if err != nil {
		return Preparation{}, err
	}
	if resolved.Tenant != requirement.Tenant || resolved.Domain != requirement.Domain || resolved.Generation != requirement.Generation {
		return Preparation{}, fmt.Errorf("%w: resolved tenant/domain generation does not match causal requirement", ErrInvalidResolution)
	}
	head := state.SourceHeads[resolved.SourceAuthority]
	if resolved.SourceRevision != head {
		return Preparation{}, fmt.Errorf("%w: tenant %q resolved source authority %q revision %d, published head is %d", ErrInvalidResolution,
			requirement.Tenant, resolved.SourceAuthority, resolved.SourceRevision, head)
	}
	if resolved.CatalogRevision < requirement.CatalogRevision {
		return Preparation{}, fmt.Errorf("%w: tenant %q resolved at catalog %d, need at least %d", ErrInvalidResolution,
			requirement.Tenant, resolved.CatalogRevision, requirement.CatalogRevision)
	}
	before := cloneState(*state)
	domain := state.Domains[resolved.Domain]
	generationChanged := domain.Generation != resolved.Generation
	if generationChanged {
		domain = DomainState{}
	}
	domain.Tenant = resolved.Tenant
	domain.Domain = resolved.Domain
	domain.Generation = resolved.Generation
	domain.Demanded = resolved.Demanded
	domain.ResolvedSourceRevision = resolved.SourceRevision
	domain.CatalogRevision = resolved.CatalogRevision
	fingerprintChanged := generationChanged || domain.Fingerprint != resolved.Fingerprint
	if fingerprintChanged || (domain.Stale() && domain.Pending == nil && domain.Quarantine == nil) {
		changeID, err := NewChangeID()
		if err != nil {
			return Preparation{}, err
		}
		operationID, err := NewOperationID()
		if err != nil {
			return Preparation{}, err
		}
		change := ChangeSet{
			SourceAuthority:  resolved.SourceAuthority,
			SourceRevision:   resolved.SourceRevision,
			ChangeID:         changeID,
			OperationID:      operationID,
			Cause:            CauseOnDemand,
			Origin:           resolved.Domain,
			OriginGeneration: resolved.Generation,
			AffectedKeys:     effectiveKeys(resolution.Effective),
		}
		if err := validateChange(change, true); err != nil {
			return Preparation{}, err
		}
		state.Revision++
		domain.Fingerprint = resolved.Fingerprint
		domain.Desired = state.Revision
		domain.DesiredChange = change
		domain.Forced = true
	} else if domain.Stale() {
		domain.Forced = true
	}
	state.Domains[resolved.Domain] = domain
	if err := e.save(ctx, *state); err != nil {
		*state = before
		return Preparation{}, err
	}
	if err := e.pump(ctx, state); err != nil {
		return preparationFor(domain), err
	}
	return preparationFor(domain), nil
}

func (e *Engine) acknowledge(ctx context.Context, state *State, ack Ack) error {
	if ack.Domain == "" || ack.Generation == 0 || ack.Revision == 0 || ack.SourceAuthority == "" || ack.SourceRevision == 0 || ack.CatalogRevision == 0 || ack.ChangeID == (ChangeID{}) || ack.OperationID == (OperationID{}) {
		return ErrUnexpectedAck
	}
	domain, ok := state.Domains[ack.Domain]
	if !ok || ack.Generation != domain.Generation || ack.Revision != domain.Notified || ack.Revision <= domain.Observed {
		return ErrUnexpectedAck
	}
	var delivered Notification
	switch {
	case domain.Pending != nil:
		delivered = domain.Pending.Notification
	case domain.Quarantine != nil:
		delivered = domain.Quarantine.Notification
	default:
		return ErrUnexpectedAck
	}
	if ack.Generation != delivered.Generation || ack.Revision != delivered.Revision || ack.SourceAuthority != delivered.SourceAuthority || ack.SourceRevision != delivered.SourceRevision ||
		ack.CatalogRevision != delivered.CatalogRevision || ack.ChangeID != delivered.ChangeID || ack.OperationID != delivered.OperationID ||
		ack.SourceAuthority != domain.NotifiedChange.SourceAuthority || ack.SourceRevision != domain.NotifiedChange.SourceRevision ||
		ack.ChangeID != domain.NotifiedChange.ChangeID || ack.OperationID != domain.NotifiedChange.OperationID {
		return ErrUnexpectedAck
	}
	before := cloneState(*state)
	domain.Observed = ack.Revision
	domain.ObservedCatalogRevision = delivered.CatalogRevision
	domain.ObservedChange = cloneChange(domain.NotifiedChange)
	domain.Pending = nil
	domain.Quarantine = nil
	if domain.Observed == domain.Desired {
		domain.Forced = false
	}
	state.Domains[ack.Domain] = domain
	if err := e.save(ctx, *state); err != nil {
		*state = before
		return err
	}
	return e.pump(ctx, state)
}

func (e *Engine) pump(ctx context.Context, state *State) error {
	for awaiting(*state) < MaxAwaiting {
		domainID, ok := nextDomain(*state)
		if !ok {
			return nil
		}
		before := cloneState(*state)
		domain := state.Domains[domainID]
		previousNotified := domain.Notified
		previousNotifiedCatalog := domain.NotifiedCatalogRevision
		previousNotifiedChange := cloneChange(domain.NotifiedChange)
		notification := notificationFor(domain)
		domain.Notified = domain.Desired
		domain.NotifiedCatalogRevision = domain.CatalogRevision
		domain.NotifiedChange = cloneChange(domain.DesiredChange)
		domain.Pending = &Pending{Notification: cloneNotification(notification), SentAt: e.clock.Now()}
		state.Domains[domainID] = domain
		if err := e.save(ctx, *state); err != nil {
			*state = before
			return err
		}
		delivery, notifyErr := e.notifier.Notify(ctx, notification)
		switch delivery {
		case DeliveryAccepted, DeliveryUnknown:
			if notifyErr != nil {
				return fmt.Errorf("convergence: notify %s revision %d: %w", domainID, domain.Desired, notifyErr)
			}
		case DeliveryNotSent:
			rollback := cloneState(*state)
			domain = state.Domains[domainID]
			domain.Notified = previousNotified
			domain.NotifiedCatalogRevision = previousNotifiedCatalog
			domain.NotifiedChange = previousNotifiedChange
			domain.Pending = nil
			state.Domains[domainID] = domain
			if err := e.save(context.WithoutCancel(ctx), *state); err != nil {
				*state = rollback
				return errors.Join(notifyErr, err)
			}
			if notifyErr == nil {
				return errors.New("convergence: notifier proved not-sent without an error")
			}
			return fmt.Errorf("convergence: notify %s revision %d: %w", domainID, domain.Desired, notifyErr)
		default:
			return errors.New("convergence: notifier returned invalid delivery outcome")
		}
	}
	return nil
}

func (e *Engine) onTimer(ctx context.Context, state *State) error {
	now := e.clock.Now()
	before := cloneState(*state)
	changed := false
	for id, domain := range state.Domains {
		if domain.Pending != nil && !domain.Pending.SentAt.Add(AckTimeout).After(now) {
			domain.Quarantine = &Quarantine{
				Notification: cloneNotification(domain.Pending.Notification),
				Since:        now,
				Until:        now.Add(QuarantineBackoff),
			}
			domain.Pending = nil
			state.Domains[id] = domain
			changed = true
			continue
		}
		if domain.Quarantine != nil && !domain.Quarantine.Until.After(now) {
			domain.Quarantine = nil
			state.Revision++
			domain.Desired = state.Revision
			state.Domains[id] = domain
			changed = true
		}
	}
	if changed {
		if err := e.save(ctx, *state); err != nil {
			*state = before
			return err
		}
	}
	return e.pump(ctx, state)
}

func (e *Engine) timer(state State) <-chan time.Time {
	now := e.clock.Now()
	var deadline time.Time
	for _, domain := range state.Domains {
		candidate := time.Time{}
		if domain.Pending != nil {
			candidate = domain.Pending.SentAt.Add(AckTimeout)
		} else if domain.Quarantine != nil {
			candidate = domain.Quarantine.Until
		}
		if !candidate.IsZero() && (deadline.IsZero() || candidate.Before(deadline)) {
			deadline = candidate
		}
	}
	if deadline.IsZero() {
		return nil
	}
	return e.clock.After(max(time.Duration(0), deadline.Sub(now)))
}

type resolvedTenant struct {
	Tenant          TenantID
	Domain          DomainID
	Generation      Generation
	SourceAuthority SourceAuthorityID
	SourceRevision  Revision
	CatalogRevision CatalogRevision
	Fingerprint     Fingerprint
	Demanded        bool
}

func resolveFleet(resolutions []Resolution, sourceAuthority SourceAuthorityID, sourceRevision Revision) ([]resolvedTenant, error) {
	result := make([]resolvedTenant, 0, len(resolutions))
	tenants := make(map[TenantID]struct{}, len(resolutions))
	domains := make(map[DomainID]struct{}, len(resolutions))
	for _, resolution := range resolutions {
		if !resolution.Registered {
			continue
		}
		if resolution.SourceRevision != sourceRevision {
			return nil, fmt.Errorf("%w: tenant %q resolved at source revision %d, want %d", ErrInvalidResolution, resolution.Tenant, resolution.SourceRevision, sourceRevision)
		}
		if resolution.SourceAuthority != sourceAuthority {
			return nil, fmt.Errorf("%w: tenant %q resolved source authority %q, want %q", ErrInvalidResolution, resolution.Tenant, resolution.SourceAuthority, sourceAuthority)
		}
		resolved, err := resolveOne(resolution)
		if err != nil {
			return nil, err
		}
		if _, duplicate := tenants[resolved.Tenant]; duplicate {
			return nil, fmt.Errorf("%w: duplicate registered domain for tenant %q", ErrInvalidResolution, resolved.Tenant)
		}
		if _, duplicate := domains[resolved.Domain]; duplicate {
			return nil, fmt.Errorf("%w: duplicate domain %q", ErrInvalidResolution, resolved.Domain)
		}
		tenants[resolved.Tenant] = struct{}{}
		domains[resolved.Domain] = struct{}{}
		result = append(result, resolved)
	}
	slices.SortFunc(result, func(a, b resolvedTenant) int { return compareString(string(a.Domain), string(b.Domain)) })
	return result, nil
}

func exactDomainCandidates(resolutions []Resolution, domain DomainID, generation Generation) ([]Resolution, error) {
	for _, resolution := range resolutions {
		if resolution.Registered && resolution.Domain == domain && resolution.Generation == generation {
			return []Resolution{resolution}, nil
		}
	}
	return nil, fmt.Errorf("%w: on-demand domain %q generation %d was not resolved exactly", ErrInvalidResolution, domain, generation)
}

func resolveOne(resolution Resolution) (resolvedTenant, error) {
	if !resolution.Registered || resolution.Tenant == "" || resolution.Domain == "" || resolution.Generation == 0 || resolution.SourceAuthority == "" || resolution.SourceRevision == 0 || resolution.CatalogRevision == 0 {
		return resolvedTenant{}, fmt.Errorf("%w: unregistered or empty tenant/domain", ErrInvalidResolution)
	}
	fingerprint, err := EffectiveFingerprint(resolution.Effective)
	if err != nil {
		return resolvedTenant{}, err
	}
	return resolvedTenant{
		Tenant:          resolution.Tenant,
		Domain:          resolution.Domain,
		Generation:      resolution.Generation,
		SourceAuthority: resolution.SourceAuthority,
		SourceRevision:  resolution.SourceRevision,
		CatalogRevision: resolution.CatalogRevision,
		Fingerprint:     fingerprint,
		Demanded:        resolution.LiveLeases > 0 && resolution.MaterializedInterests > 0,
	}, nil
}

func nextDomain(state State) (DomainID, bool) {
	ids := make([]DomainID, 0, len(state.Domains))
	for id, domain := range state.Domains {
		if domain.Pending != nil || domain.Quarantine != nil {
			continue
		}
		if (!domain.Demanded && !domain.Forced) || !domain.Stale() || domain.Desired <= domain.Notified {
			continue
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return "", false
	}
	slices.Sort(ids)
	return ids[0], true
}

func awaiting(state State) int {
	count := 0
	for _, domain := range state.Domains {
		if domain.Pending != nil {
			count++
		}
	}
	return count
}

func preparationFor(domain DomainState) Preparation {
	return preparationAt(domain, domain.Desired, domain.ResolvedSourceRevision, domain.CatalogRevision, domain.DesiredChange)
}

func preparationAt(domain DomainState, revision, source Revision, catalogRevision CatalogRevision, change ChangeSet) Preparation {
	return Preparation{
		Tenant:          domain.Tenant,
		Domain:          domain.Domain,
		Generation:      domain.Generation,
		Revision:        revision,
		SourceAuthority: change.SourceAuthority,
		SourceRevision:  source,
		CatalogRevision: catalogRevision,
		ChangeID:        change.ChangeID,
		OperationID:     change.OperationID,
	}
}

func registerWaiter(state State, waiters map[uint64]waiter, request command) result {
	if request.waiter == 0 || request.prep.Tenant == "" || request.prep.Domain == "" || request.prep.Generation == 0 || request.prep.Revision == 0 ||
		request.prep.SourceAuthority == "" || request.prep.SourceRevision == 0 || request.prep.CatalogRevision == 0 ||
		request.prep.ChangeID == (ChangeID{}) || request.prep.OperationID == (OperationID{}) {
		return result{err: fmt.Errorf("%w: invalid preparation", ErrInvalidResolution)}
	}
	domain, ok := state.Domains[request.prep.Domain]
	if !ok || domain.Tenant != request.prep.Tenant || domain.Generation != request.prep.Generation || request.prep.Revision > domain.Desired {
		return result{err: fmt.Errorf("%w: unknown preparation", ErrInvalidResolution)}
	}
	if proof, err, settled := waiterOutcome(domain, request.prep); settled {
		return result{proof: proof, err: err}
	}
	wait := make(chan result, 1)
	waiters[request.waiter] = waiter{preparation: request.prep, reply: wait}
	return result{wait: wait}
}

func settleWaiters(state State, waiters map[uint64]waiter) {
	for id, waiter := range waiters {
		domain, ok := state.Domains[waiter.preparation.Domain]
		if !ok {
			waiter.reply <- result{err: fmt.Errorf("%w: prepared domain disappeared", ErrInvalidResolution)}
			delete(waiters, id)
			continue
		}
		proof, err, settled := waiterOutcome(domain, waiter.preparation)
		if !settled {
			continue
		}
		waiter.reply <- result{proof: proof, err: err}
		delete(waiters, id)
	}
}

func waiterOutcome(domain DomainState, requested Preparation) (ObservationProof, error, bool) {
	if domain.Generation != requested.Generation {
		return ObservationProof{}, fmt.Errorf("%w: prepared generation changed", ErrInvalidResolution), true
	}
	if domain.Observed >= requested.Revision {
		source := domain.ObservedChange.SourceRevision
		catalogRevision := domain.ObservedCatalogRevision
		if domain.Observed == domain.Desired {
			source = domain.ResolvedSourceRevision
			catalogRevision = domain.CatalogRevision
		}
		return ObservationProof{
			Requested:      requested,
			Observed:       preparationAt(domain, domain.Observed, source, catalogRevision, domain.ObservedChange),
			ObservedChange: cloneChange(domain.ObservedChange),
		}, nil, true
	}
	if domain.Quarantine != nil && domain.Quarantine.Notification.Revision >= requested.Revision {
		return ObservationProof{}, &QuarantineError{
			Domain:   domain.Domain,
			Revision: domain.Quarantine.Notification.Revision,
			Until:    domain.Quarantine.Until,
		}, true
	}
	return ObservationProof{}, nil, false
}

func effectiveKeys(values []EffectiveValue) []LogicalKey {
	keys := make([]LogicalKey, len(values))
	for index, value := range values {
		keys[index] = value.Key
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

func notificationFor(domain DomainState) Notification {
	change := domain.DesiredChange
	return Notification{
		SourceAuthority:  change.SourceAuthority,
		SourceRevision:   change.SourceRevision,
		CatalogRevision:  domain.CatalogRevision,
		ChangeID:         change.ChangeID,
		OperationID:      change.OperationID,
		Cause:            change.Cause,
		Origin:           change.Origin,
		OriginGeneration: change.OriginGeneration,
		AffectedKeys:     append([]LogicalKey(nil), change.AffectedKeys...),
		Tenant:           domain.Tenant,
		Domain:           domain.Domain,
		Generation:       domain.Generation,
		Revision:         domain.Desired,
		Fingerprint:      domain.Fingerprint,
	}
}

func notificationMatches(notification Notification, domain DomainState, change ChangeSet) bool {
	return notification.Domain == domain.Domain && notification.Tenant == domain.Tenant &&
		notification.Generation == domain.Generation &&
		notification.SourceAuthority == change.SourceAuthority && notification.SourceRevision == change.SourceRevision && notification.CatalogRevision == domain.NotifiedCatalogRevision && notification.ChangeID == change.ChangeID &&
		notification.OperationID == change.OperationID && notification.Cause == change.Cause &&
		notification.Origin == change.Origin && notification.OriginGeneration == change.OriginGeneration && slices.Equal(notification.AffectedKeys, change.AffectedKeys)
}

func equalChange(a, b ChangeSet) bool {
	return a.SourceAuthority == b.SourceAuthority && a.SourceRevision == b.SourceRevision && a.ChangeID == b.ChangeID &&
		a.OperationID == b.OperationID && a.Cause == b.Cause && a.Origin == b.Origin && a.OriginGeneration == b.OriginGeneration &&
		slices.Equal(a.AffectedKeys, b.AffectedKeys)
}

func compactChanges(state *State) {
	if len(state.Changes) <= MaxAppliedChanges {
		return
	}
	changes := make([]AppliedChange, 0, len(state.Changes))
	for _, change := range state.Changes {
		changes = append(changes, change)
	}
	slices.SortFunc(changes, func(a, b AppliedChange) int {
		switch {
		case a.Change.SourceAuthority < b.Change.SourceAuthority:
			return -1
		case a.Change.SourceAuthority > b.Change.SourceAuthority:
			return 1
		case a.Change.SourceRevision < b.Change.SourceRevision:
			return -1
		case a.Change.SourceRevision > b.Change.SourceRevision:
			return 1
		default:
			return slices.Compare(a.Change.ChangeID[:], b.Change.ChangeID[:])
		}
	})
	for _, applied := range changes[:len(changes)-MaxAppliedChanges] {
		delete(state.Changes, applied.Change.ChangeID)
		if state.DedupFloors == nil {
			state.DedupFloors = make(map[SourceAuthorityID]Revision)
		}
		state.DedupFloors[applied.Change.SourceAuthority] = max(state.DedupFloors[applied.Change.SourceAuthority], applied.Change.SourceRevision)
	}
}

func validateChange(change ChangeSet, allowOnDemand bool) error {
	if change.SourceAuthority == "" || change.SourceRevision == 0 || change.ChangeID == (ChangeID{}) || change.OperationID == (OperationID{}) {
		return fmt.Errorf("%w: empty source authority or zero revision/change/operation id", ErrInvalidChange)
	}
	if len(change.AffectedKeys) == 0 {
		return fmt.Errorf("%w: no affected keys", ErrInvalidChange)
	}
	for index, key := range change.AffectedKeys {
		if key == "" {
			return fmt.Errorf("%w: empty affected key", ErrInvalidChange)
		}
		if index > 0 && change.AffectedKeys[index-1] >= key {
			return fmt.Errorf("%w: affected keys are not sorted and unique", ErrInvalidChange)
		}
	}
	switch change.Cause {
	case CauseProviderMutation:
		if change.Origin == "" || change.OriginGeneration == 0 {
			return fmt.Errorf("%w: provider mutation has no origin", ErrInvalidChange)
		}
	case CauseDaemonWrite, CauseExternalUnattributed, CauseMigration:
		if change.Origin != "" || change.OriginGeneration != 0 {
			return fmt.Errorf("%w: non-provider change has an origin", ErrInvalidChange)
		}
	case CauseOnDemand:
		if !allowOnDemand || change.Origin == "" || change.OriginGeneration == 0 {
			return fmt.Errorf("%w: invalid on-demand change", ErrInvalidChange)
		}
	default:
		return fmt.Errorf("%w: cause %q", ErrInvalidChange, change.Cause)
	}
	return nil
}

func validateState(state State) error {
	for authority, floor := range state.DedupFloors {
		if authority == "" || floor > state.SourceHeads[authority] {
			return errors.New("convergence: dedup floor exceeds source authority head")
		}
	}
	for authority, head := range state.SourceHeads {
		if authority == "" || head == 0 {
			return errors.New("convergence: invalid source authority head")
		}
	}
	if len(state.Changes) > MaxAppliedChanges {
		return fmt.Errorf("convergence: durable change journal has %d entries", len(state.Changes))
	}
	for id, applied := range state.Changes {
		if id != applied.Change.ChangeID || applied.Change.SourceRevision <= state.DedupFloors[applied.Change.SourceAuthority] ||
			applied.Change.SourceRevision > state.SourceHeads[applied.Change.SourceAuthority] || applied.EngineRevision > state.Revision {
			return fmt.Errorf("convergence: invalid durable change %x", id)
		}
		if err := validateChange(applied.Change, true); err != nil {
			return err
		}
	}
	for id, domain := range state.Domains {
		if id == "" || id != domain.Domain || domain.Tenant == "" || domain.Generation == 0 || domain.ResolvedSourceRevision == 0 || domain.CatalogRevision == 0 ||
			domain.Observed > domain.Desired || domain.Notified > domain.Desired {
			return fmt.Errorf("convergence: invalid durable domain %q", id)
		}
		if domain.Desired > 0 {
			if err := validateChange(domain.DesiredChange, true); err != nil {
				return err
			}
		}
		if domain.Notified > 0 {
			if domain.NotifiedCatalogRevision == 0 {
				return fmt.Errorf("convergence: notified domain %q has no catalog revision", id)
			}
			if err := validateChange(domain.NotifiedChange, true); err != nil {
				return err
			}
		}
		if domain.Observed > 0 {
			if domain.ObservedCatalogRevision == 0 {
				return fmt.Errorf("convergence: observed domain %q has no catalog revision", id)
			}
			if err := validateChange(domain.ObservedChange, true); err != nil {
				return err
			}
		}
		if domain.Pending != nil && (domain.Pending.Notification.Revision != domain.Notified || domain.Pending.SentAt.IsZero() || !notificationMatches(domain.Pending.Notification, domain, domain.NotifiedChange)) {
			return fmt.Errorf("convergence: invalid pending domain %q", id)
		}
		if domain.Quarantine != nil && (domain.Quarantine.Notification.Revision > domain.Notified || domain.Quarantine.Since.IsZero() ||
			!domain.Quarantine.Until.After(domain.Quarantine.Since) || !notificationMatches(domain.Quarantine.Notification, domain, domain.NotifiedChange)) {
			return fmt.Errorf("convergence: invalid quarantined domain %q", id)
		}
	}
	return nil
}

func (e *Engine) save(ctx context.Context, state State) error {
	if err := e.store.Save(ctx, cloneState(state)); err != nil {
		return fmt.Errorf("convergence: save state: %w", err)
	}
	return nil
}

func cloneState(state State) State {
	cloned := State{
		Revision:    state.Revision,
		SourceHeads: make(map[SourceAuthorityID]Revision, len(state.SourceHeads)),
		DedupFloors: make(map[SourceAuthorityID]Revision, len(state.DedupFloors)),
		Domains:     make(map[DomainID]DomainState, len(state.Domains)),
		Changes:     make(map[ChangeID]AppliedChange, len(state.Changes)),
	}
	for authority, head := range state.SourceHeads {
		cloned.SourceHeads[authority] = head
	}
	for authority, floor := range state.DedupFloors {
		cloned.DedupFloors[authority] = floor
	}
	for id, domain := range state.Domains {
		domain.DesiredChange = cloneChange(domain.DesiredChange)
		domain.NotifiedChange = cloneChange(domain.NotifiedChange)
		domain.ObservedChange = cloneChange(domain.ObservedChange)
		if domain.Pending != nil {
			pending := *domain.Pending
			pending.Notification = cloneNotification(pending.Notification)
			domain.Pending = &pending
		}
		if domain.Quarantine != nil {
			quarantine := *domain.Quarantine
			quarantine.Notification = cloneNotification(quarantine.Notification)
			domain.Quarantine = &quarantine
		}
		cloned.Domains[id] = domain
	}
	for id, applied := range state.Changes {
		applied.Change = cloneChange(applied.Change)
		if applied.CatalogRevisions != nil {
			catalogs := make(map[TenantID]CatalogRevision, len(applied.CatalogRevisions))
			for tenant, revision := range applied.CatalogRevisions {
				catalogs[tenant] = revision
			}
			applied.CatalogRevisions = catalogs
		}
		cloned.Changes[id] = applied
	}
	return cloned
}

func cloneChange(change ChangeSet) ChangeSet {
	change.AffectedKeys = append([]LogicalKey(nil), change.AffectedKeys...)
	return change
}

func cloneNotification(notification Notification) Notification {
	notification.AffectedKeys = append([]LogicalKey(nil), notification.AffectedKeys...)
	return notification
}

func (e *Engine) setEndError(err error) {
	e.mu.Lock()
	e.terminalErr = err
	e.mu.Unlock()
}

func (e *Engine) endError() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if errors.Is(e.terminalErr, context.Canceled) {
		return nil
	}
	return e.terminalErr
}

func (e *Engine) closedError() error {
	if err := e.endError(); err != nil {
		return errors.Join(ErrClosed, err)
	}
	return ErrClosed
}
