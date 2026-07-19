package catalogservice

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
)

const brokerCommandBuffer = 32

var errBrokerSessionLost = errors.New("catalog service: broker session lost")

type brokerPending struct {
	command catalogproto.BrokerCommand
	done    chan brokerOutcome
}

type brokerOutcome struct {
	delivery convergence.Delivery
	err      error
}

// RuntimeBroker owns actual-domain reconciliation and convergence delivery over one broker stream.
type RuntimeBroker struct {
	catalog *catalog.Catalog

	mu      sync.Mutex
	active  *runtimeBrokerSession
	closed  bool
	pending map[uint64]brokerPending
	ready   func()
}

// SetReady installs the non-blocking convergence retry triggered after domain reconciliation.
func (b *RuntimeBroker) SetReady(ready func()) {
	b.mu.Lock()
	b.ready = ready
	b.mu.Unlock()
}

// NewRuntimeBroker creates an unconnected broker runtime over durable catalog state.
func NewRuntimeBroker(store *catalog.Catalog) (*RuntimeBroker, error) {
	if store == nil {
		return nil, errors.New("catalog service: broker catalog is required")
	}
	return &RuntimeBroker{catalog: store, pending: make(map[uint64]brokerPending)}, nil
}

// OpenBroker installs one authenticated signed-app broker session.
func (b *RuntimeBroker) OpenBroker(ctx context.Context, _ Identity, _ string) (BrokerSession, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, errors.New("catalog service: broker runtime closed")
	}
	if b.active != nil {
		b.mu.Unlock()
		return nil, errors.New("catalog service: prior broker session has not settled")
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	session := &runtimeBrokerSession{
		hub: b, ctx: sessionCtx, cancel: cancel,
		commands: make(chan catalogproto.BrokerCommand, brokerCommandBuffer),
		done:     make(chan struct{}),
	}
	b.active = session
	b.mu.Unlock()
	if err := b.enqueue(sessionCtx, session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindListDomains}, nil); err != nil {
		session.Close(err)
		return nil, err
	}
	return session, nil
}

// Notify sends one exact convergence command or proves it was not sent.
func (b *RuntimeBroker) Notify(ctx context.Context, notification convergence.Notification) (convergence.Delivery, error) {
	b.mu.Lock()
	session := b.active
	closed := b.closed
	b.mu.Unlock()
	if closed || session == nil {
		return convergence.DeliveryNotSent, errBrokerSessionLost
	}
	targets, err := b.catalog.FileProviderSignalTargets(
		ctx, catalog.TenantID(notification.Tenant), notification.Domain,
		catalog.Generation(notification.Generation), catalog.Revision(notification.CatalogRevision),
	)
	if err != nil {
		return convergence.DeliveryNotSent, err
	}
	protocolTargets := make([]catalogproto.SignalTarget, 0, len(targets))
	for _, target := range targets {
		if target.WorkingSet {
			protocolTargets = append(protocolTargets, catalogproto.SignalTarget{Kind: catalogproto.SignalTargetKindWorkingSet})
			continue
		}
		parent := catalogproto.ObjectID(target.Parent.String())
		protocolTargets = append(protocolTargets, catalogproto.SignalTarget{Kind: catalogproto.SignalTargetKindContainer, ParentID: &parent})
	}
	changeID := catalogproto.ChangeID(hex.EncodeToString(notification.ChangeID[:]))
	operationID := catalogproto.MutationID(hex.EncodeToString(notification.OperationID[:]))
	affected := make([]string, len(notification.AffectedKeys))
	for index, key := range notification.AffectedKeys {
		affected[index] = string(key)
	}
	command := catalogproto.BrokerCommand{
		Kind: catalogproto.BrokerCommandKindSignalDomain,
		Notification: &catalogproto.ConvergenceNotification{
			Protocol: catalogproto.Version, TenantID: catalogproto.TenantID(notification.Tenant),
			DomainID: catalogproto.DomainID(notification.Domain), Generation: uint64(notification.Generation),
			Revision: uint64(notification.Revision), CatalogRevision: uint64(notification.CatalogRevision),
			SourceAuthority: catalogproto.SourceAuthorityID(notification.SourceAuthority), SourceRevision: uint64(notification.SourceRevision),
			ChangeID: changeID, OperationID: operationID, Cause: catalogproto.ConvergenceCause(notification.Cause),
			AffectedKeys: affected, Targets: protocolTargets,
		},
	}
	done := make(chan brokerOutcome, 1)
	if err := b.enqueue(ctx, session, command, done); err != nil {
		return convergence.DeliveryNotSent, err
	}
	select {
	case outcome := <-done:
		return outcome.delivery, outcome.err
	case <-ctx.Done():
		return convergence.DeliveryUnknown, nil
	case <-session.done:
		return convergence.DeliveryUnknown, nil
	}
}

// Close disconnects the broker and settles every possibly sent command as unknown.
func (b *RuntimeBroker) Close() {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return
	}
	b.closed = true
	session := b.active
	b.mu.Unlock()
	if session != nil {
		session.Close(errors.New("catalog service: broker runtime closed"))
	}
}

func (b *RuntimeBroker) enqueue(
	ctx context.Context,
	session *runtimeBrokerSession,
	command catalogproto.BrokerCommand,
	done chan brokerOutcome,
) error {
	id, err := b.catalog.NextBrokerCommandID(ctx)
	if err != nil {
		return err
	}
	command.Protocol = catalogproto.Version
	command.CommandID = id
	if err := catalogproto.Validate(command); err != nil {
		return err
	}
	b.mu.Lock()
	if b.closed || b.active != session {
		b.mu.Unlock()
		return errBrokerSessionLost
	}
	b.pending[id] = brokerPending{command: command, done: done}
	b.mu.Unlock()
	select {
	case session.commands <- command:
		return nil
	case <-ctx.Done():
		b.mu.Lock()
		delete(b.pending, id)
		b.mu.Unlock()
		return ctx.Err()
	case <-session.done:
		b.mu.Lock()
		delete(b.pending, id)
		b.mu.Unlock()
		return errBrokerSessionLost
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
	delete(b.pending, result.CommandID)
	b.mu.Unlock()

	switch result.Kind {
	case catalogproto.BrokerCommandKindListDomains:
		if result.Code != catalogproto.ErrorCodeOk || result.Domains == nil {
			return fmt.Errorf("catalog service: broker list domains failed: %s", result.Message)
		}
		if err := b.reconcile(ctx, session, *result.Domains); err != nil {
			return err
		}
		b.mu.Lock()
		ready := b.ready
		b.mu.Unlock()
		if ready != nil {
			go ready()
		}
	case catalogproto.BrokerCommandKindRegisterDomain:
		if result.Code != catalogproto.ErrorCodeOk || result.Registered == nil {
			return fmt.Errorf("catalog service: broker register domain failed: %s", result.Message)
		}
		domain, err := catalogDomain(*result.Registered)
		if err != nil {
			return err
		}
		if err := b.catalog.ConfirmFileProviderDomain(ctx, domain); err != nil {
			return err
		}
		return b.enqueue(ctx, session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindListDomains}, nil)
	case catalogproto.BrokerCommandKindRemoveDomain:
		if result.Code != catalogproto.ErrorCodeOk || result.ConfirmedAbsent == nil || !*result.ConfirmedAbsent || pending.command.DomainID == nil {
			return fmt.Errorf("catalog service: broker remove domain failed: %s", result.Message)
		}
		if err := b.catalog.ConfirmFileProviderDomainAbsent(ctx, causal.DomainID(*pending.command.DomainID)); err != nil {
			return err
		}
		return b.enqueue(ctx, session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindListDomains}, nil)
	case catalogproto.BrokerCommandKindSignalDomain:
		if result.Code == catalogproto.ErrorCodeOk && result.SignalAccepted != nil && *result.SignalAccepted {
			return b.settle(pending, convergence.DeliveryAccepted, nil)
		}
		return b.settle(pending, convergence.DeliveryUnknown, nil)
	default:
		return errors.New("catalog service: unknown runtime broker result")
	}
	return b.settle(pending, convergence.DeliveryAccepted, nil)
}

func (b *RuntimeBroker) settle(pending brokerPending, delivery convergence.Delivery, err error) error {
	if pending.done != nil {
		pending.done <- brokerOutcome{delivery: delivery, err: err}
	}
	return err
}

func (b *RuntimeBroker) reconcile(ctx context.Context, session *runtimeBrokerSession, actual []catalogproto.RegisteredDomain) error {
	desired, err := b.catalog.FileProviderDomains(ctx)
	if err != nil {
		return err
	}
	desiredByID := make(map[catalogproto.DomainID]catalog.FileProviderDomain, len(desired))
	for _, domain := range desired {
		desiredByID[catalogproto.DomainID(domain.DomainID)] = domain
	}
	actualByID := make(map[catalogproto.DomainID]catalogproto.RegisteredDomain, len(actual))
	for _, domain := range actual {
		actualByID[domain.DomainID] = domain
		desiredDomain, ok := desiredByID[domain.DomainID]
		converted, convertErr := catalogDomain(domain)
		if !ok || convertErr != nil || !sameDomainIdentity(desiredDomain, converted) {
			id := domain.DomainID
			if err := b.enqueue(ctx, session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindRemoveDomain, DomainID: &id}, nil); err != nil {
				return err
			}
			continue
		}
		if err := b.catalog.ConfirmFileProviderDomain(ctx, converted); err != nil {
			return err
		}
	}
	for id, domain := range desiredByID {
		if _, ok := actualByID[id]; ok {
			continue
		}
		registration := protocolDomainRegistration(domain)
		if err := b.enqueue(ctx, session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindRegisterDomain, Registration: &registration}, nil); err != nil {
			return err
		}
	}
	return nil
}

func (b *RuntimeBroker) sessionClosed(session *runtimeBrokerSession) {
	b.mu.Lock()
	if b.active != session {
		b.mu.Unlock()
		return
	}
	b.active = nil
	for id, pending := range b.pending {
		delete(b.pending, id)
		if pending.done != nil {
			pending.done <- brokerOutcome{delivery: convergence.DeliveryUnknown}
		}
	}
	b.mu.Unlock()
}

type runtimeBrokerSession struct {
	hub      *RuntimeBroker
	ctx      context.Context
	cancel   context.CancelFunc
	commands chan catalogproto.BrokerCommand
	done     chan struct{}
	once     sync.Once
}

func (s *runtimeBrokerSession) Commands() <-chan catalogproto.BrokerCommand { return s.commands }

func (s *runtimeBrokerSession) AcceptResult(ctx context.Context, result catalogproto.BrokerResult) error {
	return s.hub.accept(ctx, s, result)
}

func (s *runtimeBrokerSession) Close(_ error) {
	s.once.Do(func() {
		s.cancel()
		close(s.done)
		s.hub.sessionClosed(s)
	})
}

func protocolDomainRegistration(domain catalog.FileProviderDomain) catalogproto.DomainRegistration {
	return catalogproto.DomainRegistration{
		DomainID: catalogproto.DomainID(domain.DomainID), OwnerID: catalogproto.OwnerID(domain.OwnerID),
		TenantID: catalogproto.TenantID(domain.Tenant), Generation: uint64(domain.Generation),
		RootID: catalogproto.ObjectID(domain.Root.String()), AccountInstanceID: catalogproto.AccountInstanceID(domain.AccountInstance),
		DisplayName: domain.DisplayName,
	}
}

func catalogDomain(domain catalogproto.RegisteredDomain) (catalog.FileProviderDomain, error) {
	root, err := catalog.ParseObjectID(string(domain.RootID))
	if err != nil {
		return catalog.FileProviderDomain{}, err
	}
	return catalog.FileProviderDomain{
		DomainID: causal.DomainID(domain.DomainID), OwnerID: string(domain.OwnerID), Tenant: catalog.TenantID(domain.TenantID),
		Generation: catalog.Generation(domain.Generation), Root: root, AccountInstance: string(domain.AccountInstanceID),
		DisplayName: domain.DisplayName, PublicPath: domain.PublicPath, Registered: true,
	}, nil
}

func sameDomainIdentity(left, right catalog.FileProviderDomain) bool {
	return left.DomainID == right.DomainID && left.OwnerID == right.OwnerID && left.Tenant == right.Tenant &&
		left.Generation == right.Generation && left.Root == right.Root && left.AccountInstance == right.AccountInstance &&
		left.DisplayName == right.DisplayName
}

var _ BrokerService = (*RuntimeBroker)(nil)
var _ convergence.Notifier = (*RuntimeBroker)(nil)
