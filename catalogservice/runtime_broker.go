package catalogservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
)

const (
	brokerCommandBuffer   = 32
	domainCutoverClaimTTL = 30 * time.Second
)

var errBrokerSessionLost = errors.New("catalog service: broker session lost")

type brokerPending struct {
	command catalogproto.BrokerCommand
	done    chan brokerOutcome
	removal *catalog.FileProviderDomainRemoval
}

type brokerOutcome struct {
	delivery convergence.Delivery
	proof    *catalogproto.DomainAbsenceProof
	err      error
}

// RuntimeBroker owns actual-domain reconciliation and convergence delivery over one broker stream.
type RuntimeBroker struct {
	catalog  *catalog.Catalog
	identity BrokerIdentity

	mu       sync.Mutex
	active   *runtimeBrokerSession
	closed   bool
	pending  map[uint64]brokerPending
	ready    func()
	changed  chan struct{}
	boot     func() (string, error)
	uptime   func() (time.Duration, error)
	claimTTL time.Duration
}

// SetReady installs the non-blocking convergence retry triggered after domain reconciliation.
func (b *RuntimeBroker) SetReady(ready func()) {
	b.mu.Lock()
	b.ready = ready
	b.mu.Unlock()
}

// NewRuntimeBroker creates an unconnected broker runtime over durable catalog state.
func NewRuntimeBroker(store *catalog.Catalog, identity BrokerIdentity) (*RuntimeBroker, error) {
	if store == nil {
		return nil, errors.New("catalog service: broker catalog is required")
	}
	if identity.ProductBuild == "" || identity.Executable == "" || identity.DesignatedRequirement == "" ||
		identity.EntitlementValidationDigest == ([32]byte{}) {
		return nil, errors.New("catalog service: fixed broker identity is incomplete")
	}
	return &RuntimeBroker{
		catalog: store, identity: identity, pending: make(map[uint64]brokerPending), changed: make(chan struct{}),
		boot: proc.BootID, uptime: proc.MonotonicUptime, claimTTL: domainCutoverClaimTTL,
	}, nil
}

// ClaimDomainCutover atomically consumes one exact proof before the owning app
// can be stopped. Concurrent and replaying callers fail closed.
func (b *RuntimeBroker) ClaimDomainCutover(
	ctx context.Context,
	proof catalogproto.DomainAbsenceProof,
) (catalogproto.DomainCutoverClaim, error) {
	if err := catalogproto.Validate(proof); err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	operation, err := catalog.ParseMutationID(string(proof.Result.Plan.OperationID))
	if err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	proofHash, err := domainAbsenceProofDigest(proof)
	if err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	boot, uptime, err := b.cutoverClock()
	if err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	mode, err := b.catalog.ClaimFileProviderCutoverProof(ctx, operation, proofHash, boot, uptime, b.claimTTL)
	if err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	claim := catalogproto.DomainCutoverClaim{
		OperationID: catalogproto.MutationID(mode.OperationID.String()),
		ProofDigest: hex.EncodeToString(mode.ProofHash[:]), ClaimedAtUnixNano: mode.ClaimedAt.UnixNano(),
	}
	return claim, catalogproto.Validate(claim)
}

// RecoverDomainCutoverClaim returns an already-committed exact claim without
// creating or consuming another claim transition.
func (b *RuntimeBroker) RecoverDomainCutoverClaim(
	ctx context.Context,
	proof catalogproto.DomainAbsenceProof,
) (catalogproto.DomainCutoverClaim, error) {
	if err := catalogproto.Validate(proof); err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	operation, err := catalog.ParseMutationID(string(proof.Result.Plan.OperationID))
	if err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	proofHash, err := domainAbsenceProofDigest(proof)
	if err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	mode, err := b.catalog.RecoverClaimedFileProviderCutoverProof(ctx, operation, proofHash)
	if err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	proofJSON, err := json.Marshal(proof)
	if err != nil {
		return catalogproto.DomainCutoverClaim{}, err
	}
	if !bytes.Equal(mode.ProofJSON, proofJSON) {
		return catalogproto.DomainCutoverClaim{}, errors.New("catalog service: claimed domain cutover proof bytes changed")
	}
	claim := catalogproto.DomainCutoverClaim{
		OperationID: catalogproto.MutationID(mode.OperationID.String()),
		ProofDigest: hex.EncodeToString(mode.ProofHash[:]), ClaimedAtUnixNano: mode.ClaimedAt.UnixNano(),
	}
	return claim, catalogproto.Validate(claim)
}

// RecoverDomainCutoverReceipt reads the one terminal claim by canonical
// account-set identity when the caller crashed before persisting its receipt.
func (b *RuntimeBroker) RecoverDomainCutoverReceipt(
	ctx context.Context,
	key catalogproto.DomainCutoverRecoveryKey,
) (catalogproto.DomainCutoverReceipt, error) {
	if err := catalogproto.Validate(key); err != nil {
		return catalogproto.DomainCutoverReceipt{}, err
	}
	planHash, err := domainCutoverRecoveryKeyHash(key)
	if err != nil {
		return catalogproto.DomainCutoverReceipt{}, err
	}
	mode, err := b.catalog.RecoverClaimedFileProviderCutoverByPlan(ctx, planHash)
	if err != nil {
		return catalogproto.DomainCutoverReceipt{}, err
	}
	var proof catalogproto.DomainAbsenceProof
	if err := json.Unmarshal(mode.ProofJSON, &proof); err != nil {
		return catalogproto.DomainCutoverReceipt{}, fmt.Errorf("catalog service: decode claimed domain cutover proof: %w", err)
	}
	storedPlanHash, err := domainCutoverPlanHash(proof.Result.Plan)
	if err != nil || storedPlanHash != planHash {
		return catalogproto.DomainCutoverReceipt{}, errors.New("catalog service: claimed domain cutover account set changed")
	}
	receipt := catalogproto.DomainCutoverReceipt{
		Proof: proof,
		Claim: catalogproto.DomainCutoverClaim{
			OperationID: catalogproto.MutationID(mode.OperationID.String()),
			ProofDigest: hex.EncodeToString(mode.ProofHash[:]), ClaimedAtUnixNano: mode.ClaimedAt.UnixNano(),
		},
	}
	return receipt, catalogproto.Validate(receipt)
}

// RemoveTenantDomain fences and settles one exact File Provider domain before
// tenant state can be retired. A disconnected broker leaves the caller waiting
// while the durable intent is resumed by the next authenticated session.
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
	b.requestReconcile(ctx)
	for {
		state, err := b.catalog.FileProviderDomainRemovalState(ctx, owner, tenantID, generation)
		if err != nil {
			return err
		}
		if state.ConfirmedAbsent {
			return nil
		}
		b.mu.Lock()
		changed := b.changed
		closed := b.closed
		b.mu.Unlock()
		if closed {
			return errors.New("catalog service: broker runtime closed during domain removal")
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
		done:     make(chan struct{}), ready: make(chan struct{}), identity: identity,
	}
	b.active = session
	b.signalChangedLocked()
	b.mu.Unlock()
	if err := b.enqueue(sessionCtx, session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindListDomains}, nil); err != nil {
		close(session.ready)
		session.Close(err)
		return nil, err
	}
	close(session.ready)
	return session, nil
}

// ProveBrokerPeer returns the exact current signed broker session identity
// after that peer has passed the holder's full protected policy.
func (b *RuntimeBroker) ProveBrokerPeer(ctx context.Context) (catalogproto.BrokerPeerProof, error) {
	for {
		b.mu.Lock()
		session := b.active
		closed := b.closed
		changed := b.changed
		b.mu.Unlock()
		if closed {
			return catalogproto.BrokerPeerProof{}, errors.New("catalog service: broker runtime closed during peer proof")
		}
		if session == nil {
			select {
			case <-ctx.Done():
				return catalogproto.BrokerPeerProof{}, errors.Join(ErrBrokerStreamAbsent, ctx.Err())
			case <-changed:
				continue
			}
		}
		select {
		case <-session.ready:
		case <-session.done:
			continue
		case <-ctx.Done():
			return catalogproto.BrokerPeerProof{}, ctx.Err()
		}
		proof := b.peerProof(session.identity)
		return proof, catalogproto.Validate(proof)
	}
}

// CutoverDomains removes the exact planned File Provider domain identities
// through the authenticated signed broker and binds its proof to that session.
func (b *RuntimeBroker) CutoverDomains(ctx context.Context, plan catalogproto.DomainCutoverPlan) (catalogproto.DomainAbsenceProof, error) {
	if err := catalogproto.Validate(plan); err != nil {
		return catalogproto.DomainAbsenceProof{}, err
	}
	operation, err := catalog.ParseMutationID(string(plan.OperationID))
	if err != nil {
		return catalogproto.DomainAbsenceProof{}, err
	}
	planHash, err := domainCutoverPlanHash(plan)
	if err != nil {
		return catalogproto.DomainAbsenceProof{}, err
	}
	boot, uptime, err := b.cutoverClock()
	if err != nil {
		return catalogproto.DomainAbsenceProof{}, err
	}
	if _, err := b.catalog.ExpireFileProviderCutoverProof(ctx, boot, uptime, b.claimTTL); err != nil {
		return catalogproto.DomainAbsenceProof{}, err
	}
	mode, created, err := b.catalog.BeginFileProviderCutover(ctx, operation, planHash)
	if err != nil {
		return catalogproto.DomainAbsenceProof{}, err
	}
	plan.OperationID = catalogproto.MutationID(mode.OperationID.String())
	if mode.State == catalog.FileProviderCutoverProved || mode.State == catalog.FileProviderCutoverClaimed {
		var proof catalogproto.DomainAbsenceProof
		if err := json.Unmarshal(mode.ProofJSON, &proof); err != nil {
			return catalogproto.DomainAbsenceProof{}, fmt.Errorf("catalog service: decode durable domain absence proof: %w", err)
		}
		if err := catalogproto.Validate(proof); err != nil {
			return catalogproto.DomainAbsenceProof{}, err
		}
		if proof.Result.Plan.OperationID != plan.OperationID ||
			!sameDomainCutoverRecoveryKey(domainCutoverRecoveryKey(proof.Result.Plan), domainCutoverRecoveryKey(plan)) {
			return catalogproto.DomainAbsenceProof{}, errors.New("catalog service: durable domain absence proof changed the plan")
		}
		return proof, nil
	}
	if created {
		b.mu.Lock()
		active := b.active
		b.mu.Unlock()
		if active != nil {
			active.Close(errors.New("catalog service: retire broker commands before durable File Provider cutover"))
		}
	}
	for {
		b.mu.Lock()
		session := b.active
		closed := b.closed
		changed := b.changed
		b.mu.Unlock()
		if closed {
			return catalogproto.DomainAbsenceProof{}, errors.New("catalog service: broker runtime closed during domain cutover")
		}
		if session == nil {
			select {
			case <-ctx.Done():
				return catalogproto.DomainAbsenceProof{}, errors.Join(ErrBrokerStreamAbsent, ctx.Err())
			case <-changed:
				continue
			}
		}
		select {
		case <-session.ready:
		case <-session.done:
			continue
		case <-ctx.Done():
			return catalogproto.DomainAbsenceProof{}, ctx.Err()
		}
		done := make(chan brokerOutcome, 1)
		copyPlan := plan
		if err := b.enqueue(ctx, session, catalogproto.BrokerCommand{
			Kind: catalogproto.BrokerCommandKindCutoverDomains, Cutover: &copyPlan,
		}, done); err != nil {
			if errors.Is(err, errBrokerSessionLost) {
				continue
			}
			return catalogproto.DomainAbsenceProof{}, err
		}
		select {
		case outcome := <-done:
			if errors.Is(outcome.err, errBrokerSessionLost) {
				continue
			}
			if outcome.err != nil {
				return catalogproto.DomainAbsenceProof{}, outcome.err
			}
			if outcome.proof == nil {
				return catalogproto.DomainAbsenceProof{}, errors.New("catalog service: broker returned no domain absence proof")
			}
			return *outcome.proof, nil
		case <-ctx.Done():
			session.Close(ctx.Err())
			return catalogproto.DomainAbsenceProof{}, ctx.Err()
		case <-session.done:
			continue
		}
	}
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
	b.signalChangedLocked()
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
	return b.enqueuePending(ctx, session, brokerPending{command: command, done: done})
}

func (b *RuntimeBroker) enqueueRemoval(
	ctx context.Context,
	session *runtimeBrokerSession,
	removal catalog.FileProviderDomainRemoval,
) error {
	domainID := catalogproto.DomainID(removal.Domain.DomainID)
	return b.enqueuePending(ctx, session, brokerPending{
		command: catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindRemoveDomain, DomainID: &domainID},
		removal: &removal,
	})
}

func (b *RuntimeBroker) enqueuePending(
	ctx context.Context,
	session *runtimeBrokerSession,
	pending brokerPending,
) error {
	id, err := b.catalog.NextBrokerCommandID(ctx)
	if err != nil {
		return err
	}
	pending.command.Protocol = catalogproto.Version
	pending.command.CommandID = id
	if err := catalogproto.Validate(pending.command); err != nil {
		return err
	}
	b.mu.Lock()
	if b.closed || b.active != session {
		b.mu.Unlock()
		return errBrokerSessionLost
	}
	if pending.command.Kind == catalogproto.BrokerCommandKindListDomains {
		for _, existing := range b.pending {
			if existing.command.Kind == catalogproto.BrokerCommandKindListDomains {
				b.mu.Unlock()
				return nil
			}
		}
	}
	if pending.command.Kind == catalogproto.BrokerCommandKindRemoveDomain && pending.command.DomainID != nil {
		for existingID, existing := range b.pending {
			if existing.command.Kind != catalogproto.BrokerCommandKindRemoveDomain || existing.command.DomainID == nil ||
				*pending.command.DomainID != *existing.command.DomainID {
				continue
			}
			if pending.removal != nil && existing.removal == nil {
				removal := *pending.removal
				existing.removal = &removal
				b.pending[existingID] = existing
			} else if pending.removal != nil && !sameDomainIdentity(existing.removal.Domain, pending.removal.Domain) {
				b.mu.Unlock()
				return errors.New("catalog service: pending domain removal identity changed")
			}
			b.mu.Unlock()
			return nil
		}
	}
	b.pending[id] = pending
	b.mu.Unlock()
	select {
	case session.commands <- pending.command:
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
	case catalogproto.BrokerCommandKindCutoverDomains:
		if result.Code != catalogproto.ErrorCodeOk || result.CutoverResult == nil || pending.command.Cutover == nil {
			return b.settle(pending, convergence.DeliveryUnknown, fmt.Errorf("catalog service: broker domain cutover failed: %s", result.Message))
		}
		if !sameDomainCutoverPlan(*pending.command.Cutover, result.CutoverResult.Plan) {
			return b.settle(pending, convergence.DeliveryUnknown, errors.New("catalog service: broker domain cutover proof changed the plan"))
		}
		peerProof := b.peerProof(session.identity)
		proof := catalogproto.DomainAbsenceProof{
			Result: *result.CutoverResult, BrokerProductBuild: peerProof.BrokerProductBuild,
			BrokerPID: peerProof.BrokerPID, BrokerUID: peerProof.BrokerUID, BrokerStartTime: peerProof.BrokerStartTime,
			BrokerBoot: peerProof.BrokerBoot, BrokerComm: peerProof.BrokerComm, BrokerExecutable: peerProof.BrokerExecutable,
			BrokerDesignatedRequirement:       peerProof.BrokerDesignatedRequirement,
			BrokerAuditTokenDigest:            peerProof.BrokerAuditTokenDigest,
			BrokerEntitlementValidationDigest: peerProof.BrokerEntitlementValidationDigest,
		}
		if err := catalogproto.Validate(proof); err != nil {
			return b.settle(pending, convergence.DeliveryUnknown, err)
		}
		operation, err := catalog.ParseMutationID(string(proof.Result.Plan.OperationID))
		if err != nil {
			return b.settle(pending, convergence.DeliveryUnknown, err)
		}
		planHash, err := domainCutoverPlanHash(proof.Result.Plan)
		if err != nil {
			return b.settle(pending, convergence.DeliveryUnknown, err)
		}
		proofHash, err := domainAbsenceProofDigest(proof)
		if err != nil {
			return b.settle(pending, convergence.DeliveryUnknown, err)
		}
		proofJSON, err := json.Marshal(proof)
		if err != nil {
			return b.settle(pending, convergence.DeliveryUnknown, err)
		}
		boot, uptime, err := b.cutoverClock()
		if err != nil {
			return b.settle(pending, convergence.DeliveryUnknown, err)
		}
		if err := b.catalog.RecordFileProviderCutoverProof(
			ctx, operation, planHash, proofHash, proofJSON, boot, uptime,
		); err != nil {
			return b.settle(pending, convergence.DeliveryUnknown, err)
		}
		if pending.done != nil {
			pending.done <- brokerOutcome{delivery: convergence.DeliveryAccepted, proof: &proof}
		}
		return nil
	default:
		return errors.New("catalog service: unknown runtime broker result")
	}
	return b.settle(pending, convergence.DeliveryAccepted, nil)
}

func (b *RuntimeBroker) peerProof(identity Identity) catalogproto.BrokerPeerProof {
	peer := identity.Peer
	auditDigest := sha256.Sum256(peer.Audit)
	return catalogproto.BrokerPeerProof{
		BrokerProductBuild: b.identity.ProductBuild,
		BrokerPID:          int64(peer.PID), BrokerUID: int64(peer.UID), BrokerStartTime: peer.StartTime,
		BrokerBoot: peer.Boot, BrokerComm: peer.Comm, BrokerExecutable: peer.Executable,
		BrokerDesignatedRequirement:       b.identity.DesignatedRequirement,
		BrokerAuditTokenDigest:            hex.EncodeToString(auditDigest[:]),
		BrokerEntitlementValidationDigest: hex.EncodeToString(b.identity.EntitlementValidationDigest[:]),
	}
}

func domainAbsenceProofDigest(proof catalogproto.DomainAbsenceProof) ([32]byte, error) {
	if err := catalogproto.Validate(proof); err != nil {
		return [32]byte{}, err
	}
	encoded, err := json.Marshal(proof)
	if err != nil {
		return [32]byte{}, fmt.Errorf("catalog service: encode domain absence proof: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func (b *RuntimeBroker) settle(pending brokerPending, delivery convergence.Delivery, err error) error {
	if pending.done != nil {
		pending.done <- brokerOutcome{delivery: delivery, err: err}
	}
	return err
}

func (b *RuntimeBroker) reconcile(ctx context.Context, session *runtimeBrokerSession, actual []catalogproto.RegisteredDomain) error {
	_, cutoverActive, err := b.catalog.FileProviderCutoverMode(ctx)
	if err != nil {
		return err
	}
	desired, err := b.catalog.FileProviderDomains(ctx)
	if err != nil {
		return err
	}
	desiredByID := make(map[catalogproto.DomainID]catalog.FileProviderDomain, len(desired))
	for _, domain := range desired {
		desiredByID[catalogproto.DomainID(domain.DomainID)] = domain
	}
	removals, err := b.catalog.FileProviderDomainRemovals(ctx)
	if err != nil {
		return err
	}
	removingDesired := make(map[catalogproto.DomainID]bool, len(removals))
	for _, removal := range removals {
		removingDesired[catalogproto.DomainID(removal.Domain.DomainID)] = true
	}
	actualByID := make(map[catalogproto.DomainID]catalogproto.RegisteredDomain, len(actual))
	blockedRemovals := make(map[catalog.TenantID]bool, len(removals))
	for _, domain := range actual {
		actualByID[domain.DomainID] = domain
		var exactRemoval *catalog.FileProviderDomainRemoval
		removing := false
		for index := range removals {
			removal := &removals[index]
			if !registeredDomainMatchesRemoval(domain, *removal) {
				continue
			}
			removing = true
			blockedRemovals[removal.Domain.Tenant] = true
			converted, convertErr := catalogDomain(domain)
			if convertErr == nil && sameDomainIdentity(removal.Domain, converted) {
				exactRemoval = removal
			}
		}
		if removing {
			if exactRemoval != nil {
				if err := b.enqueueRemoval(ctx, session, *exactRemoval); err != nil {
					return err
				}
				continue
			}
			id := domain.DomainID
			if err := b.enqueue(ctx, session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindRemoveDomain, DomainID: &id}, nil); err != nil {
				return err
			}
			continue
		}
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
	for _, removal := range removals {
		if blockedRemovals[removal.Domain.Tenant] {
			continue
		}
		if err := b.catalog.ConfirmFileProviderDomainRemoval(ctx, removal); err != nil {
			return err
		}
		b.signalChanged()
	}
	for id, domain := range desiredByID {
		if cutoverActive {
			break
		}
		if removingDesired[id] {
			continue
		}
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

func domainCutoverPlanHash(plan catalogproto.DomainCutoverPlan) ([32]byte, error) {
	return domainCutoverRecoveryKeyHash(domainCutoverRecoveryKey(plan))
}

func domainCutoverRecoveryKey(plan catalogproto.DomainCutoverPlan) catalogproto.DomainCutoverRecoveryKey {
	accounts := make([]catalogproto.DomainCutoverRecoveryAccount, len(plan.Accounts))
	for index, account := range plan.Accounts {
		accounts[index] = catalogproto.DomainCutoverRecoveryAccount{
			AccountID: account.AccountID, ImmutableIdentity: account.ImmutableIdentity,
		}
	}
	return catalogproto.DomainCutoverRecoveryKey{OwnerID: plan.OwnerID, Accounts: accounts}
}

func domainCutoverRecoveryKeyHash(key catalogproto.DomainCutoverRecoveryKey) ([32]byte, error) {
	if err := catalogproto.Validate(key); err != nil {
		return [32]byte{}, err
	}
	payload, err := json.Marshal(key)
	if err != nil {
		return [32]byte{}, fmt.Errorf("catalog service: encode File Provider cutover recovery key: %w", err)
	}
	return sha256.Sum256(payload), nil
}

func (b *RuntimeBroker) cutoverClock() (string, time.Duration, error) {
	boot, err := b.boot()
	if err != nil {
		return "", 0, fmt.Errorf("catalog service: read cutover boot identity: %w", err)
	}
	uptime, err := b.uptime()
	if err != nil {
		return "", 0, fmt.Errorf("catalog service: read cutover monotonic uptime: %w", err)
	}
	if boot == "" || uptime < 0 {
		return "", 0, errors.New("catalog service: cutover clock identity is invalid")
	}
	return boot, uptime, nil
}

func (b *RuntimeBroker) requestReconcile(ctx context.Context) {
	b.mu.Lock()
	session := b.active
	b.mu.Unlock()
	if session != nil {
		_ = b.enqueue(ctx, session, catalogproto.BrokerCommand{Kind: catalogproto.BrokerCommandKindListDomains}, nil)
	}
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
			pending.done <- brokerOutcome{delivery: convergence.DeliveryUnknown, err: errBrokerSessionLost}
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
	ready    chan struct{}
	once     sync.Once
	identity Identity
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

func registeredDomainMatchesRemoval(domain catalogproto.RegisteredDomain, removal catalog.FileProviderDomainRemoval) bool {
	return domain.DomainID == catalogproto.DomainID(removal.Domain.DomainID) ||
		domain.TenantID == catalogproto.TenantID(removal.Domain.Tenant) ||
		(domain.OwnerID == catalogproto.OwnerID(removal.Domain.OwnerID) &&
			domain.AccountInstanceID == catalogproto.AccountInstanceID(removal.Domain.AccountInstance))
}

func sameDomainCutoverPlan(left, right catalogproto.DomainCutoverPlan) bool {
	if left.OperationID != right.OperationID || left.OwnerID != right.OwnerID || len(left.Accounts) != len(right.Accounts) {
		return false
	}
	for index := range left.Accounts {
		l, r := left.Accounts[index], right.Accounts[index]
		if l.AccountID != r.AccountID || l.ImmutableIdentity != r.ImmutableIdentity || l.LegacyDomainID != r.LegacyDomainID {
			return false
		}
		if l.AccountInstanceID == nil || r.AccountInstanceID == nil {
			if l.AccountInstanceID != nil || r.AccountInstanceID != nil {
				return false
			}
		} else if *l.AccountInstanceID != *r.AccountInstanceID {
			return false
		}
	}
	return true
}

func sameDomainCutoverRecoveryKey(left, right catalogproto.DomainCutoverRecoveryKey) bool {
	if left.OwnerID != right.OwnerID || len(left.Accounts) != len(right.Accounts) {
		return false
	}
	for index := range left.Accounts {
		if left.Accounts[index] != right.Accounts[index] {
			return false
		}
	}
	return true
}

var _ BrokerService = (*RuntimeBroker)(nil)
var _ convergence.Notifier = (*RuntimeBroker)(nil)
