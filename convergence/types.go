// Package convergence coordinates demand-aware external view refreshes.
package convergence

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/causal"
)

const (
	// MaxAwaiting bounds fleet-wide notifications whose acknowledgements are outstanding.
	MaxAwaiting = 2
	// AckTimeout quarantines a domain that does not acknowledge a notification.
	AckTimeout = 30 * time.Second
	// QuarantineBackoff spaces an explicit recovery attempt after an acknowledgement timeout.
	QuarantineBackoff = 30 * time.Second
	// MaxAppliedChanges bounds the durable source-change deduplication journal.
	MaxAppliedChanges = 256
)

var (
	// ErrClosed means the engine no longer accepts work.
	ErrClosed = errors.New("convergence: engine closed")
	// ErrInvalidChange means a change set violates the exact input contract.
	ErrInvalidChange = errors.New("convergence: invalid change")
	// ErrInvalidResolution means the resolver returned inconsistent tenant data.
	ErrInvalidResolution = errors.New("convergence: invalid resolution")
	// ErrUnexpectedAck means no notification can be settled by the acknowledgement.
	ErrUnexpectedAck = errors.New("convergence: unexpected acknowledgement")
	// ErrQuarantined means a requested revision timed out without acknowledgement.
	ErrQuarantined = errors.New("convergence: domain quarantined")
)

// TenantID identifies one logical tenant without importing a catalog model.
type TenantID = causal.TenantID

// DomainID identifies one external domain.
type DomainID = causal.DomainID

// Generation identifies one exact tenant/domain incarnation.
type Generation = causal.Generation

// LogicalKey identifies one source key whose change can affect effective content.
type LogicalKey = causal.LogicalKey

// Revision identifies source or engine revisions within their named fields.
type Revision = causal.Revision

// CatalogRevision identifies a tenant-local immutable catalog revision.
type CatalogRevision = causal.CatalogRevision

// ChangeID is the source_change_id of one published source revision.
type ChangeID = causal.ChangeID

// OperationID identifies the operation that produced or observed a source change.
type OperationID = causal.OperationID

// NewChangeID returns a cryptographically random source-change identity.
func NewChangeID() (ChangeID, error) {
	var id ChangeID
	if _, err := rand.Read(id[:]); err != nil {
		return ChangeID{}, fmt.Errorf("convergence: mint change id: %w", err)
	}
	return id, nil
}

// NewOperationID returns a cryptographically random operation identity.
func NewOperationID() (OperationID, error) {
	var id OperationID
	if _, err := rand.Read(id[:]); err != nil {
		return OperationID{}, fmt.Errorf("convergence: mint operation id: %w", err)
	}
	return id, nil
}

// Cause classifies the known source of effective-content convergence work.
type Cause = causal.Cause

const (
	// CauseProviderMutation is a write already acknowledged by its originating domain.
	CauseProviderMutation = causal.CauseProviderMutation
	// CauseDaemonWrite is a source mutation initiated by the owning daemon.
	CauseDaemonWrite = causal.CauseDaemonWrite
	// CauseExternalUnattributed is an observed external change with no guessed writer identity.
	CauseExternalUnattributed = causal.CauseExternalUnattributed
	// CauseMigration is a source mutation produced by a state migration.
	CauseMigration = causal.CauseMigration
	// CauseOnDemand is an engine-generated recovery or preparation attempt.
	CauseOnDemand = causal.CauseOnDemand
)

// ChangeSet is the complete causal contract for one published source change.
type ChangeSet = causal.ChangeSet

// EffectiveValue is one named input to a tenant's effective fingerprint.
type EffectiveValue struct {
	Key   LogicalKey
	Bytes []byte
}

// Resolution is the resolver's complete view of one affected registered tenant.
type Resolution struct {
	Tenant                TenantID
	Domain                DomainID
	Generation            Generation
	SourceRevision        Revision
	CatalogRevision       CatalogRevision
	Registered            bool
	LiveLeases            uint32
	MaterializedInterests uint32
	Effective             []EffectiveValue
}

// Resolver supplies affected content and demand without exposing its storage model.
type Resolver interface {
	ResolveAffected(ctx context.Context, change ChangeSet) ([]Resolution, error)
	ResolveTenant(ctx context.Context, tenant TenantID) (Resolution, error)
}

// Fingerprint is a deterministic digest of one tenant's effective bytes.
type Fingerprint [32]byte

// Delivery says whether Notify may have reached the external domain.
type Delivery uint8

const (
	// DeliveryNotSent proves no notification side effect occurred.
	DeliveryNotSent Delivery = iota + 1
	// DeliveryAccepted proves the notification was accepted for delivery.
	DeliveryAccepted
	// DeliveryUnknown means the side effect may have occurred and must never be replayed.
	DeliveryUnknown
)

// Notification requests one exact domain revision with its unmodified causal metadata.
type Notification struct {
	SourceRevision   Revision
	CatalogRevision  CatalogRevision
	ChangeID         ChangeID
	OperationID      OperationID
	Cause            Cause
	Origin           DomainID
	OriginGeneration Generation
	AffectedKeys     []LogicalKey
	Tenant           TenantID
	Domain           DomainID
	Generation       Generation
	Revision         Revision
	Fingerprint      Fingerprint
}

// Notifier signals external domains and reports an exact delivery outcome.
type Notifier interface {
	Notify(ctx context.Context, notification Notification) (Delivery, error)
}

// Clock owns all acknowledgement and quarantine timing.
type Clock interface {
	Now() time.Time
	After(delay time.Duration) <-chan time.Time
}

// Persistence is the narrow durable-state seam implemented by the catalog owner.
type Persistence interface {
	Load(ctx context.Context) (State, error)
	Save(ctx context.Context, state State) error
	ClaimOutbox(ctx context.Context) (*causal.OutboxBatch, error)
	SettleOutbox(ctx context.Context, change causal.ChangeID) error
}

// Pending is a durably reserved notification awaiting acknowledgement.
type Pending struct {
	Notification Notification
	SentAt       time.Time
}

// Quarantine retains the exact timed-out notification during its recovery backoff.
type Quarantine struct {
	Notification Notification
	Since        time.Time
	Until        time.Time
}

// DomainState is the durable convergence record for one domain.
type DomainState struct {
	Tenant                  TenantID
	Domain                  DomainID
	Generation              Generation
	Fingerprint             Fingerprint
	ResolvedSourceRevision  Revision
	CatalogRevision         CatalogRevision
	NotifiedCatalogRevision CatalogRevision
	ObservedCatalogRevision CatalogRevision
	Desired                 Revision
	Notified                Revision
	Observed                Revision
	DesiredChange           ChangeSet
	NotifiedChange          ChangeSet
	ObservedChange          ChangeSet
	Demanded                bool
	Forced                  bool
	Pending                 *Pending
	Quarantine              *Quarantine
}

// Stale reports logical staleness even when the domain is inactive and unnotified.
func (s DomainState) Stale() bool { return s.Desired > s.Observed }

// AppliedChange is one bounded durable deduplication-journal entry.
type AppliedChange struct {
	Change         ChangeSet
	EngineRevision Revision
}

// State is the complete durable engine state.
type State struct {
	Revision   Revision
	SourceHead Revision
	DedupFloor Revision
	Domains    map[DomainID]DomainState
	Changes    map[ChangeID]AppliedChange
}

// Preparation identifies the minimum domain revision a caller needs observed.
type Preparation struct {
	Tenant          TenantID
	Domain          DomainID
	Generation      Generation
	Revision        Revision
	SourceRevision  Revision
	CatalogRevision CatalogRevision
	ChangeID        ChangeID
	OperationID     OperationID
}

// ObservationProof proves that a domain observed an exact or newer revision.
type ObservationProof struct {
	Requested      Preparation
	Observed       Preparation
	ObservedChange ChangeSet
}

// QuarantineError reports the timed-out delivery that failed a preparation waiter.
type QuarantineError struct {
	Domain   DomainID
	Revision Revision
	Until    time.Time
}

// Error implements error.
func (e *QuarantineError) Error() string {
	return fmt.Sprintf("convergence: domain %s revision %d quarantined until %s", e.Domain, e.Revision, e.Until.Format(time.RFC3339Nano))
}

// Unwrap makes QuarantineError match ErrQuarantined.
func (e *QuarantineError) Unwrap() error { return ErrQuarantined }

// Ack settles one exact domain notification.
type Ack struct {
	Domain          DomainID
	Generation      Generation
	Revision        Revision
	SourceRevision  Revision
	CatalogRevision CatalogRevision
	ChangeID        ChangeID
	OperationID     OperationID
}

// Config contains the engine's four narrow external seams.
type Config struct {
	Resolver    Resolver
	Notifier    Notifier
	Clock       Clock
	Persistence Persistence
}
