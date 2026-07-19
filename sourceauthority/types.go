// Package sourceauthority owns durable recursive source observation and
// predecessor-fenced publication into the FuseKit catalog.
package sourceauthority

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/tenant"
)

// RetryClock schedules actor-owned reconciliation retries.
type RetryClock interface {
	After(time.Duration) <-chan time.Time
}

// OperationDeadlines bounds every supervised filesystem operation independently
// of caller lifecycle cancellation.
type OperationDeadlines struct {
	Unary           time.Duration
	Scan            time.Duration
	Materialize     time.Duration
	Mutation        time.Duration
	ObserverControl time.Duration
}

// StandardOperationDeadlines returns the FuseKit-owned worker deadline policy.
func StandardOperationDeadlines() OperationDeadlines {
	return OperationDeadlines{
		Unary: 30 * time.Second, Scan: 5 * time.Minute, Materialize: 5 * time.Minute,
		Mutation: 5 * time.Minute, ObserverControl: 30 * time.Second,
	}
}

func (d OperationDeadlines) validate() error {
	if d.Unary <= 0 || d.Scan <= 0 || d.Materialize <= 0 || d.Mutation <= 0 || d.ObserverControl <= 0 {
		return errors.New("sourceauthority: every operation deadline must be positive")
	}
	return nil
}

var (
	// ErrClosed means the authority runtime no longer accepts work.
	ErrClosed = errors.New("sourceauthority: runtime closed")
	// ErrInvalidEvent means an event violates its stream, cursor, root, or path fence.
	ErrInvalidEvent = errors.New("sourceauthority: invalid path event")
	// ErrInvalidPlan means policy returned work outside the supplied source fence.
	ErrInvalidPlan = errors.New("sourceauthority: invalid policy plan")
	// ErrSourceChanged means a named physical object changed during materialization.
	ErrSourceChanged = errors.New("sourceauthority: source changed during materialization")
	// ErrSnapshotRequired means incremental observation cannot continue from the durable baseline.
	ErrSnapshotRequired = errors.New("sourceauthority: authoritative snapshot required")
	// ErrQuarantined means durable observer state failed closed or repeated repair could not settle.
	ErrQuarantined = errors.New("sourceauthority: authority quarantined")
	// ErrMutationLocator means a source mutation locator is absent, stale, or outside this authority.
	ErrMutationLocator = errors.New("sourceauthority: mutation locator is invalid")

	// ErrMutationNotArmed means physical effects are durable but their catalog commit is not yet visible.
	ErrMutationNotArmed = errors.New("sourceauthority: mutation echo is not armed")
)

// RootID is an opaque policy-stable source-root identity.
type RootID string

// StreamIdentity identifies one persistent path-event stream.
type StreamIdentity string

// EventID is one backend-native resume position. Values may leap.
type EventID uint64

// EventOrdinal orders events sharing one backend-native event ID.
type EventOrdinal uint32

// InboxSequence is the authority's contiguous durable work sequence.
type InboxSequence uint64

// RootEpoch identifies the exact volume and stable root identities observed by a stream.
type RootEpoch string

// LogicalID is a product-policy role; FuseKit maps it to an opaque source key.
type LogicalID string

// Fingerprint is a complete stable digest of physical metadata or effective content.
type Fingerprint [32]byte

// RootSpec declares one exact physical root owned by an authority generation.
type RootSpec struct {
	Authority        causal.SourceAuthorityID
	ID               RootID
	Path             string
	Kind             RootKind
	Generation       uint64
	ExpectedIdentity FileIdentity
}

// RootKind distinguishes exact-file roots from recursively observed directories.
type RootKind uint8

const (
	RootFile RootKind = iota + 1
	RootDirectory
)

// EventKind is one backend-supplied path fact. Identity and rename pairing are
// deliberately absent; the actor derives them by stat against its index.
type EventKind uint8

const (
	EventCreated EventKind = iota + 1
	EventModified
	EventRemoved
	EventRenamed
	EventMetadata
)

// BatchFlags are backend discontinuity facts that require snapshot repair.
type BatchFlags uint32

const (
	FlagMustScanSubDirs BatchFlags = 1 << iota
	FlagUserDropped
	FlagKernelDropped
	FlagEventIDsWrapped
	FlagRootChanged
)

// RequiresSnapshot reports whether the backend proved an incremental discontinuity.
func (f BatchFlags) RequiresSnapshot() bool {
	return f&(FlagMustScanSubDirs|FlagUserDropped|FlagKernelDropped|FlagEventIDsWrapped|FlagRootChanged) != 0
}

// PathEvent is one physical path fact emitted by the backend.
type PathEvent struct {
	Root     RootID
	Relative string
	Kind     EventKind
	Flags    BatchFlags
	ID       EventID
	Ordinal  EventOrdinal
}

// EventBatch is one immutable continuous backend cursor range.
type EventBatch struct {
	Stream      StreamIdentity
	Predecessor EventID
	Cursor      EventID
	RootEpoch   RootEpoch
	Events      []PathEvent
}

// StreamCheckpoint is the exact durable position offered when opening a backend.
type StreamCheckpoint struct {
	Identity  StreamIdentity
	Cursor    EventID
	RootEpoch RootEpoch
}

// EventFence proves every event through each native cursor has been delivered
// and Inbox is the corresponding durable authority sequence.
type EventFence struct {
	Streams []StreamCheckpoint
	Inbox   InboxSequence
}

// DurableEventSink commits one deep-copied native batch before its callback returns.
type DurableEventSink func(context.Context, EventBatch) error

// EventBackend opens one paused supervised observer child over the complete root set.
type EventBackend interface {
	Open(context.Context, []RootSpec, []StreamCheckpoint, DurableEventSink) (EventStream, error)
}

// Executor is the only filesystem/process boundary used by Runtime. Every
// finite method returns only after its supervised signed worker is settled or
// TERM/KILLed and reaped. EventStream control methods provide the same proof
// for the long-lived observer child. Materialize streams child output through
// ContentSource, whose Close settles the producing operation.
type Executor interface {
	EventBackend
	PathSource
	Materialize(context.Context, MaterializationTask) (Materialization, error)
	ApplyMutation(context.Context, MutationTask) (MutationReceipt, error)
	InspectMutation(context.Context, MutationInspectionRequest) (MutationInspection, error)
	AcknowledgeMutation(context.Context, causal.SourceAuthorityID, causal.Generation, catalog.MutationID, Fingerprint) error
	AbandonMutation(context.Context, causal.SourceAuthorityID, causal.Generation, catalog.MutationID) error
	MutationTerminalProofPage(context.Context, causal.SourceAuthorityID, catalog.MutationID, int) (MutationTerminalProofPage, error)
	ForgetMutation(context.Context, causal.SourceAuthorityID, MutationTerminalProof) error
	Close() error
}

// EventStream supplies activation and explicit flush fences. Activate is the
// only operation that may begin sink callbacks. Flush returns only after every
// callback through its fence has durably returned, and Close joins callbacks.
type EventStream interface {
	Checkpoints() []StreamCheckpoint
	Activate(context.Context) error
	Flush(context.Context) ([]StreamCheckpoint, error)
	Close() error
}

// PhysicalKind classifies the actor's stat result without product semantics.
type PhysicalKind uint8

const (
	PhysicalFile PhysicalKind = iota + 1
	PhysicalDirectory
	PhysicalSymlink
)

// FileIdentity is obtained by actor-owned stat, never fabricated by path events.
type FileIdentity struct {
	VolumeUUID    string
	Inode         uint64
	BirthtimeSec  int64
	BirthtimeNsec int64
}

// PhysicalEntry is one stable-probeable physical index value.
type PhysicalEntry struct {
	Root                RootID
	Relative            string
	Exists              bool
	Kind                PhysicalKind
	Identity            FileIdentity
	Mode                uint32
	UID                 uint32
	GID                 uint32
	Size                int64
	LinkTarget          string
	MetadataFingerprint Fingerprint
	ContentFingerprint  Fingerprint
}

// ScanCursor is an opaque authoritative scan continuation.
type ScanCursor string

// ScanPage is one bounded page of an authoritative root-set scan.
type ScanPage struct {
	Entries []PhysicalEntry
	Next    ScanCursor
}

// PathSource performs actor-owned stat and bounded scans. RootFile accepts only
// relative "." and scans at most that object; RootDirectory recursively scans descendants.
type PathSource interface {
	RootIdentity(context.Context, RootSpec) (FileIdentity, error)
	Stat(context.Context, RootSpec, string) (PhysicalEntry, error)
	BeginScan(context.Context, []RootSpec) (ScanSession, error)
}

// ScanSession owns one immutable full-source scan until explicitly closed.
type ScanSession interface {
	Next(context.Context, int) (ScanPage, error)
	Close() error
}

// TenantFence is one exact desired authority tenant generation.
type TenantFence struct {
	Tenant     catalog.TenantID
	Generation catalog.Generation
}

// Fence binds policy output to one exact authority, root set, fleet, and cursor.
type Fence struct {
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	Streams             []StreamCheckpoint
	Inbox               InboxSequence
	RootDigest          Fingerprint
	FleetDigest         Fingerprint
}

// IndexedEntry is the durable physical locator and opaque logical binding view.
type IndexedEntry struct {
	Physical PhysicalEntry
	Logical  []LogicalID
}

// IndexView is an immutable authority-index view supplied to policy.
type IndexView interface {
	Fence() Fence
	Roots() []RootSpec
	Tenants() []tenant.TenantSpec
	Entry(RootID, string) (IndexedEntry, bool)
}

// SnapshotView pages a fully scanned candidate held behind a stream fence.
type SnapshotView interface {
	Fence() Fence
	Roots() []RootSpec
	Tenants() []tenant.TenantSpec
	Scan(context.Context, ScanCursor, int) (ScanPage, error)
}

// PathRef identifies one exact actor-owned physical read.
type PathRef struct {
	Root     RootID
	Relative string
}

// MaterializationRequest names one logical value, every physical input it may
// read, and immutable product-policy data interpreted only by the authority's
// registered child materializer.
type MaterializationRequest struct {
	Logical LogicalID
	Inputs  []PathRef
	Payload []byte
}

// MaterializationTask is the complete serializable child invocation. Expected
// is positionally aligned with Request.Inputs and records the actor's exact
// pre-execution stat fence.
type MaterializationTask struct {
	Fence    Fence
	Roots    []RootSpec
	Tenants  []tenant.TenantSpec
	Request  MaterializationRequest
	Expected []PhysicalEntry
}

// ImmutableContent is one pinned, immutable input snapshot supplied by FuseKit.
type ImmutableContent interface {
	Open(context.Context) (io.ReadCloser, error)
}

// MaterializerInput contains one verified input without exposing a source path.
type MaterializerInput struct {
	Physical PhysicalEntry
	Content  ImmutableContent
}

// MaterializerTask is the only source data visible to product materialization policy.
type MaterializerTask struct {
	Fence   Fence
	Tenants []tenant.TenantSpec
	Logical LogicalID
	Payload []byte
	Inputs  []MaterializerInput
}

// TenantRoot binds one desired tenant generation to an opaque authority root role.
type TenantRoot struct {
	Tenant     catalog.TenantID
	Generation catalog.Generation
	Logical    LogicalID
}

// Projection is one complete tenant object value produced from a logical materialization.
type Projection struct {
	Tenant     catalog.TenantID
	Generation catalog.Generation
	Parent     LogicalID
	Name       string
	Kind       catalog.Kind
	Mode       uint32
	Content    ContentSource
	LinkTarget string
	Visibility catalog.Visibility
}

// ContentSource opens one fresh bounded-memory stream for a file projection.
// Runtime closes every successfully opened stream, including after read errors.
type ContentSource interface {
	Open(context.Context) (contentstream.Source, error)
	Close() error
}

// Materialization is one complete logical value. Fingerprint covers every projection.
type Materialization struct {
	Logical     LogicalID
	Fingerprint Fingerprint
	Objects     []Projection
}

// Delete names one logical identity and the exact tenant generations that lose it.
type Delete struct {
	Logical LogicalID
	Tenants []TenantFence
}

// DeltaPlan is policy's complete work for one continuous event batch.
type DeltaPlan struct {
	Fence        Fence
	AffectedKeys []causal.LogicalKey
	Roots        []TenantRoot
	Reads        []MaterializationRequest
	Deletes      []Delete
}

// PhysicalBinding is one exact physical input of a logical source identity.
type PhysicalBinding struct {
	Physical PhysicalEntry
	Root     RootSpec
}

// PhysicalLocator resolves one catalog source key to every indexed physical input.
type PhysicalLocator struct {
	Source   catalog.SourceLocator
	Logical  LogicalID
	Bindings []PhysicalBinding
}

// ExpectedPhysicalState is the complete correlatable state of one mutation path.
type ExpectedPhysicalState struct {
	Exists              bool
	Kind                PhysicalKind
	Identity            FileIdentity
	Mode                uint32
	UID                 uint32
	GID                 uint32
	Size                int64
	LinkTarget          string
	MetadataFingerprint Fingerprint
	ContentFingerprint  Fingerprint
}

// MutationOutcome is policy's semantic expectation; runtime owns exact identity and fingerprints.
type MutationOutcome uint8

const (
	MutationAbsent MutationOutcome = iota + 1
	MutationPresent
)

// ExpectedEffect is one exact precondition and semantic outcome of a provider mutation.
type ExpectedEffect struct {
	Path    PathRef
	Before  ExpectedPhysicalState
	Outcome MutationOutcome
	Kind    PhysicalKind
}

// MutationRequest contains catalog-fenced locators resolved through the authority index.
type MutationRequest struct {
	Step   tenant.SourceMutationStep
	Object *PhysicalLocator
	Parent *PhysicalLocator
	Target *PhysicalLocator
}

// MutationPlan is durably retained before its worker is returned. Runtime
// records exact post-state after worker success before correlating path events.
type MutationPlan struct {
	Program MutationProgram
	Effects []ExpectedEffect
}

// MutationActionKind identifies one closed, FuseKit-executed filesystem primitive.
type MutationActionKind uint8

const (
	MutationAtomicWriteFile MutationActionKind = iota + 1
	MutationCreateDirectory
	MutationCreateSymlink
	MutationRemove
	MutationRename
)

// MutationAction is one path-safe primitive in a semantic mutation program.
type MutationAction struct {
	Kind              MutationActionKind
	Path              PathRef
	From              *PathRef
	Mode              uint32
	LinkTarget        string
	UseRequestContent bool
	Data              []byte
}

// MutationProgram is a closed sequence executed only by FuseKit's source-task child.
type MutationProgram struct {
	Actions []MutationAction
}

// MutationContent streams request-owned bytes without exposing catalog paths.
type MutationContent interface {
	Open(context.Context) (contentstream.Source, error)
	Close() error
}

// MutationTask is the exact fenced program sent to the supervised source-task child.
type MutationTask struct {
	Fence             Fence
	Roots             []RootSpec
	OperationID       catalog.MutationID
	ExpectationDigest Fingerprint
	Program           MutationProgram
	Expected          []ExpectedEffect
	Content           MutationContent
}

// MutationReceipt is the child-attested post-state of one semantic program.
type MutationReceipt struct {
	OperationID catalog.MutationID
	Effects     []PhysicalEntry
	Digest      Fingerprint
}

// MutationInspectionRequest binds one exact durable worker operation to its
// catalog-persisted expectation identity.
type MutationInspectionRequest struct {
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	Operation           catalog.MutationID
	ExpectationDigest   Fingerprint
}

// MutationInspectionState classifies one exact durable worker operation.
type MutationInspectionState uint8

const (
	MutationInspectionNotFound MutationInspectionState = iota + 1
	MutationInspectionActiveUnapplied
	MutationInspectionApplied
	MutationInspectionTerminal
	MutationInspectionConsumed
)

// MutationContentDigest is the bounded digest of request-owned content.
type MutationContentDigest struct {
	Size   int64
	Digest Fingerprint
}

// MutationInspection is one exact read-only journal result. It is obtained
// without probing source roots or opening request content.
type MutationInspection struct {
	State             MutationInspectionState
	ExpectationDigest Fingerprint
	Intent            Fingerprint
	RequestContent    *MutationContentDigest
	Receipt           *MutationReceipt
	Terminal          *MutationTerminalProof
}

// MutationCleanupOutcome distinguishes receipt acknowledgement from repaired abandonment.
type MutationCleanupOutcome uint8

const (
	MutationAcknowledged MutationCleanupOutcome = iota + 1
	MutationAbandoned
)

// MutationTerminalProof is the durable exact cleanup handshake retained until
// the catalog expectation is no longer visible.
type MutationTerminalProof struct {
	Authority           causal.SourceAuthorityID
	AuthorityGeneration causal.Generation
	Operation           catalog.MutationID
	Outcome             MutationCleanupOutcome
	Digest              Fingerprint
}

// MutationTerminalProofPageLimit is the fixed maximum orphan-proof discovery page.
const MutationTerminalProofPageLimit = 16

// MutationTerminalProofPage is one stable authority-wide page ordered by operation ID.
type MutationTerminalProofPage struct {
	Proofs []MutationTerminalProof
	Next   catalog.MutationID
	More   bool
}

// Policy supplies only product mapping and effective-content behavior.
type Policy interface {
	Roots(context.Context, []tenant.TenantSpec) ([]RootSpec, error)
	PlanDelta(context.Context, IndexView, EventBatch) (DeltaPlan, error)
	PlanSnapshot(context.Context, SnapshotView, SnapshotPlanCursor, int) (SnapshotPlanPage, error)
	PlanMutation(context.Context, MutationRequest) (MutationPlan, error)
}

// Materializer is implemented only by the fixed signed consumer executable's
// child dispatcher. Unknown authority registrations are rejected before any
// filesystem input is opened.
type Materializer interface {
	Materialize(context.Context, MaterializerTask) (Materialization, error)
}

// AuthorityPolicy is the consumer's complete authority definition. Runtime
// receives only its Policy half; holder child dispatch receives only its
// Materializer half.
type AuthorityPolicy interface {
	Policy
	Materializer
}

// BarrierResult proves the observer cursor and latest applicable tenant commit.
type BarrierResult struct {
	Fence  EventFence
	Target catalog.ConvergenceTarget
}

// Barrier flushes observer intake and returns the latest catalog-applicable commit.
type Barrier interface {
	Barrier(context.Context, catalog.TenantID, catalog.Generation) (BarrierResult, error)
}
