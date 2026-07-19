// Package sourcedriver defines the lifecycle-neutral contract between FuseKit
// and one authoritative product source.
package sourcedriver

import (
	"context"
	"crypto/sha256"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
)

const (
	// RevisionTokenMaxBytes bounds one opaque immutable source revision.
	RevisionTokenMaxBytes = 255
	// LogicalIDMaxBytes bounds one catalog-key-compatible projection identity.
	LogicalIDMaxBytes = 255
	// MaxPageItems bounds every source snapshot and delta page.
	MaxPageItems = 256
	// MaxPageBytes bounds the canonical form of every source page.
	MaxPageBytes = 256 << 10
	// MaxContentBytes bounds one streamed source object or mutation body.
	MaxContentBytes int64 = 1 << 30
	// MaxContinuationBytes bounds one opaque immutable driver continuation.
	MaxContinuationBytes = 2 << 10
	// MaxTargets bounds one immutable authority target declaration.
	MaxTargets = 10_000
	// MaxTargetPageItems bounds one immutable target-set declaration page.
	MaxTargetPageItems = 128
)

// RevisionToken is an opaque immutable revision issued by a source driver.
type RevisionToken string

// LogicalID is one stable authority-neutral, catalog-key-compatible projection identity.
type LogicalID string

// TargetDeclaration is one exact tenant projection in an authority request.
type TargetDeclaration struct {
	Tenant     catalog.TenantID  `json:"tenant"`
	Generation causal.Generation `json:"generation"`
}

// TargetSetID is one deterministic immutable target declaration identity.
type TargetSetID [16]byte

// TargetSetRef is the complete O(1) fence carried by source page requests.
type TargetSetRef struct {
	ID                  TargetSetID
	AuthorityGeneration causal.Generation
	TargetEpoch         uint64
	DeclarationDigest   [sha256.Size]byte
	TargetCount         uint64
	TargetsDigest       [sha256.Size]byte
}

// TargetSetState is the exact resumable declaration acknowledgement.
type TargetSetState struct {
	Ref            TargetSetRef
	NextPage       uint32
	DeclaredCount  uint64
	After          TargetDeclaration
	RollingDigest  [sha256.Size]byte
	LastPageDigest [sha256.Size]byte
	Complete       bool
}

// TargetSetPage is one strictly ordered immutable declaration page.
type TargetSetPage struct {
	Ref            TargetSetRef
	Sequence       uint32
	Targets        []TargetDeclaration
	Complete       bool
	PreviousDigest [sha256.Size]byte
	Digest         [sha256.Size]byte
	PageDigest     [sha256.Size]byte
}

// Head identifies one exact authoritative source revision.
type Head struct {
	Revision RevisionToken
}

// PageKind identifies one immutable source page shape.
type PageKind uint8

const (
	// PageSnapshot continues an authority snapshot.
	PageSnapshot PageKind = iota + 1
	// PageChanges continues an exact-predecessor delta.
	PageChanges
)

// PagePosition is one complete authority-global page ordering tuple.
type PagePosition struct {
	Tenant     catalog.TenantID
	Generation causal.Generation
	Sequence   uint64
	ID         LogicalID
}

// PageCursor is the exclusive, digest-bound position of one immutable page.
// Snapshot cursors leave From and Sequence empty and bind To to the snapshot.
type PageCursor struct {
	TargetSet       TargetSetRef
	Kind            PageKind
	From            RevisionToken
	To              RevisionToken
	Page            uint32
	Limit           uint32
	AfterTenant     catalog.TenantID
	AfterGeneration causal.Generation
	AfterSequence   uint64
	After           LogicalID
	Continuation    []byte
	PreviousDigest  [sha256.Size]byte
	Digest          [sha256.Size]byte
}

// ContentRef identifies an immutable source object body at one exact revision.
type ContentRef struct {
	Revision   RevisionToken
	Tenant     catalog.TenantID
	Generation causal.Generation
	Object     LogicalID
	Size       int64
	Hash       catalog.ContentHash
}

// Projection is one complete path-independent source object projection.
type Projection struct {
	Tenant     catalog.TenantID
	Generation causal.Generation
	ID         LogicalID
	Parent     LogicalID
	Name       string
	Kind       catalog.Kind
	Mode       uint32
	LinkTarget string
	Visibility catalog.Visibility
	Size       int64
	Hash       catalog.ContentHash
	Content    *ContentRef
}

// SnapshotRequest selects one bounded page from an immutable source revision.
type SnapshotRequest struct {
	TargetSet TargetSetRef
	Revision  RevisionToken
	Cursor    *PageCursor
	Limit     int
}

// SnapshotPage is one strictly target-and-ID-ordered immutable projection page.
type SnapshotPage struct {
	Revision RevisionToken
	Objects  []Projection
	Next     *PageCursor
	Digest   [sha256.Size]byte
}

// ChangeKind identifies one logical source delta.
type ChangeKind uint8

const (
	// ChangeDelete removes the prior projection for ID.
	ChangeDelete ChangeKind = iota + 1
	// ChangeUpsert creates or replaces the complete projection for ID.
	ChangeUpsert
)

// Change is one complete logical source delta.
type Change struct {
	Kind       ChangeKind
	Tenant     catalog.TenantID
	Generation causal.Generation
	Sequence   uint64
	ID         LogicalID
	Object     *Projection
}

// ChangesRequest selects one bounded page between two exact source revisions.
type ChangesRequest struct {
	TargetSet TargetSetRef
	From      RevisionToken
	To        RevisionToken
	Cursor    *PageCursor
	Limit     int
}

// ChangePage is one strictly target-sequence-and-ID-ordered immutable delta page.
type ChangePage struct {
	From    RevisionToken
	To      RevisionToken
	Changes []Change
	Next    *PageCursor
	Digest  [sha256.Size]byte
}

// MutationRequest is one exact-revision, idempotent external source operation.
type MutationRequest struct {
	TargetSet   TargetSetRef
	Tenant      catalog.TenantID
	Generation  causal.Generation
	OperationID catalog.MutationID
	Expected    RevisionToken
	Context     catalog.SourceMutationContext
	HasContent  bool
	ContentSize int64
	ContentHash catalog.ContentHash
}

// MutationState is the durable source-side state of one operation ID.
type MutationState uint8

const (
	// MutationNotFound means the source has no durable record for the operation.
	MutationNotFound MutationState = iota + 1
	// MutationPrepared means the exact request is journaled but not yet applied.
	MutationPrepared
	// MutationApplied means the exact request, committed revision, and recovery
	// view are durable until acknowledgement.
	MutationApplied
)

// MutationReceipt is the exact replayable source-side result for an operation.
type MutationReceipt struct {
	OperationID   catalog.MutationID
	State         MutationState
	RequestDigest [sha256.Size]byte
	Expected      RevisionToken
	Committed     RevisionToken
	Result        LogicalID
	Digest        [sha256.Size]byte
}

// MutationSettlementKind identifies one exact source-side receipt transition.
type MutationSettlementKind uint8

const (
	// MutationSettlementAcknowledge records that an applied receipt reached
	// FuseKit durably and permits releasing its source revision and content pin.
	MutationSettlementAcknowledge MutationSettlementKind = iota + 1
	// MutationSettlementAbandon discards one prepared operation that was not
	// applied while retaining its consumed operation ID tombstone.
	MutationSettlementAbandon
	// MutationSettlementForget removes one previously acknowledged receipt's
	// replay metadata while retaining its consumed operation ID tombstone.
	MutationSettlementForget
)

// MutationSettlement binds one receipt transition to its immutable request and authority generation.
type MutationSettlement struct {
	TargetSet     TargetSetRef
	OperationID   catalog.MutationID
	RequestDigest [32]byte
	ReceiptDigest [32]byte
	Kind          MutationSettlementKind
}

// Driver exposes one authoritative source without lifecycle or catalog access.
// ApplyMutation is exact-idempotent by OperationID: the same request replays
// the same receipt and a different request with that ID fails closed.
//
// After ApplyMutation or InspectMutation returns MutationApplied, the driver
// pins the exact Expected-to-Committed change range, the Committed snapshot for
// every complete target set accepted while the receipt is unacknowledged, and
// every ContentRef emitted by either view. Snapshot, ChangesSince, and
// OpenContent must replay those facts byte- and digest-identically across
// compaction, process restart, and lost responses. A newer head never
// substitutes for the receipt's Committed revision.
//
// A successfully accepted MutationSettlementAcknowledge proves FuseKit has
// committed the view and permits the driver to release its revision and
// content pins. The exact acknowledgement remains idempotently replayable and
// InspectMutation continues to return the receipt without the pinned data.
// MutationSettlementForget removes that request and receipt replay metadata,
// but the operation ID remains durably consumed: InspectMutation returns
// MutationNotFound and every later ApplyMutation with that ID fails closed.
type Driver interface {
	Refresh(context.Context, causal.SourceAuthorityID) (Head, error)
	InspectTargetSet(context.Context, causal.SourceAuthorityID, TargetSetRef) (TargetSetState, error)
	DeclareTargetSet(context.Context, causal.SourceAuthorityID, TargetSetPage) (TargetSetState, error)
	Snapshot(context.Context, causal.SourceAuthorityID, SnapshotRequest) (SnapshotPage, error)
	ChangesSince(context.Context, causal.SourceAuthorityID, ChangesRequest) (ChangePage, error)
	OpenContent(context.Context, causal.SourceAuthorityID, ContentRef) (contentstream.Source, error)
	ApplyMutation(context.Context, causal.SourceAuthorityID, MutationRequest, contentstream.Source) (MutationReceipt, error)
	InspectMutation(context.Context, causal.SourceAuthorityID, catalog.MutationID, [sha256.Size]byte) (MutationReceipt, error)
	SettleMutation(context.Context, causal.SourceAuthorityID, MutationSettlement) error
}
