// Package catalog owns Fusekit filesystem identity, revision, and convergence state.
package catalog

import (
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/causal"
)

// Kind is an object's immutable filesystem kind.
type Kind uint8

const (
	// KindDirectory identifies a directory.
	KindDirectory Kind = iota + 1
	// KindFile identifies a regular file.
	KindFile
	// KindSymlink identifies a symbolic link with an exact catalog target.
	KindSymlink
)

// ChangeKind identifies one ordered namespace delta.
type ChangeKind uint8

const (
	// ChangeDelete removes an object's prior namespace binding.
	ChangeDelete ChangeKind = iota + 1
	// ChangeUpsert creates or updates an object's namespace binding.
	ChangeUpsert
)

// Presentation identifies one filesystem surface over the tenant catalog.
type Presentation uint8

const (
	// PresentationMount is the mounted filesystem view.
	PresentationMount Presentation = iota + 1
	// PresentationFileProvider is the macOS File Provider view.
	PresentationFileProvider
)

// PresentationSet is a nonempty closed set of tenant presentation surfaces.
type PresentationSet uint8

const (
	// PresentMount includes the mounted filesystem surface.
	PresentMount PresentationSet = 1 << iota
	// PresentFileProvider includes the macOS File Provider surface.
	PresentFileProvider
)

// Has reports whether the set includes presentation.
func (s PresentationSet) Has(presentation Presentation) bool {
	switch presentation {
	case PresentationMount:
		return s&PresentMount != 0
	case PresentationFileProvider:
		return s&PresentFileProvider != 0
	default:
		return false
	}
}

func (s PresentationSet) valid() bool {
	return s != 0 && s&^(PresentMount|PresentFileProvider) == 0
}

// Visibility is an object's explicit membership in each presentation.
type Visibility struct {
	Mount        bool
	FileProvider bool
}

// Has reports whether the object belongs to presentation.
func (v Visibility) Has(presentation Presentation) bool {
	switch presentation {
	case PresentationMount:
		return v.Mount
	case PresentationFileProvider:
		return v.FileProvider
	default:
		return false
	}
}

func catalogPresentations() [2]Presentation {
	return [2]Presentation{PresentationMount, PresentationFileProvider}
}

// EnumerationScopeKind identifies one server-paged File Provider view.
type EnumerationScopeKind uint8

const (
	// EnumerationWorkingSet contains durably interested objects and their content/version changes.
	EnumerationWorkingSet EnumerationScopeKind = iota + 1
	// EnumerationContainer contains one directory's immediate structural children.
	EnumerationContainer
)

// EnumerationScope is one closed server-side enumeration view.
type EnumerationScope struct {
	Kind         EnumerationScopeKind
	Presentation Presentation
	Parent       ObjectID
	Domain       causal.DomainID
	Generation   causal.Generation
}

// MutationKind identifies one closed catalog mutation shape.
type MutationKind uint8

const (
	// MutationCreateTenant creates a tenant root.
	MutationCreateTenant MutationKind = iota + 1
	// MutationCreate creates an object.
	MutationCreate
	// MutationRevise appends an object revision.
	MutationRevise
	// MutationDelete tombstones an object.
	MutationDelete
	// MutationReplace atomically replaces a target binding.
	MutationReplace
	// MutationDiscardPrivate removes one unpublished object capability.
	MutationDiscardPrivate
	// MutationPromotePrivate publishes one unpublished object into an unoccupied binding.
	MutationPromotePrivate
)

// CasePolicy selects a tenant's immutable name-equivalence policy.
type CasePolicy uint8

const (
	// CaseSensitive preserves normalized case in lookup keys.
	CaseSensitive CasePolicy = iota + 1
	// CaseInsensitive folds normalized lookup keys.
	CaseInsensitive
)

// TenantAccessMode is the persisted mutation policy for a desired tenant.
type TenantAccessMode uint8

const (
	// TenantReadOnly rejects filesystem mutations.
	TenantReadOnly TenantAccessMode = iota + 1
	// TenantReadWrite permits filesystem mutations.
	TenantReadWrite
)

// ErrNotFound means the requested live catalog record does not exist.
var ErrNotFound = errors.New("catalog: not found")

// ErrConflict means a live namespace binding already exists.
var ErrConflict = errors.New("catalog: namespace conflict")

// ErrInvalidObject means an object specification violates catalog invariants.
var ErrInvalidObject = errors.New("catalog: invalid object")

// ErrInvalidTransition means a convergence or revision transition regressed.
var ErrInvalidTransition = errors.New("catalog: invalid revision transition")

// ErrIntegrity means immutable content does not match its content address.
var ErrIntegrity = errors.New("catalog: content integrity failure")

// ErrMutationConflict means a MutationID was reused for a different request.
var ErrMutationConflict = errors.New("catalog: mutation id reused with different request")

// ErrMutationExpired means a mutation's fenced target is at or below the
// tenant compaction floor.
var ErrMutationExpired = errors.New("catalog: mutation id is below compaction floor")

// ErrHandleClosed means the requested pinned handle is no longer open.
var ErrHandleClosed = errors.New("catalog: handle closed")

// ErrStateNotFound means a tenant has no persisted runtime state.
var ErrStateNotFound = errors.New("catalog: tenant state not found")

// ErrStateConflict means a tenant-state compare-and-swap version did not match.
var ErrStateConflict = errors.New("catalog: tenant state version conflict")

// ErrGenerationMismatch means a caller addressed a stale tenant incarnation.
var ErrGenerationMismatch = errors.New("catalog: tenant generation mismatch")

// ErrMutationActive means a tenant already has an uncommitted namespace mutation.
var ErrMutationActive = errors.New("catalog: tenant has an active prepared mutation")

// ErrMutationClaimed means an external source operation has an unsettled durable owner.
var ErrMutationClaimed = errors.New("catalog: prepared mutation external operation is claimed")

// ErrSchemaMismatch means the database was not created by this exact catalog schema.
var ErrSchemaMismatch = errors.New("catalog: database schema mismatch")

// ErrStorageQuota means a durable catalog storage ceiling would be exceeded.
var ErrStorageQuota = errors.New("catalog: storage quota exceeded")

// ErrTenantProvisionConflict means a requested tenant definition differs from
// its durable desired definition.
var ErrTenantProvisionConflict = errors.New("catalog: tenant provision conflict")

// ErrTenantOwnerMismatch means a caller does not own the durable tenant identity.
var ErrTenantOwnerMismatch = errors.New("catalog: tenant owner mismatch")

// ErrBrokerAttemptConflict means a command identity or signal revision was
// reused with different bytes or a different signed process generation.
var ErrBrokerAttemptConflict = errors.New("catalog: broker command attempt conflict")

// Generation identifies one nonzero tenant runtime incarnation.
type Generation uint64

// StateVersion is the compare-and-swap version of a tenant state row.
type StateVersion uint64

// PreparedMutationState is the durable external/catalog commit state.
type PreparedMutationState uint8

const (
	// MutationPrepared has durable intent but no proven external source apply.
	MutationPrepared PreparedMutationState = iota + 1
	// MutationApplying has a durable claim for an external source operation.
	MutationApplying
	// MutationCommitted has published its catalog revision.
	MutationCommitted
)

// QuarantineLane identifies the semantic lane isolated by a failure.
type QuarantineLane uint8

const (
	// QuarantineLaneCatalogMutation isolates catalog mutation convergence.
	QuarantineLaneCatalogMutation QuarantineLane = iota + 1
	// QuarantineLaneMaterialization isolates materialization convergence.
	QuarantineLaneMaterialization
	// QuarantineLaneEnumeration isolates revision enumeration.
	QuarantineLaneEnumeration
	// QuarantineLaneMountLifecycle isolates mount ownership and teardown.
	QuarantineLaneMountLifecycle
)

// QuarantineCause identifies why a semantic lane was isolated.
type QuarantineCause uint8

const (
	// QuarantineCauseConflict records a non-destructive external conflict.
	QuarantineCauseConflict QuarantineCause = iota + 1
	// QuarantineCauseIntegrity records failed identity or content verification.
	QuarantineCauseIntegrity
	// QuarantineCauseUnsettled records ownership that could not be proven settled.
	QuarantineCauseUnsettled
	// QuarantineCauseUnavailable records an unavailable required dependency.
	QuarantineCauseUnavailable
)

// Quarantine is a typed durable isolation outcome.
type Quarantine struct {
	Lane     QuarantineLane
	Revision Revision
	Cause    QuarantineCause
	Detail   string
	Since    time.Time
}

// TenantStateRecord is one CAS-protected runtime convergence snapshot.
type TenantStateRecord struct {
	Tenant     TenantID
	Generation Generation
	// ActivatedGeneration is the exact tenant generation whose presentation lifecycle settled.
	ActivatedGeneration Generation
	Desired             Revision
	Observed            Revision
	Verified            Revision
	Applied             Revision
	Version             StateVersion
	Quarantine          *Quarantine
}

// TenantMetadata is the immutable identity metadata for one tenant namespace.
type TenantMetadata struct {
	Tenant        TenantID
	Root          ObjectID
	CasePolicy    CasePolicy
	Presentations PresentationSet
}

// TenantProvision is the durable desired definition for one tenant generation.
type TenantProvision struct {
	OwnerID         string
	Tenant          TenantID
	Root            ObjectID
	Mount           MountPresentation
	BackingRoot     string
	ContentSourceID string
	Access          TenantAccessMode
	CasePolicy      CasePolicy
	Presentations   PresentationSet
	FileProvider    FileProviderPresentation
	Generation      Generation
}

// MountPresentation is the durable desired native presentation, when requested.
type MountPresentation struct {
	PresentationRoot string
}

// Enabled reports whether this tenant requests a native presentation.
func (p MountPresentation) Enabled() bool { return p.PresentationRoot != "" }

// FileProviderPresentation is the generic durable identity of one tenant domain.
type FileProviderPresentation struct {
	PresentationInstanceID string
	DisplayName            string
}

// Enabled reports whether this tenant requests a File Provider domain.
func (p FileProviderPresentation) Enabled() bool {
	return p.PresentationInstanceID != "" || p.DisplayName != ""
}

// StaleAnchorError reports an anchor older than the durable compaction floor.
type StaleAnchorError struct {
	Anchor Revision
	Floor  Revision
}

// Error implements error.
func (e *StaleAnchorError) Error() string {
	return fmt.Sprintf("catalog: stale anchor %d; compaction floor is %d", e.Anchor, e.Floor)
}

// Convergence records the desired and proven materialization revisions.
type Convergence struct {
	Desired  Revision
	Observed Revision
	Verified Revision
	Applied  Revision
}

// Object is one immutable catalog object revision.
type Object struct {
	Tenant           TenantID
	ID               ObjectID
	Parent           ObjectID
	Revision         Revision
	MetadataRevision Revision
	ContentRevision  Revision
	Name             string
	Kind             Kind
	Mode             uint32
	Size             int64
	Hash             ContentHash
	LinkTarget       string
	Convergence      Convergence
	Visibility       Visibility
	Tombstone        bool
}

// ContentRef identifies one immutable content-addressed blob.
type ContentRef struct {
	Stage StageID
	Hash  ContentHash
	Size  int64
}

// ContentUpdate is an explicit replacement of a file's immutable bytes.
type ContentUpdate struct {
	Revision Revision
	Ref      ContentRef
}

// CreateSpec is the complete initial state for a new object.
type CreateSpec struct {
	Parent          ObjectID
	Name            string
	Kind            Kind
	Mode            uint32
	ContentRevision Revision
	Content         ContentRef
	LinkTarget      string
	Convergence     Convergence
	Visibility      Visibility
}

// RevisionSpec is the complete next state for an existing object.
type RevisionSpec struct {
	Parent      ObjectID
	Name        string
	Mode        uint32
	Content     *ContentUpdate
	Convergence Convergence
	Visibility  Visibility
}

// CreateMutation creates one namespace object.
type CreateMutation struct {
	Spec CreateSpec
}

// ReviseMutation appends a revision to one namespace object.
type ReviseMutation struct {
	Object ObjectID
	Spec   RevisionSpec
}

// DeleteMutation tombstones one namespace object.
type DeleteMutation struct {
	Object ObjectID
}

// DiscardPrivateMutation durably removes one unpublished object capability.
type DiscardPrivateMutation struct {
	Object  ObjectID
	Creator MutationID
}

// PromotePrivateMutation publishes one unpublished object into an unoccupied binding.
type PromotePrivateMutation struct {
	Object     ObjectID
	Creator    MutationID
	Parent     ObjectID
	Name       string
	Visibility Visibility
	Mode       *uint32
	Content    *ContentUpdate
}

// ReplaceMutation atomically publishes Source in place of Target.
type ReplaceMutation struct {
	Source         ObjectID
	Target         ObjectID
	PrivateCreator *MutationID
	Parent         *ObjectID
	Name           *string
	Mode           *uint32
	Visibility     *Visibility
	Content        *ContentUpdate
}

// MutationIntent is one closed namespace operation plus its durable consumer payload.
type MutationIntent struct {
	SourceID       string
	SourceMetadata string
	Origin         CausalOrigin
	Disposition    MutationDisposition
	Create         *CreateMutation
	Revise         *ReviseMutation
	Delete         *DeleteMutation
	Replace        *ReplaceMutation
	DiscardPrivate *DiscardPrivateMutation
	PromotePrivate *PromotePrivateMutation
}

// MutationDisposition selects public namespace settlement or private staging.
type MutationDisposition uint8

const (
	// MutationDispositionNamespace settles a visible namespace revision.
	MutationDispositionNamespace MutationDisposition = iota + 1
	// MutationDispositionPrivate settles an unpublished private capability.
	MutationDispositionPrivate
)

// SourceLocator is one authority-owned, path-independent object locator at an
// exact causal source revision.
type SourceLocator struct {
	SourceAuthority causal.SourceAuthorityID
	SourceKey       SourceObjectKey
	SourceRevision  causal.Revision
}

// SourceMutationOperation is the path-free external operation presented to
// product policy.
type SourceMutationOperation struct {
	Kind       MutationKind
	Name       string
	ObjectKind Kind
	Mode       uint32
	LinkTarget string
	HasContent bool
}

// SourceMutationContext is the catalog-owned locator set for one prepared
// external operation.
type SourceMutationContext struct {
	Operation SourceMutationOperation
	Object    *SourceLocator
	Parent    *SourceLocator
	Target    *SourceLocator
	Private   *PrivateSourceCapability
}

// PrivateSourceCapability authorizes one exact unpublished object promotion.
type PrivateSourceCapability struct {
	Creator MutationID
}

// CausalOrigin is authenticated server metadata for one namespace mutation.
type CausalOrigin struct {
	Cause      causal.Cause
	Domain     causal.DomainID
	Generation causal.Generation
}

// PreparedMutation is the durable seam around an idempotent external source operation.
type PreparedMutation struct {
	OperationID  MutationID
	Tenant       TenantID
	Kind         MutationKind
	State        PreparedMutationState
	ExpectedHead Revision
	Intent       MutationIntent
	Source       *SourceMutationContext
	SourceResult *SourceLocator
	Claim        *MutationClaim
}

// MutationClaim is the fenced ownership token for one external source attempt.
type MutationClaim struct {
	Owner MutationOwnerID
	Epoch uint64
}

// NamespaceMutationResult is the committed generic namespace outcome.
type NamespaceMutationResult struct {
	Mutation  MutationRecord
	Primary   Object
	Secondary *Object
}

// ReplaceResult is the atomic rename-over result at one revision.
type ReplaceResult struct {
	Revision Revision
	Source   Object
	Target   Object
}

// Change is one ordered namespace delta within a catalog revision.
type Change struct {
	Revision Revision
	Sequence uint32
	Kind     ChangeKind
	Object   Object
}

// ChangeCursor is the exclusive position of one ordered namespace delta.
type ChangeCursor struct {
	Revision Revision
	Sequence uint32
}

// CompleteChangeSequence is the synthetic cursor sequence after every real row in a revision.
const CompleteChangeSequence uint32 = ^uint32(0)

// CompleteChangeCursor returns the canonical position after a whole catalog revision.
func CompleteChangeCursor(revision Revision) ChangeCursor {
	return ChangeCursor{Revision: revision, Sequence: CompleteChangeSequence}
}

// ChangePage is one bounded row page from ChangesSince.
type ChangePage struct {
	Floor    Revision
	Head     Revision
	Next     ChangeCursor
	Complete bool
	Changes  []Change
}

// SnapshotCursor pins a stable metadata-only namespace snapshot.
type SnapshotCursor struct {
	After *ObjectID
}

// SnapshotPage is one stable metadata-only page.
type SnapshotPage struct {
	Revision Revision
	Objects  []Object
	Next     *SnapshotCursor
}

// Handle is a durable pin to one immutable object revision.
type Handle struct {
	ID             HandleID
	Tenant         TenantID
	Generation     Generation
	ObjectID       ObjectID
	ObjectRevision Revision
}

// MutationRecord is the durable outcome of one idempotent mutation.
type MutationRecord struct {
	ID        MutationID
	Tenant    TenantID
	Kind      MutationKind
	Revision  Revision
	Primary   [16]byte
	Secondary [16]byte
}
