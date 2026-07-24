// Package tenant schedules filesystem-semantic convergence for isolated tenants.
package tenant

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
)

var (
	// ErrClosed means the runtime no longer accepts preparation requests.
	ErrClosed = errors.New("tenant runtime closed")
	// ErrCanceled means the runtime canceled every tenant actor.
	ErrCanceled = errors.New("tenant runtime canceled")
	// ErrTenantNotFound means no immutable specification exists for a tenant.
	ErrTenantNotFound = errors.New("tenant runtime: tenant not found")
	// ErrInvalidSpec means a tenant specification violates runtime invariants.
	ErrInvalidSpec = errors.New("tenant runtime: invalid tenant spec")
	// ErrTenantConflict means a provisioned tenant has different immutable specification data.
	ErrTenantConflict = errors.New("tenant runtime: tenant specification conflict")
	// ErrGenerationConflict means a lifecycle operation targeted a stale tenant generation.
	ErrGenerationConflict = errors.New("tenant runtime: tenant generation conflict")
	// ErrTenantChanging means the tenant is being replaced or removed.
	ErrTenantChanging = errors.New("tenant runtime: tenant lifecycle transition in progress")
	// ErrTenantOwnerMismatch means an authenticated consumer does not own the tenant.
	ErrTenantOwnerMismatch = errors.New("tenant runtime: tenant owner mismatch")
)

// OwnerID identifies the filesystem consumer that owns a tenant specification.
type OwnerID string

// AccessMode is a tenant's immutable mutation policy.
type AccessMode = catalog.TenantAccessMode

const (
	// ReadOnly rejects filesystem mutations.
	ReadOnly = catalog.TenantReadOnly
	// ReadWrite permits filesystem mutations.
	ReadWrite = catalog.TenantReadWrite
)

// BackingSpec declares the tenant's durable backing root.
type BackingSpec struct {
	Root string
}

// MountSpec declares the tenant's native presentation path.
type MountSpec struct {
	PresentationRoot string
}

// ContentSource identifies a declarative content provider.
type ContentSource struct {
	ID string
}

// TenantTraits are immutable within one tenant generation.
type TenantTraits struct {
	Access          AccessMode
	CaseSensitivity catalog.CasePolicy
	Presentations   catalog.PresentationSet
}

// FileProviderSpec declares one immutable account-instance presentation.
type FileProviderSpec struct {
	Enabled                bool
	PresentationInstanceID string
	DisplayName            string
}

// TenantSpec is the complete immutable identity and backing contract for one
// tenant generation.
type TenantSpec struct {
	OwnerID      OwnerID
	ID           catalog.TenantID
	Mount        MountSpec
	Backing      BackingSpec
	Content      ContentSource
	Traits       TenantTraits
	FileProvider FileProviderSpec
	Generation   catalog.Generation
}

func (s TenantSpec) validate() error {
	switch {
	case s.OwnerID == "":
		return fmt.Errorf("%w: owner is required", ErrInvalidSpec)
	case s.ID == "":
		return fmt.Errorf("%w: tenant id is required", ErrInvalidSpec)
	case s.Traits.Presentations.Has(catalog.PresentationMount) != (s.Mount.PresentationRoot != ""):
		return fmt.Errorf("%w: mount metadata does not match presentation set", ErrInvalidSpec)
	case s.Mount.PresentationRoot != "" && !exactAbsolutePath(s.Mount.PresentationRoot):
		return fmt.Errorf("%w: presentation root %q is not an exact absolute path", ErrInvalidSpec, s.Mount.PresentationRoot)
	case !exactAbsolutePath(s.Backing.Root):
		return fmt.Errorf("%w: backing root %q is not an exact absolute path", ErrInvalidSpec, s.Backing.Root)
	case s.Content.ID == "":
		return fmt.Errorf("%w: content source id is required", ErrInvalidSpec)
	case s.Traits.Access != ReadOnly && s.Traits.Access != ReadWrite:
		return fmt.Errorf("%w: access mode %d is invalid", ErrInvalidSpec, s.Traits.Access)
	case s.Traits.CaseSensitivity != catalog.CaseSensitive && s.Traits.CaseSensitivity != catalog.CaseInsensitive:
		return fmt.Errorf("%w: case sensitivity %d is invalid", ErrInvalidSpec, s.Traits.CaseSensitivity)
	case s.Traits.Presentations == 0 || s.Traits.Presentations&^(catalog.PresentMount|catalog.PresentFileProvider) != 0:
		return fmt.Errorf("%w: presentation set %d is invalid", ErrInvalidSpec, s.Traits.Presentations)
	case s.Traits.Presentations.Has(catalog.PresentationFileProvider) != s.FileProvider.Enabled:
		return fmt.Errorf("%w: File Provider metadata does not match presentation set", ErrInvalidSpec)
	case s.FileProvider.Enabled && (s.FileProvider.PresentationInstanceID == "" || s.FileProvider.DisplayName == "" ||
		strings.ContainsRune(s.FileProvider.PresentationInstanceID, 0) || strings.ContainsRune(s.FileProvider.DisplayName, 0)):
		return fmt.Errorf("%w: File Provider metadata is incomplete", ErrInvalidSpec)
	case s.Generation == 0:
		return fmt.Errorf("%w: generation is required", ErrInvalidSpec)
	default:
		return nil
	}
}

func exactAbsolutePath(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value && !strings.ContainsRune(value, 0)
}

func tenantProvision(spec TenantSpec) catalog.TenantProvision {
	var fileProvider catalog.FileProviderPresentation
	if spec.FileProvider.Enabled {
		fileProvider = catalog.FileProviderPresentation{
			PresentationInstanceID: spec.FileProvider.PresentationInstanceID,
			DisplayName:            spec.FileProvider.DisplayName,
		}
	}
	return catalog.TenantProvision{
		OwnerID: string(spec.OwnerID), Tenant: spec.ID,
		Mount: catalog.MountPresentation{PresentationRoot: spec.Mount.PresentationRoot}, BackingRoot: spec.Backing.Root,
		ContentSourceID: spec.Content.ID, Access: spec.Traits.Access,
		CasePolicy: spec.Traits.CaseSensitivity, Presentations: spec.Traits.Presentations,
		FileProvider: fileProvider,
		Generation:   spec.Generation,
	}
}

func provisionSpec(provision catalog.TenantProvision) TenantSpec {
	var fileProvider FileProviderSpec
	if provision.FileProvider.Enabled() {
		fileProvider = FileProviderSpec{
			Enabled:                true,
			PresentationInstanceID: provision.FileProvider.PresentationInstanceID,
			DisplayName:            provision.FileProvider.DisplayName,
		}
	}
	return TenantSpec{
		OwnerID: OwnerID(provision.OwnerID), ID: provision.Tenant,
		Mount:   MountSpec{PresentationRoot: provision.Mount.PresentationRoot},
		Backing: BackingSpec{Root: provision.BackingRoot},
		Content: ContentSource{ID: provision.ContentSourceID},
		Traits: TenantTraits{
			Access: provision.Access, CaseSensitivity: provision.CasePolicy, Presentations: provision.Presentations,
		},
		FileProvider: fileProvider,
		Generation:   provision.Generation,
	}
}

// Lane is one closed filesystem-semantic convergence lane.
type Lane = catalog.QuarantineLane

const (
	// LaneCatalogMutation applies catalog mutations.
	LaneCatalogMutation = catalog.QuarantineLaneCatalogMutation
	// LaneMaterialization makes exact content revisions locally available.
	LaneMaterialization = catalog.QuarantineLaneMaterialization
	// LaneMountLifecycle reconciles mount ownership and presentation.
	LaneMountLifecycle = catalog.QuarantineLaneMountLifecycle
)

// TenantState proves convergence for one caller's requested revision.
type TenantState struct {
	Requested  catalog.Revision
	Tenant     catalog.TenantID
	Generation catalog.Generation
	Desired    catalog.Revision
	Observed   catalog.Revision
	Verified   catalog.Revision
	Applied    catalog.Revision
	Activated  catalog.Generation
	Version    catalog.StateVersion
	Quarantine *catalog.Quarantine
}

// TenantStatus is an owner-fenced durable lifecycle snapshot.
type TenantStatus struct {
	Owner               OwnerID
	State               TenantState
	ReplacementEligible bool
}

// Prepared reports whether every convergence generation proves Requested.
func (s TenantState) Prepared() bool {
	return s.Requested > 0 &&
		s.Quarantine == nil &&
		s.Desired >= s.Requested &&
		s.Observed >= s.Requested &&
		s.Verified >= s.Requested &&
		s.Applied >= s.Requested &&
		s.Activated == s.Generation
}

func stateFor(requested catalog.Revision, record catalog.TenantStateRecord) TenantState {
	return TenantState{
		Requested:  requested,
		Tenant:     record.Tenant,
		Generation: record.Generation,
		Desired:    record.Desired,
		Observed:   record.Observed,
		Verified:   record.Verified,
		Applied:    record.Applied,
		Activated:  record.ActivatedGeneration,
		Version:    record.Version,
		Quarantine: record.Quarantine,
	}
}

// QuarantinedError reports a durably isolated semantic lane.
type QuarantinedError struct {
	State TenantState
}

// Error implements error.
func (e *QuarantinedError) Error() string {
	q := e.State.Quarantine
	return fmt.Sprintf("tenant %q lane %d quarantined at revision %d: %s", e.State.Tenant, q.Lane, q.Revision, q.Detail)
}

// Catalog is the revisioned read surface available to operation planners.
type Catalog interface {
	Tenant(ctx context.Context, tenant catalog.TenantID) (catalog.TenantMetadata, error)
	Head(ctx context.Context, tenant catalog.TenantID) (catalog.Revision, error)
	CompactionFloor(ctx context.Context, tenant catalog.TenantID) (catalog.Revision, error)
	Root(ctx context.Context, tenant catalog.TenantID) (catalog.Object, error)
	Lookup(ctx context.Context, tenant catalog.TenantID, presentation catalog.Presentation, id catalog.ObjectID) (catalog.Object, error)
	LookupName(ctx context.Context, tenant catalog.TenantID, presentation catalog.Presentation, parent catalog.ObjectID, name string) (catalog.Object, error)
	Snapshot(ctx context.Context, tenant catalog.TenantID, scope catalog.EnumerationScope, revision catalog.Revision, cursor catalog.SnapshotCursor, limit int) (catalog.SnapshotPage, error)
	ChangesSince(ctx context.Context, tenant catalog.TenantID, scope catalog.EnumerationScope, cursor catalog.ChangeCursor, limit int) (catalog.ChangePage, error)
	PendingMutation(ctx context.Context, tenant catalog.TenantID) (*catalog.PreparedMutation, error)
	PreparedMutation(ctx context.Context, tenant catalog.TenantID, id catalog.MutationID) (catalog.PreparedMutation, error)
	OpenMutationContent(ctx context.Context, tenant catalog.TenantID, id catalog.MutationID) (contentstream.Source, error)
	ClaimMutation(ctx context.Context, id catalog.MutationID, owner catalog.MutationOwnerID) (catalog.PreparedMutation, error)
	PrepareMutationSource(ctx context.Context, id catalog.MutationID, claim catalog.MutationClaim) (catalog.PreparedMutation, error)
	SetMutationSourceResult(ctx context.Context, id catalog.MutationID, claim catalog.MutationClaim, locator catalog.SourceLocator) (catalog.PreparedMutation, error)
	ReclaimMutation(ctx context.Context, id catalog.MutationID, stale catalog.MutationClaim, owner catalog.MutationOwnerID) (catalog.PreparedMutation, error)
}

// Store combines catalog reads with CAS-protected runtime convergence state.
type Store interface {
	Catalog
	ProvisionTenant(context.Context, catalog.TenantProvision) (catalog.TenantProvision, error)
	ReplaceTenantProvision(context.Context, catalog.Generation, catalog.TenantProvision) (catalog.TenantProvision, error)
	RemoveTenantProvision(context.Context, catalog.TenantID, catalog.Generation) error
	LoadTenantState(ctx context.Context, tenant catalog.TenantID) (catalog.TenantStateRecord, error)
	SaveTenantState(ctx context.Context, expected catalog.StateVersion, record catalog.TenantStateRecord) (catalog.TenantStateRecord, error)
}

// FleetTransitionKind identifies one exact desired-fleet change.
type FleetTransitionKind uint8

const (
	FleetProvision FleetTransitionKind = iota + 1
	FleetReplace
	FleetRemove
)

// FleetTransition contains the exact fleet before a change, while the changed
// tenant is fenced out, and after its durable catalog commit.
type FleetTransition struct {
	Kind      FleetTransitionKind
	Before    []TenantSpec
	Drained   []TenantSpec
	Committed []TenantSpec
}

// FleetTransitionHook atomically establishes authority fleets around catalog
// generation changes. Methods are idempotent. Prepare may return an error only
// after restoring Before. Commit and Abort settle their target fleet before
// returning unless the runtime lifecycle context is canceled.
type FleetTransitionHook interface {
	Prepare(context.Context, FleetTransition) error
	Commit(context.Context, FleetTransition) error
	Abort(context.Context, FleetTransition) error
}

// SourceMutationStep describes one idempotent external apply from the durable journal.
type SourceMutationStep struct {
	TenantID       catalog.TenantID
	Generation     catalog.Generation
	OperationID    catalog.MutationID
	SourceID       string
	SourceMetadata string
	Kind           catalog.MutationKind
	ExpectedHead   catalog.Revision
	Origin         catalog.CausalOrigin
	Source         catalog.SourceMutationContext
}

// SourceMutationOperation identifies one FuseKit-owned semantic source apply.
type SourceMutationOperation struct {
	OperationID    catalog.MutationID
	SourceID       string
	SourceMetadata string
	SourceResult   *catalog.SourceLocator
}

// SourceMutationCommit identifies a catalog-committed operation that may now be causally echoed.
type SourceMutationCommit struct {
	OperationID catalog.MutationID
	SourceID    string
}

// SourceMutationContent reopens journal-owned request bytes without exposing their path.
type SourceMutationContent interface {
	Open(context.Context) (contentstream.Source, error)
	Close() error
}

// MaterializationStep describes one exact-revision materialization step.
type MaterializationStep struct {
	Tenant   TenantSpec
	Revision catalog.Revision
}

// Planner owns semantic source mutation planning only. Presentation activation
// belongs to the composed holder runtime.
type Planner interface {
	PrepareSourceMutation(ctx context.Context, step SourceMutationStep) (SourceMutationOperation, error)
	ApplySourceMutation(ctx context.Context, step SourceMutationStep, operation SourceMutationOperation, content SourceMutationContent) error
	SourceMutationCommitted(context.Context, SourceMutationCommit) error
}
