package fusekit

import (
	"context"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

// TenantRuntime owns the live actor fleet for revisioned filesystem tenants.
type TenantRuntime = tenant.TenantRuntime

// TenantSpec is the immutable contract for one tenant generation.
type TenantSpec = tenant.TenantSpec

// TenantState proves convergence for one requested catalog revision.
type TenantState = tenant.TenantState

// OwnerID identifies the filesystem consumer that owns a tenant.
type OwnerID = tenant.OwnerID

// BackingSpec declares the tenant's durable backing root.
type BackingSpec = tenant.BackingSpec

// MountSpec declares the tenant's native presentation path.
type MountSpec = tenant.MountSpec

// ContentSource identifies the tenant's declarative content provider.
type ContentSource = tenant.ContentSource

// TenantTraits are immutable within one tenant generation.
type TenantTraits = tenant.TenantTraits

// FileProviderSpec declares one immutable account-instance presentation.
type FileProviderSpec = tenant.FileProviderSpec

// AccessMode is a tenant's immutable mutation policy.
type AccessMode = tenant.AccessMode

// PresentationSet is a nonempty closed set of tenant presentation surfaces.
type PresentationSet = catalog.PresentationSet

// TenantStore combines catalog reads with CAS-protected convergence state.
type TenantStore = tenant.Store

// WorkerPool is the killable disposable-worker surface used by tenants.
type WorkerPool = tenant.WorkerPool

// TenantPlanner builds and verifies the three explicit worker fragments.
type TenantPlanner = tenant.Planner

// FleetTransitionHook establishes exact authority fleets around tenant generation changes.
type FleetTransitionHook = tenant.FleetTransitionHook

// FleetTransition describes one exact authority-fleet transaction.
type FleetTransition = tenant.FleetTransition

const (
	// ReadOnly rejects filesystem mutations.
	ReadOnly = tenant.ReadOnly
	// ReadWrite permits filesystem mutations.
	ReadWrite = tenant.ReadWrite
	// PresentMount includes the mounted filesystem surface.
	PresentMount = catalog.PresentMount
	// PresentFileProvider includes the macOS File Provider surface.
	PresentFileProvider = catalog.PresentFileProvider
)

// NewTenantRuntime realizes one revision-fenced desired tenant snapshot.
func NewTenantRuntime(
	ctx context.Context,
	store TenantStore,
	workers WorkerPool,
	planner TenantPlanner,
	fleets FleetTransitionHook,
	desired []catalog.TenantProvision,
) (*TenantRuntime, error) {
	return tenant.NewRuntime(ctx, store, workers, planner, fleets, desired)
}
