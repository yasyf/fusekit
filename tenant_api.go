package fusekit

import (
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

// ContentSource identifies the tenant's declarative content provider.
type ContentSource = tenant.ContentSource

// TenantTraits are immutable within one tenant generation.
type TenantTraits = tenant.TenantTraits

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

// NewTenantRuntime constructs an empty dynamic tenant fleet.
func NewTenantRuntime(store TenantStore, workers WorkerPool, planner TenantPlanner) (*TenantRuntime, error) {
	return tenant.NewRuntime(store, workers, planner)
}
