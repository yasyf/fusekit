package convergence

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

// CatalogResolver resolves exact source commits against durable domain and demand state.
type CatalogResolver struct {
	store CatalogResolutionStore
	now   func() time.Time
}

// CatalogResolutionStore is the narrow catalog view needed to resolve convergence work.
type CatalogResolutionStore interface {
	CurrentConvergenceTarget(context.Context, catalog.TenantID, causal.SourceAuthorityID) (catalog.ConvergenceTarget, error)
	FileProviderDomainForTenant(context.Context, catalog.TenantID) (catalog.FileProviderDomain, bool, error)
	FileProviderDemand(context.Context, catalog.TenantID, causal.DomainID, catalog.Generation, time.Time) (uint32, uint32, error)
}

// NewCatalogResolver binds resolution to a narrow catalog view.
func NewCatalogResolver(store CatalogResolutionStore, now func() time.Time) (*CatalogResolver, error) {
	if store == nil {
		return nil, errors.New("convergence: catalog resolution store is nil")
	}
	return &CatalogResolver{store: store, now: now}, nil
}

// ResolveAffected resolves every durable target of one source change.
func (r CatalogResolver) ResolveAffected(
	ctx context.Context,
	change ChangeSet,
	commits []causal.CatalogCommit,
) ([]Resolution, error) {
	if r.store == nil {
		return nil, errors.New("convergence: catalog resolver has no catalog")
	}
	result := make([]Resolution, 0, len(commits))
	for _, commit := range commits {
		if commit.Tenant == "" || commit.CatalogRevision == 0 || commit.FileProviderFingerprint == ([32]byte{}) {
			return nil, fmt.Errorf("%w: incomplete catalog commit", ErrInvalidResolution)
		}
		target := catalog.ConvergenceTarget{
			Change:                  cloneCausalChange(change),
			Tenant:                  catalog.TenantID(commit.Tenant),
			CatalogRevision:         catalog.Revision(commit.CatalogRevision),
			FileProviderFingerprint: commit.FileProviderFingerprint,
		}
		domain, _, err := r.store.FileProviderDomainForTenant(ctx, target.Tenant)
		if err != nil {
			return nil, err
		}
		resolved, err := r.resolve(ctx, target, domain)
		if err != nil {
			return nil, err
		}
		result = append(result, resolved)
	}
	return result, nil
}

// ResolveTenant resolves the newest durable causal commit for one tenant.
func (r CatalogResolver) ResolveTenant(ctx context.Context, tenant TenantID, authority SourceAuthorityID) (Resolution, error) {
	if r.store == nil {
		return Resolution{}, errors.New("convergence: catalog resolver has no catalog")
	}
	target, err := r.store.CurrentConvergenceTarget(ctx, catalog.TenantID(tenant), causal.SourceAuthorityID(authority))
	if err != nil {
		return Resolution{}, err
	}
	domain, _, err := r.store.FileProviderDomainForTenant(ctx, target.Tenant)
	if err != nil {
		return Resolution{}, err
	}
	return r.resolve(ctx, target, domain)
}

func (r CatalogResolver) resolve(
	ctx context.Context,
	target catalog.ConvergenceTarget,
	domain catalog.FileProviderDomain,
) (Resolution, error) {
	if target.FileProviderFingerprint == ([32]byte{}) {
		return Resolution{}, fmt.Errorf("%w: target has no File Provider fingerprint", ErrInvalidResolution)
	}
	resolution := Resolution{
		Tenant:          TenantID(target.Tenant),
		Applicable:      cloneChange(target.Change),
		CatalogRevision: CatalogRevision(target.CatalogRevision),
		Fingerprint:     Fingerprint(target.FileProviderFingerprint),
	}
	if domain.Tenant == "" {
		return resolution, nil
	}
	resolution.Domain = domain.DomainID
	resolution.Generation = Generation(domain.Generation)
	resolution.Registered = domain.Registered
	if domain.Registered {
		now := time.Now()
		if r.now != nil {
			now = r.now()
		}
		leases, interests, err := r.store.FileProviderDemand(ctx, target.Tenant, domain.DomainID, domain.Generation, now)
		if err != nil {
			return Resolution{}, err
		}
		resolution.LiveLeases = leases
		resolution.MaterializedInterests = interests
	}
	return resolution, nil
}

func cloneCausalChange(change ChangeSet) causal.ChangeSet {
	return causal.ChangeSet{
		SourceAuthority: change.SourceAuthority, SourceRevision: change.SourceRevision,
		ChangeID: change.ChangeID, OperationID: change.OperationID, Cause: change.Cause,
		Origin: change.Origin, OriginGeneration: change.OriginGeneration,
		AffectedKeys: append([]causal.LogicalKey(nil), change.AffectedKeys...),
	}
}

var _ Resolver = (*CatalogResolver)(nil)
