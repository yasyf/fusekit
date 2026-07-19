package convergence

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

// CatalogResolver resolves exact source commits against durable domain and demand state.
type CatalogResolver struct {
	Catalog *catalog.Catalog
	Now     func() time.Time
}

// ResolveAffected resolves every durable target of one source change.
func (r CatalogResolver) ResolveAffected(ctx context.Context, change ChangeSet) ([]Resolution, error) {
	if r.Catalog == nil {
		return nil, errors.New("convergence: catalog resolver has no catalog")
	}
	targets, err := r.Catalog.ConvergenceTargets(ctx, change)
	if err != nil {
		return nil, err
	}
	result := make([]Resolution, 0, len(targets))
	for _, target := range targets {
		resolved, err := r.resolve(ctx, target)
		if err != nil {
			return nil, err
		}
		result = append(result, resolved)
	}
	return result, nil
}

// ResolveTenant resolves the newest durable causal commit for one tenant.
func (r CatalogResolver) ResolveTenant(ctx context.Context, tenant TenantID, authority SourceAuthorityID) (Resolution, error) {
	if r.Catalog == nil {
		return Resolution{}, errors.New("convergence: catalog resolver has no catalog")
	}
	target, err := r.Catalog.CurrentConvergenceTarget(ctx, catalog.TenantID(tenant), causal.SourceAuthorityID(authority))
	if err != nil {
		return Resolution{}, err
	}
	return r.resolve(ctx, target)
}

func (r CatalogResolver) resolve(ctx context.Context, target catalog.ConvergenceTarget) (Resolution, error) {
	domains, err := r.Catalog.FileProviderDomains(ctx)
	if err != nil {
		return Resolution{}, err
	}
	resolution := Resolution{
		Tenant:          TenantID(target.Tenant),
		Applicable:      cloneChange(target.Change),
		CatalogRevision: CatalogRevision(target.CatalogRevision),
	}
	var domain *catalog.FileProviderDomain
	for index := range domains {
		if domains[index].Tenant == target.Tenant {
			domain = &domains[index]
			break
		}
	}
	if domain == nil {
		return resolution, nil
	}
	resolution.Domain = domain.DomainID
	resolution.Generation = Generation(domain.Generation)
	resolution.Registered = domain.Registered
	if domain.Registered {
		now := time.Now()
		if r.Now != nil {
			now = r.Now()
		}
		leases, interests, err := r.Catalog.FileProviderDemand(ctx, target.Tenant, domain.DomainID, domain.Generation, now)
		if err != nil {
			return Resolution{}, err
		}
		resolution.LiveLeases = leases
		resolution.MaterializedInterests = interests
	}
	resolution.Effective, err = r.effective(ctx, target)
	if err != nil {
		return Resolution{}, err
	}
	return resolution, nil
}

func (r CatalogResolver) effective(ctx context.Context, target catalog.ConvergenceTarget) ([]EffectiveValue, error) {
	values := make([]EffectiveValue, 0, len(target.Change.AffectedKeys))
	for _, key := range target.Change.AffectedKeys {
		id, found, err := r.objectForKey(ctx, target.Change.SourceAuthority, target.Tenant, key)
		if err != nil {
			return nil, err
		}
		value := EffectiveValue{Key: key, Bytes: []byte{0}}
		if found {
			object, err := r.Catalog.LookupAt(ctx, target.Tenant, catalog.PresentationFileProvider, id, target.CatalogRevision)
			if err != nil && !errors.Is(err, catalog.ErrNotFound) {
				return nil, err
			}
			if err == nil {
				encoded, err := json.Marshal(struct {
					Parent     string              `json:"parent"`
					Name       string              `json:"name"`
					Kind       catalog.Kind        `json:"kind"`
					Mode       uint32              `json:"mode"`
					Size       int64               `json:"size"`
					Hash       catalog.ContentHash `json:"hash"`
					LinkTarget string              `json:"link_target"`
				}{
					Parent: object.Parent.String(), Name: object.Name, Kind: object.Kind, Mode: object.Mode,
					Size: object.Size, Hash: object.Hash, LinkTarget: object.LinkTarget,
				})
				if err != nil {
					return nil, fmt.Errorf("convergence: encode effective object: %w", err)
				}
				value.Bytes = encoded
			}
		}
		values = append(values, value)
	}
	return values, nil
}

func (r CatalogResolver) objectForKey(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	tenant catalog.TenantID,
	key LogicalKey,
) (catalog.ObjectID, bool, error) {
	const objectPrefix = "object:"
	if strings.HasPrefix(string(key), objectPrefix) {
		raw, err := hex.DecodeString(strings.TrimPrefix(string(key), objectPrefix))
		if err != nil || len(raw) != len(catalog.ObjectID{}) {
			return catalog.ObjectID{}, false, fmt.Errorf("%w: invalid object logical key %q", ErrInvalidResolution, key)
		}
		var id catalog.ObjectID
		copy(id[:], raw)
		return id, true, nil
	}
	return r.Catalog.SourceObjectBinding(ctx, authority, tenant, key)
}

var _ Resolver = CatalogResolver{}
