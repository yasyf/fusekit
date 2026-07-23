package holder

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/tenant"
)

type tenantLifecycleRuntime struct {
	mu      sync.Mutex
	tenants *tenant.TenantRuntime
	domains mountmux.DomainRemover
}

func (r *tenantLifecycleRuntime) ProvisionTenant(ctx context.Context, spec tenant.TenantSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tenants.ProvisionTenant(ctx, spec)
}

func (r *tenantLifecycleRuntime) ReplaceTenant(
	ctx context.Context,
	expected catalog.Generation,
	spec tenant.TenantSpec,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tenants.ReplaceTenant(ctx, expected, spec)
}

func (r *tenantLifecycleRuntime) RemoveTenant(
	ctx context.Context,
	id catalog.TenantID,
	generation catalog.Generation,
	owner tenant.OwnerID,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	status, err := r.tenants.State(ctx, owner, id)
	if errors.Is(err, tenant.ErrTenantNotFound) && r.domains != nil {
		return r.domains.ProveTenantDomainRemoved(ctx, string(owner), id, generation)
	}
	if err != nil {
		return err
	}
	if status.State.Generation != generation {
		return fmt.Errorf(
			"%w: got %d, current %d",
			tenant.ErrGenerationConflict, generation, status.State.Generation,
		)
	}
	var current tenant.TenantSpec
	for _, spec := range r.tenants.Specs() {
		if spec.ID == id {
			current = spec
			break
		}
	}
	if current.ID == "" {
		return tenant.ErrTenantNotFound
	}
	if current.FileProvider.Enabled {
		if r.domains == nil {
			return errors.New("FuseKit runtime: File Provider domain remover is required")
		}
		if err := r.domains.RemoveTenantDomain(ctx, string(owner), id, generation); err != nil {
			return err
		}
	}
	return r.tenants.RemoveTenant(ctx, id, generation)
}

func (r *tenantLifecycleRuntime) State(
	ctx context.Context,
	id catalog.TenantID,
	owner tenant.OwnerID,
) (tenant.TenantStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.tenants.State(ctx, owner, id)
}
