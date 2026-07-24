package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type testMaintenanceTotals struct {
	Mutations        int
	SourceOperations int
}

func testMaintenanceNow() time.Time { return time.Unix(1, 0).UTC() }

func prepareTestMaintenance(
	ctx context.Context,
	c *Catalog,
	tenant TenantID,
	target Revision,
) error {
	head, _, err := revisionState(ctx, c.readDB, tenant)
	if err != nil {
		return err
	}
	if target > head {
		return fmt.Errorf("%w: test maintenance target exceeds head", ErrInvalidTransition)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var generation uint64
	err = tx.QueryRowContext(ctx,
		"SELECT generation FROM tenant_state WHERE tenant = ?", string(tenant)).Scan(&generation)
	if errors.Is(err, sql.ErrNoRows) {
		generation = 1
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_state(
    tenant, generation, activated_generation, desired_revision,
    observed_revision, verified_revision, applied_revision, version
) VALUES (?, 1, 1, ?, ?, ?, ?, 1)`,
			string(tenant), uint64(target), uint64(target), uint64(target), uint64(target)); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if _, err := tx.ExecContext(ctx, `
UPDATE tenant_state SET
    activated_generation = generation,
    desired_revision = MAX(desired_revision, ?),
    observed_revision = MAX(observed_revision, ?),
    verified_revision = MAX(verified_revision, ?),
    applied_revision = MAX(applied_revision, ?),
    version = version + 1
WHERE tenant = ?`,
		uint64(target), uint64(target), uint64(target), uint64(target), string(tenant)); err != nil {
		return err
	}
	return tx.Commit()
}

func maintainTestUntilIdle(
	ctx context.Context,
	c *Catalog,
	tenant TenantID,
	target Revision,
) (testMaintenanceTotals, error) {
	if err := prepareTestMaintenance(ctx, c, tenant, target); err != nil {
		return testMaintenanceTotals{}, err
	}
	var total testMaintenanceTotals
	for step := 0; step < 10000; step++ {
		tenantResult, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow())
		if err != nil {
			return testMaintenanceTotals{}, fmt.Errorf("catalog: test tenant maintenance step %d: %w", step, err)
		}
		if tenantResult.Phase == MaintenanceMutations {
			total.Mutations += tenantResult.Retired
		}
		global, err := c.MaintainGlobal(ctx, testMaintenanceNow())
		if err != nil {
			return testMaintenanceTotals{}, fmt.Errorf(
				"catalog: test global maintenance step %d after tenant phase %d: %w",
				step, tenantResult.Phase, err,
			)
		}
		total.SourceOperations += global.SourceOperations
		if !tenantResult.More && !global.More {
			return total, nil
		}
	}
	return testMaintenanceTotals{}, errors.New("catalog: test maintenance did not converge")
}

func maintainTestTenantUntilIdle(
	ctx context.Context,
	c *Catalog,
	tenant TenantID,
	target Revision,
) (testMaintenanceTotals, error) {
	if err := prepareTestMaintenance(ctx, c, tenant, target); err != nil {
		return testMaintenanceTotals{}, err
	}
	var total testMaintenanceTotals
	for step := 0; step < 10000; step++ {
		result, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow())
		if err != nil {
			return testMaintenanceTotals{}, fmt.Errorf("catalog: test tenant maintenance step %d: %w", step, err)
		}
		if result.Phase == MaintenanceMutations {
			total.Mutations += result.Retired
		}
		if !result.More {
			return total, nil
		}
	}
	return testMaintenanceTotals{}, errors.New("catalog: test tenant maintenance did not converge")
}

func maintainTestUntilTenantPhase(
	ctx context.Context,
	c *Catalog,
	tenant TenantID,
	target Revision,
	phase MaintenancePhase,
) (MaintenanceResult, error) {
	if err := prepareTestMaintenance(ctx, c, tenant, target); err != nil {
		return MaintenanceResult{}, err
	}
	for step := 0; step < 10000; step++ {
		result, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow())
		if err != nil {
			return MaintenanceResult{}, err
		}
		if result.Phase == phase || !result.More {
			return result, nil
		}
	}
	return MaintenanceResult{}, errors.New("catalog: test tenant maintenance did not converge")
}
