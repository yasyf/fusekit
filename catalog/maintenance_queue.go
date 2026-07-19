package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const maintenanceClaimPageLimit = 256

// MaintenanceTask is one exact durable tenant-maintenance claim.
type MaintenanceTask struct {
	Tenant        TenantID
	DirtyRevision Revision
}

// RecoverMaintenanceClaims returns interrupted maintenance claims to the fair queue.
func (c *Catalog) RecoverMaintenanceClaims(ctx context.Context) error {
	for {
		result, err := c.db.ExecContext(ctx, `
UPDATE catalog_maintenance
SET running_revision = 0
WHERE tenant IN (
    SELECT tenant
    FROM catalog_maintenance
    WHERE running_revision <> 0
    ORDER BY ticket, tenant
    LIMIT ?
)`, maintenanceClaimPageLimit)
		if err != nil {
			return fmt.Errorf("catalog: recover maintenance claim page: %w", err)
		}
		recovered, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("catalog: inspect recovered maintenance claim page: %w", err)
		}
		if recovered < maintenanceClaimPageLimit {
			return nil
		}
	}
}

// ClaimMaintenance claims the oldest queued tenant at its latest dirty revision.
func (c *Catalog) ClaimMaintenance(ctx context.Context) (MaintenanceTask, bool, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return MaintenanceTask{}, false, fmt.Errorf("catalog: begin maintenance claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var tenant string
	var revision uint64
	err = tx.QueryRowContext(ctx, `
UPDATE catalog_maintenance
SET running_revision = dirty_revision
WHERE tenant = (
    SELECT tenant
    FROM catalog_maintenance
    WHERE running_revision = 0
    ORDER BY ticket, tenant
    LIMIT 1
)
RETURNING tenant, running_revision`).Scan(&tenant, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return MaintenanceTask{}, false, fmt.Errorf("catalog: finish empty maintenance claim: %w", err)
		}
		return MaintenanceTask{}, false, nil
	}
	if err != nil {
		return MaintenanceTask{}, false, fmt.Errorf("catalog: claim maintenance: %w", err)
	}
	task := MaintenanceTask{Tenant: TenantID(tenant), DirtyRevision: Revision(revision)}
	if _, err := NewTenantID(tenant); err != nil || task.DirtyRevision == 0 {
		return MaintenanceTask{}, false, fmt.Errorf("%w: corrupt maintenance task", ErrIntegrity)
	}
	if err := tx.Commit(); err != nil {
		return MaintenanceTask{}, false, fmt.Errorf("catalog: commit maintenance claim: %w", err)
	}
	return task, true, nil
}

// FinishMaintenance settles a claim, deleting completed work or rotating it
// behind every tenant that has not yet had a turn.
func (c *Catalog) FinishMaintenance(ctx context.Context, task MaintenanceTask, more bool) error {
	if _, err := NewTenantID(string(task.Tenant)); err != nil || task.DirtyRevision == 0 {
		return fmt.Errorf("%w: invalid maintenance task", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin maintenance settlement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var dirty, running uint64
	if err := tx.QueryRowContext(ctx, `
SELECT dirty_revision, running_revision
FROM catalog_maintenance
WHERE tenant = ?`, string(task.Tenant)).Scan(&dirty, &running); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: maintenance claim disappeared", ErrIntegrity)
		}
		return fmt.Errorf("catalog: read maintenance claim: %w", err)
	}
	if Revision(running) != task.DirtyRevision || running == 0 || dirty < running {
		return fmt.Errorf("%w: maintenance claim changed", ErrIntegrity)
	}
	if !more && dirty == running {
		result, err := tx.ExecContext(ctx, `
DELETE FROM catalog_maintenance
WHERE tenant = ? AND dirty_revision = ? AND running_revision = ?`,
			string(task.Tenant), dirty, running)
		if err != nil {
			return fmt.Errorf("catalog: complete maintenance claim: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("catalog: inspect completed maintenance claim: %w", err)
		}
		if changed != 1 {
			return fmt.Errorf("%w: maintenance completion lost its claim", ErrIntegrity)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
UPDATE catalog_maintenance_sequence
SET next_ticket = next_ticket + 1
WHERE singleton = 1`); err != nil {
			return fmt.Errorf("catalog: advance maintenance ticket: %w", err)
		}
		result, err := tx.ExecContext(ctx, `
UPDATE catalog_maintenance
SET running_revision = 0,
    ticket = (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
WHERE tenant = ? AND running_revision = ?`,
			string(task.Tenant), running)
		if err != nil {
			return fmt.Errorf("catalog: requeue maintenance claim: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("catalog: inspect requeued maintenance claim: %w", err)
		}
		if changed != 1 {
			return fmt.Errorf("%w: maintenance requeue lost its claim", ErrIntegrity)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit maintenance settlement: %w", err)
	}
	return nil
}

func enqueueCatalogMaintenance(ctx context.Context, tx *sql.Tx, tenant TenantID) error {
	if _, err := NewTenantID(string(tenant)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE catalog_maintenance_sequence
SET next_ticket = next_ticket + 1
WHERE singleton = 1
  AND NOT EXISTS (
      SELECT 1 FROM catalog_maintenance WHERE tenant = ?
  )`, string(tenant)); err != nil {
		return fmt.Errorf("catalog: advance maintenance enqueue ticket: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO catalog_maintenance(tenant, dirty_revision, running_revision, ticket)
SELECT tenant, head, 0,
       COALESCE(
           (SELECT ticket FROM catalog_maintenance WHERE tenant = tenants.tenant),
           (SELECT next_ticket FROM catalog_maintenance_sequence WHERE singleton = 1)
       )
FROM tenants
WHERE tenant = ?
ON CONFLICT(tenant) DO UPDATE SET
    dirty_revision = MAX(catalog_maintenance.dirty_revision, excluded.dirty_revision)`,
		string(tenant)); err != nil {
		return fmt.Errorf("catalog: enqueue tenant maintenance: %w", err)
	}
	return nil
}
