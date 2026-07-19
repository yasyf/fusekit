package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// LoadTenantState returns the tenant's current CAS-protected runtime state.
func (c *Catalog) LoadTenantState(ctx context.Context, tenant TenantID) (TenantStateRecord, error) {
	record, err := loadTenantState(ctx, c.readDB, tenant)
	if errors.Is(err, sql.ErrNoRows) {
		return TenantStateRecord{}, ErrStateNotFound
	}
	if err != nil {
		return TenantStateRecord{}, fmt.Errorf("catalog: load tenant state: %w", err)
	}
	return record, nil
}

// SaveTenantState atomically persists record when expected matches its current version.
func (c *Catalog) SaveTenantState(ctx context.Context, expected StateVersion, record TenantStateRecord) (TenantStateRecord, error) {
	if record.Version != expected {
		return TenantStateRecord{}, fmt.Errorf("%w: record version %d, expected %d", ErrStateConflict, record.Version, expected)
	}
	if err := validateTenantState(record); err != nil {
		return TenantStateRecord{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantStateRecord{}, fmt.Errorf("catalog: begin tenant state CAS: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	current, loadErr := loadTenantState(ctx, tx, record.Tenant)
	switch {
	case expected == 0 && loadErr == nil:
		return TenantStateRecord{}, ErrStateConflict
	case expected == 0 && errors.Is(loadErr, sql.ErrNoRows):
		record.Version = 1
		if err := insertTenantState(ctx, tx, record); err != nil {
			return TenantStateRecord{}, err
		}
	case loadErr != nil && errors.Is(loadErr, sql.ErrNoRows):
		return TenantStateRecord{}, ErrStateNotFound
	case loadErr != nil:
		return TenantStateRecord{}, fmt.Errorf("catalog: load tenant state for CAS: %w", loadErr)
	case current.Version != expected:
		return TenantStateRecord{}, ErrStateConflict
	default:
		if err := validateTenantStateTransition(current, record); err != nil {
			return TenantStateRecord{}, err
		}
		record.Version = expected + 1
		result, err := updateTenantState(ctx, tx, expected, record)
		if err != nil {
			return TenantStateRecord{}, err
		}
		if result != 1 {
			return TenantStateRecord{}, ErrStateConflict
		}
	}
	if err := tx.Commit(); err != nil {
		return TenantStateRecord{}, fmt.Errorf("catalog: commit tenant state CAS: %w", err)
	}
	return record, nil
}

type tenantStateScanner interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadTenantState(ctx context.Context, q tenantStateScanner, tenant TenantID) (TenantStateRecord, error) {
	var record TenantStateRecord
	var rawTenant string
	var generation, activated, desired, observed, verified, applied, version uint64
	var lane, revision, cause, since sql.NullInt64
	var detail sql.NullString
	err := q.QueryRowContext(ctx, `
SELECT tenant, generation, activated_generation, desired_revision, observed_revision, verified_revision,
       applied_revision, version, quarantine_lane, quarantine_revision,
       quarantine_cause, quarantine_detail, quarantine_since
FROM tenant_state WHERE tenant = ?`, string(tenant)).Scan(
		&rawTenant, &generation, &activated, &desired, &observed, &verified, &applied, &version,
		&lane, &revision, &cause, &detail, &since,
	)
	if err != nil {
		return TenantStateRecord{}, err
	}
	record = TenantStateRecord{
		Tenant: TenantID(rawTenant), Generation: Generation(generation),
		ActivatedGeneration: Generation(activated),
		Desired:             Revision(desired), Observed: Revision(observed),
		Verified: Revision(verified), Applied: Revision(applied),
		Version: StateVersion(version),
	}
	if lane.Valid {
		record.Quarantine = &Quarantine{
			Lane: QuarantineLane(lane.Int64), Revision: Revision(revision.Int64),
			Cause: QuarantineCause(cause.Int64), Detail: detail.String,
			Since: time.Unix(0, since.Int64).UTC(),
		}
	}
	return record, nil
}

func insertTenantState(ctx context.Context, tx *sql.Tx, record TenantStateRecord) error {
	args := tenantStateArgs(record)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_state(
    tenant, generation, activated_generation, desired_revision, observed_revision, verified_revision,
    applied_revision, version, quarantine_lane, quarantine_revision,
    quarantine_cause, quarantine_detail, quarantine_since
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, args...); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func updateTenantState(ctx context.Context, tx *sql.Tx, expected StateVersion, record TenantStateRecord) (int64, error) {
	args := tenantStateArgs(record)[1:]
	args = append(args, string(record.Tenant), uint64(expected))
	result, err := tx.ExecContext(ctx, `
UPDATE tenant_state SET
    generation = ?, activated_generation = ?, desired_revision = ?, observed_revision = ?,
    verified_revision = ?, applied_revision = ?, version = ?,
    quarantine_lane = ?, quarantine_revision = ?, quarantine_cause = ?,
    quarantine_detail = ?, quarantine_since = ?
WHERE tenant = ? AND version = ?`, args...)
	if err != nil {
		return 0, fmt.Errorf("catalog: update tenant state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("catalog: inspect tenant state CAS: %w", err)
	}
	return rows, nil
}

func tenantStateArgs(record TenantStateRecord) []any {
	var lane, revision, cause, detail, since any
	if record.Quarantine != nil {
		lane = uint8(record.Quarantine.Lane)
		revision = uint64(record.Quarantine.Revision)
		cause = uint8(record.Quarantine.Cause)
		detail = record.Quarantine.Detail
		since = record.Quarantine.Since.UnixNano()
	}
	return []any{
		string(record.Tenant), uint64(record.Generation), uint64(record.ActivatedGeneration), uint64(record.Desired),
		uint64(record.Observed), uint64(record.Verified), uint64(record.Applied),
		uint64(record.Version), lane, revision, cause, detail, since,
	}
}

func validateTenantState(record TenantStateRecord) error {
	if _, err := NewTenantID(string(record.Tenant)); err != nil {
		return err
	}
	if record.Generation == 0 {
		return fmt.Errorf("%w: tenant generation is zero", ErrInvalidTransition)
	}
	if record.ActivatedGeneration != 0 && record.ActivatedGeneration != record.Generation {
		return fmt.Errorf("%w: activated generation does not match tenant generation", ErrInvalidTransition)
	}
	if err := validateProofOrder(record.Desired, record.Observed, record.Verified, record.Applied); err != nil {
		return err
	}
	if record.Quarantine == nil {
		return nil
	}
	q := record.Quarantine
	if q.Lane < QuarantineLaneCatalogMutation || q.Lane > QuarantineLaneMountLifecycle {
		return fmt.Errorf("%w: unknown quarantine lane %d", ErrInvalidTransition, q.Lane)
	}
	if q.Cause < QuarantineCauseConflict || q.Cause > QuarantineCauseUnavailable {
		return fmt.Errorf("%w: unknown quarantine cause %d", ErrInvalidTransition, q.Cause)
	}
	if q.Revision == 0 || q.Detail == "" || q.Since.IsZero() {
		return fmt.Errorf("%w: incomplete quarantine", ErrInvalidTransition)
	}
	return nil
}

func validateTenantStateTransition(current, next TenantStateRecord) error {
	if next.Generation < current.Generation {
		return fmt.Errorf("%w: tenant generation regressed", ErrInvalidTransition)
	}
	if next.Generation != current.Generation {
		return nil
	}
	if current.ActivatedGeneration != 0 && next.ActivatedGeneration != current.ActivatedGeneration {
		return fmt.Errorf("%w: activated generation changed within tenant generation", ErrInvalidTransition)
	}
	if next.Desired < current.Desired || next.Observed < current.Observed ||
		next.Verified < current.Verified || next.Applied < current.Applied {
		return fmt.Errorf("%w: tenant convergence regressed", ErrInvalidTransition)
	}
	return nil
}

func validateProofOrder(desired, observed, verified, applied Revision) error {
	if applied > verified || verified > observed || observed > desired {
		return fmt.Errorf("%w: convergence proof order", ErrInvalidTransition)
	}
	return nil
}
