package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	provisionAfterBegin   = "provision.after_begin"
	provisionAfterCatalog = "provision.after_catalog"
	provisionBeforeCommit = "provision.before_commit"
	provisionAfterCommit  = "provision.after_commit"
)

// ProvisionTenant atomically creates catalog identity, the stable root,
// initial convergence state, and one durable desired tenant definition.
func (c *Catalog) ProvisionTenant(ctx context.Context, provision TenantProvision) (TenantProvision, error) {
	if err := validateTenantProvision(provision); err != nil {
		return TenantProvision{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: begin tenant provision: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := c.trip(provisionAfterBegin); err != nil {
		return TenantProvision{}, err
	}
	existing, found, err := tenantProvision(ctx, tx, provision.Tenant)
	if err != nil {
		return TenantProvision{}, err
	}
	if found {
		if !equalTenantProvision(existing, provision) {
			return TenantProvision{}, ErrTenantProvisionConflict
		}
		if err := tx.Commit(); err != nil {
			return TenantProvision{}, fmt.Errorf("catalog: finish tenant provision lookup: %w", err)
		}
		return existing, nil
	}
	root, policy, presentations, retained, err := retainedTenant(ctx, tx, provision.Tenant)
	if err != nil {
		return TenantProvision{}, err
	}
	if retained {
		if policy != provision.CasePolicy || presentations != provision.Presentations {
			return TenantProvision{}, ErrTenantProvisionConflict
		}
		provision.Root = root
	} else {
		root, err = NewObjectID()
		if err != nil {
			return TenantProvision{}, err
		}
		provision.Root = root
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenants(tenant, root_id, case_policy, presentation_set, head, floor)
VALUES (?, ?, ?, ?, 1, 0)`, string(provision.Tenant), root[:], uint8(provision.CasePolicy), uint8(provision.Presentations)); err != nil {
			return TenantProvision{}, mapConstraint(err)
		}
		rootObject := Object{
			Tenant: provision.Tenant, ID: root, Parent: root, Revision: 1,
			MetadataRevision: 1, Name: "", Kind: KindDirectory,
			Visibility: Visibility{
				Mount:        provision.Presentations.Has(PresentationMount),
				FileProvider: provision.Presentations.Has(PresentationFileProvider),
			},
		}
		if err := writeNewObject(ctx, tx, rootObject); err != nil {
			return TenantProvision{}, err
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO desired_tenants(
    tenant, owner_id, presentation_root, backing_root, content_source_id, access_mode, generation
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		string(provision.Tenant), provision.OwnerID, provision.PresentationRoot, provision.BackingRoot,
		provision.ContentSourceID, uint8(provision.Access), uint64(provision.Generation)); err != nil {
		return TenantProvision{}, mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_state(
    tenant, generation, activated_generation, desired_revision, observed_revision,
    verified_revision, applied_revision, version
) VALUES (?, ?, 0, 0, 0, 0, 0, 1)
ON CONFLICT(tenant) DO UPDATE SET
    generation = excluded.generation, activated_generation = 0,
    desired_revision = 0, observed_revision = 0, verified_revision = 0,
    applied_revision = 0, version = tenant_state.version + 1,
    quarantine_lane = NULL, quarantine_revision = NULL, quarantine_cause = NULL,
    quarantine_detail = NULL, quarantine_since = NULL`, string(provision.Tenant), uint64(provision.Generation)); err != nil {
		return TenantProvision{}, mapConstraint(err)
	}
	if err := c.trip(provisionAfterCatalog); err != nil {
		return TenantProvision{}, err
	}
	if err := c.trip(provisionBeforeCommit); err != nil {
		return TenantProvision{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: commit tenant provision: %w", err)
	}
	if err := c.trip(provisionAfterCommit); err != nil {
		return TenantProvision{}, err
	}
	return provision, nil
}

func retainedTenant(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant TenantID) (ObjectID, CasePolicy, PresentationSet, bool, error) {
	var raw []byte
	var policy, presentations uint8
	err := query.QueryRowContext(ctx, `
SELECT root_id, case_policy, presentation_set FROM tenants WHERE tenant = ?`, string(tenant)).
		Scan(&raw, &policy, &presentations)
	if errors.Is(err, sql.ErrNoRows) {
		return ObjectID{}, 0, 0, false, nil
	}
	if err != nil {
		return ObjectID{}, 0, 0, false, fmt.Errorf("catalog: read retained tenant: %w", err)
	}
	root, err := objectID(raw)
	if err != nil {
		return ObjectID{}, 0, 0, false, err
	}
	return root, CasePolicy(policy), PresentationSet(presentations), true, nil
}

// ReplaceTenantProvision atomically advances one durable desired generation.
func (c *Catalog) ReplaceTenantProvision(ctx context.Context, expected Generation, next TenantProvision) (TenantProvision, error) {
	if expected == 0 {
		return TenantProvision{}, fmt.Errorf("%w: expected generation is required", ErrInvalidObject)
	}
	if err := validateTenantProvision(next); err != nil {
		return TenantProvision{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: begin tenant provision replacement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, found, err := tenantProvision(ctx, tx, next.Tenant)
	if err != nil {
		return TenantProvision{}, err
	}
	if !found {
		return TenantProvision{}, ErrNotFound
	}
	if equalTenantProvision(current, next) {
		if err := tx.Commit(); err != nil {
			return TenantProvision{}, fmt.Errorf("catalog: finish tenant replacement lookup: %w", err)
		}
		return current, nil
	}
	if current.Generation != expected || next.Generation <= expected {
		return TenantProvision{}, ErrTenantProvisionConflict
	}
	if current.OwnerID != next.OwnerID || current.CasePolicy != next.CasePolicy || current.Presentations != next.Presentations {
		return TenantProvision{}, ErrTenantProvisionConflict
	}
	result, err := tx.ExecContext(ctx, `
UPDATE desired_tenants SET presentation_root = ?, backing_root = ?, content_source_id = ?, access_mode = ?, generation = ?
WHERE tenant = ? AND generation = ?`, next.PresentationRoot, next.BackingRoot, next.ContentSourceID,
		uint8(next.Access), uint64(next.Generation), string(next.Tenant), uint64(expected))
	if err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: replace tenant provision: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: inspect tenant provision replacement: %w", err)
	}
	if changed != 1 {
		return TenantProvision{}, ErrTenantProvisionConflict
	}
	stateResult, err := tx.ExecContext(ctx, `
UPDATE tenant_state SET generation = ?, activated_generation = 0,
    desired_revision = 0, observed_revision = 0, verified_revision = 0,
    applied_revision = 0, version = version + 1,
    quarantine_lane = NULL, quarantine_revision = NULL, quarantine_cause = NULL,
    quarantine_detail = NULL, quarantine_since = NULL
WHERE tenant = ? AND generation = ?`, uint64(next.Generation), string(next.Tenant), uint64(expected))
	if err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: reset replaced tenant state: %w", err)
	}
	stateChanged, err := stateResult.RowsAffected()
	if err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: inspect reset tenant state: %w", err)
	}
	if stateChanged != 1 {
		return TenantProvision{}, ErrTenantProvisionConflict
	}
	if err := tx.Commit(); err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: commit tenant provision replacement: %w", err)
	}
	next.Root = current.Root
	return next, nil
}

// RemoveTenantProvision removes one exact desired generation without deleting
// catalog history.
func (c *Catalog) RemoveTenantProvision(ctx context.Context, tenant TenantID, generation Generation) error {
	if tenant == "" || generation == 0 {
		return fmt.Errorf("%w: tenant and generation are required", ErrInvalidObject)
	}
	result, err := c.db.ExecContext(ctx, `DELETE FROM desired_tenants WHERE tenant = ? AND generation = ?`, string(tenant), uint64(generation))
	if err != nil {
		return fmt.Errorf("catalog: remove tenant provision: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect removed tenant provision: %w", err)
	}
	if changed != 1 {
		return ErrNotFound
	}
	return nil
}

// TenantProvisions returns every durable desired tenant ordered by identity.
func (c *Catalog) TenantProvisions(ctx context.Context) ([]TenantProvision, error) {
	rows, err := c.readDB.QueryContext(ctx, `
SELECT d.tenant, t.root_id, d.owner_id, d.presentation_root, d.backing_root,
       d.content_source_id, d.access_mode, t.case_policy, t.presentation_set, d.generation
FROM desired_tenants d JOIN tenants t ON t.tenant = d.tenant
ORDER BY d.tenant`)
	if err != nil {
		return nil, fmt.Errorf("catalog: list tenant provisions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var provisions []TenantProvision
	for rows.Next() {
		provision, err := scanTenantProvision(rows)
		if err != nil {
			return nil, err
		}
		provisions = append(provisions, provision)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: list tenant provisions: %w", err)
	}
	return provisions, nil
}

func tenantProvision(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant TenantID) (TenantProvision, bool, error) {
	row := query.QueryRowContext(ctx, `
SELECT d.tenant, t.root_id, d.owner_id, d.presentation_root, d.backing_root,
       d.content_source_id, d.access_mode, t.case_policy, t.presentation_set, d.generation
FROM desired_tenants d JOIN tenants t ON t.tenant = d.tenant
WHERE d.tenant = ?`, string(tenant))
	provision, err := scanTenantProvision(row)
	if errors.Is(err, sql.ErrNoRows) {
		return TenantProvision{}, false, nil
	}
	if err != nil {
		return TenantProvision{}, false, err
	}
	return provision, true, nil
}

type provisionScanner interface{ Scan(...any) error }

func scanTenantProvision(scanner provisionScanner) (TenantProvision, error) {
	var provision TenantProvision
	var tenant string
	var root []byte
	var access, policy, presentations uint8
	var generation uint64
	err := scanner.Scan(&tenant, &root, &provision.OwnerID, &provision.PresentationRoot,
		&provision.BackingRoot, &provision.ContentSourceID, &access, &policy, &presentations, &generation)
	if err != nil {
		return TenantProvision{}, err
	}
	provision.Tenant = TenantID(tenant)
	parsedRoot, err := objectID(root)
	if err != nil {
		return TenantProvision{}, err
	}
	provision.Root = parsedRoot
	provision.Access = TenantAccessMode(access)
	provision.CasePolicy = CasePolicy(policy)
	provision.Presentations = PresentationSet(presentations)
	provision.Generation = Generation(generation)
	if err := validateTenantProvision(provision); err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: corrupt tenant provision: %w", err)
	}
	return provision, nil
}

func validateTenantProvision(provision TenantProvision) error {
	switch {
	case provision.OwnerID == "" || provision.Tenant == "" || provision.ContentSourceID == "":
		return fmt.Errorf("%w: tenant provision identity is incomplete", ErrInvalidObject)
	case !exactAbsolutePath(provision.PresentationRoot) || !exactAbsolutePath(provision.BackingRoot):
		return fmt.Errorf("%w: tenant provision paths must be exact and absolute", ErrInvalidObject)
	case provision.Access != TenantReadOnly && provision.Access != TenantReadWrite:
		return fmt.Errorf("%w: invalid tenant access mode %d", ErrInvalidObject, provision.Access)
	case provision.CasePolicy != CaseSensitive && provision.CasePolicy != CaseInsensitive:
		return fmt.Errorf("%w: invalid tenant case policy %d", ErrInvalidObject, provision.CasePolicy)
	case !provision.Presentations.valid() || provision.Generation == 0:
		return fmt.Errorf("%w: invalid tenant presentations or generation", ErrInvalidObject)
	default:
		return nil
	}
}

func exactAbsolutePath(value string) bool {
	return value != "" && !strings.ContainsRune(value, 0) && filepath.IsAbs(value) && filepath.Clean(value) == value
}

func equalTenantProvision(left, right TenantProvision) bool {
	right.Root = left.Root
	return left == right
}
