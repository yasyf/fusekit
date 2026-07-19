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
	removal, removing, err := fileProviderDomainRemoval(ctx, tx, provision.Tenant)
	if err != nil {
		return TenantProvision{}, err
	}
	if found {
		if removing || !equalTenantProvision(existing, provision) {
			return TenantProvision{}, ErrTenantProvisionConflict
		}
		if err := tx.Commit(); err != nil {
			return TenantProvision{}, fmt.Errorf("catalog: finish tenant provision lookup: %w", err)
		}
		return existing, nil
	}
	var removedGeneration uint64
	removalErr := tx.QueryRowContext(ctx,
		`SELECT generation FROM tenant_provision_removals WHERE tenant = ?`, string(provision.Tenant)).Scan(&removedGeneration)
	if removalErr != nil && !errors.Is(removalErr, sql.ErrNoRows) {
		return TenantProvision{}, fmt.Errorf("catalog: read tenant provision removal fence: %w", removalErr)
	}
	if removalErr == nil && provision.Generation <= Generation(removedGeneration) {
		return TenantProvision{}, ErrTenantProvisionConflict
	}
	if removing {
		if !removal.ConfirmedAbsent {
			return TenantProvision{}, ErrTenantProvisionConflict
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM file_provider_domain_removals WHERE tenant = ?`, string(provision.Tenant)); err != nil {
			return TenantProvision{}, fmt.Errorf("catalog: retire completed File Provider removal before reprovision: %w", err)
		}
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
    tenant, owner_id, presentation_root, backing_root, content_source_id,
    file_provider_account_id, file_provider_display_name, access_mode, generation
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(provision.Tenant), provision.OwnerID, provision.PresentationRoot, provision.BackingRoot,
		provision.ContentSourceID, provision.FileProvider.AccountInstanceID, provision.FileProvider.DisplayName,
		uint8(provision.Access), uint64(provision.Generation)); err != nil {
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
	if _, err := advanceTopologyTx(
		ctx, tx, SourceAuthorityFleetOwnerID(provision.OwnerID),
		TopologyChangeTenant, provision.Tenant, 0, 1,
	); err != nil {
		return TenantProvision{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: commit tenant provision: %w", err)
	}
	c.topology.signal()
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
	if _, removing, err := fileProviderDomainRemoval(ctx, tx, next.Tenant); err != nil {
		return TenantProvision{}, err
	} else if removing {
		return TenantProvision{}, ErrTenantProvisionConflict
	}
	if reserved, err := activeSourceDriverMutationReservation(ctx, tx, next.Tenant); err != nil {
		return TenantProvision{}, err
	} else if reserved {
		return TenantProvision{}, ErrMutationActive
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
UPDATE desired_tenants SET presentation_root = ?, backing_root = ?, content_source_id = ?,
    file_provider_account_id = ?, file_provider_display_name = ?, access_mode = ?, generation = ?
WHERE tenant = ? AND generation = ?`, next.PresentationRoot, next.BackingRoot, next.ContentSourceID,
		next.FileProvider.AccountInstanceID, next.FileProvider.DisplayName, uint8(next.Access), uint64(next.Generation),
		string(next.Tenant), uint64(expected))
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_provider_leases WHERE tenant = ?`, string(next.Tenant)); err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: retire replaced File Provider leases: %w", err)
	}
	if _, err := advanceTopologyTx(
		ctx, tx, SourceAuthorityFleetOwnerID(next.OwnerID),
		TopologyChangeTenant, next.Tenant, 0, 0,
	); err != nil {
		return TenantProvision{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantProvision{}, fmt.Errorf("catalog: commit tenant provision replacement: %w", err)
	}
	c.topology.signal()
	next.Root = current.Root
	return next, nil
}

// RemoveTenantProvision removes one exact desired generation without deleting
// catalog history.
func (c *Catalog) RemoveTenantProvision(ctx context.Context, tenant TenantID, generation Generation) error {
	if tenant == "" || generation == 0 {
		return fmt.Errorf("%w: tenant and generation are required", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin tenant provision removal: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	owner, changed, err := removeTenantProvisionTx(ctx, tx, tenant, generation)
	if err != nil {
		return err
	}
	if changed {
		if _, err := advanceTopologyTx(
			ctx, tx, SourceAuthorityFleetOwnerID(owner), TopologyChangeTenant, tenant, 0, -1,
		); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit tenant provision removal: %w", err)
	}
	if changed {
		c.topology.signal()
	}
	return nil
}

func removeTenantProvisionTx(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	generation Generation,
) (string, bool, error) {
	if reserved, err := activeSourceDriverMutationReservation(ctx, tx, tenant); err != nil {
		return "", false, err
	} else if reserved {
		return "", false, ErrMutationActive
	}
	var currentGeneration uint64
	var owner string
	err := tx.QueryRowContext(ctx, `SELECT owner_id, generation FROM desired_tenants WHERE tenant = ?`, string(tenant)).Scan(&owner, &currentGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		var removed uint64
		err = tx.QueryRowContext(ctx, `SELECT generation FROM tenant_provision_removals WHERE tenant = ?`, string(tenant)).Scan(&removed)
		if err == nil && Generation(removed) == generation {
			return "", false, nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return "", false, fmt.Errorf("catalog: read removed tenant generation: %w", err)
		}
		return "", false, ErrNotFound
	}
	if err != nil {
		return "", false, fmt.Errorf("catalog: read tenant generation for removal: %w", err)
	}
	if Generation(currentGeneration) != generation {
		return "", false, ErrNotFound
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM desired_tenants WHERE tenant = ? AND generation = ?`, string(tenant), uint64(generation))
	if err != nil {
		return "", false, fmt.Errorf("catalog: remove tenant provision: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return "", false, fmt.Errorf("catalog: inspect removed tenant provision: %w", err)
	}
	if changed != 1 {
		return "", false, ErrNotFound
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM file_provider_leases WHERE tenant = ?`, string(tenant)); err != nil {
		return "", false, fmt.Errorf("catalog: retire removed File Provider leases: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_provision_removals(tenant, generation) VALUES (?, ?)
ON CONFLICT(tenant) DO UPDATE SET generation = excluded.generation
WHERE tenant_provision_removals.generation < excluded.generation`, string(tenant), uint64(generation)); err != nil {
		return "", false, fmt.Errorf("catalog: record removed tenant generation: %w", err)
	}
	return owner, true, nil
}

func activeSourceDriverMutationReservation(ctx context.Context, tx *sql.Tx, tenant TenantID) (bool, error) {
	var active int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_driver_mutation_reservations
    WHERE mutation_tenant = ? AND committed = 0
)`,
		string(tenant)).Scan(&active); err != nil {
		return false, fmt.Errorf("catalog: inspect source driver mutation reservation: %w", err)
	}
	return active != 0, nil
}

const (
	// TenantProvisionRecordMaxBytes bounds one desired-tenant wire record.
	TenantProvisionRecordMaxBytes = 4 << 10
)

func tenantProvision(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant TenantID) (TenantProvision, bool, error) {
	row := query.QueryRowContext(ctx, `
SELECT d.tenant, t.root_id, d.owner_id, d.presentation_root, d.backing_root,
       d.content_source_id, d.file_provider_account_id, d.file_provider_display_name,
       d.access_mode, t.case_policy, t.presentation_set, d.generation
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
		&provision.BackingRoot, &provision.ContentSourceID,
		&provision.FileProvider.AccountInstanceID, &provision.FileProvider.DisplayName,
		&access, &policy, &presentations, &generation)
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
	case provision.Presentations.Has(PresentationFileProvider) != provision.FileProvider.Enabled():
		return fmt.Errorf("%w: File Provider presentation metadata does not match presentation set", ErrInvalidObject)
	case strings.ContainsRune(provision.FileProvider.AccountInstanceID, 0) || strings.ContainsRune(provision.FileProvider.DisplayName, 0):
		return fmt.Errorf("%w: File Provider presentation metadata contains NUL", ErrInvalidObject)
	case tenantProvisionRecordBytes(provision) > TenantProvisionRecordMaxBytes:
		return fmt.Errorf("%w: tenant provision exceeds raw byte limit", ErrInvalidObject)
	default:
		return nil
	}
}

func tenantProvisionRecordBytes(provision TenantProvision) int {
	return len(provision.OwnerID) + len(provision.Tenant) + len(provision.PresentationRoot) +
		len(provision.BackingRoot) + len(provision.ContentSourceID) +
		len(provision.FileProvider.AccountInstanceID) + len(provision.FileProvider.DisplayName) + 64
}

func exactAbsolutePath(value string) bool {
	return value != "" && !strings.ContainsRune(value, 0) && filepath.IsAbs(value) && filepath.Clean(value) == value
}

func equalTenantProvision(left, right TenantProvision) bool {
	right.Root = left.Root
	return left == right
}
