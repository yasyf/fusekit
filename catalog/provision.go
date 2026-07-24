package catalog

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

const (
	// TenantProvisionRecordMaxBytes bounds one desired-tenant wire record.
	TenantProvisionRecordMaxBytes = 4 << 10
)

// ProvisionTenant durably declares one exact Present generation and ensures its
// retained catalog namespace before a runtime actor can be constructed.
func (c *Catalog) ProvisionTenant(ctx context.Context, provision TenantProvision) (TenantProvision, error) {
	canonical, err := canonicalizeTenantProvision(provision)
	if err != nil {
		return TenantProvision{}, err
	}
	for attempt := 0; attempt < 8; attempt++ {
		state, found, err := loadTenantLifecycle(ctx, c.readDB, provision.Tenant)
		if err != nil {
			return TenantProvision{}, err
		}
		if found {
			if state.OwnerID != provision.OwnerID || state.Intent.Kind != TenantIntentPresent || state.Target == nil ||
				state.Target.Definition.Generation != provision.Generation || state.Target.SpecHash != canonical.SpecHash {
				return TenantProvision{}, ErrTenantProvisionConflict
			}
			persisted, err := c.ensureProvisionedNamespace(ctx, state)
			if errors.Is(err, ErrTenantLifecycleStale) {
				continue
			}
			return persisted, err
		}
		mutation, err := c.newProvisionMutation(provision.OwnerID, 0)
		if err != nil {
			return TenantProvision{}, err
		}
		state, err = c.SetTenantPresent(ctx, mutation, provision)
		if errors.Is(err, ErrTenantLifecycleStale) {
			continue
		}
		if err != nil {
			return TenantProvision{}, err
		}
		persisted, err := c.ensureProvisionedNamespace(ctx, state)
		if errors.Is(err, ErrTenantLifecycleStale) {
			continue
		}
		return persisted, err
	}
	return TenantProvision{}, ErrTenantLifecycleStale
}

// ReplaceTenantProvision durably advances one exact Present generation while
// leaving the old active generation serving until activation flips the pointer.
func (c *Catalog) ReplaceTenantProvision(
	ctx context.Context,
	expected Generation,
	next TenantProvision,
) (TenantProvision, error) {
	if expected == 0 {
		return TenantProvision{}, fmt.Errorf("%w: expected generation is required", ErrInvalidObject)
	}
	canonical, err := canonicalizeTenantProvision(next)
	if err != nil {
		return TenantProvision{}, err
	}
	for attempt := 0; attempt < 8; attempt++ {
		state, found, err := loadTenantLifecycle(ctx, c.readDB, next.Tenant)
		if err != nil {
			return TenantProvision{}, err
		}
		if !found {
			return TenantProvision{}, ErrNotFound
		}
		if state.OwnerID != next.OwnerID || state.Intent.Kind != TenantIntentPresent || state.Target == nil {
			return TenantProvision{}, ErrTenantProvisionConflict
		}
		if state.Target.Definition.Generation == next.Generation {
			if state.Target.SpecHash != canonical.SpecHash {
				return TenantProvision{}, ErrTenantProvisionConflict
			}
			persisted, err := c.ensureProvisionedNamespace(ctx, state)
			if errors.Is(err, ErrTenantLifecycleStale) {
				continue
			}
			return persisted, err
		}
		if state.Target.Definition.Generation != expected || next.Generation <= expected {
			return TenantProvision{}, ErrTenantProvisionConflict
		}
		if state.Target.Definition.CasePolicy != next.CasePolicy ||
			state.Target.Definition.Presentations != next.Presentations {
			return TenantProvision{}, ErrTenantProvisionConflict
		}
		mutation, err := c.newProvisionMutation(next.OwnerID, state.Intent.Revision)
		if err != nil {
			return TenantProvision{}, err
		}
		state, err = c.SetTenantPresent(ctx, mutation, next)
		if errors.Is(err, ErrTenantLifecycleStale) {
			continue
		}
		if err != nil {
			return TenantProvision{}, err
		}
		persisted, err := c.ensureProvisionedNamespace(ctx, state)
		if errors.Is(err, ErrTenantLifecycleStale) {
			continue
		}
		return persisted, err
	}
	return TenantProvision{}, ErrTenantLifecycleStale
}

// RemoveTenantProvision durably declares one exact generation Absent. Serving
// state remains pinned until presentation retirement and activation clearing.
func (c *Catalog) RemoveTenantProvision(ctx context.Context, tenant TenantID, expected Generation) error {
	if tenant == "" || expected == 0 {
		return fmt.Errorf("%w: tenant and generation are required", ErrInvalidObject)
	}
	for attempt := 0; attempt < 8; attempt++ {
		state, found, err := loadTenantLifecycle(ctx, c.readDB, tenant)
		if err != nil {
			return err
		}
		if !found {
			return ErrNotFound
		}
		if state.Intent.Kind == TenantIntentAbsent {
			var latest uint64
			if err := c.readDB.QueryRowContext(ctx,
				`SELECT COALESCE(MAX(generation), 0) FROM tenant_generations WHERE tenant_id = ?`,
				string(tenant)).Scan(&latest); err != nil {
				return err
			}
			if Generation(latest) == expected {
				return nil
			}
			return ErrNotFound
		}
		if state.Target == nil || state.Target.Definition.Generation != expected {
			return ErrNotFound
		}
		mutation, err := c.newProvisionMutation(state.OwnerID, state.Intent.Revision)
		if err != nil {
			return err
		}
		if _, err := c.SetTenantAbsent(ctx, mutation, tenant); errors.Is(err, ErrTenantLifecycleStale) {
			continue
		} else if err != nil {
			return err
		}
		return nil
	}
	return ErrTenantLifecycleStale
}

func (c *Catalog) ensureProvisionedNamespace(
	ctx context.Context,
	state TenantLifecycleState,
) (TenantProvision, error) {
	if state.Target == nil || state.Intent.Kind != TenantIntentPresent {
		return TenantProvision{}, ErrTenantLifecycleStale
	}
	namespace, err := c.EnsureTenantNamespace(ctx, EnsureTenantNamespaceRequest{
		OwnerID: state.OwnerID, Tenant: state.Intent.Tenant,
		Generation: state.Intent.TargetGeneration, IntentRevision: state.Intent.Revision,
	})
	if err != nil {
		return TenantProvision{}, err
	}
	persisted := state.Target.Definition
	persisted.Root = namespace.Root
	return persisted, nil
}

func (c *Catalog) newProvisionMutation(owner string, revision TenantIntentRevision) (TenantMutation, error) {
	operation, err := NewTenantOperationID()
	if err != nil {
		return TenantMutation{}, err
	}
	return TenantMutation{
		OperationID: operation, HolderRuntimeGeneration: "catalog-" + hex.EncodeToString(c.owner[:]),
		OwnerID: owner, ExpectedIntentRevision: revision,
	}, nil
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

func appliedTenantProvision(ctx context.Context, query interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, tenant TenantID) (TenantProvision, bool, error) {
	row := query.QueryRowContext(ctx, `
SELECT generation.tenant_id, tenant.root_id, generation.owner_id,
       generation.mount_presentation_root, generation.backing_root,
       generation.content_source_id, generation.file_provider_presentation_instance_id,
       generation.file_provider_display_name, generation.access_mode, generation.case_policy,
       generation.presentation_set, generation.generation
FROM tenant_activations activation
JOIN tenant_generations generation
  ON generation.tenant_id = activation.tenant_id
 AND generation.generation = activation.active_generation
JOIN tenants tenant ON tenant.tenant = activation.tenant_id
JOIN tenant_applications application
  ON application.tenant_id = activation.tenant_id
 AND application.generation = activation.active_generation
 AND application.staged_view_id = activation.active_view_id
WHERE activation.tenant_id = ?
  AND activation.active_generation IS NOT NULL
  AND application.phase = ?
  AND application.staged_catalog_head = activation.active_catalog_head
  AND (
      SELECT COALESCE(SUM(CASE presentation.backend WHEN 1 THEN 1 WHEN 2 THEN 2 ELSE 0 END), 0)
      FROM presentation_materializations presentation
      WHERE presentation.tenant_id = activation.tenant_id
        AND presentation.generation = activation.active_generation
        AND presentation.phase = ?
        AND presentation.staged_view_id = activation.active_view_id
        AND presentation.staged_view_digest = application.staged_view_digest
        AND presentation.observed_revision = activation.active_catalog_head
  ) = generation.required_backends
  AND (
      SELECT COUNT(*) FROM presentation_materializations presentation
      WHERE presentation.tenant_id = activation.tenant_id
        AND presentation.generation = activation.active_generation
        AND presentation.phase = ?
        AND presentation.staged_view_id = activation.active_view_id
        AND presentation.staged_view_digest = application.staged_view_digest
        AND presentation.observed_revision = activation.active_catalog_head
  ) = CASE generation.required_backends WHEN 3 THEN 2 ELSE 1 END
  AND NOT EXISTS (
      SELECT 1 FROM presentation_materializations presentation
      WHERE presentation.tenant_id = activation.tenant_id
        AND presentation.generation = activation.active_generation
        AND (presentation.phase <> ?
          OR presentation.staged_view_id <> activation.active_view_id
          OR presentation.staged_view_digest <> application.staged_view_digest
          OR presentation.observed_revision <> activation.active_catalog_head)
  )`, string(tenant), uint8(TenantApplicationStaged), uint8(PresentationMaterializationActive),
		uint8(PresentationMaterializationActive), uint8(PresentationMaterializationActive))
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
	err := scanner.Scan(&tenant, &root, &provision.OwnerID, &provision.Mount.PresentationRoot,
		&provision.BackingRoot, &provision.ContentSourceID,
		&provision.FileProvider.PresentationInstanceID, &provision.FileProvider.DisplayName,
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
	case provision.Presentations.Has(PresentationMount) != provision.Mount.Enabled():
		return fmt.Errorf("%w: mount presentation metadata does not match presentation set", ErrInvalidObject)
	case provision.Mount.Enabled() && !exactAbsolutePath(provision.Mount.PresentationRoot):
		return fmt.Errorf("%w: mount presentation root must be exact and absolute", ErrInvalidObject)
	case !exactAbsolutePath(provision.BackingRoot):
		return fmt.Errorf("%w: tenant provision paths must be exact and absolute", ErrInvalidObject)
	case provision.Access != TenantReadOnly && provision.Access != TenantReadWrite:
		return fmt.Errorf("%w: invalid tenant access mode %d", ErrInvalidObject, provision.Access)
	case provision.CasePolicy != CaseSensitive && provision.CasePolicy != CaseInsensitive:
		return fmt.Errorf("%w: invalid tenant case policy %d", ErrInvalidObject, provision.CasePolicy)
	case !provision.Presentations.valid() || provision.Generation == 0:
		return fmt.Errorf("%w: invalid tenant presentations or generation", ErrInvalidObject)
	case provision.Presentations.Has(PresentationFileProvider) != provision.FileProvider.Enabled():
		return fmt.Errorf("%w: File Provider presentation metadata does not match presentation set", ErrInvalidObject)
	case strings.ContainsRune(provision.FileProvider.PresentationInstanceID, 0) || strings.ContainsRune(provision.FileProvider.DisplayName, 0):
		return fmt.Errorf("%w: File Provider presentation metadata contains NUL", ErrInvalidObject)
	case tenantProvisionRecordBytes(provision) > TenantProvisionRecordMaxBytes:
		return fmt.Errorf("%w: tenant provision exceeds raw byte limit", ErrInvalidObject)
	default:
		return nil
	}
}

func tenantProvisionRecordBytes(provision TenantProvision) int {
	return len(provision.OwnerID) + len(provision.Tenant) + len(provision.Mount.PresentationRoot) +
		len(provision.BackingRoot) + len(provision.ContentSourceID) +
		len(provision.FileProvider.PresentationInstanceID) + len(provision.FileProvider.DisplayName) + 64
}

func exactAbsolutePath(value string) bool {
	return value != "" && !strings.ContainsRune(value, 0) && filepath.IsAbs(value) && filepath.Clean(value) == value
}
