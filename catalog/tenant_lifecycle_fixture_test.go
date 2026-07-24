package catalog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func provisionTenantForTest(t *testing.T, c *Catalog, ctx context.Context, definition TenantProvision) (TenantProvision, error) {
	t.Helper()
	persisted, err := c.ProvisionTenant(ctx, definition)
	if err != nil {
		return TenantProvision{}, err
	}
	state, err := c.TenantLifecycle(ctx, definition.OwnerID, definition.Tenant)
	if err != nil {
		return TenantProvision{}, err
	}
	if state.Ready() {
		return persisted, nil
	}
	return applyTenantLifecycleForTest(t, c, ctx, state)
}

func replaceTenantForTest(t *testing.T, c *Catalog, ctx context.Context, expected Generation, definition TenantProvision) (TenantProvision, error) {
	t.Helper()
	persisted, err := c.ReplaceTenantProvision(ctx, expected, definition)
	if err != nil {
		return TenantProvision{}, err
	}
	state, err := c.TenantLifecycle(ctx, definition.OwnerID, definition.Tenant)
	if err != nil {
		return TenantProvision{}, err
	}
	if state.Ready() && state.Activation.ActiveGeneration == definition.Generation {
		return persisted, nil
	}
	return applyTenantLifecycleForTest(t, c, ctx, state)
}

func removeTenantForTest(t *testing.T, c *Catalog, ctx context.Context, tenant TenantID, generation Generation) error {
	t.Helper()
	if err := c.RemoveTenantProvision(ctx, tenant, generation); err != nil {
		return err
	}
	state, found, err := loadTenantLifecycle(ctx, c.readDB, tenant)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if !state.Activation.Active() {
		return nil
	}
	for _, row := range state.Presentations {
		if row.Generation != generation || row.Phase != PresentationMaterializationRetiring {
			continue
		}
		state, err = c.RetirePresentation(ctx, RetirementRequest{
			Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
			Tenant:   tenant, Generation: generation, Backend: row.Backend,
		})
		if err != nil {
			return err
		}
	}
	state, err = c.RetireApplication(ctx, RetirementRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   tenant, Generation: generation,
	})
	if err != nil {
		return err
	}
	_, err = c.ClearTenantActivation(ctx, RetirementRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   tenant, Generation: generation,
	})
	return err
}

func applyTenantLifecycleForTest(
	t *testing.T,
	c *Catalog,
	ctx context.Context,
	state TenantLifecycleState,
) (TenantProvision, error) {
	t.Helper()
	if state.Intent.Kind != TenantIntentPresent || state.Target == nil {
		return TenantProvision{}, ErrTenantProvisionConflict
	}
	definition := state.Target.Definition
	if _, err := c.EnsureTenantNamespace(ctx, EnsureTenantNamespaceRequest{
		OwnerID: state.OwnerID, Tenant: definition.Tenant, Generation: definition.Generation,
		IntentRevision: state.Intent.Revision,
	}); err != nil {
		return TenantProvision{}, err
	}
	publication, publicationDigest, err := seedTenantPublicationForTest(t, c, ctx, definition)
	if err != nil {
		return TenantProvision{}, err
	}
	lease, state, err := c.StageApplication(ctx, StageApplicationRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		Authority: causal.SourceAuthorityID(definition.ContentSourceID), Publication: publication,
		PublicationDigest: publicationDigest,
	})
	if err != nil {
		return TenantProvision{}, err
	}
	for _, backend := range state.Target.RequiredBackends.Backends() {
		state, err = c.RecordPresentation(ctx, PresentationReceipt{
			Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
			Lease:    lease, Backend: backend,
			BackendGeneration: fmt.Sprintf("test-backend-%d", backend), ObservedRevision: lease.CatalogHead,
		})
		if err != nil {
			return TenantProvision{}, err
		}
	}
	activation, err := c.ActivateTenant(ctx, ActivateTenantRequest{
		Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedActiveGeneration:   state.Activation.ActiveGeneration,
		ExpectedTargetingRevision:  mustTargetingRevision(t, c, definition.Tenant),
		CausePublications:          []causal.OperationID{publication},
	})
	if err != nil {
		return TenantProvision{}, err
	}
	state = activation.State
	for _, row := range state.Presentations {
		if row.Generation == definition.Generation || row.Phase != PresentationMaterializationRetiring {
			continue
		}
		state, err = c.RetirePresentation(ctx, RetirementRequest{
			Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
			Tenant:   definition.Tenant, Generation: row.Generation, Backend: row.Backend,
		})
		if err != nil {
			return TenantProvision{}, err
		}
	}
	for _, row := range state.Applications {
		if row.Generation == definition.Generation || row.Phase != TenantApplicationRetiring {
			continue
		}
		state, err = c.RetireApplication(ctx, RetirementRequest{
			Mutation: tenantMutationForTest(t, state.OwnerID, state.Intent.Revision),
			Tenant:   definition.Tenant, Generation: row.Generation,
		})
		if err != nil {
			return TenantProvision{}, err
		}
	}
	if !state.Ready() {
		return TenantProvision{}, fmt.Errorf("catalog test: tenant lifecycle did not become ready")
	}
	provision, found, err := appliedTenantProvision(ctx, c.readDB, definition.Tenant)
	if err != nil {
		return TenantProvision{}, err
	}
	if !found {
		return TenantProvision{}, ErrNotFound
	}
	return provision, nil
}

func seedTenantPublicationForTest(
	t *testing.T,
	c *Catalog,
	ctx context.Context,
	definition TenantProvision,
) (causal.OperationID, [sha256.Size]byte, error) {
	t.Helper()
	publication := causal.OperationID(mustTenantOperationID(t))
	sourceOperation := causal.OperationID(mustTenantOperationID(t))
	changeID := causal.ChangeID(mustTenantOperationID(t))
	publicationDigest := sha256.Sum256(append([]byte("publication:"), publication[:]...))
	identityDigest := sha256.Sum256(append([]byte("identity:"), publication[:]...))
	targetsDigest := sha256.Sum256(append([]byte("targets:"), publication[:]...))
	affectedDigest := sha256.Sum256([]byte("test-key"))
	headDigest := sha256.Sum256(append([]byte("head:"), publication[:]...))
	providerDigest := sha256.Sum256(append([]byte("provider:"), publication[:]...))
	catalogOperation := sha256.Sum256(append([]byte("catalog:"), publication[:]...))
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	defer func() { _ = tx.Rollback() }()
	var sourceRevision uint64
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(MAX(source_revision), 0) + 1
FROM source_driver_publications WHERE source_authority = ?`, definition.ContentSourceID).Scan(&sourceRevision); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	var catalogHead uint64
	if err := tx.QueryRowContext(ctx, `SELECT head FROM tenants WHERE tenant = ?`, string(definition.Tenant)).Scan(&catalogHead); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	var rootID []byte
	if err := tx.QueryRowContext(ctx, `SELECT root_id FROM tenants WHERE tenant = ?`, string(definition.Tenant)).Scan(&rootID); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	root, err := scanObject(tx.QueryRowContext(ctx, `SELECT `+objectColumns+`
FROM object_versions
WHERE tenant = ? AND object_id = ? AND revision = (
    SELECT MAX(revision) FROM object_versions
    WHERE tenant = ? AND object_id = ? AND revision <= ?
)`, string(definition.Tenant), rootID, string(definition.Tenant), rootID, catalogHead))
	if err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publications(
    source_authority, publication_id, source_operation_id, change_id, cause,
    origin_domain, origin_generation, affected_key_count, affected_keys_digest,
    publication_kind, identity_digest, target_count, targets_digest,
    stage_sequence, stage_item_count, stage_byte_count, stage_digest,
    predecessor_publication_id, predecessor_revision, source_revision,
    expected_visibility_epoch, target_epoch, phase, cursor_tenant, cursor_key,
    initialized_target_count, prepared_target_count, item_count, byte_count,
    rolling_digest, prepared
) VALUES (?, ?, ?, ?, ?, '', 0, 1, ?, 1, ?, 1, ?, 1, 1, 1, ?, x'', 0, ?, 0, ?, ?, '', '', 1, 1, 1, 1, ?, 1)`,
		definition.ContentSourceID, publication[:], sourceOperation[:], changeID[:],
		string(causal.CauseBootstrap), affectedDigest[:], identityDigest[:], targetsDigest[:],
		publicationDigest[:], sourceRevision, sourceRevision, sourceDriverPublicationPrepared,
		publicationDigest[:]); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_targets(
    source_authority, publication_id, tenant, generation, root_key, catalog_operation_id,
    predecessor_head, catalog_head, catalog_fingerprint, file_provider_fingerprint,
    changed, provider_changed, object_count, phase, cursor_key, cursor_object_id,
    cursor_revision, catalog_state, provider_state, next_change_sequence, prepared
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1, 1, ?, '', x'', 0, x'', x'', 0, 1)`,
		definition.ContentSourceID, publication[:], string(definition.Tenant), uint64(definition.Generation),
		"root:"+string(definition.Tenant), catalogOperation[:], catalogHead, catalogHead,
		headDigest[:], providerDigest[:], sourceDriverTargetPrepared); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	preparedRoot := sourceDriverPreparedObject{
		key:     sourceRootKey(definition),
		nameKey: normalizeName(definition.CasePolicy, root.Name),
		object:  root,
	}
	identity := SourceDriverStageIdentity{
		Authority: causal.SourceAuthorityID(definition.ContentSourceID),
		Operation: publication,
	}
	if err := insertSourceDriverPreparedObject(ctx, tx, identity, preparedRoot); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	if err := insertSourceDriverPreparedVersion(ctx, tx, identity, preparedRoot); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_changes(
    source_authority, publication_id, tenant, revision, scope_kind, presentation,
    scope_parent, scope_domain, scope_generation, sequence, kind, object_id, object_revision
) VALUES (?, ?, ?, ?, ?, ?, ?, '', 0, 0, ?, ?, ?)`,
		definition.ContentSourceID, publication[:], string(definition.Tenant), uint64(root.Revision),
		uint8(EnumerationContainer), uint8(PresentationMount), root.Parent[:], uint8(ChangeUpsert),
		root.ID[:], uint64(root.Revision)); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_affected(source_authority, publication_id, affected_key)
VALUES (?, ?, 'test-key')`, definition.ContentSourceID, publication[:]); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	if err := tx.Commit(); err != nil {
		return causal.OperationID{}, [sha256.Size]byte{}, err
	}
	return publication, publicationDigest, nil
}

func tenantMutationForTest(
	t *testing.T,
	owner string,
	expected TenantIntentRevision,
) TenantMutation {
	t.Helper()
	return TenantMutation{
		OperationID: mustTenantOperationID(t), HolderRuntimeGeneration: "catalog-test-holder",
		OwnerID: owner, ExpectedIntentRevision: expected,
	}
}
