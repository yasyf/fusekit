package catalog

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMaintainTenantAdvancesOnlyThroughDurableRetentionAnchors(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "maintenance-anchors", CaseSensitive)
	first := createTestFile(t, c, tenant, root.ID, "first", "first")
	owner, err := NewRetentionOwner("maintenance-test")
	if err != nil {
		t.Fatalf("NewRetentionOwner: %v", err)
	}
	handle, err := c.OpenAt(ctx, owner, tenant, PresentationFileProvider, 1, first.ID, first.Revision)
	if err != nil {
		t.Fatalf("OpenAt: %v", err)
	}
	createTestFile(t, c, tenant, root.ID, "second", "second")
	head := mustCatalogHead(t, c, tenant)
	convergeTestTenantState(t, c, tenant, head)

	result, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow())
	if err != nil {
		t.Fatalf("MaintainTenant(pinned): %v", err)
	}
	if result.Phase != MaintenanceAdvanceFloor || result.Floor != 2 || !result.More {
		t.Fatalf("pinned maintenance = %+v, want floor 2", result)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for call := 0; call < 20; call++ {
		result, err = c.MaintainTenant(ctx, tenant, testMaintenanceNow())
		if err != nil {
			t.Fatalf("MaintainTenant(unpinned %d): %v", call, err)
		}
		if result.Floor == head {
			break
		}
	}
	if result.Floor != head {
		t.Fatalf("maintenance floor = %d, want %d", result.Floor, head)
	}
}

func TestMaintainTenantRetiresChangesInFixedPages(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "maintenance-pages", CaseSensitive)
	ensureTestGeneration(t, c, tenant, 1)
	const total = MaintenancePageLimit + 44
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	for sequence := 0; sequence < total; sequence++ {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO changes(
    tenant, revision, scope_kind, presentation, scope_parent,
    scope_domain, scope_generation, sequence, kind, object_id, object_revision
) VALUES (?, 2, ?, ?, ?, '', 0, ?, ?, ?, 1)`,
			string(tenant), uint8(EnumerationContainer), uint8(PresentationMount),
			root.ID[:], sequence, uint8(ChangeUpsert), root.ID[:]); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert change %d: %v", sequence, err)
		}
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE tenants SET head = 3 WHERE tenant = ?", string(tenant)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("advance head: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit fixture: %v", err)
	}
	convergeTestTenantState(t, c, tenant, 3)
	if result, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow()); err != nil ||
		result.Phase != MaintenanceAdvanceFloor || result.Floor != 3 {
		t.Fatalf("floor maintenance = %+v, %v", result, err)
	}
	first, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow())
	if err != nil {
		t.Fatalf("MaintainTenant(first page): %v", err)
	}
	if first.Phase != MaintenanceChanges || first.Retired != MaintenancePageLimit || !first.More {
		t.Fatalf("first change page = %+v", first)
	}
	var second MaintenanceResult
	for call := 0; call < tenantMaintenancePhaseCount; call++ {
		second, err = c.MaintainTenant(ctx, tenant, testMaintenanceNow())
		if err != nil {
			t.Fatalf("MaintainTenant(second page %d): %v", call, err)
		}
		if second.Phase == MaintenanceChanges {
			break
		}
	}
	if second.Phase != MaintenanceChanges || second.Retired != 44 || !second.More {
		t.Fatalf("second change page = %+v", second)
	}
}

func TestMaintainTenantRotatesPastContinuouslyAdvancingFloor(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "maintenance-floor-fairness", CaseSensitive)
	createTestFile(t, c, tenant, root.ID, "first", "first")
	firstHead := mustCatalogHead(t, c, tenant)
	convergeTestTenantState(t, c, tenant, firstHead)
	first, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow())
	if err != nil {
		t.Fatalf("MaintainTenant(first floor): %v", err)
	}
	if first.Phase != MaintenanceAdvanceFloor {
		t.Fatalf("first maintenance = %+v, want floor advance", first)
	}

	createTestFile(t, c, tenant, root.ID, "second", "second")
	convergeTestTenantState(t, c, tenant, mustCatalogHead(t, c, tenant))
	second, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow())
	if err != nil {
		t.Fatalf("MaintainTenant(after new head): %v", err)
	}
	if second.Phase == MaintenanceAdvanceFloor || second.Phase == MaintenanceIdle {
		t.Fatalf("second maintenance = %+v, want later-phase retirement before another floor advance", second)
	}
}

func TestMaintenancePhaseCursorsRotateDurably(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	tenant, _ := createTestTenant(t, c, "maintenance-phase-cursors", CaseSensitive)
	gotTenant, err := c.claimTenantMaintenancePhase(ctx, tenant)
	if err != nil {
		t.Fatalf("claimTenantMaintenancePhase(before restart): %v", err)
	}
	if gotTenant != tenantMaintenanceFloor {
		t.Fatalf("tenant phase before restart = %d, want %d", gotTenant, tenantMaintenanceFloor)
	}
	gotGlobal, err := c.claimGlobalMaintenancePhase(ctx)
	if err != nil {
		t.Fatalf("claimGlobalMaintenancePhase(before restart): %v", err)
	}
	if gotGlobal != globalMaintenanceSourceHistory {
		t.Fatalf("global phase before restart = %d, want %d", gotGlobal, globalMaintenanceSourceHistory)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(restart): %v", err)
	}
	gotTenant, err = c.claimTenantMaintenancePhase(ctx, tenant)
	if err != nil {
		t.Fatalf("claimTenantMaintenancePhase(after restart): %v", err)
	}
	if gotTenant != tenantMaintenanceChanges {
		t.Fatalf("tenant phase after restart = %d, want %d", gotTenant, tenantMaintenanceChanges)
	}
	gotGlobal, err = c.claimGlobalMaintenancePhase(ctx)
	if err != nil {
		t.Fatalf("claimGlobalMaintenancePhase(after restart): %v", err)
	}
	if gotGlobal != globalMaintenanceBrokerAttempts {
		t.Fatalf("global phase after restart = %d, want %d", gotGlobal, globalMaintenanceBrokerAttempts)
	}
}

func TestMaintainTenantDoesNotExpireUnobservedLiveLeaseAnchor(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "maintenance-lease-anchor", CaseSensitive)
	createTestFile(t, c, tenant, root.ID, "first", "first")
	createTestFile(t, c, tenant, root.ID, "second", "second")
	head := mustCatalogHead(t, c, tenant)
	convergeTestTenantState(t, c, tenant, head)
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO file_provider_leases(
    lease_id, tenant, domain_id, generation, root_id, presentation_instance_id,
    state, session_id, process_identity, policy_digest, resolution_digest,
    catalog_head, source_authority, source_publication, source_revision,
    activation_generation, expires_unix_nano
) VALUES ('lease', ?, 'domain', 1, ?, 'presentation', 2, 'session', 'process',
    zeroblob(32), zeroblob(32), ?, 'source', zeroblob(16), 1, 'activation', ?)`,
		string(tenant), root.ID[:], uint64(head), testMaintenanceNow().Add(time.Hour).UnixNano()); err != nil {
		t.Fatalf("insert lease: %v", err)
	}
	result, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow())
	if err != nil {
		t.Fatalf("MaintainTenant(live unobserved lease): %v", err)
	}
	if result.Floor != 0 || result.Phase == MaintenanceAdvanceFloor {
		t.Fatalf("live unobserved lease maintenance = %+v, want floor 0", result)
	}
	if _, err := c.db.ExecContext(ctx,
		"DELETE FROM file_provider_leases WHERE lease_id = 'lease'"); err != nil {
		t.Fatalf("delete lease: %v", err)
	}
	result, err = c.MaintainTenant(ctx, tenant, testMaintenanceNow())
	if err != nil {
		t.Fatalf("MaintainTenant(released lease): %v", err)
	}
	if result.Phase != MaintenanceAdvanceFloor || result.Floor != head {
		t.Fatalf("released lease maintenance = %+v, want floor %d", result, head)
	}
}

func TestMaintenanceQueriesUseBoundedIndexes(t *testing.T) {
	c := newTestCatalog(t)
	tenant, _ := createTestTenant(t, c, "maintenance-plan", CaseSensitive)
	rows, err := c.readDB.Query(`
EXPLAIN QUERY PLAN
SELECT rowid
FROM changes
WHERE tenant = ? AND revision < ?
ORDER BY revision, sequence
LIMIT ?`, string(tenant), 100, MaintenancePageLimit)
	if err != nil {
		t.Fatalf("EXPLAIN: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var details []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan EXPLAIN: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read EXPLAIN: %v", err)
	}
	plan := strings.Join(details, "\n")
	if !strings.Contains(plan, "changes_compaction") {
		t.Fatalf("change maintenance query plan is not aligned:\n%s", plan)
	}
	plans := []struct {
		name      string
		statement string
		args      []any
		indexes   []string
	}{
		{
			name: "mutation journal",
			statement: `
SELECT mutation_id
FROM mutation_journal
WHERE tenant = ? AND revision <= ?
ORDER BY revision, mutation_id
LIMIT ?`,
			args:    []any{string(tenant), 100, MaintenancePageLimit + 1},
			indexes: []string{"mutation_journal_tenant_revision"},
		},
		{
			name: "safe floor anchors",
			statement: `
SELECT
    (SELECT MIN(opened_head) FROM handles WHERE tenant = ? AND closed = 0),
    (SELECT MIN(target_revision) FROM mutation_pins WHERE tenant = ? AND closed = 0)`,
			args: []any{
				string(tenant), string(tenant),
			},
			indexes: []string{
				"handles_compaction",
				"mutation_pins_live",
			},
		},
		{
			name: "File Provider acknowledgement watermark",
			statement: `
SELECT COUNT(*), COUNT(activation.presentation_id),
       MIN(COALESCE(activation.observed_catalog_head, 0))
FROM file_provider_leases lease
LEFT JOIN activation_outbox activation
  ON activation.presentation_id = lease.domain_id
 AND activation.tenant_id = lease.tenant
 AND activation.tenant_generation = lease.generation
 AND activation.state = ?
 AND NOT EXISTS (
     SELECT 1 FROM activation_outbox newer
     WHERE newer.presentation_id = activation.presentation_id
       AND newer.tenant_id = activation.tenant_id
       AND newer.tenant_generation = activation.tenant_generation
       AND newer.state = ?
       AND newer.expected_activation_revision > activation.expected_activation_revision
 )
WHERE lease.tenant = ? AND lease.expires_unix_nano > ?`,
			args: []any{
				uint8(activationOutboxAcked), uint8(activationOutboxAcked),
				string(tenant), testMaintenanceNow().UnixNano(),
			},
			indexes: []string{
				"file_provider_leases_tenant_expiry",
				"activation_outbox_ack_watermark",
			},
		},
		{
			name: "retired stage owner",
			statement: `
SELECT generation.owner_id
FROM catalog_generations generation
WHERE generation.retired = 1
  AND EXISTS (
      SELECT 1 FROM content_stages stage
      WHERE stage.owner_id = generation.owner_id
        AND stage.mutation_id IS NULL
        AND stage.source_operation_id IS NULL
  )
ORDER BY generation.owner_id
LIMIT 1`,
			indexes: []string{"catalog_generations_retired", "content_stages_orphan_owner"},
		},
		{
			name: "retired stage page",
			statement: `
SELECT stage_id, temp_name, published, hash
FROM content_stages
WHERE owner_id = ? AND mutation_id IS NULL AND source_operation_id IS NULL
ORDER BY stage_id
LIMIT ?`,
			args:    []any{c.owner[:], MaintenancePageLimit + 1},
			indexes: []string{"content_stages_owner"},
		},
	}
	for _, test := range plans {
		t.Run(test.name, func(t *testing.T) {
			plan := queryPlanForTest(t, c, test.statement, test.args...)
			for _, index := range test.indexes {
				if !strings.Contains(plan, index) {
					t.Fatalf("%s plan missing %s:\n%s", test.name, index, plan)
				}
			}
		})
	}
}

func convergeTestTenantState(
	t *testing.T,
	c *Catalog,
	tenant TenantID,
	revision Revision,
) {
	t.Helper()
	state, err := c.LoadTenantState(context.Background(), tenant)
	if err != nil {
		t.Fatalf("LoadTenantState: %v", err)
	}
	state.ActivatedGeneration = state.Generation
	state.Desired = revision
	state.Observed = revision
	state.Verified = revision
	state.Applied = revision
	if _, err := c.SaveTenantState(context.Background(), state.Version, state); err != nil {
		t.Fatalf("SaveTenantState(%s, %d): %v", tenant, revision, err)
	}
}
