package catalog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/convergence"
)

func TestPersistedEnumValuesMatchHardSchema(t *testing.T) {
	values := []struct {
		name string
		got  uint8
		want uint8
	}{
		{"KindDirectory", uint8(KindDirectory), 1},
		{"KindFile", uint8(KindFile), 2},
		{"KindSymlink", uint8(KindSymlink), 3},
		{"ChangeDelete", uint8(ChangeDelete), 1},
		{"ChangeUpsert", uint8(ChangeUpsert), 2},
		{"PresentationMount", uint8(PresentationMount), 1},
		{"PresentationFileProvider", uint8(PresentationFileProvider), 2},
		{"PresentMount", uint8(PresentMount), 1},
		{"PresentFileProvider", uint8(PresentFileProvider), 2},
		{"EnumerationWorkingSet", uint8(EnumerationWorkingSet), 1},
		{"EnumerationContainer", uint8(EnumerationContainer), 2},
		{"MutationCreateTenant", uint8(MutationCreateTenant), 1},
		{"MutationCreate", uint8(MutationCreate), 2},
		{"MutationRevise", uint8(MutationRevise), 3},
		{"MutationDelete", uint8(MutationDelete), 4},
		{"MutationReplace", uint8(MutationReplace), 5},
		{"MutationAddInterest", uint8(MutationAddInterest), 6},
		{"MutationRemoveInterest", uint8(MutationRemoveInterest), 7},
		{"CaseSensitive", uint8(CaseSensitive), 1},
		{"CaseInsensitive", uint8(CaseInsensitive), 2},
		{"TenantReadOnly", uint8(TenantReadOnly), 1},
		{"TenantReadWrite", uint8(TenantReadWrite), 2},
		{"MutationPrepared", uint8(MutationPrepared), 1},
		{"MutationApplying", uint8(MutationApplying), 2},
		{"MutationApplied", uint8(MutationApplied), 3},
		{"MutationCommitted", uint8(MutationCommitted), 4},
		{"MutationRecoveryRequired", uint8(MutationRecoveryRequired), 5},
		{"QuarantineLaneCatalogMutation", uint8(QuarantineLaneCatalogMutation), 1},
		{"QuarantineLaneMaterialization", uint8(QuarantineLaneMaterialization), 2},
		{"QuarantineLaneEnumeration", uint8(QuarantineLaneEnumeration), 3},
		{"QuarantineLaneMountLifecycle", uint8(QuarantineLaneMountLifecycle), 4},
		{"QuarantineCauseConflict", uint8(QuarantineCauseConflict), 1},
		{"QuarantineCauseIntegrity", uint8(QuarantineCauseIntegrity), 2},
		{"QuarantineCauseUnsettled", uint8(QuarantineCauseUnsettled), 3},
		{"QuarantineCauseUnavailable", uint8(QuarantineCauseUnavailable), 4},
		{"SourceSnapshot", uint8(SourceSnapshot), 1},
		{"SourceDelta", uint8(SourceDelta), 2},
		{"SourceObserverSnapshotRequired", uint8(SourceObserverSnapshotRequired), 1},
		{"SourceObserverIncremental", uint8(SourceObserverIncremental), 2},
		{"SourceObserverQuarantined", uint8(SourceObserverQuarantined), 3},
		{"SourceObserverStreamResetRequired", uint8(SourceObserverStreamResetRequired), 4},
		{"SourceMutationExpectationPlanned", SourceMutationExpectationPlanned, 1},
		{"SourceMutationExpectationComplete", SourceMutationExpectationComplete, 2},
		{"SourceMutationExpectationArmed", SourceMutationExpectationArmed, 3},
		{"SourceMutationExpectationRepairRequired", SourceMutationExpectationRepairRequired, 4},
		{"SourceMutationExpectationRepairPublished", SourceMutationExpectationRepairPublished, 5},
		{"BrokerCommandPlanned", uint8(BrokerCommandPlanned), 1},
		{"BrokerCommandSent", uint8(BrokerCommandSent), 2},
		{"BrokerCommandAccepted", uint8(BrokerCommandAccepted), 3},
		{"BrokerCommandDeliveryUnknown", uint8(BrokerCommandDeliveryUnknown), 4},
		{"tenantMaintenanceFloor", uint8(tenantMaintenanceFloor), 1},
		{"tenantMaintenanceRetainedIdentities", uint8(tenantMaintenanceRetainedIdentities), 4},
		{"globalMaintenanceSourceHistory", uint8(globalMaintenanceSourceHistory), 1},
		{"globalMaintenanceBlobs", uint8(globalMaintenanceBlobs), 7},
		{"storageEntryTemporary", uint8(storageEntryTemporary), 1},
		{"storageEntryPublished", uint8(storageEntryPublished), 2},
		{"storageEntryPending", uint8(storageEntryPending), 1},
		{"storageEntryStable", uint8(storageEntryStable), 2},
		{"storageTransitionCreateTemporary", uint8(storageTransitionCreateTemporary), 1},
		{"storageTransitionPublish", uint8(storageTransitionPublish), 2},
		{"storageTransitionDeleteTemporary", uint8(storageTransitionDeleteTemporary), 3},
		{"storageTransitionDeletePublished", uint8(storageTransitionDeletePublished), 4},
		{"activationOutboxPending", uint8(activationOutboxPending), 1},
		{"activationOutboxDelivering", uint8(activationOutboxDelivering), 2},
		{"activationOutboxAwaitingAck", uint8(activationOutboxAwaitingAck), 3},
		{"activationOutboxAcked", uint8(activationOutboxAcked), 4},
		{"activationOutboxSuperseded", uint8(activationOutboxSuperseded), 5},
		{"activationOutboxQuarantined", uint8(activationOutboxQuarantined), 6},
		{"activationOutboxOutcomeNone", 0, 0},
		{"DeliveryNotSent", uint8(convergence.DeliveryNotSent), 1},
		{"DeliverySent", uint8(convergence.DeliverySent), 2},
		{"DeliveryUnknown", uint8(convergence.DeliveryUnknown), 3},
	}
	for _, value := range values {
		if value.got != value.want {
			t.Errorf("%s = %d, want hard-schema value %d", value.name, value.got, value.want)
		}
	}

	c := newTestCatalog(t)
	fragments := map[string][]string{
		"tenants": {
			"case_policy INTEGER NOT NULL CHECK (case_policy IN (1, 2))",
			"presentation_set INTEGER NOT NULL CHECK (presentation_set BETWEEN 1 AND 3)",
		},
		"tenant_generations": {
			"access_mode INTEGER NOT NULL CHECK (access_mode IN (1, 2))",
		},
		"catalog_global_maintenance": {
			"next_phase INTEGER NOT NULL CHECK (next_phase BETWEEN 1 AND 7)",
		},
		"catalog_maintenance": {
			"next_phase INTEGER NOT NULL DEFAULT 1 CHECK (next_phase BETWEEN 1 AND 4)",
		},
		"broker_command_attempts": {
			"state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 4)",
		},
		"activation_outbox": {
			"state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 6)",
			"outcome INTEGER NOT NULL CHECK (outcome BETWEEN 0 AND 3)",
		},
		"source_operations": {
			"mode INTEGER NOT NULL CHECK (mode IN (1, 2))",
		},
		"source_publication_stage_revisions": {
			"mode INTEGER NOT NULL CHECK (mode IN (1, 2))",
		},
		"source_publication_stages": {
			"stage_kind INTEGER NOT NULL DEFAULT 1 CHECK (stage_kind IN (1, 2))",
		},
		"source_driver_checkpoints": {
			"snapshot_required INTEGER NOT NULL DEFAULT 0 CHECK (snapshot_required IN (0, 2, 3))",
		},
		"source_driver_stages": {
			"mode INTEGER NOT NULL CHECK (mode IN (1, 2, 3))",
			"snapshot_reason INTEGER NOT NULL CHECK (snapshot_reason IN (0, 1, 2, 3))",
		},
		"source_driver_stage_entries": {
			"action INTEGER NOT NULL CHECK (action IN (1, 2))",
		},
		"source_observer_streams": {
			"state INTEGER NOT NULL CHECK (state IN (1, 2, 3, 4))",
		},
		"source_observer_configuration_receipts": {
			"state INTEGER NOT NULL CHECK (state IN (1, 2, 3, 4))",
		},
		"source_mutation_expectations": {
			"state INTEGER NOT NULL CHECK (state IN (1, 2, 3, 4, 5))",
		},
		"tenant_state": {
			"quarantine_lane INTEGER CHECK (quarantine_lane BETWEEN 1 AND 4)",
			"quarantine_cause INTEGER CHECK (quarantine_cause BETWEEN 1 AND 4)",
		},
		"objects": {
			"kind INTEGER NOT NULL CHECK (kind IN (1, 2, 3))",
		},
		"object_versions": {
			"kind INTEGER NOT NULL CHECK (kind IN (1, 2, 3))",
		},
		"source_snapshot_objects": {
			"object_kind INTEGER NOT NULL CHECK (object_kind BETWEEN 1 AND 3)",
		},
		"changes": {
			"scope_kind INTEGER NOT NULL CHECK (scope_kind IN (1, 2))",
			"presentation INTEGER NOT NULL CHECK (presentation IN (1, 2))",
			"kind INTEGER NOT NULL CHECK (kind IN (1, 2))",
		},
		"materialization_interests": {
			"owner_presentation INTEGER NOT NULL CHECK (owner_presentation IN (1, 2))",
		},
		"prepared_mutations": {
			"kind INTEGER NOT NULL CHECK (kind BETWEEN 2 AND 5)",
			"state INTEGER NOT NULL CHECK (state BETWEEN 1 AND 5)",
		},
		"mutation_journal": {
			"kind INTEGER NOT NULL CHECK (kind BETWEEN 1 AND 7)",
		},
		"storage_entries": {
			"kind INTEGER NOT NULL CHECK (kind IN (1, 2))",
			"state INTEGER NOT NULL CHECK (state IN (1, 2))",
		},
		"storage_transitions": {
			"kind INTEGER NOT NULL CHECK (kind BETWEEN 1 AND 4)",
		},
	}
	for table, expected := range fragments {
		var ddl string
		if err := c.db.QueryRowContext(t.Context(), `
SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?`, table).Scan(&ddl); err != nil {
			t.Fatalf("read %s DDL: %v", table, err)
		}
		for _, fragment := range expected {
			if !strings.Contains(ddl, fragment) {
				t.Errorf("%s DDL does not contain %q", table, fragment)
			}
		}
	}
}

func TestStoredObjectRejectsUnknownAndSemanticallyInvalidKind(t *testing.T) {
	tests := []struct {
		name string
		kind Kind
	}{
		{name: "unknown", kind: 99},
		{name: "file missing content", kind: KindFile},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newTestCatalog(t)
			tenant, object := createPersistedEnumTenant(t, c, "corrupt-object-"+test.name)
			allowInvalidChecks(t, c)
			if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_driver_publication_objects SET kind = ? WHERE tenant = ? AND object_id = ?`,
				uint8(test.kind), string(tenant), object.ID[:]); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Inspect(t.Context(), tenant, object.ID); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("Inspect corrupt object error = %v, want ErrIntegrity", err)
			}
		})
	}
}

func TestChangesSinceRejectsUnknownKind(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createPersistedEnumTenant(t, c, "corrupt-change")
	allowInvalidChecks(t, c)
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_driver_publication_changes SET kind = ?
WHERE tenant = ? AND revision = ? AND object_id = ?`,
		uint8(99), string(tenant), uint64(root.Revision), root.ID[:]); err != nil {
		t.Fatal(err)
	}
	_, err := c.ChangesSince(
		t.Context(),
		tenant,
		EnumerationScope{
			Kind: EnumerationContainer, Presentation: PresentationMount, Parent: root.ID,
		},
		CompleteChangeCursor(root.Revision-1),
		10,
	)
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("ChangesSince corrupt change error = %v, want ErrIntegrity", err)
	}
}

func TestMutationRejectsUnknownAndSemanticallyInvalidKind(t *testing.T) {
	tests := []struct {
		name string
		kind MutationKind
	}{
		{name: "unknown", kind: 99},
		{name: "replace without secondary object", kind: MutationReplace},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newTestCatalog(t)
			tenant, _ := createPersistedEnumTenant(t, c, "corrupt-mutation-"+test.name)
			id := latestMutationID(t, c, tenant, MutationCreateTenant)
			allowInvalidChecks(t, c)
			if _, err := c.db.ExecContext(t.Context(), `
UPDATE mutation_journal SET kind = ? WHERE mutation_id = ?`,
				uint8(test.kind), id[:]); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Mutation(t.Context(), tenant, id); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("Mutation corrupt kind error = %v, want ErrIntegrity", err)
			}
		})
	}
}

func TestPreparedMutationRejectsKindIntentMismatchAndUnknownClaimedState(t *testing.T) {
	t.Run("kind does not match intent", func(t *testing.T) {
		c := newTestCatalog(t)
		tenant, root := createPersistedEnumTenant(t, c, "corrupt-prepared-kind")
		prepared, err := c.BeginMutation(
			t.Context(),
			tenant,
			mustCatalogHead(t, c, tenant),
			testCreateIntent(root.ID, "file", stageTestContent(t, c, "body")),
		)
		if err != nil {
			t.Fatal(err)
		}
		allowInvalidChecks(t, c)
		if _, err := c.db.ExecContext(t.Context(), `
UPDATE prepared_mutations SET kind = ? WHERE mutation_id = ?`,
			uint8(MutationRevise), prepared.OperationID[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := c.PreparedMutation(
			t.Context(), tenant, prepared.OperationID,
		); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("PreparedMutation mismatched kind error = %v, want ErrIntegrity", err)
		}
	})

	t.Run("unknown claimed state", func(t *testing.T) {
		c := newTestCatalog(t)
		tenant, root := createPersistedEnumTenant(t, c, "corrupt-prepared-state")
		prepared, err := c.BeginMutation(
			t.Context(),
			tenant,
			mustCatalogHead(t, c, tenant),
			testCreateIntent(root.ID, "file", stageTestContent(t, c, "body")),
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.ClaimMutation(t.Context(), prepared.OperationID, mustMutationOwner(t)); err != nil {
			t.Fatal(err)
		}
		allowInvalidChecks(t, c)
		if _, err := c.db.ExecContext(t.Context(), `
UPDATE prepared_mutations SET state = 99 WHERE mutation_id = ?`,
			prepared.OperationID[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := c.PreparedMutation(
			t.Context(), tenant, prepared.OperationID,
		); !errors.Is(err, ErrIntegrity) {
			t.Fatalf("PreparedMutation unknown state error = %v, want ErrIntegrity", err)
		}
	})
}

func TestLoadTenantStateRejectsUnknownQuarantineEnum(t *testing.T) {
	c := newTestCatalog(t)
	tenant, _ := createPersistedEnumTenant(t, c, "corrupt-state")
	record := TenantStateRecord{
		Tenant: tenant, Generation: 1, Desired: 1,
		Quarantine: testQuarantine(1),
	}
	if _, err := c.SaveTenantState(t.Context(), 0, record); err != nil {
		t.Fatal(err)
	}
	allowInvalidChecks(t, c)
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE tenant_state SET quarantine_lane = 99 WHERE tenant = ?`, string(tenant)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.LoadTenantState(t.Context(), tenant); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("LoadTenantState corrupt lane error = %v, want ErrIntegrity", err)
	}
}

func createPersistedEnumTenant(t *testing.T, c *Catalog, name string) (TenantID, Object) {
	t.Helper()
	provision, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, name, 1))
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	root, err := c.Root(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	return provision.Tenant, root
}

func allowInvalidChecks(t *testing.T, c *Catalog) {
	t.Helper()
	if _, err := c.db.ExecContext(t.Context(), "PRAGMA ignore_check_constraints = ON"); err != nil {
		t.Fatalf("disable CHECK constraints: %v", err)
	}
	t.Cleanup(func() {
		if _, err := c.db.ExecContext(context.Background(), "PRAGMA ignore_check_constraints = OFF"); err != nil {
			t.Errorf("restore CHECK constraints: %v", err)
		}
	})
}

func latestMutationID(
	t *testing.T,
	c *Catalog,
	tenant TenantID,
	kind MutationKind,
) MutationID {
	t.Helper()
	var raw []byte
	if err := c.db.QueryRowContext(t.Context(), `
SELECT mutation_id FROM mutation_journal
WHERE tenant = ? AND kind = ?
ORDER BY revision DESC LIMIT 1`, string(tenant), uint8(kind)).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	id, err := mutationID(raw)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestPersistedEnumSchemaRejectsUnknownValues(t *testing.T) {
	c := newTestCatalog(t)
	tenant, object := createPersistedEnumTenant(t, c, "enum-check")
	mutation := latestMutationID(t, c, tenant, MutationCreateTenant)
	tests := []struct {
		name      string
		statement string
		args      []any
	}{
		{
			name:      "object kind",
			statement: "UPDATE objects SET kind = 99 WHERE tenant = ? AND object_id = ?",
			args:      []any{string(tenant), object.ID[:]},
		},
		{
			name:      "change kind",
			statement: "UPDATE source_driver_publication_changes SET kind = 99 WHERE tenant = ? AND revision = ?",
			args:      []any{string(tenant), uint64(object.Revision)},
		},
		{
			name:      "mutation kind",
			statement: "UPDATE mutation_journal SET kind = 99 WHERE mutation_id = ?",
			args:      []any{mutation[:]},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := c.db.ExecContext(t.Context(), test.statement, test.args...); err == nil {
				t.Fatal("unknown persisted enum value was accepted")
			} else if !strings.Contains(strings.ToLower(fmt.Sprint(err)), "constraint") {
				t.Fatalf("unknown persisted enum error = %v, want constraint", err)
			}
		})
	}
}
