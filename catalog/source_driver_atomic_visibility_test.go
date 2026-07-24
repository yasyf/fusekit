package catalog

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestSourceDriverPublicationPointerCASIsOldOrNewAcrossReopen(t *testing.T) {
	tests := []struct {
		name         string
		point        string
		visibleNew   bool
		lostResponse bool
	}{
		{name: "before pointer CAS", point: sourceDriverBeforeVisibilityCASPoint},
		{name: "after pointer CAS before commit", point: sourceDriverAfterVisibilityCASPoint},
		{
			name: "after pointer commit before response", point: sourceDriverAfterVisibilityCommitPoint,
			visibleNew: true, lostResponse: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.sqlite")
			store, provision, declaration, targets := openAtomicVisibilityCatalog(t, path)
			t.Cleanup(func() {
				if store != nil {
					_ = store.Close()
				}
			})

			baseline := sourceDriverIdentityAtHeadForTest(
				t, store, declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotReset,
				"", "baseline-token", 101,
			)
			baselineState := appendAtomicVisibilityObject(t, store, baseline, provision, "old")
			prepareAtomicVisibilityPublication(t, store, baseline)
			baselineResult, err := store.CommitSourceDriverStage(t.Context(), baselineState)
			if err != nil {
				t.Fatalf("commit baseline: %v", err)
			}
			if err := store.AcknowledgeSourceDriverCommittedReceipt(t.Context(), baselineResult); err != nil {
				t.Fatal(err)
			}
			if err := store.ForgetSourceDriverCommittedReceipt(t.Context(), baselineResult); err != nil {
				t.Fatal(err)
			}
			collectAtomicVisibilityStage(t, store, baseline)
			assertAtomicPublication(t, store, provision, baseline, "old")

			next := sourceDriverIdentityAtHeadForTest(
				t, store, declaration, targets, SourceDriverDelta, 0,
				"baseline-token", "next-token", 102,
			)
			nextState := appendAtomicVisibilityObject(t, store, next, provision, "new")
			prepareAtomicVisibilityPublication(t, store, next)
			injected := errors.New("publication visibility failpoint")
			store.failpoint = func(point string) error {
				if point == test.point {
					return injected
				}
				return nil
			}
			if _, err := store.CommitSourceDriverStage(t.Context(), nextState); !errors.Is(err, injected) {
				t.Fatalf("commit at %s = %v, want injected error", test.point, err)
			}
			store.failpoint = nil
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = nil

			reopened, err := Open(t.Context(), path)
			if err != nil {
				t.Fatalf("reopen after %s: %v", test.point, err)
			}
			store = reopened
			visible := baseline
			if test.visibleNew {
				visible = next
			}
			assertAtomicPublication(t, store, provision, visible, map[bool]string{false: "old", true: "new"}[test.visibleNew])

			var durable *SourceDriverCommittedReceipt
			if test.lostResponse {
				durable, err = store.PendingSourceDriverCommittedReceipt(t.Context(), next.Authority)
				if err != nil || durable == nil || durable.Result.Identity.Operation != next.Operation {
					t.Fatalf("lost-response receipt = %+v, %v", durable, err)
				}
			} else {
				durable, err = store.PendingSourceDriverCommittedReceipt(t.Context(), next.Authority)
				if err != nil || durable != nil {
					t.Fatalf("rolled-back publication receipt = %+v, %v, want absent", durable, err)
				}
			}

			replayed, err := store.CommitSourceDriverStage(t.Context(), nextState)
			if err != nil {
				t.Fatalf("commit replay after %s: %v", test.point, err)
			}
			if durable != nil && replayed.ReceiptDigest != durable.Result.ReceiptDigest {
				t.Fatalf("lost-response replay digest = %x, want %x",
					replayed.ReceiptDigest, durable.Result.ReceiptDigest)
			}
			assertAtomicPublication(t, store, provision, next, "new")
		})
	}
}

func collectAtomicVisibilityStage(
	t *testing.T,
	store *Catalog,
	identity SourceDriverStageIdentity,
) {
	t.Helper()
	for step := 0; step < 128; step++ {
		result, err := store.drainSourceDriverStageRows(t.Context(), identity.Authority, identity.Operation)
		if err != nil {
			t.Fatalf("collect committed stage step %d: %v", step, err)
		}
		if result.Complete {
			return
		}
	}
	t.Fatal("committed source driver stage collection did not converge")
}

func appendAtomicVisibilityObject(
	t *testing.T,
	store *Catalog,
	identity SourceDriverStageIdentity,
	provision TenantProvision,
	name string,
) SourceDriverStageState {
	t.Helper()
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatalf("begin %s publication: %v", name, err)
	}
	var changeSequence uint64
	if identity.Mode != SourceDriverSnapshot {
		changeSequence = 1
	}
	state, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: [sha256.Size]byte{name[0]}, Complete: true,
			Entries: []SourceDriverStageEntry{{
				Tenant: provision.Tenant, Generation: provision.Generation,
				ChangeSequence: changeSequence, Key: "item",
				Object: &SourceObject{
					Key: "item", Name: name, Kind: KindDirectory,
					Visibility: Visibility{Mount: true, FileProvider: true},
				},
			}},
		},
	))
	if err != nil {
		t.Fatalf("append %s publication: %v", name, err)
	}
	return state
}

func prepareAtomicVisibilityPublication(
	t *testing.T,
	store *Catalog,
	identity SourceDriverStageIdentity,
) SourceDriverPreparationState {
	t.Helper()
	for step := 0; step < 128; step++ {
		state, err := store.PrepareSourceDriverPublicationBatch(t.Context(), identity)
		if err != nil {
			t.Fatalf("prepare publication step %d: %v", step, err)
		}
		if state.Prepared {
			return state
		}
	}
	t.Fatal("source driver publication preparation did not converge")
	return SourceDriverPreparationState{}
}

func assertAtomicPublication(
	t *testing.T,
	store *Catalog,
	provision TenantProvision,
	identity SourceDriverStageIdentity,
	wantName string,
) {
	t.Helper()
	var publication []byte
	var sourceRevision, catalogHead uint64
	var name string
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT head.publication_id, head.source_revision, target.catalog_head, object.name
FROM source_driver_publication_heads head
JOIN source_driver_publication_targets target
  ON target.source_authority = head.source_authority
 AND target.publication_id = head.publication_id
JOIN source_driver_publication_objects object
  ON object.source_authority = target.source_authority
 AND object.publication_id = target.publication_id
 AND object.tenant = target.tenant
WHERE head.source_authority = ? AND target.tenant = ? AND object.source_key = 'item'`,
		string(identity.Authority), string(provision.Tenant)).Scan(
		&publication, &sourceRevision, &catalogHead, &name,
	); err != nil {
		t.Fatal(err)
	}
	if !equalBytes(publication, identity.Operation[:]) ||
		sourceRevision != uint64(identity.Predecessor+1) || name != wantName || catalogHead == 0 {
		t.Fatalf("publication=%x revision=%d head=%d name=%q; want %x/%d/%q",
			publication, sourceRevision, catalogHead, name, identity.Operation,
			identity.Predecessor+1, wantName)
	}
}

func openAtomicVisibilityCatalog(
	t *testing.T,
	path string,
) (*Catalog, TenantProvision, [sha256.Size]byte, []SourceDriverTarget) {
	t.Helper()
	store, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	provision := testTenantProvision(t, "atomic-visibility", 1)
	provision.ContentSourceID = "driver-authority"
	provision, err = provisionTenantForTest(t, store, t.Context(), provision)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	fleet := reconcileSourceAuthorityFleetForTest(t, store, "driver-owner", 0, 1, "driver-authority")
	acknowledgeSourceAuthorityFleetForTest(t, store, fleet)
	declaration := sourceAuthorityDeclarationsForTest("driver-authority")[0].DeclarationDigest
	targets := sourceDriverTargetsForProvisions(t, provision)
	seedSourceDriverLifecycleCheckpointForTest(t, store, declaration, []TenantProvision{provision}, targets)
	return store, provision, declaration, targets
}

func TestSourceDriverFinalCommitStatementCountIsTargetCountIndependent(t *testing.T) {
	one := sourceDriverFinalCommitStatementCount(t, 1)
	tenThousand := sourceDriverFinalCommitStatementCount(t, SourceDriverTargetLimit)
	if one.statements == 0 || tenThousand.statements != one.statements {
		t.Fatalf("final statement count one=%d ten-thousand=%d, want equal nonzero",
			one.statements, tenThousand.statements)
	}
	if one.targetObjectWrites != 0 || tenThousand.targetObjectWrites != 0 {
		t.Fatalf("final per-target/object writes one=%d ten-thousand=%d, want zero",
			one.targetObjectWrites, tenThousand.targetObjectWrites)
	}
}

type sourceDriverFinalCommitMetrics struct {
	statements         int
	targetObjectWrites int
}

func sourceDriverFinalCommitStatementCount(t *testing.T, targetCount int) sourceDriverFinalCommitMetrics {
	t.Helper()
	store, identity, state := preparedSourceDriverFinalizationFixture(t, targetCount)
	defer func() { _ = store.Close() }()
	before := sourceDriverFinalTargetObjectRows(t, store, identity)
	installSourceDriverFinalWriteGuards(t, store)
	statements := 0
	store.failpoint = func(point string) error {
		if point == sourceDriverFinalCommitStatementPoint {
			statements++
		}
		return nil
	}
	if _, err := store.CommitSourceDriverStage(t.Context(), state); err != nil {
		t.Fatalf("finalize %d targets: %v", targetCount, err)
	}
	store.failpoint = nil
	if identity.TargetCount != uint64(targetCount) {
		t.Fatalf("fixture target count = %d, want %d", identity.TargetCount, targetCount)
	}
	after := sourceDriverFinalTargetObjectRows(t, store, identity)
	return sourceDriverFinalCommitMetrics{
		statements: statements, targetObjectWrites: after - before,
	}
}

func installSourceDriverFinalWriteGuards(t *testing.T, store *Catalog) {
	t.Helper()
	tables := []string{
		"source_driver_publication_targets",
		"source_driver_publication_objects",
		"source_driver_publication_versions",
		"source_driver_publication_changes",
		"source_driver_stage_targets",
	}
	for tableIndex, table := range tables {
		for operationIndex, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			name := fmt.Sprintf("test_source_driver_final_no_write_%d_%d", tableIndex, operationIndex)
			statement := fmt.Sprintf(`CREATE TRIGGER %s BEFORE %s ON %s
BEGIN SELECT RAISE(ABORT, 'source driver final transaction wrote target/object rows'); END`,
				name, operation, table)
			if _, err := store.db.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func sourceDriverFinalTargetObjectRows(
	t *testing.T,
	store *Catalog,
	identity SourceDriverStageIdentity,
) int {
	t.Helper()
	queries := []struct {
		statement string
		arguments []any
	}{
		{`SELECT COUNT(*) FROM source_driver_publication_targets
WHERE source_authority = ? AND publication_id = ?`, []any{string(identity.Authority), identity.Operation[:]}},
		{`SELECT COUNT(*) FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ?`, []any{string(identity.Authority), identity.Operation[:]}},
		{`SELECT COUNT(*) FROM source_driver_publication_versions
WHERE source_authority = ? AND publication_id = ?`, []any{string(identity.Authority), identity.Operation[:]}},
		{`SELECT COUNT(*) FROM source_driver_publication_changes
WHERE source_authority = ? AND publication_id = ?`, []any{string(identity.Authority), identity.Operation[:]}},
		{`SELECT COUNT(*) FROM source_driver_checkpoint_targets
WHERE source_authority = ?`, []any{string(identity.Authority)}},
		{`SELECT COUNT(*) FROM source_tenant_targets
WHERE source_authority = ?`, []any{string(identity.Authority)}},
	}
	total := 0
	for _, query := range queries {
		var count int
		if err := store.readDB.QueryRowContext(t.Context(), query.statement, query.arguments...).Scan(&count); err != nil {
			t.Fatal(err)
		}
		total += count
	}
	return total
}

func preparedSourceDriverFinalizationFixture(
	t *testing.T,
	targetCount int,
) (*Catalog, SourceDriverStageIdentity, SourceDriverStageState) {
	t.Helper()
	if targetCount < 1 || targetCount > SourceDriverTargetLimit {
		t.Fatalf("invalid target count %d", targetCount)
	}
	store, err := Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	targets := seedSourceDriverFinalizationTenants(t, store, targetCount)
	fleet := reconcileSourceAuthorityFleetForTest(t, store, "driver-owner", 0, 1, "driver-authority")
	acknowledgeSourceAuthorityFleetForTest(t, store, fleet)
	declaration := sourceAuthorityDeclarationsForTest("driver-authority")[0].DeclarationDigest
	identity := sourceDriverIdentityForTest(
		declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotInitial,
		"", "scale-token", 0, 111,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		_ = store.Close()
		t.Fatalf("begin source driver finalization stage: %v", err)
	}
	if err := seedPreparedSourceDriverFinalization(t, store, identity, targets); err != nil {
		_ = store.Close()
		t.Fatalf("seed prepared source driver finalization: %v", err)
	}
	state, found, err := readSourceDriverStageState(t.Context(), store.readDB, identity.Authority)
	if err != nil || !found || state.Identity != identity {
		_ = store.Close()
		t.Fatalf("read seeded terminal stage = %+v, %t, %v", state, found, err)
	}
	return store, identity, state
}

func seedSourceDriverFinalizationTenants(
	t *testing.T,
	store *Catalog,
	targetCount int,
) []SourceDriverTarget {
	t.Helper()
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	targets := make([]SourceDriverTarget, targetCount)
	for start := 0; start < targetCount; start += 128 {
		end := min(start+128, targetCount)
		var tenants, generations, intents, activations strings.Builder
		tenants.WriteString(`INSERT INTO tenants(
tenant, root_id, case_policy, presentation_set, head, floor) VALUES `)
		generations.WriteString(`INSERT INTO tenant_generations(
	tenant_id, generation, owner_id, spec, spec_hash, required_backends,
	mount_presentation_root, backing_root, content_source_id,
	file_provider_presentation_instance_id, file_provider_display_name,
	access_mode, case_policy, presentation_set) VALUES `)
		intents.WriteString(`INSERT INTO tenant_intents(
	tenant_id, state, target_generation, intent_revision, current_operation_id, version) VALUES `)
		activations.WriteString(`INSERT INTO tenant_activations(
	tenant_id, active_generation, active_view_id, active_catalog_head, source_revision,
	activation_revision, retiring, version, last_operation_id) VALUES `)
		tenantArguments := make([]any, 0, (end-start)*6)
		generationArguments := make([]any, 0, (end-start)*14)
		intentArguments := make([]any, 0, (end-start)*2)
		activationArguments := make([]any, 0, (end-start)*2)
		for index := start; index < end; index++ {
			if index != start {
				tenants.WriteByte(',')
				generations.WriteByte(',')
				intents.WriteByte(',')
				activations.WriteByte(',')
			}
			tenant := TenantID(fmt.Sprintf("scale-%05d", index))
			root := make([]byte, len(ObjectID{}))
			binary.BigEndian.PutUint64(root[8:], uint64(index+1))
			tenants.WriteString("(?, ?, ?, ?, 1, 0)")
			tenantArguments = append(tenantArguments, string(tenant), root,
				uint8(CaseSensitive), uint8(PresentMount))
			definition := TenantProvision{
				OwnerID: "scale-owner", Tenant: tenant,
				Mount:       MountPresentation{PresentationRoot: "/scale/" + string(tenant)},
				BackingRoot: "/backing/" + string(tenant), ContentSourceID: "driver-authority",
				Access: TenantReadOnly, CasePolicy: CaseSensitive, Presentations: PresentMount, Generation: 1,
			}
			canonical, err := canonicalizeTenantProvision(definition)
			if err != nil {
				t.Fatal(err)
			}
			operation := make([]byte, len(TenantOperationID{}))
			binary.BigEndian.PutUint64(operation[8:], uint64(index+1))
			generations.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?)")
			generationArguments = append(generationArguments, string(tenant), uint64(definition.Generation),
				"scale-owner", canonical.CanonicalSpec, canonical.SpecHash[:], uint8(canonical.RequiredBackends),
				definition.Mount.PresentationRoot, definition.BackingRoot, definition.ContentSourceID,
				uint8(definition.Access), uint8(definition.CasePolicy), uint8(definition.Presentations))
			intents.WriteString("(?, 1, 1, 1, ?, 1)")
			intentArguments = append(intentArguments, string(tenant), operation)
			activations.WriteString("(?, NULL, NULL, 0, 0, 0, 0, 1, ?)")
			activationArguments = append(activationArguments, string(tenant), operation)
			targets[index] = SourceDriverTarget{Tenant: tenant, Generation: 1}
		}
		if _, err := tx.ExecContext(t.Context(), tenants.String(), tenantArguments...); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), generations.String(), generationArguments...); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), intents.String(), intentArguments...); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), activations.String(), activationArguments...); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_driver_target_epochs(source_authority, target_epoch)
VALUES ('driver-authority', 1)`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return targets
}

func seedPreparedSourceDriverFinalization(
	t *testing.T,
	store *Catalog,
	identity SourceDriverStageIdentity,
	targets []SourceDriverTarget,
) error {
	t.Helper()
	digest, err := validateSourceDriverStageIdentity(identity)
	if err != nil {
		return err
	}
	stageDigest := sha256.Sum256([]byte("prepared source driver scale publication"))
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
UPDATE source_publication_stages SET
    last_revision = 1, next_sequence = 1, item_count = 1, byte_count = 1,
    complete = 1, rolling_digest = ?
WHERE source_authority = ? AND stage_operation_id = ?`, stageDigest[:],
		string(identity.Authority), identity.Operation[:]); err != nil {
		return err
	}
	if _, err := tx.ExecContext(t.Context(), `
UPDATE source_driver_stages SET driver_page_digest = ?
WHERE source_authority = ? AND stage_operation_id = ?`, stageDigest[:],
		string(identity.Authority), identity.Operation[:]); err != nil {
		return err
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_publication_stage_revisions(
    source_authority, stage_operation_id, source_revision, predecessor_revision,
    mode, operation_id, change_id, cause, origin_domain, origin_generation,
    last_affected_key, complete
) VALUES (?, ?, 1, 0, ?, ?, ?, ?, '', 0, 'driver', 1)`,
		string(identity.Authority), identity.Operation[:], uint8(SourceSnapshot),
		identity.SourceOperation[:], identity.ChangeID[:], string(identity.Cause)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_publication_stage_affected(
    source_authority, stage_operation_id, source_revision, affected_key
) VALUES (?, ?, 1, 'driver')`, string(identity.Authority), identity.Operation[:]); err != nil {
		return err
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_driver_publication_heads(
    source_authority, publication_id, source_revision, epoch
) VALUES (?, zeroblob(0), 0, 0)`, string(identity.Authority)); err != nil {
		return err
	}
	affectedDigest := sha256.Sum256([]byte("driver"))
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_driver_publications(
    source_authority, publication_id, source_operation_id, change_id, cause,
    origin_domain, origin_generation, affected_key_count, affected_keys_digest,
    identity_digest, target_count, targets_digest,
    stage_sequence, stage_item_count, stage_byte_count, stage_digest,
    predecessor_publication_id, predecessor_revision, source_revision,
    expected_visibility_epoch, target_epoch, phase, cursor_tenant, cursor_key,
    initialized_target_count, prepared_target_count, item_count, byte_count,
    rolling_digest, prepared
) VALUES (?, ?, ?, ?, ?, '', 0, 1, ?, ?, ?, ?, 1, 1, 1, ?, zeroblob(0), 0, 1, 0,
          (SELECT target_epoch FROM source_driver_target_epochs WHERE source_authority = ?), ?, '', '',
          ?, ?, ?, ?, ?, 1)`, string(identity.Authority), identity.Operation[:],
		identity.SourceOperation[:], identity.ChangeID[:], string(identity.Cause),
		affectedDigest[:], digest[:], identity.TargetCount, identity.TargetsDigest[:], stageDigest[:],
		string(identity.Authority), sourceDriverPublicationPrepared,
		len(targets), len(targets), len(targets), len(targets),
		stageDigest[:]); err != nil {
		return err
	}
	for start := 0; start < len(targets); start += 128 {
		end := min(start+128, len(targets))
		var statement strings.Builder
		statement.WriteString(`INSERT INTO source_driver_publication_targets(
    source_authority, publication_id, tenant, generation, root_key, catalog_operation_id,
    predecessor_head, catalog_head, catalog_fingerprint, file_provider_fingerprint,
    changed, provider_changed, object_count, phase, cursor_key, cursor_object_id,
    cursor_revision, catalog_state, provider_state, next_change_sequence, prepared
) VALUES `)
		arguments := make([]any, 0, (end-start)*7)
		for index, target := range targets[start:end] {
			if index != 0 {
				statement.WriteByte(',')
			}
			root, err := DeriveSourceDriverRootKey(identity.Authority, target.Tenant)
			if err != nil {
				return err
			}
			catalogOperation := sourceCatalogOperation(identity.SourceOperation, target.Tenant)
			statement.WriteString("(?, ?, ?, ?, ?, ?, 1, 1, zeroblob(32), zeroblob(32), " +
				"0, 0, 0, ?, '', zeroblob(0), 0, zeroblob(32), zeroblob(32), 0, 1)")
			arguments = append(arguments, string(identity.Authority), identity.Operation[:],
				string(target.Tenant), uint64(target.Generation), string(root), catalogOperation[:],
				sourceDriverTargetPrepared)
		}
		if _, err := tx.ExecContext(t.Context(), statement.String(), arguments...); err != nil {
			return err
		}
	}
	return tx.Commit()
}
