package catalog

import (
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceHistoryCompactionIsPageBoundedAndConverges(t *testing.T) {
	c := newTestCatalog(t)
	_, _ = createTestTenant(t, c, "source-history-pages", CaseSensitive)
	const total = sourceHistoryCompactionPage*2 + 88
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for revision := 1; revision <= total; revision++ {
		operation := numberedOperation(uint64(revision))
		change := numberedChange(uint64(revision))
		result, err := json.Marshal(SourceResult{
			Authority: "history", Revision: causal.Revision(revision), ChangeID: change, Operation: operation,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_operations(
    operation_id, change_id, source_authority, source_revision,
    predecessor_revision, mode, request_hash, result_json
) VALUES (?, ?, 'history', ?, ?, ?, ?, ?)`,
			operation[:], change[:], revision, revision-1, uint8(SourceDelta), make([]byte, 32), result); err != nil {
			t.Fatal(err)
		}
	}
	currentOperation := numberedOperation(total)
	currentChange := numberedChange(total)
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_watermarks(source_authority, source_revision, change_id, operation_id)
VALUES ('history', ?, ?, ?)`, total, currentChange[:], currentOperation[:]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var removed int
	for call := 0; ; call++ {
		result, err := c.MaintainGlobal(t.Context(), time.Unix(1, 0).UTC())
		if err != nil {
			t.Fatal(err)
		}
		if result.Phase != MaintenanceSourceHistory {
			break
		}
		if result.SourceOperations > sourceHistoryCompactionPage ||
			result.ConvergenceChanges > sourceHistoryCompactionPage {
			t.Fatalf("compaction call %d exceeded page: %+v", call, result)
		}
		removed += result.SourceOperations
		if call > 4 {
			t.Fatal("bounded source compaction did not converge")
		}
	}
	if removed != total-1 {
		t.Fatalf("removed source operations = %d, want %d", removed, total-1)
	}
	var remaining int
	if err := c.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM source_operations").Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 1 {
		t.Fatalf("remaining source operations = %d, want current watermark only", remaining)
	}
}

func TestSourceHistoryCompactionRetiresChangeBeforeItsOperation(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "source-history-dependency", CaseSensitive)
	if _, err := c.Create(t.Context(), tenant, CreateSpec{
		Parent: root.ID, Name: "advance", Kind: KindDirectory,
		Visibility: Visibility{Mount: true, FileProvider: true},
	}); err != nil {
		t.Fatal(err)
	}
	result := SourceResult{
		Authority: "history-dependency", Revision: 1,
		ChangeID: numberedChange(900), Operation: numberedOperation(900),
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	catalogOperation := sourceCatalogOperation(result.Operation, tenant)
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_operations(
    operation_id, change_id, source_authority, source_revision,
    predecessor_revision, mode, request_hash, result_json
) VALUES (?, ?, ?, 1, 0, ?, ?, ?)`,
		result.Operation[:], result.ChangeID[:], string(result.Authority),
		uint8(SourceDelta), make([]byte, 32), encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_commits(
    catalog_operation_id, source_operation_id, tenant, generation, catalog_revision,
    catalog_fingerprint, file_provider_fingerprint
) VALUES (?, ?, ?, 1, 1, zeroblob(32), zeroblob(32))`,
		catalogOperation[:], result.Operation[:], string(tenant)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO convergence_changes(
    change_id, source_operation_id, source_authority, source_revision,
    cause, origin_domain, origin_generation, outbox_state
) VALUES (?, ?, ?, 1, ?, '', 0, ?)`,
		result.ChangeID[:], result.Operation[:], string(result.Authority), string(causal.CauseBootstrap),
		uint8(outboxSettled)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(),
		`INSERT INTO convergence_change_affected(change_id, affected_key) VALUES (?, 'key')`,
		result.ChangeID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(),
		`INSERT INTO convergence_change_targets(change_id, tenant) VALUES (?, ?)`,
		result.ChangeID[:], string(tenant)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO convergence_outbox(
    catalog_operation_id, change_id, tenant, catalog_revision, file_provider_fingerprint, state
) VALUES (?, ?, ?, 1, zeroblob(32), ?)`,
		catalogOperation[:], result.ChangeID[:], string(tenant), uint8(outboxSettled)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	compacted, err := maintainTestUntilIdle(t.Context(), c, tenant, 2)
	if err != nil {
		t.Fatal(err)
	}
	if compacted.SourceOperations != 1 || compacted.ConvergenceChanges != 1 {
		t.Fatalf("dependent source compaction = %+v", compacted)
	}
	var operations, changes int
	if err := c.db.QueryRowContext(t.Context(), `SELECT
    (SELECT COUNT(*) FROM source_operations WHERE operation_id = ?),
    (SELECT COUNT(*) FROM convergence_changes WHERE change_id = ?)`,
		result.Operation[:], result.ChangeID[:]).Scan(&operations, &changes); err != nil {
		t.Fatal(err)
	}
	if operations != 0 || changes != 0 {
		t.Fatalf("dependent source residue = %d operations / %d changes", operations, changes)
	}
}

func numberedOperation(value uint64) (result causal.OperationID) {
	binary.BigEndian.PutUint64(result[len(result)-8:], value)
	return result
}

func numberedChange(value uint64) (result causal.ChangeID) {
	binary.BigEndian.PutUint64(result[len(result)-8:], value)
	return result
}
