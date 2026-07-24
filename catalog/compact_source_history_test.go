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
		if result.SourceOperations > sourceHistoryCompactionPage {
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

func numberedOperation(value uint64) (result causal.OperationID) {
	binary.BigEndian.PutUint64(result[len(result)-8:], value)
	return result
}

func numberedChange(value uint64) (result causal.ChangeID) {
	binary.BigEndian.PutUint64(result[len(result)-8:], value)
	return result
}
