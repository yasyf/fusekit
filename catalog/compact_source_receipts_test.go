package catalog

import (
	"encoding/binary"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceReceiptCompactionSharesOneBudgetAndRetainsExactLatest(t *testing.T) {
	c := newTestCatalog(t)
	const (
		authority = "receipt-compaction"
		total     = 80
		limit     = 64
	)
	fleet := reconcileSourceAuthorityFleetForTest(
		t, c, "receipt-compaction-owner", 0, 1, causal.SourceAuthorityID(authority),
	)
	acknowledgeSourceAuthorityFleetForTest(t, c, fleet)
	declaration := sourceAuthorityDeclarationsForTest(causal.SourceAuthorityID(authority))[0]
	for index := 1; index <= total; index++ {
		operation := sourceReceiptOperationID(index)
		digest := sourceReceiptDigest(index)
		if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_publication_stage_receipts(
    source_authority, stage_operation_id, fleet_owner_id, authority_generation,
    driver_id, declaration_digest, through_sequence, first_revision, last_revision,
    revision_count, stage_sequence, stage_item_count, stage_byte_count, stage_digest, receipt_digest
) VALUES (?, ?, 'receipt-compaction-owner', 1, ?, ?, ?, ?, ?, 1, 1, 1, 1, ?, ?)`,
			authority, operation[:], declaration.DriverID, declaration.DeclarationDigest[:],
			index, index, index, digest[:], digest[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_observer_settlement_receipts(
    source_authority, stage_operation_id, fleet_owner_id, authority_generation,
    driver_id, declaration_digest, through_sequence, source_revision,
    stage_sequence, stage_item_count, stage_byte_count, stage_digest, receipt_digest,
    acknowledged
) VALUES (?, ?, 'receipt-compaction-owner', 1, ?, ?, ?, ?, 1, 1, 1, ?, ?, 1)`,
			authority, operation[:], declaration.DriverID, declaration.DeclarationDigest[:],
			index, index, digest[:], digest[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_observer_configuration_receipts(
    source_authority, fleet_owner_id, fleet_generation,
    operation_id, sequence, root_count, checkpoint_count, byte_count,
    stage_digest, stream_identity, root_epoch, root_set_digest, fleet_digest,
    last_received_sequence, last_applied_sequence, state, quarantine_detail, acknowledged
) VALUES (?, 'receipt-compaction-owner', 1, ?, 1, 1, 1, 1, ?,
          'stream', 'epoch', ?, ?, 0, 0, 1, '', 1)`,
			authority, operation[:], digest[:], digest[:], digest[:]); err != nil {
			t.Fatal(err)
		}
	}
	pinnedOperation := sourceReceiptOperationID(10)
	pinnedChange := sourceReceiptChangeID(10)
	pinnedDigest := sourceReceiptDigest(10)
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_operations(
    operation_id, change_id, source_authority, source_revision,
    predecessor_revision, mode, request_hash, result_json
) VALUES (?, ?, ?, 10, 9, 2, ?, '{}')`,
		pinnedOperation[:], pinnedChange[:], authority, pinnedDigest[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_watermarks(source_authority, source_revision, change_id, operation_id)
VALUES (?, 10, ?, ?)`, authority, pinnedChange[:], pinnedOperation[:]); err != nil {
		t.Fatal(err)
	}

	for call := 0; ; call++ {
		result := compactSourceHistoryForTest(t, c, limit)
		receipts := result.publicationReceipts + result.settlementReceipts + result.configurationReceipts
		if result.operations+result.changes+receipts > limit {
			t.Fatalf("compaction call %d exceeded shared budget: %+v", call, result)
		}
		if !result.more {
			break
		}
		if call > 20 {
			t.Fatal("receipt compaction did not converge")
		}
	}
	assertSourceReceiptCounts(t, c, authority, 2, 1, 1)

	latest := sourceReceiptOperationID(total)
	for table, operationColumn := range map[string]string{
		"source_publication_stage_receipts":      "stage_operation_id",
		"source_observer_settlement_receipts":    "stage_operation_id",
		"source_observer_configuration_receipts": "operation_id",
	} {
		var retained int
		if err := c.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM `+table+` WHERE source_authority = ? AND `+operationColumn+` = ?`,
			authority, latest[:]).Scan(&retained); err != nil {
			t.Fatal(err)
		}
		if retained != 1 {
			t.Fatalf("%s latest receipt retained = %d, want 1", table, retained)
		}
	}

	nextChange := sourceReceiptChangeID(total)
	nextOperation := sourceReceiptOperationID(total)
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_watermarks SET source_revision = ?, change_id = ?, operation_id = ?
WHERE source_authority = ?`, total, nextChange[:], nextOperation[:], authority); err != nil {
		t.Fatal(err)
	}
	result := compactSourceHistoryForTest(t, c, limit)
	if result.operations != 1 || result.publicationReceipts != 1 {
		t.Fatalf("unpinned operation and receipt compaction = %+v, want one each", result)
	}
	assertSourceReceiptCounts(t, c, authority, 1, 1, 1)
}

func compactSourceHistoryForTest(
	t *testing.T,
	c *Catalog,
	limit int,
) sourceHistoryCompactionResult {
	t.Helper()
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := compactSettledSourceHistory(t.Context(), tx, limit)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertSourceReceiptCounts(
	t *testing.T,
	c *Catalog,
	authority string,
	publication int,
	settlement int,
	configuration int,
) {
	t.Helper()
	for table, want := range map[string]int{
		"source_publication_stage_receipts":      publication,
		"source_observer_settlement_receipts":    settlement,
		"source_observer_configuration_receipts": configuration,
	} {
		var got int
		if err := c.readDB.QueryRowContext(
			t.Context(), "SELECT COUNT(*) FROM "+table+" WHERE source_authority = ?", authority,
		).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s rows = %d, want %d", table, got, want)
		}
	}
}

func sourceReceiptOperationID(index int) causal.OperationID {
	var id causal.OperationID
	binary.BigEndian.PutUint64(id[len(id)-8:], uint64(index))
	return id
}

func sourceReceiptChangeID(index int) causal.ChangeID {
	var id causal.ChangeID
	binary.BigEndian.PutUint64(id[len(id)-8:], uint64(index+10_000))
	return id
}

func sourceReceiptDigest(index int) [32]byte {
	var digest [32]byte
	binary.BigEndian.PutUint64(digest[len(digest)-8:], uint64(index))
	return digest
}
