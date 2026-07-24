package catalog

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

func TestSourceAuthorityRuntimeRecoveryClosesExactOwnerSetAtomicallyAndReplays(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	owner := SourceAuthorityFleetOwnerID("runtime-recovery")
	process := sourceAuthorityRuntimeProcessForTest("runtime-recovery-first")
	fences := seedSourceAuthorityRuntimeRecoveryFleet(
		t, c, owner, process, "alpha", "beta",
	)
	receipt := sourceAuthorityReapReceiptForTest(t, process)

	wrong := process
	wrong.StartTime = "different-start"
	wrongReceipt := receipt
	wrongReceipt.Record = wrong
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), wrongReceipt,
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("invalid owner proof = %v, want ErrInvalidObject", err)
	}
	for _, fence := range fences {
		assertSourceAuthorityRuntimeClosed(t, c, fence, false)
	}

	boom := errors.New("crash after close-all")
	c.failpoint = func(point string) error {
		if point == sourceAuthorityRuntimeRecoveryAfterClose {
			return boom
		}
		return nil
	}
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), receipt,
	); !errors.Is(err, boom) {
		t.Fatalf("crashed recovery = %v, want failpoint", err)
	}
	c.failpoint = nil
	for _, fence := range fences {
		assertSourceAuthorityRuntimeClosed(t, c, fence, false)
	}

	result, err := c.RecoverReapedSourceAuthorityRuntimes(t.Context(), receipt)
	if err != nil {
		t.Fatalf("close-all recovery: %v", err)
	}
	if !reflect.DeepEqual(result.Closed, fences) {
		t.Fatalf("closed set = %+v, want %+v", result.Closed, fences)
	}
	for _, fence := range fences {
		assertSourceAuthorityRuntimeClosed(t, c, fence, true)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close reopened catalog: %v", err)
		}
	}()
	replayed, err := c.RecoverReapedSourceAuthorityRuntimes(t.Context(), receipt)
	if err != nil {
		t.Fatalf("lost-response replay after reopen: %v", err)
	}
	if !reflect.DeepEqual(replayed, result) {
		t.Fatalf("replayed result = %+v, want %+v", replayed, result)
	}
	other := sourceAuthorityRuntimeProcessForTest("runtime-recovery-other")
	empty, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), sourceAuthorityReapReceiptForTestAt(t, other, receipt.LedgerID, 2),
	)
	if err != nil || empty.Summary.ClosedCount != 0 || len(empty.Closed) != 0 {
		t.Fatalf("zero-match recovery = %+v, %v; want durable empty result", empty, err)
	}
}

func TestSourceAuthorityRuntimeRecoveryPersistsExactEmptyResult(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	process := sourceAuthorityRuntimeProcessForTest("runtime-recovery-empty")
	receipt := sourceAuthorityReapReceiptForTest(t, process)
	wrong := receipt
	wrong.Record.StartTime = "wrong-start"
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(t.Context(), wrong); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("wrong empty receipt = %v, want ErrInvalidObject", err)
	}

	result, err := c.RecoverReapedSourceAuthorityRuntimes(t.Context(), receipt)
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest, err := SourceAuthorityRuntimeRecoveryDigest(receipt, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary != (SourceAuthorityRuntimeRecoverySummary{
		ReceiptDigest: receipt.Digest,
		ClosedDigest:  expectedDigest,
	}) || len(result.Closed) != 0 {
		t.Fatalf("empty recovery = %+v", result)
	}
	var receipts, members int
	if err := c.db.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_authority_runtime_recovery_receipts
WHERE receipt_digest = ?`, receipt.Digest[:]).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := c.db.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_authority_runtime_recovery_members
WHERE receipt_digest = ?`, receipt.Digest[:]).Scan(&members); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || members != 0 {
		t.Fatalf("durable empty proof = receipts %d members %d", receipts, members)
	}
	if compacted := compactSourceHistoryForTest(t, c, 256); compacted.runtimeRecoveryReceipts != 0 {
		t.Fatalf("empty recovery proof compacted before acknowledgement: %+v", compacted)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Close(); err != nil {
			t.Errorf("close reopened catalog: %v", err)
		}
	}()
	replayed, err := c.RecoverReapedSourceAuthorityRuntimes(t.Context(), receipt)
	if err != nil || !reflect.DeepEqual(replayed, result) {
		t.Fatalf("empty lost-response replay = %+v, %v; want %+v", replayed, err, result)
	}
	floor := proc.ReapReceiptFloor{
		LedgerID: receipt.LedgerID, RecoveryID: recoveryid.SourceOwner, Sequence: receipt.Sequence,
	}
	if err := c.AcknowledgeSourceAuthorityRuntimeRecovery(t.Context(), floor); err != nil {
		t.Fatal(err)
	}
	if compacted := compactSourceHistoryForTest(t, c, 256); compacted.runtimeRecoveryReceipts != 1 {
		t.Fatalf("acknowledged empty recovery compaction = %+v, want one receipt", compacted)
	}
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), receipt,
	); !errors.Is(err, proc.ErrReapReceiptStale) {
		t.Fatalf("compacted empty recovery replay = %v, want stale receipt", err)
	}
}

func TestSourceAuthorityRuntimeRecoveryFencesLedgerSequenceAndAcknowledgementFloor(t *testing.T) {
	c := newTestCatalog(t)
	ledgerID := proc.ReceiptLedgerID{7}
	firstProcess := sourceAuthorityRuntimeProcessForTest("runtime-recovery-floor-first")
	first := sourceAuthorityReapReceiptForTestAt(t, firstProcess, ledgerID, 1)
	gap := sourceAuthorityReapReceiptForTestAt(t, firstProcess, ledgerID, 2)
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), gap,
	); !errors.Is(err, proc.ErrReapReceiptOrder) {
		t.Fatalf("initial receipt gap = %v, want reap receipt order", err)
	}
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	wrongLedger := sourceAuthorityReapReceiptForTestAt(
		t, sourceAuthorityRuntimeProcessForTest("runtime-recovery-wrong-ledger"),
		proc.ReceiptLedgerID{8}, 1,
	)
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), wrongLedger,
	); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("wrong recovery ledger = %v, want mutation conflict", err)
	}
	wrongClassProcess := sourceAuthorityRuntimeProcessForTest("runtime-recovery-wrong-class")
	wrongClassProcess.RecoveryID = recoveryid.CatalogWorker
	wrongClass := sourceAuthorityReapReceiptForTestAt(t, wrongClassProcess, ledgerID, 2)
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), wrongClass,
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("wrong recovery class = %v, want invalid object", err)
	}
	if err := c.AcknowledgeSourceAuthorityRuntimeRecovery(
		t.Context(),
		proc.ReapReceiptFloor{
			LedgerID: proc.ReceiptLedgerID{8}, RecoveryID: recoveryid.SourceOwner, Sequence: 1,
		},
	); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("wrong acknowledgement ledger = %v, want mutation conflict", err)
	}
	if err := c.AcknowledgeSourceAuthorityRuntimeRecovery(
		t.Context(),
		proc.ReapReceiptFloor{
			LedgerID: ledgerID, RecoveryID: recoveryid.CatalogWorker, Sequence: 1,
		},
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("wrong acknowledgement class = %v, want invalid object", err)
	}
	if err := c.AcknowledgeSourceAuthorityRuntimeRecovery(
		t.Context(),
		proc.ReapReceiptFloor{
			LedgerID: ledgerID, RecoveryID: recoveryid.SourceOwner, Sequence: 2,
		},
	); !errors.Is(err, proc.ErrReapReceiptOrder) {
		t.Fatalf("future acknowledgement floor = %v, want reap receipt order", err)
	}
	firstFloor := proc.ReapReceiptFloor{
		LedgerID: ledgerID, RecoveryID: recoveryid.SourceOwner, Sequence: 1,
	}
	if err := c.AcknowledgeSourceAuthorityRuntimeRecovery(t.Context(), firstFloor); err != nil {
		t.Fatal(err)
	}
	if err := c.AcknowledgeSourceAuthorityRuntimeRecovery(t.Context(), firstFloor); err != nil {
		t.Fatalf("acknowledgement replay: %v", err)
	}
	if compacted := compactSourceHistoryForTest(t, c, 256); compacted.runtimeRecoveryReceipts != 1 {
		t.Fatalf("acknowledged recovery compaction = %+v, want one receipt", compacted)
	}
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), first,
	); !errors.Is(err, proc.ErrReapReceiptStale) {
		t.Fatalf("compacted recovery receipt = %v, want stale receipt", err)
	}
	second := sourceAuthorityReapReceiptForTestAt(
		t, sourceAuthorityRuntimeProcessForTest("runtime-recovery-floor-second"), ledgerID, 2,
	)
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(t.Context(), second); err != nil {
		t.Fatalf("next recovery after compacted floor: %v", err)
	}
	secondFloor := firstFloor
	secondFloor.Sequence = 2
	if err := c.AcknowledgeSourceAuthorityRuntimeRecovery(t.Context(), secondFloor); err != nil {
		t.Fatal(err)
	}
	var processed, acknowledged uint64
	if err := c.db.QueryRowContext(t.Context(), `
SELECT processed_sequence, acknowledged_sequence
FROM source_authority_runtime_recovery_floors
WHERE ledger_id = ?`, ledgerID[:]).Scan(&processed, &acknowledged); err != nil {
		t.Fatal(err)
	}
	if processed != 2 || acknowledged != 2 {
		t.Fatalf("recovery floors = processed %d acknowledged %d, want 2/2", processed, acknowledged)
	}
}

func TestSourceAuthorityRuntimeRecoveryStaleOwnerProducesExactEmptyProof(t *testing.T) {
	c := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("runtime-recovery-stale")
	stale := sourceAuthorityRuntimeProcessForTest("runtime-recovery-stale")
	fences := seedSourceAuthorityRuntimeRecoveryFleet(t, c, owner, stale, "alpha")
	if err := c.CloseSourceAuthorityRuntime(t.Context(), fences[0]); err != nil {
		t.Fatal(err)
	}
	successor := sourceAuthorityRuntimeProcessForTest("runtime-recovery-successor")
	if err := c.TakeoverSourceAuthorityRuntime(t.Context(), SourceAuthorityRuntimeTakeover{
		Ref: SourceAuthorityRuntimeRef{
			Owner: owner, Generation: fences[0].Generation, Authority: fences[0].Authority,
		},
		ExpectedEpoch: fences[0].Epoch,
		Epoch:         [16]byte{9},
		Process:       successor,
	}); err != nil {
		t.Fatal(err)
	}
	result, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), sourceAuthorityReapReceiptForTest(t, stale),
	)
	if err != nil || result.Summary.ClosedCount != 0 || len(result.Closed) != 0 {
		t.Fatalf("stale owner recovery = %+v, %v; want exact empty proof", result, err)
	}
	state, err := c.SourceAuthorityRuntimeStatus(t.Context(), SourceAuthorityRuntimeRef{
		Owner: owner, Generation: fences[0].Generation, Authority: fences[0].Authority,
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.Closed || state.Process != successor || state.Epoch != ([16]byte{9}) {
		t.Fatalf("stale receipt changed successor runtime: %+v", state)
	}
}

func TestSourceAuthorityRuntimeRecoveryIncludesAlreadyClosedExactOwnerRows(t *testing.T) {
	c := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("runtime-partial-close")
	process := sourceAuthorityRuntimeProcessForTest("runtime-partial-close")
	fences := seedSourceAuthorityRuntimeRecoveryFleet(
		t, c, owner, process, "alpha", "beta",
	)
	if err := c.CloseSourceAuthorityRuntime(t.Context(), fences[0]); err != nil {
		t.Fatal(err)
	}
	result, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), sourceAuthorityReapReceiptForTest(t, process),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Closed, fences) {
		t.Fatalf("partial-close recovery = %+v, want %+v", result.Closed, fences)
	}
	for _, fence := range fences {
		assertSourceAuthorityRuntimeClosed(t, c, fence, true)
	}
}

func TestSourceAuthorityRuntimeRecoveryReceiptCompactsOnlyAfterDaemonAcknowledgement(t *testing.T) {
	c := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("runtime-recovery-retention")
	process := sourceAuthorityRuntimeProcessForTest("runtime-recovery-retention")
	seedSourceAuthorityRuntimeRecoveryFleet(
		t, c, owner, process, "alpha", "beta",
	)
	receipt := sourceAuthorityReapReceiptForTest(t, process)
	result, err := c.RecoverReapedSourceAuthorityRuntimes(t.Context(), receipt)
	if err != nil {
		t.Fatal(err)
	}
	if compacted := compactSourceHistoryForTest(t, c, 256); compacted.runtimeRecoveryReceipts != 0 {
		t.Fatalf("current recovery receipt compacted early: %+v", compacted)
	}
	replayed, err := c.RecoverReapedSourceAuthorityRuntimes(t.Context(), receipt)
	if err != nil || !reflect.DeepEqual(replayed, result) {
		t.Fatalf("retained recovery replay = %+v, %v; want %+v", replayed, err, result)
	}
	floor := proc.ReapReceiptFloor{
		LedgerID: receipt.LedgerID, RecoveryID: recoveryid.SourceOwner, Sequence: receipt.Sequence,
	}
	if err := c.AcknowledgeSourceAuthorityRuntimeRecovery(t.Context(), floor); err != nil {
		t.Fatal(err)
	}
	if compacted := compactSourceHistoryForTest(t, c, 256); compacted.runtimeRecoveryReceipts != 1 {
		t.Fatalf("acknowledged recovery compaction = %+v, want one receipt", compacted)
	}
	if _, err := c.RecoverReapedSourceAuthorityRuntimes(
		t.Context(), receipt,
	); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("compacted stale recovery = %v, want ErrMutationConflict", err)
	}
}

func TestSourceAuthorityRuntimeRecoveryOwnerProbeUsesCoveringIndex(t *testing.T) {
	c := newTestCatalog(t)
	process := sourceAuthorityRuntimeProcessForTest("runtime-recovery-plan")
	ownerJSON, ownerDigest, err := sourceAuthorityRuntimeOwner(process)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := c.db.QueryContext(t.Context(), `EXPLAIN QUERY PLAN
SELECT member.owner_id, member.generation, member.source_authority, member.runtime_epoch
FROM source_authority_fleet_members member INDEXED BY source_authority_fleet_members_runtime_owner
JOIN source_authority_fleet_heads head
  ON head.owner_id = member.owner_id AND head.generation = member.generation
WHERE member.runtime_owner_digest = ? AND member.runtime_owner_json = ?
ORDER BY member.owner_id, member.generation, member.source_authority`, ownerDigest[:], ownerJSON)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close runtime recovery plan rows: %v", err)
		}
	}()
	var details string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		details += "\n" + detail
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(details, "source_authority_fleet_members_runtime_owner") {
		t.Fatalf("runtime recovery plan missed owner index:%s", details)
	}
}

func TestSourceAuthorityRuntimeRecoverySQLTransitionsRejectUnfencedOwnerRewrites(t *testing.T) {
	c := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("runtime-sql-fence")
	fleet := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, "source")
	acknowledgeSourceAuthorityFleetForTest(t, c, fleet)
	epochOne := [16]byte{1}
	epochTwo := [16]byte{2}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_authority_fleet_members
SET runtime_epoch = ?, runtime_closed = 0
WHERE owner_id = ? AND generation = 1 AND source_authority = 'source'`,
		epochOne[:], string(owner)); err == nil {
		t.Fatal("direct runtime open without owner proof succeeded")
	}
	ref := SourceAuthorityRuntimeRef{
		Owner: owner, Generation: 1, Authority: "source",
	}
	process := sourceAuthorityRuntimeProcessForTest("runtime-sql-fence")
	if err := c.TakeoverSourceAuthorityRuntime(
		t.Context(),
		SourceAuthorityRuntimeTakeover{
			Ref: ref, Epoch: epochOne, Process: process,
		},
	); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_authority_fleet_members
SET runtime_owner_json = runtime_owner_json,
    runtime_owner_digest = runtime_owner_digest,
    runtime_epoch = runtime_epoch,
    runtime_closed = runtime_closed
WHERE owner_id = ? AND generation = 1 AND source_authority = 'source'`,
		string(owner)); err != nil {
		t.Fatalf("exact runtime state no-op: %v", err)
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_authority_fleet_members
SET runtime_epoch = ?
WHERE owner_id = ? AND generation = 1 AND source_authority = 'source'`,
		epochTwo[:], string(owner)); err == nil {
		t.Fatal("open runtime epoch was rewritten directly")
	}
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_authority_fleet_members
SET runtime_closed = 1, runtime_owner_json = X'01'
WHERE owner_id = ? AND generation = 1 AND source_authority = 'source'`,
		string(owner)); err == nil {
		t.Fatal("runtime close changed its owner proof")
	}
	state, err := c.SourceAuthorityRuntimeStatus(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if state.Closed || state.Epoch != epochOne || state.Process != process {
		t.Fatalf("rejected SQL rewrites changed runtime = %+v", state)
	}
}

func seedSourceAuthorityRuntimeRecoveryFleet(
	t *testing.T,
	c *Catalog,
	owner SourceAuthorityFleetOwnerID,
	process proc.Record,
	authorities ...causal.SourceAuthorityID,
) []SourceAuthorityRuntimeFence {
	t.Helper()
	fleet := reconcileSourceAuthorityFleetForTest(t, c, owner, 0, 1, authorities...)
	acknowledgeSourceAuthorityFleetForTest(t, c, fleet)
	fences := make([]SourceAuthorityRuntimeFence, 0, len(authorities))
	for index, authority := range authorities {
		fence := SourceAuthorityRuntimeFence{
			Owner: owner, Generation: 1, Authority: authority,
			Epoch: [16]byte{byte(index + 1)},
		}
		if err := c.TakeoverSourceAuthorityRuntime(
			t.Context(),
			SourceAuthorityRuntimeTakeover{
				Ref: SourceAuthorityRuntimeRef{
					Owner: owner, Generation: 1, Authority: authority,
				},
				Epoch: fence.Epoch, Process: process,
			},
		); err != nil {
			t.Fatalf("take over %s: %v", authority, err)
		}
		fences = append(fences, fence)
	}
	return fences
}

func assertSourceAuthorityRuntimeClosed(
	t *testing.T,
	c *Catalog,
	fence SourceAuthorityRuntimeFence,
	closed bool,
) {
	t.Helper()
	state, err := c.SourceAuthorityRuntimeStatus(t.Context(), SourceAuthorityRuntimeRef{
		Owner: fence.Owner, Generation: fence.Generation, Authority: fence.Authority,
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.Epoch != fence.Epoch || state.Closed != closed {
		t.Fatalf("runtime state = %+v, want epoch %x closed=%v", state, fence.Epoch, closed)
	}
}

func sourceAuthorityReapReceiptForTest(t *testing.T, record proc.Record) proc.ReapReceipt {
	return sourceAuthorityReapReceiptForTestAt(t, record, proc.ReceiptLedgerID{1}, 1)
}

func sourceAuthorityReapReceiptForTestAt(
	t *testing.T,
	record proc.Record,
	ledgerID proc.ReceiptLedgerID,
	sequence uint64,
) proc.ReapReceipt {
	t.Helper()
	payload, err := json.Marshal(struct {
		LedgerID         proc.ReceiptLedgerID `json:"ledger_id"`
		Sequence         uint64               `json:"sequence"`
		Record           proc.Record          `json:"record"`
		ReaperGeneration proc.OwnerGeneration `json:"reaper_generation"`
		Outcome          proc.ReapOutcome     `json:"outcome"`
	}{
		LedgerID: ledgerID, Sequence: sequence,
		Record: record, ReaperGeneration: sourceAuthorityRuntimeProcessForTest("runtime-recovery-successor").Generation,
		Outcome: proc.ReapAbsent,
	})
	if err != nil {
		t.Fatal(err)
	}
	receipt := proc.ReapReceipt{
		LedgerID: ledgerID, Sequence: sequence,
		Record: record, ReaperGeneration: sourceAuthorityRuntimeProcessForTest("runtime-recovery-successor").Generation,
		Outcome: proc.ReapAbsent, Digest: sha256.Sum256(payload),
	}
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	return receipt
}
