package holder

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

func sequenceTestOwner(pid int, label string) catalog.ProcessRecord {
	return catalog.ProcessRecord{
		RecoveryID: recoveryid.SourceOwner, PID: pid,
		StartTime: label, Boot: "retired-boot",
		Generation: holderOwnerGeneration(label),
	}
}

func sequenceTestWorker(id recoveryid.ID, pid int, label string) catalog.ProcessRecord {
	return catalog.ProcessRecord{
		RecoveryID: id, PID: pid,
		StartTime: label, Boot: "retired-boot",
		Generation: holderOwnerGeneration(label), ProcessGroup: true, SessionID: pid,
	}
}

func TestReclaimNumbersEveryRecoveryIDInItsOwnSpace(t *testing.T) {
	ledger := processRecoveryLedger(t,
		sequenceTestOwner(10_001, "owner-a"),
		sequenceTestWorker(recoveryid.CatalogWorker, 10_002, "worker-a"),
		sequenceTestOwner(10_003, "owner-b"),
		sequenceTestWorker(recoveryid.SourceObserver, 10_004, "observer-a"),
		sequenceTestOwner(10_005, "owner-c"),
	)
	want := map[recoveryid.ID][]uint64{
		recoveryid.SourceOwner:    {1, 2, 3},
		recoveryid.CatalogWorker:  {1},
		recoveryid.SourceObserver: {1},
	}
	for id, sequences := range want {
		var got []uint64
		for _, receipt := range ledger.Receipts(id, 0) {
			got = append(got, receipt.Sequence)
		}
		if len(got) != len(sequences) {
			t.Fatalf("%s receipts = %v, want %v", id, got, sequences)
		}
		for index, sequence := range sequences {
			if got[index] != sequence {
				t.Fatalf("%s receipts = %v, want %v", id, got, sequences)
			}
		}
	}
}

// A catalog floor tracks one recovery ID, so a receipt minted for any other ID
// must not consume a sequence the floor is waiting for.
func TestInterleavedRecoveryIDsLeaveSourceOwnerRecoveryContiguous(t *testing.T) {
	ledger := processRecoveryLedger(t,
		sequenceTestOwner(10_001, "owner-a"),
		sequenceTestWorker(recoveryid.CatalogWorker, 10_002, "worker-a"),
		sequenceTestOwner(10_003, "owner-b"),
	)
	database, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if err := recoverProcessGroupReceipts(t.Context(), ledger, recoveryid.CatalogWorker); err != nil {
		t.Fatalf("catalog-worker recovery: %v", err)
	}
	if err := recoverSourceOwnerReceipts(t.Context(), ledger, database); err != nil {
		t.Fatalf("source-owner recovery after an interleaved catalog-worker receipt: %v", err)
	}
	if pending := ledger.Receipts(recoveryid.SourceOwner, 0); len(pending) != 0 {
		t.Fatalf("drained ledger retained %+v", pending)
	}
}

func TestOpenProcessLedgerSeedsLegacySequencesFromTheGlobalHighWater(t *testing.T) {
	path := filepath.Join(t.TempDir(), "processes.db")
	seed, err := openProcessLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	record := sequenceTestOwner(10_001, "owner-a")
	if err := seed.Track(record); err != nil {
		t.Fatal(err)
	}
	if err := seed.Reclaim(nil); err != nil {
		t.Fatal(err)
	}
	legacy := processLedgerState{
		LedgerID: seed.state.LedgerID,
		Sequence: 7,
		Receipts: seed.state.Receipts,
	}
	for index := range legacy.Receipts {
		legacy.Receipts[index].Sequence = 7
		digest, digestErr := catalog.NewReapReceipt(
			legacy.LedgerID, 7, legacy.Receipts[index].Record,
			legacy.Receipts[index].ReaperGeneration, legacy.Receipts[index].Outcome,
		)
		if digestErr != nil {
			t.Fatal(digestErr)
		}
		legacy.Receipts[index] = digest
	}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	reopened, err := openProcessLedger(path)
	if err != nil {
		t.Fatalf("reopen a pre-per-recovery-ID ledger: %v", err)
	}
	if reopened.state.LedgerID != legacy.LedgerID {
		t.Fatalf(
			"reopened ledger id = %x, want the preserved %x",
			reopened.state.LedgerID, legacy.LedgerID,
		)
	}
	if _, err := os.Stat(path + ".v20"); !os.IsNotExist(err) {
		t.Fatalf("a legacy ledger was archived rather than migrated: %v", err)
	}
	for _, id := range receiptRecoveryIDs {
		if got := reopened.state.Sequences[id]; got != 7 {
			t.Fatalf("%s seeded at %d, want the retired global high-water 7", id, got)
		}
	}
	if pending := reopened.Receipts(recoveryid.SourceOwner, 0); len(pending) != 1 {
		t.Fatalf("migration dropped the pending receipts: %+v", pending)
	}

	next := sequenceTestOwner(10_002, "owner-b")
	if err := reopened.Track(next); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Reclaim(nil); err != nil {
		t.Fatal(err)
	}
	minted := reopened.Receipts(recoveryid.SourceOwner, 0)
	if len(minted) != 2 || minted[1].Sequence != 8 {
		t.Fatalf("post-migration receipts = %+v, want the second at sequence 8", minted)
	}
}

func TestSeedLegacySequencesIsWrittenThroughBeforeUse(t *testing.T) {
	path := filepath.Join(t.TempDir(), "processes.db")
	seed, err := openProcessLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := processLedgerState{LedgerID: seed.state.LedgerID, Sequence: 4}
	encoded, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openProcessLedger(path); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var persisted processLedgerState
	if err := json.Unmarshal(raw, &persisted); err != nil {
		t.Fatal(err)
	}
	for _, id := range receiptRecoveryIDs {
		if got := persisted.Sequences[id]; got != 4 {
			t.Fatalf("persisted %s = %d, want 4: %s", id, got, raw)
		}
	}
}

func TestLegacyLedgerReceiptsSurviveValidation(t *testing.T) {
	for _, sequence := range []uint64{1, 4} {
		t.Run(fmt.Sprintf("sequence-%d", sequence), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "processes.db")
			seed, err := openProcessLedger(path)
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := catalog.NewReapReceipt(
				seed.state.LedgerID, sequence, sequenceTestOwner(10_001, "owner-a"),
				holderOwnerGeneration("reaper"), catalog.ReapAbsent,
			)
			if err != nil {
				t.Fatal(err)
			}
			legacy := processLedgerState{
				LedgerID: seed.state.LedgerID, Sequence: 4,
				Receipts: []catalog.ReapReceipt{receipt},
			}
			if err := legacy.Validate(); err != nil {
				t.Fatalf("a legacy ledger failed validation and would be archived: %v", err)
			}
		})
	}
}
