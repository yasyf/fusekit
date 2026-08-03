package holder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

func holderOwnerGeneration(label string) catalog.ProcessGeneration {
	digest := sha256.Sum256([]byte(label))
	var generation catalog.ProcessGeneration
	copy(generation[:], digest[:len(generation)])
	return generation
}

func processRecoveryLedger(t *testing.T, records ...catalog.ProcessRecord) *processLedger {
	t.Helper()
	ledger, err := openProcessLedger(filepath.Join(t.TempDir(), "processes.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err := ledger.Track(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := ledger.Reclaim(nil); err != nil {
		t.Fatal(err)
	}
	return ledger
}

func processRecoveryReceipt(
	t *testing.T,
	ledger *processLedger,
	record catalog.ProcessRecord,
) catalog.ReapReceipt {
	t.Helper()
	for _, receipt := range ledger.Receipts(record.RecoveryID, 0) {
		if receipt.Record == record {
			return receipt
		}
	}
	t.Fatalf("no reap receipt for %+v", record)
	return catalog.ReapReceipt{}
}

func processRecoveryPending(ledger *processLedger, receipt catalog.ReapReceipt) bool {
	return slices.Contains(ledger.Receipts(receipt.Record.RecoveryID, 0), receipt)
}

func TestOpenProcessLedgerArchivesPreV21ProcessStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "processes.db")
	legacy := []byte(`{"version":1,"processes":[],"reap_receipts":[]}` + "\n")
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatal(err)
	}
	ledger, err := openProcessLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.state.LedgerID == (catalog.ReceiptLedgerID{}) || ledger.state.Sequence != 0 ||
		len(ledger.state.Records) != 0 || len(ledger.state.Receipts) != 0 {
		t.Fatalf("minted ledger state = %+v", ledger.state)
	}
	archived, err := os.ReadFile(path + ".v20")
	if err != nil {
		t.Fatalf("read archived pre-v0.21 store: %v", err)
	}
	if !bytes.Equal(archived, legacy) {
		t.Fatalf("archived pre-v0.21 store = %q, want %q", archived, legacy)
	}
	reopened, err := openProcessLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.state.LedgerID != ledger.state.LedgerID {
		t.Fatalf(
			"reopened ledger id = %x, want the minted %x",
			reopened.state.LedgerID, ledger.state.LedgerID,
		)
	}
}

func TestSourceOwnerReceiptRecoveryDrainsEveryReceiptAndReplaysLostAck(t *testing.T) {
	const count = 5
	records := make([]catalog.ProcessRecord, count)
	for index := range records {
		records[index] = catalog.ProcessRecord{
			RecoveryID: recoveryid.SourceOwner,
			PID:        10_000 + index,
			StartTime:  fmt.Sprintf("start-%d", index),
			Boot:       "retired-boot",
			Generation: holderOwnerGeneration(fmt.Sprintf("retired-%d", index)),
		}
	}
	ledger := processRecoveryLedger(t, records...)
	database, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	first := ledger.Receipts(recoveryid.SourceOwner, 1)
	if len(first) != 1 || first[0].Sequence != 1 || first[0].Record != records[0] {
		t.Fatalf("first pending receipt = %+v", first)
	}
	if _, err := database.RecoverReapedSourceAuthorityRuntimes(t.Context(), first[0]); err != nil {
		t.Fatalf("commit semantic recovery before lost acknowledgement: %v", err)
	}

	store := &processRecoveryRecordingStore{sourceOwnerRecoveryStore: database}
	if err := recoverSourceOwnerReceipts(t.Context(), ledger, store); err != nil {
		t.Fatal(err)
	}
	wantApplied := make([]uint64, count)
	for index := range wantApplied {
		wantApplied[index] = uint64(index) + 1
	}
	if !slices.Equal(store.applied, wantApplied) {
		t.Fatalf("replayed receipt sequences = %v, want %v", store.applied, wantApplied)
	}
	wantFloor := catalog.ReapReceiptFloor{
		LedgerID: ledger.state.LedgerID, RecoveryID: recoveryid.SourceOwner, Sequence: count,
	}
	if store.acknowledged != 1 || store.floor != wantFloor {
		t.Fatalf(
			"acknowledgements = %d, floor = %+v, want 1 and %+v",
			store.acknowledged, store.floor, wantFloor,
		)
	}
	if pending := ledger.Receipts(recoveryid.SourceOwner, 0); len(pending) != 0 {
		t.Fatalf("drained ledger retained %+v", pending)
	}
	for restart := 0; restart < 100; restart++ {
		if err := recoverSourceOwnerReceipts(t.Context(), ledger, store); err != nil {
			t.Fatalf("empty restart replay %d: %v", restart, err)
		}
	}
	if store.acknowledged != 1 {
		t.Fatalf("empty restarts acknowledged %d times, want 1", store.acknowledged)
	}
}

func TestSourceOwnerReceiptRecoveryReplaysLostCatalogResponseBeforeAcknowledgement(t *testing.T) {
	record := sourceAuthorityRetiredProcessForTest("retired-holder")
	ledger := processRecoveryLedger(t, record)
	receipt := processRecoveryReceipt(t, ledger, record)
	database, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	spec := testSourceAuthoritySpec("source")
	epoch := [16]byte{1}
	seedSourceAuthorityOpenRuntimeForTest(t, database, spec, record, epoch)

	lostResponse := errors.New("lost catalog response")
	uncertain := &sourceOwnerLostResponseStore{
		sourceOwnerRecoveryStore: database,
		responseErr:              lostResponse,
	}
	if err := recoverSourceOwnerReceipts(t.Context(), ledger, uncertain); !errors.Is(err, lostResponse) {
		t.Fatalf("first recovery = %v, want lost catalog response", err)
	}
	if !processRecoveryPending(ledger, receipt) {
		t.Fatalf("uncertain catalog result retired receipt %+v", receipt)
	}
	state, err := database.SourceAuthorityRuntimeStatus(t.Context(), catalog.SourceAuthorityRuntimeRef{
		Owner: "holder-test", Generation: 1, Authority: spec.Authority,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !state.Closed || state.Epoch != epoch || state.Process == nil || *state.Process != record {
		t.Fatalf("lost-response durable catalog state = %+v", state)
	}

	if err := recoverSourceOwnerReceipts(t.Context(), ledger, database); err != nil {
		t.Fatalf("restart recovery: %v", err)
	}
	if processRecoveryPending(ledger, receipt) {
		t.Fatalf("replayed receipt %+v was not retired", receipt)
	}
}

func TestReceiptRecoveryIDCatalogIsExact(t *testing.T) {
	want := map[recoveryid.ID]struct{}{
		recoveryid.SourceOwner: {}, recoveryid.SourceDriver: {}, recoveryid.Broker: {},
		recoveryid.NativeMount: {}, recoveryid.CatalogWorker: {}, recoveryid.SourceObserver: {},
		recoveryid.SourceTask: {}, recoveryid.Holder: {},
	}
	seen := make(map[recoveryid.ID]struct{}, len(receiptRecoveryIDs))
	for _, id := range receiptRecoveryIDs {
		if err := id.Validate(); err != nil {
			t.Fatalf("listed recovery ID %q is invalid: %v", id, err)
		}
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("recovery ID %q is listed twice", id)
		}
		if _, expected := want[id]; !expected {
			t.Fatalf("unexpected recovery ID %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != len(want) {
		t.Fatalf("recovery IDs = %v, want %v", seen, want)
	}
}

func TestHolderReceiptCannotPassAnotherRecoveryIDLiability(t *testing.T) {
	holderRecord := catalog.ProcessRecord{
		RecoveryID: recoveryid.Holder,
		PID:        20_001,
		StartTime:  "holder-start",
		Boot:       "retired-boot",
		Generation: holderOwnerGeneration("retired-holder"),
	}
	driverRecord := catalog.ProcessRecord{
		RecoveryID:   recoveryid.SourceDriver,
		PID:          20_002,
		StartTime:    "driver-start",
		Boot:         "retired-boot",
		Generation:   holderOwnerGeneration("retired-driver"),
		ProcessGroup: true,
		SessionID:    20_002,
	}
	ledger := processRecoveryLedger(t, holderRecord, driverRecord)
	holderReceipt := processRecoveryReceipt(t, ledger, holderRecord)
	driverReceipt := processRecoveryReceipt(t, ledger, driverRecord)
	if err := recoverHolderReceipts(t.Context(), ledger); err == nil {
		t.Fatal("holder receipt crossed an unsettled source-driver liability")
	}
	if !processRecoveryPending(ledger, holderReceipt) {
		t.Fatalf("holder receipt %+v was not retained", holderReceipt)
	}
	if !processRecoveryPending(ledger, driverReceipt) {
		t.Fatalf("driver receipt %+v was not retained", driverReceipt)
	}
}

func TestSourceDriverReceiptWaitsForSemanticCatalogRecovery(t *testing.T) {
	record := catalog.ProcessRecord{
		RecoveryID: recoveryid.SourceDriver,
		PID:        20_010, StartTime: "driver-start", Boot: "retired-boot",
		Generation: holderOwnerGeneration("retired-driver"), ProcessGroup: true, SessionID: 20_010,
	}
	ledger := processRecoveryLedger(t, record)
	receipt := processRecoveryReceipt(t, ledger, record)
	barrier := &sourceDriverReceiptBarrier{pending: "semantic"}
	if err := recoverSourceDriverReceipts(t.Context(), ledger, barrier); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("unsettled source-driver catalog receipt = %v, want integrity", err)
	}
	if !processRecoveryPending(ledger, receipt) {
		t.Fatalf("uncertain source-driver receipt %+v was not retained", receipt)
	}
	barrier.pending = ""
	if err := recoverSourceDriverReceipts(t.Context(), ledger, barrier); err != nil {
		t.Fatal(err)
	}
	if barrier.calls != 2 {
		t.Fatalf("catalog receipt barrier calls = %d, want 2", barrier.calls)
	}
	if processRecoveryPending(ledger, receipt) {
		t.Fatalf("settled source-driver receipt %+v was not retired", receipt)
	}
}

func TestSourceOwnerRecoveryDoesNotDeadlockBehindSourceDriverReceipt(t *testing.T) {
	ownerRecord := catalog.ProcessRecord{
		RecoveryID: recoveryid.SourceOwner,
		PID:        20_020, StartTime: "owner-start", Boot: "retired-boot",
		Generation: holderOwnerGeneration("retired-owner"),
	}
	driverRecord := catalog.ProcessRecord{
		RecoveryID: recoveryid.SourceDriver,
		PID:        20_021, StartTime: "driver-start", Boot: "retired-boot",
		Generation:   holderOwnerGeneration("retired-driver"),
		ProcessGroup: true, SessionID: 20_021,
	}
	ledger := processRecoveryLedger(t, ownerRecord, driverRecord)
	ownerReceipt := processRecoveryReceipt(t, ledger, ownerRecord)
	driverReceipt := processRecoveryReceipt(t, ledger, driverRecord)
	database, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := recoverSourceOwnerReceipts(t.Context(), ledger, database); err != nil {
		t.Fatalf("source-owner recovery blocked behind source-driver receipt: %v", err)
	}
	if processRecoveryPending(ledger, ownerReceipt) {
		t.Fatalf("source-owner receipt %+v was not retired", ownerReceipt)
	}
	if !processRecoveryPending(ledger, driverReceipt) {
		t.Fatalf("source-driver receipt %+v was not retained", driverReceipt)
	}
	if err := recoverSourceDriverReceipts(t.Context(), ledger, &sourceDriverReceiptBarrier{}); err != nil {
		t.Fatalf("source-driver recovery after owner settlement: %v", err)
	}
	if err := requireNoReceiptLiabilities(t.Context(), ledger); err != nil {
		t.Fatalf("mixed recovery left a liability: %v", err)
	}
}

type sourceDriverReceiptBarrier struct {
	pending causal.SourceAuthorityID
	calls   int
}

func (b *sourceDriverReceiptBarrier) PendingSourceDriverReceiptAuthorities(
	context.Context,
	causal.SourceAuthorityID,
	int,
) (catalog.SourceDriverReceiptAuthorityPage, error) {
	b.calls++
	if b.pending == "" {
		return catalog.SourceDriverReceiptAuthorityPage{}, nil
	}
	return catalog.SourceDriverReceiptAuthorityPage{Authorities: []causal.SourceAuthorityID{b.pending}}, nil
}

func TestOwnerRecoveryIDTransitionSettlesSourceBeforeHolder(t *testing.T) {
	sourceRecord := catalog.ProcessRecord{
		RecoveryID: recoveryid.SourceOwner,
		PID:        21_001,
		StartTime:  "source-owner-start",
		Boot:       "retired-boot",
		Generation: holderOwnerGeneration("source-capable-generation"),
	}
	holderRecord := catalog.ProcessRecord{
		RecoveryID: recoveryid.Holder,
		PID:        21_002,
		StartTime:  "holder-start",
		Boot:       "retired-boot",
		Generation: holderOwnerGeneration("mount-only-generation"),
	}
	ledger := processRecoveryLedger(t, sourceRecord, holderRecord)
	sourceReceipt := processRecoveryReceipt(t, ledger, sourceRecord)
	holderReceipt := processRecoveryReceipt(t, ledger, holderRecord)
	database, err := catalog.Open(t.Context(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if err := recoverSourceOwnerReceipts(t.Context(), ledger, database); err != nil {
		t.Fatalf("recover source-capable generation: %v", err)
	}
	if processRecoveryPending(ledger, sourceReceipt) {
		t.Fatalf("source-owner receipt %+v was not retired", sourceReceipt)
	}
	if !processRecoveryPending(ledger, holderReceipt) {
		t.Fatalf("mount-only holder receipt %+v was not retained", holderReceipt)
	}
	if err := recoverHolderReceipts(t.Context(), ledger); err != nil {
		t.Fatalf("recover mount-only generation: %v", err)
	}
	if err := requireNoReceiptLiabilities(t.Context(), ledger); err != nil {
		t.Fatalf("owner transition left a liability: %v", err)
	}
}

type processRecoveryRecordingStore struct {
	sourceOwnerRecoveryStore
	applied      []uint64
	floor        catalog.ReapReceiptFloor
	acknowledged int
}

func (s *processRecoveryRecordingStore) RecoverReapedSourceAuthorityRuntimes(
	ctx context.Context,
	receipt catalog.ReapReceipt,
) (catalog.SourceAuthorityRuntimeRecoveryResult, error) {
	s.applied = append(s.applied, receipt.Sequence)
	return s.sourceOwnerRecoveryStore.RecoverReapedSourceAuthorityRuntimes(ctx, receipt)
}

func (s *processRecoveryRecordingStore) AcknowledgeSourceAuthorityRuntimeRecovery(
	ctx context.Context,
	floor catalog.ReapReceiptFloor,
) error {
	s.acknowledged++
	s.floor = floor
	return s.sourceOwnerRecoveryStore.AcknowledgeSourceAuthorityRuntimeRecovery(ctx, floor)
}

type sourceOwnerLostResponseStore struct {
	sourceOwnerRecoveryStore
	responseErr error
	called      bool
}

func (s *sourceOwnerLostResponseStore) RecoverReapedSourceAuthorityRuntimes(
	ctx context.Context,
	receipt catalog.ReapReceipt,
) (catalog.SourceAuthorityRuntimeRecoveryResult, error) {
	result, err := s.sourceOwnerRecoveryStore.RecoverReapedSourceAuthorityRuntimes(ctx, receipt)
	if err != nil {
		return catalog.SourceAuthorityRuntimeRecoveryResult{}, err
	}
	if !s.called {
		s.called = true
		return catalog.SourceAuthorityRuntimeRecoveryResult{}, s.responseErr
	}
	return result, nil
}
