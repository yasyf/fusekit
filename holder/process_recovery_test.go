package holder

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

func holderOwnerGeneration(label string) proc.OwnerGeneration {
	digest := sha256.Sum256([]byte(label))
	var generation proc.OwnerGeneration
	copy(generation[:], digest[:len(generation)])
	return generation
}

func TestSourceOwnerReceiptRecoveryDrainsPastPageAndReplaysLostAck(t *testing.T) {
	dir := t.TempDir()
	store := &proc.FileStore{Path: filepath.Join(dir, "processes.db")}
	registry := &durableProcessRegistry{Reaper: &proc.Reaper{Store: store, Generation: holderOwnerGeneration("successor")}}
	const count = proc.ReapReceiptPageLimit + 2
	for index := 0; index < count; index++ {
		seedRecoveryReceipt(t, store, proc.Record{
			RecoveryID: recoveryid.SourceOwner,
			PID:        10_000 + index,
			StartTime:  fmt.Sprintf("start-%d", index),
			Boot:       "retired-boot",
			Generation: holderOwnerGeneration(fmt.Sprintf("retired-%d", index)),
		})
	}
	database, err := catalog.Open(t.Context(), filepath.Join(dir, "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	first, err := registry.ReapReceipts(
		t.Context(), recoveryid.SourceOwner, proc.ReapReceiptCursor{}, 1,
	)
	if err != nil || len(first.Receipts) != 1 {
		t.Fatalf("first receipt page = %+v, %v", first, err)
	}
	if _, err := database.RecoverReapedSourceAuthorityRuntimes(t.Context(), first.Receipts[0]); err != nil {
		t.Fatalf("commit semantic recovery before lost acknowledgement: %v", err)
	}

	if err := recoverSourceOwnerReceipts(t.Context(), registry, database); err != nil {
		t.Fatal(err)
	}
	page, err := registry.ReapReceipts(
		t.Context(), recoveryid.SourceOwner, proc.ReapReceiptCursor{}, proc.ReapReceiptPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if page.More || len(page.Receipts) != 0 || page.Floor.Sequence != count {
		t.Fatalf("drained receipt page = %+v, want floor %d and no liability", page, count)
	}
	for restart := 0; restart < 100; restart++ {
		if err := recoverSourceOwnerReceipts(t.Context(), registry, database); err != nil {
			t.Fatalf("empty restart replay %d: %v", restart, err)
		}
	}
}

func TestSourceOwnerReceiptRecoveryReplaysLostCatalogResponseBeforeAcknowledgement(t *testing.T) {
	dir := t.TempDir()
	processStore := &proc.FileStore{Path: filepath.Join(dir, "processes.db")}
	registry := &durableProcessRegistry{Reaper: &proc.Reaper{
		Store: processStore, Generation: holderOwnerGeneration("successor"),
	}}
	record := sourceAuthorityRetiredProcessForTest("retired-holder")
	receipt := seedRecoveryReceipt(t, processStore, record)
	database, err := catalog.Open(t.Context(), filepath.Join(dir, "catalog.sqlite"))
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
	if err := recoverSourceOwnerReceipts(t.Context(), registry, uncertain); !errors.Is(err, lostResponse) {
		t.Fatalf("first recovery = %v, want lost catalog response", err)
	}
	if found, err := processStore.HasReapReceipt(t.Context(), receipt); err != nil || !found {
		t.Fatalf("uncertain catalog result retained receipt = %t, %v", found, err)
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

	if err := recoverSourceOwnerReceipts(t.Context(), registry, database); err != nil {
		t.Fatalf("restart recovery: %v", err)
	}
	if found, err := processStore.HasReapReceipt(t.Context(), receipt); found || !errors.Is(err, proc.ErrReapReceiptStale) {
		t.Fatalf("replayed receipt floor = found %t, error %v", found, err)
	}
}

func TestReceiptRecoveryIDCatalogIsExact(t *testing.T) {
	want := map[proc.RecoveryID]struct{}{
		recoveryid.SourceOwner: {}, recoveryid.SourceDriver: {}, recoveryid.Broker: {},
		recoveryid.NativeMount: {}, recoveryid.CatalogWorker: {}, recoveryid.SourceObserver: {},
		proc.RecoveryTaskID: {}, proc.RecoveryServiceID: {}, proc.RecoveryTrustID: {},
		recoveryid.Holder: {}, proc.RecoveryStopControlID: {},
	}
	seen := make(map[proc.RecoveryID]struct{}, len(receiptRecoveryIDs))
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
	dir := t.TempDir()
	store := &proc.FileStore{Path: filepath.Join(dir, "processes.db")}
	registry := &durableProcessRegistry{Reaper: &proc.Reaper{Store: store, Generation: holderOwnerGeneration("successor")}}
	holderReceipt := seedRecoveryReceipt(t, store, proc.Record{
		RecoveryID: recoveryid.Holder,
		PID:        20_001,
		StartTime:  "holder-start",
		Boot:       "retired-boot",
		Generation: holderOwnerGeneration("retired-holder"),
	})
	driverReceipt := seedRecoveryReceipt(t, store, proc.Record{
		RecoveryID:   recoveryid.SourceDriver,
		PID:          20_002,
		StartTime:    "driver-start",
		Boot:         "retired-boot",
		Generation:   holderOwnerGeneration("retired-driver"),
		ProcessGroup: true,
		SessionID:    20_002,
	})
	if err := recoverHolderReceipts(t.Context(), registry); err == nil {
		t.Fatal("holder receipt crossed an unsettled source-driver liability")
	}
	if found, err := store.HasReapReceipt(t.Context(), holderReceipt); err != nil || !found {
		t.Fatalf("holder receipt retained = %t, %v", found, err)
	}
	if found, err := store.HasReapReceipt(t.Context(), driverReceipt); err != nil || !found {
		t.Fatalf("driver receipt retained = %t, %v", found, err)
	}
}

func TestSourceDriverReceiptWaitsForSemanticCatalogRecovery(t *testing.T) {
	dir := t.TempDir()
	store := &proc.FileStore{Path: filepath.Join(dir, "processes.db")}
	registry := &durableProcessRegistry{Reaper: &proc.Reaper{Store: store, Generation: holderOwnerGeneration("successor")}}
	receipt := seedRecoveryReceipt(t, store, proc.Record{
		RecoveryID: recoveryid.SourceDriver,
		PID:        20_010, StartTime: "driver-start", Boot: "retired-boot",
		Generation: holderOwnerGeneration("retired-driver"), ProcessGroup: true, SessionID: 20_010,
	})
	barrier := &sourceDriverReceiptBarrier{pending: "semantic"}
	if err := recoverSourceDriverReceipts(t.Context(), registry, barrier); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("unsettled source-driver catalog receipt = %v, want integrity", err)
	}
	if found, err := store.HasReapReceipt(t.Context(), receipt); err != nil || !found {
		t.Fatalf("uncertain source-driver receipt retained = %t, %v", found, err)
	}
	barrier.pending = ""
	if err := recoverSourceDriverReceipts(t.Context(), registry, barrier); err != nil {
		t.Fatal(err)
	}
	if barrier.calls != 2 {
		t.Fatalf("catalog receipt barrier calls = %d, want 2", barrier.calls)
	}
	if found, err := store.HasReapReceipt(t.Context(), receipt); found || !errors.Is(err, proc.ErrReapReceiptStale) {
		t.Fatalf("settled source-driver receipt = found %t, error %v", found, err)
	}
}

func TestSourceOwnerRecoveryDoesNotDeadlockBehindSourceDriverReceipt(t *testing.T) {
	dir := t.TempDir()
	store := &proc.FileStore{Path: filepath.Join(dir, "processes.db")}
	registry := &durableProcessRegistry{Reaper: &proc.Reaper{Store: store, Generation: holderOwnerGeneration("successor")}}
	ownerReceipt := seedRecoveryReceipt(t, store, proc.Record{
		RecoveryID: recoveryid.SourceOwner,
		PID:        20_020, StartTime: "owner-start", Boot: "retired-boot", Generation: holderOwnerGeneration("retired-owner"),
	})
	driverReceipt := seedRecoveryReceipt(t, store, proc.Record{
		RecoveryID: recoveryid.SourceDriver,
		PID:        20_021, StartTime: "driver-start", Boot: "retired-boot", Generation: holderOwnerGeneration("retired-driver"),
		ProcessGroup: true, SessionID: 20_021,
	})
	database, err := catalog.Open(t.Context(), filepath.Join(dir, "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := recoverSourceOwnerReceipts(t.Context(), registry, database); err != nil {
		t.Fatalf("source-owner recovery blocked behind source-driver receipt: %v", err)
	}
	if found, err := store.HasReapReceipt(t.Context(), ownerReceipt); found || !errors.Is(err, proc.ErrReapReceiptStale) {
		t.Fatalf("source-owner receipt = found %t, error %v", found, err)
	}
	if found, err := store.HasReapReceipt(t.Context(), driverReceipt); err != nil || !found {
		t.Fatalf("source-driver receipt was not retained = found %t, error %v", found, err)
	}
	if err := recoverSourceDriverReceipts(t.Context(), registry, &sourceDriverReceiptBarrier{}); err != nil {
		t.Fatalf("source-driver recovery after owner settlement: %v", err)
	}
	if err := requireNoReceiptLiabilities(t.Context(), registry); err != nil {
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
	dir := t.TempDir()
	store := &proc.FileStore{Path: filepath.Join(dir, "processes.db")}
	registry := &durableProcessRegistry{Reaper: &proc.Reaper{Store: store, Generation: holderOwnerGeneration("successor")}}
	sourceReceipt := seedRecoveryReceipt(t, store, proc.Record{
		RecoveryID: recoveryid.SourceOwner,
		PID:        21_001,
		StartTime:  "source-owner-start",
		Boot:       "retired-boot",
		Generation: holderOwnerGeneration("source-capable-generation"),
	})
	holderReceipt := seedRecoveryReceipt(t, store, proc.Record{
		RecoveryID: recoveryid.Holder,
		PID:        21_002,
		StartTime:  "holder-start",
		Boot:       "retired-boot",
		Generation: holderOwnerGeneration("mount-only-generation"),
	})
	database, err := catalog.Open(t.Context(), filepath.Join(dir, "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if err := recoverSourceOwnerReceipts(t.Context(), registry, database); err != nil {
		t.Fatalf("recover source-capable generation: %v", err)
	}
	if found, err := store.HasReapReceipt(t.Context(), sourceReceipt); found || !errors.Is(err, proc.ErrReapReceiptStale) {
		t.Fatalf("source-owner receipt floor = found %t, error %v", found, err)
	}
	if found, err := store.HasReapReceipt(t.Context(), holderReceipt); err != nil || !found {
		t.Fatalf("mount-only holder receipt retained = %t, %v", found, err)
	}
	if err := recoverHolderReceipts(t.Context(), registry); err != nil {
		t.Fatalf("recover mount-only generation: %v", err)
	}
	if err := requireNoReceiptLiabilities(t.Context(), registry); err != nil {
		t.Fatalf("owner transition left a liability: %v", err)
	}
}

func TestServiceReceiptRequiresControllerReconciliation(t *testing.T) {
	dir := t.TempDir()
	store := &proc.FileStore{Path: filepath.Join(dir, "processes.db")}
	registry := &durableProcessRegistry{Reaper: &proc.Reaper{Store: store, Generation: holderOwnerGeneration("successor")}}
	serviceReceipt := seedRecoveryReceipt(t, store, proc.Record{
		RecoveryID:   proc.RecoveryServiceID,
		PID:          22_001,
		StartTime:    "service-start",
		Boot:         "retired-boot",
		Generation:   holderOwnerGeneration("retired-service"),
		ProcessGroup: true,
		SessionID:    22_001,
	})
	if err := requireNoReceiptLiabilities(t.Context(), registry); err == nil {
		t.Fatal("service receipt crossed the aggregate recovery barrier")
	}
	if found, err := store.HasReapReceipt(t.Context(), serviceReceipt); err != nil || !found {
		t.Fatalf("service receipt retained = %t, %v", found, err)
	}
}

func seedRecoveryReceipt(t *testing.T, store proc.Store, record proc.Record) proc.ReapReceipt {
	t.Helper()
	if err := store.Add(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginReap(t.Context(), record, holderOwnerGeneration("successor")); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.CommitReap(t.Context(), record, holderOwnerGeneration("successor"), proc.ReapAbsent)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}

type sourceOwnerLostResponseStore struct {
	sourceOwnerRecoveryStore
	responseErr error
	called      bool
}

func (s *sourceOwnerLostResponseStore) RecoverReapedSourceAuthorityRuntimes(
	ctx context.Context,
	receipt proc.ReapReceipt,
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
