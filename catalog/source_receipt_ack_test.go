package catalog

import (
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestUnacknowledgedSourceReceiptsSurviveCompactionRestartAndExactReplay(t *testing.T) {
	database := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	authority := causal.SourceAuthorityID("receipt-replay")
	roots := []SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/root", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	checkpoints := []SourceObserverCheckpointRecord{{Stream: "stream", RootEpoch: "epoch"}}
	firstConfig := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{15: 0xa1}, "stream", "epoch", roots, checkpoints,
	)
	firstConfigRef, firstStream := commitSourceObserverConfigurationForReceiptTest(
		t, store, firstConfig, roots, checkpoints,
	)
	secondConfig := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{15: 0xa2}, "stream-next", "epoch-next", roots,
		[]SourceObserverCheckpointRecord{{Stream: "stream-next", RootEpoch: "epoch-next"}},
	)
	secondConfig.Reset = true
	secondConfigRef, _ := commitSourceObserverConfigurationForReceiptTest(
		t, store, secondConfig, roots,
		[]SourceObserverCheckpointRecord{{Stream: "stream-next", RootEpoch: "epoch-next"}},
	)
	if err := store.AcknowledgeSourceObserverConfiguration(t.Context(), secondConfigRef); err != nil {
		t.Fatalf("AcknowledgeSourceObserverConfiguration(newer): %v", err)
	}

	firstSettlement := stageObserverSettlementOnlyForTest(t, store, causal.OperationID{0x51})
	firstSettlementResult, err := store.CommitSourcePublicationStage(t.Context(), firstSettlement)
	if err != nil {
		t.Fatalf("CommitSourcePublicationStage(first): %v", err)
	}
	settlementStream, err := store.SourceObserverStream(t.Context(), firstSettlement.Authority)
	if err != nil {
		t.Fatal(err)
	}
	secondSettlementIdentity := SourcePublicationStageIdentity{
		Authority: firstSettlement.Authority, FleetOwner: firstSettlement.FleetOwner,
		FleetGeneration: firstSettlement.FleetGeneration, DriverID: firstSettlement.DriverID,
		DeclarationDigest: firstSettlement.DeclarationDigest, Operation: causal.OperationID{0x52},
		Stream: settlementStream.Stream, RootEpoch: settlementStream.RootEpoch,
		Through: firstSettlement.Through, Predecessor: firstSettlementResult.Last,
	}
	if err := store.BeginSourcePublicationStage(t.Context(), secondSettlementIdentity); err != nil {
		t.Fatal(err)
	}
	secondSettlement, err := store.AppendSourcePublicationStage(
		t.Context(), secondSettlementIdentity,
		SourcePublicationStagePage{
			Index:    []SourcePhysicalIndexRecord{observerIndexRecord("derived.json")},
			Complete: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitSourcePublicationStage(t.Context(), secondSettlement); err != nil {
		t.Fatalf("CommitSourcePublicationStage(second): %v", err)
	}
	if err := store.AcknowledgeSourceObserverSettlement(t.Context(), secondSettlement); err != nil {
		t.Fatalf("AcknowledgeSourceObserverSettlement(newer): %v", err)
	}

	compactSourceHistoryForTest(t, store, 1024)
	assertSourceReceiptAcknowledgementForTest(
		t, store, "source_observer_configuration_receipts", "operation_id",
		authority, firstConfig.Operation, 0,
	)
	assertSourceReceiptAcknowledgementForTest(
		t, store, "source_observer_settlement_receipts", "stage_operation_id",
		firstSettlement.Authority, firstSettlement.Operation, 0,
	)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	replayedStream, err := store.CommitSourceObserverConfiguration(t.Context(), firstConfigRef)
	if err != nil || replayedStream != firstStream {
		t.Fatalf("configuration replay after restart = %+v, %v; want %+v", replayedStream, err, firstStream)
	}
	replayedSettlement, err := store.CommitSourcePublicationStage(t.Context(), firstSettlement)
	if err != nil || replayedSettlement != firstSettlementResult {
		t.Fatalf(
			"settlement replay after restart = %+v, %v; want %+v",
			replayedSettlement, err, firstSettlementResult,
		)
	}
	if err := store.AcknowledgeSourceObserverConfiguration(t.Context(), firstConfigRef); err != nil {
		t.Fatalf("AcknowledgeSourceObserverConfiguration(first): %v", err)
	}
	if err := store.AcknowledgeSourceObserverSettlement(t.Context(), firstSettlement); err != nil {
		t.Fatalf("AcknowledgeSourceObserverSettlement(first): %v", err)
	}
	for compactSourceHistoryForTest(t, store, 1024).more {
	}
	assertSourceReceiptMissingForTest(
		t, store, "source_observer_configuration_receipts", "operation_id",
		authority, firstConfig.Operation,
	)
	assertSourceReceiptMissingForTest(
		t, store, "source_observer_settlement_receipts", "stage_operation_id",
		firstSettlement.Authority, firstSettlement.Operation,
	)
}

func commitSourceObserverConfigurationForReceiptTest(
	t *testing.T,
	store *Catalog,
	identity SourceObserverConfigurationIdentity,
	roots []SourceObserverRootRecord,
	checkpoints []SourceObserverCheckpointRecord,
) (SourceObserverConfigurationRef, SourceObserverStreamRecord) {
	t.Helper()
	ensureSourceObserverFleetForTest(t, store, identity)
	if err := store.BeginSourceObserverConfiguration(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := store.AppendSourceObserverConfigurationRoots(
		t.Context(), identity.Authority, identity.Operation,
		SourceObserverRootAppendPage{Records: roots},
	)
	if err != nil {
		t.Fatal(err)
	}
	ref, err = store.AppendSourceObserverConfigurationCheckpoints(
		t.Context(), identity.Authority, identity.Operation,
		SourceObserverCheckpointAppendPage{Sequence: ref.Sequence, Records: checkpoints},
	)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := store.CommitSourceObserverConfiguration(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	return ref, stream
}

func assertSourceReceiptAcknowledgementForTest(
	t *testing.T,
	store *Catalog,
	table, operationColumn string,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
	want int,
) {
	t.Helper()
	var got int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT acknowledged FROM `+table+`
WHERE source_authority = ? AND `+operationColumn+` = ?`,
		string(authority), operation[:]).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s acknowledged = %d, want %d", table, got, want)
	}
}

func assertSourceReceiptMissingForTest(
	t *testing.T,
	store *Catalog,
	table, operationColumn string,
	authority causal.SourceAuthorityID,
	operation causal.OperationID,
) {
	t.Helper()
	var count int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM `+table+`
WHERE source_authority = ? AND `+operationColumn+` = ?`,
		string(authority), operation[:]).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("%s retained acknowledged predecessor", table)
	}
}
