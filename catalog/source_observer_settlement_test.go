package catalog

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceObserverSettlementReceiptIsExactAndDoesNotAdvanceSourceRevision(t *testing.T) {
	c := newTestCatalog(t)
	ref := stageObserverSettlementOnlyForTest(t, c, causal.OperationID{0x51})
	before, err := c.SourceWatermark(t.Context(), ref.Authority)
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.CommitSourcePublicationStage(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if result.Count != 0 || result.First != before || result.Last != before {
		t.Fatalf("settlement result = %+v, prior watermark %d", result, before)
	}
	after, err := c.SourceWatermark(t.Context(), ref.Authority)
	if err != nil || after != before {
		t.Fatalf("settlement advanced source watermark to %d, %v; want %d", after, err, before)
	}
	replayed, err := c.CommitSourcePublicationStage(t.Context(), ref)
	if err != nil || replayed != result {
		t.Fatalf("lost-response replay = %+v, %v; want %+v", replayed, err, result)
	}
	forged := ref
	forged.Digest[0] ^= 0xff
	if _, err := c.CommitSourcePublicationStage(t.Context(), forged); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("forged settlement receipt = %v, want mutation conflict", err)
	}
	var publicationReceipts, settlementReceipts int
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT
  (SELECT COUNT(*) FROM source_publication_stage_receipts
   WHERE source_authority = ? AND stage_operation_id = ?),
  (SELECT COUNT(*) FROM source_observer_settlement_receipts
   WHERE source_authority = ? AND stage_operation_id = ?)`,
		string(ref.Authority), ref.Operation[:], string(ref.Authority), ref.Operation[:]).
		Scan(&publicationReceipts, &settlementReceipts); err != nil {
		t.Fatal(err)
	}
	if publicationReceipts != 0 || settlementReceipts != 1 {
		t.Fatalf("receipt classes publication=%d settlement=%d", publicationReceipts, settlementReceipts)
	}
}

func TestSourceObserverSettlementSurvivesCrashBeforeAndAfterCommit(t *testing.T) {
	database := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	ref := stageObserverSettlementOnlyForTest(t, c, causal.OperationID{0x52})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	want, err := c.CommitSourcePublicationStage(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	got, err := c.CommitSourcePublicationStage(t.Context(), ref)
	if err != nil || got != want {
		t.Fatalf("post-commit crash replay = %+v, %v; want %+v", got, err, want)
	}
}

func stageObserverSettlementOnlyForTest(
	t *testing.T,
	c *Catalog,
	operation causal.OperationID,
) SourcePublicationStageRef {
	t.Helper()
	fixture := stageDriverBackedObserverPublication(t, c, "settlement-only", 1, false)
	if _, err := c.CommitSourcePublicationStage(t.Context(), fixture.Ref); err != nil {
		t.Fatalf("commit baseline observer publication: %v", err)
	}
	predecessor, err := c.SourceWatermark(t.Context(), fixture.Ref.Authority)
	if err != nil {
		t.Fatalf("read settlement predecessor: %v", err)
	}
	identity := SourcePublicationStageIdentity{
		Authority: fixture.Identity.Authority, FleetOwner: fixture.Identity.FleetOwner,
		FleetGeneration: fixture.Identity.FleetGeneration, DriverID: fixture.Identity.DriverID,
		DeclarationDigest: fixture.Identity.DeclarationDigest, Operation: operation,
		Stream: fixture.Identity.Stream, RootEpoch: fixture.Identity.RootEpoch,
		Through: fixture.Identity.Through, Predecessor: predecessor,
	}
	if err := c.BeginSourcePublicationStage(t.Context(), identity); err != nil {
		t.Fatalf("begin settlement-only stage: %v", err)
	}
	ref, err := c.AppendSourcePublicationStage(t.Context(), identity, SourcePublicationStagePage{
		Index:    []SourcePhysicalIndexRecord{observerIndexRecord("settlement.json")},
		Complete: true,
	})
	if err != nil {
		t.Fatalf("append settlement-only stage: %v", err)
	}
	return ref
}
