package catalog

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceObserverSparseWatermarksInterleaveAndCompactContiguousFloor(t *testing.T) {
	for _, test := range []struct {
		name  string
		order []uint64
	}{
		{name: "forward", order: []uint64{1, 3}},
		{name: "reverse", order: []uint64{3, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newTestCatalog(t)
			authority := causal.SourceAuthorityID("sparse-interleave-" + test.name)
			configureSparseSourceObserverForTest(t, c, authority)
			appendSparseSourceObserverInboxForTest(t, c, authority)

			settleSparseSourceObserverForTest(t, c, authority, test.order[0])
			firstApplied := uint64(0)
			if test.order[0] == 1 {
				firstApplied = 1
			}
			assertSparseSourceObserverState(t, c, authority, 3, firstApplied, map[string]sparseCheckpointState{
				"a": {received: 2, applied: boolValue(test.order[0] == 1, 1), sequence: boolValue(test.order[0] == 1, 1)},
				"b": {received: 1, applied: boolValue(test.order[0] == 3, 1), sequence: boolValue(test.order[0] == 3, 3)},
			})

			settleSparseSourceObserverForTest(t, c, authority, test.order[1])
			assertSparseSourceObserverState(t, c, authority, 3, 1, map[string]sparseCheckpointState{
				"a": {received: 2, applied: 1, sequence: 1},
				"b": {received: 1, applied: 1, sequence: 3},
			})
			assertSparseSourceObserverInbox(t, c, authority, []uint64{2})
			next, err := c.SourceObserverNextInbox(t.Context(), authority, 1)
			if err != nil {
				t.Fatal(err)
			}
			if next == nil || next.Sequence != 2 || next.Stream != "a" || next.NativeCursor != 2 {
				t.Fatalf("next unapplied inbox = %+v, want stream a sequence 2", next)
			}
			next, err = c.SourceObserverNextInbox(t.Context(), authority, 2)
			if err != nil || next != nil {
				t.Fatalf("sparsely applied stream b was scheduled again: %+v, %v", next, err)
			}

			settleSparseSourceObserverForTest(t, c, authority, 2)
			assertSparseSourceObserverState(t, c, authority, 3, 3, map[string]sparseCheckpointState{
				"a": {received: 2, applied: 2, sequence: 2},
				"b": {received: 1, applied: 1, sequence: 3},
			})
			assertSparseSourceObserverInbox(t, c, authority, nil)
		})
	}
}

func TestSourceObserverSparseWatermarksReverseStreamInterleaving(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("sparse-reverse-stream-interleaving")
	configureSparseSourceObserverForTest(t, c, authority)
	appendSourceObserverRecordsForTest(t, c, authority, []SourceObserverInboxRecord{
		{Stream: "b", RootEpoch: "epoch-b", NativePredecessor: 0, NativeCursor: 1},
		{Stream: "a", RootEpoch: "epoch-a", NativePredecessor: 0, NativeCursor: 1},
		{Stream: "a", RootEpoch: "epoch-a", NativePredecessor: 1, NativeCursor: 2},
	})

	settleSparseSourceObserverForTest(t, c, authority, 2)
	assertSparseSourceObserverState(t, c, authority, 3, 0, map[string]sparseCheckpointState{
		"a": {received: 2, applied: 1, sequence: 2},
		"b": {received: 1},
	})
	assertSparseSourceObserverInbox(t, c, authority, []uint64{1, 3})
	settleSparseSourceObserverForTest(t, c, authority, 1)
	assertSparseSourceObserverState(t, c, authority, 3, 2, map[string]sparseCheckpointState{
		"a": {received: 2, applied: 1, sequence: 2},
		"b": {received: 1, applied: 1, sequence: 1},
	})
	assertSparseSourceObserverInbox(t, c, authority, []uint64{3})
	next, err := c.SourceObserverNextInbox(t.Context(), authority, 0)
	if err != nil || next == nil || next.Sequence != 3 || next.Stream != "a" {
		t.Fatalf("next reverse-interleaved inbox = %+v, %v", next, err)
	}
	settleSparseSourceObserverForTest(t, c, authority, 3)
	assertSparseSourceObserverInbox(t, c, authority, nil)
}

func TestSourceObserverSparseSettlementStatementsAreAtomicAcrossReopen(t *testing.T) {
	statementCount := sparseSourceObserverSettlementStatementCount(t)
	for ordinal := 1; ordinal <= statementCount; ordinal++ {
		t.Run(fmt.Sprintf("statement-%02d", ordinal), func(t *testing.T) {
			database := filepath.Join(t.TempDir(), "catalog.sqlite")
			c, err := Open(t.Context(), database)
			if err != nil {
				t.Fatal(err)
			}
			authority := causal.SourceAuthorityID(fmt.Sprintf("sparse-settlement-atomic-%02d", ordinal))
			configureSparseSourceObserverForTest(t, c, authority)
			appendSparseSourceObserverInboxForTest(t, c, authority)
			before := sourceObserverInboxRecordsForTest(t, c, authority)
			injected := errors.New("sparse settlement statement failpoint")
			seen := 0
			c.failpoint = func(point string) error {
				if point != sourceObserverSettlementStatementPoint {
					return nil
				}
				seen++
				if seen == ordinal {
					return injected
				}
				return nil
			}
			if err := c.SettleSourceObserver(t.Context(), sparseSourceObserverSettlement(authority, 3)); !errors.Is(err, injected) {
				t.Fatalf("statement %d settlement = %v, want injected", ordinal, err)
			}
			c.failpoint = nil
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}

			c, err = Open(t.Context(), database)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			assertSparseSourceObserverState(t, c, authority, 3, 0, map[string]sparseCheckpointState{
				"a": {received: 2},
				"b": {received: 1},
			})
			after := sourceObserverInboxRecordsForTest(t, c, authority)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("statement %d changed durable inbox after rollback:\n before=%+v\n after=%+v",
					ordinal, before, after)
			}
			settleSparseSourceObserverForTest(t, c, authority, 3)
			assertSparseSourceObserverState(t, c, authority, 3, 0, map[string]sparseCheckpointState{
				"a": {received: 2},
				"b": {received: 1, applied: 1, sequence: 3},
			})
			assertSparseSourceObserverInbox(t, c, authority, []uint64{1, 2})
		})
	}
}

func TestSourceObserverSparseWatermarksSurviveRestart(t *testing.T) {
	database := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	authority := causal.SourceAuthorityID("sparse-restart")
	configureSparseSourceObserverForTest(t, c, authority)
	appendSparseSourceObserverInboxForTest(t, c, authority)
	settleSparseSourceObserverForTest(t, c, authority, 1)
	settleSparseSourceObserverForTest(t, c, authority, 3)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	assertSparseSourceObserverState(t, c, authority, 3, 1, map[string]sparseCheckpointState{
		"a": {received: 2, applied: 1, sequence: 1},
		"b": {received: 1, applied: 1, sequence: 3},
	})
	next, err := c.SourceObserverNextInbox(t.Context(), authority, 1)
	if err != nil || next == nil || next.Sequence != 2 || next.Stream != "a" {
		t.Fatalf("next inbox after restart = %+v, %v", next, err)
	}
	settleSparseSourceObserverForTest(t, c, authority, 2)
	assertSparseSourceObserverInbox(t, c, authority, nil)
}

func TestSourceObserverSparseCompactionReplaysLostResponsesAcrossRestart(t *testing.T) {
	database := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	authority := causal.SourceAuthorityID("sparse-lost-response-replay")
	configureSparseSourceObserverForTest(t, c, authority)
	appendSparseSourceObserverInboxForTest(t, c, authority)
	settlement := sparseSourceObserverSettlement(authority, 3)
	if err := c.SettleSourceObserver(t.Context(), settlement); err != nil {
		t.Fatal(err)
	}
	assertSparseSourceObserverInbox(t, c, authority, []uint64{1, 2})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	payload := []byte("b:0:1")
	record := SourceObserverInboxRecord{
		Authority: authority, Stream: "b", RootEpoch: "epoch-b",
		NativePredecessor: 0, NativeCursor: 1, EventCount: 1,
		Digest: sha256.Sum256(payload), Payload: payload,
	}
	sequence, err := c.AppendSourceObserverInbox(t.Context(), record)
	if err != nil || sequence != 3 {
		t.Fatalf("compacted append replay = %d, %v; want 3", sequence, err)
	}
	if err := c.SettleSourceObserver(t.Context(), settlement); err != nil {
		t.Fatalf("compacted settlement replay: %v", err)
	}
	forgedSettlement := settlement
	forgedSettlement.Operation = causal.OperationID{0xff}
	if err := c.SettleSourceObserver(t.Context(), forgedSettlement); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("forged compacted settlement replay = %v, want conflict", err)
	}
	forgedRecord := record
	forgedRecord.Payload = []byte("forged")
	forgedRecord.Digest = sha256.Sum256(forgedRecord.Payload)
	if _, err := c.AppendSourceObserverInbox(t.Context(), forgedRecord); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("forged compacted append replay = %v, want conflict", err)
	}
	assertSparseSourceObserverState(t, c, authority, 3, 0, map[string]sparseCheckpointState{
		"a": {received: 2},
		"b": {received: 1, applied: 1, sequence: 3},
	})
	assertSparseSourceObserverInbox(t, c, authority, []uint64{1, 2})
}

func TestSourceObserverSparseWatermarksRetainMutationCorrelationInbox(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("sparse-correlation")
	configureSparseSourceObserverForTest(t, c, authority)
	appendSparseSourceObserverInboxForTest(t, c, authority)
	prepared := beginSourceExpectationMutation(t, c, authority, "sparse-correlation")
	payload := []byte("retain-correlation-window")
	if err := c.PutSourceMutationExpectation(t.Context(), SourceMutationExpectationRecord{
		Operation: prepared.OperationID, Authority: authority, Tenant: prepared.Tenant, Generation: 1,
		Origin: prepared.Intent.Origin, Digest: sha256.Sum256(payload), Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}

	for _, sequence := range []uint64{3, 1, 2} {
		settleSparseSourceObserverForTest(t, c, authority, sequence)
	}
	assertSparseSourceObserverState(t, c, authority, 3, 3, map[string]sparseCheckpointState{
		"a": {received: 2, applied: 2, sequence: 2},
		"b": {received: 1, applied: 1, sequence: 3},
	})
	assertSparseSourceObserverInbox(t, c, authority, []uint64{1, 2, 3})
	next, err := c.SourceObserverNextInbox(t.Context(), authority, 0)
	if err != nil || next != nil {
		t.Fatalf("retained correlation row became runnable: %+v, %v", next, err)
	}
}

func TestSourceSnapshotPromotionAdvancesEverySparseWatermark(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("sparse-snapshot")
	provision := configureSparseSourceObserverForTest(t, c, authority)
	appendSparseSourceObserverInboxForTest(t, c, authority)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "sparse-snapshot"); err != nil {
		t.Fatal(err)
	}
	ref := stageSparseObserverSnapshotForTest(t, c, authority, "sparse-snapshot", provision)
	if _, err := c.PromoteSourceSnapshot(t.Context(), ref, sourceSnapshotSettlementForTest(ref, 3)); err != nil {
		t.Fatal(err)
	}
	assertSparseSourceObserverState(t, c, authority, 3, 3, map[string]sparseCheckpointState{
		"a": {received: 2, applied: 2, sequence: 3},
		"b": {received: 1, applied: 1, sequence: 3},
	})
	assertSparseSourceObserverInbox(t, c, authority, nil)
}

func TestSourceSnapshotPromotionFailpointRollsBackEverySparseWatermark(t *testing.T) {
	database := filepath.Join(t.TempDir(), "catalog.sqlite")
	boom := errors.New("setwise promotion failpoint")
	fail := false
	c, err := open(t.Context(), database, func(point string) error {
		if point == sourceSnapshotAfterSetwisePromotion && fail {
			fail = false
			return boom
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	authority := causal.SourceAuthorityID("sparse-snapshot-rollback")
	provision := configureSparseSourceObserverForTest(t, c, authority)
	appendSparseSourceObserverInboxForTest(t, c, authority)
	fail = true
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "sparse-snapshot-rollback"); err != nil {
		t.Fatal(err)
	}
	ref := stageSparseObserverSnapshotForTest(t, c, authority, "sparse-snapshot-rollback", provision)
	settlement := sourceSnapshotSettlementForTest(ref, 3)
	if _, err := c.PromoteSourceSnapshot(t.Context(), ref, settlement); !errors.Is(err, boom) {
		t.Fatalf("PromoteSourceSnapshot failpoint = %v, want %v", err, boom)
	}
	assertSparseSourceObserverState(t, c, authority, 3, 0, map[string]sparseCheckpointState{
		"a": {received: 2},
		"b": {received: 1},
	})
	assertSparseSourceObserverInbox(t, c, authority, []uint64{1, 2, 3})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	c, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	assertSparseSourceObserverState(t, c, authority, 3, 0, map[string]sparseCheckpointState{
		"a": {received: 2},
		"b": {received: 1},
	})
	if _, err := c.PromoteSourceSnapshot(t.Context(), ref, settlement); err != nil {
		t.Fatalf("PromoteSourceSnapshot after restart: %v", err)
	}
	assertSparseSourceObserverState(t, c, authority, 3, 3, map[string]sparseCheckpointState{
		"a": {received: 2, applied: 2, sequence: 3},
		"b": {received: 1, applied: 1, sequence: 3},
	})
	assertSparseSourceObserverInbox(t, c, authority, nil)
}

type sparseCheckpointState struct {
	received uint64
	applied  uint64
	sequence uint64
}

func configureSparseSourceObserverForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
) TenantProvision {
	t.Helper()
	roots := []SourceObserverRootRecord{
		{ID: "root-a", Generation: 1, Path: "/source/a", VolumeUUID: "volume", Inode: 1, Kind: 1},
		{ID: "root-b", Generation: 1, Path: "/source/b", VolumeUUID: "volume", Inode: 2, Kind: 1},
	}
	checkpoints := []SourceObserverCheckpointRecord{
		{Stream: "a", RootEpoch: "epoch-a"},
		{Stream: "b", RootEpoch: "epoch-b"},
	}
	identity := sourceObserverConfigurationIdentityForTest(
		t, authority, causal.OperationID{0x61}, "stream", "epoch", roots, checkpoints,
	)
	stageSourceObserverConfigurationForTest(t, c, identity, roots, checkpoints)
	bootstrap := "bootstrap-" + string(authority)
	provisionSpec := testTenantProvision(t, bootstrap, 1)
	provisionSpec.ContentSourceID = string(authority)
	provision, err := provisionTenantForTest(t, c, t.Context(), provisionSpec)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, bootstrap); err != nil {
		t.Fatal(err)
	}
	ref := stageSparseObserverSnapshotForTest(t, c, authority, bootstrap, provision)
	if _, err := c.PromoteSourceSnapshot(t.Context(), ref, sourceSnapshotSettlementForTest(ref, 0)); err != nil {
		t.Fatalf("bootstrap source snapshot: %v", err)
	}
	return provision
}

func appendSparseSourceObserverInboxForTest(t *testing.T, c *Catalog, authority causal.SourceAuthorityID) {
	t.Helper()
	appendSourceObserverRecordsForTest(t, c, authority, []SourceObserverInboxRecord{
		{Stream: "a", RootEpoch: "epoch-a", NativePredecessor: 0, NativeCursor: 1},
		{Stream: "a", RootEpoch: "epoch-a", NativePredecessor: 1, NativeCursor: 2},
		{Stream: "b", RootEpoch: "epoch-b", NativePredecessor: 0, NativeCursor: 1},
	})
}

func appendSourceObserverRecordsForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	records []SourceObserverInboxRecord,
) {
	t.Helper()
	for index := range records {
		record := &records[index]
		record.Authority = authority
		record.EventCount = 1
		record.Payload = []byte(fmt.Sprintf("%s:%d:%d", record.Stream, record.NativePredecessor, record.NativeCursor))
		record.Digest = sha256.Sum256(record.Payload)
		sequence, err := c.AppendSourceObserverInbox(t.Context(), *record)
		if err != nil {
			t.Fatalf("AppendSourceObserverInbox(%s:%d): %v", record.Stream, record.NativeCursor, err)
		}
		if want := uint64(index + 1); sequence != want {
			t.Fatalf("AppendSourceObserverInbox(%s:%d) sequence = %d, want %d",
				record.Stream, record.NativeCursor, sequence, want)
		}
	}
}

func settleSparseSourceObserverForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	through uint64,
) {
	t.Helper()
	if err := c.SettleSourceObserver(t.Context(), sparseSourceObserverSettlement(authority, through)); err != nil {
		t.Fatalf("SettleSourceObserver(%d): %v", through, err)
	}
}

func sparseSourceObserverSettlement(
	authority causal.SourceAuthorityID,
	through uint64,
) SourceObserverSettlement {
	return SourceObserverSettlement{
		Authority: authority, Stream: "stream", RootEpoch: "epoch", Through: through,
		Operation: causal.OperationID{byte(0x70 + through)},
	}
}

func assertSparseSourceObserverState(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	wantReceived uint64,
	wantApplied uint64,
	want map[string]sparseCheckpointState,
) {
	t.Helper()
	stream, err := c.SourceObserverStream(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	if stream.LastReceived != wantReceived || stream.LastApplied != wantApplied {
		t.Fatalf("observer stream = received %d applied %d, want %d/%d",
			stream.LastReceived, stream.LastApplied, wantReceived, wantApplied)
	}
	configured, err := c.SourceObserverCheckpointsPage(
		t.Context(), authority, "", SourceObserverConfigurationPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(configured.Records) != len(want) {
		t.Fatalf("configured checkpoint count = %d, want %d: %+v",
			len(configured.Records), len(want), configured.Records)
	}
	for _, checkpoint := range configured.Records {
		state, ok := want[checkpoint.Stream]
		if !ok || checkpoint.EventID != state.received {
			t.Fatalf("received checkpoint = %+v, want %+v", checkpoint, state)
		}
	}
	applied, err := c.SourceObserverAppliedCheckpointsPage(
		t.Context(), authority, "", SourceObserverConfigurationPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if applied.LastReceived != wantReceived || len(applied.Records) != len(want) {
		t.Fatalf("applied checkpoint page = received %d records %+v, want %d/%d",
			applied.LastReceived, applied.Records, wantReceived, len(want))
	}
	for _, checkpoint := range applied.Records {
		state, ok := want[checkpoint.Stream]
		if !ok {
			t.Fatalf("unexpected checkpoint %+v", checkpoint)
		}
		if checkpoint.ReceivedEventID != state.received || checkpoint.EventID != state.applied ||
			checkpoint.Sequence != state.sequence {
			t.Fatalf("checkpoint %s = received %d applied %d sequence %d, want %+v",
				checkpoint.Stream, checkpoint.ReceivedEventID, checkpoint.EventID,
				checkpoint.Sequence, state)
		}
	}
}

func assertSparseSourceObserverInbox(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	want []uint64,
) {
	t.Helper()
	stream, err := c.SourceObserverStream(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.SourceObserverInboxPage(
		t.Context(), authority, 0, stream.LastReceived, SourceObserverInboxPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != len(want) {
		t.Fatalf("inbox sequences = %+v, want %v", page.Records, want)
	}
	for index, record := range page.Records {
		if record.Sequence != want[index] {
			t.Fatalf("inbox[%d] sequence = %d, want %d", index, record.Sequence, want[index])
		}
	}
}

func sourceObserverInboxRecordsForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
) []SourceObserverInboxRecord {
	t.Helper()
	stream, err := c.SourceObserverStream(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.SourceObserverInboxPage(
		t.Context(), authority, 0, stream.LastReceived, SourceObserverInboxPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	return page.Records
}

func sparseSourceObserverSettlementStatementCount(t *testing.T) int {
	t.Helper()
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("sparse-settlement-statement-count")
	configureSparseSourceObserverForTest(t, c, authority)
	appendSparseSourceObserverInboxForTest(t, c, authority)
	statements := 0
	c.failpoint = func(point string) error {
		if point == sourceObserverSettlementStatementPoint {
			statements++
		}
		return nil
	}
	settleSparseSourceObserverForTest(t, c, authority, 3)
	c.failpoint = nil
	if statements == 0 {
		t.Fatal("sparse settlement executed no failpoint-fenced statements")
	}
	return statements
}

func stageSparseObserverSnapshotForTest(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	snapshot string,
	provision TenantProvision,
) SourceSnapshotStageRef {
	t.Helper()
	change := sourceSnapshotChangeAtDriverHeadForTest(t, c, authority)
	identity := SourceSnapshotIdentity{
		Authority: authority, AuthorityGeneration: 1, Snapshot: snapshot,
		FenceDigest: sourceSnapshotFenceDigestForTest(t, c, authority), Change: change,
	}
	if err := c.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := c.AppendSourceSnapshotPublication(t.Context(), identity, SourceSnapshotPublicationPage{
		AffectedKeys: []causal.LogicalKey{"repair"},
		Roots: []SourceSnapshotRoot{{
			Tenant: provision.Tenant, Generation: provision.Generation, LogicalID: "root", RootKey: "root-key",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func boolValue(condition bool, value uint64) uint64 {
	if condition {
		return value
	}
	return 0
}
