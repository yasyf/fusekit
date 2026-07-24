package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceObserverInboxQuotaCoalescesToSnapshotWithoutPayloadGrowth(t *testing.T) {
	tests := []struct {
		name       string
		rows       int
		payloadLen int
		eventCount uint64
	}{
		{name: "rows", rows: sourceObserverInboxMaxRows, payloadLen: 1, eventCount: 1},
		{name: "bytes", rows: 1, payloadLen: sourceObserverInboxMaxBytes, eventCount: 1},
		{name: "events", rows: 1, payloadLen: 1, eventCount: sourceObserverInboxMaxEvents},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := newTestCatalog(t)
			authority := causal.SourceAuthorityID("inbox-quota-" + test.name)
			configureSourceObserverForIndexTest(t, c, authority)
			seedSourceObserverInbox(t, c, authority, test.rows, test.payloadLen, test.eventCount)
			payload := []byte("overflow")
			if _, err := c.AppendSourceObserverInbox(t.Context(), SourceObserverInboxRecord{
				Authority: authority, Stream: "stream", RootEpoch: "epoch",
				NativePredecessor: uint64(test.rows), NativeCursor: uint64(test.rows + 1), EventCount: 1,
				Digest: sha256.Sum256(payload), Payload: payload,
			}); !errors.Is(err, ErrSourceObserverSnapshotRequired) {
				t.Fatalf("quota append = %v, want snapshot required", err)
			}
			assertSourceObserverOverflowState(t, c, authority, uint64(test.rows+1))
			older := []byte("older-replay")
			if _, err := c.AppendSourceObserverInbox(t.Context(), SourceObserverInboxRecord{
				Authority: authority, Stream: "stream", RootEpoch: "epoch",
				NativePredecessor: uint64(test.rows - 1), NativeCursor: uint64(test.rows), EventCount: 1,
				Digest: sha256.Sum256(older), Payload: older,
			}); !errors.Is(err, ErrSourceObserverInboxCoalesced) {
				t.Fatalf("older overflow replay = %v, want durable coalescing", err)
			}
			newer := []byte("newer-coalesced")
			if _, err := c.AppendSourceObserverInbox(t.Context(), SourceObserverInboxRecord{
				Authority: authority, Stream: "stream", RootEpoch: "epoch",
				NativePredecessor: uint64(test.rows + 1), NativeCursor: uint64(test.rows + 2), EventCount: 1,
				Digest: sha256.Sum256(newer), Payload: newer,
			}); !errors.Is(err, ErrSourceObserverInboxCoalesced) {
				t.Fatalf("newer snapshot-required append = %v, want durable coalescing", err)
			}
			assertSourceObserverOverflowState(t, c, authority, uint64(test.rows+2))
		})
	}
}

func TestSourceObserverInboxPageIsBoundedAndContinuous(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("inbox-page")
	configureSourceObserverForIndexTest(t, c, authority)
	seedSourceObserverInbox(t, c, authority, SourceObserverInboxPageLimit+6, 1, 1)

	first, err := c.SourceObserverInboxPage(t.Context(), authority, 0, SourceObserverInboxPageLimit+6, SourceObserverInboxPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != SourceObserverInboxPageLimit || first.Next != SourceObserverInboxPageLimit {
		t.Fatalf("first page = %d records next %d", len(first.Records), first.Next)
	}
	second, err := c.SourceObserverInboxPage(
		t.Context(), authority, first.Next, SourceObserverInboxPageLimit+6, SourceObserverInboxPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 6 || second.Next != 0 ||
		second.Records[0].Sequence != SourceObserverInboxPageLimit+1 {
		t.Fatalf("second page = %+v", second)
	}
	if _, err := c.SourceObserverInboxPage(
		t.Context(), authority, 0, SourceObserverInboxPageLimit+6, SourceObserverInboxPageLimit+1,
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("oversized inbox page = %v, want invalid object", err)
	}
}

func TestSourceMutationCorrelationWindowHasIndependentBound(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("mutation-window-quota")
	configureSourceObserverForIndexTest(t, c, authority)
	prepared := beginSourceExpectationMutation(t, c, authority, "mutation-window-quota")
	operation := prepared.OperationID
	payload := []byte("expectation")
	if err := reserveSourceMutationExpectationForTest(t, c, SourceMutationExpectationRecord{
		Operation: operation, Authority: authority, Tenant: prepared.Tenant, Generation: 1,
		Origin: prepared.Intent.Origin, Digest: sha256.Sum256(payload), Payload: payload,
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.CompleteSourceMutationExpectation(t.Context(), authority, operation, []byte("receipt")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(t.Context(), `UPDATE source_mutation_expectations SET state = 3 WHERE operation_id = ?`, operation[:]); err != nil {
		t.Fatal(err)
	}
	seedSourceObserverInbox(t, c, authority, sourceMutationWindowMaxRows, 1, 1)
	overflow := []byte("overflow")
	if _, err := c.AppendSourceObserverInbox(t.Context(), SourceObserverInboxRecord{
		Authority: authority, Stream: "stream", RootEpoch: "epoch",
		NativePredecessor: sourceMutationWindowMaxRows, NativeCursor: sourceMutationWindowMaxRows + 1,
		EventCount: 1, Digest: sha256.Sum256(overflow), Payload: overflow,
	}); !errors.Is(err, ErrSourceObserverSnapshotRequired) {
		t.Fatalf("correlation window append = %v, want snapshot required", err)
	}
	assertSourceObserverOverflowState(t, c, authority, sourceMutationWindowMaxRows+1)
	expectation, err := c.SourceMutationExpectation(t.Context(), authority, operation)
	if err != nil || expectation.Operation != operation ||
		expectation.State != SourceMutationExpectationRepairRequired {
		t.Fatalf("overflow lost durable mutation expectation: %+v, %v", expectation, err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "repair"); err != nil {
		t.Fatal(err)
	}
	promoteObserverSnapshotForTest(t, c, authority, "repair", 0, true)
	expectation, err = c.SourceMutationExpectation(t.Context(), authority, operation)
	if err != nil || expectation.State != SourceMutationExpectationRepairPublished {
		t.Fatalf("snapshot settlement lost cleanup proof: %+v, %v", expectation, err)
	}
	if err := c.CompleteSourceMutationRepair(t.Context(), authority, operation); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SourceMutationExpectation(t.Context(), authority, operation); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed repair retained expectation: %v", err)
	}
}

func TestSnapshotRequiredCoalescingRejectsForeignStreamAndRootEpoch(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("snapshot-coalesce-fence")
	configureSourceObserverForIndexTest(t, c, authority)
	if err := c.RequireSourceObserverSnapshot(t.Context(), authority); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name, stream, epoch string
	}{
		{name: "stream", stream: "foreign", epoch: "epoch"},
		{name: "epoch", stream: "stream", epoch: "foreign"},
	} {
		t.Run(test.name, func(t *testing.T) {
			payload := []byte(test.name)
			if _, err := c.AppendSourceObserverInbox(t.Context(), SourceObserverInboxRecord{
				Authority: authority, Stream: test.stream, RootEpoch: test.epoch,
				NativePredecessor: 0, NativeCursor: 1, EventCount: 1,
				Digest: sha256.Sum256(payload), Payload: payload,
			}); !errors.Is(err, ErrSourceObserverConflict) {
				t.Fatalf("foreign %s append = %v, want conflict", test.name, err)
			}
			assertSourceObserverOverflowState(t, c, authority, 0)
		})
	}
}

func TestSourceObserverOverflowRequiresExpectationRepairWithoutChangingMutationTerminal(t *testing.T) {
	for _, complete := range []bool{false, true} {
		name := "planned"
		if complete {
			name = "receipt-complete"
		}
		t.Run(name, func(t *testing.T) {
			c := newTestCatalog(t)
			provision := provisionSourceMutationTenant(t, c, "overflow-"+name)
			configureSourceObserverForIndexTest(t, c, "test-source")
			root := mustSourceObject(t, c, provision.Tenant, provision.Root)
			prepared := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
				SourceID: "test-source", Origin: testCausalOrigin(), Disposition: MutationDispositionNamespace, Create: &CreateMutation{Spec: CreateSpec{
					Parent: root.ID, Name: "created", Kind: KindDirectory, Mode: 0o700,
					Visibility: Visibility{Mount: true, FileProvider: true},
				}},
			})
			expectation := []byte("expectation-" + name)
			if err := reserveSourceMutationExpectationForTest(t, c, SourceMutationExpectationRecord{
				Operation: prepared.OperationID, Authority: "test-source", Tenant: provision.Tenant, Generation: provision.Generation,
				Origin: testCausalOrigin(), Digest: sha256.Sum256(expectation), Payload: expectation,
			}); err != nil {
				t.Fatal(err)
			}
			if complete {
				if err := c.CompleteSourceMutationExpectation(t.Context(), "test-source", prepared.OperationID, []byte("receipt")); err != nil {
					t.Fatal(err)
				}
			}
			seedSourceObserverInbox(t, c, "test-source", 1, 1, sourceObserverInboxMaxEvents)
			overflow := []byte("overflow")
			if _, err := c.AppendSourceObserverInbox(t.Context(), SourceObserverInboxRecord{
				Authority: "test-source", Stream: "stream", RootEpoch: "epoch", NativePredecessor: 1, NativeCursor: 2,
				EventCount: 1, Digest: sha256.Sum256(overflow), Payload: overflow,
			}); !errors.Is(err, ErrSourceObserverInboxCoalesced) {
				t.Fatalf("overflow = %v, want coalesced", err)
			}
			prepared, err := c.PreparedMutation(t.Context(), provision.Tenant, prepared.OperationID)
			if err != nil || prepared.State != MutationApplying {
				t.Fatalf("prepared mutation after overflow = %+v, %v", prepared, err)
			}
			expectationRecord, err := c.SourceMutationExpectation(t.Context(), "test-source", prepared.OperationID)
			if err != nil || expectationRecord.State != SourceMutationExpectationRepairRequired {
				t.Fatalf("source expectation after overflow = %+v, %v", expectationRecord, err)
			}
		})
	}
}

func seedSourceObserverInbox(t *testing.T, c *Catalog, authority causal.SourceAuthorityID, rows, payloadLen int, eventCount uint64) {
	t.Helper()
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	payload := bytes.Repeat([]byte{'x'}, payloadLen)
	digest := sha256.Sum256(payload)
	for index := range rows {
		count := uint64(1)
		if rows == 1 {
			count = eventCount
		}
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_observer_inbox(
    source_authority, sequence, predecessor_sequence, stream_identity,
    predecessor_event, through_event, root_epoch, event_count, payload_digest, payload
) VALUES (?, ?, ?, 'stream', ?, ?, 'epoch', ?, ?, ?)`, string(authority), index+1, index, index, index+1, count, digest[:], payload); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := tx.ExecContext(t.Context(), `
UPDATE source_observer_streams SET last_received_sequence = ? WHERE source_authority = ?`, rows, string(authority)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
UPDATE source_observer_checkpoints SET native_event_id = ? WHERE source_authority = ? AND stream_identity = 'stream'`,
		rows, string(authority)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func assertSourceObserverOverflowState(t *testing.T, c *Catalog, authority causal.SourceAuthorityID, checkpoint uint64) {
	t.Helper()
	stream, err := c.SourceObserverStream(t.Context(), authority)
	if err != nil {
		t.Fatal(err)
	}
	checkpoints, err := c.SourceObserverCheckpointsPage(
		t.Context(), authority, "", SourceObserverConfigurationPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	inbox, err := c.SourceObserverInboxPage(
		t.Context(), authority, 0, stream.LastReceived, SourceObserverInboxPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if stream.Mode != SourceObserverSnapshotRequired || stream.LastReceived != stream.LastApplied ||
		len(inbox.Records) != 0 || len(checkpoints.Records) != 1 ||
		checkpoints.Records[0].EventID != checkpoint {
		t.Fatalf("overflow stream=%+v checkpoints=%+v inbox=%+v checkpoint=%d",
			stream, checkpoints, inbox, checkpoint)
	}
}

func TestSourceObserverBindingLookupDoesNotDecodeUnrelatedPhysicalRows(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("indexed-authority")
	configureSourceObserverForIndexTest(t, c, authority)
	total := testScaleCount(10_000)
	zero := [32]byte{}
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for index := range total {
		logical := fmt.Sprintf("logical-%05d", index)
		relative := fmt.Sprintf("object-%05d", index)
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_physical_index(
    source_authority, root_id, relative_path, file_identity, physical_kind,
    metadata_fingerprint, content_fingerprint, payload
) VALUES (?, 'root', ?, ?, 1, ?, ?, X'01')`, string(authority), relative, []byte(relative), zero[:], zero[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_physical_logical(source_authority, logical_id, root_id, relative_path)
VALUES (?, ?, 'root', ?)`, string(authority), logical, relative); err != nil {
			t.Fatal(err)
		}
	}
	targetLogical := fmt.Sprintf("logical-%05d", total-1)
	targetKey := SourceObjectKey("opaque-target-key")
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_authority_bindings(source_authority, logical_id, source_key, effective_fingerprint)
VALUES (?, ?, ?, ?)`, string(authority), targetLogical, string(targetKey), zero[:]); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	binding, err := c.SourceObserverBindingForKey(t.Context(), authority, targetKey)
	if err != nil {
		t.Fatalf("SourceObserverBindingForKey: %v", err)
	}
	page, err := c.SourceObserverBindingIndexPage(
		t.Context(), authority, targetKey, SourceIndexLocator{}, SourcePhysicalIndexPageLimit,
	)
	if err != nil {
		t.Fatalf("SourceObserverBindingIndexPage: %v", err)
	}
	if binding.LogicalID != targetLogical || len(page.Records) != 1 ||
		page.Records[0].Relative != fmt.Sprintf("object-%05d", total-1) {
		t.Fatalf("lookup = binding %+v physical %+v", binding, page)
	}
}

func TestSourceObserverBindingLookupReturnsEveryPhysicalInputDeterministically(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("multi-input-authority")
	configureSourceObserverForIndexTest(t, c, authority)
	zero := [32]byte{}
	key := SourceObjectKey("multi-input-key")
	if _, err := c.ReserveSourceAuthorityBinding(t.Context(), authority, "logical", key); err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{"b", "a"} {
		if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_physical_index(
    source_authority, root_id, relative_path, file_identity, physical_kind,
    metadata_fingerprint, content_fingerprint, payload
) VALUES (?, 'root', ?, ?, 1, ?, ?, X'01')`, string(authority), relative, []byte(relative), zero[:], zero[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_physical_logical(source_authority, logical_id, root_id, relative_path)
VALUES (?, 'logical', 'root', ?)`, string(authority), relative); err != nil {
			t.Fatal(err)
		}
	}
	binding, err := c.SourceObserverBindingForKey(t.Context(), authority, key)
	if err != nil {
		t.Fatal(err)
	}
	if binding.LogicalID != "logical" {
		t.Fatalf("binding = %+v", binding)
	}
	page, err := c.SourceObserverBindingIndexPage(t.Context(), authority, key, SourceIndexLocator{}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 2 || page.Records[0].Relative != "a" || page.Records[1].Relative != "b" {
		t.Fatalf("physical inputs = %+v", page)
	}
}

func TestSourceSnapshotStagePageEnforcesExactBoundsOrderAndCursor(t *testing.T) {
	authority := causal.SourceAuthorityID("snapshot-page-bounds")
	record := func(relative string, payload []byte) SourcePhysicalIndexRecord {
		return SourcePhysicalIndexRecord{
			Authority: authority, RootID: "root", Relative: relative,
			FileIdentity: []byte(relative), Kind: 1, Payload: payload,
		}
	}
	begin := func(t *testing.T, snapshot string) *Catalog {
		t.Helper()
		c := newTestCatalog(t)
		configureSourceObserverForIndexTest(t, c, authority)
		if err := c.BeginSourceSnapshotStage(t.Context(), authority, snapshot); err != nil {
			t.Fatal(err)
		}
		return c
	}

	t.Run("exact item limit", func(t *testing.T) {
		c := begin(t, "exact-items")
		records := make([]SourcePhysicalIndexRecord, sourceSnapshotPhysicalPageLimit)
		for index := range records {
			relative := fmt.Sprintf("%03d", index)
			records[index] = record(relative, []byte{1})
		}
		page := SourceSnapshotPage{
			Records: records,
			Next:    SourceIndexLocator{RootID: "root", Relative: records[len(records)-1].Relative},
		}
		if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "exact-items", page); err != nil {
			t.Fatalf("exact source snapshot page: %v", err)
		}
		stored, err := c.SourceSnapshotStagePage(
			t.Context(), authority, "exact-items", SourceIndexLocator{}, sourceSnapshotPhysicalPageLimit,
		)
		if err != nil || len(stored.Records) != sourceSnapshotPhysicalPageLimit ||
			stored.Next != (SourceIndexLocator{}) {
			t.Fatalf("stored exact source snapshot page = %+v, %v", stored, err)
		}
	})

	t.Run("over item limit", func(t *testing.T) {
		c := begin(t, "over-items")
		records := make([]SourcePhysicalIndexRecord, sourceSnapshotPhysicalPageLimit+1)
		for index := range records {
			relative := fmt.Sprintf("%03d", index)
			records[index] = record(relative, []byte{1})
		}
		page := SourceSnapshotPage{
			Records: records,
			Next:    SourceIndexLocator{RootID: "root", Relative: records[len(records)-1].Relative},
		}
		if err := c.AppendSourceSnapshotStagePage(
			t.Context(), authority, "over-items", page,
		); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("over-limit source snapshot page = %v, want ErrInvalidObject", err)
		}
	})

	t.Run("exact encoded byte limit", func(t *testing.T) {
		c := begin(t, "exact-bytes")
		low, high := 1, sourceSnapshotPhysicalPageBytes
		var page SourceSnapshotPage
		for low < high {
			middle := low + (high-low+1)/2
			candidate := record("file", make([]byte, middle))
			page = SourceSnapshotPage{
				Records: []SourcePhysicalIndexRecord{candidate},
				Next:    SourceIndexLocator{RootID: "root", Relative: "file"},
			}
			encoded, err := json.Marshal(page)
			if err != nil {
				t.Fatal(err)
			}
			if len(encoded) <= sourceSnapshotPhysicalPageBytes {
				low = middle
			} else {
				high = middle - 1
			}
		}
		page = SourceSnapshotPage{
			Records: []SourcePhysicalIndexRecord{record("file", make([]byte, low))},
			Next:    SourceIndexLocator{RootID: "root", Relative: "file"},
		}
		encoded, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		padding := sourceSnapshotPhysicalPageBytes - len(encoded)
		page.Records[0].Logical = []string{strings.Repeat("x", padding)}
		encoded, err = json.Marshal(page)
		if err != nil || len(encoded) != sourceSnapshotPhysicalPageBytes {
			t.Fatalf("exact encoded source snapshot bytes = %d, %v", len(encoded), err)
		}
		if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "exact-bytes", page); err != nil {
			t.Fatalf("exact encoded source snapshot page: %v", err)
		}

		over := begin(t, "over-bytes")
		page.Records[0].Logical[0] += "x"
		if err := over.AppendSourceSnapshotStagePage(
			t.Context(), authority, "over-bytes", page,
		); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("over encoded source snapshot page = %v, want ErrInvalidObject", err)
		}
	})

	t.Run("order and cursor", func(t *testing.T) {
		c := begin(t, "ordering")
		unordered := SourceSnapshotPage{
			Records: []SourcePhysicalIndexRecord{
				record("b", []byte{1}),
				record("a", []byte{1}),
			},
			Next: SourceIndexLocator{RootID: "root", Relative: "a"},
		}
		if err := c.AppendSourceSnapshotStagePage(
			t.Context(), authority, "ordering", unordered,
		); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("unordered source snapshot page = %v, want ErrInvalidObject", err)
		}
		wrongCursor := SourceSnapshotPage{
			Records: []SourcePhysicalIndexRecord{record("a", []byte{1})},
			Next:    SourceIndexLocator{RootID: "root", Relative: "b"},
		}
		if err := c.AppendSourceSnapshotStagePage(
			t.Context(), authority, "ordering", wrongCursor,
		); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("mismatched source snapshot cursor = %v, want ErrInvalidObject", err)
		}
		first := SourceSnapshotPage{
			Records: []SourcePhysicalIndexRecord{record("b", []byte{1})},
			Next:    SourceIndexLocator{RootID: "root", Relative: "b"},
		}
		if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "ordering", first); err != nil {
			t.Fatal(err)
		}
		stale := SourceSnapshotPage{
			Records: []SourcePhysicalIndexRecord{record("a", []byte{1})},
			Next:    SourceIndexLocator{RootID: "root", Relative: "a"},
		}
		if err := c.AppendSourceSnapshotStagePage(
			t.Context(), authority, "ordering", stale,
		); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("stale source snapshot cursor = %v, want ErrInvalidTransition", err)
		}
	})
}

func TestSourceMutationExpectationsPageHasExactLimitAndCursor(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("expectation-page")
	configureSourceObserverForIndexTest(t, c, authority)
	const total = SourceMutationExpectationPageLimit + 1
	for index := range total {
		var operation MutationID
		binary.BigEndian.PutUint64(operation[len(operation)-8:], uint64(index+1))
		payload := []byte(fmt.Sprintf("payload-%03d", index))
		if err := insertSourceMutationExpectationForTest(t, c, SourceMutationExpectationRecord{
			Operation: operation, Authority: authority, Tenant: "expectation-page", Generation: 1,
			Origin: CausalOrigin{
				Cause: causal.CauseProviderMutation, Domain: "domain-expectation-page", Generation: 1,
			},
			Digest: sha256.Sum256(payload), Payload: payload,
			State: SourceMutationExpectationPlanned,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first, err := c.SourceMutationExpectationsPage(
		t.Context(), authority, MutationID{}, SourceMutationExpectationPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != SourceMutationExpectationPageLimit ||
		first.Next != first.Records[len(first.Records)-1].Operation {
		t.Fatalf("first source mutation expectation page = %+v", first)
	}
	for index := 1; index < len(first.Records); index++ {
		if bytes.Compare(first.Records[index-1].Operation[:], first.Records[index].Operation[:]) >= 0 {
			t.Fatalf("source mutation expectation page is unordered at %d", index)
		}
	}
	second, err := c.SourceMutationExpectationsPage(
		t.Context(), authority, first.Next, SourceMutationExpectationPageLimit,
	)
	if err != nil || len(second.Records) != 1 || second.Next != (MutationID{}) {
		t.Fatalf("second source mutation expectation page = %+v, %v", second, err)
	}
	if _, err := c.SourceMutationExpectationsPage(
		t.Context(), authority, MutationID{}, SourceMutationExpectationPageLimit+1,
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("over-limit source mutation expectation page = %v, want ErrInvalidObject", err)
	}
}

func TestSourcePhysicalIndexRecordsPageHasExactLimitAndCursor(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("physical-page")
	configureSourceObserverForIndexTest(t, c, authority)
	zero := [32]byte{}
	const total = SourcePhysicalIndexPageLimit + 1
	for index := range total {
		relative := fmt.Sprintf("%03d", index)
		if _, err := c.db.ExecContext(t.Context(), `
INSERT INTO source_physical_index(
    source_authority, root_id, relative_path, file_identity, physical_kind,
    metadata_fingerprint, content_fingerprint, payload
) VALUES (?, 'root', ?, ?, 1, ?, ?, X'01')`,
			string(authority), relative, []byte(relative), zero[:], zero[:]); err != nil {
			t.Fatal(err)
		}
	}
	first, err := c.SourcePhysicalIndexRecordsPage(
		t.Context(), authority, SourceIndexLocator{}, SourcePhysicalIndexPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != SourcePhysicalIndexPageLimit ||
		first.Next != (SourceIndexLocator{RootID: "root", Relative: "255"}) {
		t.Fatalf("first source physical index page = %+v", first)
	}
	second, err := c.SourcePhysicalIndexRecordsPage(
		t.Context(), authority, first.Next, SourcePhysicalIndexPageLimit,
	)
	if err != nil || len(second.Records) != 1 || second.Records[0].Relative != "256" ||
		second.Next != (SourceIndexLocator{}) {
		t.Fatalf("second source physical index page = %+v, %v", second, err)
	}
	if _, err := c.SourcePhysicalIndexRecordsPage(
		t.Context(), authority, SourceIndexLocator{}, SourcePhysicalIndexPageLimit+1,
	); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("over-limit source physical index page = %v, want ErrInvalidObject", err)
	}
}

func TestSourceSnapshotSettlementDeletesHighVolumeArmedMutationState(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("expectation-authority")
	configureSourceObserverForIndexTest(t, c, authority)
	const total = 1_000
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	insert, err := tx.PrepareContext(t.Context(), `
INSERT INTO source_mutation_expectations(
    operation_id, source_authority, tenant, generation, causal_origin,
    payload_digest, payload, receipt_digest, receipt, state
) VALUES (?, ?, 'expectation-fixture', 1, X'01', ?, ?, X'', X'01', ?)`)
	if err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	for index := range total {
		operation := MutationID{0xe1}
		operation[28] = byte(index >> 24)
		operation[29] = byte(index >> 16)
		operation[30] = byte(index >> 8)
		operation[31] = byte(index)
		payload := []byte(fmt.Sprintf("expectation-%d", index))
		digest := sha256.Sum256(payload)
		if _, err := insert.ExecContext(t.Context(), operation[:], string(authority), digest[:], payload,
			uint8(SourceMutationExpectationArmed)); err != nil {
			_ = insert.Close()
			_ = tx.Rollback()
			t.Fatal(err)
		}
	}
	if err := insert.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "settle-terminal"); err != nil {
		t.Fatal(err)
	}
	promoteObserverSnapshotForTest(t, c, authority, "settle-terminal", 0, true)
	page, err := c.SourceMutationExpectationsPage(t.Context(), authority, MutationID{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 0 || page.Next != (MutationID{}) {
		t.Fatalf("terminal expectations retained = %+v", page)
	}
}

func TestAbortSourceSnapshotStageIsExactIdempotentAndGenerationFenced(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("snapshot-abort-authority")
	configureSourceObserverForIndexTest(t, c, authority)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "first"); err != nil {
		t.Fatal(err)
	}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "first", SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: authority, RootID: "root", Relative: "file", FileIdentity: []byte("identity"), Kind: 1,
			Payload: []byte("payload"),
		}},
		Next: SourceIndexLocator{RootID: "root", Relative: "file"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.AbortSourceSnapshotStage(t.Context(), authority, "other"); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("stale snapshot abort = %v, want conflict", err)
	}
	if err := c.AbortSourceSnapshotStage(t.Context(), authority, "first"); err != nil {
		t.Fatal(err)
	}
	if err := c.AbortSourceSnapshotStage(t.Context(), authority, "first"); err != nil {
		t.Fatalf("idempotent snapshot abort = %v", err)
	}
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "second"); err != nil {
		t.Fatal(err)
	}
	if err := c.AbortSourceSnapshotStage(t.Context(), authority, "first"); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("predecessor abort deleted successor = %v", err)
	}
	var current string
	if err := c.db.QueryRowContext(t.Context(), `SELECT snapshot_id FROM source_snapshot_sessions WHERE source_authority = ?`,
		string(authority)).Scan(&current); err != nil || current != "second" {
		t.Fatalf("successor snapshot = %q, %v", current, err)
	}
}

func configureSourceObserverForIndexTest(t *testing.T, c *Catalog, authority causal.SourceAuthorityID) {
	t.Helper()
	roots := []SourceObserverRootRecord{{
		ID: "root", Generation: 1, Path: "/source", VolumeUUID: "volume", Inode: 1, Kind: 1,
	}}
	checkpoints := []SourceObserverCheckpointRecord{{Stream: "stream", RootEpoch: "epoch"}}
	operationDigest := sha256.Sum256([]byte(
		"fusekit.test.source-observer-configuration\x00" + string(authority),
	))
	var operation causal.OperationID
	copy(operation[:], operationDigest[:])
	identity := sourceObserverConfigurationIdentityForTest(
		t, authority, operation, "stream", "epoch", roots, checkpoints,
	)
	stageSourceObserverConfigurationForTest(t, c, identity, roots, checkpoints)
}

func reserveSourceMutationExpectationForTest(
	t *testing.T,
	c *Catalog,
	record SourceMutationExpectationRecord,
) error {
	t.Helper()
	if _, err := c.db.ExecContext(t.Context(), `
UPDATE source_observer_streams SET state = ? WHERE source_authority = ?`,
		uint8(SourceObserverIncremental), string(record.Authority)); err != nil {
		return err
	}
	reservation, err := sourceMutationExpectationReservationForTest(t, c, record)
	if err != nil {
		return err
	}
	return c.ReserveSourceMutationExpectation(t.Context(), reservation)
}

func sourceMutationExpectationReservationForTest(
	t *testing.T,
	c *Catalog,
	record SourceMutationExpectationRecord,
) (SourceMutationExpectationReservation, error) {
	t.Helper()
	stream, err := c.SourceObserverStream(t.Context(), record.Authority)
	if err != nil {
		return SourceMutationExpectationReservation{}, err
	}
	checkpoints, applied, err := readSourceObserverCheckpointVectors(t.Context(), c.readDB, record.Authority)
	if err != nil {
		return SourceMutationExpectationReservation{}, err
	}
	checkpointsDigest, err := SourceObserverCheckpointsDigest(checkpoints)
	if err != nil {
		return SourceMutationExpectationReservation{}, err
	}
	appliedDigest, err := SourceObserverAppliedCheckpointsDigest(applied)
	if err != nil {
		return SourceMutationExpectationReservation{}, err
	}
	record.State = 0
	return SourceMutationExpectationReservation{
		Record: record, Stream: stream.Stream, RootEpoch: stream.RootEpoch,
		LastReceived: stream.LastReceived, LastApplied: stream.LastApplied,
		CheckpointsDigest: checkpointsDigest, AppliedCheckpointsDigest: appliedDigest,
	}, nil
}

func insertSourceMutationExpectationForTest(
	t *testing.T,
	c *Catalog,
	record SourceMutationExpectationRecord,
) error {
	t.Helper()
	origin, err := json.Marshal(record.Origin)
	if err != nil {
		return err
	}
	_, err = c.db.ExecContext(t.Context(), `
INSERT INTO source_mutation_expectations(
    operation_id, source_authority, tenant, generation, causal_origin,
    payload_digest, payload, receipt_digest, receipt, state
) VALUES (?, ?, ?, ?, ?, ?, ?, X'', X'', ?)`, record.Operation[:], string(record.Authority),
		string(record.Tenant), uint64(record.Generation), origin, record.Digest[:], record.Payload,
		SourceMutationExpectationPlanned)
	return err
}

func beginSourceExpectationMutation(
	t *testing.T,
	c *Catalog,
	authority causal.SourceAuthorityID,
	name string,
) PreparedMutation {
	t.Helper()
	provision, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, name, 1))
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	prepared, err := c.BeginMutation(t.Context(), provision.Tenant, 1, MutationIntent{
		SourceID: string(authority), Disposition: MutationDispositionNamespace,
		Origin: CausalOrigin{
			Cause: causal.CauseProviderMutation, Domain: causal.DomainID("domain-" + name), Generation: 1,
		},
		Create: &CreateMutation{Spec: CreateSpec{
			Parent: provision.Root, Name: "pending", Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		}},
	})
	if err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	return prepared
}
