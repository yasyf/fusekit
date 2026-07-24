package catalog

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestCompactedSourceObserverInboxRetainsExactReplayProof(t *testing.T) {
	database := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	authority := causal.SourceAuthorityID("observer-replay")
	configureSourceObserverForIndexTest(t, store, authority)
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_observer_streams SET state = ? WHERE source_authority = ?`,
		uint8(SourceObserverIncremental), string(authority)); err != nil {
		t.Fatal(err)
	}
	payload := []byte("event")
	record := SourceObserverInboxRecord{
		Authority: authority, Stream: "stream", RootEpoch: "epoch",
		NativeCursor: 1, EventCount: 1, Digest: sha256.Sum256(payload), Payload: payload,
	}
	sequence, err := store.AppendSourceObserverInbox(t.Context(), record)
	if err != nil || sequence != 1 {
		t.Fatalf("append = %d, %v; want 1", sequence, err)
	}
	settlement := SourceObserverSettlement{
		Authority: authority, Stream: "stream", RootEpoch: "epoch",
		Through: sequence, Operation: causal.OperationID{0x41},
	}
	if err := store.SettleSourceObserver(t.Context(), settlement); err != nil {
		t.Fatal(err)
	}
	var retained int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_observer_inbox WHERE source_authority = ?`, string(authority)).Scan(&retained); err != nil {
		t.Fatal(err)
	}
	if retained != 0 {
		t.Fatalf("compacted inbox retained %d hot rows", retained)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if replayed, err := store.AppendSourceObserverInbox(t.Context(), record); err != nil || replayed != sequence {
		t.Fatalf("append replay = %d, %v; want %d", replayed, err, sequence)
	}
	if err := store.SettleSourceObserver(t.Context(), settlement); err != nil {
		t.Fatalf("settlement replay: %v", err)
	}
	forged := settlement
	forged.Operation = causal.OperationID{0x42}
	if err := store.SettleSourceObserver(t.Context(), forged); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("forged settlement replay = %v, want conflict", err)
	}
	forgedRecord := record
	forgedRecord.Payload = []byte("forged")
	forgedRecord.Digest = sha256.Sum256(forgedRecord.Payload)
	if _, err := store.AppendSourceObserverInbox(t.Context(), forgedRecord); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("forged append replay = %v, want conflict", err)
	}
}

func TestSourceMutationExpectationReservationRejectsObserverFenceRace(t *testing.T) {
	store := newTestCatalog(t)
	authority := causal.SourceAuthorityID("mutation-fence-race")
	configureSourceObserverForIndexTest(t, store, authority)
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_observer_streams SET state = ? WHERE source_authority = ?`,
		uint8(SourceObserverIncremental), string(authority)); err != nil {
		t.Fatal(err)
	}
	record := SourceMutationExpectationRecord{
		Operation: MutationID{0x51}, Authority: authority, Tenant: "tenant", Generation: 1,
		Origin:  CausalOrigin{Cause: causal.CauseDaemonWrite},
		Payload: []byte("mutation-plan"),
	}
	record.Digest = sha256.Sum256(record.Payload)
	stale, err := sourceMutationExpectationReservationForTest(t, store, record)
	if err != nil {
		t.Fatal(err)
	}
	event := []byte("event")
	sequence, err := store.AppendSourceObserverInbox(t.Context(), SourceObserverInboxRecord{
		Authority: authority, Stream: "stream", RootEpoch: "epoch", NativeCursor: 1,
		EventCount: 1, Digest: sha256.Sum256(event), Payload: event,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveSourceMutationExpectation(t.Context(), stale); !errors.Is(err, ErrSourceObserverFenceChanged) {
		t.Fatalf("stale observer reservation = %v, want fence changed", err)
	}
	if _, err := store.SourceMutationExpectation(t.Context(), authority, record.Operation); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale reservation persisted expectation: %v", err)
	}
	if err := store.SettleSourceObserver(t.Context(), SourceObserverSettlement{
		Authority: authority, Stream: "stream", RootEpoch: "epoch",
		Through: sequence, Operation: causal.OperationID{0x52},
	}); err != nil {
		t.Fatal(err)
	}
	fresh, err := sourceMutationExpectationReservationForTest(t, store, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveSourceMutationExpectation(t.Context(), fresh); err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveSourceMutationExpectation(t.Context(), stale); err != nil {
		t.Fatalf("exact lost-response replay consulted mutable fence: %v", err)
	}
	conflicting := fresh
	conflicting.Record.Operation = MutationID{0x53}
	conflicting.Record.Payload = []byte("other-plan")
	conflicting.Record.Digest = sha256.Sum256(conflicting.Record.Payload)
	if err := store.ReserveSourceMutationExpectation(t.Context(), conflicting); !errors.Is(err, ErrSourceObserverConflict) {
		t.Fatalf("concurrent mutation reservation = %v, want conflict", err)
	}
}

func TestSourceMutationExpectationReservationRejectsOrphanedInbox(t *testing.T) {
	store := newTestCatalog(t)
	authority := causal.SourceAuthorityID("mutation-fence-orphan")
	configureSourceObserverForIndexTest(t, store, authority)
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_observer_streams SET state = ? WHERE source_authority = ?`,
		uint8(SourceObserverIncremental), string(authority)); err != nil {
		t.Fatal(err)
	}
	event := []byte("event")
	if _, err := store.AppendSourceObserverInbox(t.Context(), SourceObserverInboxRecord{
		Authority: authority, Stream: "stream", RootEpoch: "epoch", NativeCursor: 1,
		EventCount: 1, Digest: sha256.Sum256(event), Payload: event,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_observer_inbox SET root_epoch = 'orphaned' WHERE source_authority = ?`,
		string(authority)); err != nil {
		t.Fatal(err)
	}
	record := SourceMutationExpectationRecord{
		Operation: MutationID{0x61}, Authority: authority, Tenant: "tenant", Generation: 1,
		Origin: CausalOrigin{Cause: causal.CauseDaemonWrite}, Payload: []byte("mutation-plan"),
	}
	record.Digest = sha256.Sum256(record.Payload)
	reservation, err := sourceMutationExpectationReservationForTest(t, store, record)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReserveSourceMutationExpectation(t.Context(), reservation); !errors.Is(err, ErrSourceObserverFenceChanged) {
		t.Fatalf("orphaned inbox reservation = %v, want fence changed", err)
	}
	if _, err := store.SourceMutationExpectation(t.Context(), authority, record.Operation); !errors.Is(err, ErrNotFound) {
		t.Fatalf("orphaned inbox persisted expectation: %v", err)
	}
}
