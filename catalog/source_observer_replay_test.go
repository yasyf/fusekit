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
