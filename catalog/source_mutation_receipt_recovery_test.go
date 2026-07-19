package catalog

import (
	"crypto/sha256"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestRecoverSourceMutationExpectationReceiptSurvivesRestartInEveryRecoverableState(t *testing.T) {
	tests := []struct {
		name string
		from uint8
		want uint8
	}{
		{name: "planned", from: SourceMutationExpectationPlanned, want: SourceMutationExpectationComplete},
		{name: "repair-required", from: SourceMutationExpectationRepairRequired, want: SourceMutationExpectationRepairRequired},
		{name: "repair-published", from: SourceMutationExpectationRepairPublished, want: SourceMutationExpectationRepairPublished},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.sqlite")
			store, err := Open(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			authority := causal.SourceAuthorityID("receipt-recovery-" + test.name)
			configureSourceObserverForIndexTest(t, store, authority)
			operation := MutationID{byte(index + 1), 29}
			payload := []byte("expectation-" + test.name)
			if err := store.PutSourceMutationExpectation(t.Context(), SourceMutationExpectationRecord{
				Operation: operation, Authority: authority, Tenant: "tenant", Generation: 1,
				Origin: CausalOrigin{Cause: causal.CauseDaemonWrite},
				Digest: sha256.Sum256(payload), Payload: payload,
			}); err != nil {
				t.Fatal(err)
			}
			if test.from != SourceMutationExpectationPlanned {
				if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_mutation_expectations SET state = ?
WHERE source_authority = ? AND operation_id = ?`, test.from, string(authority), operation[:]); err != nil {
					t.Fatal(err)
				}
			}
			receipt := []byte("receipt-" + test.name)
			if err := store.RecoverSourceMutationExpectationReceipt(t.Context(), authority, operation, receipt); err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(t.Context(), path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			record, err := store.SourceMutationExpectation(t.Context(), authority, operation)
			if err != nil || record.State != test.want || string(record.Receipt) != string(receipt) ||
				record.ReceiptDigest != sha256.Sum256(receipt) {
				t.Fatalf("recovered receipt after restart = %+v, %v", record, err)
			}
			if err := store.RecoverSourceMutationExpectationReceipt(t.Context(), authority, operation, receipt); err != nil {
				t.Fatalf("exact recovered receipt replay: %v", err)
			}
			if err := store.RecoverSourceMutationExpectationReceipt(
				t.Context(), authority, operation, []byte("different"),
			); !errors.Is(err, ErrSourceObserverConflict) {
				t.Fatalf("different recovered receipt = %v, want ErrSourceObserverConflict", err)
			}
		})
	}
}
