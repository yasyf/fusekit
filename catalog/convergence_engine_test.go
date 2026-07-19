package catalog

import (
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestConvergenceEngineMutationProofsAreBounded(t *testing.T) {
	store, err := Open(t.Context(), t.TempDir()+"/catalog.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	for index := range convergenceEngineProofRetention + 44 {
		operation, err := NewConvergenceEngineOperation()
		if err != nil {
			t.Fatal(err)
		}
		if err := store.StageConvergenceEngineMutation(t.Context(), ConvergenceEngineStage{
			Operation: operation, ExpectedVersion: uint64(index), Sequence: 0, PageCount: 1,
			Page: ConvergenceEngineDeltaPage{},
		}); err != nil {
			t.Fatalf("StageConvergenceEngineMutation(%d): %v", index, err)
		}
		if _, err := store.PublishConvergenceEngineMutation(t.Context(), operation); err != nil {
			t.Fatalf("PublishConvergenceEngineMutation(%d): %v", index, err)
		}
	}

	for table, want := range map[string]int{
		"convergence_engine_mutations":      convergenceEngineProofRetention,
		"convergence_engine_mutation_pages": convergenceEngineProofRetention,
	} {
		var got int
		if err := store.readDB.QueryRowContext(t.Context(), "SELECT count(*) FROM "+table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s rows = %d, want %d", table, got, want)
		}
	}
}

func TestDiscardUnpublishedConvergenceEngineMutationRemovesPages(t *testing.T) {
	store, err := Open(t.Context(), t.TempDir()+"/catalog.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operation, err := NewConvergenceEngineOperation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StageConvergenceEngineMutation(t.Context(), ConvergenceEngineStage{
		Operation: operation, Sequence: 0, PageCount: 1, Page: ConvergenceEngineDeltaPage{},
	}); err != nil {
		t.Fatal(err)
	}
	future, err := NewConvergenceEngineOperation()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StageConvergenceEngineMutation(t.Context(), ConvergenceEngineStage{
		Operation: future, ExpectedVersion: 99, Sequence: 0, PageCount: 1, Page: ConvergenceEngineDeltaPage{},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.DiscardUnpublishedConvergenceEngineMutations(t.Context(), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PublishConvergenceEngineMutation(t.Context(), operation); !errors.Is(err, ErrNotFound) {
		t.Fatalf("PublishConvergenceEngineMutation after discard = %v, want ErrNotFound", err)
	}
	var pages, mutations int
	if err := store.readDB.QueryRowContext(
		t.Context(), "SELECT count(*) FROM convergence_engine_mutation_pages",
	).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if err := store.readDB.QueryRowContext(
		t.Context(), "SELECT count(*) FROM convergence_engine_mutations",
	).Scan(&mutations); err != nil {
		t.Fatal(err)
	}
	if pages != 0 || mutations != 0 {
		t.Fatalf("staged residue after discard = %d mutations / %d pages, want 0/0", mutations, pages)
	}
}

func TestConvergenceEngineStageEnforcesPageAndByteBounds(t *testing.T) {
	store, err := Open(t.Context(), t.TempDir()+"/catalog.sqlite")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	operation, err := NewConvergenceEngineOperation()
	if err != nil {
		t.Fatal(err)
	}
	oversizedRecords := make([]ConvergenceEngineHead, ConvergenceEnginePageLimit+1)
	if err := store.StageConvergenceEngineMutation(t.Context(), ConvergenceEngineStage{
		Operation: operation, Sequence: 0, PageCount: 1,
		Page: ConvergenceEngineDeltaPage{UpsertHeads: oversizedRecords},
	}); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("record-bound stage = %v, want ErrInvalidObject", err)
	}
	exactRecords := make([]ConvergenceEngineHead, ConvergenceEnginePageLimit)
	if err := store.StageConvergenceEngineMutation(t.Context(), ConvergenceEngineStage{
		Operation: operation, Sequence: 0, PageCount: 1,
		Page: ConvergenceEngineDeltaPage{
			UpsertHeads: exactRecords, ResetOutbox: true,
		},
	}); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("outbox-inclusive record-bound stage = %v, want ErrInvalidObject", err)
	}
	if err := store.StageConvergenceEngineMutation(t.Context(), ConvergenceEngineStage{
		Operation: operation, Sequence: 0, PageCount: 1,
		Page: ConvergenceEngineDeltaPage{
			Outbox: &ConvergenceEngineOutbox{},
		},
	}); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("outbox without reset = %v, want ErrInvalidObject", err)
	}
	oversizedBytes := make([]ConvergenceEngineHead, ConvergenceEnginePageLimit)
	for index := range oversizedBytes {
		oversizedBytes[index].Authority = causal.SourceAuthorityID(strings.Repeat("x", 17<<10))
	}
	if err := store.StageConvergenceEngineMutation(t.Context(), ConvergenceEngineStage{
		Operation: operation, Sequence: 0, PageCount: 1,
		Page: ConvergenceEngineDeltaPage{UpsertHeads: oversizedBytes},
	}); !errors.Is(err, ErrStorageQuota) {
		t.Fatalf("byte-bound stage = %v, want ErrStorageQuota", err)
	}
}
