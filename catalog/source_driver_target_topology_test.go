package catalog

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestTenantIntentOwnsSourceDriverTargetEpoch(t *testing.T) {
	database := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}

	first := testTenantProvision(t, "target-epoch", 1)
	declare := tenantMutationForTest(t, first.OwnerID, 0)
	state, err := store.SetTenantPresent(t.Context(), declare, first)
	if err != nil {
		t.Fatal(err)
	}
	requireSourceDriverTargetEpoch(t, store, first.ContentSourceID, 1)

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), database)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	replayed, err := store.SetTenantPresent(t.Context(), declare, first)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Intent.Revision != state.Intent.Revision || replayed.Intent.CurrentOperation != declare.OperationID {
		t.Fatalf("lost-response replay = %+v, want revision %d operation %x",
			replayed.Intent, state.Intent.Revision, declare.OperationID)
	}
	requireSourceDriverTargetEpoch(t, store, first.ContentSourceID, 1)

	second := first
	second.Generation = 2
	replace := tenantMutationForTest(t, first.OwnerID, state.Intent.Revision)
	state, err = store.SetTenantPresent(t.Context(), replace, second)
	if err != nil {
		t.Fatal(err)
	}
	requireSourceDriverTargetEpoch(t, store, first.ContentSourceID, 2)
	if err := store.ValidateSourceDriverTargetEpoch(t.Context(), causal.SourceAuthorityID(first.ContentSourceID), 1); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("stale replacement epoch = %v, want ErrGenerationMismatch", err)
	}
	if _, err := store.SetTenantPresent(t.Context(), replace, second); err != nil {
		t.Fatal(err)
	}
	requireSourceDriverTargetEpoch(t, store, first.ContentSourceID, 2)

	remove := tenantMutationForTest(t, first.OwnerID, state.Intent.Revision)
	if _, err := store.SetTenantAbsent(t.Context(), remove, first.Tenant); err != nil {
		t.Fatal(err)
	}
	requireSourceDriverTargetEpoch(t, store, first.ContentSourceID, 3)
	if err := store.ValidateSourceDriverTargetEpoch(t.Context(), causal.SourceAuthorityID(first.ContentSourceID), 2); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("removed generation epoch = %v, want ErrGenerationMismatch", err)
	}
	if _, err := store.SetTenantAbsent(t.Context(), remove, first.Tenant); err != nil {
		t.Fatal(err)
	}
	requireSourceDriverTargetEpoch(t, store, first.ContentSourceID, 3)
}

func TestTenantIntentMovesTargetEpochBetweenAuthorities(t *testing.T) {
	store := newTestCatalog(t)
	first := testTenantProvision(t, "target-authority", 1)
	state, err := store.SetTenantPresent(t.Context(), tenantMutationForTest(t, first.OwnerID, 0), first)
	if err != nil {
		t.Fatal(err)
	}
	requireSourceDriverTargetEpoch(t, store, first.ContentSourceID, 1)

	second := first
	second.Generation = 2
	second.ContentSourceID = "next-source"
	if _, err := store.SetTenantPresent(
		t.Context(), tenantMutationForTest(t, first.OwnerID, state.Intent.Revision), second,
	); err != nil {
		t.Fatal(err)
	}
	requireSourceDriverTargetEpoch(t, store, first.ContentSourceID, 2)
	requireSourceDriverTargetEpoch(t, store, second.ContentSourceID, 1)
}

func requireSourceDriverTargetEpoch(t *testing.T, store *Catalog, authority string, want uint64) {
	t.Helper()
	got, err := store.SourceDriverTargetEpoch(t.Context(), causal.SourceAuthorityID(authority))
	if err != nil || got != want {
		t.Fatalf("SourceDriverTargetEpoch(%q) = %d, %v; want %d", authority, got, err, want)
	}
}
