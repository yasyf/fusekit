package catalog

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
)

func TestSourceDriverStageCollectionCoversEveryTransientTable(t *testing.T) {
	want := []string{
		"source_driver_stage_entries",
		"source_publication_stage_affected",
		"source_publication_stage_index_logical",
		"source_publication_stage_index_deletes",
		"source_publication_stage_bindings",
		"source_publication_stage_mutations",
		"source_publication_stage_pages",
		"source_publication_stage_index",
		"source_publication_stage_revisions",
		"source_driver_stage_targets",
		"source_driver_stages",
	}
	got := make([]string, 0, len(sourceDriverStageGCTables))
	for _, table := range sourceDriverStageGCTables {
		got = append(got, table.name)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("transient collection order = %v, want %v", got, want)
	}
}

func TestSourceDriverStageCollectionIsBoundedResumableAndParentLast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, provisions, declaration, targets := openRestartableSourceDriverCatalog(t, path, "gc-tenant")
	identity := sourceDriverIdentityAtHeadForTest(
		t, store, declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotReset,
		"", "snapshot-token", 91,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	entries := make([]SourceDriverStageEntry, 300)
	for index := range entries {
		key := SourceObjectKey(fmt.Sprintf("entry-%03d", index))
		entries[index] = SourceDriverStageEntry{
			Tenant: provisions[0].Tenant, Generation: provisions[0].Generation, Key: key,
			Object: &SourceObject{
				Key: key, Name: string(key), Kind: KindDirectory,
				Visibility: Visibility{Mount: true, FileProvider: true},
			},
		}
	}
	first, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{
			Digest: [sha256.Size]byte{91}, Cursor: []byte("continue"), Entries: entries[:256],
		},
	))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendSourceDriverStage(t.Context(), identity, sourceDriverPageForTest(
		first, SourceDriverStagePage{
			Digest: [sha256.Size]byte{92}, Complete: true, Entries: entries[256:],
		},
	)); err != nil {
		t.Fatal(err)
	}

	initialChildren := sourceDriverStageGCChildCount(t, store, identity)
	if initialChildren <= sourceDriverStageGCDeleteLimit {
		t.Fatalf("stage child rows = %d, want more than one collection batch", initialChildren)
	}
	for calls := 0; ; calls++ {
		before := sourceDriverStageGCChildCount(t, store, identity)
		parentBefore := sourceDriverStageGCParentCount(t, store, identity)
		result, err := store.drainSourceDriverStageRows(t.Context(), identity.Authority, identity.Operation)
		if err != nil {
			t.Fatalf("collection call %d: %v", calls, err)
		}
		if result.Deleted < 0 || result.Deleted > sourceDriverStageGCDeleteLimit {
			t.Fatalf("collection call %d deleted %d rows", calls, result.Deleted)
		}
		after := sourceDriverStageGCChildCount(t, store, identity)
		parentAfter := sourceDriverStageGCParentCount(t, store, identity)
		if result.ParentDeleted {
			if before != 0 || after != 0 || parentBefore != 1 || parentAfter != 0 || !result.Complete {
				t.Fatalf("parent deletion before=%d after=%d parent=%d/%d result=%+v",
					before, after, parentBefore, parentAfter, result)
			}
		} else {
			if before-after != result.Deleted || parentBefore != 1 || parentAfter != 1 || result.Complete {
				t.Fatalf("child batch before=%d after=%d parent=%d/%d result=%+v",
					before, after, parentBefore, parentAfter, result)
			}
		}
		if calls == 2 {
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store, err = Open(t.Context(), path)
			if err != nil {
				t.Fatalf("reopen interrupted collection: %v", err)
			}
		}
		if result.Complete {
			break
		}
		if calls > 100 {
			t.Fatal("source driver stage collection did not converge")
		}
	}
	result, err := store.drainSourceDriverStageRows(t.Context(), identity.Authority, identity.Operation)
	if err != nil || !result.Complete || result.Deleted != 0 || result.ParentDeleted {
		t.Fatalf("completed collection replay = %+v, %v", result, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSourceDriverStageCollectionRequiresReleasedContentClaims(t *testing.T) {
	store, provisions, declaration, targets := newSourceDriverCatalog(t, "gc-content")
	identity := sourceDriverIdentityAtHeadForTest(
		t, store, declaration, targets, SourceDriverSnapshot, SourceDriverSnapshotReset,
		"", "snapshot-token", 93,
	)
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatal(err)
	}
	ref, err := store.StageContent(t.Context(), bytes.NewBufferString("claimed"))
	if err != nil {
		t.Fatal(err)
	}
	page := sourceDriverPageForTest(SourceDriverStageState{}, SourceDriverStagePage{
		Digest: [sha256.Size]byte{93}, Cursor: []byte("continue"),
		Entries: []SourceDriverStageEntry{{
			Tenant: provisions[0].Tenant, Generation: provisions[0].Generation, Key: "claimed",
			Object: &SourceObject{
				Key: "claimed", Name: "claimed", Kind: KindFile, ContentRevision: 1, Content: ref,
				Visibility: Visibility{Mount: true},
			},
		}},
	})
	if _, err := store.AppendSourceDriverStage(t.Context(), identity, page); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE content_stages SET source_operation_id = ? WHERE stage_id = ?`,
		identity.Operation[:], ref.Stage[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := store.drainSourceDriverStageRows(t.Context(), identity.Authority, identity.Operation); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("collection with claimed content = %v, want invalid transition", err)
	}
	if got := sourceDriverStageGCParentCount(t, store, identity); got != 1 {
		t.Fatalf("parent rows after rejected collection = %d, want 1", got)
	}
	if err := store.AbortSourceDriverStage(t.Context(), identity); err != nil {
		t.Fatalf("abort partial source driver stage: %v", err)
	}
	var stages int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM content_stages WHERE stage_id = ?`, ref.Stage[:]).Scan(&stages); err != nil {
		t.Fatal(err)
	}
	if stages != 0 {
		t.Fatalf("claimed content stages after abort = %d, want 0", stages)
	}
	if pending, err := store.PendingSourceDriverStage(t.Context(), identity.Authority); err != nil || pending != nil {
		t.Fatalf("pending stage after abort = %+v, %v, want absent", pending, err)
	}
}

func openRestartableSourceDriverCatalog(
	t *testing.T,
	path string,
	tenantNames ...string,
) (*Catalog, []TenantProvision, [sha256.Size]byte, []SourceDriverTarget) {
	t.Helper()
	store, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	provisions := make([]TenantProvision, 0, len(tenantNames))
	for _, name := range tenantNames {
		provision := testTenantProvision(t, name, 1)
		provision.ContentSourceID = "driver-authority"
		persisted, err := provisionTenantForTest(t, store, t.Context(), provision)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		provisions = append(provisions, persisted)
	}
	fleet := reconcileSourceAuthorityFleetForTest(t, store, "driver-owner", 0, 1, "driver-authority")
	acknowledgeSourceAuthorityFleetForTest(t, store, fleet)
	declaration := sourceAuthorityDeclarationsForTest("driver-authority")[0].DeclarationDigest
	targets := sourceDriverTargetsForProvisions(t, provisions...)
	seedSourceDriverLifecycleCheckpointForTest(t, store, declaration, provisions, targets)
	return store, provisions, declaration, targets
}

func sourceDriverStageGCChildCount(t *testing.T, store *Catalog, identity SourceDriverStageIdentity) int {
	t.Helper()
	total := 0
	for _, table := range sourceDriverStageGCTables {
		var count int
		query := "SELECT COUNT(*) FROM " + table.name + " WHERE " + table.key
		if err := store.readDB.QueryRowContext(t.Context(), query, string(identity.Authority), identity.Operation[:]).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table.name, err)
		}
		total += count
	}
	return total
}

func sourceDriverStageGCParentCount(t *testing.T, store *Catalog, identity SourceDriverStageIdentity) int {
	t.Helper()
	var count int
	if err := store.readDB.QueryRowContext(t.Context(), `
SELECT COUNT(*) FROM source_publication_stages
WHERE source_authority = ? AND stage_operation_id = ?`,
		string(identity.Authority), identity.Operation[:]).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
