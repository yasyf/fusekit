package catalog

import (
	"errors"
	"fmt"
	"testing"
)

func TestTombstoneCollectionRemainsBoundedUnderLongRunningChurn(t *testing.T) {
	ctx := t.Context()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "tombstone-pages", CaseSensitive)
	const total = RetainedIdentityPageLimit + 17
	for index := 0; index < total; index++ {
		object, err := c.Create(ctx, tenant, CreateSpec{
			Parent: root.ID, Name: fmt.Sprintf("retired-%03d", index),
			Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		})
		if err != nil {
			t.Fatalf("Create(%d): %v", index, err)
		}
		if _, err := c.Delete(ctx, tenant, object.ID); err != nil {
			t.Fatalf("Delete(%d): %v", index, err)
		}
	}
	if _, err := c.Create(ctx, tenant, CreateSpec{
		Parent: root.ID, Name: "floor-advance", Kind: KindDirectory, Mode: 0o700,
		Visibility: Visibility{Mount: true, FileProvider: true},
	}); err != nil {
		t.Fatal(err)
	}
	head := mustCatalogHead(t, c, tenant)
	prepareTombstoneGCFixture(t, c, tenant, head)
	var versions, objects int
	for page := 0; page < 10; page++ {
		result, err := c.CollectRetainedIdentityGarbage(ctx, tenant, head)
		if err != nil {
			t.Fatal(err)
		}
		retired := result.Handles + result.MutationPins +
			result.ObjectVersions + result.Objects + result.Owners
		if retired > RetainedIdentityPageLimit {
			t.Fatalf("collection page %d retired %d rows: %+v", page, retired, result)
		}
		versions += result.ObjectVersions
		objects += result.Objects
		if !result.More {
			break
		}
	}
	if versions < total || objects != total {
		t.Fatalf("long-running collection versions=%d objects=%d, want >=%d/%d",
			versions, objects, total, total)
	}
	var tombstones int
	if err := c.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM objects WHERE tenant = ? AND tombstone = 1",
		string(tenant)).Scan(&tombstones); err != nil {
		t.Fatal(err)
	}
	if tombstones != 0 {
		t.Fatalf("retained tombstone rows = %d", tombstones)
	}
}

func TestTombstoneCollectionRequiresEveryDurableReachabilityFence(t *testing.T) {
	ctx := t.Context()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "tombstone-fences", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "target", "old")
	source := createTestFile(t, c, tenant, root.ID, "temporary", "new")
	owner, err := NewRetentionOwner("native:tombstone-fences")
	if err != nil {
		t.Fatal(err)
	}
	handle, err := c.OpenAt(
		ctx, owner, tenant, PresentationMount, 1, target.ID, target.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	replaced, err := c.Replace(ctx, tenant, source.ID, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	createTestFile(t, c, tenant, root.ID, "floor-advance", "later")
	head := mustCatalogHead(t, c, tenant)
	if replaced.Target.Revision >= head {
		t.Fatalf("tombstone revision %d did not precede head %d", replaced.Target.Revision, head)
	}
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO source_object_ids(source_authority, source_key, object_id)
VALUES ('retention-test', 'target', ?)`, target.ID[:]); err != nil {
		t.Fatal(err)
	}
	prepareTombstoneGCFixture(t, c, tenant, replaced.Revision)

	result, err := c.CollectRetainedIdentityGarbage(ctx, tenant, replaced.Revision)
	if err != nil {
		t.Fatal(err)
	}
	versions := result.ObjectVersions
	if result.Objects != 0 {
		t.Fatalf("reachable floor tombstone collected: %+v", result)
	}
	if _, err := c.db.ExecContext(ctx,
		"UPDATE tenants SET floor = ? WHERE tenant = ?",
		uint64(head), string(tenant)); err != nil {
		t.Fatal(err)
	}
	result, err = c.CollectRetainedIdentityGarbage(ctx, tenant, head)
	if err != nil {
		t.Fatal(err)
	}
	versions += result.ObjectVersions
	if result.Objects != 0 {
		t.Fatalf("reachable tombstone collected: %+v", result)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Forget(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := c.db.ExecContext(ctx, `
DELETE FROM source_object_ids
WHERE source_authority = 'retention-test' AND source_key = 'target'`); err != nil {
		t.Fatal(err)
	}
	var objects, candidates int
	for {
		result, err = c.CollectRetainedIdentityGarbage(ctx, tenant, head)
		if err != nil {
			t.Fatal(err)
		}
		versions += result.ObjectVersions
		objects += result.Objects
		if !result.More {
			break
		}
	}
	if versions == 0 || objects != 1 {
		t.Fatalf("unreachable tombstone collection versions=%d objects=%d", versions, objects)
	}
	if err := c.db.QueryRowContext(ctx, `
SELECT COUNT(*) FROM blob_gc_candidates WHERE hash = ?`,
		target.Hash[:]).Scan(&candidates); err != nil {
		t.Fatal(err)
	}
	if candidates != 1 {
		t.Fatalf("tombstone blob candidates = %d, want 1", candidates)
	}
	if _, err := c.Lookup(ctx, tenant, PresentationMount, target.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("collected tombstone lookup = %v, want not found", err)
	}
}

func prepareTombstoneGCFixture(
	t *testing.T,
	c *Catalog,
	tenant TenantID,
	floor Revision,
) {
	t.Helper()
	ctx := t.Context()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM changes WHERE tenant = ?", string(tenant)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM prepared_mutations WHERE tenant = ?", string(tenant)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM mutation_journal WHERE tenant = ?", string(tenant)); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE tenants SET floor = ? WHERE tenant = ?",
		uint64(floor), string(tenant)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
