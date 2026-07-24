package catalog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestEnumerationScopeShapesAreClosed(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "scope-shape", CaseSensitive)
	tests := []struct {
		name  string
		scope EnumerationScope
	}{
		{name: "working set with parent", scope: EnumerationScope{Kind: EnumerationWorkingSet, Parent: root.ID}},
		{name: "container without parent", scope: EnumerationScope{Kind: EnumerationContainer}},
		{name: "unknown", scope: EnumerationScope{Kind: 99}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := c.Snapshot(context.Background(), tenant, test.scope, root.Revision, SnapshotCursor{}, 10); !errors.Is(err, ErrInvalidObject) {
				t.Fatalf("Snapshot() error = %v, want ErrInvalidObject", err)
			}
			if _, err := c.ChangesSince(context.Background(), tenant, test.scope, CompleteChangeCursor(0), 10); !errors.Is(err, ErrInvalidObject) {
				t.Fatalf("ChangesSince() error = %v, want ErrInvalidObject", err)
			}
		})
	}
}

func TestDirectMutationWithoutActivePublicationFailsExact(t *testing.T) {
	c := newTestCatalog(t)
	tenant, err := NewTenantID("scope-move")
	if err != nil {
		t.Fatal(err)
	}
	root, err := c.CreateTenant(t.Context(), tenant, CaseSensitive, PresentMount|PresentFileProvider)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Create(context.Background(), tenant, CreateSpec{
		Parent: root.ID, Name: "removed-direct-mutation", Kind: KindDirectory, Mode: 0o755,
		Visibility: Visibility{Mount: true, FileProvider: true},
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("direct mutation without active publication = %v, want ErrNotFound", err)
	}
}

func TestContainerSnapshotUsesParentIndexAndNeverReadsContent(t *testing.T) {
	c := newTestCatalog(t)
	definition := testTenantProvision(t, "scope-index", 1)
	definition.OwnerID = "driver-owner"
	definition.ContentSourceID = "driver-authority"
	provision, err := provisionTenantForTest(t, c, t.Context(), definition)
	if err != nil {
		t.Fatal(err)
	}
	fleet := reconcileSourceAuthorityFleetForTest(t, c, "driver-owner", 0, 1, "driver-authority")
	acknowledgeSourceAuthorityFleetForTest(t, c, fleet)
	declaration := sourceAuthorityDeclarationsForTest("driver-authority")[0].DeclarationDigest
	targets := sourceDriverTargetsForProvisions(t, provision)
	seedSourceDriverLifecycleCheckpointForTest(t, c, declaration, []TenantProvision{provision}, targets, false)
	tenant := provision.Tenant
	root, err := c.Root(t.Context(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	left := createTestDirectory(t, c, tenant, root.ID, "left")
	right := createTestDirectory(t, c, tenant, root.ID, "right")
	wanted := createTestFile(t, c, tenant, left.ID, "wanted", "content")
	var authority string
	var predecessor []byte
	var predecessorRevision, predecessorHead, visibilityEpoch uint64
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT head.source_authority, head.publication_id, head.source_revision, target.catalog_head, head.epoch
FROM source_driver_publication_heads head
JOIN source_driver_publication_targets target
  ON target.source_authority = head.source_authority
 AND target.publication_id = head.publication_id
WHERE target.tenant = ?`, string(tenant)).Scan(
		&authority, &predecessor, &predecessorRevision, &predecessorHead, &visibilityEpoch,
	); err != nil {
		t.Fatal(err)
	}
	head := Revision(predecessorHead + 1)
	objects := []Object{root, left, right, wanted}
	for index := 0; index < testScaleCount(10_000); index++ {
		id, err := NewObjectID()
		if err != nil {
			t.Fatal(err)
		}
		object := wanted
		object.ID = id
		object.Parent = right.ID
		object.Revision = head
		object.MetadataRevision = head
		object.ContentRevision = head
		object.Name = fmt.Sprintf("other-%05d", index)
		objects = append(objects, object)
	}
	publicationID, err := NewObjectID()
	if err != nil {
		t.Fatal(err)
	}
	insertVisibilityPublication(t, c, publicationID[:], predecessor,
		predecessorRevision+1, predecessorRevision, Revision(predecessorHead), head,
		objects, nil, objects[len(objects)-1])
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
UPDATE source_driver_publication_heads
SET publication_id = ?, source_revision = ?, epoch = ?
WHERE source_authority = ?`, publicationID[:], predecessorRevision+1, visibilityEpoch+1,
		authority); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), "UPDATE tenants SET head = ? WHERE tenant = ?",
		uint64(head), string(tenant)); err != nil {
		t.Fatal(err)
	}
	if err := alignTenantApplicationsWithSourceHeadForTest(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(c.blobPath(wanted.Hash)); err != nil {
		t.Fatalf("remove content blob: %v", err)
	}
	head, err = c.Head(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	page, err := c.Snapshot(context.Background(), tenant, EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: left.ID}, head, SnapshotCursor{}, 10)
	if err != nil {
		t.Fatalf("Snapshot(left): %v", err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != wanted.ID {
		t.Fatalf("left snapshot = %+v, want wanted only", page.Objects)
	}
	rows, err := c.readDB.QueryContext(context.Background(), `
EXPLAIN QUERY PLAN
SELECT v.object_id FROM source_driver_publication_objects v
INDEXED BY source_driver_publication_objects_provider_parent
WHERE v.source_authority = ? AND v.publication_id = ? AND v.tenant = ?
  AND v.parent_id = ? AND v.object_id <> ? AND v.revision <= ?
  AND v.tombstone = 0 AND v.file_provider_visible = 1
ORDER BY v.object_id LIMIT 11`, authority, publicationID[:], string(tenant),
		left.ID[:], left.ID[:], uint64(head))
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("Close query plan rows: %v", err)
		}
	}()
	plan := ""
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		plan += detail
	}
	if !strings.Contains(plan, "source_driver_publication_objects_provider_parent") {
		t.Fatalf("container query plan = %q, want parent index", plan)
	}
}

func TestChangesSinceUsesScopedIndexAndNeverReadsRootOrContent(t *testing.T) {
	c := newTestCatalog(t)
	definition := testTenantProvision(t, "changes-index", 1)
	definition.OwnerID = "driver-owner"
	definition.ContentSourceID = "driver-authority"
	provision, err := provisionTenantForTest(t, c, t.Context(), definition)
	if err != nil {
		t.Fatal(err)
	}
	fleet := reconcileSourceAuthorityFleetForTest(t, c, "driver-owner", 0, 1, "driver-authority")
	acknowledgeSourceAuthorityFleetForTest(t, c, fleet)
	declaration := sourceAuthorityDeclarationsForTest("driver-authority")[0].DeclarationDigest
	targets := sourceDriverTargetsForProvisions(t, provision)
	seedSourceDriverLifecycleCheckpointForTest(t, c, declaration, []TenantProvision{provision}, targets, false)
	tenant := provision.Tenant
	root, err := c.Root(t.Context(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	left := createTestDirectory(t, c, tenant, root.ID, "left")
	wanted := createTestFile(t, c, tenant, left.ID, "wanted", "content")
	var authority string
	var predecessor []byte
	var predecessorRevision, predecessorHead, visibilityEpoch uint64
	if err := c.readDB.QueryRowContext(t.Context(), `
SELECT head.source_authority, head.publication_id, head.source_revision, target.catalog_head, head.epoch
FROM source_driver_publication_heads head
JOIN source_driver_publication_targets target
  ON target.source_authority = head.source_authority
 AND target.publication_id = head.publication_id
WHERE target.tenant = ?`, string(tenant)).Scan(
		&authority, &predecessor, &predecessorRevision, &predecessorHead, &visibilityEpoch,
	); err != nil {
		t.Fatal(err)
	}
	anchor := Revision(predecessorHead)
	head := anchor + 1
	wanted.Revision = head
	wanted.MetadataRevision = head
	wanted.ContentRevision = head
	objects := []Object{root, left, wanted}
	for index := 0; index < testScaleCount(10_000); index++ {
		id, err := NewObjectID()
		if err != nil {
			t.Fatal(err)
		}
		object := wanted
		object.ID = id
		object.Parent = root.ID
		object.Name = fmt.Sprintf("other-%05d", index)
		objects = append(objects, object)
	}
	publicationID, err := NewObjectID()
	if err != nil {
		t.Fatal(err)
	}
	insertVisibilityPublication(t, c, publicationID[:], predecessor,
		predecessorRevision+1, predecessorRevision, anchor, head,
		objects, []Object{wanted}, wanted)
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
UPDATE source_driver_publication_changes SET presentation = ?
WHERE source_authority = ? AND publication_id = ?`,
		uint8(PresentationFileProvider), authority, publicationID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
UPDATE source_driver_publication_heads
SET publication_id = ?, source_revision = ?, epoch = ?
WHERE source_authority = ?`, publicationID[:], predecessorRevision+1, visibilityEpoch+1,
		authority); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), "UPDATE tenants SET head = ? WHERE tenant = ?",
		uint64(head), string(tenant)); err != nil {
		t.Fatal(err)
	}
	if err := alignTenantApplicationsWithSourceHeadForTest(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(c.blobPath(wanted.Hash)); err != nil {
		t.Fatalf("remove content blob: %v", err)
	}
	scope := EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: left.ID}
	page, err := c.ChangesSince(context.Background(), tenant, scope, CompleteChangeCursor(anchor), 10)
	if err != nil {
		t.Fatalf("ChangesSince(left): %v", err)
	}
	if len(page.Changes) != 1 || page.Changes[0].Kind != ChangeUpsert || page.Changes[0].Object.ID != wanted.ID || page.Changes[0].Object.Revision != wanted.Revision {
		t.Fatalf("scoped changes = %+v, want only revised object", page.Changes)
	}

	rows, err := c.readDB.QueryContext(context.Background(), `
EXPLAIN QUERY PLAN
SELECT revision, sequence, kind, object_id
FROM source_driver_publication_changes INDEXED BY source_driver_publication_changes_range
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND scope_kind = ? AND presentation = ? AND scope_parent = ?
  AND scope_domain = ? AND scope_generation = ?
	  AND revision <= ?
	  AND (revision > ? OR (revision = ? AND sequence > ?))
ORDER BY revision, sequence LIMIT ?`,
		authority, publicationID[:], string(tenant), uint8(EnumerationContainer),
		uint8(PresentationFileProvider), left.ID[:], "", uint64(0),
		uint64(page.Head), uint64(anchor), uint64(anchor), CompleteChangeSequence, 11)
	if err != nil {
		t.Fatalf("EXPLAIN QUERY PLAN: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("Close query plan rows: %v", err)
		}
	}()
	plan := ""
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan query plan: %v", err)
		}
		plan += detail
	}
	if !strings.Contains(plan, "source_driver_publication_changes_range") {
		t.Fatalf("changes query plan = %q, want scoped change index", plan)
	}
}

func insertMetadataObjects(t *testing.T, c *Catalog, tenant TenantID, parent ObjectID, count int, template Object) {
	t.Helper()
	tx, err := c.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	head, _, err := revisionState(context.Background(), tx, tenant)
	if err != nil {
		t.Fatalf("revisionState: %v", err)
	}
	revision := head + 1
	if _, err := tx.Exec("UPDATE tenants SET head = ? WHERE tenant = ?", uint64(revision), string(tenant)); err != nil {
		t.Fatalf("advance head: %v", err)
	}
	version, err := tx.Prepare(`
INSERT INTO object_versions(
    tenant, object_id, parent_id, revision, metadata_revision, content_revision,
    name, name_key, kind, mode, size, hash, link_target, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("prepare object version: %v", err)
	}
	defer func() {
		if err := version.Close(); err != nil {
			t.Errorf("Close object version statement: %v", err)
		}
	}()
	current, err := tx.Prepare(`
INSERT INTO objects(
    tenant, object_id, parent_id, revision, metadata_revision, content_revision,
    name, name_key, kind, mode, size, hash, link_target, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("prepare object: %v", err)
	}
	defer func() {
		if err := current.Close(); err != nil {
			t.Errorf("Close current object statement: %v", err)
		}
	}()
	for index := range count {
		mutation := MutationID{0xee}
		mutation[8] = byte(index >> 8)
		mutation[9] = byte(index)
		mutation[10] = byte(index >> 16)
		object := template
		object.ID = objectFromMutation(mutation)
		object.Parent = parent
		object.Revision = revision
		object.MetadataRevision = revision
		object.Name = fmt.Sprintf("other-%05d", index)
		args := objectArgs(object, object.Name)
		if _, err := version.Exec(args...); err != nil {
			t.Fatalf("insert object version %d: %v", index, err)
		}
		if _, err := current.Exec(args...); err != nil {
			t.Fatalf("insert object %d: %v", index, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func createTestDirectory(t *testing.T, c *Catalog, tenant TenantID, parent ObjectID, name string) Object {
	t.Helper()
	object, err := c.Create(context.Background(), tenant, CreateSpec{
		Parent: parent, Name: name, Kind: KindDirectory, Mode: 0o755, Visibility: Visibility{Mount: true, FileProvider: true},
	})
	if err != nil {
		t.Fatalf("Create(directory %s): %v", name, err)
	}
	return object
}
