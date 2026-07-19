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

func TestCrossParentMoveJournalsOldDeleteAndNewUpsert(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "scope-move", CaseSensitive)
	left := createTestDirectory(t, c, tenant, root.ID, "left")
	right := createTestDirectory(t, c, tenant, root.ID, "right")
	file := createTestFile(t, c, tenant, left.ID, "file", "body")
	anchor := file.Revision
	moved, err := c.Revise(context.Background(), mustMutation(t), tenant, file.ID, RevisionSpec{
		Parent: right.ID, Name: file.Name, Mode: file.Mode, Convergence: file.Convergence, Visibility: file.Visibility,
	})
	if err != nil {
		t.Fatalf("Revise(move): %v", err)
	}
	leftChanges, err := c.ChangesSince(context.Background(), tenant, EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: left.ID}, CompleteChangeCursor(anchor), 10)
	if err != nil {
		t.Fatalf("ChangesSince(left): %v", err)
	}
	rightChanges, err := c.ChangesSince(context.Background(), tenant, EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: right.ID}, CompleteChangeCursor(anchor), 10)
	if err != nil {
		t.Fatalf("ChangesSince(right): %v", err)
	}
	if len(leftChanges.Changes) != 1 || leftChanges.Changes[0].Kind != ChangeDelete || leftChanges.Changes[0].Object.ID != file.ID || leftChanges.Changes[0].Object.Revision != file.Revision {
		t.Fatalf("left changes = %+v, want old-parent delete", leftChanges.Changes)
	}
	if len(rightChanges.Changes) != 1 || rightChanges.Changes[0].Kind != ChangeUpsert || rightChanges.Changes[0].Object.ID != file.ID || rightChanges.Changes[0].Object.Revision != moved.Revision {
		t.Fatalf("right changes = %+v, want new-parent upsert", rightChanges.Changes)
	}
}

func TestWorkingSetIsInterestDrivenAndContentScoped(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "scope-working", CaseSensitive)
	file := createTestFile(t, c, tenant, root.ID, "file", "one")
	owner := fileProviderInterestOwner("scope-working")
	interest, err := c.AddInterest(context.Background(), mustMutation(t), tenant, file.ID, owner, file.ContentRevision)
	if err != nil {
		t.Fatalf("AddInterest: %v", err)
	}
	working := workingSetScope(owner)
	page, err := c.Snapshot(context.Background(), tenant, working, interest.CreatedRevision, SnapshotCursor{}, 10)
	if err != nil {
		t.Fatalf("Snapshot(working): %v", err)
	}
	if len(page.Objects) != 1 || page.Objects[0].ID != file.ID {
		t.Fatalf("working snapshot = %+v, want interested file", page.Objects)
	}
	added, err := c.ChangesSince(context.Background(), tenant, working, CompleteChangeCursor(file.Revision), 10)
	if err != nil || len(added.Changes) != 1 || added.Changes[0].Kind != ChangeUpsert {
		t.Fatalf("interest changes = %+v, %v", added, err)
	}
	ref := stageTestContent(t, c, "two")
	revised, err := c.Revise(context.Background(), mustMutation(t), tenant, file.ID, RevisionSpec{
		Parent: file.Parent, Name: file.Name, Mode: file.Mode,
		Content: &ContentUpdate{Revision: 2, Ref: ref}, Convergence: Convergence{Desired: 2}, Visibility: file.Visibility,
	})
	if err != nil {
		t.Fatalf("Revise(content): %v", err)
	}
	contentChanges, err := c.ChangesSince(context.Background(), tenant, working, CompleteChangeCursor(interest.CreatedRevision), 10)
	if err != nil || len(contentChanges.Changes) != 1 || contentChanges.Changes[0].Object.Revision != revised.Revision {
		t.Fatalf("content changes = %+v, %v", contentChanges, err)
	}
	containerChanges, err := c.ChangesSince(context.Background(), tenant, EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}, CompleteChangeCursor(interest.CreatedRevision), 10)
	if err != nil || len(containerChanges.Changes) != 0 {
		t.Fatalf("content-only container changes = %+v, %v", containerChanges, err)
	}
	removed, err := c.RemoveInterest(context.Background(), mustMutation(t), tenant, interest.ID)
	if err != nil {
		t.Fatalf("RemoveInterest: %v", err)
	}
	removedChanges, err := c.ChangesSince(context.Background(), tenant, working, CompleteChangeCursor(revised.Revision), 10)
	if err != nil || len(removedChanges.Changes) != 1 || removedChanges.Changes[0].Kind != ChangeDelete {
		t.Fatalf("removed interest changes = %+v, %v", removedChanges, err)
	}
	page, err = c.Snapshot(context.Background(), tenant, working, removed.RemovedRevision, SnapshotCursor{}, 10)
	if err != nil || len(page.Objects) != 0 {
		t.Fatalf("removed working snapshot = %+v, %v", page.Objects, err)
	}
}

func TestContainerSnapshotUsesParentIndexAndNeverReadsContent(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "scope-index", CaseSensitive)
	left := createTestDirectory(t, c, tenant, root.ID, "left")
	right := createTestDirectory(t, c, tenant, root.ID, "right")
	wanted := createTestFile(t, c, tenant, left.ID, "wanted", "content")
	insertMetadataObjects(t, c, tenant, right.ID, 10_000, wanted)
	if err := os.Remove(c.blobPath(wanted.Hash)); err != nil {
		t.Fatalf("remove content blob: %v", err)
	}
	head, err := c.Head(context.Background(), tenant)
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
SELECT v.object_id FROM object_versions v
WHERE v.tenant = ? AND v.parent_id = ? AND v.object_id <> ?
ORDER BY v.object_id LIMIT 11`, string(tenant), left.ID[:], left.ID[:])
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
	if !strings.Contains(plan, "object_versions_container_snapshot") {
		t.Fatalf("container query plan = %q, want parent index", plan)
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
    name, name_key, kind, mode, size, hash, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
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
    name, name_key, kind, mode, size, hash, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
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
	object, err := c.Create(context.Background(), mustMutation(t), tenant, CreateSpec{
		Parent: parent, Name: name, Kind: KindDirectory, Mode: 0o755, Visibility: Visibility{Mount: true, FileProvider: true},
	})
	if err != nil {
		t.Fatalf("Create(directory %s): %v", name, err)
	}
	return object
}
