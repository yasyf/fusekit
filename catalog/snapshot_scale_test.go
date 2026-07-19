package catalog

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSnapshotPagesTenThousandMetadataRowsWithoutContentReads(t *testing.T) {
	c := newTestCatalog(t)
	tenant, _ := createTestTenant(t, c, "scale", CaseSensitive)
	ref := stageTestContent(t, c, "shared-content")
	tx, err := c.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec("UPDATE tenants SET head = 2 WHERE tenant = ?", string(tenant)); err != nil {
		t.Fatalf("advance head: %v", err)
	}
	version, err := tx.Prepare(`
INSERT INTO object_versions(
    tenant, object_id, parent_id, revision, metadata_revision, content_revision,
    name, name_key, kind, mode, size, hash, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		t.Fatalf("prepare version: %v", err)
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
		t.Fatalf("prepare current: %v", err)
	}
	defer func() {
		if err := current.Close(); err != nil {
			t.Errorf("Close current object statement: %v", err)
		}
	}()
	root, err := c.Root(context.Background(), tenant)
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	for i := 0; i < 10_000; i++ {
		mutation := MutationID{1}
		mutation[8] = byte(i >> 8)
		mutation[9] = byte(i)
		mutation[10] = byte(i >> 16)
		id := objectFromMutation(mutation)
		name := fmt.Sprintf("metadata-%05d", i)
		obj := Object{
			Tenant: tenant, ID: id, Parent: root.ID, Revision: 2,
			MetadataRevision: 2, ContentRevision: 1, Name: name,
			Kind: KindFile, Mode: 0o400, Size: ref.Size, Hash: ref.Hash,
			Convergence: Convergence{Desired: 1}, Visibility: Visibility{Mount: true, FileProvider: true},
		}
		args := objectArgs(obj, name)
		if _, err := version.Exec(args...); err != nil {
			t.Fatalf("insert version %d: %v", i, err)
		}
		if _, err := current.Exec(args...); err != nil {
			t.Fatalf("insert current %d: %v", i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	cursor := SnapshotCursor{}
	total := 0
	started := time.Now()
	for {
		page, err := c.Snapshot(context.Background(), tenant, EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}, 2, cursor, 777)
		if err != nil {
			t.Fatalf("Snapshot: %v", err)
		}
		total += len(page.Objects)
		for _, object := range page.Objects {
			if object.Kind == KindFile && (object.Hash != ref.Hash || object.Size != ref.Size) {
				t.Fatalf("metadata content ref changed for %s", object.ID)
			}
		}
		if page.Next == nil {
			break
		}
		cursor = *page.Next
	}
	if total != 10_000 {
		t.Fatalf("snapshot rows = %d, want 10000 children", total)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("metadata-only snapshot took %s, want <= 10s", elapsed)
	}
}
