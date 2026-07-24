package catalog

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestChangesSinceBoundsRowsWithinOneRevisionAndReplaysCursor(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "change-cursor", CaseSensitive)
	file := createTestFile(t, c, tenant, root.ID, "file", "body")
	revision := file.Revision
	scope := EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	var authority string
	var publication []byte
	if err := tx.QueryRowContext(ctx, `
SELECT application.source_authority, application.source_publication_id
FROM tenant_activations activation
JOIN tenant_applications application
  ON application.tenant_id = activation.tenant_id
 AND application.generation = activation.active_generation
 AND application.staged_view_id = activation.active_view_id
WHERE activation.tenant_id = ?`, string(tenant)).Scan(&authority, &publication); err != nil {
		_ = tx.Rollback()
		t.Fatalf("active publication: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM source_driver_publication_changes
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND scope_kind = ? AND presentation = ? AND scope_parent = ?
  AND scope_domain = '' AND scope_generation = 0`, authority, publication, string(tenant),
		uint8(scope.Kind), uint8(scope.Presentation), scope.Parent[:]); err != nil {
		_ = tx.Rollback()
		t.Fatalf("clear publication changes: %v", err)
	}
	changeCount := testScaleCount(10_000)
	for sequence := range uint32(changeCount) {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_publication_changes(
    source_authority, publication_id, tenant, revision, scope_kind, presentation,
    scope_parent, scope_domain, scope_generation, sequence, kind, object_id, object_revision
) VALUES (?, ?, ?, ?, ?, ?, ?, '', 0, ?, ?, ?, ?)`, authority, publication, string(tenant),
			uint64(revision), uint8(scope.Kind), uint8(scope.Presentation), scope.Parent[:], sequence,
			uint8(ChangeUpsert), file.ID[:], uint64(file.Revision)); err != nil {
			_ = tx.Rollback()
			t.Fatalf("insert publication change %d: %v", sequence, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	cursor := CompleteChangeCursor(file.Revision - 1)
	first, err := c.ChangesSince(ctx, tenant, scope, cursor, 137)
	if err != nil {
		t.Fatalf("ChangesSince(first): %v", err)
	}
	replayed, err := c.ChangesSince(ctx, tenant, scope, cursor, 137)
	if err != nil {
		t.Fatalf("ChangesSince(replay): %v", err)
	}
	if !reflect.DeepEqual(first, replayed) {
		t.Fatalf("same cursor produced different pages\nfirst=%#v\nreplay=%#v", first, replayed)
	}

	seen := 0
	for {
		page, err := c.ChangesSince(ctx, tenant, scope, cursor, 137)
		if err != nil {
			t.Fatalf("ChangesSince(%#v): %v", cursor, err)
		}
		if len(page.Changes) > 137 {
			t.Fatalf("page rows = %d, want <= 137", len(page.Changes))
		}
		seen += len(page.Changes)
		if page.Complete {
			if page.Next != CompleteChangeCursor(revision) {
				t.Fatalf("complete cursor = %#v, want %#v", page.Next, CompleteChangeCursor(revision))
			}
			break
		}
		last := page.Changes[len(page.Changes)-1]
		if page.Next != (ChangeCursor{Revision: last.Revision, Sequence: last.Sequence}) {
			t.Fatalf("next = %#v, last = %#v", page.Next, last)
		}
		cursor = page.Next
	}
	if seen != changeCount {
		t.Fatalf("changes = %d, want %d", seen, changeCount)
	}

	if _, err := maintainTestTenantUntilIdle(ctx, c, tenant, revision); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	middle := uint32(changeCount / 2)
	partial, err := c.ChangesSince(ctx, tenant, scope, ChangeCursor{Revision: revision, Sequence: middle - 1}, 10)
	if err != nil || len(partial.Changes) != 10 || partial.Changes[0].Sequence != middle {
		t.Fatalf("floor partial page = %#v, %v", partial, err)
	}
}

func TestWriteChangeRejectsCompleteCursorSentinel(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "change-sentinel", CaseSensitive)
	file := createTestFile(t, c, tenant, root.ID, "file", "body")
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	err = writeChange(ctx, tx, tenant, file.Revision+1,
		EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID},
		CompleteChangeSequence, ChangeUpsert, file.ID, file.Revision)
	if !errors.Is(err, ErrIntegrity) {
		t.Fatalf("writeChange(sentinel) = %v, want ErrIntegrity", err)
	}
}
