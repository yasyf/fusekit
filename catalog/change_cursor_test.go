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
	revision := file.Revision + 1
	scope := EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}

	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	for sequence := range uint32(10_000) {
		if err := writeChange(ctx, tx, tenant, revision, scope, sequence, ChangeUpsert, file.ID, file.Revision); err != nil {
			_ = tx.Rollback()
			t.Fatalf("writeChange(%d): %v", sequence, err)
		}
	}
	if _, err := tx.ExecContext(ctx, "UPDATE tenants SET head = ? WHERE tenant = ?", uint64(revision), string(tenant)); err != nil {
		_ = tx.Rollback()
		t.Fatalf("advance head: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	cursor := CompleteChangeCursor(file.Revision)
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
	if seen != 10_000 {
		t.Fatalf("changes = %d, want 10000", seen)
	}

	if _, err := maintainTestUntilIdle(ctx, c, tenant, revision); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	partial, err := c.ChangesSince(ctx, tenant, scope, ChangeCursor{Revision: revision, Sequence: 4_999}, 10)
	if err != nil || len(partial.Changes) != 10 || partial.Changes[0].Sequence != 5_000 {
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
