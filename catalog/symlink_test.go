package catalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
)

func TestSymlinkIsInlineContentAndNeverOpensAsBody(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "symlink-inline", CaseSensitive)
	target := "../settings.json"
	link, err := c.Create(ctx, tenant, CreateSpec{
		Parent: root.ID, Name: "settings", Kind: KindSymlink, Mode: 0o777,
		ContentRevision: 1, LinkTarget: target,
		Visibility: Visibility{Mount: true, FileProvider: true},
	})
	if err != nil {
		t.Fatalf("Create(symlink): %v", err)
	}
	digest := sha256.Sum256([]byte(target))
	if link.LinkTarget != target || link.Size != int64(len(target)) || link.Hash != ContentHash(digest) {
		t.Fatalf("symlink content = %+v", link)
	}
	ensureTestGeneration(t, c, tenant, 1)
	if _, err := c.OpenAt(ctx, testRetentionOwner, tenant, PresentationMount, 1, link.ID, link.Revision); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("OpenAt(symlink) = %v, want ErrInvalidObject", err)
	}
	var stages int
	if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM content_stages").Scan(&stages); err != nil {
		t.Fatal(err)
	}
	if stages != 0 {
		t.Fatalf("symlink staged body count = %d", stages)
	}
}

func TestSymlinkRejectsMalformedTargetsAndBodyUpdates(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "symlink-invalid", CaseSensitive)
	for _, target := range []string{"", "bad\x00target", string(make([]byte, 4097))} {
		if _, err := c.Create(context.Background(), tenant, CreateSpec{
			Parent: root.ID, Name: "bad", Kind: KindSymlink, Mode: 0o777,
			ContentRevision: 1, LinkTarget: target, Visibility: Visibility{Mount: true},
		}); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("Create(target length %d) = %v, want ErrInvalidObject", len(target), err)
		}
	}
	link, err := c.Create(context.Background(), tenant, CreateSpec{
		Parent: root.ID, Name: "good", Kind: KindSymlink, Mode: 0o777,
		ContentRevision: 1, LinkTarget: "target", Visibility: Visibility{Mount: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	ref := stageTestContent(t, c, "replacement")
	if _, err := c.Revise(context.Background(), tenant, link.ID, RevisionSpec{
		Parent: link.Parent, Name: link.Name, Mode: link.Mode,
		Content: &ContentUpdate{Revision: 2, Ref: ref}, Visibility: link.Visibility,
	}); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("Revise(symlink body) = %v, want ErrInvalidObject", err)
	}
}
