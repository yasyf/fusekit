package catalog

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestCompactionRetainsFloorSnapshotAndOpenHandleVersion(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "compact-handle", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "target", "old-content")
	source := createTestFile(t, c, tenant, root.ID, "temp", "new-content")
	handle, err := c.OpenAt(context.Background(), tenant, PresentationFileProvider, 1, target.ID, target.Revision)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	result, err := c.Replace(context.Background(), mustMutation(t), tenant, source.ID, target.ID)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if err := c.Compact(context.Background(), tenant, result.Revision); err != nil {
		t.Fatalf("Compact(pinned): %v", err)
	}
	oldPath := c.blobPath(target.Hash)
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("pinned old blob missing: %v", err)
	}
	content, err := io.ReadAll(handle)
	if err != nil || string(content) != "old-content" {
		t.Fatalf("pinned read = %q, %v", content, err)
	}
	page, err := c.Snapshot(context.Background(), tenant, EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}, result.Revision, SnapshotCursor{}, 20)
	if err != nil {
		t.Fatalf("floor Snapshot: %v", err)
	}
	if len(page.Objects) != 1 {
		t.Fatalf("floor snapshot rows = %d, want replacement", len(page.Objects))
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("Close(handle): %v", err)
	}
	if err := c.Compact(context.Background(), tenant, result.Revision); err != nil {
		t.Fatalf("Compact(unpinned): %v", err)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unpinned old blob stat = %v, want absent", err)
	}
}

func TestRestartRecoversStaleHandlePin(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	tenant, root := createTestTenant(t, c, "crash-pin", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "target", "old")
	source := createTestFile(t, c, tenant, root.ID, "temp", "new")
	ensureTestGeneration(t, c, tenant, 7)
	handle, err := c.OpenAt(ctx, tenant, PresentationFileProvider, 7, target.ID, target.Revision)
	if err != nil {
		t.Fatalf("Open handle: %v", err)
	}
	result, err := c.Replace(ctx, mustMutation(t), tenant, source.ID, target.ID)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if err := handle.file.Close(); err != nil {
		t.Fatalf("simulate descriptor loss: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close catalog: %v", err)
	}
	c, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(restart): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	var closed bool
	if err := c.db.QueryRow("SELECT closed FROM handles WHERE handle_id = ?", handle.Handle.ID[:]).Scan(&closed); err != nil {
		t.Fatalf("read recovered handle: %v", err)
	}
	if !closed {
		t.Fatal("stale handle remained open after owner restart")
	}
	if err := c.Compact(ctx, tenant, result.Revision); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if _, err := os.Stat(c.blobPath(target.Hash)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("crash-pinned blob stat = %v, want collected", err)
	}
}

func TestStageContentDeduplicatesWithoutReplacingInode(t *testing.T) {
	c := newTestCatalog(t)
	first, err := c.StageContent(context.Background(), &patternReader{remaining: 16384})
	if err != nil {
		t.Fatalf("StageContent(first): %v", err)
	}
	before, err := os.Stat(c.blobPath(first.Hash))
	if err != nil {
		t.Fatalf("Stat(before): %v", err)
	}
	second, err := c.StageContent(context.Background(), &patternReader{remaining: 16384})
	if err != nil {
		t.Fatalf("StageContent(second): %v", err)
	}
	after, err := os.Stat(c.blobPath(first.Hash))
	if err != nil {
		t.Fatalf("Stat(after): %v", err)
	}
	if first.Stage == second.Stage || first.Hash != second.Hash || first.Size != second.Size || !os.SameFile(before, after) {
		t.Fatalf("dedupe replaced immutable inode: first=%+v second=%+v", first, second)
	}
}

func TestTamperedBlobFailsCreateAndOpenIntegrity(t *testing.T) {
	c := newTestCatalog(t)
	ref := stageTestContent(t, c, "abcd")
	path := c.blobPath(ref.Hash)
	if err := os.WriteFile(path, []byte("wxyz"), 0o600); err != nil {
		t.Fatalf("tamper blob: %v", err)
	}
	tenant, root := createTestTenant(t, c, "tamper-create", CaseSensitive)
	if _, err := c.Create(context.Background(), mustMutation(t), tenant, fileSpec(root.ID, "bad", ref, 1)); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("Create tampered ref err = %v, want ErrIntegrity", err)
	}

	clean := stageTestContent(t, c, "clean")
	object, err := c.Create(context.Background(), mustMutation(t), tenant, fileSpec(root.ID, "clean", clean, 1))
	if err != nil {
		t.Fatalf("Create clean: %v", err)
	}
	if err := os.WriteFile(c.blobPath(clean.Hash), []byte("dirty"), 0o600); err != nil {
		t.Fatalf("tamper clean blob: %v", err)
	}
	if _, err := c.OpenAt(context.Background(), tenant, PresentationFileProvider, 1, object.ID, object.Revision); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("Open tampered blob err = %v, want ErrIntegrity", err)
	}
}
