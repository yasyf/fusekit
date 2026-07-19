package catalog

import (
	"context"
	"errors"
	"io"
	"testing"
)

func TestMetadataOnlyRevisionPreservesContentWithoutStage(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "metadata-only", CaseSensitive)
	before := createTestFile(t, c, tenant, root.ID, "before", "content")
	after, err := c.Revise(ctx, mustMutation(t), tenant, before.ID, RevisionSpec{
		Parent: before.Parent, Name: "after", Mode: 0o400, Convergence: before.Convergence, Visibility: before.Visibility,
	})
	if err != nil {
		t.Fatalf("Revise: %v", err)
	}
	if after.Hash != before.Hash || after.Size != before.Size || after.ContentRevision != before.ContentRevision {
		t.Fatalf("metadata-only revision changed content: before=%+v after=%+v", before, after)
	}
	if after.MetadataRevision != after.Revision || after.MetadataRevision == before.MetadataRevision {
		t.Fatalf("metadata revision = %d, object revision = %d, before = %d",
			after.MetadataRevision, after.Revision, before.MetadataRevision)
	}
	if _, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, before.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old name err = %v, want ErrNotFound", err)
	}
	bound, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, after.Name)
	if err != nil || bound.ID != before.ID {
		t.Fatalf("new binding = %+v, %v", bound, err)
	}
}

func TestOpenAtReturnsCapturedRevisionAfterReplace(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "open-at-replace", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "settings.json", "old")
	captured, err := c.Lookup(ctx, tenant, PresentationFileProvider, target.ID)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	source := createTestFile(t, c, tenant, root.ID, ".settings.json.tmp", "new")
	if _, err := c.Replace(ctx, mustMutation(t), tenant, source.ID, target.ID); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if _, err := c.Lookup(ctx, tenant, PresentationFileProvider, target.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(replaced target) err = %v, want ErrNotFound", err)
	}
	handle, err := c.OpenAt(ctx, tenant, PresentationFileProvider, 1, captured.ID, captured.Revision)
	if err != nil {
		t.Fatalf("OpenAt(captured): %v", err)
	}
	defer func() {
		if err := handle.Close(); err != nil {
			t.Errorf("Close handle: %v", err)
		}
	}()
	content, err := io.ReadAll(handle)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(content) != "old" || handle.Object.Revision != captured.Revision {
		t.Fatalf("captured open = %q at %d, want old at %d", content, handle.Object.Revision, captured.Revision)
	}
}
