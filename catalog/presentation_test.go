package catalog

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
)

func TestUnpresentedObjectPublishesOnlyThroughAtomicReplace(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "hidden-replace", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "settings.json", "old")
	anchor, err := c.Head(ctx, tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	ref := stageTestContent(t, c, "new")
	spec := fileSpec(root.ID, ".settings.json.tmp", ref, 1)
	spec.Visibility = Visibility{}
	source, err := c.Create(ctx, mustMutation(t), tenant, spec)
	if err != nil {
		t.Fatalf("Create(hidden): %v", err)
	}
	if source.Visibility.FileProvider {
		t.Fatal("hidden source is presented")
	}
	if _, err := c.Lookup(ctx, tenant, PresentationFileProvider, source.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Lookup(hidden by ID) err = %v, want ErrNotFound", err)
	}
	if _, err := c.lookupAnyObject(ctx, tenant, source.ID); err != nil {
		t.Fatalf("internal lookup(hidden by ID): %v", err)
	}
	if _, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, spec.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LookupName(hidden) err = %v, want ErrNotFound", err)
	}
	scope := EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}
	page, err := c.Snapshot(ctx, tenant, scope, 0, SnapshotCursor{}, 20)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if containsObject(page.Objects, source.ID) {
		t.Fatal("snapshot contains hidden source")
	}
	changes, err := c.ChangesSince(ctx, tenant, scope, CompleteChangeCursor(anchor), 20)
	if err != nil {
		t.Fatalf("ChangesSince(hidden create): %v", err)
	}
	if len(changes.Changes) != 0 || changes.Next != CompleteChangeCursor(source.Revision) {
		t.Fatalf("hidden changes = %+v, want no deltas through %d", changes, source.Revision)
	}

	result, err := c.Replace(ctx, mustMutation(t), tenant, source.ID, target.ID)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if result.Source.ID != source.ID || !result.Source.Visibility.FileProvider || !result.Target.Tombstone {
		t.Fatalf("replace result = %+v", result)
	}
	bound, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, target.Name)
	if err != nil {
		t.Fatalf("LookupName(replacement): %v", err)
	}
	if bound.ID != source.ID {
		t.Fatalf("replacement ID = %s, want source %s", bound.ID, source.ID)
	}
	changes, err = c.ChangesSince(ctx, tenant, scope, CompleteChangeCursor(source.Revision), 20)
	if err != nil {
		t.Fatalf("ChangesSince(replace): %v", err)
	}
	if len(changes.Changes) != 2 ||
		changes.Changes[0].Kind != ChangeDelete || changes.Changes[0].Object.ID != target.ID ||
		changes.Changes[1].Kind != ChangeUpsert || changes.Changes[1].Object.ID != source.ID ||
		changes.Changes[0].Revision != result.Revision || changes.Changes[1].Revision != result.Revision {
		t.Fatalf("replace changes = %+v", changes.Changes)
	}
}

func TestRestartLeavesHiddenObjectAndCanonicalBindingSeparated(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	tenant, root := createTestTenant(t, c, "hidden-restart", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "settings.json", "old")
	ref := stageTestContent(t, c, "new")
	spec := fileSpec(root.ID, ".settings.json.tmp", ref, 1)
	spec.Visibility = Visibility{}
	source, err := c.Create(ctx, mustMutation(t), tenant, spec)
	if err != nil {
		t.Fatalf("Create(hidden): %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(restart): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	bound, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, target.Name)
	if err != nil {
		t.Fatalf("LookupName(canonical): %v", err)
	}
	if bound.ID != target.ID {
		t.Fatalf("canonical binding = %s, want %s", bound.ID, target.ID)
	}
	hidden, err := c.lookupAnyObject(ctx, tenant, source.ID)
	if err != nil {
		t.Fatalf("Lookup(hidden): %v", err)
	}
	if hidden.Visibility.FileProvider {
		t.Fatal("hidden object became presented after restart")
	}
}

func TestReplacePublishesFinalMetadataAndContentInOneRevision(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "streamed-replace", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "settings.json", "old")
	staged := stageTestContent(t, c, "placeholder")
	sourceSpec := fileSpec(root.ID, ".settings.json.tmp", staged, 1)
	sourceSpec.Visibility = Visibility{}
	source, err := c.Create(ctx, mustMutation(t), tenant, sourceSpec)
	if err != nil {
		t.Fatalf("Create(hidden source): %v", err)
	}
	head, err := c.Head(ctx, tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	content := stageTestContent(t, c, "final")
	name := "renamed.json"
	mode := uint32(0o600)
	presented := true
	result, err := c.testNamespaceMutation(ctx, mustMutation(t), tenant, MutationIntent{
		SourceID: "fileprovider",
		Replace: &ReplaceMutation{
			Source: source.ID, Target: target.ID,
			Parent: &root.ID, Name: &name, Mode: &mode, Visibility: &Visibility{FileProvider: presented},
			Content: &ContentUpdate{Revision: source.ContentRevision + 1, Ref: content},
		},
	})
	if err != nil {
		t.Fatalf("Replace(streamed): %v", err)
	}
	if result.Mutation.Revision != head+1 || result.Primary.Revision != head+1 || result.Secondary.Revision != head+1 {
		t.Fatalf("replace revisions = mutation %d, source %d, target %d, want %d",
			result.Mutation.Revision, result.Primary.Revision, result.Secondary.Revision, head+1)
	}
	if result.Primary.ID != source.ID || result.Primary.Name != name || result.Primary.Mode != mode ||
		!result.Primary.Visibility.FileProvider || result.Primary.ContentRevision != source.ContentRevision+1 ||
		result.Primary.Hash != content.Hash || result.Primary.Size != content.Size {
		t.Fatalf("published source = %+v", result.Primary)
	}
	if result.Secondary.ID != target.ID || !result.Secondary.Tombstone {
		t.Fatalf("replaced target = %+v", result.Secondary)
	}
	if _, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, target.Name); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old binding err = %v, want ErrNotFound", err)
	}
	bound, err := c.LookupName(ctx, tenant, PresentationFileProvider, root.ID, name)
	if err != nil || bound.ID != source.ID {
		t.Fatalf("final binding = %+v, %v", bound, err)
	}
	handle, err := c.OpenAt(ctx, tenant, PresentationFileProvider, 1, source.ID, result.Primary.Revision)
	if err != nil {
		t.Fatalf("OpenAt(final): %v", err)
	}
	defer func() {
		if err := handle.Close(); err != nil {
			t.Errorf("Close handle: %v", err)
		}
	}()
	payload, err := io.ReadAll(handle)
	if err != nil {
		t.Fatalf("ReadAll(final): %v", err)
	}
	if string(payload) != "final" {
		t.Fatalf("final content = %q", payload)
	}
	scope := EnumerationScope{Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: root.ID}
	changes, err := c.ChangesSince(ctx, tenant, scope, CompleteChangeCursor(head), 10)
	if err != nil {
		t.Fatalf("ChangesSince(replace): %v", err)
	}
	if changes.Next != CompleteChangeCursor(head+1) || len(changes.Changes) != 2 ||
		changes.Changes[0].Revision != head+1 || changes.Changes[0].Kind != ChangeDelete || changes.Changes[0].Object.ID != target.ID ||
		changes.Changes[1].Revision != head+1 || changes.Changes[1].Kind != ChangeUpsert || changes.Changes[1].Object.ID != source.ID {
		t.Fatalf("atomic replace changes = %+v", changes)
	}
}

func containsObject(objects []Object, id ObjectID) bool {
	for _, object := range objects {
		if object.ID == id {
			return true
		}
	}
	return false
}
