package catalog

import (
	"context"
	"testing"
)

func TestReplaceTransfersTargetOnlyInterestToCanonicalSource(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "replace-target-interest", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "settings.json", "old")
	source := createTestFile(t, c, tenant, root.ID, ".settings.tmp", "new")
	owner := fileProviderInterestOwner("replace-target-interest")
	interest, err := c.AddInterest(
		ctx, tenant, mustCatalogHead(t, c, tenant), target.ID, owner, target.ContentRevision,
	)
	if err != nil {
		t.Fatalf("AddInterest(target): %v", err)
	}
	result, err := c.Replace(ctx, tenant, source.ID, target.ID)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	targetInterests, err := c.Interests(ctx, tenant, target.ID)
	if err != nil {
		t.Fatalf("Interests(target): %v", err)
	}
	sourceInterests, err := c.Interests(ctx, tenant, source.ID)
	if err != nil {
		t.Fatalf("Interests(source): %v", err)
	}
	if len(targetInterests) != 0 || len(sourceInterests) != 1 || sourceInterests[0].Owner != interest.Owner ||
		sourceInterests[0].DesiredRevision != interest.DesiredRevision || sourceInterests[0].CreatedRevision != result.Revision {
		t.Fatalf("transferred interests target=%#v source=%#v", targetInterests, sourceInterests)
	}
	page, err := c.ChangesSince(ctx, tenant,
		workingSetScope(owner),
		CompleteChangeCursor(interest.CreatedRevision), 10)
	if err != nil {
		t.Fatalf("ChangesSince: %v", err)
	}
	if len(page.Changes) != 2 || page.Changes[0].Kind != ChangeDelete || page.Changes[0].Object.ID != target.ID ||
		page.Changes[1].Kind != ChangeUpsert || page.Changes[1].Object.ID != source.ID ||
		page.Changes[0].Revision != result.Revision || page.Changes[1].Revision != result.Revision {
		t.Fatalf("working-set replace changes = %#v", page.Changes)
	}
}

func TestReplaceDeduplicatesOverlappingInterestOwners(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "replace-interest-dedupe", CaseSensitive)
	target := createTestFile(t, c, tenant, root.ID, "settings.json", "old")
	source := createTestFile(t, c, tenant, root.ID, ".settings.tmp", "new")
	owner := fileProviderInterestOwner("replace-interest-dedupe")
	if _, err := c.AddInterest(
		ctx, tenant, mustCatalogHead(t, c, tenant), source.ID, owner, 1,
	); err != nil {
		t.Fatalf("AddInterest(source): %v", err)
	}
	if _, err := c.AddInterest(
		ctx, tenant, mustCatalogHead(t, c, tenant), target.ID, owner, 3,
	); err != nil {
		t.Fatalf("AddInterest(target): %v", err)
	}
	if _, err := c.Replace(ctx, tenant, source.ID, target.ID); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	interests, err := c.Interests(ctx, tenant, source.ID)
	if err != nil {
		t.Fatalf("Interests(source): %v", err)
	}
	if len(interests) != 1 || interests[0].Owner != owner || interests[0].DesiredRevision != 3 {
		t.Fatalf("deduplicated interests = %#v", interests)
	}
	targetInterests, err := c.Interests(ctx, tenant, target.ID)
	if err != nil || len(targetInterests) != 0 {
		t.Fatalf("target interests = %#v, %v", targetInterests, err)
	}
}
