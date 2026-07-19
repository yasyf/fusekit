package catalog

import (
	"context"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestMountOnlyMutationsAllocateNoFileProviderOutbox(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	mountTenant, err := NewTenantID("mount-only-outbox")
	if err != nil {
		t.Fatalf("NewTenantID(mount): %v", err)
	}
	mountRoot, err := c.CreateTenant(ctx, mountTenant, CaseSensitive, PresentMount)
	if err != nil {
		t.Fatalf("CreateTenant(mount): %v", err)
	}
	metadata, err := c.Tenant(ctx, mountTenant)
	if err != nil {
		t.Fatalf("Tenant(mount): %v", err)
	}
	if metadata.Presentations != PresentMount || !mountRoot.Visibility.Mount || mountRoot.Visibility.FileProvider {
		t.Fatalf("mount tenant metadata/root = %+v / %+v", metadata, mountRoot.Visibility)
	}
	directory, err := c.Create(ctx, mountTenant, CreateSpec{
		Parent: mountRoot.ID, Name: "dir", Kind: KindDirectory,
		Visibility: Visibility{Mount: true},
	})
	if err != nil {
		t.Fatalf("Create(mount directory): %v", err)
	}
	mountOwner := InterestOwner{Presentation: PresentationMount, Domain: "mount-domain", Generation: 1}
	if _, err := c.AddInterest(
		ctx, mountTenant, mustCatalogHead(t, c, mountTenant),
		directory.ID, mountOwner, directory.Revision,
	); err != nil {
		t.Fatalf("AddInterest(mount): %v", err)
	}
	if record, err := c.ClaimConvergenceOutbox(ctx); err != nil || record != nil {
		t.Fatalf("mount-only outbox = %+v, %v", record, err)
	}

	fileProviderTenant, root := createTestTenant(t, c, "fp-after-mount", CaseSensitive)
	owner := fileProviderInterestOwner("fp-after-mount")
	if _, err := c.AddInterest(
		ctx, fileProviderTenant, mustCatalogHead(t, c, fileProviderTenant),
		root.ID, owner, root.Revision,
	); err != nil {
		t.Fatalf("AddInterest(File Provider): %v", err)
	}
	batch, err := claimConvergenceOutboxForTest(ctx, c)
	if err != nil {
		t.Fatalf("ClaimConvergenceOutbox: %v", err)
	}
	if batch == nil || len(batch.Commits) != 1 || batch.Commits[0].Tenant != causal.TenantID(fileProviderTenant) || batch.Change.SourceRevision != 1 ||
		batch.Change.OperationID == (causal.OperationID{}) || batch.Change.Origin != owner.Domain ||
		batch.Change.OriginGeneration != owner.Generation || batch.Change.Cause != causal.CauseOnDemand {
		t.Fatalf("File Provider outbox after mount-only work = %+v", batch)
	}
}

func TestRemoveInterestJournalsAndEmitsOnlyItsExactDomain(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "exact-interest-domain", CaseSensitive)
	ownerA := fileProviderInterestOwner("domain-a")
	ownerB := fileProviderInterestOwner("domain-b")
	interestA, err := c.AddInterest(
		ctx, tenant, mustCatalogHead(t, c, tenant), root.ID, ownerA, root.Revision,
	)
	if err != nil {
		t.Fatalf("AddInterest(A): %v", err)
	}
	interestB, err := c.AddInterest(
		ctx, tenant, mustCatalogHead(t, c, tenant), root.ID, ownerB, root.Revision,
	)
	if err != nil {
		t.Fatalf("AddInterest(B): %v", err)
	}
	removed, err := c.RemoveInterest(
		ctx, tenant, mustCatalogHead(t, c, tenant), interestA.ID,
	)
	if err != nil {
		t.Fatalf("RemoveInterest(A): %v", err)
	}
	changesA, err := c.ChangesSince(ctx, tenant, workingSetScope(ownerA), CompleteChangeCursor(interestB.CreatedRevision), 10)
	if err != nil || len(changesA.Changes) != 1 || changesA.Changes[0].Kind != ChangeDelete {
		t.Fatalf("domain A removal changes = %+v, %v", changesA.Changes, err)
	}
	changesB, err := c.ChangesSince(ctx, tenant, workingSetScope(ownerB), CompleteChangeCursor(interestB.CreatedRevision), 10)
	if err != nil || len(changesB.Changes) != 0 {
		t.Fatalf("domain B removal changes = %+v, %v", changesB.Changes, err)
	}
	pageB, err := c.Snapshot(ctx, tenant, workingSetScope(ownerB), removed.RemovedRevision, SnapshotCursor{}, 10)
	if err != nil || len(pageB.Objects) != 1 || pageB.Objects[0].ID != root.ID {
		t.Fatalf("domain B working set = %+v, %v", pageB.Objects, err)
	}
	var batch *causal.OutboxPage
	for range 3 {
		batch, err = claimConvergenceOutboxForTest(ctx, c)
		if err != nil {
			t.Fatalf("ClaimConvergenceOutbox: %v", err)
		}
		if batch == nil {
			t.Fatal("ClaimConvergenceOutbox returned no remove-interest batch")
		}
		if len(batch.Commits) == 1 &&
			batch.Commits[0].CatalogRevision == causal.CatalogRevision(removed.RemovedRevision) {
			break
		}
		if err := c.SettleConvergenceOutbox(ctx, *batch.Settlement); err != nil {
			t.Fatalf("SettleConvergenceOutbox(prior): %v", err)
		}
	}
	if batch.Change.OperationID == (causal.OperationID{}) || len(batch.Commits) != 1 ||
		batch.Commits[0].Tenant != causal.TenantID(tenant) || batch.Commits[0].CatalogRevision != causal.CatalogRevision(removed.RemovedRevision) ||
		batch.Change.Origin != ownerA.Domain || batch.Change.OriginGeneration != ownerA.Generation || batch.Change.Cause != causal.CauseOnDemand {
		t.Fatalf("remove outbox batch = %+v", batch)
	}
	if err := c.SettleConvergenceOutbox(ctx, *batch.Settlement); err != nil {
		t.Fatalf("SettleConvergenceOutbox(remove): %v", err)
	}
	remaining, err := c.ClaimConvergenceOutbox(ctx)
	if err != nil || remaining != nil {
		t.Fatalf("unexpected outbox after remove = %+v, %v", remaining, err)
	}
}
