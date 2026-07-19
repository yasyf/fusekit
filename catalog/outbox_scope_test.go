package catalog

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
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
	mountRoot, err := c.CreateTenant(ctx, mustMutation(t), mountTenant, CaseSensitive, PresentMount)
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
	directory, err := c.Create(ctx, mustMutation(t), mountTenant, CreateSpec{
		Parent: mountRoot.ID, Name: "dir", Kind: KindDirectory,
		Visibility: Visibility{Mount: true},
	})
	if err != nil {
		t.Fatalf("Create(mount directory): %v", err)
	}
	mountOwner := InterestOwner{Presentation: PresentationMount, Domain: "mount-domain", Generation: 1}
	if _, err := c.AddInterest(ctx, mustMutation(t), mountTenant, directory.ID, mountOwner, directory.Revision); err != nil {
		t.Fatalf("AddInterest(mount): %v", err)
	}
	if record, err := c.ClaimConvergenceOutbox(ctx); err != nil || record != nil {
		t.Fatalf("mount-only outbox = %+v, %v", record, err)
	}

	fileProviderTenant, root := createTestTenant(t, c, "fp-after-mount", CaseSensitive)
	operation := mustMutation(t)
	owner := fileProviderInterestOwner("fp-after-mount")
	if _, err := c.AddInterest(ctx, operation, fileProviderTenant, root.ID, owner, root.Revision); err != nil {
		t.Fatalf("AddInterest(File Provider): %v", err)
	}
	batch, err := c.ClaimConvergenceOutbox(ctx)
	if err != nil {
		t.Fatalf("ClaimConvergenceOutbox: %v", err)
	}
	if batch == nil || len(batch.Commits) != 1 || batch.Commits[0].Tenant != causal.TenantID(fileProviderTenant) || batch.Change.SourceRevision != 1 ||
		batch.Change.OperationID != causal.OperationID(operation) || batch.Change.Origin != owner.Domain ||
		batch.Change.OriginGeneration != owner.Generation || batch.Change.Cause != causal.CauseOnDemand {
		t.Fatalf("File Provider outbox after mount-only work = %+v", batch)
	}
}

func TestSourceChangeRejectsRegressingRevisionWithDifferentIdentity(t *testing.T) {
	c := newTestCatalog(t)
	tenantA, rootA := createTestTenant(t, c, "source-order-a", CaseSensitive)
	tenantB, rootB := createTestTenant(t, c, "source-order-b", CaseSensitive)
	first := testSourceChange(10, 1)
	if err := commitSourceDirectory(t.Context(), c, tenantA, rootA, "first", first, []causal.TenantID{causal.TenantID(tenantA)}); err != nil {
		t.Fatalf("commit first source change: %v", err)
	}
	regressing := testSourceChange(9, 2)
	if err := commitSourceDirectory(t.Context(), c, tenantB, rootB, "regressing", regressing, []causal.TenantID{causal.TenantID(tenantB)}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("regressing source change = %v, want ErrInvalidTransition", err)
	}
	batch, err := c.ClaimConvergenceOutbox(context.Background())
	if err != nil {
		t.Fatalf("ClaimConvergenceOutbox(first): %v", err)
	}
	if batch == nil || batch.Change.ChangeID != first.ChangeID || batch.Change.OperationID != first.OperationID || len(batch.Commits) != 1 {
		t.Fatalf("outbox after regressing source change = %+v, want first change only", batch)
	}
	if err := c.SettleConvergenceOutbox(context.Background(), batch.Change.ChangeID); err != nil {
		t.Fatalf("SettleConvergenceOutbox(first): %v", err)
	}
	remaining, err := c.ClaimConvergenceOutbox(context.Background())
	if err != nil || remaining != nil {
		t.Fatalf("regressing source change left an outbox batch: %+v, %v", remaining, err)
	}
}

func TestConcurrentFirstSourceRevisionAcceptsExactlyOneChangeIdentity(t *testing.T) {
	c := newTestCatalog(t)
	tenantA, rootA := createTestTenant(t, c, "source-race-a", CaseSensitive)
	tenantB, rootB := createTestTenant(t, c, "source-race-b", CaseSensitive)
	changes := []causal.ChangeSet{testSourceChange(1, 1), testSourceChange(1, 2)}
	tenants := []TenantID{tenantA, tenantB}
	roots := []Object{rootA, rootB}
	start := make(chan struct{})
	errorsByIndex := make([]error, len(changes))
	var group sync.WaitGroup
	for index := range changes {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			errorsByIndex[index] = commitSourceDirectory(
				context.Background(), c, tenants[index], roots[index], fmt.Sprintf("race-%d", index),
				changes[index], []causal.TenantID{causal.TenantID(tenants[index])},
			)
		}()
	}
	close(start)
	group.Wait()
	succeeded := 0
	failedAsConflict := 0
	for _, err := range errorsByIndex {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrConflict), errors.Is(err, ErrInvalidTransition), errors.Is(err, ErrMutationConflict):
			failedAsConflict++
		default:
			t.Fatalf("concurrent source insertion failed unexpectedly: %v", err)
		}
	}
	if succeeded != 1 || failedAsConflict != 1 {
		t.Fatalf("concurrent source results = %+v", errorsByIndex)
	}
	batch, err := c.ClaimConvergenceOutbox(context.Background())
	if err != nil || batch == nil || len(batch.Commits) != 1 || batch.Change.SourceRevision != 1 {
		t.Fatalf("winning source batch = %+v, %v", batch, err)
	}
}

func TestRemoveInterestJournalsAndEmitsOnlyItsExactDomain(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "exact-interest-domain", CaseSensitive)
	ownerA := fileProviderInterestOwner("domain-a")
	ownerB := fileProviderInterestOwner("domain-b")
	interestA, err := c.AddInterest(ctx, mustMutation(t), tenant, root.ID, ownerA, root.Revision)
	if err != nil {
		t.Fatalf("AddInterest(A): %v", err)
	}
	interestB, err := c.AddInterest(ctx, mustMutation(t), tenant, root.ID, ownerB, root.Revision)
	if err != nil {
		t.Fatalf("AddInterest(B): %v", err)
	}
	removeOperation := mustMutation(t)
	removed, err := c.RemoveInterest(ctx, removeOperation, tenant, interestA.ID)
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
	var batch *causal.OutboxBatch
	for range 3 {
		batch, err = c.ClaimConvergenceOutbox(ctx)
		if err != nil {
			t.Fatalf("ClaimConvergenceOutbox: %v", err)
		}
		if batch == nil {
			t.Fatal("ClaimConvergenceOutbox returned no remove-interest batch")
		}
		if batch.Change.OperationID == causal.OperationID(removeOperation) {
			break
		}
		if err := c.SettleConvergenceOutbox(ctx, batch.Change.ChangeID); err != nil {
			t.Fatalf("SettleConvergenceOutbox(prior): %v", err)
		}
	}
	if batch.Change.OperationID != causal.OperationID(removeOperation) || len(batch.Commits) != 1 ||
		batch.Commits[0].Tenant != causal.TenantID(tenant) || batch.Commits[0].CatalogRevision != causal.CatalogRevision(removed.RemovedRevision) ||
		batch.Change.Origin != ownerA.Domain || batch.Change.OriginGeneration != ownerA.Generation || batch.Change.Cause != causal.CauseOnDemand {
		t.Fatalf("remove outbox batch = %+v", batch)
	}
	if err := c.SettleConvergenceOutbox(ctx, batch.Change.ChangeID); err != nil {
		t.Fatalf("SettleConvergenceOutbox(remove): %v", err)
	}
	remaining, err := c.ClaimConvergenceOutbox(ctx)
	if err != nil || remaining != nil {
		t.Fatalf("unexpected outbox after remove = %+v, %v", remaining, err)
	}
}

func testSourceChange(revision uint64, identity uint64) causal.ChangeSet {
	var changeID causal.ChangeID
	var operationID causal.OperationID
	binary.BigEndian.PutUint64(changeID[8:], identity)
	binary.BigEndian.PutUint64(operationID[8:], identity)
	return causal.ChangeSet{
		SourceAuthority: causal.SourceAuthorityID("test-source"),
		SourceRevision:  causal.Revision(revision), ChangeID: changeID, OperationID: operationID,
		Cause: causal.CauseDaemonWrite, AffectedKeys: []causal.LogicalKey{"config"},
	}
}

func commitSourceDirectory(
	ctx context.Context,
	c *Catalog,
	tenant TenantID,
	root Object,
	name string,
	change causal.ChangeSet,
	targets []causal.TenantID,
) error {
	_, err := c.testNamespaceMutation(ctx, MutationID(change.OperationID), tenant, MutationIntent{
		SourceID: "source-order-test",
		Origin:   CausalOrigin{Change: &change, Targets: targets},
		Create: &CreateMutation{Spec: CreateSpec{
			Parent: root.ID, Name: name, Kind: KindDirectory,
			Visibility: Visibility{FileProvider: true},
		}},
	})
	return err
}
