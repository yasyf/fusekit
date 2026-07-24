package catalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestMutationCompactionExpiresReplayOnlyAfterLivePinCloses(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createMountMutationTenant(t, c, "compact-mutation-pin")
	ref := stageTestContent(t, c, "payload")
	intent := testMountCreateIntent(root.ID, "file", ref)
	prepared, err := c.BeginMutation(ctx, tenant, mustCatalogHead(t, c, tenant), intent)
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.finishTestNamespaceMutation(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	pin, err := c.PinMutation(ctx, testRetentionOwner, tenant, prepared.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintainTestTenantUntilIdle(ctx, c, tenant, result.Mutation.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutation(ctx, tenant, prepared.OperationID); err != nil {
		t.Fatalf("pinned mutation disappeared: %v", err)
	}
	if err := c.CloseMutationPin(ctx, pin); err != nil {
		t.Fatal(err)
	}
	compacted, err := maintainTestTenantUntilIdle(ctx, c, tenant, result.Mutation.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if compacted.Mutations == 0 {
		t.Fatal("closed mutation pin retained the journal")
	}
	for name, call := range map[string]func() error{
		"mutation": func() error {
			_, err := c.Mutation(ctx, tenant, prepared.OperationID)
			return err
		},
		"prepared": func() error {
			_, err := c.PreparedMutation(ctx, tenant, prepared.OperationID)
			return err
		},
		"content": func() error {
			_, err := c.OpenMutationContent(ctx, tenant, prepared.OperationID)
			return err
		},
		"begin": func() error {
			_, err := c.BeginMutation(ctx, tenant, prepared.ExpectedHead, intent)
			return err
		},
	} {
		if err := call(); !errors.Is(err, ErrMutationExpired) {
			t.Fatalf("%s after compaction = %v, want ErrMutationExpired", name, err)
		}
	}
}

func TestMutationCompactionExpiresReplayWhileRetainingSnapshotContent(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createMountMutationTenant(t, c, "compact-snapshot-pin")
	ref := stageTestContent(t, c, "payload")
	prepared, err := c.BeginMutation(
		ctx, tenant, mustCatalogHead(t, c, tenant), testMountCreateIntent(root.ID, "file", ref),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.finishTestNamespaceMutation(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := c.OpenAt(
		ctx, testRetentionOwner, tenant, PresentationMount, 1,
		result.Primary.ID, result.Primary.Revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintainTestTenantUntilIdle(ctx, c, tenant, result.Mutation.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutation(ctx, tenant, prepared.OperationID); !errors.Is(err, ErrMutationExpired) {
		t.Fatalf("mutation with unrelated snapshot handle = %v, want ErrMutationExpired", err)
	}
	content := make([]byte, len("payload"))
	if _, err := handle.ReadAt(content, 0); err != nil {
		t.Fatalf("read retained snapshot content: %v", err)
	}
	if !bytes.Equal(content, []byte("payload")) {
		t.Fatalf("retained snapshot content = %q, want payload", content)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMutationCompactionUsesFixedPages(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createMountMutationTenant(t, c, "compact-mutation-pages")
	for index := 0; index < MaintenancePageLimit+44; index++ {
		if _, err := c.Create(ctx, tenant, CreateSpec{
			Parent: root.ID, Name: fmt.Sprintf("directory-%04d", index),
			Kind: KindDirectory, Mode: 0o700, Visibility: Visibility{Mount: true},
		}); err != nil {
			t.Fatalf("Create(%d): %v", index, err)
		}
	}
	head := mustCatalogHead(t, c, tenant)
	first, err := maintainTestUntilTenantPhase(ctx, c, tenant, head, MaintenanceMutations)
	if err != nil {
		t.Fatal(err)
	}
	if first.Retired != MaintenancePageLimit || !first.More {
		t.Fatalf("first compaction = %+v, want %d and more", first, MaintenancePageLimit)
	}
	second, err := maintainTestUntilTenantPhase(ctx, c, tenant, head, MaintenanceMutations)
	if err != nil {
		t.Fatal(err)
	}
	if second.Retired != 45 || !second.More {
		t.Fatalf("second compaction = %+v, want 45 terminal rows and a final yield", second)
	}
	if settled, err := c.MaintainTenant(ctx, tenant, testMaintenanceNow()); err != nil || settled.More {
		t.Fatalf("final mutation maintenance = %+v, %v; want idle", settled, err)
	}
}

func TestMutationCompactionFailpointRollsBackFloorAndJournal(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	boom := errors.New("compact mutation crash")
	armed := true
	c, err := open(ctx, path, func(point string) error {
		if armed && point == compactMutationAfterDelete {
			armed = false
			return boom
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	tenant, root := createMountMutationTenant(t, c, "compact-mutation-crash")
	ref := stageTestContent(t, c, "payload")
	prepared, err := c.BeginMutation(
		ctx, tenant, mustCatalogHead(t, c, tenant), testMountCreateIntent(root.ID, "file", ref),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.finishTestNamespaceMutation(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := maintainTestUntilTenantPhase(
		ctx, c, tenant, result.Mutation.Revision, MaintenanceMutations,
	); !errors.Is(err, boom) {
		t.Fatalf("Compact failpoint = %v, want boom", err)
	}
	floor, err := c.CompactionFloor(ctx, tenant)
	if err != nil || floor != result.Mutation.Revision {
		t.Fatalf("floor after page rollback = %d, %v; want durable %d", floor, err, result.Mutation.Revision)
	}
	if _, err := c.Mutation(ctx, tenant, prepared.OperationID); err != nil {
		t.Fatalf("journal after rollback: %v", err)
	}
	if _, err := maintainTestTenantUntilIdle(ctx, c, tenant, result.Mutation.Revision); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Mutation(ctx, tenant, prepared.OperationID); !errors.Is(err, ErrMutationExpired) {
		t.Fatalf("mutation after successful retry = %v, want ErrMutationExpired", err)
	}
}

func createMountMutationTenant(t *testing.T, c *Catalog, name string) (TenantID, Object) {
	t.Helper()
	tenant, root := createTestTenant(t, c, name, CaseSensitive)
	ensureTestGeneration(t, c, tenant, 1)
	return tenant, root
}

func testMountCreateIntent(parent ObjectID, name string, ref ContentRef) MutationIntent {
	spec := fileSpec(parent, name, ref, 1)
	spec.Visibility = Visibility{Mount: true}
	return MutationIntent{
		SourceID: "test-source", SourceMetadata: "operation-metadata", Disposition: MutationDispositionNamespace,
		Origin: testCausalOrigin(), Create: &CreateMutation{Spec: spec},
	}
}
