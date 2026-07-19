package catalog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestPreparedSourceLocatorsCoverEveryNamespaceMutation(t *testing.T) {
	c := newTestCatalog(t)
	provision := provisionSourceMutationTenant(t, c, "source-locator-shapes")
	applySourceMutationSnapshot(t, c, provision, "parent", "source", "target")
	root := mustSourceObject(t, c, provision.Tenant, provision.Root)
	parent := mustSourceName(t, c, provision.Tenant, provision.Root, "parent")
	source := mustSourceName(t, c, provision.Tenant, provision.Root, "source")
	target := mustSourceName(t, c, provision.Tenant, provision.Root, "target")

	created := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(), Create: &CreateMutation{Spec: CreateSpec{
			Parent: root.ID, Name: "created", Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		}},
	})
	created = prepareSourceMutation(t, c, created)
	assertSourceLocator(t, created.Source.Parent, SourceObjectKey("root:"+string(provision.Tenant)), 1)
	if created.Source.Object != nil || created.Source.Target != nil || created.Source.Operation.Kind != MutationCreate {
		t.Fatalf("create source context = %+v", created.Source)
	}
	result := SourceLocator{SourceAuthority: "test-source", SourceKey: "created", SourceRevision: 1}
	created, err := c.SetMutationSourceResult(t.Context(), created.OperationID, *created.Claim, result)
	if err != nil {
		t.Fatalf("SetMutationSourceResult: %v", err)
	}
	if created.SourceResult == nil || *created.SourceResult != result {
		t.Fatalf("create source result = %+v", created.SourceResult)
	}
	if _, err := c.SetMutationSourceResult(t.Context(), created.OperationID, *created.Claim, result); err != nil {
		t.Fatalf("SetMutationSourceResult retry: %v", err)
	}
	finishSourceMutation(t, c, created)
	createdObject := mustSourceName(t, c, provision.Tenant, provision.Root, "created")

	revised := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(), Revise: &ReviseMutation{
			Object: createdObject.ID,
			Spec: RevisionSpec{
				Parent: parent.ID, Name: "renamed", Mode: createdObject.Mode,
				Convergence: createdObject.Convergence, Visibility: createdObject.Visibility,
			},
		},
	})
	revised = prepareSourceMutation(t, c, revised)
	assertSourceLocator(t, revised.Source.Object, "created", 1)
	assertSourceLocator(t, revised.Source.Parent, "parent", 1)
	if revised.Source.Target != nil || revised.Source.Operation.Name != "renamed" || revised.Source.Operation.Kind != MutationRevise {
		t.Fatalf("revise source context = %+v", revised.Source)
	}
	finishSourceMutation(t, c, revised)
	createdObject = mustSourceName(t, c, provision.Tenant, parent.ID, "renamed")

	deleted := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(), Delete: &DeleteMutation{Object: createdObject.ID},
	})
	deleted = prepareSourceMutation(t, c, deleted)
	assertSourceLocator(t, deleted.Source.Object, "created", 1)
	assertSourceLocator(t, deleted.Source.Parent, "parent", 1)
	if deleted.Source.Target != nil || deleted.Source.Operation.Kind != MutationDelete {
		t.Fatalf("delete source context = %+v", deleted.Source)
	}
	finishSourceMutation(t, c, deleted)

	replaced := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(), Replace: &ReplaceMutation{Source: source.ID, Target: target.ID},
	})
	replaced = prepareSourceMutation(t, c, replaced)
	assertSourceLocator(t, replaced.Source.Object, "source", 1)
	assertSourceLocator(t, replaced.Source.Target, "target", 1)
	assertSourceLocator(t, replaced.Source.Parent, SourceObjectKey("root:"+string(provision.Tenant)), 1)
	if replaced.Source.Operation.Name != target.Name || replaced.Source.Operation.Kind != MutationReplace {
		t.Fatalf("replace source context = %+v", replaced.Source)
	}
	finishSourceMutation(t, c, replaced)
}

func TestPreparedSourceLocatorRejectsMissingAndStaleAuthorityState(t *testing.T) {
	t.Run("missing publication", func(t *testing.T) {
		c := newTestCatalog(t)
		provision := provisionSourceMutationTenant(t, c, "source-locator-missing")
		root := mustSourceObject(t, c, provision.Tenant, provision.Root)
		prepared := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
			SourceID: "test", Origin: testCausalOrigin(), Create: &CreateMutation{Spec: CreateSpec{
				Parent: root.ID, Name: "created", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			}},
		})
		if _, err := c.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim); !errors.Is(err, ErrSourceLocatorMissing) {
			t.Fatalf("PrepareMutationSource = %v, want ErrSourceLocatorMissing", err)
		}
	})

	t.Run("unbound object", func(t *testing.T) {
		c := newTestCatalog(t)
		provision := provisionSourceMutationTenant(t, c, "source-locator-unbound")
		applySourceMutationSnapshot(t, c, provision)
		unbound, err := c.Create(t.Context(), mustMutation(t), provision.Tenant, CreateSpec{
			Parent: provision.Root, Name: "unbound", Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		})
		if err != nil {
			t.Fatalf("Create unbound object: %v", err)
		}
		prepared := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
			SourceID: "test", Origin: testCausalOrigin(), Delete: &DeleteMutation{Object: unbound.ID},
		})
		if _, err := c.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim); !errors.Is(err, ErrSourceLocatorMissing) {
			t.Fatalf("PrepareMutationSource = %v, want ErrSourceLocatorMissing", err)
		}
	})

	t.Run("watermark advanced", func(t *testing.T) {
		c := newTestCatalog(t)
		provision := provisionSourceMutationTenant(t, c, "source-locator-stale")
		applySourceMutationSnapshot(t, c, provision)
		prepared := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
			SourceID: "test", Origin: testCausalOrigin(), Create: &CreateMutation{Spec: CreateSpec{
				Parent: provision.Root, Name: "created", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			}},
		})
		prepared = prepareSourceMutation(t, c, prepared)
		if _, err := c.ApplySource(t.Context(), SourcePublication{
			Mode: SourceDelta, Predecessor: 1, Change: sourceChange(2),
			Tenants: []SourceTenant{{
				Tenant: provision.Tenant, Generation: provision.Generation, RootKey: sourceRootKey(provision),
			}},
		}); err != nil {
			t.Fatalf("ApplySource(delta): %v", err)
		}
		if _, err := c.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim); !errors.Is(err, ErrSourceLocatorStale) {
			t.Fatalf("PrepareMutationSource = %v, want ErrSourceLocatorStale", err)
		}
	})
}

func TestPreparedSourceContextAndResultRecoverExactlyAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	provision := provisionSourceMutationTenant(t, c, "source-locator-restart")
	applySourceMutationSnapshot(t, c, provision)
	prepared := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(), Create: &CreateMutation{Spec: CreateSpec{
			Parent: provision.Root, Name: "created", Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		}},
	})
	prepared = prepareSourceMutation(t, c, prepared)
	result := SourceLocator{SourceAuthority: "test-source", SourceKey: "created", SourceRevision: 1}
	prepared, err = c.SetMutationSourceResult(t.Context(), prepared.OperationID, *prepared.Claim, result)
	if err != nil {
		t.Fatalf("SetMutationSourceResult: %v", err)
	}
	staleClaim := *prepared.Claim
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c, err = Open(t.Context(), path)
	if err != nil {
		t.Fatalf("Open(restart): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	pending, err := c.PendingMutations(t.Context(), provision.Tenant)
	if err != nil || len(pending) != 1 || pending[0].Source == nil || pending[0].SourceResult == nil || *pending[0].SourceResult != result {
		t.Fatalf("recovered pending = %+v, %v", pending, err)
	}
	prepared, err = c.ReclaimMutation(t.Context(), prepared.OperationID, staleClaim, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ReclaimMutation: %v", err)
	}
	prepared, err = c.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim)
	if err != nil {
		t.Fatalf("PrepareMutationSource(restart): %v", err)
	}
	prepared, err = c.SetMutationSourceResult(t.Context(), prepared.OperationID, *prepared.Claim, result)
	if err != nil {
		t.Fatalf("SetMutationSourceResult(restart): %v", err)
	}
	finishSourceMutation(t, c, prepared)
	created := mustSourceName(t, c, provision.Tenant, provision.Root, "created")
	locator, err := sourceKeyForObjectForTest(t.Context(), c, provision, created.ID)
	if err != nil || locator != "created" {
		t.Fatalf("created binding = %q, %v", locator, err)
	}
}

func TestSourceKeyReservationBlocksPublicationUntilCatalogCommit(t *testing.T) {
	c := newTestCatalog(t)
	provision := provisionSourceMutationTenant(t, c, "source-key-reservation")
	applySourceMutationSnapshot(t, c, provision)
	prepared := beginClaimedSourceMutation(t, c, provision.Tenant, MutationIntent{
		SourceID: "test", Origin: testCausalOrigin(), Create: &CreateMutation{Spec: CreateSpec{
			Parent: provision.Root, Name: "created", Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		}},
	})
	prepared = prepareSourceMutation(t, c, prepared)
	result := SourceLocator{SourceAuthority: "test-source", SourceKey: "created", SourceRevision: 1}
	prepared, err := c.SetMutationSourceResult(t.Context(), prepared.OperationID, *prepared.Claim, result)
	if err != nil {
		t.Fatalf("SetMutationSourceResult: %v", err)
	}
	delta := SourcePublication{
		Mode: SourceDelta, Predecessor: 1, Change: sourceChange(2),
		Tenants: []SourceTenant{{
			Tenant: provision.Tenant, Generation: provision.Generation, RootKey: sourceRootKey(provision),
			Objects: []SourceObject{{
				Key: "created", Name: "created", Kind: KindDirectory, Mode: 0o700,
				Visibility: Visibility{Mount: true, FileProvider: true},
			}},
		}},
	}
	if _, err := c.ApplySource(t.Context(), delta); !errors.Is(err, ErrMutationActive) {
		t.Fatalf("ApplySource while key reserved = %v, want ErrMutationActive", err)
	}
	committed := finishSourceMutation(t, c, prepared)
	if _, err := c.ApplySource(t.Context(), delta); err != nil {
		t.Fatalf("ApplySource after catalog commit: %v", err)
	}
	current := mustSourceName(t, c, provision.Tenant, provision.Root, "created")
	if current.ID != committed.Primary.ID {
		t.Fatalf("source publication changed reserved identity: got %s want %s", current.ID, committed.Primary.ID)
	}
}

func provisionSourceMutationTenant(t *testing.T, c *Catalog, name string) TenantProvision {
	t.Helper()
	provision, err := c.ProvisionTenant(t.Context(), testTenantProvision(t, name, 1))
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	return provision
}

func applySourceMutationSnapshot(t *testing.T, c *Catalog, provision TenantProvision, keys ...SourceObjectKey) {
	t.Helper()
	objects := make([]SourceObject, len(keys))
	for index, key := range keys {
		objects[index] = SourceObject{
			Key: key, Name: string(key), Kind: KindDirectory, Mode: 0o700,
			Visibility: Visibility{Mount: true, FileProvider: true},
		}
	}
	if _, err := c.ApplySource(t.Context(), SourcePublication{
		Mode: SourceSnapshot, Change: sourceChange(1),
		Tenants: []SourceTenant{{
			Tenant: provision.Tenant, Generation: provision.Generation,
			RootKey: sourceRootKey(provision), Objects: objects,
		}},
	}); err != nil {
		t.Fatalf("ApplySource(snapshot): %v", err)
	}
}

func beginClaimedSourceMutation(t *testing.T, c *Catalog, tenant TenantID, intent MutationIntent) PreparedMutation {
	t.Helper()
	head, err := c.Head(t.Context(), tenant)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	id := mustMutation(t)
	if _, err := c.BeginMutation(t.Context(), id, tenant, head, intent); err != nil {
		t.Fatalf("BeginMutation: %v", err)
	}
	prepared, err := c.ClaimMutation(t.Context(), id, mustMutationOwner(t))
	if err != nil {
		t.Fatalf("ClaimMutation: %v", err)
	}
	return prepared
}

func prepareSourceMutation(t *testing.T, c *Catalog, prepared PreparedMutation) PreparedMutation {
	t.Helper()
	result, err := c.PrepareMutationSource(t.Context(), prepared.OperationID, *prepared.Claim)
	if err != nil {
		t.Fatalf("PrepareMutationSource: %v", err)
	}
	return result
}

func finishSourceMutation(t *testing.T, c *Catalog, prepared PreparedMutation) NamespaceMutationResult {
	t.Helper()
	if _, err := c.MarkMutationApplied(t.Context(), prepared.OperationID, *prepared.Claim); err != nil {
		t.Fatalf("MarkMutationApplied: %v", err)
	}
	result, err := c.CommitMutation(t.Context(), prepared.OperationID)
	if err != nil {
		t.Fatalf("CommitMutation: %v", err)
	}
	return result
}

func assertSourceLocator(t *testing.T, locator *SourceLocator, key SourceObjectKey, revision causal.Revision) {
	t.Helper()
	if locator == nil || locator.SourceAuthority != "test-source" || locator.SourceKey != key || locator.SourceRevision != revision {
		t.Fatalf("source locator = %+v, want test-source/%q@%d", locator, key, revision)
	}
}

func mustSourceObject(t *testing.T, c *Catalog, tenant TenantID, id ObjectID) Object {
	t.Helper()
	object, err := c.Lookup(t.Context(), tenant, PresentationFileProvider, id)
	if err != nil {
		t.Fatalf("Lookup(%s): %v", id, err)
	}
	return object
}

func mustSourceName(t *testing.T, c *Catalog, tenant TenantID, parent ObjectID, name string) Object {
	t.Helper()
	object, err := c.LookupName(t.Context(), tenant, PresentationFileProvider, parent, name)
	if err != nil {
		t.Fatalf("LookupName(%q): %v", name, err)
	}
	return object
}

func sourceKeyForObjectForTest(ctx context.Context, c *Catalog, provision TenantProvision, id ObjectID) (SourceObjectKey, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	return sourceKeyForObject(ctx, tx, "test-source", provision.Tenant, provision.Root, sourceRootKey(provision), id)
}
