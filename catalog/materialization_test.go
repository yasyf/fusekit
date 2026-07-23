package catalog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestVerifyMaterializationReadsExactInterestedSnapshotAndRejectsCorruption(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	provision, err := c.ProvisionTenant(ctx, testTenantProvision(t, "materialize", 1))
	if err != nil {
		t.Fatal(err)
	}
	root, err := c.Root(ctx, provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	file := createTestFile(t, c, provision.Tenant, root.ID, "settings.json", "body")
	domain, err := causal.DeriveDomainID(provision.OwnerID, provision.FileProvider.PresentationInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddInterest(ctx, provision.Tenant, mustCatalogHead(t, c, provision.Tenant), file.ID, InterestOwner{
		Presentation: PresentationFileProvider, Domain: domain, Generation: 1,
	}, file.ContentRevision); err != nil {
		t.Fatal(err)
	}
	head, err := c.Head(ctx, provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.VerifyMaterialization(ctx, provision.Tenant, 1, head); err != nil {
		t.Fatalf("VerifyMaterialization: %v", err)
	}
	if err := os.WriteFile(c.blobPath(file.Hash), []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := c.VerifyMaterialization(ctx, provision.Tenant, 1, head); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("corrupt VerifyMaterialization = %v", err)
	}
}
