package catalog

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestTenantDesiredRejectsOversizedCanonicalRecord(t *testing.T) {
	c := newTestCatalog(t)
	oversized := testTenantProvision(t, "page-too-large", 1)
	oversized.FileProvider.DisplayName = strings.Repeat("a", TenantProvisionRecordMaxBytes)
	if _, err := c.SetTenantPresent(t.Context(), tenantMutationForTest(t, oversized.OwnerID, 0), oversized); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("SetTenantPresent(oversized) = %v, want ErrInvalidObject", err)
	}
}

func TestTenantRetirementProofIsExactAndDurable(t *testing.T) {
	c := newTestCatalog(t)
	provision := testTenantProvision(t, "retirement-proof", 1)
	provision.Presentations = PresentMount
	provision.FileProvider = FileProviderPresentation{}
	if _, err := c.ProvisionTenant(t.Context(), provision); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveTenantProvision(t.Context(), provision.Tenant, provision.Generation); err != nil {
		t.Fatal(err)
	}
	want := TenantRetirementProof{
		Tenant: provision.Tenant, Generation: provision.Generation, FileProviderAbsent: true,
	}
	proof, err := c.ProveTenantRetired(t.Context(), provision.OwnerID, provision.Tenant, provision.Generation)
	if err != nil || proof != want {
		t.Fatalf("ProveTenantRetired = %+v, %v", proof, err)
	}
	if _, err := c.ProveTenantRetired(t.Context(), provision.OwnerID, provision.Tenant, 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong generation proof = %v, want not found", err)
	}
	if _, err := c.ProveTenantRetired(t.Context(), "foreign", provision.Tenant, provision.Generation); !errors.Is(err, ErrTenantOwnerMismatch) {
		t.Fatalf("foreign owner proof = %v, want owner mismatch", err)
	}
}

func testTenantProvision(t *testing.T, name string, generation Generation) TenantProvision {
	t.Helper()
	tenant, err := NewTenantID(name)
	if err != nil {
		t.Fatalf("NewTenantID: %v", err)
	}
	root := t.TempDir()
	return TenantProvision{
		OwnerID: "test-owner", Tenant: tenant,
		Mount:           MountPresentation{PresentationRoot: filepath.Join(root, "presentation")},
		BackingRoot:     filepath.Join(root, "backing"),
		ContentSourceID: "test-source", Access: TenantReadWrite,
		CasePolicy: CaseSensitive, Presentations: PresentMount | PresentFileProvider,
		FileProvider: FileProviderPresentation{PresentationInstanceID: name + "-instance", DisplayName: name},
		Generation:   generation,
	}
}
