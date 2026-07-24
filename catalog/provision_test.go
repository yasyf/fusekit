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
