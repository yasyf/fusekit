package catalog

import (
	"context"
	"errors"
	"testing"
)

func TestOpenAtRejectsStaleTenantGeneration(t *testing.T) {
	ctx := context.Background()
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "open-generation", CaseSensitive)
	file := createTestFile(t, c, tenant, root.ID, "file", "body")
	state, err := c.LoadTenantState(ctx, tenant)
	if err != nil {
		t.Fatalf("LoadTenantState: %v", err)
	}
	state.Generation++
	if _, err := c.SaveTenantState(ctx, state.Version, state); err != nil {
		t.Fatalf("SaveTenantState: %v", err)
	}
	if _, err := c.OpenAt(ctx, testRetentionOwner, tenant, PresentationFileProvider, state.Generation-1, file.ID, file.Revision); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("OpenAt(stale generation) = %v, want ErrGenerationMismatch", err)
	}
	handle, err := c.OpenAt(ctx, testRetentionOwner, tenant, PresentationFileProvider, state.Generation, file.ID, file.Revision)
	if err != nil {
		t.Fatalf("OpenAt(current generation): %v", err)
	}
	if err := handle.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
