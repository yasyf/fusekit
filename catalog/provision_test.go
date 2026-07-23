package catalog

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestProvisionTenantIsOneExactDurableDefinition(t *testing.T) {
	c := newTestCatalog(t)
	provision := testTenantProvision(t, "provision", 1)
	created, err := c.ProvisionTenant(context.Background(), provision)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	if created.Root == (ObjectID{}) {
		t.Fatal("ProvisionTenant returned zero root")
	}
	replayed, err := c.ProvisionTenant(context.Background(), provision)
	if err != nil {
		t.Fatalf("ProvisionTenant(replay): %v", err)
	}
	if replayed != created {
		t.Fatalf("replayed provision = %+v, want %+v", replayed, created)
	}
	conflict := provision
	conflict.BackingRoot = filepath.Join(t.TempDir(), "other")
	if _, err := c.ProvisionTenant(context.Background(), conflict); !errors.Is(err, ErrTenantProvisionConflict) {
		t.Fatalf("ProvisionTenant(conflict) = %v, want ErrTenantProvisionConflict", err)
	}
	metadata, err := c.Tenant(context.Background(), provision.Tenant)
	if err != nil {
		t.Fatalf("Tenant: %v", err)
	}
	if metadata.Root != created.Root || metadata.CasePolicy != provision.CasePolicy || metadata.Presentations != provision.Presentations {
		t.Fatalf("metadata = %+v, want root=%s policy=%d presentations=%d", metadata, created.Root, provision.CasePolicy, provision.Presentations)
	}
	state, err := c.LoadTenantState(context.Background(), provision.Tenant)
	if err != nil {
		t.Fatalf("LoadTenantState: %v", err)
	}
	if state.Generation != provision.Generation || state.Version != 1 {
		t.Fatalf("state = %+v, want generation=%d version=1", state, provision.Generation)
	}
	listed, err := allTenantProvisions(t, c)
	if err != nil || len(listed) != 1 || listed[0] != created {
		t.Fatalf("TenantProvisions = %+v, %v; want [%+v]", listed, err, created)
	}
}

func TestProvisionTenantFailpointsExposeOnlyCommittedState(t *testing.T) {
	for _, point := range []string{provisionAfterBegin, provisionAfterCatalog, provisionBeforeCommit} {
		t.Run(point, func(t *testing.T) {
			c := newTestCatalog(t)
			provision := testTenantProvision(t, point, 1)
			c.failpoint = func(got string) error {
				if got == point {
					return errors.New("injected")
				}
				return nil
			}
			if _, err := c.ProvisionTenant(context.Background(), provision); err == nil {
				t.Fatal("ProvisionTenant succeeded at failpoint")
			}
			c.failpoint = nil
			if provisions, err := allTenantProvisions(t, c); err != nil || len(provisions) != 0 {
				t.Fatalf("TenantProvisions after rollback = %+v, %v", provisions, err)
			}
			if _, err := c.Tenant(context.Background(), provision.Tenant); !errors.Is(err, ErrNotFound) {
				t.Fatalf("Tenant after rollback = %v, want ErrNotFound", err)
			}
		})
	}

	c := newTestCatalog(t)
	provision := testTenantProvision(t, "after-commit", 1)
	c.failpoint = func(point string) error {
		if point == provisionAfterCommit {
			return errors.New("response lost")
		}
		return nil
	}
	if _, err := c.ProvisionTenant(context.Background(), provision); err == nil {
		t.Fatal("ProvisionTenant(after commit) succeeded")
	}
	c.failpoint = nil
	replayed, err := c.ProvisionTenant(context.Background(), provision)
	if err != nil || replayed.Root == (ObjectID{}) {
		t.Fatalf("ProvisionTenant retry = %+v, %v", replayed, err)
	}
}

func TestTenantProvisionReplaceAndRemoveAreGenerationFenced(t *testing.T) {
	c := newTestCatalog(t)
	first := testTenantProvision(t, "replace", 1)
	created, err := c.ProvisionTenant(context.Background(), first)
	if err != nil {
		t.Fatalf("ProvisionTenant: %v", err)
	}
	next := first
	next.Generation = 2
	next.BackingRoot = filepath.Join(filepath.Dir(first.BackingRoot), "backing-2")
	replaced, err := c.ReplaceTenantProvision(context.Background(), 1, next)
	if err != nil {
		t.Fatalf("ReplaceTenantProvision: %v", err)
	}
	if replaced.Root != created.Root || replaced.Generation != 2 {
		t.Fatalf("replaced = %+v, want root=%s generation=2", replaced, created.Root)
	}
	if replayed, err := c.ReplaceTenantProvision(context.Background(), 1, next); err != nil || replayed != replaced {
		t.Fatalf("ReplaceTenantProvision replay = %+v, %v", replayed, err)
	}
	stale := next
	stale.Generation = 3
	if _, err := c.ReplaceTenantProvision(context.Background(), 1, stale); !errors.Is(err, ErrTenantProvisionConflict) {
		t.Fatalf("ReplaceTenantProvision(stale) = %v, want conflict", err)
	}
	if err := c.RemoveTenantProvision(context.Background(), first.Tenant, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RemoveTenantProvision(stale) = %v, want ErrNotFound", err)
	}
	if err := c.RemoveTenantProvision(context.Background(), first.Tenant, 2); err != nil {
		t.Fatalf("RemoveTenantProvision: %v", err)
	}
	if err := c.RemoveTenantProvision(context.Background(), first.Tenant, 2); err != nil {
		t.Fatalf("RemoveTenantProvision replay: %v", err)
	}
	if provisions, err := allTenantProvisions(t, c); err != nil || len(provisions) != 0 {
		t.Fatalf("TenantProvisions after remove = %+v, %v", provisions, err)
	}
	reprovision := next
	reprovision.Generation = 3
	recreated, err := c.ProvisionTenant(context.Background(), reprovision)
	if err != nil {
		t.Fatalf("ProvisionTenant retained catalog: %v", err)
	}
	if recreated.Root != created.Root || recreated.Generation != 3 {
		t.Fatalf("reprovisioned = %+v, want root=%s generation=3", recreated, created.Root)
	}
	state, err := c.LoadTenantState(context.Background(), first.Tenant)
	if err != nil || state.Generation != 3 || state.ActivatedGeneration != 0 || state.Desired != 0 || state.Observed != 0 || state.Verified != 0 || state.Applied != 0 {
		t.Fatalf("reprovisioned state = %+v, %v", state, err)
	}
	if err := c.RemoveTenantProvision(context.Background(), first.Tenant, 3); err != nil {
		t.Fatalf("RemoveTenantProvision generation 3: %v", err)
	}
	mismatch := reprovision
	mismatch.Generation = 4
	mismatch.CasePolicy = CaseInsensitive
	if _, err := c.ProvisionTenant(context.Background(), mismatch); !errors.Is(err, ErrTenantProvisionConflict) {
		t.Fatalf("ProvisionTenant retained metadata mismatch = %v, want conflict", err)
	}
	if provisions, err := allTenantProvisions(t, c); err != nil || len(provisions) != 0 {
		t.Fatalf("TenantProvisions after rejected reprovision = %+v, %v", provisions, err)
	}
}

func TestTenantProvisionRejectsOversizedRecord(t *testing.T) {
	c := newTestCatalog(t)
	oversized := testTenantProvision(t, "page-too-large", 1)
	oversized.FileProvider.DisplayName = strings.Repeat("a", TenantProvisionRecordMaxBytes)
	if _, err := c.ProvisionTenant(t.Context(), oversized); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("oversized provision = %v, want ErrInvalidObject", err)
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

func allTenantProvisions(t *testing.T, c *Catalog) ([]TenantProvision, error) {
	t.Helper()
	owner := SourceAuthorityFleetOwnerID("test-owner")
	head, err := c.TopologyHead(t.Context(), owner)
	if err != nil {
		return nil, err
	}
	var provisions []TenantProvision
	var cursor TopologyCursor
	for {
		page, err := c.TopologySnapshot(t.Context(), TopologySnapshotRequest{
			Owner: owner, Revision: head.Revision, Cursor: cursor, Limit: TopologyPageLimit,
		})
		if err != nil {
			return nil, err
		}
		provisions = append(provisions, page.Tenants...)
		if page.Next == (TopologyCursor{}) {
			return provisions, nil
		}
		cursor = page.Next
	}
}
