package catalog

import (
	"bytes"
	"context"
	"testing"
)

func TestFileProviderSignalPlanIsBoundedAndDigestsExactSet(t *testing.T) {
	ctx := context.Background()
	c := openDomainTestCatalog(t)
	provision, err := c.ProvisionTenant(ctx, testTenantProvision(t, "signal", 1))
	if err != nil {
		t.Fatal(err)
	}
	domain, found, err := c.FileProviderDomainForTenant(ctx, provision.Tenant)
	if err != nil || !found {
		t.Fatalf("FileProviderDomainForTenant = %+v, %t, %v", domain, found, err)
	}

	writeSignalTargets(t, c, provision.Tenant, 10, MaxFileProviderSignalTargets)
	exact, err := c.FileProviderSignalPlan(
		ctx, provision.Tenant, domain.DomainID, provision.Generation, 10,
	)
	if err != nil {
		t.Fatal(err)
	}
	if exact.Coalesced || exact.ExactCount != MaxFileProviderSignalTargets ||
		len(exact.Targets) != MaxFileProviderSignalTargets {
		t.Fatalf("exact signal plan = %+v", exact)
	}

	writeSignalTargets(t, c, provision.Tenant, 11, MaxFileProviderSignalTargets+1)
	coalesced, err := c.FileProviderSignalPlan(
		ctx, provision.Tenant, domain.DomainID, provision.Generation, 11,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !coalesced.Coalesced || coalesced.ExactCount != MaxFileProviderSignalTargets+1 ||
		len(coalesced.Targets) != 1 || !coalesced.Targets[0].WorkingSet {
		t.Fatalf("coalesced signal plan = %+v", coalesced)
	}
	if bytes.Equal(coalesced.ExactDigest[:], exact.ExactDigest[:]) {
		t.Fatal("different exact target sets have the same digest")
	}
	recovered, err := c.FileProviderSignalPlan(
		ctx, provision.Tenant, domain.DomainID, provision.Generation, 11,
	)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.ExactCount != coalesced.ExactCount ||
		recovered.ExactDigest != coalesced.ExactDigest ||
		recovered.Coalesced != coalesced.Coalesced {
		t.Fatalf("recovered signal plan = %+v, want %+v", recovered, coalesced)
	}
}

func TestFileProviderSignalPlanDefaultsToWorkingSet(t *testing.T) {
	ctx := context.Background()
	c := openDomainTestCatalog(t)
	provision, err := c.ProvisionTenant(ctx, testTenantProvision(t, "signal-empty", 1))
	if err != nil {
		t.Fatal(err)
	}
	domain, found, err := c.FileProviderDomainForTenant(ctx, provision.Tenant)
	if err != nil || !found {
		t.Fatalf("FileProviderDomainForTenant = %+v, %t, %v", domain, found, err)
	}
	plan, err := c.FileProviderSignalPlan(
		ctx, provision.Tenant, domain.DomainID, provision.Generation, 99,
	)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Coalesced || plan.ExactCount != 1 || len(plan.Targets) != 1 || !plan.Targets[0].WorkingSet {
		t.Fatalf("default signal plan = %+v", plan)
	}
}

func writeSignalTargets(t *testing.T, c *Catalog, tenant TenantID, revision Revision, count int) {
	t.Helper()
	ctx := context.Background()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for index := 0; index < count; index++ {
		value := index + 1
		var parent ObjectID
		parent[0] = byte(value >> 8)
		parent[1] = byte(value)
		object := parent
		object[15] = 1
		if err := writeChange(ctx, tx, tenant, revision, EnumerationScope{
			Kind: EnumerationContainer, Presentation: PresentationFileProvider, Parent: parent,
		}, 0, ChangeUpsert, object, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
