package catalog

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

func TestFileProviderDomainRegistrationAndLeaseExpiryAreExact(t *testing.T) {
	ctx := context.Background()
	c := openDomainTestCatalog(t)
	provision := testTenantProvision(t, "domain", 7)
	created, err := provisionTenantForTest(t, c, ctx, provision)
	if err != nil {
		t.Fatal(err)
	}
	domains, err := allFileProviderDomains(t, c)
	if err != nil || len(domains) != 1 || domains[0].Registered {
		t.Fatalf("FileProviderDomains before confirmation = %+v, %v", domains, err)
	}
	if domains[0].Access != provision.Access {
		t.Fatalf("FileProviderDomains access = %v, want %v", domains[0].Access, provision.Access)
	}
	keyed, found, err := c.FileProviderDomainForTenant(ctx, created.Tenant)
	if err != nil || !found || keyed != domains[0] {
		t.Fatalf("FileProviderDomainForTenant before confirmation = %+v, %t, %v", keyed, found, err)
	}
	domain := domains[0]
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	domain.ActivationGeneration = "activation-domain"
	domain.Registered = true
	if err := c.ConfirmFileProviderDomain(ctx, domain); err != nil {
		t.Fatalf("ConfirmFileProviderDomain: %v", err)
	}
	keyed, found, err = c.FileProviderDomainForTenant(ctx, created.Tenant)
	if err != nil || !found || keyed != domain {
		t.Fatalf("FileProviderDomainForTenant after confirmation = %+v, %t, %v", keyed, found, err)
	}
	root := created.Root
	if _, err := c.AddInterest(ctx, created.Tenant, mustCatalogHead(t, c, created.Tenant), root, InterestOwner{
		Presentation: PresentationFileProvider, Domain: domain.DomainID, Generation: causal.Generation(created.Generation),
	}, 1); err != nil {
		t.Fatalf("AddInterest: %v", err)
	}
	now := time.Unix(100, 0)
	if err := c.RenewFileProviderLease(ctx, FileProviderLease{
		ID: "lease-1", Tenant: created.Tenant, DomainID: domain.DomainID,
		Generation: created.Generation, ExpiresAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("RenewFileProviderLease: %v", err)
	}
	leases, interests, err := c.FileProviderDemand(ctx, created.Tenant, domain.DomainID, created.Generation, now)
	if err != nil || leases != 1 || interests != 1 {
		t.Fatalf("live demand = %d, %d, %v", leases, interests, err)
	}
	leases, interests, err = c.FileProviderDemand(ctx, created.Tenant, domain.DomainID, created.Generation, now.Add(time.Minute))
	if err != nil || leases != 0 || interests != 1 {
		t.Fatalf("expired demand = %d, %d, %v", leases, interests, err)
	}

	stale := domain
	stale.Generation++
	if err := c.ConfirmFileProviderDomain(ctx, stale); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("stale generation confirmation = %v", err)
	}
	stale = domain
	stale.Root, err = NewObjectID()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.ConfirmFileProviderDomain(ctx, stale); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("stale root confirmation = %v", err)
	}
	stale = domain
	stale.Access = TenantReadOnly
	if err := c.ConfirmFileProviderDomain(ctx, stale); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("stale access confirmation = %v", err)
	}
}

func TestNoDomainTenantNeverInventsFileProviderState(t *testing.T) {
	ctx := context.Background()
	c := openDomainTestCatalog(t)
	provision := testTenantProvision(t, "mount-only", 1)
	provision.Presentations = PresentMount
	provision.FileProvider = FileProviderPresentation{}
	if _, err := provisionTenantForTest(t, c, ctx, provision); err != nil {
		t.Fatal(err)
	}
	domains, err := allFileProviderDomains(t, c)
	if err != nil || len(domains) != 0 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
	}
}

func TestInvalidateFileProviderDomainRemovesActivationProof(t *testing.T) {
	c := openDomainTestCatalog(t)
	created, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, "invalidate-domain", 3))
	if err != nil {
		t.Fatal(err)
	}
	domain, found, err := c.FileProviderDomainForTenant(t.Context(), created.Tenant)
	if err != nil || !found {
		t.Fatalf("FileProviderDomainForTenant = %+v, %t, %v", domain, found, err)
	}
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	domain.ActivationGeneration = "activation-before-restart"
	domain.Registered = true
	if err := c.ConfirmFileProviderDomain(t.Context(), domain); err != nil {
		t.Fatal(err)
	}
	if err := c.InvalidateFileProviderDomain(t.Context(), created.Tenant, created.Generation); err != nil {
		t.Fatal(err)
	}
	invalidated, found, err := c.FileProviderDomainForTenant(t.Context(), created.Tenant)
	if err != nil || !found || invalidated.Registered || invalidated.PublicPath != "" || invalidated.ActivationGeneration != "" {
		t.Fatalf("invalidated domain = %+v, %t, %v", invalidated, found, err)
	}
}

func TestFileProviderDomainRemovalIsExactDurableAndClearedOnlyAfterReprovision(t *testing.T) {
	ctx := context.Background()
	c := openDomainTestCatalog(t)
	provision := testTenantProvision(t, "retire-domain", 4)
	created, err := provisionTenantForTest(t, c, ctx, provision)
	if err != nil {
		t.Fatal(err)
	}
	domains, err := allFileProviderDomains(t, c)
	if err != nil || len(domains) != 1 {
		t.Fatalf("domains before removal = %+v, %v", domains, err)
	}
	registered := domains[0]
	registered.PublicPath = filepath.Join(t.TempDir(), "Domain")
	registered.ActivationGeneration = "activation-retirement"
	registered.Registered = true
	if err := c.ConfirmFileProviderDomain(ctx, registered); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginFileProviderDomainRemoval(ctx, "wrong-owner", created.Tenant, created.Generation); !errors.Is(err, ErrTenantOwnerMismatch) {
		t.Fatalf("wrong owner removal = %v", err)
	}
	if _, err := c.BeginFileProviderDomainRemoval(ctx, created.OwnerID, created.Tenant, created.Generation+1); !errors.Is(err, ErrGenerationMismatch) {
		t.Fatalf("wrong generation removal = %v", err)
	}
	removal, err := c.BeginFileProviderDomainRemoval(ctx, created.OwnerID, created.Tenant, created.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if removal.ConfirmedAbsent {
		t.Fatal("new removal was already confirmed")
	}
	if domains, err := allFileProviderDomains(t, c); err != nil || len(domains) != 0 {
		t.Fatalf("desired domains after removal fence = %+v, %v", domains, err)
	}
	if replayed, err := provisionTenantForTest(t, c, ctx, created); err != nil || replayed != created {
		t.Fatalf("exact desired replay during domain removal = %+v, %v", replayed, err)
	}
	if err := c.ConfirmFileProviderDomainRemoval(ctx, removal); err != nil {
		t.Fatal(err)
	}
	var registeredCount int
	if err := c.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM file_provider_domains WHERE tenant = ?`, string(created.Tenant)).Scan(&registeredCount); err != nil {
		t.Fatal(err)
	}
	if registeredCount != 0 {
		t.Fatalf("registered domains after exact absence = %d", registeredCount)
	}
	state, err := c.FileProviderDomainRemovalState(ctx, created.OwnerID, created.Tenant, created.Generation)
	if err != nil || !state.ConfirmedAbsent {
		t.Fatalf("confirmed removal = %+v, %v", state, err)
	}
	if err := removeTenantForTest(t, c, ctx, created.Tenant, created.Generation); err != nil {
		t.Fatal(err)
	}
	next := created
	next.Generation++
	if _, err := provisionTenantForTest(t, c, ctx, next); err != nil {
		t.Fatalf("reprovision after exact absence = %v", err)
	}
	if _, err := c.FileProviderDomainRemovalState(ctx, created.OwnerID, created.Tenant, created.Generation); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed removal tombstone after reprovision = %v", err)
	}
}

func TestFileProviderDomainAndRemovalPagesHaveExclusiveBoundedCursors(t *testing.T) {
	c := openDomainTestCatalog(t)
	first, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, "domain-page-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	second, err := provisionTenantForTest(t, c, t.Context(), testTenantProvision(t, "domain-page-b", 1))
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.PageFileProviderDomains(t.Context(), "", 1)
	if err != nil || len(page.Domains) != 1 || page.Domains[0].Tenant != first.Tenant || page.Next != first.Tenant {
		t.Fatalf("first domain page = %+v, %v", page, err)
	}
	page, err = c.PageFileProviderDomains(t.Context(), page.Next, 1)
	if err != nil || len(page.Domains) != 1 || page.Domains[0].Tenant != second.Tenant || page.Next != "" {
		t.Fatalf("second domain page = %+v, %v", page, err)
	}
	if _, err := c.BeginFileProviderDomainRemoval(t.Context(), first.OwnerID, first.Tenant, first.Generation); err != nil {
		t.Fatal(err)
	}
	if _, err := c.BeginFileProviderDomainRemoval(t.Context(), second.OwnerID, second.Tenant, second.Generation); err != nil {
		t.Fatal(err)
	}
	removals, err := c.PageFileProviderDomainRemovals(t.Context(), "", 1)
	if err != nil || len(removals.Removals) != 1 || removals.Removals[0].Domain.Tenant != first.Tenant ||
		removals.Next != first.Tenant {
		t.Fatalf("first removal page = %+v, %v", removals, err)
	}
	removals, err = c.PageFileProviderDomainRemovals(t.Context(), removals.Next, 1)
	if err != nil || len(removals.Removals) != 1 || removals.Removals[0].Domain.Tenant != second.Tenant ||
		removals.Next != "" {
		t.Fatalf("second removal page = %+v, %v", removals, err)
	}
}

func openDomainTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	c, err := Open(context.Background(), filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return c
}

func allFileProviderDomains(t *testing.T, c *Catalog) ([]FileProviderDomain, error) {
	t.Helper()
	var domains []FileProviderDomain
	for after := TenantID(""); ; {
		page, err := c.PageFileProviderDomains(t.Context(), after, FileProviderDomainPageLimit)
		if err != nil {
			return nil, err
		}
		domains = append(domains, page.Domains...)
		if page.Next == "" {
			return domains, nil
		}
		after = page.Next
	}
}
