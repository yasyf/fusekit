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
	created, err := c.ProvisionTenant(ctx, provision)
	if err != nil {
		t.Fatal(err)
	}
	domains, err := c.FileProviderDomains(ctx)
	if err != nil || len(domains) != 1 || domains[0].Registered {
		t.Fatalf("FileProviderDomains before confirmation = %+v, %v", domains, err)
	}
	domain := domains[0]
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	domain.Registered = true
	if err := c.ConfirmFileProviderDomain(ctx, domain); err != nil {
		t.Fatalf("ConfirmFileProviderDomain: %v", err)
	}
	root := created.Root
	if _, err := c.AddInterest(ctx, mustMutation(t), created.Tenant, root, InterestOwner{
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
}

func TestNoDomainTenantNeverInventsFileProviderState(t *testing.T) {
	ctx := context.Background()
	c := openDomainTestCatalog(t)
	provision := testTenantProvision(t, "mount-only", 1)
	provision.Presentations = PresentMount
	provision.FileProvider = FileProviderPresentation{}
	if _, err := c.ProvisionTenant(ctx, provision); err != nil {
		t.Fatal(err)
	}
	domains, err := c.FileProviderDomains(ctx)
	if err != nil || len(domains) != 0 {
		t.Fatalf("FileProviderDomains = %+v, %v", domains, err)
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
