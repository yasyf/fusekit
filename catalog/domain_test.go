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

func TestFileProviderCutoverFenceIsDurableOneShot(t *testing.T) {
	ctx := context.Background()
	c := openDomainTestCatalog(t)
	firstOperation := MutationID{1}
	firstHash := [32]byte{2}
	if _, err := c.RecoverClaimedFileProviderCutoverByPlan(ctx, firstHash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("recover before fence = %v", err)
	}
	mode, created, err := c.BeginFileProviderCutover(ctx, firstOperation, firstHash)
	if err != nil || !created || mode.OperationID != firstOperation || mode.PlanHash != firstHash {
		t.Fatalf("first cutover = %+v, %t, %v", mode, created, err)
	}
	mode, created, err = c.BeginFileProviderCutover(ctx, MutationID{3}, firstHash)
	if err != nil || created || mode.OperationID != firstOperation {
		t.Fatalf("cutover replay = %+v, %t, %v", mode, created, err)
	}
	if _, _, err := c.BeginFileProviderCutover(ctx, MutationID{4}, [32]byte{5}); !errors.Is(err, ErrConflict) {
		t.Fatalf("different cutover plan = %v", err)
	}
	if _, err := c.RecoverClaimedFileProviderCutoverByPlan(ctx, firstHash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("recover fenced cutover = %v", err)
	}
	if _, err := c.RecoverClaimedFileProviderCutoverByPlan(ctx, [32]byte{5}); !errors.Is(err, ErrConflict) {
		t.Fatalf("recover wrong cutover plan = %v", err)
	}
	proofHash := [32]byte{6}
	proofJSON := []byte(`{"proof":"one"}`)
	if err := c.RecordFileProviderCutoverProof(ctx, firstOperation, firstHash, proofHash, proofJSON, "boot-1", 10*time.Second); err != nil {
		t.Fatalf("record proof = %v", err)
	}
	if err := c.RecordFileProviderCutoverProof(ctx, firstOperation, firstHash, proofHash, proofJSON, "boot-1", 10*time.Second); err != nil {
		t.Fatalf("record identical proof = %v", err)
	}
	if _, err := c.RecoverClaimedFileProviderCutoverByPlan(ctx, firstHash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("recover proved cutover = %v", err)
	}
	claims := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := c.ClaimFileProviderCutoverProof(ctx, firstOperation, proofHash, "boot-1", 20*time.Second, 30*time.Second)
			claims <- err
		}()
	}
	claimed, rejected := 0, 0
	for range 2 {
		if err := <-claims; err == nil {
			claimed++
		} else if errors.Is(err, ErrInvalidTransition) || errors.Is(err, ErrConflict) {
			rejected++
		} else {
			t.Fatalf("unexpected concurrent claim error = %v", err)
		}
	}
	if claimed != 1 || rejected != 1 {
		t.Fatalf("concurrent claims = %d accepted, %d rejected", claimed, rejected)
	}
	if _, err := c.ClaimFileProviderCutoverProof(ctx, firstOperation, proofHash, "boot-1", 20*time.Second, 30*time.Second); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("replayed claim = %v", err)
	}
	read, found, err := c.FileProviderCutoverMode(ctx)
	if err != nil || !found || read.OperationID != mode.OperationID || read.PlanHash != mode.PlanHash ||
		read.State != FileProviderCutoverClaimed || read.ProofHash != proofHash || read.ClaimedAt.IsZero() {
		t.Fatalf("durable cutover = %+v, %t, %v", read, found, err)
	}
	recovered, err := c.RecoverClaimedFileProviderCutoverByPlan(ctx, firstHash)
	if err != nil || recovered.OperationID != firstOperation || recovered.ProofHash != proofHash ||
		recovered.State != FileProviderCutoverClaimed {
		t.Fatalf("recover claimed cutover = %+v, %v", recovered, err)
	}
}

func TestFileProviderCutoverProofExpiryIsBootBoundAndMonotonic(t *testing.T) {
	const ttl = 30 * time.Second
	setup := func(t *testing.T) (*Catalog, MutationID, [32]byte, [32]byte) {
		t.Helper()
		c := openDomainTestCatalog(t)
		operation, planHash, proofHash := MutationID{1}, [32]byte{2}, [32]byte{3}
		if _, _, err := c.BeginFileProviderCutover(t.Context(), operation, planHash); err != nil {
			t.Fatal(err)
		}
		if err := c.RecordFileProviderCutoverProof(
			t.Context(), operation, planHash, proofHash, []byte(`{"proof":"one"}`), "boot-1", 100*time.Second,
		); err != nil {
			t.Fatal(err)
		}
		return c, operation, planHash, proofHash
	}

	t.Run("before deadline survives restart and wall clock jump", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "catalog.sqlite")
		c, err := Open(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		operation, planHash, proofHash := MutationID{1}, [32]byte{2}, [32]byte{3}
		if _, _, err := c.BeginFileProviderCutover(t.Context(), operation, planHash); err != nil {
			t.Fatal(err)
		}
		if err := c.RecordFileProviderCutoverProof(
			t.Context(), operation, planHash, proofHash, []byte(`{"proof":"one"}`), "boot-1", 100*time.Second,
		); err != nil {
			t.Fatal(err)
		}
		if _, err := c.db.ExecContext(t.Context(), `UPDATE file_provider_cutover SET started_unix_nano = ?`, time.Now().Add(100*365*24*time.Hour).UnixNano()); err != nil {
			t.Fatal(err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		c, err = Open(t.Context(), path)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		claimed, err := c.ClaimFileProviderCutoverProof(t.Context(), operation, proofHash, "boot-1", 100*time.Second+ttl-time.Nanosecond, ttl)
		if err != nil || claimed.State != FileProviderCutoverClaimed {
			t.Fatalf("claim before deadline = %+v, %v", claimed, err)
		}
		if expired, err := c.ExpireFileProviderCutoverProof(t.Context(), "boot-2", 0, ttl); err != nil || expired {
			t.Fatalf("terminal claim expired after reboot = %t, %v", expired, err)
		}
		if _, err := c.RecoverClaimedFileProviderCutoverProof(t.Context(), operation, proofHash); err != nil {
			t.Fatalf("claimed receipt after deadline/reboot remains durable: %v", err)
		}
	})

	for _, tc := range []struct {
		name   string
		boot   string
		uptime time.Duration
	}{
		{"at deadline", "boot-1", 100*time.Second + ttl},
		{"after deadline", "boot-1", 100*time.Second + ttl + time.Nanosecond},
		{"reboot", "boot-2", 100 * time.Second},
		{"monotonic regression", "boot-1", 99 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, operation, planHash, proofHash := setup(t)
			if _, err := c.ClaimFileProviderCutoverProof(t.Context(), operation, proofHash, tc.boot, tc.uptime, ttl); !errors.Is(err, ErrCutoverProofExpired) {
				t.Fatalf("expired claim = %v", err)
			}
			mode, found, err := c.FileProviderCutoverMode(t.Context())
			if err != nil || !found || mode.State != FileProviderCutoverExpired {
				t.Fatalf("expired state = %+v, %t, %v", mode, found, err)
			}
			rotated, created, err := c.BeginFileProviderCutover(t.Context(), MutationID{9}, planHash)
			if err != nil || !created || rotated.OperationID != (MutationID{9}) || rotated.State != FileProviderCutoverFenced {
				t.Fatalf("fresh proof rotation = %+v, %t, %v", rotated, created, err)
			}
		})
	}

	t.Run("concurrent boundary claims never consume an expired proof", func(t *testing.T) {
		c, operation, _, proofHash := setup(t)
		errs := make(chan error, 2)
		for range 2 {
			go func() {
				_, err := c.ClaimFileProviderCutoverProof(t.Context(), operation, proofHash, "boot-1", 100*time.Second+ttl, ttl)
				errs <- err
			}()
		}
		for range 2 {
			if err := <-errs; !errors.Is(err, ErrCutoverProofExpired) {
				t.Fatalf("boundary claim = %v", err)
			}
		}
	})
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

func TestFileProviderDomainRemovalIsExactDurableAndClearedOnlyAfterReprovision(t *testing.T) {
	ctx := context.Background()
	c := openDomainTestCatalog(t)
	provision := testTenantProvision(t, "retire-domain", 4)
	created, err := c.ProvisionTenant(ctx, provision)
	if err != nil {
		t.Fatal(err)
	}
	domains, err := c.FileProviderDomains(ctx)
	if err != nil || len(domains) != 1 {
		t.Fatalf("domains before removal = %+v, %v", domains, err)
	}
	registered := domains[0]
	registered.PublicPath = filepath.Join(t.TempDir(), "Domain")
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
	if domains, err := c.FileProviderDomains(ctx); err != nil || len(domains) != 0 {
		t.Fatalf("desired domains after removal fence = %+v, %v", domains, err)
	}
	if _, err := c.ProvisionTenant(ctx, created); !errors.Is(err, ErrTenantProvisionConflict) {
		t.Fatalf("provision during removal = %v", err)
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
	if err := c.RemoveTenantProvision(ctx, created.Tenant, created.Generation); err != nil {
		t.Fatal(err)
	}
	next := created
	next.Generation++
	if _, err := c.ProvisionTenant(ctx, next); err != nil {
		t.Fatalf("reprovision after exact absence = %v", err)
	}
	if _, err := c.FileProviderDomainRemovalState(ctx, created.OwnerID, created.Tenant, created.Generation); !errors.Is(err, ErrNotFound) {
		t.Fatalf("completed removal tombstone after reprovision = %v", err)
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
