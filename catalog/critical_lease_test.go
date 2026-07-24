package catalog

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

func TestFileProviderLeaseReleaseReplayBindsFullIdentityAndDemotesPolicy(t *testing.T) {
	store := openDomainTestCatalog(t)
	provision, err := provisionTenantForTest(t, store, t.Context(), testTenantProvision(t, "critical-lease", 1))
	if err != nil {
		t.Fatal(err)
	}
	domain, found, err := store.FileProviderDomainForTenant(t.Context(), provision.Tenant)
	if err != nil || !found {
		t.Fatalf("FileProviderDomainForTenant: found=%t err=%v", found, err)
	}
	domain.PublicPath = filepath.Join(t.TempDir(), "Domain")
	domain.ActivationGeneration = "activation-critical-lease"
	domain.Registered = true
	if err := store.ConfirmFileProviderDomain(t.Context(), domain); err != nil {
		t.Fatalf("ConfirmFileProviderDomain: %v", err)
	}
	now := time.Unix(100, 0)
	committed := commitTestFileProviderLease(t, store, testFileProviderLease(t, provision, domain, "lease", now.Add(time.Hour)))
	receipt := committed
	receipt.CriticalObjects = nil
	eager, err := store.FileProviderContentPolicy(t.Context(), provision.Tenant, domain.DomainID, provision.Generation, domain.Root, now)
	if err != nil || !eager {
		t.Fatalf("FileProviderContentPolicy before release = %t, %v", eager, err)
	}
	if _, err := store.ReleaseFileProviderLease(t.Context(), receipt); err != nil {
		t.Fatalf("ReleaseFileProviderLease: %v", err)
	}
	eager, err = store.FileProviderContentPolicy(t.Context(), provision.Tenant, domain.DomainID, provision.Generation, domain.Root, now)
	if err != nil || eager {
		t.Fatalf("FileProviderContentPolicy after release = %t, %v", eager, err)
	}
	if _, err := store.ReleaseFileProviderLease(t.Context(), receipt); err != nil {
		t.Fatalf("ReleaseFileProviderLease replay: %v", err)
	}
	substituted := receipt
	substituted.SessionID = "different-session"
	if _, err := store.ReleaseFileProviderLease(t.Context(), substituted); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("ReleaseFileProviderLease substituted replay = %v, want ErrInvalidTransition", err)
	}
}

func testFileProviderLease(
	t *testing.T,
	provision TenantProvision,
	domain FileProviderDomain,
	id string,
	expires time.Time,
) FileProviderLease {
	t.Helper()
	var publication causal.OperationID
	publication[0] = 1
	object := ResolvedCriticalObject{
		LogicalID: "settings", Role: "settings", ObjectID: domain.Root,
		ObjectRevision: 1, ContentRevision: 1, Size: 8,
	}
	object.Hash[0] = 1
	policy, err := CriticalObjectPolicyDigest(requirementsFromResolution([]ResolvedCriticalObject{object}))
	if err != nil {
		t.Fatal(err)
	}
	resolution, err := CriticalObjectResolutionDigest(CriticalObjectResolution{
		Authority: causal.SourceAuthorityID(provision.ContentSourceID), Publication: publication,
		Tenant: provision.Tenant, Generation: provision.Generation, Head: 1,
		Objects: []ResolvedCriticalObject{object},
	})
	if err != nil {
		t.Fatal(err)
	}
	return FileProviderLease{
		ID: id, Tenant: provision.Tenant, DomainID: domain.DomainID, Generation: provision.Generation,
		Root: domain.Root, PresentationInstance: domain.PresentationInstance,
		State: FileProviderLeaseProvisional, PolicyDigest: policy, ResolutionDigest: resolution,
		CatalogHead: 1, SourceAuthority: causal.SourceAuthorityID(provision.ContentSourceID),
		SourcePublication: publication, SourceRevision: 1,
		ActivationGeneration: domain.ActivationGeneration, ExpiresAt: expires,
		CriticalObjects: []ResolvedCriticalObject{object},
	}
}

func commitTestFileProviderLease(t *testing.T, store *Catalog, lease FileProviderLease) FileProviderLease {
	t.Helper()
	if _, err := store.PrepareFileProviderLease(t.Context(), lease); err != nil {
		t.Fatalf("PrepareFileProviderLease: %v", err)
	}
	lease.State = FileProviderLeaseCommitted
	lease.SessionID = "session-" + lease.ID
	lease.ProcessIdentity = "pid=42;start=test"
	committed, err := store.CommitFileProviderLease(t.Context(), lease)
	if err != nil {
		t.Fatalf("CommitFileProviderLease: %v", err)
	}
	return committed
}
