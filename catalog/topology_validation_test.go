package catalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSourceDriverIDContractIsExactASCII(t *testing.T) {
	for _, valid := range []string{
		"cc-pool.claude-authority", "Driver_01-alpha.beta", strings.Repeat("a", SourceDriverIDMaxBytes),
	} {
		if err := ValidateSourceDriverID(valid); err != nil {
			t.Fatalf("valid driver ID %q: %v", valid, err)
		}
	}
	for _, invalid := range []string{
		"", strings.Repeat("a", SourceDriverIDMaxBytes+1), "driver/name", "driver name", "drivér", "driver:1",
	} {
		if err := ValidateSourceDriverID(invalid); !errors.Is(err, ErrInvalidObject) {
			t.Fatalf("invalid driver ID %q = %v", invalid, err)
		}
	}
}

func TestSourceDriverConfigIsBoundedAndDeclarationBound(t *testing.T) {
	base := SourceAuthorityDeclaration{
		Authority: "authority", DriverID: "driver",
		DriverConfig: []byte("first"), DeclarationDigest: sha256.Sum256([]byte("declaration")),
	}
	if err := validateSourceAuthorityDeclarations([]SourceAuthorityDeclaration{base}); err != nil {
		t.Fatalf("valid driver config: %v", err)
	}
	first, err := SourceAuthorityFleetDeclarationsDigest([]SourceAuthorityDeclaration{base})
	if err != nil {
		t.Fatal(err)
	}
	changed := base
	changed.DriverConfig = []byte("second")
	second, err := SourceAuthorityFleetDeclarationsDigest([]SourceAuthorityDeclaration{changed})
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("driver config bytes did not change declarations digest")
	}
	changed.DriverConfig = make([]byte, SourceDriverConfigMaxBytes+1)
	if err := validateSourceAuthorityDeclarations([]SourceAuthorityDeclaration{changed}); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("oversized driver config = %v, want ErrInvalidObject", err)
	}
}

func TestTopologyEmptyOwnerWaitsForFirstProvision(t *testing.T) {
	store := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("topology-first-provision")
	head, err := store.TopologyHead(t.Context(), owner)
	if err != nil || head.Owner != owner || head.Revision != 0 || head.Floor != 0 || head.Fleet != nil {
		t.Fatalf("empty topology head = %+v, %v", head, err)
	}
	page, err := store.TopologySnapshot(t.Context(), TopologySnapshotRequest{
		Owner: owner, Revision: 0, Limit: TopologyPageLimit,
	})
	if err != nil || page.Head != head || len(page.Tenants) != 0 || len(page.Authorities) != 0 {
		t.Fatalf("empty topology snapshot = %+v, %v", page, err)
	}
	changes, err := store.TopologyChangesSince(t.Context(), TopologyChangesRequest{
		Owner: owner, After: 0, Limit: TopologyPageLimit,
	})
	if err != nil || changes.Head != head || len(changes.Changes) != 0 {
		t.Fatalf("empty topology changes = %+v, %v", changes, err)
	}

	waitContext, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	waited := make(chan TopologyChangePage, 1)
	waitErr := make(chan error, 1)
	go func() {
		result, err := store.WaitTopologyChanges(waitContext, TopologyChangesRequest{
			Owner: owner, After: 0, Limit: TopologyPageLimit,
		})
		if err != nil {
			waitErr <- err
			return
		}
		waited <- result
	}()
	if _, err := store.ProvisionTenant(t.Context(), topologyTenantProvision(t, owner, "first", 1)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitErr:
		t.Fatal(err)
	case result := <-waited:
		if result.Head.Revision != 1 || len(result.Changes) != 1 || result.Changes[0].Revision != 1 {
			t.Fatalf("first topology change = %+v", result)
		}
	case <-waitContext.Done():
		t.Fatal(waitContext.Err())
	}
}

func TestTopologyValidatorsRejectAdversarialWorkerPages(t *testing.T) {
	owner := SourceAuthorityFleetOwnerID("topology-validator")
	digest := sha256.Sum256([]byte("topology-validator"))
	fleet := DesiredSourceAuthorityFleetState{
		Owner: owner, Generation: 3, AuthorityCount: 1,
		AuthoritiesDigest: digest, DeclarationsDigest: digest,
	}
	head := TopologyHeadState{Owner: owner, Revision: 8, Floor: 1, Fleet: &fleet}
	emptyHead := TopologyHeadState{Owner: owner}
	if err := (TopologySnapshotPage{
		Head: emptyHead,
		Next: TopologyCursor{Owner: owner, Section: TopologySectionAuthorities},
	}).Validate(TopologySnapshotRequest{Owner: owner, Revision: 0, Limit: 1}); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("empty topology accepted fabricated cursor: %v", err)
	}

	snapshotRequest := TopologySnapshotRequest{
		Owner: owner, Revision: 8, Limit: 1,
		Cursor: TopologyCursor{Owner: owner, Revision: 8, Section: TopologySectionAuthorities},
	}
	malformedSnapshot := TopologySnapshotPage{
		Head:    head,
		Tenants: []TenantProvision{{OwnerID: string(owner), Tenant: "late-tenant"}},
	}
	if err := malformedSnapshot.Validate(snapshotRequest); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("authority cursor accepted tenant rows: %v", err)
	}
	boundaryRequest := TopologySnapshotRequest{Owner: owner, Revision: 8, Limit: 1}
	malformedBoundary := TopologySnapshotPage{
		Head: head,
		Next: TopologyCursor{
			Owner: owner, Revision: 8, Section: TopologySectionAuthorities,
			AfterAuthority: "skipped-authority",
		},
	}
	if err := malformedBoundary.Validate(boundaryRequest); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("boundary cursor accepted skipped authority: %v", err)
	}
	interleavedHead := head
	interleavedHead.TenantCount = 2
	interleaved := TopologySnapshotPage{
		Head:    interleavedHead,
		Tenants: []TenantProvision{topologyTenantProvision(t, owner, "interleaved", 1)},
		Authorities: []TopologySourceAuthority{{
			Owner: owner, FleetGeneration: fleet.Generation, Authority: "early-authority",
			DriverID: "driver", DeclarationDigest: digest,
		}},
		Next: TopologyCursor{
			Owner: owner, Revision: 8, Section: TopologySectionTenants,
			AfterTenant: "tenant", TenantOffset: 1,
		},
	}
	if err := interleaved.Validate(boundaryRequest); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("interleaved authority page accepted: %v", err)
	}
	missingTenantHead := head
	missingTenantHead.TenantCount = 1
	if err := (TopologySnapshotPage{Head: missingTenantHead}).Validate(boundaryRequest); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("early terminal tenant page accepted: %v", err)
	}

	changesRequest := TopologyChangesRequest{Owner: owner, After: 4, Limit: 2}
	for name, page := range map[string]TopologyChangePage{
		"skipped revision": {
			Head:    head,
			Changes: []TopologyChange{{Revision: 6, Kind: TopologyChangeTenant, Tenant: "tenant"}},
		},
		"empty before head": {Head: head},
		"false terminal": {
			Head:    head,
			Changes: []TopologyChange{{Revision: 5, Kind: TopologyChangeTenant, Tenant: "tenant"}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := page.Validate(changesRequest); !errors.Is(err, ErrIntegrity) {
				t.Fatalf("malformed change page accepted: %v", err)
			}
		})
	}
	if err := (TopologyChangePage{Head: head}).Validate(TopologyChangesRequest{
		Owner: owner, After: 9, Limit: 1,
	}); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("cursor ahead of head accepted: %v", err)
	}
	compacted := head
	compacted.Floor = 6
	if err := (TopologyChangePage{Head: compacted}).Validate(TopologyChangesRequest{
		Owner: owner, After: 4, Limit: 1,
	}); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("cursor below floor accepted: %v", err)
	}
}

func TestTopologySnapshotRecoversAcknowledgedEmptyFleetAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	owner := SourceAuthorityFleetOwnerID("topology-empty-fleet")
	first := topologyFleetStage(t, store, owner, 0, 1, "retired-authority")
	if _, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	empty := topologyFleetStage(t, store, owner, 1, 2)
	if _, err := store.RetireSourceAuthority(t.Context(), SourceAuthorityRetireRequest{
		Owner: owner, ExpectedGeneration: 1, Generation: 2,
		Authority: "retired-authority", StageDigest: empty.StageDigest,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), empty); err != nil {
		t.Fatal(err)
	}
	head, err := store.TopologyHead(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	page, err := store.TopologySnapshot(t.Context(), TopologySnapshotRequest{
		Owner: owner, Revision: head.Revision, Limit: TopologyPageLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if page.Head.Fleet == nil || page.Head.Fleet.Generation != 2 ||
		page.Head.Fleet.AuthorityCount != 0 || len(page.Authorities) != 0 {
		t.Fatalf("reopened empty fleet topology = %+v", page)
	}
}
