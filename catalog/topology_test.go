package catalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/fusekit/causal"
)

func TestTopologyOwnerIsolationAndTransactionalRevisionBumps(t *testing.T) {
	store := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("topology-owner")
	other := SourceAuthorityFleetOwnerID("topology-other")
	provision := topologyTenantProvision(t, owner, "topology-revision", 1)

	created, err := provisionTenantForTest(t, store, t.Context(), provision)
	if err != nil {
		t.Fatal(err)
	}
	assertTopologyRevision(t, store, owner, 1)
	if _, err := provisionTenantForTest(t, store, t.Context(), provision); err != nil {
		t.Fatal(err)
	}
	assertTopologyRevision(t, store, owner, 1)
	foreign, err := store.TopologyHead(t.Context(), other)
	if err != nil || foreign.Owner != other || foreign.Revision != 0 || foreign.Floor != 0 || foreign.Fleet != nil {
		t.Fatalf("foreign topology head = %+v, %v; want canonical empty", foreign, err)
	}

	replacement := provision
	replacement.Generation = 2
	replacement.BackingRoot = filepath.Join(filepath.Dir(provision.BackingRoot), "backing-2")
	if _, err := replaceTenantForTest(t, store, t.Context(), 1, replacement); err != nil {
		t.Fatal(err)
	}
	assertTopologyRevision(t, store, owner, 2)
	if _, err := replaceTenantForTest(t, store, t.Context(), 1, replacement); err != nil {
		t.Fatal(err)
	}
	assertTopologyRevision(t, store, owner, 2)
	if err := removeTenantForTest(t, store, t.Context(), created.Tenant, 2); err != nil {
		t.Fatal(err)
	}
	assertTopologyRevision(t, store, owner, 3)
	if err := removeTenantForTest(t, store, t.Context(), created.Tenant, 2); err != nil {
		t.Fatal(err)
	}
	assertTopologyRevision(t, store, owner, 3)

	stage := topologyFleetStage(t, store, owner, 0, 1, "topology-authority")
	assertTopologyRevision(t, store, owner, 4)
	before, err := store.TopologySnapshot(t.Context(), TopologySnapshotRequest{
		Owner: owner, Revision: 4, Limit: TopologyPageLimit,
	})
	if err != nil || len(before.Tenants) != 0 || len(before.Authorities) != 1 {
		t.Fatalf("desired fleet missing from topology = %+v, %v", before, err)
	}
	if _, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), stage); err != nil {
		t.Fatal(err)
	}
	assertTopologyRevision(t, store, owner, 4)
	if _, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), stage); err != nil {
		t.Fatal(err)
	}
	assertTopologyRevision(t, store, owner, 4)

	after, err := store.TopologySnapshot(t.Context(), TopologySnapshotRequest{
		Owner: owner, Revision: 4, Limit: TopologyPageLimit,
	})
	if err != nil || len(after.Tenants) != 0 || len(after.Authorities) != 1 ||
		after.Authorities[0].Authority != "topology-authority" || after.Authorities[0].DriverID == "" ||
		string(after.Authorities[0].DriverConfig) != "config:topology-authority" {
		t.Fatalf("acknowledged topology = %+v, %v", after, err)
	}
	changes, err := store.TopologyChangesSince(t.Context(), TopologyChangesRequest{
		Owner: owner, After: 0, Limit: TopologyPageLimit,
	})
	if err != nil || len(changes.Changes) != 4 || changes.Changes[0].Kind != TopologyChangeTenant ||
		changes.Changes[1].Kind != TopologyChangeTenant || changes.Changes[2].Kind != TopologyChangeTenant ||
		changes.Changes[3].Kind != TopologyChangeSourceAuthorityFleet || changes.Changes[3].FleetGeneration != 1 {
		t.Fatalf("topology changes = %+v, %v", changes, err)
	}
}

func TestTopologySnapshotExactTenantBoundaryContinuesToAuthorities(t *testing.T) {
	store := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("topology-boundary")
	for index := 0; index < TopologyPageLimit; index++ {
		name := filepath.Base(filepath.Join("tenant", topologyTestOrdinal(index)))
		if _, err := provisionTenantForTest(t, store, t.Context(), topologyTenantProvision(t, owner, name, 1)); err != nil {
			t.Fatalf("provision tenant %d: %v", index, err)
		}
	}
	stage := topologyFleetStage(t, store, owner, 0, 1, "boundary-authority")
	if _, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), stage); err != nil {
		t.Fatal(err)
	}
	head, err := store.TopologyHead(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	request := TopologySnapshotRequest{Owner: owner, Revision: head.Revision, Limit: TopologyPageLimit}
	first, err := store.TopologySnapshot(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := store.TopologySnapshot(t.Context(), request)
	if err != nil || !reflect.DeepEqual(replayed, first) {
		t.Fatalf("snapshot replay = %+v, %v; first %+v", replayed, err, first)
	}
	if len(first.Tenants) != TopologyPageLimit || len(first.Authorities) != 0 ||
		first.Next.Owner != owner || first.Next.Revision != head.Revision ||
		first.Next.Section != TopologySectionAuthorities || first.Next.AfterTenant != "" || first.Next.AfterAuthority != "" {
		t.Fatalf("exact-boundary first page = %+v", first)
	}
	second, err := store.TopologySnapshot(t.Context(), TopologySnapshotRequest{
		Owner: owner, Revision: head.Revision, Cursor: first.Next, Limit: TopologyPageLimit,
	})
	if err != nil || len(second.Tenants) != 0 || len(second.Authorities) != 1 ||
		second.Authorities[0].Authority != "boundary-authority" || second.Next != (TopologyCursor{}) {
		t.Fatalf("exact-boundary authority page = %+v, %v", second, err)
	}
	wrongOwner := first.Next
	wrongOwner.Owner = "other-owner"
	if _, err := store.TopologySnapshot(t.Context(), TopologySnapshotRequest{
		Owner: owner, Revision: head.Revision, Cursor: wrongOwner, Limit: 1,
	}); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("foreign cursor = %v, want invalid object", err)
	}
	wrongRevision := first.Next
	wrongRevision.Revision++
	if _, err := store.TopologySnapshot(t.Context(), TopologySnapshotRequest{
		Owner: owner, Revision: head.Revision, Cursor: wrongRevision, Limit: 1,
	}); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("cross-revision cursor = %v, want invalid object", err)
	}
}

func TestTopologyChangesRejectCompactedRevision(t *testing.T) {
	store := newTestCatalog(t)
	owner := SourceAuthorityFleetOwnerID("topology-compacted")
	if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO desired_topology_heads(owner_id, revision, floor, tenant_count)
VALUES (?, 9, 5, 1)`, string(owner)); err != nil {
		t.Fatal(err)
	}
	for revision := 5; revision <= 9; revision++ {
		if _, err := store.db.ExecContext(t.Context(), `
INSERT INTO desired_topology_changes(owner_id, revision, kind, tenant, fleet_generation)
VALUES (?, ?, ?, ?, 0)`, string(owner), revision, uint8(TopologyChangeTenant), "tenant"); err != nil {
			t.Fatal(err)
		}
	}
	_, err := store.TopologyChangesSince(t.Context(), TopologyChangesRequest{Owner: owner, After: 3, Limit: 1})
	var stale *StaleTopologyRevisionError
	if !errors.As(err, &stale) || stale.Revision != 3 || stale.Floor != 5 {
		t.Fatalf("compacted topology changes = %v, want stale revision 3 floor 5", err)
	}
	page, err := store.TopologyChangesSince(t.Context(), TopologyChangesRequest{Owner: owner, After: 4, Limit: 2})
	if err != nil || len(page.Changes) != 2 || page.Changes[0].Revision != 5 || page.Changes[1].Revision != 6 || page.Next != 6 {
		t.Fatalf("floor topology changes = %+v, %v", page, err)
	}
}

func TestWaitTopologyChangesHasNoLostWakeupAndSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	store, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	owner := SourceAuthorityFleetOwnerID("topology-wait")
	first := topologyTenantProvision(t, owner, "wait-first", 1)
	if _, err := provisionTenantForTest(t, store, t.Context(), first); err != nil {
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
	defer func() { _ = store.Close() }()

	type result struct {
		page TopologyChangePage
		err  error
	}
	waiting := make(chan result, 1)
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelWait()
	go func() {
		page, err := store.WaitTopologyChanges(waitCtx, TopologyChangesRequest{
			Owner: owner, After: head.Revision, Limit: TopologyPageLimit,
		})
		waiting <- result{page: page, err: err}
	}()
	second := topologyTenantProvision(t, owner, "wait-second", 1)
	if _, err := provisionTenantForTest(t, store, t.Context(), second); err != nil {
		t.Fatal(err)
	}
	got := <-waiting
	if got.err != nil || len(got.page.Changes) != 1 || got.page.Changes[0].Tenant != second.Tenant {
		t.Fatalf("reopened topology wait = %+v, %v", got.page, got.err)
	}

	third := topologyTenantProvision(t, owner, "wait-third", 1)
	beforeThird := got.page.Head.Revision
	if _, err := provisionTenantForTest(t, store, t.Context(), third); err != nil {
		t.Fatal(err)
	}
	immediate, err := store.WaitTopologyChanges(t.Context(), TopologyChangesRequest{
		Owner: owner, After: beforeThird, Limit: TopologyPageLimit,
	})
	if err != nil || len(immediate.Changes) != 1 || immediate.Changes[0].Tenant != third.Tenant {
		t.Fatalf("preexisting topology wakeup = %+v, %v", immediate, err)
	}

	current, err := store.TopologyHead(t.Context(), owner)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := store.WaitTopologyChanges(cancelled, TopologyChangesRequest{
		Owner: owner, After: current.Revision, Limit: 1,
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled topology wait = %v, want context canceled", err)
	}
}

func topologyTenantProvision(
	t *testing.T,
	owner SourceAuthorityFleetOwnerID,
	name string,
	generation Generation,
) TenantProvision {
	t.Helper()
	provision := testTenantProvision(t, name, generation)
	provision.OwnerID = string(owner)
	return provision
}

func topologyFleetStage(
	t *testing.T,
	store *Catalog,
	owner SourceAuthorityFleetOwnerID,
	expected causal.Generation,
	generation causal.Generation,
	authorities ...causal.SourceAuthorityID,
) SourceAuthorityFleetAcknowledgement {
	t.Helper()
	declarations := make([]SourceAuthorityDeclaration, len(authorities))
	for index, authority := range authorities {
		declarations[index] = SourceAuthorityDeclaration{
			Authority: authority, DriverID: "driver." + string(authority),
			DriverConfig:      []byte("config:" + authority),
			DeclarationDigest: sha256.Sum256([]byte("declaration:" + authority)),
		}
	}
	authoritiesDigest, err := SourceAuthorityFleetDigest(authorities)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	state, err := store.ReconcileSourceAuthorityFleet(t.Context(), SourceAuthorityFleetReconcileRequest{
		Owner: owner, ExpectedGeneration: expected, Generation: generation,
		Declarations: declarations, Complete: true, AuthorityCount: uint64(len(authorities)),
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	return SourceAuthorityFleetAcknowledgement{
		Owner: owner, ExpectedGeneration: expected, Generation: generation,
		AuthorityCount: uint64(len(authorities)), AuthoritiesDigest: authoritiesDigest,
		DeclarationsDigest: declarationsDigest, StageDigest: state.StageDigest,
	}
}

func assertTopologyRevision(t *testing.T, store *Catalog, owner SourceAuthorityFleetOwnerID, want TopologyRevision) {
	t.Helper()
	head, err := store.TopologyHead(t.Context(), owner)
	if err != nil || head.Owner != owner || head.Revision != want {
		t.Fatalf("topology head = %+v, %v; want revision %d", head, err, want)
	}
}

func topologyTestOrdinal(value int) string {
	const digits = "0123456789abcdef"
	return string([]byte{'t', digits[(value>>8)&0xf], digits[(value>>4)&0xf], digits[value&0xf]})
}
