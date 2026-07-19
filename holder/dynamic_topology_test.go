package holder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/tenant"
)

type recordingFleetReplacer struct {
	calls   int
	next    *authorityRegistry
	tenants []tenant.TenantSpec
}

func (r *recordingFleetReplacer) replace(
	_ context.Context,
	next *authorityRegistry,
	tenants []tenant.TenantSpec,
) error {
	r.calls++
	r.next = next
	r.tenants = append([]tenant.TenantSpec(nil), tenants...)
	return nil
}

func TestDynamicTopologyControllerAppliesFirstReplaceAndEmptyFleet(t *testing.T) {
	drivers, err := NewDriverFactories(map[string]DriverFactory{
		"git-v1": {Semantic: func(context.Context, SourceDriverInvocation) (sourcedriver.Driver, error) {
			return nil, nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	replacer := &recordingFleetReplacer{}
	var built []SourceAuthorityFleet
	controller := &topologyController{
		drivers: drivers, authorities: replacer,
		build: func(fleet SourceAuthorityFleet) (*authorityRegistry, error) {
			built = append(built, fleet)
			return &authorityRegistry{fleetOwner: fleet.Owner, fleetGeneration: fleet.Generation}, nil
		},
		current: desiredTopology{Head: catalog.TopologyHeadState{Owner: "product"}},
	}
	first := desiredSemanticTopology(t, "product", 1, "source", []byte("repo=/one"))
	if err := controller.apply(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	if replacer.calls != 1 || len(built) != 1 || built[0].Generation != 1 || len(built[0].Authorities) != 1 {
		t.Fatalf("first desired fleet = calls %d, built %+v", replacer.calls, built)
	}
	spec, ok := built[0].Authorities[0].(SemanticDriverSpec)
	if !ok || spec.DriverID != "git-v1" || !bytes.Equal(spec.DriverConfig, []byte("repo=/one")) {
		t.Fatalf("first source declaration = %#v", built[0].Authorities[0])
	}

	noOp := first
	noOp.Head.Revision++
	if err := controller.apply(t.Context(), noOp); err != nil {
		t.Fatal(err)
	}
	if replacer.calls != 1 {
		t.Fatalf("unchanged fleet replacements = %d, want 1", replacer.calls)
	}

	replacement := desiredSemanticTopology(t, "product", 2, "source", []byte("repo=/two"))
	if err := controller.apply(t.Context(), replacement); err != nil {
		t.Fatal(err)
	}
	if replacer.calls != 2 || len(built) != 2 || built[1].Generation != 2 {
		t.Fatalf("replacement fleet = calls %d, built %+v", replacer.calls, built)
	}

	empty := desiredEmptyTopology(t, "product", 3)
	if err := controller.apply(t.Context(), empty); err != nil {
		t.Fatal(err)
	}
	if replacer.calls != 3 || len(built) != 3 || built[2].Generation != 3 || len(built[2].Authorities) != 0 {
		t.Fatalf("empty fleet replacement = calls %d, built %+v", replacer.calls, built)
	}
}

type staleTopologyStore struct {
	mu       sync.Mutex
	owner    catalog.SourceAuthorityFleetOwnerID
	initial  desiredTopology
	advanced desiredTopology
	current  desiredTopology
}

func (s *staleTopologyStore) TopologyHead(
	_ context.Context,
	_ catalog.SourceAuthorityFleetOwnerID,
) (catalog.TopologyHeadState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current.Head, nil
}

func (s *staleTopologyStore) TopologySnapshot(
	_ context.Context,
	request catalog.TopologySnapshotRequest,
) (catalog.TopologySnapshotPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var topology desiredTopology
	switch request.Revision {
	case s.initial.Head.Revision:
		topology = s.initial
	case s.advanced.Head.Revision:
		topology = s.advanced
	default:
		return catalog.TopologySnapshotPage{}, catalog.ErrGenerationMismatch
	}
	return catalog.TopologySnapshotPage{
		Head: topology.Head, Tenants: append([]catalog.TenantProvision(nil), topology.Tenants...),
		Authorities: append([]catalog.TopologySourceAuthority(nil), topology.Authorities...),
	}, nil
}

func (s *staleTopologyStore) TopologyChangesSince(
	_ context.Context,
	request catalog.TopologyChangesRequest,
) (catalog.TopologyChangePage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return catalog.TopologyChangePage{Head: s.current.Head}, nil
}

func (s *staleTopologyStore) WaitTopologyChanges(
	ctx context.Context,
	request catalog.TopologyChangesRequest,
) (catalog.TopologyChangePage, error) {
	s.mu.Lock()
	if request.After >= s.advanced.Head.Revision {
		s.mu.Unlock()
		<-ctx.Done()
		return catalog.TopologyChangePage{}, ctx.Err()
	}
	s.current = s.advanced
	s.mu.Unlock()
	return catalog.TopologyChangePage{}, &catalog.StaleTopologyRevisionError{
		Revision: request.After, Floor: s.advanced.Head.Revision,
	}
}

func TestTopologyReconcilerResnapshotsAfterStaleCursor(t *testing.T) {
	owner := catalog.SourceAuthorityFleetOwnerID("product")
	initial := desiredTopology{Head: catalog.TopologyHeadState{Owner: owner, Revision: 1, Floor: 1}}
	advanced := desiredSemanticTopology(t, owner, 2, "source", []byte("repo=/advanced"))
	advanced.Head.Floor = 2
	store := &staleTopologyStore{owner: owner, initial: initial, advanced: advanced, current: initial}
	ctx, cancel := context.WithCancel(t.Context())
	var applied []desiredTopology
	reconciler := topologyReconciler{
		store: store, owner: owner,
		apply: func(_ context.Context, topology desiredTopology) error {
			applied = append(applied, topology)
			if len(applied) == 2 {
				cancel()
			}
			return nil
		},
	}
	err := reconciler.run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("reconciler termination = %v, want canceled", err)
	}
	if len(applied) != 2 || applied[0].Head.Revision != 1 || applied[1].Head.Revision != 2 ||
		len(applied[1].Authorities) != 1 {
		t.Fatalf("applied resnapshots = %+v", applied)
	}
}

func TestAuthorityRouterHardReplacesAndAcknowledgesDesiredFleet(t *testing.T) {
	store := openHolderFleetCatalog(t)
	router := &authorityRouter{}
	firstSpec := testSourceAuthoritySpec("source")
	firstSpec.DriverConfig = []byte("root=/first")
	publishDesiredFleetForRouterTest(t, store, 0, 1, firstSpec)
	firstRuntime := newTestAuthority()
	first := newHolderFleetRegistry(
		t, store, SourceAuthorityFleet{Owner: "holder-test", Generation: 1, Authorities: []SourceAuthoritySpec{firstSpec}},
		func(context.Context, sourceauthority.Config) (managedAuthority, error) { return firstRuntime, nil },
	)
	if err := router.replace(t.Context(), first, nil); err != nil {
		t.Fatal(err)
	}
	assertAppliedFleetGeneration(t, store, 1)

	secondSpec := testSourceAuthoritySpec("source")
	secondSpec.DriverConfig = []byte("root=/second")
	secondSpec.DeclarationDigest = sha256.Sum256([]byte("declaration:second"))
	publishDesiredFleetForRouterTest(t, store, 1, 2, secondSpec)
	secondRuntime := newTestAuthority()
	second := newHolderFleetRegistry(
		t, store, SourceAuthorityFleet{Owner: "holder-test", Generation: 2, Authorities: []SourceAuthoritySpec{secondSpec}},
		func(context.Context, sourceauthority.Config) (managedAuthority, error) { return secondRuntime, nil },
	)
	if err := router.replace(t.Context(), second, nil); err != nil {
		t.Fatal(err)
	}
	firstRuntime.mu.Lock()
	firstCloseCalls, firstWaitCalls := firstRuntime.closeCalls, firstRuntime.waitCalls
	firstRuntime.mu.Unlock()
	if firstCloseCalls == 0 || firstWaitCalls == 0 {
		t.Fatalf("prior runtime settlement = close %d, wait %d", firstCloseCalls, firstWaitCalls)
	}
	assertAppliedFleetGeneration(t, store, 2)

	publishDesiredFleetForRouterTest(t, store, 2, 3)
	empty := newHolderFleetRegistry(
		t, store, SourceAuthorityFleet{Owner: "holder-test", Generation: 3},
		func(context.Context, sourceauthority.Config) (managedAuthority, error) {
			t.Fatal("empty fleet constructed an authority runtime")
			return nil, nil
		},
	)
	if err := router.replace(t.Context(), empty, nil); err != nil {
		t.Fatal(err)
	}
	assertAppliedFleetGeneration(t, store, 3)
	if err := router.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func publishDesiredFleetForRouterTest(
	t *testing.T,
	store *catalog.Catalog,
	expected causal.Generation,
	generation causal.Generation,
	specs ...SourceAuthoritySpec,
) {
	t.Helper()
	declarations := make([]catalog.SourceAuthorityDeclaration, len(specs))
	for index, spec := range specs {
		authority, digest := sourceAuthorityIdentity(spec)
		declarations[index] = catalog.SourceAuthorityDeclaration{
			Authority: authority, DriverID: sourceAuthorityDriverID(spec),
			DriverConfig:      append([]byte(nil), sourceAuthorityDriverConfig(spec)...),
			DeclarationDigest: digest,
		}
	}
	if _, err := store.PublishDesiredSourceFleet(t.Context(), catalog.PublishDesiredSourceFleetRequest{
		Owner: "holder-test", ExpectedGeneration: expected, Generation: generation,
		Declarations: declarations,
	}); err != nil {
		t.Fatal(err)
	}
}

func assertAppliedFleetGeneration(t *testing.T, store *catalog.Catalog, generation causal.Generation) {
	t.Helper()
	status, err := store.SourceAuthorityFleetHead(t.Context(), "holder-test")
	if err != nil {
		t.Fatal(err)
	}
	if status.Current == nil || status.Current.Generation != generation || status.Pending != nil {
		t.Fatalf("applied fleet status = %+v, want generation %d and no pending", status, generation)
	}
}

func desiredSemanticTopology(
	t *testing.T,
	owner catalog.SourceAuthorityFleetOwnerID,
	generation causal.Generation,
	authority causal.SourceAuthorityID,
	config []byte,
) desiredTopology {
	t.Helper()
	declaration := catalog.SourceAuthorityDeclaration{
		Authority: authority, DriverID: "git-v1", DriverConfig: append([]byte(nil), config...),
		DeclarationDigest: sha256.Sum256(append([]byte("declaration:"), config...)),
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest([]causal.SourceAuthorityID{authority})
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest([]catalog.SourceAuthorityDeclaration{declaration})
	if err != nil {
		t.Fatal(err)
	}
	fleet := catalog.DesiredSourceAuthorityFleetState{
		Owner: owner, Generation: generation, AuthorityCount: 1,
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
	}
	return desiredTopology{
		Head: catalog.TopologyHeadState{Owner: owner, Revision: catalog.TopologyRevision(generation), Fleet: &fleet},
		Authorities: []catalog.TopologySourceAuthority{{
			Owner: owner, FleetGeneration: generation, Authority: authority,
			DriverID: declaration.DriverID, DriverConfig: append([]byte(nil), declaration.DriverConfig...),
			DeclarationDigest: declaration.DeclarationDigest,
		}},
	}
}

func desiredEmptyTopology(
	t *testing.T,
	owner catalog.SourceAuthorityFleetOwnerID,
	generation causal.Generation,
) desiredTopology {
	t.Helper()
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(nil)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(nil)
	if err != nil {
		t.Fatal(err)
	}
	fleet := catalog.DesiredSourceAuthorityFleetState{
		Owner: owner, Generation: generation,
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
	}
	return desiredTopology{
		Head: catalog.TopologyHeadState{Owner: owner, Revision: catalog.TopologyRevision(generation), Fleet: &fleet},
	}
}
