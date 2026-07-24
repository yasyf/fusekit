package holder

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

func TestLocalTenantControllerDelegatesLifecycleAndComposesExactProof(t *testing.T) {
	bootstrap := &bootstrapGate{}
	bootstrap.open()
	lifecycle := newLocalTestLifecycle()
	sibling := localTestDeclaration("authority-a", "driver-a")
	declaration := localTestDeclaration("authority-b", "driver-b")
	fleets := &localTestSourceFleets{
		state:        localTestFleetState(t, "product", 1, []catalog.SourceAuthorityDeclaration{sibling}),
		declarations: []catalog.SourceAuthorityDeclaration{sibling},
	}
	preparation := &localTestPreparation{}
	scope := newLocalControllerScope()
	defer scope.close()
	runtime := &Runtime{
		config: Config{Owner: "product", RuntimeBuild: "build-v1"},
	}
	graph := &runtimeGraph{
		readiness: &runtimeReadiness{bootstrap: bootstrap}, tenantLifecycle: lifecycle,
		tenantPreparation: preparation, sourceFleets: fleets, tenantSpecs: lifecycle, tenantRetirements: lifecycle,
		presentationLeases: localTestLeaseStore{}, activationGeneration: "activation-7",
	}
	controller := &LocalTenantController{runtime: runtime, owner: "product", graph: graph, scope: scope}
	spec := localTestSpec(filepath.Join(t.TempDir(), "Library", "CloudStorage"), "authority-b", 1)

	foreign := spec
	foreign.OwnerID = "foreign"
	if _, err := controller.Provision(t.Context(), foreign); !errors.Is(err, tenant.ErrTenantOwnerMismatch) {
		t.Fatalf("foreign Provision = %v, want owner mismatch", err)
	}
	ack, err := controller.Provision(t.Context(), spec)
	if err != nil || ack != localTenantAcknowledgement(spec) {
		t.Fatalf("Provision = %+v, %v", ack, err)
	}
	status, err := controller.State(t.Context(), spec.ID)
	if err != nil || status.State.Generation != spec.Generation {
		t.Fatalf("State = %+v, %v", status, err)
	}
	next := spec
	next.Generation = 2
	ack, err = controller.Replace(t.Context(), spec.Generation, next)
	if err != nil || ack.Generation != next.Generation {
		t.Fatalf("Replace = %+v, %v", ack, err)
	}
	removed, err := controller.Retire(t.Context(), next.ID, next.Generation)
	if err != nil || !removed.FileProviderAbsent || removed.Tenant != next.ID || removed.Generation != next.Generation {
		t.Fatalf("Remove = %+v, %v", removed, err)
	}

	request := LocalProvisionRequest{
		Declaration: declaration, Tenant: spec,
		Preparation: LocalPreparationRequest{Generation: spec.Generation, Presentation: catalog.PresentationMount},
	}
	preparation.proof = localTestPreparationProof(request)
	proof, err := controller.ProvisionAndPrepare(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if proof.Fleet.Generation != 2 || proof.Fleet.AuthorityCount != 2 || proof.Tenant.Tenant != spec.ID {
		t.Fatalf("ProvisionAndPrepare = %+v", proof)
	}
	fleets.mu.Lock()
	published := append([]catalog.SourceAuthorityDeclaration(nil), fleets.declarations...)
	fleets.mu.Unlock()
	if len(published) != 2 || published[0].Authority != sibling.Authority || published[1].Authority != declaration.Authority {
		t.Fatalf("published declarations = %+v", published)
	}
}

func TestLocalAndWireTenantLifecycleShareColdPresentationState(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	runtime, err := New(t.Context(), testConfig(dir, "local-wire-v1", native))
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	controller := runtime.LocalTenantController()
	readiness, err := controller.Readiness(t.Context())
	if err != nil || readiness.RuntimeBuild != "local-wire-v1" || readiness.ActivationGeneration == "" {
		t.Fatalf("Readiness = %+v, %v", readiness, err)
	}
	spec := tenant.TenantSpec{
		OwnerID: "holder-test", ID: "local-wire",
		Mount:   tenant.MountSpec{PresentationRoot: filepath.Join(testPresentationRoot(dir), "local-wire")},
		Backing: tenant.BackingSpec{Root: filepath.Join(dir, "backing")},
		Content: tenant.ContentSource{ID: "source-local-wire"},
		Traits: tenant.TenantTraits{
			Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount,
		},
		Generation: 1,
	}
	if _, err := controller.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if starts, _ := native.counts(); starts != 1 {
		t.Fatalf("cold local provision native starts = %d, want one", starts)
	}
	client, err := mountservice.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(filepath.Join(dir, "fusekit.sock")), WireBuild: transportproto.WireBuild,
		Role: trust.UnprotectedRole,
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := client.State(t.Context(), spec.ID)
	if err != nil || state.Code != mountproto.ErrorCodeOk || state.State == nil || state.State.Generation != 1 {
		t.Fatalf("wire State after local Provision = %+v, %v", state, err)
	}
	next := spec
	next.Generation = 2
	definition := mountproto.TenantDefinition{
		Mount:       &mountproto.MountSpec{PresentationRoot: next.Mount.PresentationRoot},
		BackingRoot: next.Backing.Root, ContentSourceID: next.Content.ID,
		AccessMode: mountproto.AccessModeReadWrite, CasePolicy: mountproto.CasePolicySensitive,
		Presentations: []mountproto.Presentation{mountproto.PresentationMount}, Generation: 2,
	}
	response, err := client.ReplaceTenant(t.Context(), next.ID, spec.Generation, definition)
	if err != nil || response.Code != mountproto.ErrorCodeOk {
		t.Fatalf("wire Replace = %+v, %v", response, err)
	}
	localState, err := controller.State(t.Context(), next.ID)
	if err != nil || localState.State.Generation != next.Generation {
		t.Fatalf("local State after wire Replace = %+v, %v", localState, err)
	}
	removed, err := controller.Retire(t.Context(), next.ID, next.Generation)
	if err != nil || !removed.FileProviderAbsent {
		t.Fatalf("local Remove = %+v, %v", removed, err)
	}
	replayed, err := controller.Retire(t.Context(), next.ID, next.Generation)
	if err != nil || replayed != removed {
		t.Fatalf("local Retire replay = %+v, %v", replayed, err)
	}
	closeRuntime(t, runtime, done)
	_ = client.Close()
	if _, err := controller.State(t.Context(), next.ID); !errors.Is(err, ErrLocalTenantControllerUnavailable) {
		t.Fatalf("State after settlement = %v, want unavailable", err)
	}
	restarted, err := New(t.Context(), testConfig(dir, "local-wire-v2", newTestNative(nil)))
	if err != nil {
		t.Fatal(err)
	}
	restartedDone := runRuntime(t, restarted)
	waitRuntimeReady(t, restarted, restartedDone)
	if _, err := restarted.LocalTenantController().Readiness(t.Context()); err != nil {
		t.Fatal(err)
	}
	restartedProof, err := restarted.LocalTenantController().Retire(t.Context(), next.ID, next.Generation)
	if err != nil || restartedProof != removed {
		t.Fatalf("Retire after restart = %+v, %v", restartedProof, err)
	}
	closeRuntime(t, restarted, restartedDone)
}

type localTestLifecycle struct {
	mu      sync.Mutex
	specs   map[catalog.TenantID]tenant.TenantSpec
	retired map[catalog.TenantID]catalog.Generation
}

func newLocalTestLifecycle() *localTestLifecycle {
	return &localTestLifecycle{
		specs: make(map[catalog.TenantID]tenant.TenantSpec), retired: make(map[catalog.TenantID]catalog.Generation),
	}
}

func (r *localTestLifecycle) ProvisionTenant(_ context.Context, spec tenant.TenantSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current, ok := r.specs[spec.ID]; ok && current != spec {
		return tenant.ErrTenantConflict
	}
	r.specs[spec.ID] = spec
	return nil
}

func (r *localTestLifecycle) ReplaceTenant(_ context.Context, expected catalog.Generation, next tenant.TenantSpec) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.specs[next.ID]
	if !ok {
		return tenant.ErrTenantNotFound
	}
	if current.Generation != expected {
		return tenant.ErrGenerationConflict
	}
	r.specs[next.ID] = next
	return nil
}

func (r *localTestLifecycle) RemoveTenant(_ context.Context, id catalog.TenantID, expected catalog.Generation, owner tenant.OwnerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.specs[id]
	if !ok {
		return nil
	}
	if current.OwnerID != owner {
		return tenant.ErrTenantOwnerMismatch
	}
	if current.Generation != expected {
		return tenant.ErrGenerationConflict
	}
	delete(r.specs, id)
	r.retired[id] = expected
	return nil
}

func (r *localTestLifecycle) ProveTenantRetired(
	_ context.Context,
	owner string,
	id catalog.TenantID,
	generation catalog.Generation,
) (catalog.TenantRetirementProof, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if owner != "product" || r.retired[id] != generation {
		return catalog.TenantRetirementProof{}, catalog.ErrNotFound
	}
	return catalog.TenantRetirementProof{Tenant: id, Generation: generation, FileProviderAbsent: true}, nil
}

func (r *localTestLifecycle) State(_ context.Context, id catalog.TenantID, owner tenant.OwnerID) (tenant.TenantStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.specs[id]
	if !ok {
		return tenant.TenantStatus{}, tenant.ErrTenantNotFound
	}
	if current.OwnerID != owner {
		return tenant.TenantStatus{}, tenant.ErrTenantOwnerMismatch
	}
	return tenant.TenantStatus{
		Owner: owner, ReplacementEligible: true,
		State: tenant.TenantState{Tenant: id, Generation: current.Generation, Activated: current.Generation},
	}, nil
}

func (r *localTestLifecycle) Specs() []tenant.TenantSpec {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]tenant.TenantSpec, 0, len(r.specs))
	for _, spec := range r.specs {
		result = append(result, spec)
	}
	slices.SortFunc(result, func(left, right tenant.TenantSpec) int {
		return strings.Compare(string(left.ID), string(right.ID))
	})
	return result
}

type localTestPreparation struct {
	proof catalogproto.TenantPreparationProof
}

func (p *localTestPreparation) PrepareTenant(
	context.Context,
	catalogservice.Identity,
	catalog.TenantID,
	catalogproto.PrepareTenantRequest,
) (catalogproto.TenantPreparationProof, error) {
	return p.proof, nil
}

type localTestSourceFleets struct {
	mu           sync.Mutex
	state        catalog.DesiredSourceAuthorityFleetState
	declarations []catalog.SourceAuthorityDeclaration
}

func (s *localTestSourceFleets) DesiredSourceFleetPage(
	context.Context,
	catalog.DesiredSourceFleetPageRequest,
) (catalog.DesiredSourceFleetPage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return catalog.DesiredSourceFleetPage{State: s.state, Declarations: append([]catalog.SourceAuthorityDeclaration(nil), s.declarations...)}, nil
}

func (s *localTestSourceFleets) PublishDesiredSourceFleet(
	_ context.Context,
	request catalog.PublishDesiredSourceFleetRequest,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.declarations = append([]catalog.SourceAuthorityDeclaration(nil), request.Declarations...)
	s.state.Owner = request.Owner
	s.state.Generation = request.Generation
	s.state.AuthorityCount = uint64(len(request.Declarations))
	authorities := make([]causal.SourceAuthorityID, len(request.Declarations))
	for index, declaration := range request.Declarations {
		authorities[index] = declaration.Authority
	}
	s.state.AuthoritiesDigest, _ = catalog.SourceAuthorityFleetDigest(authorities)
	s.state.DeclarationsDigest, _ = catalog.SourceAuthorityFleetDeclarationsDigest(request.Declarations)
	return s.state, nil
}

type localTestLeaseStore struct{}

func (localTestLeaseStore) PrepareFileProviderLease(_ context.Context, lease catalog.FileProviderLease) (catalog.FileProviderLease, error) {
	return lease, nil
}

func (localTestLeaseStore) CommitFileProviderLease(_ context.Context, lease catalog.FileProviderLease) (catalog.FileProviderLease, error) {
	return lease, nil
}

func (localTestLeaseStore) RenewFileProviderLease(_ context.Context, lease catalog.FileProviderLease) (catalog.FileProviderLease, error) {
	return lease, nil
}

func (localTestLeaseStore) ReleaseFileProviderLease(_ context.Context, lease catalog.FileProviderLease) (catalog.FileProviderLease, error) {
	lease.State = catalog.FileProviderLeaseReleased
	return lease, nil
}

func localTestFleetState(
	t *testing.T,
	owner catalog.SourceAuthorityFleetOwnerID,
	generation causal.Generation,
	declarations []catalog.SourceAuthorityDeclaration,
) catalog.DesiredSourceAuthorityFleetState {
	t.Helper()
	authorities := make([]causal.SourceAuthorityID, len(declarations))
	for index, declaration := range declarations {
		authorities[index] = declaration.Authority
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(authorities)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	return catalog.DesiredSourceAuthorityFleetState{
		Owner: owner, Generation: generation, AuthorityCount: uint64(len(declarations)),
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
	}
}

func localTestSpec(root, source string, generation catalog.Generation) tenant.TenantSpec {
	return tenant.TenantSpec{
		OwnerID: "product", ID: "tenant-local",
		Mount:   tenant.MountSpec{PresentationRoot: filepath.Join(root, "tenant-local")},
		Backing: tenant.BackingSpec{Root: filepath.Join(root, "backing")}, Content: tenant.ContentSource{ID: source},
		Traits: tenant.TenantTraits{
			Access: tenant.ReadWrite, CaseSensitivity: catalog.CaseSensitive, Presentations: catalog.PresentMount,
		},
		Generation: generation,
	}
}

func localTestDeclaration(authority, driver string) catalog.SourceAuthorityDeclaration {
	return catalog.SourceAuthorityDeclaration{
		Authority: causal.SourceAuthorityID(authority), DriverID: driver, DriverConfig: []byte(driver),
		DeclarationDigest: [32]byte{byte(len(authority))},
	}
}

func localTestPreparationProof(request LocalProvisionRequest) catalogproto.TenantPreparationProof {
	return catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(request.Tenant.ID), Generation: uint64(request.Tenant.Generation),
			Requested: 7, Desired: 7, Observed: 7, Verified: 7, Applied: 7,
		},
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindMount,
			Mount: &catalogproto.MountPresentationProof{
				TenantID: catalogproto.TenantID(request.Tenant.ID), Generation: uint64(request.Tenant.Generation),
				PublicPath: request.Tenant.Mount.PresentationRoot, ActivationGeneration: "activation-7",
			},
		},
		SourceAuthority:   catalogproto.SourceAuthorityID(request.Declaration.Authority),
		SourcePublication: catalogproto.OperationID(strings.Repeat("1", 32)), SourceRevision: 4, CatalogRevision: 7,
		ChangeID: catalogproto.ChangeID(strings.Repeat("2", 32)), OperationID: catalogproto.OperationID(strings.Repeat("3", 32)),
	}
}
