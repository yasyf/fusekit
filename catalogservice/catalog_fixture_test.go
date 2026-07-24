package catalogservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverruntime"
)

const catalogServiceFixtureRuntimeGeneration = "catalogservice-fixture-runtime-v1"

func provisionCatalogServiceTenant(
	t *testing.T,
	store *catalog.Catalog,
	definition catalog.TenantProvision,
) catalog.TenantProvision {
	t.Helper()
	persisted, err := store.ProvisionTenant(t.Context(), definition)
	if err != nil {
		t.Fatalf("provision fixture tenant: %v", err)
	}
	state, err := store.TenantLifecycle(t.Context(), definition.OwnerID, definition.Tenant)
	if err != nil {
		t.Fatalf("load fixture lifecycle: %v", err)
	}
	if state.Ready() {
		return persisted
	}

	authority := causal.SourceAuthorityID(definition.ContentSourceID)
	owner := catalog.SourceAuthorityFleetOwnerID(definition.OwnerID)
	declarationDigest := sha256.Sum256([]byte("catalogservice-fixture:" + definition.ContentSourceID))
	declarations := []catalog.SourceAuthorityDeclaration{{
		Authority: authority, DriverID: "test-driver",
		DriverConfig: []byte("catalogservice-fixture:" + definition.ContentSourceID), DeclarationDigest: declarationDigest,
	}}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest([]causal.SourceAuthorityID{authority})
	if err != nil {
		t.Fatalf("digest fixture source fleet: %v", err)
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatalf("digest fixture source declarations: %v", err)
	}
	fleet, err := store.ReconcileSourceAuthorityFleet(t.Context(), catalog.SourceAuthorityFleetReconcileRequest{
		Owner: owner, Generation: 1, Declarations: declarations, Complete: true, AuthorityCount: 1,
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
	})
	if err != nil {
		t.Fatalf("stage fixture source fleet: %v", err)
	}
	if _, err := store.AcknowledgeSourceAuthorityFleet(t.Context(), catalog.SourceAuthorityFleetAcknowledgement{
		Owner: owner, Generation: 1, AuthorityCount: 1,
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest, StageDigest: fleet.StageDigest,
	}); err != nil {
		t.Fatalf("acknowledge fixture source fleet: %v", err)
	}
	driver := &catalogServiceFixtureDriver{
		head:       sourcedriver.RevisionToken("catalogservice-fixture-head-v1"),
		targetSets: make(map[sourcedriver.TargetSetID]sourcedriver.TargetSetState),
	}
	runtime, err := sourcedriverruntime.NewRuntime(t.Context(), sourcedriverruntime.Config{
		Store: store, Driver: driver, Authority: authority,
		FleetOwner: owner, AuthorityGeneration: 1, DeclarationDigest: declarationDigest,
		Targets:   []catalog.SourceDriverTarget{{Tenant: definition.Tenant, Generation: definition.Generation}},
		PageLimit: 128,
	})
	if err != nil {
		t.Fatalf("start fixture source runtime: %v", err)
	}
	result, reconcileErr := runtime.Reconcile(t.Context())
	closeErr := runtime.Close()
	if err := errors.Join(reconcileErr, closeErr); err != nil {
		t.Fatalf("publish fixture source: %v", err)
	}
	checkpoint := result.Checkpoint
	if checkpoint.PublicationID == (causal.OperationID{}) || checkpoint.PublicationDigest == ([sha256.Size]byte{}) {
		t.Fatalf("source publication checkpoint = %+v", checkpoint)
	}

	lease, state, err := store.StageApplication(t.Context(), catalog.StageApplicationRequest{
		Mutation: catalogServiceFixtureTenantMutation(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		Authority: authority, Publication: checkpoint.PublicationID, PublicationDigest: checkpoint.PublicationDigest,
	})
	if err != nil {
		t.Fatalf("stage fixture application: %v", err)
	}
	for _, backend := range state.Target.RequiredBackends.Backends() {
		state, err = store.RecordPresentation(t.Context(), catalog.PresentationReceipt{
			Mutation: catalogServiceFixtureTenantMutation(t, state.OwnerID, state.Intent.Revision),
			Lease:    lease, Backend: backend,
			BackendGeneration: fmt.Sprintf("catalogservice-fixture-backend-%d", backend),
			ObservedRevision:  lease.CatalogHead,
		})
		if err != nil {
			t.Fatalf("record fixture presentation %d: %v", backend, err)
		}
	}
	targetingRevision, err := store.TenantTargetingRevision(t.Context(), definition.Tenant)
	if err != nil {
		t.Fatalf("read fixture targeting revision: %v", err)
	}
	activation, err := store.ActivateTenant(t.Context(), catalog.ActivateTenantRequest{
		Mutation: catalogServiceFixtureTenantMutation(t, state.OwnerID, state.Intent.Revision),
		Tenant:   definition.Tenant, Generation: definition.Generation,
		ViewID: lease.ViewID, ViewDigest: lease.ViewDigest,
		ExpectedActivationRevision: state.Activation.Revision,
		ExpectedActiveGeneration:   state.Activation.ActiveGeneration,
		ExpectedTargetingRevision:  targetingRevision,
		CausePublications:          []causal.OperationID{checkpoint.PublicationID},
	})
	if err != nil {
		t.Fatalf("activate fixture tenant: %v", err)
	}
	if !activation.State.Ready() {
		t.Fatalf("tenant lifecycle did not become ready: %+v", activation.State)
	}
	return persisted
}

func catalogServiceFixtureTenantMutation(
	t *testing.T,
	owner string,
	revision catalog.TenantIntentRevision,
) catalog.TenantMutation {
	t.Helper()
	operation, err := catalog.NewTenantOperationID()
	if err != nil {
		t.Fatal(err)
	}
	return catalog.TenantMutation{
		OperationID: operation, HolderRuntimeGeneration: catalogServiceFixtureRuntimeGeneration,
		OwnerID: owner, ExpectedIntentRevision: revision,
	}
}

type catalogServiceFixtureDriver struct {
	head       sourcedriver.RevisionToken
	targetSets map[sourcedriver.TargetSetID]sourcedriver.TargetSetState
}

func (d *catalogServiceFixtureDriver) Refresh(
	context.Context,
	causal.SourceAuthorityID,
) (sourcedriver.Head, error) {
	return sourcedriver.Head{Revision: d.head}, nil
}

func (d *catalogServiceFixtureDriver) InspectTargetSet(
	_ context.Context,
	_ causal.SourceAuthorityID,
	ref sourcedriver.TargetSetRef,
) (sourcedriver.TargetSetState, error) {
	state, found := d.targetSets[ref.ID]
	if !found || state.Ref != ref {
		return sourcedriver.TargetSetState{}, sourcedriver.ErrNotFound
	}
	return state, nil
}

func (d *catalogServiceFixtureDriver) DeclareTargetSet(
	_ context.Context,
	authority causal.SourceAuthorityID,
	page sourcedriver.TargetSetPage,
) (sourcedriver.TargetSetState, error) {
	state, found := d.targetSets[page.Ref.ID]
	if !found {
		var err error
		state, err = sourcedriver.NewTargetSetState(authority, page.Ref)
		if err != nil {
			return sourcedriver.TargetSetState{}, err
		}
	}
	next, err := sourcedriver.ApplyTargetSetPage(state, page)
	if err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	d.targetSets[page.Ref.ID] = next
	return next, nil
}

func (d *catalogServiceFixtureDriver) Snapshot(
	_ context.Context,
	_ causal.SourceAuthorityID,
	request sourcedriver.SnapshotRequest,
) (sourcedriver.SnapshotPage, error) {
	digest, err := sourcedriver.SnapshotPageDigest(request.Revision, nil)
	return sourcedriver.SnapshotPage{Revision: request.Revision, Digest: digest}, err
}

func (*catalogServiceFixtureDriver) ChangesSince(
	context.Context,
	causal.SourceAuthorityID,
	sourcedriver.ChangesRequest,
) (sourcedriver.ChangePage, error) {
	return sourcedriver.ChangePage{}, errors.New("catalogservice fixture: unexpected delta request")
}

func (*catalogServiceFixtureDriver) OpenContent(
	context.Context,
	causal.SourceAuthorityID,
	sourcedriver.ContentRef,
) (contentstream.Source, error) {
	return nil, errors.New("catalogservice fixture: unexpected content request")
}

func (*catalogServiceFixtureDriver) ApplyMutation(
	context.Context,
	causal.SourceAuthorityID,
	sourcedriver.MutationRequest,
	contentstream.Source,
) (sourcedriver.MutationReceipt, error) {
	return sourcedriver.MutationReceipt{}, errors.New("catalogservice fixture: unexpected mutation")
}

func (*catalogServiceFixtureDriver) InspectMutation(
	_ context.Context,
	_ causal.SourceAuthorityID,
	operation catalog.MutationID,
	_ [sha256.Size]byte,
) (sourcedriver.MutationReceipt, error) {
	return sourcedriver.MutationReceipt{OperationID: operation, State: sourcedriver.MutationNotFound}, nil
}

func (*catalogServiceFixtureDriver) SettleMutation(
	context.Context,
	causal.SourceAuthorityID,
	sourcedriver.MutationSettlement,
) error {
	return nil
}

var _ sourcedriver.Driver = (*catalogServiceFixtureDriver)(nil)
