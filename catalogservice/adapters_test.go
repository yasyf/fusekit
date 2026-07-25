package catalogservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
)

func TestMutationIntentDerivesProviderOriginFromAuthorization(t *testing.T) {
	domain, err := catalogproto.DeriveDomainID("owner", "account")
	if err != nil {
		t.Fatalf("DeriveDomainID: %v", err)
	}
	parent := catalogproto.ObjectID("00112233445566778899aabbccddeeff")
	name := "settings.json"
	mode := uint32(0o644)
	kind := catalogproto.ObjectKindDirectory
	authorization := Authorization{
		Principal: "provider-principal", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider,
		Route: Route{Tenant: "tenant", Generation: 9, Domain: domain, Forwarded: true},
	}
	intent, err := (MutationAdapter{}).intent(context.Background(), authorization, "tenant", catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindCreate, ObjectKind: &kind, ParentID: &parent, Name: &name, Mode: &mode,
	}, nil)
	if err != nil {
		t.Fatalf("intent: %v", err)
	}
	if intent.SourceID != "fileprovider:"+string(domain) || intent.SourceMetadata != "generation=9" {
		t.Fatalf("source identity = %q/%q", intent.SourceID, intent.SourceMetadata)
	}
	want := catalog.CausalOrigin{
		Cause: causal.CauseProviderMutation, Domain: causal.DomainID(domain), Generation: 9,
	}
	if !reflect.DeepEqual(intent.Origin, want) {
		t.Fatalf("provider origin = %+v, want %+v", intent.Origin, want)
	}
}

func TestPreparationOperationIDsAreExactAndDomainSeparated(t *testing.T) {
	publication := causal.OperationID{1}
	view := catalog.StagedViewID{2}
	first := preparationOperationID("stage", "runtime-1", "tenant-1", 4, 7, publication, view, 0)
	replay := preparationOperationID("stage", "runtime-1", "tenant-1", 4, 7, publication, view, 0)
	backend := preparationOperationID("presentation", "runtime-1", "tenant-1", 4, 7, publication, view, catalog.TenantBackendNative)
	if first == (catalog.TenantOperationID{}) || first != replay || first == backend {
		t.Fatalf("operation IDs = %x replay=%x backend=%x", first, replay, backend)
	}
}

func TestActivationReceiptRequiresExactServingPointer(t *testing.T) {
	view := catalog.StagedViewID{1}
	head := sha256.Sum256([]byte("head"))
	viewDigest := sha256.Sum256([]byte("view"))
	provision := adapterTestProvision()
	state := catalog.TenantLifecycleState{
		OwnerID: "owner",
		Intent: catalog.TenantIntent{
			Tenant: "tenant-1", Kind: catalog.TenantIntentPresent, TargetGeneration: 4, Revision: 2,
		},
		Active: &catalog.TenantGeneration{Definition: provision, RequiredBackends: catalog.TenantBackendSet(3)},
		Activation: catalog.TenantActivation{
			Tenant: "tenant-1", ActiveGeneration: 4, ActiveView: view, ActiveCatalogHead: 9, Revision: 3,
		},
		Applications: []catalog.TenantApplication{{
			Tenant: "tenant-1", Generation: 4, IntentRevision: 2, Phase: catalog.TenantApplicationStaged,
			ViewID: view, ViewDigest: viewDigest, StagedCatalogHead: 9, StagedHeadDigest: head,
		}},
		Presentations: []catalog.PresentationMaterialization{
			{Tenant: "tenant-1", Generation: 4, Backend: catalog.TenantBackendNative, IntentRevision: 2,
				Phase: catalog.PresentationMaterializationActive, ViewID: view, ViewDigest: viewDigest,
				BackendGeneration: "activation-4", ObservedRevision: 9},
			{Tenant: "tenant-1", Generation: 4, Backend: catalog.TenantBackendBroker, IntentRevision: 2,
				Phase: catalog.PresentationMaterializationActive, ViewID: view, ViewDigest: viewDigest,
				BackendGeneration: "activation-4", ObservedRevision: 9},
		},
	}
	receipt, err := activationReceipt(state, 4)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Owner != "owner" || receipt.Tenant != "tenant-1" || receipt.Generation != 4 ||
		receipt.ActivationRevision != 3 || receipt.CatalogHead != 9 || receipt.ViewID != view || receipt.HeadDigest != head {
		t.Fatalf("activation receipt = %+v", receipt)
	}
	state.Applications[0].StagedCatalogHead++
	if _, err := activationReceipt(state, 4); !errors.Is(err, catalog.ErrTenantLifecycleStale) {
		t.Fatalf("mismatched serving pointer error = %v", err)
	}
}

func TestActivationReceiptRejectsIncompleteApplicationAndPresentationState(t *testing.T) {
	base := exactActivationStateForTest()
	tests := []struct {
		name   string
		mutate func(*catalog.TenantLifecycleState)
	}{
		{name: "active definition generation", mutate: func(state *catalog.TenantLifecycleState) {
			state.Active.Definition.Generation++
		}},
		{name: "application phase", mutate: func(state *catalog.TenantLifecycleState) {
			state.Applications[0].Phase = catalog.TenantApplicationPending
		}},
		{name: "application digest", mutate: func(state *catalog.TenantLifecycleState) {
			state.Applications[0].StagedHeadDigest = [sha256.Size]byte{}
		}},
		{name: "missing required backend", mutate: func(state *catalog.TenantLifecycleState) {
			state.Presentations = state.Presentations[:1]
		}},
		{name: "presentation view", mutate: func(state *catalog.TenantLifecycleState) {
			state.Presentations[0].ViewID[0]++
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			state := base
			state.Applications = append([]catalog.TenantApplication(nil), base.Applications...)
			state.Presentations = append([]catalog.PresentationMaterialization(nil), base.Presentations...)
			active := *base.Active
			state.Active = &active
			test.mutate(&state)
			if _, err := activationReceipt(state, 4); !errors.Is(err, catalog.ErrTenantLifecycleStale) {
				t.Fatalf("activationReceipt error = %v, want lifecycle stale", err)
			}
		})
	}
}

type testPresentationPreparer struct {
	domain catalog.FileProviderDomain
}

func (testPresentationPreparer) PrepareMountPresentation(
	context.Context,
	catalog.TenantID,
	catalog.Generation,
) error {
	return nil
}

func (p testPresentationPreparer) PrepareFileProviderPresentation(
	context.Context,
	catalog.TenantID,
	catalog.Generation,
) (catalog.FileProviderDomain, error) {
	return p.domain, nil
}

func TestPreparationAdapterReturnsClosedTypedPresentationProof(t *testing.T) {
	provision := adapterTestProvision()
	domainID, err := causal.DeriveDomainID(provision.OwnerID, provision.FileProvider.PresentationInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	adapter := PreparationAdapter{
		ActivationGeneration: "activation-4",
		Mounts:               testPresentationPreparer{},
		Presentations: testPresentationPreparer{domain: catalog.FileProviderDomain{
			DomainID: domainID, OwnerID: provision.OwnerID, Tenant: provision.Tenant, Generation: provision.Generation,
			Root: provision.Root, Access: provision.Access,
			PresentationInstance: provision.FileProvider.PresentationInstanceID, DisplayName: provision.FileProvider.DisplayName,
			PublicPath: "/Library/CloudStorage/tenant-1", ActivationGeneration: "activation-4", Registered: true,
		}},
	}
	mount, err := adapter.preparePresentation(t.Context(), catalogproto.PresentationKindMount, provision)
	if err != nil || mount.Mount == nil || mount.FileProvider != nil ||
		mount.Mount.PublicPath != provision.Mount.PresentationRoot || mount.Mount.ActivationGeneration != "activation-4" {
		t.Fatalf("mount proof = %+v, %v", mount, err)
	}
	fileProvider, err := adapter.preparePresentation(t.Context(), catalogproto.PresentationKindFileProvider, provision)
	if err != nil || fileProvider.FileProvider == nil || fileProvider.Mount != nil ||
		fileProvider.FileProvider.DomainID != catalogproto.DomainID(domainID) ||
		fileProvider.FileProvider.PublicPath != "/Library/CloudStorage/tenant-1" {
		t.Fatalf("File Provider proof = %+v, %v", fileProvider, err)
	}
}

func TestPreparationAdapterRejectsInexactFileProviderDomainProof(t *testing.T) {
	provision := adapterTestProvision()
	domainID, err := causal.DeriveDomainID(provision.OwnerID, provision.FileProvider.PresentationInstanceID)
	if err != nil {
		t.Fatal(err)
	}
	exact := catalog.FileProviderDomain{
		DomainID: domainID, OwnerID: provision.OwnerID, Tenant: provision.Tenant, Generation: provision.Generation,
		Root: provision.Root, Access: provision.Access,
		PresentationInstance: provision.FileProvider.PresentationInstanceID, DisplayName: provision.FileProvider.DisplayName,
		PublicPath: "/Library/CloudStorage/tenant-1", ActivationGeneration: "activation-4", Registered: true,
	}
	tests := []struct {
		name   string
		mutate func(*catalog.FileProviderDomain)
	}{
		{name: "registered", mutate: func(domain *catalog.FileProviderDomain) { domain.Registered = false }},
		{name: "domain", mutate: func(domain *catalog.FileProviderDomain) { domain.DomainID = "wrong" }},
		{name: "owner", mutate: func(domain *catalog.FileProviderDomain) { domain.OwnerID = "wrong" }},
		{name: "tenant", mutate: func(domain *catalog.FileProviderDomain) { domain.Tenant = "wrong" }},
		{name: "generation", mutate: func(domain *catalog.FileProviderDomain) { domain.Generation++ }},
		{name: "root", mutate: func(domain *catalog.FileProviderDomain) { domain.Root[0]++ }},
		{name: "access", mutate: func(domain *catalog.FileProviderDomain) { domain.Access = catalog.TenantReadOnly }},
		{name: "presentation", mutate: func(domain *catalog.FileProviderDomain) { domain.PresentationInstance = "wrong" }},
		{name: "display name", mutate: func(domain *catalog.FileProviderDomain) { domain.DisplayName = "wrong" }},
		{name: "public path", mutate: func(domain *catalog.FileProviderDomain) { domain.PublicPath = "relative" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			domain := exact
			test.mutate(&domain)
			adapter := PreparationAdapter{
				ActivationGeneration: "activation-4",
				Presentations:        testPresentationPreparer{domain: domain},
			}
			if _, err := adapter.preparePresentation(t.Context(), catalogproto.PresentationKindFileProvider, provision); !errors.Is(err, catalog.ErrIntegrity) {
				t.Fatalf("preparePresentation error = %v, want integrity", err)
			}
		})
	}
	wrongRuntime := exact
	wrongRuntime.ActivationGeneration = "activation-3"
	adapter := PreparationAdapter{
		ActivationGeneration: "activation-4",
		Presentations:        testPresentationPreparer{domain: wrongRuntime},
	}
	if _, err := adapter.preparePresentation(t.Context(), catalogproto.PresentationKindFileProvider, provision); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("wrong activation generation error = %v", err)
	}
}

type replayPreparationStore struct {
	state      catalog.TenantLifecycleState
	stageCalls int
	records    []catalog.PresentationReceipt
	activation catalog.ActivateTenantRequest
}

func (s *replayPreparationStore) TenantLifecycle(context.Context, string, catalog.TenantID) (catalog.TenantLifecycleState, error) {
	return s.state, nil
}

func (s *replayPreparationStore) StageApplication(context.Context, catalog.StageApplicationRequest) (catalog.StagedViewLease, catalog.TenantLifecycleState, error) {
	s.stageCalls++
	return catalog.StagedViewLease{}, catalog.TenantLifecycleState{}, errors.New("unexpected restage")
}

func (s *replayPreparationStore) RecordPresentation(_ context.Context, receipt catalog.PresentationReceipt) (catalog.TenantLifecycleState, error) {
	s.records = append(s.records, receipt)
	return s.state, nil
}

func (*replayPreparationStore) TenantTargetingRevision(context.Context, catalog.TenantID) (uint64, error) {
	return 17, nil
}

func (s *replayPreparationStore) ActivateTenant(_ context.Context, request catalog.ActivateTenantRequest) (catalog.TenantActivationResult, error) {
	s.activation = request
	return catalog.TenantActivationResult{State: s.state}, nil
}

func TestActivateDesiredGenerationResumesStagedLeaseAfterSourceAdvances(t *testing.T) {
	provision := adapterTestProvision()
	provision.Presentations = catalog.PresentMount
	provision.FileProvider = catalog.FileProviderPresentation{}
	view := catalog.StagedViewID{7}
	publication := causal.OperationID{8}
	stageOperation := preparationOperationID(
		"stage", "activation-4", provision.Tenant, provision.Generation, 2, publication, catalog.StagedViewID{}, 0,
	)
	state := catalog.TenantLifecycleState{
		OwnerID: provision.OwnerID,
		Intent: catalog.TenantIntent{
			Tenant: provision.Tenant, Kind: catalog.TenantIntentPresent,
			TargetGeneration: provision.Generation, Revision: 2,
		},
		Target: &catalog.TenantGeneration{Definition: provision, RequiredBackends: catalog.TenantBackendSet(1)},
		Applications: []catalog.TenantApplication{{
			Tenant: provision.Tenant, Generation: provision.Generation, IntentRevision: 2,
			ContentSourceID: provision.ContentSourceID, Phase: catalog.TenantApplicationStaged,
			SourceAuthority: causal.SourceAuthorityID(provision.ContentSourceID), SourcePublication: publication,
			ViewID: view, ViewDigest: sha256.Sum256([]byte("view")),
			StagedCatalogHead: 9, StagedHeadDigest: sha256.Sum256([]byte("head")), StagedSourceRevision: 8,
			PublicationDigest: sha256.Sum256([]byte("publication")), HolderRuntimeGeneration: "activation-4",
			OperationID: stageOperation,
		}},
	}
	store := &replayPreparationStore{state: state}
	adapter := PreparationAdapter{
		Store: store, Mounts: testPresentationPreparer{}, ActivationGeneration: "activation-4",
	}
	newerPublication := causal.OperationID{9}
	result, err := adapter.activateDesiredGeneration(t.Context(), state, catalog.SourceDriverCheckpoint{
		Authority: causal.SourceAuthorityID(provision.ContentSourceID), PublicationID: newerPublication,
		PublicationDigest: sha256.Sum256([]byte("newer")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Intent.Tenant != provision.Tenant || store.stageCalls != 0 || len(store.records) != 1 {
		t.Fatalf("resume result=%+v stage calls=%d records=%d", result, store.stageCalls, len(store.records))
	}
	record := store.records[0]
	wantRecordOperation := preparationOperationID(
		"presentation", "activation-4", provision.Tenant, provision.Generation, 2,
		publication, view, catalog.TenantBackendNative,
	)
	if record.Lease.SourcePublication != publication || record.Mutation.OperationID != wantRecordOperation {
		t.Fatalf("record replay identity = %+v", record)
	}
	wantActivateOperation := preparationOperationID(
		"activate", "activation-4", provision.Tenant, provision.Generation, 2, publication, view, 0,
	)
	if store.activation.Mutation.OperationID != wantActivateOperation ||
		len(store.activation.CausePublications) != 1 || store.activation.CausePublications[0] != publication ||
		store.activation.ExpectedTargetingRevision != 17 {
		t.Fatalf("activation replay identity = %+v", store.activation)
	}
}

func TestStagedApplicationLeaseRejectsCrossHolderReplay(t *testing.T) {
	state := stagedLifecycleStateForTest()
	if _, _, err := stagedApplicationLease(state, "activation-5"); !errors.Is(err, catalog.ErrGenerationMismatch) {
		t.Fatalf("cross-holder stagedApplicationLease error = %v", err)
	}
}

func adapterTestProvision() catalog.TenantProvision {
	return catalog.TenantProvision{
		OwnerID: "owner", Tenant: "tenant-1", Root: catalog.ObjectID{1},
		Mount:       catalog.MountPresentation{PresentationRoot: "/Volumes/FuseKit/tenant-1"},
		BackingRoot: "/tmp/fusekit/tenant-1", ContentSourceID: "source-1",
		Access: catalog.TenantReadWrite, CasePolicy: catalog.CaseSensitive,
		Presentations: catalog.PresentMount | catalog.PresentFileProvider,
		FileProvider: catalog.FileProviderPresentation{
			PresentationInstanceID: "presentation-1", DisplayName: "Tenant 1",
		},
		Generation: 4,
	}
}

func exactActivationStateForTest() catalog.TenantLifecycleState {
	view := catalog.StagedViewID{1}
	viewDigest := sha256.Sum256([]byte("view"))
	provision := adapterTestProvision()
	return catalog.TenantLifecycleState{
		OwnerID: provision.OwnerID,
		Intent: catalog.TenantIntent{
			Tenant: provision.Tenant, Kind: catalog.TenantIntentPresent,
			TargetGeneration: provision.Generation, Revision: 2,
		},
		Active: &catalog.TenantGeneration{Definition: provision, RequiredBackends: catalog.TenantBackendSet(3)},
		Activation: catalog.TenantActivation{
			Tenant: provision.Tenant, ActiveGeneration: provision.Generation, ActiveView: view,
			ActiveCatalogHead: 9, Revision: 3,
		},
		Applications: []catalog.TenantApplication{{
			Tenant: provision.Tenant, Generation: provision.Generation, IntentRevision: 2,
			Phase: catalog.TenantApplicationStaged, ViewID: view, ViewDigest: viewDigest,
			StagedCatalogHead: 9, StagedHeadDigest: sha256.Sum256([]byte("head")),
		}},
		Presentations: []catalog.PresentationMaterialization{
			{Tenant: provision.Tenant, Generation: provision.Generation, Backend: catalog.TenantBackendNative,
				IntentRevision: 2, Phase: catalog.PresentationMaterializationActive, ViewID: view,
				ViewDigest: viewDigest, BackendGeneration: "activation-4", ObservedRevision: 9},
			{Tenant: provision.Tenant, Generation: provision.Generation, Backend: catalog.TenantBackendBroker,
				IntentRevision: 2, Phase: catalog.PresentationMaterializationActive, ViewID: view,
				ViewDigest: viewDigest, BackendGeneration: "activation-4", ObservedRevision: 9},
		},
	}
}

func stagedLifecycleStateForTest() catalog.TenantLifecycleState {
	provision := adapterTestProvision()
	publication := causal.OperationID{8}
	view := catalog.StagedViewID{7}
	return catalog.TenantLifecycleState{
		OwnerID: provision.OwnerID,
		Intent: catalog.TenantIntent{
			Tenant: provision.Tenant, Kind: catalog.TenantIntentPresent,
			TargetGeneration: provision.Generation, Revision: 2,
		},
		Target: &catalog.TenantGeneration{Definition: provision, RequiredBackends: catalog.TenantBackendSet(3)},
		Applications: []catalog.TenantApplication{{
			Tenant: provision.Tenant, Generation: provision.Generation, IntentRevision: 2,
			ContentSourceID: provision.ContentSourceID, Phase: catalog.TenantApplicationStaged,
			SourceAuthority: causal.SourceAuthorityID(provision.ContentSourceID), SourcePublication: publication,
			ViewID: view, ViewDigest: sha256.Sum256([]byte("view")), StagedCatalogHead: 9,
			StagedHeadDigest: sha256.Sum256([]byte("head")), StagedSourceRevision: 8,
			PublicationDigest: sha256.Sum256([]byte("publication")), HolderRuntimeGeneration: "activation-4",
			OperationID: preparationOperationID(
				"stage", "activation-4", provision.Tenant, provision.Generation, 2,
				publication, catalog.StagedViewID{}, 0,
			),
		}},
	}
}
