package catalogservice

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/tenant"
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
		Kind: catalogproto.MutationKindCreate, Disposition: catalogproto.MutationDispositionNamespace,
		ObjectKind: &kind, ParentID: &parent, Name: &name, Mode: &mode,
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

func TestMutationIntentCarriesPrivateCapabilitiesWithoutPathClassification(t *testing.T) {
	domain, err := catalogproto.DeriveDomainID("owner", "account")
	if err != nil {
		t.Fatal(err)
	}
	parent := catalogproto.ObjectID("00112233445566778899aabbccddeeff")
	object := catalogproto.ObjectID("ffeeddccbbaa99887766554433221100")
	creator := catalogproto.MutationID("0000000000000002100000000000000000000000000000000000000000000001")
	name := ".settings.tmp"
	mode := uint32(0o755)
	kind := catalogproto.ObjectKindDirectory
	authorization := Authorization{
		Principal: "provider-principal", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider,
		Route: Route{Tenant: "tenant", Generation: 9, Domain: domain, Forwarded: true},
	}
	adapter := MutationAdapter{}
	created, err := adapter.intent(t.Context(), authorization, "tenant", catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindCreate, Disposition: catalogproto.MutationDispositionPrivateStaging,
		ObjectKind: &kind, ParentID: &parent, Name: &name, Mode: &mode,
	}, nil)
	if err != nil || created.Disposition != catalog.MutationDispositionPrivate ||
		created.Create == nil || created.Create.Spec.Visibility != (catalog.Visibility{}) {
		t.Fatalf("private create intent = %+v, %v", created, err)
	}
	discarded, err := adapter.intent(t.Context(), authorization, "tenant", catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindDelete, Disposition: catalogproto.MutationDispositionPrivateStaging,
		ObjectID: &object, PrivateCreator: &creator,
	}, nil)
	if err != nil || discarded.DiscardPrivate == nil ||
		discarded.DiscardPrivate.Object.String() != string(object) ||
		discarded.DiscardPrivate.Creator.String() != string(creator) {
		t.Fatalf("private discard intent = %+v, %v", discarded, err)
	}
	promoted, err := adapter.intent(t.Context(), authorization, "tenant", catalogproto.MutationRequest{
		Kind: catalogproto.MutationKindPromote, Disposition: catalogproto.MutationDispositionNamespace,
		ObjectID: &object, PrivateCreator: &creator, ParentID: &parent, Name: &name,
	}, nil)
	if err != nil || promoted.PromotePrivate == nil ||
		promoted.PromotePrivate.Visibility != (catalog.Visibility{FileProvider: true}) {
		t.Fatalf("private promotion intent = %+v, %v", promoted, err)
	}
}

func TestPrivateMutationAuthorizationRequiresExactFileProviderRoute(t *testing.T) {
	domain, err := catalogproto.DeriveDomainID("owner", "account")
	if err != nil {
		t.Fatal(err)
	}
	kind := catalogproto.ObjectKindDirectory
	parent := catalogproto.ObjectID("00112233445566778899aabbccddeeff")
	name := ".settings.tmp"
	mode := uint32(0o700)
	request := catalogproto.MutationRequest{
		Protocol: catalogproto.Version, RequestID: "11111111111111111111111111111111",
		Generation: 9, ExpectedRevision: 1, Kind: catalogproto.MutationKindCreate,
		Disposition: catalogproto.MutationDispositionPrivateStaging,
		ObjectKind:  &kind, ParentID: &parent, Name: &name, Mode: &mode,
	}
	exact := Authorization{
		Principal: "provider", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider,
		Route: Route{Tenant: "tenant", Generation: 9, Domain: domain, Forwarded: true},
	}
	if err := validatePrivateMutationAuthorization(exact, "tenant", request); err != nil {
		t.Fatalf("exact File Provider route: %v", err)
	}
	tests := []struct {
		name          string
		authorization Authorization
		tenant        catalog.TenantID
		generation    uint64
	}{
		{name: "mount role", authorization: Authorization{Principal: "mount", Role: RoleMount, Presentation: catalog.PresentationMount, Route: Route{Tenant: "tenant", Generation: 9}}, tenant: "tenant", generation: 9},
		{name: "missing presentation", authorization: Authorization{Principal: "provider", Role: RoleFileProvider, Route: exact.Route}, tenant: "tenant", generation: 9},
		{name: "unforwarded", authorization: Authorization{Principal: "provider", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: Route{Tenant: "tenant", Generation: 9, Domain: domain}}, tenant: "tenant", generation: 9},
		{name: "missing domain", authorization: Authorization{Principal: "provider", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: Route{Tenant: "tenant", Generation: 9, Forwarded: true}}, tenant: "tenant", generation: 9},
		{name: "cross tenant", authorization: exact, tenant: "other", generation: 9},
		{name: "cross generation", authorization: exact, tenant: "tenant", generation: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := request
			candidate.Generation = test.generation
			if err := validatePrivateMutationAuthorization(test.authorization, test.tenant, candidate); err == nil {
				t.Fatal("private mutation authorization succeeded")
			}
		})
	}
	namespace := request
	namespace.Disposition = catalogproto.MutationDispositionNamespace
	if err := validatePrivateMutationAuthorization(Authorization{
		Principal: "mount", Role: RoleMount, Presentation: catalog.PresentationMount,
		Route: Route{Tenant: "tenant", Generation: 9},
	}, "tenant", namespace); err != nil {
		t.Fatalf("ordinary mount mutation: %v", err)
	}
	for _, disposition := range []catalogproto.MutationDisposition{"", "future"} {
		invalid := namespace
		invalid.Disposition = disposition
		if _, err := (MutationAdapter{}).intent(t.Context(), exact, "tenant", invalid, nil); err == nil {
			t.Fatalf("intent accepted mutation disposition %q", disposition)
		}
	}
}

type testPresentationPreparer struct {
	domain catalog.FileProviderDomain
}

type testMountPreparer struct{ calls atomic.Int64 }

func (p *testMountPreparer) PrepareMountPresentation(
	context.Context,
	catalog.TenantID,
	catalog.Generation,
) error {
	p.calls.Add(1)
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
	spec := tenant.TenantSpec{
		ID: "tenant-1", Mount: tenant.MountSpec{PresentationRoot: "/Volumes/FuseKit/tenant-1"}, Generation: 4,
		Traits: tenant.TenantTraits{Presentations: catalog.PresentMount | catalog.PresentFileProvider},
	}
	domainID, err := causal.DeriveDomainID("owner", "presentation-1")
	if err != nil {
		t.Fatal(err)
	}
	mounts := &testMountPreparer{}
	adapter := PreparationAdapter{
		ActivationGeneration: "activation-4",
		Mounts:               mounts,
		Presentations: testPresentationPreparer{domain: catalog.FileProviderDomain{
			DomainID: domainID, Tenant: spec.ID, Generation: spec.Generation,
			PublicPath: "/Library/CloudStorage/tenant-1", ActivationGeneration: "activation-4", Registered: true,
		}},
	}
	mount, err := adapter.preparePresentation(t.Context(), catalogproto.PresentationKindMount, spec)
	if err != nil || mount.Mount == nil || mount.FileProvider != nil ||
		mount.Mount.PublicPath != spec.Mount.PresentationRoot || mount.Mount.ActivationGeneration != "activation-4" {
		t.Fatalf("mount proof = %+v, %v", mount, err)
	}
	if mounts.calls.Load() != 1 {
		t.Fatalf("mount preparation calls = %d, want one", mounts.calls.Load())
	}
	fileProvider, err := adapter.preparePresentation(t.Context(), catalogproto.PresentationKindFileProvider, spec)
	if err != nil || fileProvider.FileProvider == nil || fileProvider.Mount != nil ||
		fileProvider.FileProvider.DomainID != catalogproto.DomainID(domainID) ||
		fileProvider.FileProvider.PublicPath != "/Library/CloudStorage/tenant-1" {
		t.Fatalf("File Provider proof = %+v, %v", fileProvider, err)
	}
}
