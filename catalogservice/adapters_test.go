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
