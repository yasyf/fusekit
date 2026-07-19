package catalogservice

import (
	"context"
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
