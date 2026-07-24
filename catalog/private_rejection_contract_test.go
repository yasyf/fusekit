package catalog

import (
	"errors"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestPrivateReplaceCapabilityRejectionsAreNonConsuming(t *testing.T) {
	store := newTestCatalog(t)
	tenant, root := createTestTenant(t, store, "private-replace-capability", CaseSensitive)
	target := createTestFile(t, store, tenant, root.ID, "settings.json", "old")
	otherTenant, otherRoot := createTestTenant(t, store, "private-replace-capability-other", CaseSensitive)
	otherTarget := createTestFile(t, store, otherTenant, otherRoot.ID, "settings.json", "other")
	origin := CausalOrigin{
		Cause: causal.CauseProviderMutation, Domain: "private-domain", Generation: 1,
	}
	private := stagePrivateReplacementCapability(t, store, tenant, root.ID, origin)

	tests := []struct {
		name     string
		tenant   TenantID
		sourceID string
		origin   CausalOrigin
		source   ObjectID
		target   Object
		creator  MutationID
	}{
		{
			name: "absent object", tenant: tenant, sourceID: "test", origin: origin,
			source: ObjectID{0xff}, target: target, creator: private.Mutation,
		},
		{
			name: "wrong source owner", tenant: tenant, sourceID: "other-source", origin: origin,
			source: private.ObjectID, target: target, creator: private.Mutation,
		},
		{
			name: "wrong tenant", tenant: otherTenant, sourceID: "test", origin: origin,
			source: private.ObjectID, target: otherTarget, creator: private.Mutation,
		},
		{
			name: "wrong creator", tenant: tenant, sourceID: "test", origin: origin,
			source: private.ObjectID, target: target, creator: MutationID{0xff},
		},
		{
			name: "wrong origin", tenant: tenant, sourceID: "test",
			origin: CausalOrigin{Cause: causal.CauseExternalUnattributed},
			source: private.ObjectID, target: target, creator: private.Mutation,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			head := mustCatalogHead(t, store, test.tenant)
			creator := test.creator
			_, err := store.BeginMutation(t.Context(), test.tenant, head, MutationIntent{
				SourceID: test.sourceID, Origin: test.origin,
				Disposition: MutationDispositionNamespace,
				Replace: &ReplaceMutation{
					Source: test.source, Target: test.target.ID, PrivateCreator: &creator,
				},
			})
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("BeginMutation = %v, want ErrNotFound", err)
			}
			if got := mustCatalogHead(t, store, test.tenant); got != head {
				t.Fatalf("catalog head = %d after rejection, want %d", got, head)
			}
			assertPrivateReplacementCapability(t, store, tenant, private, origin)
			bound, err := store.LookupName(
				t.Context(), test.tenant, PresentationFileProvider, test.target.Parent, test.target.Name,
			)
			if err != nil || bound.ID != test.target.ID {
				t.Fatalf("target after rejection = %+v, %v", bound, err)
			}
		})
	}
}

func stagePrivateReplacementCapability(
	t *testing.T,
	store *Catalog,
	tenant TenantID,
	parent ObjectID,
	origin CausalOrigin,
) privateMutationObjectRecord {
	t.Helper()
	ref := stageTestContent(t, store, "new")
	result, err := store.commitTestAuthoritativeMutation(t.Context(), tenant, MutationIntent{
		SourceID: "test", Origin: origin, Disposition: MutationDispositionPrivate,
		Create: &CreateMutation{Spec: CreateSpec{
			Parent: parent, Name: ".settings.json.tmp", Kind: KindFile, Mode: 0o600,
			ContentRevision: 1, Content: ref, Convergence: Convergence{Desired: 1},
		}},
	})
	if err != nil {
		t.Fatalf("private create: %v", err)
	}
	private, found, err := readPrivatePromotionSource(
		t.Context(), store.readDB, tenant, result.Primary.ID, "test",
	)
	if err != nil || !found {
		t.Fatalf("private capability = %+v, found %t, err %v", private, found, err)
	}
	return private
}

func assertPrivateReplacementCapability(
	t *testing.T,
	store *Catalog,
	tenant TenantID,
	want privateMutationObjectRecord,
	origin CausalOrigin,
) {
	t.Helper()
	got, found, err := readPrivatePromotionSource(
		t.Context(), store.readDB, tenant, want.ObjectID, "test",
	)
	if err != nil || !found || got.Mutation != want.Mutation || got.Origin != origin {
		t.Fatalf("private capability after rejection = %+v, found %t, err %v", got, found, err)
	}
}
