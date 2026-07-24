package catalog

import (
	"errors"
	"io"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestPrivateMutationLookupOpenPromoteAndDiscard(t *testing.T) {
	c := newTestCatalog(t)
	tenant, root := createTestTenant(t, c, "private-terminal", CaseSensitive)
	origin := CausalOrigin{
		Cause: causal.CauseProviderMutation, Domain: "private-domain", Generation: 1,
	}

	stage := func(name, body string) privateMutationObjectRecord {
		t.Helper()
		ref := stageTestContent(t, c, body)
		result, err := c.commitTestAuthoritativeMutation(t.Context(), tenant, MutationIntent{
			SourceID: "test", Origin: origin, Disposition: MutationDispositionPrivate,
			Create: &CreateMutation{Spec: CreateSpec{
				Parent: root.ID, Name: name, Kind: KindFile, Mode: 0o600,
				ContentRevision: 1, Content: ref, Convergence: Convergence{Desired: 1},
			}},
		})
		if err != nil {
			t.Fatalf("private create %q: %v", name, err)
		}
		record, found, err := readPrivatePromotionSource(t.Context(), c.readDB, tenant, result.Primary.ID, "test")
		if err != nil || !found {
			t.Fatalf("read private %q: found=%v err=%v", name, found, err)
		}
		return record
	}

	promoted := stage("promote.tmp", "promoted body")
	if _, err := c.Lookup(t.Context(), tenant, PresentationFileProvider, promoted.ObjectID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ordinary Lookup(private) = %v, want ErrNotFound", err)
	}
	lookedUp, err := c.PrivateMutationObject(t.Context(), tenant, promoted.ObjectID, origin)
	if err != nil || lookedUp.Mutation != promoted.Mutation {
		t.Fatalf("PrivateMutationObject = %#v, %v", lookedUp, err)
	}
	if _, err := c.PrivateMutationObject(t.Context(), tenant, promoted.ObjectID, CausalOrigin{
		Cause: causal.CauseProviderMutation, Domain: "wrong-domain", Generation: 1,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong-origin private lookup = %v, want ErrNotFound", err)
	}
	opened, content, err := c.OpenPrivateContent(t.Context(), tenant, promoted.Generation,
		promoted.ObjectID, promoted.Mutation, origin)
	if err != nil {
		t.Fatalf("OpenPrivateContent: %v", err)
	}
	if opened.Mutation != promoted.Mutation {
		t.Fatalf("opened private = %#v", opened)
	}
	body, err := io.ReadAll(content)
	if err != nil {
		t.Fatalf("ReadAll(private): %v", err)
	}
	if err := content.Settle(nil); err != nil {
		t.Fatalf("Settle(private): %v", err)
	}
	if err := content.Wait(t.Context()); err != nil {
		t.Fatalf("Wait(private): %v", err)
	}
	if string(body) != "promoted body" {
		t.Fatalf("private content = %q", body)
	}

	head := mustCatalogHead(t, c, tenant)
	promotion, err := c.commitTestAuthoritativeMutation(t.Context(), tenant, MutationIntent{
		SourceID: "test", Origin: origin, Disposition: MutationDispositionNamespace,
		PromotePrivate: &PromotePrivateMutation{
			Object: promoted.ObjectID, Creator: promoted.Mutation, Parent: root.ID,
			Name: "promoted.txt", Visibility: Visibility{Mount: true, FileProvider: true},
		},
	})
	if err != nil {
		t.Fatalf("promote private: %v", err)
	}
	if promotion.Primary.ID != promoted.ObjectID || mustCatalogHead(t, c, tenant) != head+1 {
		t.Fatalf("promotion = %#v head=%d, want id=%x head=%d",
			promotion.Primary, mustCatalogHead(t, c, tenant), promoted.ObjectID, head+1)
	}
	if _, err := c.PrivateMutationObject(t.Context(), tenant, promoted.ObjectID, origin); !errors.Is(err, ErrNotFound) {
		t.Fatalf("private lookup after promotion = %v, want ErrNotFound", err)
	}

	discarded := stage("discard.tmp", "discarded body")
	head = mustCatalogHead(t, c, tenant)
	if _, err := c.commitTestAuthoritativeMutation(t.Context(), tenant, MutationIntent{
		SourceID: "test", Origin: origin, Disposition: MutationDispositionPrivate,
		DiscardPrivate: &DiscardPrivateMutation{Object: discarded.ObjectID, Creator: discarded.Mutation},
	}); err != nil {
		t.Fatalf("discard private: %v", err)
	}
	if mustCatalogHead(t, c, tenant) != head {
		t.Fatalf("discard advanced catalog head: got %d want %d", mustCatalogHead(t, c, tenant), head)
	}
	if _, err := c.PrivateMutationObject(t.Context(), tenant, discarded.ObjectID, origin); !errors.Is(err, ErrNotFound) {
		t.Fatalf("private lookup after discard = %v, want ErrNotFound", err)
	}
}
