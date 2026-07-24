package catalogworker

import (
	"crypto/sha256"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func testSourceAuthorityFleet(
	t *testing.T,
	authorities []causal.SourceAuthorityID,
) ([]catalog.SourceAuthorityDeclaration, [32]byte, [32]byte) {
	t.Helper()
	declarations := make([]catalog.SourceAuthorityDeclaration, len(authorities))
	for index, authority := range authorities {
		declarations[index] = catalog.SourceAuthorityDeclaration{
			Authority: authority, DriverID: "catalogworker-test",
			DeclarationDigest: sha256.Sum256([]byte(authority)),
		}
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(authorities)
	if err != nil {
		t.Fatal(err)
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(declarations)
	if err != nil {
		t.Fatal(err)
	}
	return declarations, authoritiesDigest, declarationsDigest
}

func TestSourceAuthorityRuntimeStatusCarriesClosedUnownedStateAcrossWire(t *testing.T) {
	manager, _ := newTestManager(t)
	owner := catalog.SourceAuthorityFleetOwnerID("unowned-runtime-wire")
	authority := causal.SourceAuthorityID("alpha")
	declarations, authoritiesDigest, declarationsDigest := testSourceAuthorityFleet(
		t, []causal.SourceAuthorityID{authority},
	)
	stage, err := manager.ReconcileSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetReconcileRequest{
			Owner: owner, Generation: 1, Declarations: declarations,
			Complete: true, AuthorityCount: 1, AuthoritiesDigest: authoritiesDigest,
			DeclarationsDigest: declarationsDigest,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.AcknowledgeSourceAuthorityFleet(
		t.Context(),
		catalog.SourceAuthorityFleetAcknowledgement{
			Owner: owner, Generation: 1, AuthorityCount: 1,
			AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
			StageDigest: stage.StageDigest,
		},
	); err != nil {
		t.Fatal(err)
	}
	ref := catalog.SourceAuthorityRuntimeRef{
		Owner: owner, Generation: 1, Authority: authority,
	}
	state, err := manager.SourceAuthorityRuntimeStatus(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if state.Ref != ref || state.DeclarationDigest != declarations[0].DeclarationDigest ||
		!state.Closed || state.Epoch != ([16]byte{}) || state.Process != nil {
		t.Fatalf("closed unowned runtime state = %+v", state)
	}
}
