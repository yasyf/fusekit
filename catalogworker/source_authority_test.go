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
