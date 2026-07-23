package holder

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/sourcedriver"
)

type exactSourceDriverRegistry struct {
	expected SourceDriverInvocation
	calls    int
}

func (r *exactSourceDriverRegistry) SourceDriver(
	_ context.Context,
	invocation SourceDriverInvocation,
) (sourcedriver.Driver, error) {
	r.calls++
	if !reflect.DeepEqual(invocation, r.expected) {
		return nil, errors.New("source driver invocation changed")
	}
	return nil, errors.New("driver construction intentionally stopped")
}

func TestSourceDriverChildBindsExactInvocationBeforeSessionIO(t *testing.T) {
	spec := SemanticDriverSpec{
		Authority: "semantic", DriverID: "git-driver",
		DriverConfig:      []byte("repo=/tmp/example"),
		DeclarationDigest: sha256.Sum256([]byte("product declaration")),
	}
	fleet := SourceAuthorityFleet{Owner: "product", Generation: 7, Authorities: []SourceAuthoritySpec{spec}}
	targets := []catalog.SourceDriverTarget{{Tenant: "tenant", Generation: 3}}
	arguments, err := sourceDriverChildArguments(fleet, spec, targets)
	if err != nil {
		t.Fatal(err)
	}
	parsed, recognized, err := parseSourceDriverChildArguments(arguments)
	if err != nil || !recognized {
		t.Fatalf("parse exact invocation = %+v, %t, %v", parsed, recognized, err)
	}
	registry := &exactSourceDriverRegistry{expected: parsed.SourceDriverInvocation}
	arguments[6] = "stale-driver"
	handled, err := runSourceDriverChild(t.Context(), arguments, registry)
	if !handled || err == nil || registry.calls != 1 {
		t.Fatalf("mismatched invocation = handled %t, error %v, calls %d", handled, err, registry.calls)
	}
}

func TestSourceDriverChildRejectsOversizedDriverConfigBeforeArguments(t *testing.T) {
	spec := SemanticDriverSpec{
		Authority: "semantic", DriverID: "git-driver",
		DriverConfig:      make([]byte, catalog.SourceDriverConfigMaxBytes+1),
		DeclarationDigest: sha256.Sum256([]byte("product declaration")),
	}
	_, err := sourceDriverChildArguments(
		SourceAuthorityFleet{Owner: "product", Generation: 1, Authorities: []SourceAuthoritySpec{spec}},
		spec,
		[]catalog.SourceDriverTarget{{Tenant: "tenant", Generation: 1}},
	)
	if err == nil {
		t.Fatal("oversized DriverConfig was accepted")
	}
}
