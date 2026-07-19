package holder

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
)

type catalogAuthorizerFunc func(
	context.Context,
	catalogservice.Identity,
	catalogproto.Operation,
	catalogservice.Route,
) (catalogservice.Authorization, error)

func (f catalogAuthorizerFunc) Authorize(
	ctx context.Context,
	identity catalogservice.Identity,
	operation catalogproto.Operation,
	route catalogservice.Route,
) (catalogservice.Authorization, error) {
	return f(ctx, identity, operation, route)
}

func TestProtectedProductAdminAuthorizerRequiresExactSignedRuntime(t *testing.T) {
	const executable = "/Applications/CCNotesHolder.app/Contents/MacOS/CCNotesHolder"
	for _, test := range []struct {
		name       string
		executable string
		verifyErr  error
		want       error
	}{
		{name: "fixed signed holder", executable: executable},
		{name: "same uid unsigned", executable: executable, verifyErr: trust.ErrNoVerifier, want: trust.ErrNoVerifier},
		{name: "same uid wrong team", executable: executable, verifyErr: trust.ErrUntrustedPeer, want: trust.ErrUntrustedPeer},
		{name: "same uid wrong bundle", executable: executable, verifyErr: trust.ErrUntrustedPeer, want: trust.ErrUntrustedPeer},
		{name: "same uid missing entitlement", executable: executable, verifyErr: trust.ErrUntrustedPeer, want: trust.ErrUntrustedPeer},
		{name: "same uid wrong executable", executable: "/tmp/CCNotesHolder", want: trust.ErrUntrustedPeer},
	} {
		t.Run(test.name, func(t *testing.T) {
			verified := false
			protectedPeer := candidateProtectedPeer(executable, func(context.Context, wire.Peer) error {
				verified = true
				return test.verifyErr
			})
			authorizer := protectedProductAdminAuthorizer{
				principal:     "cc-notes",
				protectedPeer: protectedPeer,
				next: catalogAuthorizerFunc(func(
					context.Context,
					catalogservice.Identity,
					catalogproto.Operation,
					catalogservice.Route,
				) (catalogservice.Authorization, error) {
					return catalogservice.Authorization{
						Principal: "cc-notes", Role: catalogservice.RoleProductAdmin,
					}, nil
				}),
			}
			_, err := authorizer.Authorize(t.Context(), catalogservice.Identity{
				Peer: wire.Peer{UID: os.Geteuid(), Executable: test.executable, Audit: []byte(test.name)},
			}, catalogproto.OperationSourceAuthorityPublishDesiredFleet, catalogservice.Route{})
			if !errors.Is(err, test.want) {
				t.Fatalf("Authorize error = %v, want %v", err, test.want)
			}
			if test.executable == executable && !verified {
				t.Fatal("exact executable did not run the mandatory signed-peer verifier")
			}
			if test.executable != executable && verified {
				t.Fatal("wrong executable reached the signed-peer verifier")
			}
		})
	}
}

func TestProtectedProductAdminAuthorizerRejectsWrongOwnerAndCannotBeDisabled(t *testing.T) {
	next := catalogAuthorizerFunc(func(
		context.Context,
		catalogservice.Identity,
		catalogproto.Operation,
		catalogservice.Route,
	) (catalogservice.Authorization, error) {
		return catalogservice.Authorization{
			Principal: "other-product", Role: catalogservice.RoleProductAdmin,
		}, nil
	})
	for _, test := range []struct {
		name       string
		principal  string
		verifyPeer func(context.Context, wire.Peer) error
	}{
		{name: "wrong owner", principal: "cc-notes", verifyPeer: func(context.Context, wire.Peer) error { return nil }},
		{name: "missing verifier", principal: "other-product"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := (protectedProductAdminAuthorizer{
				next: next, principal: test.principal, protectedPeer: test.verifyPeer,
			}).Authorize(t.Context(), catalogservice.Identity{}, catalogproto.OperationSourceAuthorityReadDesiredFleet, catalogservice.Route{})
			if err == nil {
				t.Fatal("Authorize succeeded")
			}
		})
	}
}
