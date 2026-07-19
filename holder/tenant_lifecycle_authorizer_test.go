package holder

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
)

type mountAuthorizerFunc func(
	context.Context,
	mountservice.Identity,
	mountproto.Operation,
	catalog.TenantID,
	catalog.Generation,
) (tenant.OwnerID, error)

func (f mountAuthorizerFunc) Authorize(
	ctx context.Context,
	identity mountservice.Identity,
	operation mountproto.Operation,
	tenantID catalog.TenantID,
	generation catalog.Generation,
) (tenant.OwnerID, error) {
	return f(ctx, identity, operation, tenantID, generation)
}

func (mountAuthorizerFunc) AuthorizeNative(context.Context, mountservice.Identity, mountproto.Operation) error {
	return nil
}

func TestProtectedTenantLifecycleAuthorizerRequiresExactSignedRuntime(t *testing.T) {
	const executable = "/Applications/CCNotesHolder.app/Contents/MacOS/CCNotesHolder"
	for _, operation := range []mountproto.Operation{
		mountproto.OperationTenantProvision,
		mountproto.OperationTenantReplace,
		mountproto.OperationTenantRemove,
	} {
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
			t.Run(string(operation)+"/"+test.name, func(t *testing.T) {
				verified := false
				authorized := false
				protectedPeer := candidateProtectedPeer(executable, func(context.Context, wire.Peer) error {
					verified = true
					return test.verifyErr
				})
				authorizer := protectedTenantLifecycleAuthorizer{
					owner:         "cc-notes",
					protectedPeer: protectedPeer,
					next: mountAuthorizerFunc(func(
						context.Context,
						mountservice.Identity,
						mountproto.Operation,
						catalog.TenantID,
						catalog.Generation,
					) (tenant.OwnerID, error) {
						authorized = true
						return "cc-notes", nil
					}),
				}
				_, err := authorizer.Authorize(t.Context(), mountservice.Identity{
					Peer: wire.Peer{UID: os.Geteuid(), Executable: test.executable, Audit: []byte(test.name)},
				}, operation, "repo-18", 1)
				if !errors.Is(err, test.want) {
					t.Fatalf("Authorize error = %v, want %v", err, test.want)
				}
				if test.executable == executable && !verified {
					t.Fatal("exact executable did not run the mandatory signed-peer verifier")
				}
				if test.executable != executable && verified {
					t.Fatal("wrong executable reached the signed-peer verifier")
				}
				if test.want != nil && authorized {
					t.Fatal("untrusted peer reached the consumer authorizer")
				}
				if test.want == nil && !authorized {
					t.Fatal("trusted peer did not reach the consumer authorizer")
				}
			})
		}
	}
}

func TestProtectedTenantLifecycleAuthorizerRejectsWrongOwnerAndCannotBeDisabled(t *testing.T) {
	next := mountAuthorizerFunc(func(
		context.Context,
		mountservice.Identity,
		mountproto.Operation,
		catalog.TenantID,
		catalog.Generation,
	) (tenant.OwnerID, error) {
		return "other-product", nil
	})
	for _, test := range []struct {
		name       string
		owner      tenant.OwnerID
		verifyPeer func(context.Context, wire.Peer) error
	}{
		{name: "wrong owner", owner: "cc-notes", verifyPeer: func(context.Context, wire.Peer) error { return nil }},
		{name: "missing verifier", owner: "other-product"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := (protectedTenantLifecycleAuthorizer{
				next: next, owner: test.owner, protectedPeer: test.verifyPeer,
			}).Authorize(t.Context(), mountservice.Identity{}, mountproto.OperationTenantProvision, "repo-18", 1)
			if err == nil {
				t.Fatal("Authorize succeeded")
			}
		})
	}
}

func TestProtectedTenantLifecycleAuthorizerLeavesStateReadOnly(t *testing.T) {
	verified := false
	authorizer := protectedTenantLifecycleAuthorizer{
		owner: "cc-notes",
		protectedPeer: func(context.Context, wire.Peer) error {
			verified = true
			return trust.ErrUntrustedPeer
		},
		next: mountAuthorizerFunc(func(
			context.Context,
			mountservice.Identity,
			mountproto.Operation,
			catalog.TenantID,
			catalog.Generation,
		) (tenant.OwnerID, error) {
			return "cc-notes", nil
		}),
	}
	owner, err := authorizer.Authorize(
		t.Context(), mountservice.Identity{}, mountproto.OperationTenantState, "repo-18", 0,
	)
	if err != nil || owner != "cc-notes" {
		t.Fatalf("Authorize state = %q, %v", owner, err)
	}
	if verified {
		t.Fatal("read-only state authorization reached the signed-peer verifier")
	}
}
