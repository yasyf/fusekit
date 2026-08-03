package holder

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/yasyf/daemonkit"
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

func TestProductTenantLifecycleAuthorizerDelegatesPeerPolicyAndPinsOwner(t *testing.T) {
	denied := errors.New("consumer denied peer")
	allowed := mountservice.Identity{
		Caller: daemonkit.Caller{PID: 42, UID: uint32(os.Geteuid())},
	}
	consumer := mountAuthorizerFunc(func(
		_ context.Context,
		identity mountservice.Identity,
		_ mountproto.Operation,
		_ catalog.TenantID,
		_ catalog.Generation,
	) (tenant.OwnerID, error) {
		if identity.Caller.PID <= 1 || identity.Caller.UID != uint32(os.Geteuid()) {
			return "", denied
		}
		return "cc-notes", nil
	})
	authorizer := productTenantLifecycleAuthorizer{next: consumer, owner: "cc-notes"}
	for _, operation := range []mountproto.Operation{
		mountproto.OperationTenantProvision,
		mountproto.OperationTenantReplace,
		mountproto.OperationTenantRemove,
	} {
		t.Run(string(operation)+"/approved daemon", func(t *testing.T) {
			owner, err := authorizer.Authorize(t.Context(), allowed, operation, "repo-18", 1)
			if err != nil || owner != "cc-notes" {
				t.Fatalf("Authorize = %q, %v", owner, err)
			}
		})
		for _, test := range []struct {
			name     string
			identity mountservice.Identity
		}{
			{name: "wrong uid", identity: func() mountservice.Identity { value := allowed; value.Caller.UID++; return value }()},
			{name: "invalid pid", identity: func() mountservice.Identity { value := allowed; value.Caller.PID = 1; return value }()},
		} {
			t.Run(string(operation)+"/"+test.name, func(t *testing.T) {
				if _, err := authorizer.Authorize(t.Context(), test.identity, operation, "repo-18", 1); !errors.Is(err, denied) {
					t.Fatalf("Authorize error = %v, want %v", err, denied)
				}
			})
		}
	}
}

func TestProductTenantLifecycleAuthorizerRejectsWrongOwner(t *testing.T) {
	next := mountAuthorizerFunc(func(
		context.Context,
		mountservice.Identity,
		mountproto.Operation,
		catalog.TenantID,
		catalog.Generation,
	) (tenant.OwnerID, error) {
		return "other-product", nil
	})
	_, err := (productTenantLifecycleAuthorizer{
		next: next, owner: "cc-notes",
	}).Authorize(t.Context(), mountservice.Identity{}, mountproto.OperationTenantProvision, "repo-18", 1)
	if !errors.Is(err, mountservice.ErrUnauthorized) {
		t.Fatalf("Authorize error = %v, want %v", err, mountservice.ErrUnauthorized)
	}
}

func TestProductTenantLifecycleAuthorizerLeavesStateReadOnly(t *testing.T) {
	called := false
	authorizer := productTenantLifecycleAuthorizer{
		owner: "cc-notes",
		next: mountAuthorizerFunc(func(
			context.Context,
			mountservice.Identity,
			mountproto.Operation,
			catalog.TenantID,
			catalog.Generation,
		) (tenant.OwnerID, error) {
			called = true
			return "cc-notes", nil
		}),
	}
	owner, err := authorizer.Authorize(
		t.Context(), mountservice.Identity{}, mountproto.OperationTenantState, "repo-18", 0,
	)
	if err != nil || owner != "cc-notes" {
		t.Fatalf("Authorize state = %q, %v", owner, err)
	}
	if !called {
		t.Fatal("read-only state authorization did not reach consumer policy")
	}
}
