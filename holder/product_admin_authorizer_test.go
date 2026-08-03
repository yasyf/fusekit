package holder

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountservice"
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

func TestProtectedProductAdminAuthorizerPinsPrincipal(t *testing.T) {
	for _, test := range []struct {
		name      string
		principal string
		role      catalogservice.Role
		wantErr   bool
	}{
		{name: "exact product admin", principal: "cc-notes", role: catalogservice.RoleProductAdmin},
		{name: "wrong owner", principal: "other-product", role: catalogservice.RoleProductAdmin, wantErr: true},
		{name: "non-admin role passes through", principal: "other-product", role: catalogservice.RoleFileProvider},
	} {
		t.Run(test.name, func(t *testing.T) {
			authorizer := protectedProductAdminAuthorizer{
				principal: "cc-notes",
				next: catalogAuthorizerFunc(func(
					context.Context,
					catalogservice.Identity,
					catalogproto.Operation,
					catalogservice.Route,
				) (catalogservice.Authorization, error) {
					return catalogservice.Authorization{
						Principal: test.principal, Role: test.role,
					}, nil
				}),
			}
			_, err := authorizer.Authorize(
				t.Context(), catalogservice.Identity{},
				catalogproto.OperationSourceAuthorityPublishDesiredFleet, catalogservice.Route{},
			)
			if test.wantErr {
				if !errors.Is(err, mountservice.ErrUnauthorized) {
					t.Fatalf("Authorize error = %v, want %v", err, mountservice.ErrUnauthorized)
				}
				return
			}
			if err != nil {
				t.Fatalf("Authorize error = %v", err)
			}
		})
	}
}

func TestProtectedProductAdminAuthorizerPropagatesConsumerRefusal(t *testing.T) {
	denied := errors.New("consumer denied")
	authorizer := protectedProductAdminAuthorizer{
		principal: "cc-notes",
		next: catalogAuthorizerFunc(func(
			context.Context,
			catalogservice.Identity,
			catalogproto.Operation,
			catalogservice.Route,
		) (catalogservice.Authorization, error) {
			return catalogservice.Authorization{}, denied
		}),
	}
	_, err := authorizer.Authorize(
		t.Context(), catalogservice.Identity{},
		catalogproto.OperationSourceAuthorityReadDesiredFleet, catalogservice.Route{},
	)
	if !errors.Is(err, denied) {
		t.Fatalf("Authorize error = %v, want %v", err, denied)
	}
}
