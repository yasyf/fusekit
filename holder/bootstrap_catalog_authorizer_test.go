package holder

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
)

type recordingCatalogAuthorizer struct {
	authorize func(context.Context, catalogservice.Identity, catalogproto.Operation, catalogservice.Route) (catalogservice.Authorization, error)
	calls     []catalogproto.Operation
}

func (a *recordingCatalogAuthorizer) Authorize(
	ctx context.Context,
	identity catalogservice.Identity,
	operation catalogproto.Operation,
	route catalogservice.Route,
) (catalogservice.Authorization, error) {
	a.calls = append(a.calls, operation)
	return a.authorize(ctx, identity, operation, route)
}

func TestBootstrapCatalogAuthorizerAllowsOnlyBrokerOpenWhileStarting(t *testing.T) {
	gate := &bootstrapGate{}
	want := catalogservice.Authorization{
		Principal: "signed-broker", Role: catalogservice.RoleFileProvider,
		Presentation: catalog.PresentationFileProvider,
	}
	next := &recordingCatalogAuthorizer{authorize: func(
		context.Context,
		catalogservice.Identity,
		catalogproto.Operation,
		catalogservice.Route,
	) (catalogservice.Authorization, error) {
		return want, nil
	}}
	authorizer := bootstrapCatalogAuthorizer{gate: gate, next: next}
	identity := catalogservice.Identity{Peer: wire.Peer{PID: 42, Executable: "/Applications/Product.app/Contents/MacOS/Product"}}
	got, err := authorizer.Authorize(t.Context(), identity, catalogproto.OperationBrokerOpen, catalogservice.Route{})
	if err != nil || got != want {
		t.Fatalf("BrokerOpen while starting = %#v, %v", got, err)
	}

	for _, operation := range []catalogproto.Operation{
		catalogproto.OperationCatalogRoot,
		catalogproto.OperationCatalogHead,
		catalogproto.OperationCatalogSnapshot,
		catalogproto.OperationCatalogChangesSince,
		catalogproto.OperationCatalogLookup,
		catalogproto.OperationCatalogLookupName,
		catalogproto.OperationCatalogOpenAt,
		catalogproto.OperationCatalogMutate,
		catalogproto.OperationTenantPrepare,
		catalogproto.OperationDomainPrepare,
		catalogproto.OperationConvergenceAck,
		catalogproto.OperationConvergenceNotify,
		catalogproto.OperationSourceAuthorityPublishDesiredFleet,
		catalogproto.OperationSourceAuthorityReadDesiredFleet,
		catalogproto.OperationBrokerBindDomain,
		catalogproto.OperationBrokerForward,
	} {
		if _, err := authorizer.Authorize(t.Context(), identity, operation, catalogservice.Route{}); !errors.Is(err, errRuntimeStarting) {
			t.Fatalf("%s while starting = %v, want errRuntimeStarting", operation, err)
		}
	}
	if len(next.calls) != 1 || next.calls[0] != catalogproto.OperationBrokerOpen {
		t.Fatalf("downstream calls while starting = %v", next.calls)
	}

	gate.open()
	if _, err := authorizer.Authorize(t.Context(), identity, catalogproto.OperationCatalogHead, catalogservice.Route{}); err != nil {
		t.Fatalf("ordinary operation after ready = %v", err)
	}
	if len(next.calls) != 2 || next.calls[1] != catalogproto.OperationCatalogHead {
		t.Fatalf("downstream calls after ready = %v", next.calls)
	}
}

func TestBootstrapCatalogAuthorizerPreservesBrokerAuthenticationFailures(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "wrong peer", err: fmt.Errorf("%w: wrong broker peer", trust.ErrUntrustedPeer)},
		{name: "wrong role", err: errors.New("broker role is not File Provider")},
		{name: "wrong signature", err: fmt.Errorf("%w: broker signature mismatch", trust.ErrUntrustedPeer)},
	} {
		t.Run(test.name, func(t *testing.T) {
			next := &recordingCatalogAuthorizer{authorize: func(
				context.Context,
				catalogservice.Identity,
				catalogproto.Operation,
				catalogservice.Route,
			) (catalogservice.Authorization, error) {
				return catalogservice.Authorization{}, test.err
			}}
			authorizer := bootstrapCatalogAuthorizer{gate: &bootstrapGate{}, next: next}
			if _, err := authorizer.Authorize(
				t.Context(), catalogservice.Identity{}, catalogproto.OperationBrokerOpen, catalogservice.Route{},
			); !errors.Is(err, test.err) {
				t.Fatalf("BrokerOpen authentication error = %v, want %v", err, test.err)
			}
			if len(next.calls) != 1 || next.calls[0] != catalogproto.OperationBrokerOpen {
				t.Fatalf("downstream calls = %v", next.calls)
			}
		})
	}
}

func TestBrokerOpenAuthorizationCanUnblockBootstrapReadiness(t *testing.T) {
	gate := &bootstrapGate{}
	bound := make(chan struct{})
	ready := make(chan struct{})
	next := &recordingCatalogAuthorizer{authorize: func(
		_ context.Context,
		_ catalogservice.Identity,
		operation catalogproto.Operation,
		_ catalogservice.Route,
	) (catalogservice.Authorization, error) {
		if operation == catalogproto.OperationBrokerOpen {
			close(bound)
		}
		return catalogservice.Authorization{
			Principal: "signed-broker", Role: catalogservice.RoleFileProvider,
			Presentation: catalog.PresentationFileProvider,
		}, nil
	}}
	go func() {
		<-bound
		gate.open()
		close(ready)
	}()
	authorizer := bootstrapCatalogAuthorizer{gate: gate, next: next}
	if _, err := authorizer.Authorize(
		t.Context(), catalogservice.Identity{}, catalogproto.OperationBrokerOpen, catalogservice.Route{},
	); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-t.Context().Done():
		t.Fatalf("broker bind did not unblock readiness: %v", t.Context().Err())
	}
	if _, err := authorizer.Authorize(
		t.Context(), catalogservice.Identity{}, catalogproto.OperationCatalogHead, catalogservice.Route{},
	); err != nil {
		t.Fatalf("ordinary operation after broker-bound readiness = %v", err)
	}
}
