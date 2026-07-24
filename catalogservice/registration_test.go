package catalogservice

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/transportproto"
)

func TestNewValidatesGenerationLocalServices(t *testing.T) {
	core := testCoreConfig()
	fileProvider := testFileProviderConfig()
	if _, err := New(core, &fileProvider); err != nil {
		t.Fatalf("New: %v", err)
	}

	coreTests := []struct {
		name   string
		mutate func(*CoreConfig)
	}{
		{name: "reader", mutate: func(config *CoreConfig) { config.Reader = nil }},
		{name: "mutations", mutate: func(config *CoreConfig) { config.Mutations = nil }},
		{name: "preparation", mutate: func(config *CoreConfig) { config.Preparation = nil }},
		{name: "leases", mutate: func(config *CoreConfig) { config.Leases = nil }},
		{name: "source fleets", mutate: func(config *CoreConfig) { config.SourceFleets = nil }},
		{name: "authorizer", mutate: func(config *CoreConfig) { config.Authorizer = nil }},
	}
	for _, test := range coreTests {
		t.Run("core "+test.name, func(t *testing.T) {
			config := core
			test.mutate(&config)
			if _, err := New(config, nil); err == nil {
				t.Fatal("incomplete core constructed")
			}
		})
	}

	fileProviderTests := []struct {
		name   string
		mutate func(*FileProviderConfig)
	}{
		{name: "activations", mutate: func(config *FileProviderConfig) { config.Activations = nil }},
		{name: "broker", mutate: func(config *FileProviderConfig) { config.Broker = nil }},
		{name: "materialization", mutate: func(config *FileProviderConfig) { config.Materialization = nil }},
		{name: "protected peer", mutate: func(config *FileProviderConfig) { config.ProtectedPeer = nil }},
	}
	for _, test := range fileProviderTests {
		t.Run("File Provider "+test.name, func(t *testing.T) {
			config := fileProvider
			test.mutate(&config)
			if _, err := New(core, &config); err == nil {
				t.Fatal("incomplete File Provider service constructed")
			}
		})
	}
}

func TestRegisterInstallsExactStaticRouteSet(t *testing.T) {
	tests := []struct {
		name         string
		fileProvider bool
	}{
		{name: "core"},
		{name: "File Provider", fileProvider: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wireServer := &wire.Server{WireBuild: transportproto.WireBuild}
			if err := Register(wireServer, Routes{FileProvider: test.fileProvider}, func(wire.Request) (*Server, error) {
				return nil, errors.New("not invoked during registration")
			}); err != nil {
				t.Fatalf("Register: %v", err)
			}
			for _, route := range coreRoutes() {
				assertRouteRegistered(t, wireServer, route)
			}
			for _, route := range fileProviderRoutes() {
				if test.fileProvider {
					assertRouteRegistered(t, wireServer, route)
					continue
				}
				route.register(wireServer)
			}
			wireServer.Register(wire.HandlerSpec{
				Op:         "catalog.test.unrelated",
				Handler:    func(context.Context, wire.Request) (any, error) { return nil, nil },
				Concurrent: true,
			})
		})
	}
}

func TestRegisterValidatesStaticInputs(t *testing.T) {
	resolver := func(wire.Request) (*Server, error) { return nil, nil }
	if err := Register(nil, Routes{}, resolver); err == nil {
		t.Fatal("nil daemonkit server accepted")
	}
	if err := Register(&wire.Server{WireBuild: "wrong"}, Routes{}, resolver); err == nil {
		t.Fatal("wrong transport suite accepted")
	}
	if err := Register(&wire.Server{WireBuild: transportproto.WireBuild}, Routes{}, nil); err == nil {
		t.Fatal("nil resolver accepted")
	}
}

func TestResolvedHandlerResolvesExactlyOncePerRequest(t *testing.T) {
	service, err := New(testCoreConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	request := wire.Request{ID: 17, Op: wire.Op(catalogproto.OperationCatalogRoot), Tenant: "acct-18"}
	calls := 0
	handler := resolvedHandler(func(got wire.Request) (*Server, error) {
		calls++
		if got.ID != request.ID || got.Op != request.Op || got.Tenant != request.Tenant {
			t.Fatalf("resolver request = %#v, want %#v", got, request)
		}
		return service, nil
	}, false, func(got *Server, _ context.Context, gotRequest wire.Request) (any, error) {
		if got != service || gotRequest.ID != request.ID {
			t.Fatal("handler did not receive the resolved generation")
		}
		return "ok", nil
	})
	value, err := handler(t.Context(), request)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if value != "ok" || calls != 1 {
		t.Fatalf("value, resolver calls = %v, %d; want ok, 1", value, calls)
	}
}

func TestResolvedFileProviderHandlerRejectsCapabilityMismatch(t *testing.T) {
	service, err := New(testCoreConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	called := false
	handler := resolvedHandler(func(wire.Request) (*Server, error) { return service, nil }, true,
		func(*Server, context.Context, wire.Request) (any, error) {
			called = true
			return nil, nil
		})
	if _, err := handler(t.Context(), wire.Request{}); err == nil {
		t.Fatal("File Provider route accepted a core-only generation")
	}
	if called {
		t.Fatal("capability-mismatched handler ran")
	}
}

type registeredRoute struct {
	operation  catalogproto.Operation
	concurrent bool
}

func (r registeredRoute) register(server *wire.Server) {
	server.Register(wire.HandlerSpec{
		Op: wire.Op(r.operation), Concurrent: r.concurrent,
		Handler: func(context.Context, wire.Request) (any, error) { return nil, nil },
	})
}

func coreRoutes() []registeredRoute {
	return []registeredRoute{
		{catalogproto.OperationCatalogRoot, true},
		{catalogproto.OperationCatalogHead, true},
		{catalogproto.OperationCatalogSnapshot, true},
		{catalogproto.OperationCatalogChangesSince, true},
		{catalogproto.OperationCatalogLookup, true},
		{catalogproto.OperationCatalogLookupName, true},
		{catalogproto.OperationCatalogOpenAt, true},
		{catalogproto.OperationCatalogMutate, true},
		{catalogproto.OperationTenantPrepare, true},
		{catalogproto.OperationPresentationLeaseCommit, true},
		{catalogproto.OperationPresentationLeaseRenew, true},
		{catalogproto.OperationPresentationLeaseRelease, true},
		{catalogproto.OperationSourceAuthorityPublishDesiredFleet, true},
		{catalogproto.OperationSourceAuthorityReadDesiredFleet, true},
	}
}

func fileProviderRoutes() []registeredRoute {
	return []registeredRoute{
		{catalogproto.OperationActivationAck, true},
		{catalogproto.OperationBrokerForward, true},
		{catalogproto.OperationBrokerOpen, false},
	}
}

func assertRouteRegistered(t *testing.T, server *wire.Server, route registeredRoute) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatalf("route %q was not registered", route.operation)
		}
	}()
	route.register(server)
}

func testCoreConfig() CoreConfig {
	return CoreConfig{
		Reader: newFakeReader(1), Mutations: &fakeMutations{}, Preparation: fakePreparation{}, Leases: fakeFileProviderLeaseStore{},
		SourceFleets: fakeSourceFleetService{}, Authorizer: fakeAuthorizer{},
	}
}

func testFileProviderConfig() FileProviderConfig {
	return FileProviderConfig{
		Activations: fakeActivations{}, Broker: fakeBroker{}, Materialization: &fakeMaterialization{},
		ProtectedPeer: func(context.Context, wire.Peer) error { return nil },
	}
}
