package catalogservice

import (
	"context"
	"testing"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/transportproto"
)

func TestRegisterCoreRegistersNoFileProviderRoutes(t *testing.T) {
	server, _ := registerTestCore(t)
	assertFileProviderRoutesUnregistered(t, server)
}

func TestRegisterFileProviderValidatesEveryCapabilityBeforeRegisteringRoutes(t *testing.T) {
	valid := FileProviderConfig{
		Convergence: fakeConvergence{},
		Broker:      fakeBroker{},
		ProtectedPeer: func(context.Context, wire.Peer) error {
			return nil
		},
	}
	tests := []struct {
		name   string
		config FileProviderConfig
	}{
		{name: "convergence", config: FileProviderConfig{Broker: valid.Broker, ProtectedPeer: valid.ProtectedPeer}},
		{name: "broker", config: FileProviderConfig{Convergence: valid.Convergence, ProtectedPeer: valid.ProtectedPeer}},
		{name: "protected peer", config: FileProviderConfig{Convergence: valid.Convergence, Broker: valid.Broker}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wireServer, core := registerTestCore(t)
			if err := RegisterFileProvider(core, test.config); err == nil {
				t.Fatal("incomplete File Provider capability registered")
			}
			assertFileProviderRoutesUnregistered(t, wireServer)
		})
	}
}

func TestRegisterFileProviderRegistersExactRouteSetOnce(t *testing.T) {
	wireServer, core := registerTestCore(t)
	config := FileProviderConfig{
		Convergence: fakeConvergence{},
		Broker:      fakeBroker{},
		ProtectedPeer: func(context.Context, wire.Peer) error {
			return nil
		},
	}
	if err := RegisterFileProvider(core, config); err != nil {
		t.Fatalf("RegisterFileProvider: %v", err)
	}
	for _, route := range fileProviderRoutes() {
		t.Run(string(route.operation), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("File Provider route was not registered")
				}
			}()
			route.register(wireServer)
		})
	}
	if err := RegisterFileProvider(core, config); err == nil {
		t.Fatal("File Provider capability registered twice")
	}
	wireServer.Register(wire.HandlerSpec{
		Op:         "catalog.test.unrelated",
		Handler:    func(context.Context, wire.Request) (any, error) { return nil, nil },
		Concurrent: true,
	})
}

type fileProviderRoute struct {
	operation catalogproto.Operation
	control   bool
}

func (r fileProviderRoute) register(server *wire.Server) {
	handler := func(context.Context, wire.Request) (any, error) { return nil, nil }
	server.Register(wire.HandlerSpec{Op: wire.Op(r.operation), Handler: handler, Concurrent: !r.control})
}

func fileProviderRoutes() []fileProviderRoute {
	return []fileProviderRoute{
		{operation: catalogproto.OperationActivationAck},
		{operation: catalogproto.OperationBrokerForward},
		{operation: catalogproto.OperationBrokerOpen, control: true},
	}
}

func assertFileProviderRoutesUnregistered(t *testing.T, server *wire.Server) {
	t.Helper()
	for _, route := range fileProviderRoutes() {
		route.register(server)
	}
}

func registerTestCore(t *testing.T) (*wire.Server, *Server) {
	t.Helper()
	wireServer := &wire.Server{WireBuild: transportproto.WireBuild}
	service, err := RegisterCore(wireServer, CoreConfig{
		Reader:       newFakeReader(1),
		Mutations:    &fakeMutations{},
		Preparation:  fakePreparation{},
		SourceFleets: fakeSourceFleetService{},
		Authorizer:   fakeAuthorizer{},
	})
	if err != nil {
		t.Fatalf("RegisterCore: %v", err)
	}
	return wireServer, service
}
