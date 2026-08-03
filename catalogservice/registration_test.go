package catalogservice

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/yasyf/daemonkit"
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
		{name: "critical fetches", mutate: func(config *FileProviderConfig) { config.CriticalFetches = nil }},
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

func coreOperations() []catalogproto.Operation {
	return []catalogproto.Operation{
		catalogproto.OperationCatalogRoot,
		catalogproto.OperationCatalogHead,
		catalogproto.OperationCatalogSnapshot,
		catalogproto.OperationCatalogChangesSince,
		catalogproto.OperationCatalogLookup,
		catalogproto.OperationCatalogLookupName,
		catalogproto.OperationCatalogOpenAt,
		catalogproto.OperationCatalogRead,
		catalogproto.OperationCatalogClose,
		catalogproto.OperationCatalogMutateBegin,
		catalogproto.OperationCatalogMutateChunk,
		catalogproto.OperationCatalogMutateCommit,
		catalogproto.OperationTenantPrepare,
		catalogproto.OperationPresentationLeaseCommit,
		catalogproto.OperationPresentationLeaseRenew,
		catalogproto.OperationPresentationLeaseRelease,
		catalogproto.OperationSourceAuthorityPublishDesiredFleet,
		catalogproto.OperationSourceAuthorityReadDesiredFleet,
	}
}

func fileProviderOperations() []catalogproto.Operation {
	return []catalogproto.Operation{
		catalogproto.OperationCatalogLookupPrivate,
		catalogproto.OperationCatalogOpenPrivate,
		catalogproto.OperationActivationAck,
		catalogproto.OperationActivationPoll,
		catalogproto.OperationBrokerPoll,
		catalogproto.OperationBrokerResult,
		catalogproto.OperationCriticalReadinessResolve,
		catalogproto.OperationCriticalReadinessFetchAck,
		catalogproto.OperationMaterializationSnapshotBegin,
		catalogproto.OperationMaterializationSnapshotSuspend,
		catalogproto.OperationMaterializationSnapshotStagePage,
		catalogproto.OperationMaterializationSnapshotCommit,
	}
}

func TestRegisterInstallsExactStaticRouteSet(t *testing.T) {
	resolver := func(daemonkit.Request) (*Server, error) { return nil, errors.New("not invoked during registration") }
	tests := []struct {
		name         string
		fileProvider bool
	}{
		{name: "core"},
		{name: "File Provider", fileProvider: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			specs, err := Register(Routes{FileProvider: test.fileProvider}, resolver)
			if err != nil {
				t.Fatalf("Register: %v", err)
			}
			expected := coreOperations()
			if test.fileProvider {
				expected = append(expected, fileProviderOperations()...)
			}
			if len(specs) != len(expected) {
				t.Fatalf("registered %d routes, want %d", len(specs), len(expected))
			}
			registered := make(map[string]transportproto.HandlerSpec, len(specs))
			for _, spec := range specs {
				if _, duplicate := registered[spec.Op]; duplicate {
					t.Fatalf("operation %q registered twice", spec.Op)
				}
				if spec.Handler == nil || !spec.Concurrent {
					t.Fatalf("route %q = %+v", spec.Op, spec)
				}
				registered[spec.Op] = spec
			}
			for _, operation := range expected {
				if _, ok := registered[string(operation)]; !ok {
					t.Fatalf("route %q was not registered", operation)
				}
			}
		})
	}
}

func TestRegisterValidatesStaticInputs(t *testing.T) {
	if _, err := Register(Routes{}, nil); err == nil {
		t.Fatal("nil resolver accepted")
	}
}

func TestResolvedHandlerResolvesExactlyOncePerRequest(t *testing.T) {
	service, err := New(testCoreConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	payload := json.RawMessage(`{"probe":true}`)
	body, err := json.Marshal(requestEnvelope{Tenant: "acct-18", Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	request := daemonkit.Request{Op: string(catalogproto.OperationCatalogRoot), Body: body}
	calls := 0
	handler := resolvedHandler(func(got daemonkit.Request) (*Server, error) {
		calls++
		if got.Op != request.Op || string(got.Body) != string(payload) {
			t.Fatalf("resolver request = %#v", got)
		}
		return service, nil
	}, false, func(got *Server, ctx context.Context, gotRequest daemonkit.Request) ([]byte, error) {
		if got != service || string(gotRequest.Body) != string(payload) {
			t.Fatal("handler did not receive the resolved generation")
		}
		if routing, _ := ctx.Value(routingTenantKey{}).(string); routing != "acct-18" {
			t.Fatalf("routing tenant = %q", routing)
		}
		return []byte("ok"), nil
	})
	value, err := handler(t.Context(), request)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	if string(value) != "ok" || calls != 1 {
		t.Fatalf("value, resolver calls = %q, %d; want ok, 1", value, calls)
	}
}

func TestResolvedFileProviderHandlerRejectsCapabilityMismatch(t *testing.T) {
	service, err := New(testCoreConfig(), nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	body, err := json.Marshal(requestEnvelope{Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	called := false
	handler := resolvedHandler(func(daemonkit.Request) (*Server, error) { return service, nil }, true,
		func(*Server, context.Context, daemonkit.Request) ([]byte, error) {
			called = true
			return nil, nil
		})
	if _, err := handler(t.Context(), daemonkit.Request{Body: body}); err == nil {
		t.Fatal("File Provider route accepted a core-only generation")
	}
	if called {
		t.Fatal("capability-mismatched handler ran")
	}
}

func testCoreConfig() CoreConfig {
	return CoreConfig{
		Reader: newFakeReader(1), Mutations: &fakeMutations{}, Preparation: fakePreparation{}, Leases: fakeFileProviderLeaseStore{},
		SourceFleets: fakeSourceFleetService{}, Authorizer: fakeAuthorizer{},
	}
}

func testFileProviderConfig() FileProviderConfig {
	return FileProviderConfig{
		Activations: fakeActivations{}, Broker: fakeBroker{}, Materialization: &fakeMaterialization{}, CriticalFetches: fakeCriticalFetches{},
		ProtectedPeer: func(context.Context, daemonkit.Caller) error { return nil },
	}
}
