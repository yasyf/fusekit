package holder

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/tenant"
	"github.com/yasyf/fusekit/transportproto"
)

func TestOneSessionServesMountAndCatalogAndOwnsOneRoot(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	runtime, err := New(t.Context(), testConfig(dir, "v1.0.0", native))
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitNativeStart(t, native, done)

	mountClient, err := mountservice.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(filepath.Join(dir, "holder.sock")), Build: transportproto.Build,
	})
	if err != nil {
		t.Fatal(err)
	}
	definition := mountproto.TenantDefinition{
		PresentationRoot: filepath.Join(dir, "mount", "acct-18"),
		BackingRoot:      filepath.Join(dir, "backing"), ContentSourceID: "source",
		AccessMode: mountproto.AccessModeReadWrite, CasePolicy: mountproto.CasePolicySensitive,
		Presentations: []mountproto.Presentation{mountproto.PresentationMount}, Generation: 1,
	}
	if response, err := mountClient.RegisterTenant(t.Context(), "acct-18", definition); err != nil || response.Code != mountproto.ErrorCodeOk {
		t.Fatalf("RegisterTenant = %#v, %v", response, err)
	}
	if err := mountClient.Close(); err != nil {
		t.Fatal(err)
	}

	catalogClient, err := catalogservice.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(filepath.Join(dir, "holder.sock")), Build: transportproto.Build,
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := catalogClient.Head(t.Context(), "acct-18", 1)
	if err != nil || response.Code != catalogproto.ErrorCodeOk || response.Revision == 0 {
		t.Fatalf("Head = %#v, %v", response, err)
	}
	if err := catalogClient.Close(); err != nil {
		t.Fatal(err)
	}

	closeRuntime(t, runtime, done)
	if starts, closes := native.counts(); starts != 1 || closes != 1 {
		t.Fatalf("native lifecycle = %d starts, %d closes", starts, closes)
	}
}

func TestHolderServesExactTransportBeforeNativeStartup(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	native.onStart = func(ctx context.Context) error {
		client, err := wire.NewClient(ctx, wire.ClientConfig{
			Dial: wire.UnixDialer(filepath.Join(dir, "holder.sock")), Build: transportproto.Build,
		})
		if err != nil {
			return err
		}
		return client.Close()
	}
	runtime, err := New(t.Context(), testConfig(dir, "v1.0.0", native))
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitNativeStart(t, native, done)
	closeRuntime(t, runtime, done)
}

func TestHolderRejectsWorkerLimitConsumedEntirelyByNativeChild(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.WorkerLimit = 1
	if _, err := New(t.Context(), config); err == nil {
		t.Fatal("worker limit one was accepted")
	}
}

func TestHolderRequiresTypedSignedPeerRequirement(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.trustCheck = nil
	if _, err := New(t.Context(), config); err == nil {
		t.Fatal("empty signed peer requirement was accepted")
	}
}

func TestNewerRuntimeUnmountsIncumbentBeforeStartingSuccessorRoot(t *testing.T) {
	dir := shortTempDir(t)
	var events []string
	var eventsMu sync.Mutex
	record := func(event string) {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
	}
	oldNative := newTestNative(func(event string) { record("old-" + event) })
	oldConfig := testConfig(filepath.Join(dir, "old"), "v1.0.0", oldNative)
	oldConfig.Socket = filepath.Join(dir, "holder.sock")
	oldConfig.Root = filepath.Join(dir, "mount")
	oldRuntime, err := New(t.Context(), oldConfig)
	if err != nil {
		t.Fatal(err)
	}
	oldDone := runRuntime(t, oldRuntime)
	waitNativeStart(t, oldNative, oldDone)

	newNative := newTestNative(func(event string) { record("new-" + event) })
	newConfig := testConfig(filepath.Join(dir, "new"), "v1.1.0", newNative)
	newConfig.Socket = oldConfig.Socket
	newConfig.Root = oldConfig.Root
	newRuntime, err := New(t.Context(), newConfig)
	if err != nil {
		t.Fatal(err)
	}
	newDone := runRuntime(t, newRuntime)
	waitNativeStart(t, newNative, newDone)
	if err := waitRuntime(oldDone); err != nil {
		t.Fatalf("incumbent Run: %v", err)
	}
	closeRuntime(t, newRuntime, newDone)

	eventsMu.Lock()
	defer eventsMu.Unlock()
	oldClose, newStart := eventIndex(events, "old-close"), eventIndex(events, "new-start")
	if oldClose < 0 || newStart < 0 || oldClose >= newStart {
		t.Fatalf("takeover order = %v", events)
	}
}

func testConfig(dir, build string, native nativeController) Config {
	return Config{
		Socket: filepath.Join(dir, "holder.sock"), Root: filepath.Join(dir, "mount"),
		CatalogPath: filepath.Join(dir, "catalog.sqlite"), Build: build,
		Planner: testPlanner{}, WorkerRegistry: testRegistry{}, native: native,
		Authorizer: testMountAuthorizer{}, trustCheck: func(wire.Peer) error { return nil },
		CatalogService:  testCatalogService,
		ShutdownTimeout: 5 * time.Second,
	}
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "fk-holder-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
	})
	return dir
}

func testCatalogService(ctx context.Context, store *catalog.Catalog, runtime *tenant.TenantRuntime) (catalogservice.Config, error) {
	id, err := catalog.NewMutationID()
	if err != nil {
		return catalogservice.Config{}, err
	}
	if _, err := store.CreateTenant(ctx, id, "acct-18", catalog.CaseSensitive, catalog.PresentMount); err != nil {
		return catalogservice.Config{}, err
	}
	return catalogservice.Config{
		Reader: catalogservice.CatalogReader{Catalog: store}, Mutations: testMutations{},
		Preparation: testPreparation{runtime: runtime}, Convergence: testConvergence{},
		Broker: testBroker{}, Authorizer: testCatalogAuthorizer{},
	}, nil
}

type testNative struct {
	mu       sync.Mutex
	starts   int
	closes   int
	started  chan struct{}
	recorder func(string)
	onStart  func(context.Context) error
}

func newTestNative(recorder func(string)) *testNative {
	return &testNative{started: make(chan struct{}), recorder: recorder}
}

func (n *testNative) Start(ctx context.Context, _ string, _ mountmux.Resolver) error {
	n.mu.Lock()
	onStart := n.onStart
	n.starts++
	if n.recorder != nil {
		n.recorder("start")
	}
	n.mu.Unlock()
	if onStart != nil {
		if err := onStart(ctx); err != nil {
			return err
		}
	}
	n.mu.Lock()
	select {
	case <-n.started:
	default:
		close(n.started)
	}
	n.mu.Unlock()
	return nil
}

func (n *testNative) Close(context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.closes++
	if n.recorder != nil {
		n.recorder("close")
	}
	return nil
}

func (n *testNative) counts() (int, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.starts, n.closes
}

func (*testNative) Bind(context.Context, mountservice.Identity) error  { return nil }
func (*testNative) Ready(context.Context, mountservice.Identity) error { return nil }
func (*testNative) Unbind(mountservice.Identity)                       {}
func (*testNative) HealthState() daemon.State                          { return daemon.StateHealthy }

func runRuntime(t *testing.T, runtime *Runtime) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(context.Background()) }()
	return done
}

func waitNativeStart(t *testing.T, native *testNative, done <-chan error) {
	t.Helper()
	select {
	case <-native.started:
	case err := <-done:
		t.Fatalf("runtime stopped before native root start: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("native root did not start")
	}
}

func closeRuntime(t *testing.T, runtime *Runtime, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := waitRuntime(done); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func waitRuntime(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		return errors.New("runtime did not stop")
	}
}

func eventIndex(events []string, target string) int {
	for index, event := range events {
		if event == target {
			return index
		}
	}
	return -1
}

type testRegistry struct{}

func (testRegistry) TrackGroup(context.Context, int) (proc.Record, error) {
	return proc.Record{}, errors.New("unexpected worker")
}
func (testRegistry) Untrack(context.Context, proc.Record) error { return nil }
func (testRegistry) Owns(proc.Record) (bool, error)             { return false, nil }
func (testRegistry) Reap(context.Context) error                 { return nil }

type testPlanner struct{}

func (testPlanner) PrepareSourceMutation(context.Context, tenant.SourceMutationStep) (tenant.SourceMutationWorker, error) {
	return tenant.SourceMutationWorker{}, errors.New("unexpected source mutation")
}
func (testPlanner) PrepareMaterialization(context.Context, tenant.Catalog, tenant.MaterializationStep) (tenant.WorkerSpec, error) {
	return tenant.WorkerSpec{}, errors.New("unexpected materialization")
}
func (testPlanner) PrepareMountLifecycle(context.Context, tenant.Catalog, tenant.MountLifecycleStep) (*tenant.WorkerSpec, error) {
	return nil, nil
}

type testMountAuthorizer struct{}

func (testMountAuthorizer) Authorize(_ context.Context, _ mountservice.Identity, _ mountproto.Operation, _ catalog.TenantID, _ catalog.Generation) (tenant.OwnerID, error) {
	return "owner", nil
}

func (testMountAuthorizer) AuthorizeNative(context.Context, mountservice.Identity, mountproto.Operation) error {
	return nil
}

type testCatalogAuthorizer struct{}

func (testCatalogAuthorizer) Authorize(_ context.Context, _ catalogservice.Identity, _ catalogproto.Operation, route catalogservice.Route) (catalogservice.Authorization, error) {
	return catalogservice.Authorization{
		Principal: "owner", Role: catalogservice.RoleMount, Presentation: catalog.PresentationMount, Route: route,
	}, nil
}

type testMutations struct{}

func (testMutations) StageMutation(context.Context, catalogservice.Identity, catalogservice.Authorization, catalog.TenantID, catalog.MutationID, catalog.Generation, bool, io.Reader) (catalogservice.MutationStage, error) {
	return catalogservice.MutationStage{}, errors.New("unexpected mutation")
}
func (testMutations) SubmitMutation(context.Context, catalogservice.Identity, catalogservice.Authorization, catalogservice.MutationSubmission) (catalogservice.MutationResult, error) {
	return catalogservice.MutationResult{}, errors.New("unexpected mutation")
}

type testPreparation struct{ runtime *tenant.TenantRuntime }

func (p testPreparation) PrepareTenant(context.Context, catalogservice.Identity, catalog.TenantID, catalogproto.PrepareTenantRequest) (catalogproto.PreparationProof, error) {
	return catalogproto.PreparationProof{}, errors.New("unexpected preparation")
}

type testConvergence struct{}

func (testConvergence) AckConvergence(context.Context, catalogservice.Identity, catalog.TenantID, catalogproto.AckConvergenceRequest) (catalogproto.DomainObservation, error) {
	return catalogproto.DomainObservation{}, errors.New("unexpected acknowledgement")
}

type testBroker struct{}

func (testBroker) OpenBroker(context.Context, catalogservice.Identity, string) (catalogservice.BrokerSession, error) {
	return testBrokerSession{}, nil
}

type testBrokerSession struct{}

func (testBrokerSession) Commands() <-chan catalogproto.BrokerCommand {
	return make(chan catalogproto.BrokerCommand)
}
func (testBrokerSession) AcceptResult(context.Context, catalogproto.BrokerResult) error { return nil }
func (testBrokerSession) Close(error)                                                   {}

var _ supervise.WorkerRegistry = testRegistry{}
