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
		Dial: wire.UnixDialer(filepath.Join(dir, "fusekit.sock")), Build: transportproto.Build,
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
	if response, err := mountClient.ProvisionTenant(t.Context(), "acct-18", definition); err != nil || response.Code != mountproto.ErrorCodeOk {
		t.Fatalf("ProvisionTenant = %#v, %v", response, err)
	}
	if err := mountClient.Close(); err != nil {
		t.Fatal(err)
	}

	catalogClient, err := catalogservice.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(filepath.Join(dir, "fusekit.sock")), Build: transportproto.Build,
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
			Dial: wire.UnixDialer(filepath.Join(dir, "fusekit.sock")), Build: transportproto.Build,
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

func TestProductionRuntimeOwnsConvergenceBrokerAndOrderedShutdown(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "v1.0.0", native)
	config.planner = nil
	config.catalogService = nil
	config.SourceMutation = testPlanner{}
	config.CatalogAuthorizer = testCatalogAuthorizer{}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.engine == nil || runtime.broker == nil {
		t.Fatal("production convergence runtime was not composed")
	}
	done := runRuntime(t, runtime)
	waitNativeStart(t, native, done)
	closeRuntime(t, runtime, done)
	if err := runtime.engine.Wait(t.Context()); err != nil {
		t.Fatalf("convergence engine did not settle before holder shutdown: %v", err)
	}
	if _, err := runtime.broker.OpenBroker(t.Context(), catalogservice.Identity{}, "principal"); err == nil {
		t.Fatal("broker accepted a session after holder shutdown")
	}
}

func TestHolderRequiresPlan(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.Plan = Plan{}
	if _, err := New(t.Context(), config); err == nil {
		t.Fatal("empty holder plan was accepted")
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
	oldConfig := testConfig(dir, "v1.0.0", oldNative)
	oldRuntime, err := New(t.Context(), oldConfig)
	if err != nil {
		t.Fatal(err)
	}
	oldDone := runRuntime(t, oldRuntime)
	waitNativeStart(t, oldNative, oldDone)

	newNative := newTestNative(func(event string) { record("new-" + event) })
	newConfig := testConfig(dir, "v1.1.0", newNative)
	newRecovery := &recoveryRegistry{recorder: func() { record("new-recover") }}
	newConfig.workerRegistry = newRecovery
	newRuntime, err := New(t.Context(), newConfig)
	if err != nil {
		t.Fatal(err)
	}
	if newRecovery.callCount() != 0 {
		t.Fatal("successor recovered workers before acquiring daemon ownership")
	}
	newDone := runRuntime(t, newRuntime)
	waitNativeStart(t, newNative, newDone)
	if err := waitRuntime(oldDone); err != nil {
		t.Fatalf("incumbent Run: %v", err)
	}
	closeRuntime(t, newRuntime, newDone)

	eventsMu.Lock()
	defer eventsMu.Unlock()
	oldClose := eventIndex(events, "old-close")
	newRecover := eventIndex(events, "new-recover")
	newStart := eventIndex(events, "new-start")
	if oldClose < 0 || newRecover < 0 || newStart < 0 || oldClose >= newRecover || newRecover >= newStart {
		t.Fatalf("takeover order = %v", events)
	}
}

func testConfig(dir, build string, native nativeController) Config {
	plan, err := newPlan(PlanSpec{
		Application: SignedApplication{
			AppPath: "/Applications/Example.app", BundleID: "com.example.holder",
			TeamID: "ABCDE12345", ExecutableName: "Example", SigningIdentifier: "com.example.holder",
		},
		RuntimeDirectory: dir,
	}, filepath.Dir(dir))
	if err != nil {
		panic(err)
	}
	return Config{
		Plan: plan, Build: build,
		planner: testPlanner{}, workerRegistry: testRegistry{}, native: native,
		Authorizer: testMountAuthorizer{}, protectedPeer: func(wire.Peer) error { return nil },
		catalogService:  testCatalogService,
		ShutdownTimeout: 5 * time.Second,
	}
}

func shortTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "fk-holder-")
	if err != nil {
		t.Fatal(err)
	}
	dir, err = filepath.EvalSymlinks(dir)
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

func testCatalogService(_ context.Context, store *catalog.Catalog, runtime *tenant.TenantRuntime) (catalogservice.Config, error) {
	return catalogservice.Config{
		Reader: catalogservice.CatalogReader{Catalog: store}, Mutations: testMutations{}, Sources: testSources{},
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

func (testCatalogAuthorizer) Authorize(_ context.Context, _ catalogservice.Identity, operation catalogproto.Operation, route catalogservice.Route) (catalogservice.Authorization, error) {
	if operation == catalogproto.OperationTenantPrepare {
		return catalogservice.Authorization{
			Principal: "owner", Role: catalogservice.RoleTenantOwner, Route: route,
		}, nil
	}
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

type testSources struct{}

func (testSources) StageSourceObject(context.Context, catalogservice.Identity, catalogservice.Authorization, catalogproto.SourceReconcileRequest, catalogproto.SourceTenantRecord, catalogproto.SourceObjectRecord, io.Reader) (catalog.SourceObject, error) {
	return catalog.SourceObject{}, errors.New("unexpected source staging")
}

func (testSources) DiscardSource(context.Context, catalogservice.Identity, catalogservice.Authorization, []catalog.SourceTenant) error {
	return nil
}

func (testSources) ApplySource(context.Context, catalogservice.Identity, catalogservice.Authorization, catalogservice.SourceSubmission) (catalog.SourceResult, error) {
	return catalog.SourceResult{}, errors.New("unexpected source publication")
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

func (testBroker) ProveBrokerPeer(context.Context) (catalogproto.BrokerPeerProof, error) {
	return catalogproto.BrokerPeerProof{}, errors.New("unexpected broker peer proof")
}

func (testBroker) CutoverDomains(context.Context, catalogproto.DomainCutoverPlan) (catalogproto.DomainAbsenceProof, error) {
	return catalogproto.DomainAbsenceProof{}, errors.New("unexpected domain cutover")
}

func (testBroker) ClaimDomainCutover(context.Context, catalogproto.DomainAbsenceProof) (catalogproto.DomainCutoverClaim, error) {
	return catalogproto.DomainCutoverClaim{}, errors.New("unexpected domain cutover claim")
}

func (testBroker) RecoverDomainCutoverClaim(context.Context, catalogproto.DomainAbsenceProof) (catalogproto.DomainCutoverClaim, error) {
	return catalogproto.DomainCutoverClaim{}, errors.New("unexpected domain cutover claim recovery")
}

func (testBroker) RecoverDomainCutoverReceipt(context.Context, catalogproto.DomainCutoverRecoveryKey) (catalogproto.DomainCutoverReceipt, error) {
	return catalogproto.DomainCutoverReceipt{}, errors.New("unexpected domain cutover receipt recovery")
}

type testBrokerSession struct{}

func (testBrokerSession) Commands() <-chan catalogproto.BrokerCommand {
	return make(chan catalogproto.BrokerCommand)
}
func (testBrokerSession) AcceptResult(context.Context, catalogproto.BrokerResult) error { return nil }
func (testBrokerSession) Close(error)                                                   {}

var _ supervise.WorkerRegistry = testRegistry{}
