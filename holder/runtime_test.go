package holder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/catalogworker"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/tenant"
)

const holderTestEventTimeout = 30 * time.Second

// daemonkitHomeEnv relocates every daemonkit home-derived path, so a test
// daemon's socket, lock, and owner record land under a short /tmp home rather
// than the real one.
const daemonkitHomeEnv = "DAEMONKIT_HOME"

func TestMain(m *testing.M) {
	if len(os.Args) > 1 {
		if err := configureCriticalReadWorkerTestChild(os.Args[1:]); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if recognized, err := RunChild(context.Background(), os.Args[1:], ChildConfig{Stdin: os.Stdin, Stdout: os.Stdout}); recognized {
			if err != nil && !errors.Is(err, context.Canceled) {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			os.Exit(0)
		}
	}
	os.Exit(m.Run())
}

func TestOneSessionServesMountAndCatalogAndOwnsOneRoot(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	runtime, err := New(t.Context(), testConfig(dir, "v1.0.0", native))
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	graph := publishedRuntimeGraph(runtime)
	if graph == nil || graph.pool == nil || graph.ledger == nil {
		t.Fatal("holder did not publish its process and worker owners")
	}
	if graph.runtimeOwnerRecord.PID != os.Getpid() ||
		graph.runtimeOwnerRecord.RecoveryID != recoveryid.Holder {
		t.Fatalf("published runtime owner record = %+v", graph.runtimeOwnerRecord)
	}
	if starts, _ := native.counts(); starts != 0 {
		t.Fatalf("native starts before demand = %d", starts)
	}
	if err := graph.presentations.EnsureNative(t.Context()); err != nil {
		t.Fatalf("start native presentation: %v", err)
	}

	product := holderTestProduct(t, runtime)
	definition := mountproto.TenantDefinition{
		Mount:       &mountproto.MountSpec{PresentationRoot: filepath.Join(testPresentationRoot(dir), "acct-18")},
		BackingRoot: filepath.Join(dir, "backing"), ContentSourceID: "source",
		AccessMode: mountproto.AccessModeReadWrite, CasePolicy: mountproto.CasePolicySensitive,
		Presentations: []mountproto.Presentation{mountproto.PresentationMount}, Generation: 1,
	}
	response, err := holderProvisionTenant(t.Context(), product, "acct-18", definition)
	if err != nil || response.Code != mountproto.ErrorCodeOk {
		t.Fatalf("ProvisionTenant = %#v, %v", response, err)
	}
	lifecycle, err := graph.catalog.TenantLifecycle(t.Context(), "holder-test", "acct-18")
	if err != nil || lifecycle.Target == nil || lifecycle.Target.Definition.Generation != 1 || lifecycle.Active != nil {
		t.Fatalf("provisioned catalog lifecycle = %+v, %v", lifecycle, err)
	}

	var head catalogproto.HeadResponse
	if err := holderCatalogCall(
		t.Context(), product, catalogproto.OperationCatalogHead, "acct-18",
		catalogproto.HeadRequest{Protocol: catalogproto.Version, Generation: 1}, &head,
	); err != nil {
		t.Fatal(err)
	}
	if head.Code != catalogproto.ErrorCodeNotFound || head.Revision != 0 {
		t.Fatalf("Head = %#v", head)
	}

	closeRuntime(t, runtime, done)
	if starts, closes := native.counts(); starts != 1 || closes != 1 {
		t.Fatalf("native lifecycle = %d starts, %d closes", starts, closes)
	}
}

func TestBrokerCapableRuntimeStartsEmptyAndProvisionsFirstFileProvider(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "broker-capable", native)
	configureTestBroker(&config)
	config.catalogService = nil
	config.CatalogAuthorizer = testCatalogAuthorizer{}
	if _, ok := config.Plan.Broker(); !ok {
		t.Fatal("File Provider test plan has no broker")
	}
	brokerRecord := catalog.ProcessRecord{
		RecoveryID: recoveryid.Broker,
		PID:        42_418, StartTime: "broker-start", Boot: "broker-boot",
		Generation: holderOwnerGeneration("broker-generation"), ProcessGroup: true, SessionID: 42_418,
	}
	brokerProcess := newFakeManagedProcess(brokerRecord)
	brokerRecorded := make(chan struct{})
	config.brokerStart = testBrokerProcessStart(brokerProcess, brokerRecorded)

	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	graph := publishedRuntimeGraph(runtime)
	if graph == nil || graph.topology == nil || len(graph.tenants.Specs()) != 0 {
		t.Fatalf("cold broker-capable tenant fleet = %#v, want empty", graph)
	}
	if starts, _ := native.counts(); starts != 0 {
		t.Fatalf("native starts before demand = %d", starts)
	}
	brokerReady := make(chan error, 1)
	go func() { brokerReady <- graph.presentations.EnsureBroker(context.Background()) }()
	select {
	case <-brokerRecorded:
	case err := <-done:
		t.Fatalf("runtime stopped before broker registration: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker process was not durably registered")
	}
	brokerSession, err := graph.broker.OpenBroker(
		t.Context(), catalogservice.Identity{Caller: testBrokerCaller(brokerRecord)}, "principal",
	)
	if err != nil {
		t.Fatal(err)
	}
	brokerErr := make(chan error, 1)
	brokerRegistered := make(chan struct{})
	go func() {
		var domains []catalogproto.RegisteredDomain
		for command := range brokerSession.Commands() {
			result := catalogproto.BrokerResult{
				Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
				CommandID: command.CommandID, Kind: command.Kind,
			}
			switch command.Kind {
			case catalogproto.BrokerCommandKindListDomains:
				page := append([]catalogproto.RegisteredDomain(nil), domains...)
				result.Domains = observedHolderDomainPage(page)
			case catalogproto.BrokerCommandKindRegisterDomain:
				if command.Registration == nil {
					brokerErr <- errors.New("register command has no registration")
					return
				}
				registered := catalogproto.RegisteredDomain{
					DomainID: command.Registration.DomainID, OwnerID: command.Registration.OwnerID,
					TenantID: command.Registration.TenantID, Generation: command.Registration.Generation,
					RootID: command.Registration.RootID, AccessMode: command.Registration.AccessMode,
					PresentationInstanceID: command.Registration.PresentationInstanceID,
					DisplayName:            command.Registration.DisplayName,
					PublicPath:             filepath.Join(dir, "file-provider-domain"),
				}
				domains = []catalogproto.RegisteredDomain{registered}
				result.Registered = &registered
				select {
				case <-brokerRegistered:
				default:
					close(brokerRegistered)
				}
			default:
				brokerErr <- fmt.Errorf("unexpected broker command %q", command.Kind)
				return
			}
			if err := brokerSession.AcceptResult(context.Background(), result); err != nil {
				brokerErr <- err
				return
			}
		}
	}()
	select {
	case err := <-brokerReady:
		if err != nil {
			t.Fatalf("start File Provider presentation: %v", err)
		}
	case err := <-done:
		t.Fatalf("runtime stopped before File Provider presentation readiness: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("File Provider presentation did not become ready")
	}
	graph.topology.mu.Lock()
	topologyStarted := graph.topology.cancel != nil
	graph.topology.mu.Unlock()
	if !topologyStarted {
		t.Fatal("cold broker-capable runtime did not start its topology controller")
	}

	definition := mountproto.TenantDefinition{
		Mount:           &mountproto.MountSpec{PresentationRoot: filepath.Join(testPresentationRoot(dir), "acct-18")},
		BackingRoot:     filepath.Join(dir, "backing", "acct-18"),
		ContentSourceID: "source",
		AccessMode:      mountproto.AccessModeReadWrite,
		CasePolicy:      mountproto.CasePolicySensitive,
		Presentations: []mountproto.Presentation{
			mountproto.PresentationMount,
			mountproto.PresentationFileProvider,
		},
		FileProviderPresentationInstanceID: "instance-18",
		FileProviderDisplayName:            "Account 18",
		Generation:                         1,
	}
	response, err := holderProvisionTenant(t.Context(), holderTestProduct(t, runtime), "acct-18", definition)
	if err != nil || response.Code != mountproto.ErrorCodeOk {
		t.Fatalf("first File Provider ProvisionTenant = %#v, %v", response, err)
	}
	specs := graph.tenants.Specs()
	if len(specs) != 1 || !specs[0].Traits.Presentations.Has(catalog.PresentationFileProvider) ||
		!specs[0].FileProvider.Enabled || specs[0].FileProvider.PresentationInstanceID != "instance-18" {
		t.Fatalf("provisioned tenant fleet = %#v", specs)
	}
	graph.topology.mu.Lock()
	current, terminalErr, stopped := graph.topology.current, graph.topology.err, graph.topology.stopped
	graph.topology.mu.Unlock()
	if terminalErr != nil || stopped {
		t.Fatalf("topology controller stopped after first File Provider provision: %v", terminalErr)
	}
	if len(current.Tenants) != 0 {
		t.Fatalf("unprepared tenant entered active topology: %+v", current.Tenants)
	}
	select {
	case <-brokerRegistered:
		t.Fatal("unprepared tenant registered a File Provider domain")
	default:
	}
	closeRuntime(t, runtime, done)
}

func TestRuntimeOwnerRecoveryIDFollowsImmutableSourceCapability(t *testing.T) {
	for _, test := range []struct {
		name          string
		sourceCapable bool
		want          recoveryid.ID
	}{
		{name: "mount-only holder", want: recoveryid.Holder},
		{name: "empty source-capable owner", sourceCapable: true, want: recoveryid.SourceOwner},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := shortTempDir(t)
			native := newTestNative(nil)
			config := testConfig(dir, "owner-class", native)
			if test.sourceCapable {
				configureTestSourceFleet(&config, testSourceAuthoritySpec("source"))
			}
			checked := false
			config.catalogManager = func(
				ctx context.Context,
				managerConfig catalogworker.ManagerConfig,
			) (*catalogworker.Manager, error) {
				records, loadErr := holderLedgerRecords(config.Plan.Paths().ProcessStore)
				if loadErr != nil {
					return nil, loadErr
				}
				if len(records) != 1 || records[0].RecoveryID != test.want {
					return nil, fmt.Errorf("runtime owner records = %+v, want one ID %q", records, test.want)
				}
				checked = true
				return testCatalogManager(ctx, managerConfig)
			}
			runtime, err := New(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			done := runRuntime(t, runtime)
			waitRuntimeReady(t, runtime, done)
			if !checked {
				t.Fatal("catalog opened before immutable runtime owner registration")
			}
			closeRuntime(t, runtime, done)
		})
	}
}

func TestHolderServesExactTransportBeforeNativeStartup(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	var runtime *Runtime
	native.onStart = func(ctx context.Context) error {
		if err := runtime.WaitReady(ctx); err != nil {
			return err
		}
		if publishedRuntimeGraph(runtime) == nil {
			return errors.New("native startup began before the holder published its graph")
		}
		conn, err := net.Dial("unix", runtime.socket)
		if err != nil {
			return fmt.Errorf("dial serving transport during native startup: %w", err)
		}
		return conn.Close()
	}
	runtime, err := New(t.Context(), testConfig(dir, "v1.0.0", native))
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	if err := publishedRuntimeGraph(runtime).presentations.EnsureNative(t.Context()); err != nil {
		t.Fatalf("native demand: %v", err)
	}
	closeRuntime(t, runtime, done)
}

func TestHolderRemainsReadyWhileNativePresentationStartsOnDemand(t *testing.T) {
	dir := shortTempDir(t)
	var readinessLog bytes.Buffer
	native := newTestNative(nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	native.onStart = func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	config := testConfig(dir, "v1.0.0", native)
	config.RuntimeStderr = &readinessLog
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	graph := publishedRuntimeGraph(runtime)
	if graph == nil {
		t.Fatal("runtime graph was not published")
	}
	generation := graph.runtimeOwnerRecord.Generation.String()
	health := observedHolderRuntimeHealth(t, runtime)
	if health.State != mountproto.RuntimeStateHealthy || !health.Ready ||
		health.RuntimeBuild != "v1.0.0" || health.RuntimeProtocol != mountproto.RuntimeProtocolVersion ||
		health.ProcessGeneration != generation || health.ActivationGeneration != generation ||
		health.ReadinessPhase != mountproto.ReadinessPhaseReady ||
		health.ReadinessStep != mountproto.ReadinessStepPublished ||
		health.NativePhase != mountproto.NativePhaseIdle || health.NativeMount != nil ||
		health.BrokerPhase != mountproto.BrokerPhaseDisabled {
		t.Fatalf("post-bootstrap health = %#v, want healthy and ready", health)
	}
	product := holderTestProduct(t, runtime)
	definition := mountproto.TenantDefinition{
		Mount:       &mountproto.MountSpec{PresentationRoot: filepath.Join(testPresentationRoot(dir), "acct-18")},
		BackingRoot: filepath.Join(dir, "backing"), ContentSourceID: "source",
		AccessMode: mountproto.AccessModeReadWrite, CasePolicy: mountproto.CasePolicySensitive,
		Presentations: []mountproto.Presentation{mountproto.PresentationMount}, Generation: 1,
	}
	type provisionResult struct {
		response mountproto.ProvisionTenantResponse
		err      error
	}
	provisioned := make(chan provisionResult, 1)
	go func() {
		response, provisionErr := holderProvisionTenant(context.Background(), product, "acct-18", definition)
		provisioned <- provisionResult{response: response, err: provisionErr}
	}()
	waitRuntimeEvent(t, entered, done, "native presentation start")
	starting := observedHolderRuntimeHealth(t, runtime)
	if starting.State != mountproto.RuntimeStateHealthy || !starting.Ready ||
		starting.NativePhase != mountproto.NativePhaseStarting ||
		starting.ProcessGeneration != generation {
		t.Fatalf("starting runtime health = %#v", starting)
	}
	close(release)
	select {
	case result := <-provisioned:
		if result.err != nil || result.response.Code != mountproto.ErrorCodeOk {
			t.Fatalf("ProvisionTenant after native presentation readiness = %#v, %v", result.response, result.err)
		}
	case err := <-done:
		t.Fatalf("runtime stopped before tenant provision: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("tenant provision did not complete after native presentation readiness")
	}
	published := observedHolderRuntimeHealth(t, runtime)
	if published.RuntimeBuild != "v1.0.0" || published.RuntimeProtocol != mountproto.RuntimeProtocolVersion ||
		published.ProcessGeneration != generation || published.ActivationGeneration != generation ||
		published.State != mountproto.RuntimeStateHealthy || !published.Ready ||
		published.ReadinessPhase != mountproto.ReadinessPhaseReady ||
		published.ReadinessStep != mountproto.ReadinessStepPublished ||
		published.NativePhase != mountproto.NativePhaseLive || published.NativeMount == nil ||
		published.BrokerPhase != mountproto.BrokerPhaseDisabled {
		t.Fatalf("published runtime health = %#v", published)
	}
	closeRuntime(t, runtime, done)
	wantReadinessLog := []string{
		"step=receipts result=settling",
		"step=receipts result=settled",
		"step=published result=ready",
	}
	logOutput := readinessLog.String()
	last := -1
	for _, event := range wantReadinessLog {
		index := strings.Index(logOutput, event)
		if index <= last {
			t.Fatalf("runtime readiness log event %q out of order:\n%s", event, logOutput)
		}
		last = index
	}
	if !strings.Contains(logOutput, fmt.Sprintf(`runtime_build="v1.0.0" activation_generation=%q`, generation)) {
		t.Fatalf("runtime readiness log lacks exact identities:\n%s", logOutput)
	}
}

func TestHolderRejectsWorkerLimitConsumedEntirelyByNativeChild(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.WorkerLimit = 1
	if _, err := New(t.Context(), config); err == nil {
		t.Fatal("worker limit one was accepted")
	}
}

func TestHolderRejectsBuildThatDiffersFromRuntimePlan(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.RuntimeBuild = "transport-schema-build"
	if _, err := New(t.Context(), config); err == nil || !strings.Contains(err.Error(), "does not match runtime plan build") {
		t.Fatalf("New with mismatched build = %v", err)
	}
}

func TestHolderReservesObserverAndDisposableWorkerCapacity(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.planner = nil
	configureTestSourceFleet(
		&config,
		testSourceAuthoritySpec("alpha"),
		testSourceAuthoritySpec("beta"),
	)
	config.WorkerLimit = 3
	if _, err := New(t.Context(), config); err == nil {
		t.Fatal("worker limit consumed by native and observer children was accepted")
	}
}

func TestHolderRejectsOversizedSourceFleetBeforeStartingObservers(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.planner = nil
	configureTestSourceFleet(
		&config,
		testSourceAuthoritySpec("alpha"),
		testSourceAuthoritySpec("beta"),
	)
	config.WorkerLimit = fixedWorkerReservations(config) + sourceObserverReservations
	started := 0
	config.authorityFactory = func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		started++
		return newTestAuthority(), nil
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	err = waitRuntime(runRuntime(t, runtime))
	if err == nil || !strings.Contains(err.Error(), "cannot run 2 source observers") {
		t.Fatalf("Run = %v, want source observer capacity failure", err)
	}
	if started != 0 {
		t.Fatalf("undersized source fleet started %d observers", started)
	}
	if publishedRuntimeGraph(runtime) != nil {
		t.Fatal("undersized source fleet published a partial runtime graph")
	}
}

func TestProductionRuntimeOwnsConvergenceBrokerAndOrderedShutdown(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "v1.0.0", native)
	config.planner = nil
	config.catalogService = nil
	configureTestSourceFleet(&config, testSourceAuthoritySpec("source"))
	configureTestBroker(&config)
	if _, ok := config.Plan.Broker(); !ok {
		t.Fatal("File Provider test plan has no broker")
	}
	brokerRecord := catalog.ProcessRecord{
		RecoveryID: recoveryid.Broker,
		PID:        42_424, StartTime: "broker-start", Boot: "broker-boot",
		Generation: holderOwnerGeneration("broker-generation"), ProcessGroup: true, SessionID: 42_424,
	}
	brokerProcess := newFakeManagedProcess(brokerRecord)
	brokerRecorded := make(chan struct{})
	config.brokerStart = testBrokerProcessStart(brokerProcess, brokerRecorded)
	config.authorityFactory = func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		return newTestAuthority(), nil
	}
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	config.CatalogAuthorizer = testCatalogAuthorizer{}
	seed, err := catalog.Open(t.Context(), config.Plan.Paths().Catalog)
	if err != nil {
		t.Fatal(err)
	}
	tenantID, err := catalog.NewTenantID("file-provider")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := seed.ProvisionTenant(t.Context(), catalog.TenantProvision{
		OwnerID: string(config.Owner), Tenant: tenantID,
		Mount:       catalog.MountPresentation{PresentationRoot: filepath.Join(testPresentationRoot(dir), string(tenantID))},
		BackingRoot: filepath.Join(dir, "backing"), ContentSourceID: "source",
		Access: catalog.TenantReadWrite, CasePolicy: catalog.CaseSensitive,
		Presentations: catalog.PresentMount | catalog.PresentFileProvider,
		FileProvider: catalog.FileProviderPresentation{
			PresentationInstanceID: "file-provider-instance", DisplayName: "File Provider",
		},
		Generation: 1,
	}); err != nil {
		_ = seed.Close()
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	graph := publishedRuntimeGraph(runtime)
	if graph == nil || graph.engine == nil || graph.broker == nil {
		t.Fatal("production convergence runtime was not composed")
	}
	if err := graph.presentations.EnsureNative(t.Context()); err != nil {
		t.Fatalf("native demand: %v", err)
	}
	brokerReady := make(chan error, 1)
	go func() { brokerReady <- graph.presentations.EnsureBroker(context.Background()) }()
	select {
	case <-brokerRecorded:
	case err := <-done:
		t.Fatalf("runtime stopped before broker registration: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker process was not durably registered")
	}
	session, err := graph.broker.OpenBroker(
		t.Context(), catalogservice.Identity{Caller: testBrokerCaller(brokerRecord)}, "principal",
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case command := <-session.Commands():
		domains := []catalogproto.RegisteredDomain{}
		if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
			Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
			CommandID: command.CommandID, Kind: command.Kind, Domains: observedHolderDomainPage(domains),
		}); err != nil {
			t.Fatal(err)
		}
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker emitted no initial domain reconciliation")
	}
	var registration catalogproto.BrokerCommand
	select {
	case registration = <-session.Commands():
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker emitted no domain registration")
	}
	if registration.Kind != catalogproto.BrokerCommandKindRegisterDomain || registration.Registration == nil {
		t.Fatalf("domain registration = %+v", registration)
	}
	registered := catalogproto.RegisteredDomain{
		DomainID: registration.Registration.DomainID, OwnerID: registration.Registration.OwnerID,
		TenantID: registration.Registration.TenantID, Generation: registration.Registration.Generation,
		RootID: registration.Registration.RootID, AccessMode: registration.Registration.AccessMode,
		PresentationInstanceID: registration.Registration.PresentationInstanceID,
		DisplayName:            registration.Registration.DisplayName,
		PublicPath:             filepath.Join(dir, "file-provider-domain"),
	}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: registration.CommandID, Kind: registration.Kind, Registered: &registered,
	}); err != nil {
		t.Fatal(err)
	}
	var confirmation catalogproto.BrokerCommand
	select {
	case confirmation = <-session.Commands():
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker emitted no post-registration confirmation")
	}
	if confirmation.Kind != catalogproto.BrokerCommandKindListDomains {
		t.Fatalf("post-registration confirmation = %+v", confirmation)
	}
	domains := []catalogproto.RegisteredDomain{registered}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: confirmation.CommandID, Kind: confirmation.Kind, Domains: observedHolderDomainPage(domains),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-brokerReady:
		if err != nil {
			t.Fatalf("start File Provider presentation: %v", err)
		}
	case err := <-done:
		t.Fatalf("runtime stopped before File Provider presentation readiness: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("File Provider presentation did not become ready")
	}
	waitRuntimeReady(t, runtime, done)
	brokerHealth := observedHolderRuntimeHealth(t, runtime)
	generation := graph.runtimeOwnerRecord.Generation.String()
	if brokerHealth.ReadinessPhase != mountproto.ReadinessPhaseReady ||
		brokerHealth.ReadinessStep != mountproto.ReadinessStepPublished ||
		brokerHealth.BrokerPhase != mountproto.BrokerPhaseLive ||
		brokerHealth.RuntimeProtocol != mountproto.RuntimeProtocolVersion ||
		brokerHealth.ProcessGeneration != generation || brokerHealth.ActivationGeneration != generation ||
		brokerHealth.State != mountproto.RuntimeStateHealthy || !brokerHealth.Ready {
		t.Fatalf("broker RuntimeHealth after reconciliation = %#v", brokerHealth)
	}
	closeRuntime(t, runtime, done)
	if _, err := graph.broker.OpenBroker(t.Context(), catalogservice.Identity{}, "principal"); err == nil {
		t.Fatal("broker accepted a session after holder shutdown")
	}
}

func TestFileProviderOnlyRuntimeUsesBrokerReadinessWithoutNativeMount(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "v1.9.0", native)
	config.planner = nil
	config.catalogService = nil
	configureTestBroker(&config)
	configureTestFileProviderOnly(&config)
	config.CatalogAuthorizer = testCatalogAuthorizer{}

	if _, ok := config.Plan.Broker(); !ok {
		t.Fatal("File Provider-only plan has no broker")
	}
	brokerRecord := catalog.ProcessRecord{
		RecoveryID: recoveryid.Broker,
		PID:        42_425, StartTime: "broker-start", Boot: "broker-boot",
		Generation: holderOwnerGeneration("broker-generation"), ProcessGroup: true, SessionID: 42_425,
	}
	brokerProcess := newFakeManagedProcess(brokerRecord)
	brokerRecorded := make(chan struct{})
	config.brokerStart = testBrokerProcessStart(brokerProcess, brokerRecorded)

	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(testPresentationRoot(dir)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("File Provider-only native root exists before run: %v", err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	graph := publishedRuntimeGraph(runtime)
	if graph == nil || graph.broker == nil || graph.mount != nil || graph.native != nil {
		t.Fatalf("File Provider-only runtime graph = %#v", graph)
	}
	brokerReady := make(chan error, 1)
	go func() { brokerReady <- graph.presentations.EnsureBroker(context.Background()) }()
	select {
	case <-brokerRecorded:
	case err := <-done:
		t.Fatalf("runtime stopped before broker registration: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker process was not durably registered")
	}
	session, err := graph.broker.OpenBroker(
		t.Context(), catalogservice.Identity{Caller: testBrokerCaller(brokerRecord)}, "principal",
	)
	if err != nil {
		t.Fatal(err)
	}
	var command catalogproto.BrokerCommand
	select {
	case command = <-session.Commands():
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker emitted no initial domain reconciliation")
	}
	empty := []catalogproto.RegisteredDomain{}
	if err := session.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: command.CommandID, Kind: command.Kind, Domains: observedHolderDomainPage(empty),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-brokerReady:
		if err != nil {
			t.Fatalf("start File Provider presentation: %v", err)
		}
	case err := <-done:
		t.Fatalf("runtime stopped before File Provider presentation readiness: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("File Provider presentation did not become ready")
	}
	health := observedHolderRuntimeHealth(t, runtime)
	if !health.Ready || health.NativePhase != mountproto.NativePhaseDisabled || health.NativeMount != nil ||
		health.BrokerPhase != mountproto.BrokerPhaseLive || health.ReadinessPhase != mountproto.ReadinessPhaseReady ||
		health.ReadinessStep != mountproto.ReadinessStepPublished {
		t.Fatalf("File Provider-only RuntimeHealth = %#v", health)
	}
	definition := mountproto.TenantDefinition{
		BackingRoot: filepath.Join(dir, "backing"), ContentSourceID: "source",
		AccessMode: mountproto.AccessModeReadWrite, CasePolicy: mountproto.CasePolicySensitive,
		Presentations:                      []mountproto.Presentation{mountproto.PresentationFileProvider},
		FileProviderPresentationInstanceID: "presentation", FileProviderDisplayName: "Presentation", Generation: 1,
	}
	response, err := holderProvisionTenant(
		t.Context(), holderTestProduct(t, runtime), "file-provider-only", definition,
	)
	if err != nil || response.Code != mountproto.ErrorCodeOk {
		t.Fatalf("File Provider-only lifecycle provision = %#v, %v", response, err)
	}
	if starts, _ := native.counts(); starts != 0 {
		t.Fatalf("File Provider-only runtime started native %d times", starts)
	}
	session.Close(nil)
	closeRuntime(t, runtime, done)
}

func TestHolderShutdownDeadlineBoundsCallerAndRetainsExactResourceSettlement(t *testing.T) {
	dir := shortTempDir(t)
	nativeFailure := errors.New("native terminal failure")
	native := newTestNative(nil)
	native.closeEntered = make(chan struct{})
	native.closeRelease = make(chan struct{})
	native.closeErr = nativeFailure
	authority := newTestAuthority()
	config := testConfig(dir, "v1.0.0", native)
	configureTestSourceFleet(&config, testSourceAuthoritySpec("source"))
	config.authorityFactory = func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		return authority, nil
	}
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	graph := publishedRuntimeGraph(runtime)
	if graph == nil {
		t.Fatal("runtime graph was not published")
	}
	if err := graph.presentations.EnsureNative(t.Context()); err != nil {
		t.Fatalf("native demand: %v", err)
	}
	closed := make(chan error, 1)
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelClose()
	go func() { closed <- runtime.Close(closeCtx) }()
	<-native.closeEntered
	closeErr := <-closed
	if !errors.Is(closeErr, context.DeadlineExceeded) || errors.Is(closeErr, nativeFailure) {
		t.Fatalf("Close error = %v, want deadline before native terminal failure", closeErr)
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned before exact resource settlement: %v", err)
	default:
	}
	close(native.closeRelease)
	if err := waitRuntime(done); !errors.Is(err, nativeFailure) {
		t.Fatalf("Run error = %v, want native terminal failure", err)
	}
	<-authority.done
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("settled Close = %v, want nil", err)
	}
	_, closes := native.counts()
	if closes != 1 {
		t.Fatalf("native physical closes = %d, want 1", closes)
	}
}

func TestHolderWaitReadyUsesExactComposedBarrier(t *testing.T) {
	native := newTestNative(nil)
	startEntered := make(chan struct{})
	startRelease := make(chan struct{})
	native.onStart = func(ctx context.Context) error {
		close(startEntered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-startRelease:
			return nil
		}
	}
	runtime, err := New(t.Context(), testConfig(shortTempDir(t), "v1.0.0", native))
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	if starts, _ := native.counts(); starts != 0 {
		t.Fatalf("native starts before demand = %d", starts)
	}
	graph := publishedRuntimeGraph(runtime)
	if graph == nil {
		t.Fatal("runtime graph was not published")
	}
	nativeReady := make(chan error, 1)
	go func() { nativeReady <- graph.presentations.EnsureNative(context.Background()) }()
	waitRuntimeEvent(t, startEntered, done, "native startup")
	readyCtx, cancelReady := context.WithTimeout(t.Context(), time.Second)
	defer cancelReady()
	if err := runtime.WaitReady(readyCtx); err != nil {
		t.Fatalf("WaitReady while native presentation starts = %v", err)
	}
	close(startRelease)
	if err := <-nativeReady; err != nil {
		t.Fatalf("native presentation readiness = %v", err)
	}
	closeRuntime(t, runtime, done)
}

func TestHolderWaitReadyRefusesAfterActivationFailure(t *testing.T) {
	activationErr := errors.New("catalog worker manager failed")
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.catalogManager = func(context.Context, catalogworker.ManagerConfig) (*catalogworker.Manager, error) {
		return nil, activationErr
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	if err := waitRuntime(done); !errors.Is(err, activationErr) {
		t.Fatalf("Run = %v, want activation failure", err)
	}
	readyErr := runtime.WaitReady(t.Context())
	if readyErr == nil || readyErr.Error() != "FuseKit runtime: daemon returned before readiness" {
		t.Fatalf("WaitReady = %v, want the unready daemon refusal", readyErr)
	}
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatalf("Wait after settlement = %v, want nil", err)
	}
}

func TestHolderWaitReadyHonorsCancellation(t *testing.T) {
	runtime, err := New(t.Context(), testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runtime.WaitReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReady = %v, want context.Canceled", err)
	}
}

func TestHolderConcurrentCloseAndWaitShareTerminalBarrier(t *testing.T) {
	terminalErr := errors.New("native close failed")
	native := newTestNative(nil)
	native.closeEntered = make(chan struct{})
	native.closeRelease = make(chan struct{})
	native.closeErr = terminalErr
	runtime, err := New(t.Context(), testConfig(shortTempDir(t), "v1.0.0", native))
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	graph := publishedRuntimeGraph(runtime)
	if graph == nil {
		t.Fatal("runtime graph was not published")
	}
	if err := graph.presentations.EnsureNative(t.Context()); err != nil {
		t.Fatalf("native demand: %v", err)
	}
	closed := make(chan error, 1)
	waited := make(chan error, 1)
	go func() { closed <- runtime.Close(context.Background()) }()
	go func() { waited <- runtime.Wait(context.Background()) }()
	<-native.closeEntered
	select {
	case err := <-closed:
		t.Fatalf("Close returned before exact settlement: %v", err)
	case err := <-waited:
		t.Fatalf("Wait returned before exact settlement: %v", err)
	default:
	}
	close(native.closeRelease)
	for operation, result := range map[string]<-chan error{
		"Close": closed,
		"Wait":  waited,
	} {
		if err := <-result; err != nil {
			t.Fatalf("%s = %v, want nil after settlement", operation, err)
		}
	}
	if err := <-done; !errors.Is(err, terminalErr) {
		t.Fatalf("Run = %v, want terminal failure", err)
	}
	if err := runtime.Wait(context.Background()); err != nil {
		t.Fatalf("replayed Wait = %v, want nil", err)
	}
}

func TestHolderRequiresPlan(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.Plan = RuntimePlan{}
	if _, err := New(t.Context(), config); err == nil {
		t.Fatal("empty holder plan was accepted")
	}
}

func TestHolderOpensCatalogOnlyAfterDaemonOwnership(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "v1.0.0", native)
	var opens atomic.Int64
	config.catalogManager = func(ctx context.Context, managerConfig catalogworker.ManagerConfig) (*catalogworker.Manager, error) {
		opens.Add(1)
		return testCatalogManager(ctx, managerConfig)
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if opens.Load() != 0 || publishedRuntimeGraph(runtime) != nil {
		t.Fatalf("New activated graph with %d catalog opens", opens.Load())
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	if opens.Load() != 1 || publishedRuntimeGraph(runtime) == nil {
		t.Fatalf("owned activation graph = %v after %d catalog opens", publishedRuntimeGraph(runtime), opens.Load())
	}
	closeRuntime(t, runtime, done)
}

func TestHolderRetainsCatalogWorkerLifetimeAfterActivation(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "v1.0.0", native)
	var catalogLifetime context.Context
	config.catalogManager = func(ctx context.Context, managerConfig catalogworker.ManagerConfig) (*catalogworker.Manager, error) {
		catalogLifetime = ctx
		return testCatalogManager(ctx, managerConfig)
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)
	if catalogLifetime == nil {
		t.Fatal("catalog worker manager did not receive a lifecycle context")
	}
	if err := catalogLifetime.Err(); err != nil {
		t.Fatalf("catalog worker lifecycle ended after activation: %v", err)
	}
	graph := publishedRuntimeGraph(runtime)
	if graph == nil {
		t.Fatal("holder did not publish its active graph")
	}
	if _, err := graph.catalog.TopologyHead(t.Context(), config.Owner); err != nil {
		t.Fatalf("catalog worker unavailable after activation: %v", err)
	}
	closeRuntime(t, runtime, done)
}

func TestHolderActivationFailureCleansPrivateGraphBeforeReturning(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "v1.0.0", native)
	configureTestSourceFleet(&config, testSourceAuthoritySpec("source"))
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	config.authorityFactory = func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		return nil, errors.New("injected authority startup failure")
	}
	var opened *catalogworker.Manager
	config.catalogManager = func(ctx context.Context, managerConfig catalogworker.ManagerConfig) (*catalogworker.Manager, error) {
		manager, err := testCatalogManager(ctx, managerConfig)
		opened = manager
		return manager, err
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	err = waitRuntime(runRuntime(t, runtime))
	if err == nil || !strings.Contains(err.Error(), "injected authority startup failure") {
		t.Fatalf("Run = %v, want activation failure", err)
	}
	if publishedRuntimeGraph(runtime) != nil {
		t.Fatal("failed activation published a partial graph")
	}
	if opened == nil {
		t.Fatal("activation did not reach catalog open")
	}
	if _, err := opened.TopologyHead(t.Context(), config.Owner); err == nil {
		t.Fatal("failed activation left its private catalog open")
	}
	if starts, _ := native.counts(); starts != 0 {
		t.Fatalf("failed activation started native root %d times", starts)
	}
}

func TestHolderActivationFailureJoinsExactAuthoritySettlement(t *testing.T) {
	activationFailure := errors.New("catalog service activation failed")
	authorityFailure := errors.New("authority terminal failure")
	authority := newTestAuthority()
	authority.waitEntered = make(chan struct{})
	authority.waitRelease = make(chan struct{})
	authority.waitErr = authorityFailure
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	configureTestSourceFleet(&config, testSourceAuthoritySpec("source"))
	config.authorityExecutors = func(SourceAuthoritySpec) (sourceauthority.Executor, error) {
		return testAuthorityExecutor{}, nil
	}
	config.authorityFactory = func(context.Context, sourceauthority.Config) (managedAuthority, error) {
		return authority, nil
	}
	config.catalogService = func(
		context.Context,
		*catalogworker.Manager,
		*tenant.TenantRuntime,
	) (catalogservice.CoreConfig, error) {
		return catalogservice.CoreConfig{}, activationFailure
	}
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	<-authority.waitEntered
	select {
	case err := <-done:
		t.Fatalf("Run returned before activation authority settled: %v", err)
	default:
	}
	close(authority.waitRelease)
	if err := <-done; !errors.Is(err, activationFailure) || !errors.Is(err, authorityFailure) {
		t.Fatalf("Run error = %v, want activation and authority terminal failures", err)
	}
	authority.mu.Lock()
	waitCalls := authority.waitCalls
	authority.mu.Unlock()
	if waitCalls != 1 {
		t.Fatalf("authority Wait calls = %d, want 1", waitCalls)
	}
}

type testRecoveryStep struct {
	name   string
	events *[]string
	err    error
}

func (s testRecoveryStep) Recover(context.Context) error {
	*s.events = append(*s.events, s.name)
	return s.err
}

func TestBrokerRecoveryRequiresCompletedProcessRecovery(t *testing.T) {
	events := []string{}
	broker := testRecoveryStep{name: "broker", events: &events}
	if err := recoverBrokerAfterProcesses(t.Context(), processRecoveryProof{complete: true}, broker); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0] != "broker" {
		t.Fatalf("settled recovery events = %v, want one broker recovery", events)
	}

	events = nil
	if err := recoverBrokerAfterProcesses(t.Context(), processRecoveryProof{}, broker); err == nil {
		t.Fatal("broker recovery accepted missing process settlement proof")
	}
	if len(events) != 0 {
		t.Fatalf("broker recovery ran without proof: %v", events)
	}
}

func TestWorkerLimitReservesSignedBrokerOnlyWhenConfigured(t *testing.T) {
	config := testConfig(shortTempDir(t), "build", newTestNative(nil))
	config.WorkerLimit = fixedWorkerReservations(config)
	if err := validateConfig(config); err != nil {
		t.Fatalf("mount-only minimum worker limit: %v", err)
	}
	configureTestBroker(&config)
	if err := validateConfig(config); err == nil {
		t.Fatal("File Provider plan without signed broker capacity was accepted")
	}
	config.WorkerLimit += brokerProcessReservations
	if err := validateConfig(config); err != nil {
		t.Fatalf("minimum worker limit with signed broker capacity: %v", err)
	}
	configureTestFileProviderOnly(&config)
	config.WorkerLimit = fixedWorkerReservations(config)
	if err := validateConfig(config); err != nil {
		t.Fatalf("File Provider-only minimum worker limit: %v", err)
	}
}

func testConfig(dir, build string, native nativeController) Config {
	home := filepath.Dir(dir)
	application := testSignedApplication(testHelperAppPath(home), "com.example.holder", "ProductHelper")
	application.Broker = SignedExecutable{}
	materializeTestBundle(application)
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application:      application,
		RuntimeDirectory: dir,
		Native:           testNativeRuntimeSpec(testPresentationRoot(dir)),
		BuildID:          build,
		Readiness:        StandardReadinessContract(),
		RuntimePolicy:    EntitlementPolicy{},
	}, home)
	if err != nil {
		panic(err)
	}
	return Config{
		Plan: plan, RuntimeBuild: build, Owner: "holder-test",
		Trust:   RuntimeTrust{Controller: testProcessRequirement("controller")},
		planner: testPlanner{}, native: native,
		fleetTransitions:        testFleetTransitions{},
		Authorizer:              testMountAuthorizer{},
		protectedPeer:           func(context.Context, daemonkit.Caller) error { return nil },
		catalogService:          testCatalogService,
		catalogManager:          testCatalogManager,
		CatalogReadinessTimeout: 30 * time.Second,
		CatalogOperationTimeout: 30 * time.Second,
		ShutdownTimeout:         5 * time.Second,
	}
}

// materializeTestBundle plants the regular executable file daemonkit.InBundle
// stats when New resolves the holder daemon's program. Every holder test shares
// one bundle identity, so the path and its contents are identical per run.
func materializeTestBundle(application SignedApplication) {
	executable := filepath.Join(
		application.AppPath, "Contents", "MacOS", application.Runtime.ExecutableName,
	)
	if err := os.MkdirAll(filepath.Dir(executable), 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		panic(err)
	}
	if err := os.Chmod(executable, 0o755); err != nil {
		panic(err)
	}
}

func configureTestSourceFleet(config *Config, specs ...SourceAuthoritySpec) {
	configureTestSourceRuntime(config, specs...)
	declarations := make([]catalog.SourceAuthorityDeclaration, len(specs))
	for index, spec := range specs {
		authority, digest := sourceAuthorityIdentity(spec)
		declarations[index] = catalog.SourceAuthorityDeclaration{
			Authority: authority, DriverID: sourceAuthorityDriverID(spec),
			DriverConfig:      append([]byte(nil), sourceAuthorityDriverConfig(spec)...),
			DeclarationDigest: digest,
		}
	}
	store, err := catalog.Open(context.Background(), config.Plan.Paths().Catalog)
	if err != nil {
		panic(err)
	}
	if _, err := store.PublishDesiredSourceFleet(context.Background(), catalog.PublishDesiredSourceFleetRequest{
		Owner: config.Owner, Generation: 1, Declarations: declarations,
	}); err != nil {
		_ = store.Close()
		panic(err)
	}
	if err := store.Close(); err != nil {
		panic(err)
	}
}

func configureTestSourceRuntime(config *Config, specs ...SourceAuthoritySpec) {
	if config == nil {
		panic("nil holder test config")
	}
	if _, ok := config.Plan.Broker(); ok {
		panic("source fleet test helper requires a brokerless plan")
	}
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application: config.Plan.Application(), RuntimeDirectory: config.Plan.Paths().Directory,
		Native:        testNativeRuntimeSpec(config.Plan.Paths().PresentationRoot),
		BuildID:       config.Plan.BuildID(),
		Readiness:     config.Plan.Readiness(),
		SourceCapable: true, RuntimePolicy: EntitlementPolicy{},
	}, config.Plan.deployment.home)
	if err != nil {
		panic(err)
	}
	config.Plan = plan
	entries := make(map[string]DriverFactory, len(specs))
	for _, spec := range specs {
		source, ok := spec.(PhysicalSourceSpec)
		if !ok {
			panic("holder test source fleet helper requires physical sources")
		}
		policy := source.Policy
		entries[source.DriverID] = DriverFactory{
			Physical: func(context.Context, sourceauthority.SourceTaskIdentity) (sourceauthority.AuthorityPolicy, error) {
				return policy, nil
			},
		}
	}
	drivers, err := NewDriverFactories(entries)
	if err != nil {
		panic(err)
	}
	config.Drivers = drivers
}

func configureTestBroker(config *Config) {
	if config == nil {
		panic("nil holder test config")
	}
	application := config.Plan.Application()
	application.Broker = application.Runtime
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application: application, RuntimeDirectory: config.Plan.Paths().Directory,
		Native: func() *NativeRuntimeSpec {
			if native, ok := config.Plan.NativePresentation(); ok {
				return testNativeRuntimeSpec(native.PresentationRoot)
			}
			return nil
		}(),
		BuildID:       config.Plan.BuildID(),
		Readiness:     config.Plan.Readiness(),
		SourceCapable: config.Plan.SourceCapable(),
		BrokerPolicy:  EntitlementPolicy{}, RuntimePolicy: EntitlementPolicy{},
	}, config.Plan.deployment.home)
	if err != nil {
		panic(err)
	}
	config.Plan = plan
	config.Trust.FileProviderExtension = testProcessRequirement("file-provider-extension")
}

func configureTestFileProviderOnly(config *Config) {
	if config == nil {
		panic("nil holder test config")
	}
	if _, ok := config.Plan.Broker(); !ok {
		panic("File Provider-only test helper requires a broker")
	}
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application: config.Plan.Application(), RuntimeDirectory: config.Plan.Paths().Directory,
		BuildID: config.Plan.BuildID(), Readiness: config.Plan.Readiness(),
		SourceCapable: config.Plan.SourceCapable(),
		BrokerPolicy:  EntitlementPolicy{}, RuntimePolicy: EntitlementPolicy{},
	}, config.Plan.deployment.home)
	if err != nil {
		panic(err)
	}
	config.Plan = plan
	config.native = nil
}

func testCatalogManager(
	ctx context.Context, managerConfig catalogworker.ManagerConfig,
) (*catalogworker.Manager, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		return nil, err
	}
	managerConfig.Executable = executable
	managerConfig.Exec = daemonkit.ServingSameUser()
	return catalogworker.NewManager(ctx, managerConfig)
}

type testFleetTransitions struct{}

func (testFleetTransitions) Prepare(context.Context, tenant.FleetTransition) error { return nil }
func (testFleetTransitions) Commit(context.Context, tenant.FleetTransition) error  { return nil }
func (testFleetTransitions) Abort(context.Context, tenant.FleetTransition) error   { return nil }

// shortTempDir returns one private /tmp runtime directory per test and points
// the daemonkit home at it, so each served holder daemon owns its own socket,
// state dir, and owner-record lock. t.Setenv is load-bearing beyond the
// restore: the daemonkit home is process-global, so a caller that also ran
// t.Parallel would let one test's daemon write into another's directory while
// that test removes it. t.Setenv's panic is the enforcement.
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
	t.Setenv(daemonkitHomeEnv, dir)
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove temp dir: %v", err)
		}
		if err := os.RemoveAll(testPresentationRoot(dir)); err != nil {
			t.Errorf("remove presentation root: %v", err)
		}
	})
	return dir
}

func testPresentationRoot(runtimeDirectory string) string {
	return filepath.Join(
		filepath.Dir(runtimeDirectory), filepath.Base(runtimeDirectory)+"-presentation",
	)
}

func testCatalogService(_ context.Context, store *catalogworker.Manager, runtime *tenant.TenantRuntime) (catalogservice.CoreConfig, error) {
	return catalogservice.CoreConfig{
		Reader: catalogservice.CatalogReader{Store: store}, Mutations: testMutations{},
		Preparation: testPreparation{runtime: runtime}, Leases: store, SourceFleets: testSourceFleetService{},
		Authorizer: testCatalogAuthorizer{},
	}, nil
}

type testNative struct {
	mu           sync.Mutex
	starts       int
	closes       int
	live         bool
	root         string
	started      chan struct{}
	recorder     func(string)
	onStart      func(context.Context) error
	closeEntered chan struct{}
	closeRelease chan struct{}
	closeErr     error
	closeOnce    sync.Once
	healthState  mountproto.RuntimeState
}

func newTestNative(recorder func(string)) *testNative {
	return &testNative{started: make(chan struct{}), recorder: recorder}
}

func (n *testNative) Start(ctx context.Context, root string, _ mountmux.Resolver) error {
	n.mu.Lock()
	onStart := n.onStart
	n.starts++
	n.root = root
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
	n.live = true
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
	n.closes++
	n.live = false
	if n.recorder != nil {
		n.recorder("close")
	}
	entered, release, err := n.closeEntered, n.closeRelease, n.closeErr
	n.mu.Unlock()
	if entered != nil {
		n.closeOnce.Do(func() { close(entered) })
	}
	if release != nil {
		<-release
	}
	return err
}

func (n *testNative) counts() (int, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.starts, n.closes
}

func (*testNative) Bind(context.Context, mountservice.Identity) error { return nil }
func (*testNative) Mounted(context.Context, mountservice.Identity, mountservice.NativeMountIdentity, string) error {
	return nil
}

func (*testNative) Ready(context.Context, mountservice.Identity, mountservice.NativeMountProof) error {
	return nil
}
func (*testNative) Unbind(mountservice.Identity)         {}
func (*testNative) Settled(mountservice.Identity, error) {}
func (*testNative) VerifyCaller(daemonkit.Caller) error  { return nil }

func (n *testNative) HealthState() mountproto.RuntimeState {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.healthState == "" {
		return mountproto.RuntimeStateHealthy
	}
	return n.healthState
}

func (n *testNative) RuntimeHealth(generation string) mountservice.RuntimeHealth {
	n.mu.Lock()
	defer n.mu.Unlock()
	health := mountservice.RuntimeHealth{ActivationGeneration: generation, NativePhase: mountproto.NativePhaseIdle}
	if n.starts != 0 {
		health.NativePhase = mountproto.NativePhaseStarting
	}
	if n.live {
		health.NativePhase = mountproto.NativePhaseLive
		proof := testNativeMountProof(n.root)
		health.NativeMount = &proof
	}
	if n.closes != 0 {
		health.NativePhase = mountproto.NativePhaseClosed
		health.NativeMount = nil
	}
	return health
}

func runRuntime(t *testing.T, runtime *Runtime) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	settled := make(chan struct{})
	go func() {
		done <- runtime.Run(context.Background())
		close(settled)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtime.Close(ctx)
		select {
		case <-settled:
		case <-ctx.Done():
			t.Errorf("cleanup Run settlement: %v", ctx.Err())
		}
	})
	return done
}

func publishedRuntimeGraph(runtime *Runtime) *runtimeGraph {
	runtime.graphMu.Lock()
	defer runtime.graphMu.Unlock()
	return runtime.graph
}

// holderTestProduct builds the same daemonkit product the served daemon
// dispatches to. The holder's business trust is a disjunction of signed code
// requirements, so an unsigned test binary can never be admitted as a wire
// peer; in-process dispatch keeps mux routing, op registration, resolver
// wiring, authorizer wrapping, and handler behavior under test.
func holderTestProduct(t *testing.T, runtime *Runtime) *runtimeProduct {
	t.Helper()
	graph := publishedRuntimeGraph(runtime)
	if graph == nil {
		t.Fatal("holder published no runtime graph")
	}
	product, err := runtime.newProduct(graph)
	if err != nil {
		t.Fatal(err)
	}
	return product
}

func holderTestCaller() daemonkit.Caller {
	return daemonkit.Caller{UID: uint32(os.Getuid()), PID: os.Getpid()}
}

func testBrokerCaller(record catalog.ProcessRecord) daemonkit.Caller {
	return daemonkit.Caller{UID: uint32(os.Getuid()), PID: record.PID}
}

func holderMountCall(
	ctx context.Context,
	product *runtimeProduct,
	operation mountproto.Operation,
	request, response any,
) error {
	payload, err := mountproto.Encode(request)
	if err != nil {
		return err
	}
	reply, err := product.Handle(ctx, daemonkit.Request{
		Op: string(operation), Body: payload, Caller: holderTestCaller(),
	})
	if err != nil {
		return err
	}
	if len(reply.Body) == 0 {
		return fmt.Errorf("holder %q reply has no payload", operation)
	}
	return mountproto.Decode(reply.Body, response)
}

func holderProvisionTenant(
	ctx context.Context,
	product *runtimeProduct,
	id catalog.TenantID,
	definition mountproto.TenantDefinition,
) (mountproto.ProvisionTenantResponse, error) {
	var response mountproto.ProvisionTenantResponse
	err := holderMountCall(ctx, product, mountproto.OperationTenantProvision, mountproto.ProvisionTenantRequest{
		Protocol: mountproto.Version, Tenant: mountproto.TenantID(id), Definition: definition,
	}, &response)
	return response, err
}

func holderCatalogCall(
	ctx context.Context,
	product *runtimeProduct,
	operation catalogproto.Operation,
	tenantID catalogproto.TenantID,
	request, response any,
) error {
	payload, err := catalogproto.Encode(request)
	if err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		Tenant  string          `json:"tenant,omitempty"`
		Payload json.RawMessage `json:"payload"`
	}{Tenant: string(tenantID), Payload: payload})
	if err != nil {
		return err
	}
	reply, err := product.Handle(ctx, daemonkit.Request{
		Op: string(operation), Body: body, Caller: holderTestCaller(),
	})
	if err != nil {
		return err
	}
	if len(reply.Body) == 0 {
		return fmt.Errorf("holder %q reply has no payload", operation)
	}
	return catalogproto.Decode(reply.Body, response)
}

// observedHolderRuntimeHealth renders the health detail the holder reports
// through daemonkit.Ctx.Report, built by the production reporter off the
// published graph.
func observedHolderRuntimeHealth(t *testing.T, runtime *Runtime) mountservice.RuntimeHealth {
	t.Helper()
	graph := publishedRuntimeGraph(runtime)
	if graph == nil {
		t.Fatal("holder published no runtime graph")
	}
	var health mountservice.RuntimeHealth
	reported := false
	healthReporter(func(detail []byte) {
		if err := json.Unmarshal(detail, &health); err != nil {
			t.Errorf("decode reported runtime health: %v", err)
			return
		}
		reported = true
	}, runtime.config, graph)()
	if !reported {
		t.Fatal("holder reported no runtime health")
	}
	return health
}

func holderLedgerRecords(path string) ([]catalog.ProcessRecord, error) {
	ledger, err := openProcessLedger(path)
	if err != nil {
		return nil, err
	}
	return ledger.state.Records, nil
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
	case <-time.After(holderTestEventTimeout):
		return errors.New("runtime did not stop")
	}
}

func waitRuntimeReady(t *testing.T, runtime *Runtime, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), StandardReadinessContract().StartupTimeout())
	defer cancel()
	ready := make(chan error, 1)
	go func() { ready <- runtime.WaitReady(ctx) }()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatalf("WaitReady: %v", err)
		}
	case err := <-done:
		t.Fatalf("runtime stopped before composed readiness: %v", err)
	case <-ctx.Done():
		t.Fatalf("composed runtime did not become ready: %v", ctx.Err())
	}
	if publishedRuntimeGraph(runtime) == nil {
		t.Fatal("ready runtime published no graph")
	}
}

func waitRuntimeEvent(t *testing.T, event <-chan struct{}, done <-chan error, name string) {
	t.Helper()
	select {
	case <-event:
	case err := <-done:
		t.Fatalf("runtime stopped before %s: %v", name, err)
	case <-time.After(holderTestEventTimeout):
		t.Fatalf("runtime did not reach %s", name)
	}
}

func testBrokerProcessStart(process *fakeManagedProcess, prepared chan<- struct{}) brokerProcessStart {
	return func(config managedSpawnConfig, _ io.Writer) (managedProcess, error) {
		if process == nil || config.id != recoveryid.Broker {
			return nil, errors.New("invalid broker process preparation")
		}
		process.start = func(context.Context) error {
			close(prepared)
			return nil
		}
		return process, nil
	}
}

type testPlanner struct{}

func (testPlanner) PrepareSourceMutation(context.Context, tenant.SourceMutationStep) (tenant.SourceMutationOperation, error) {
	return tenant.SourceMutationOperation{}, errors.New("unexpected source mutation")
}

func (testPlanner) ApplySourceMutation(
	context.Context,
	tenant.SourceMutationStep,
	tenant.SourceMutationOperation,
	tenant.SourceMutationContent,
) error {
	return errors.New("unexpected source mutation completion")
}

func (testPlanner) SourceMutationCommitted(context.Context, tenant.SourceMutationCommit) error {
	return nil
}

type testMountAuthorizer struct{}

func (testMountAuthorizer) Authorize(_ context.Context, _ mountservice.Identity, _ mountproto.Operation, _ catalog.TenantID, _ catalog.Generation) (tenant.OwnerID, error) {
	return "holder-test", nil
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
	if operation == catalogproto.OperationSourceAuthorityPublishDesiredFleet ||
		operation == catalogproto.OperationSourceAuthorityReadDesiredFleet {
		return catalogservice.Authorization{
			Principal: "holder-test", Role: catalogservice.RoleProductAdmin, Route: route,
		}, nil
	}
	return catalogservice.Authorization{
		Principal: "owner", Role: catalogservice.RoleMount, Presentation: catalog.PresentationMount, Route: route,
	}, nil
}

type testMutations struct{}

func (testMutations) LookupPrivate(
	context.Context,
	catalogservice.Identity,
	catalogservice.Authorization,
	catalog.TenantID,
	catalog.ObjectID,
) (catalog.PrivateMutationResult, error) {
	return catalog.PrivateMutationResult{}, catalog.ErrNotFound
}

func (testMutations) OpenPrivate(
	context.Context,
	catalogservice.Identity,
	catalogservice.Authorization,
	catalog.TenantID,
	catalog.Generation,
	catalog.ObjectID,
	catalog.MutationID,
) (catalogservice.PrivateOpenResult, error) {
	return catalogservice.PrivateOpenResult{}, catalog.ErrNotFound
}

func (testMutations) StageMutation(
	ctx context.Context,
	_ catalogservice.Identity,
	_ catalogservice.Authorization,
	_ catalog.TenantID,
	_ catalogproto.MutationRequestID,
	_ catalog.Generation,
	_ bool,
	source contentstream.Source,
) (catalogservice.MutationStage, error) {
	err := errors.New("unexpected mutation")
	settleErr := source.Settle(err)
	waitErr := source.Wait(ctx)
	err = errors.Join(err, settleErr, waitErr)
	return catalogservice.MutationStage{}, err
}

func (testMutations) SubmitMutation(context.Context, catalogservice.Identity, catalogservice.Authorization, catalogservice.MutationSubmission) (catalogservice.MutationResult, error) {
	return catalogservice.MutationResult{}, errors.New("unexpected mutation")
}

type testPreparation struct{ runtime *tenant.TenantRuntime }

func (p testPreparation) PrepareTenant(context.Context, catalogservice.Identity, catalog.TenantID, catalogproto.PrepareTenantRequest) (catalogproto.TenantPreparationProof, error) {
	return catalogproto.TenantPreparationProof{}, errors.New("unexpected preparation")
}

func observedHolderDomainPage(
	domains []catalogproto.RegisteredDomain,
) *[]catalogproto.ObservedDomain {
	observed := make([]catalogproto.ObservedDomain, len(domains))
	for index := range domains {
		managed := domains[index]
		observedID, err := catalogproto.EncodeObservedDomainID(string(managed.DomainID))
		if err != nil {
			panic(err)
		}
		observed[index] = catalogproto.ObservedDomain{
			ObservedID: observedID,
			Managed:    &managed,
		}
	}
	return &observed
}

type testSourceFleetService struct{}

func (testSourceFleetService) PublishDesiredSourceFleet(
	context.Context,
	catalog.PublishDesiredSourceFleetRequest,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	return catalog.DesiredSourceAuthorityFleetState{}, errors.New("unexpected source fleet publication")
}

func (testSourceFleetService) DesiredSourceFleetPage(
	context.Context,
	catalog.DesiredSourceFleetPageRequest,
) (catalog.DesiredSourceFleetPage, error) {
	return catalog.DesiredSourceFleetPage{}, errors.New("unexpected source fleet read")
}
