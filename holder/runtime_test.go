package holder

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
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
	"github.com/yasyf/fusekit/transportproto"
	"github.com/yasyf/fusekit/trustroles"
)

const holderTestEventTimeout = 30 * time.Second

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
	if graph == nil || graph.pool == nil || graph.children == nil {
		t.Fatal("holder did not publish its process and worker owners")
	}
	if starts, _ := native.counts(); starts != 0 {
		t.Fatalf("native starts before demand = %d", starts)
	}
	if err := graph.presentations.EnsureNative(t.Context()); err != nil {
		t.Fatalf("start native presentation: %v", err)
	}

	mountClient, err := mountservice.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(filepath.Join(dir, "fusekit.sock")), WireBuild: transportproto.WireBuild,
		Role: trust.UnprotectedRole,
	})
	if err != nil {
		t.Fatal(err)
	}
	definition := mountproto.TenantDefinition{
		Mount:       &mountproto.MountSpec{PresentationRoot: filepath.Join(testPresentationRoot(dir), "acct-18")},
		BackingRoot: filepath.Join(dir, "backing"), ContentSourceID: "source",
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
		Dial: wire.UnixDialer(filepath.Join(dir, "fusekit.sock")), WireBuild: transportproto.WireBuild,
		Role: trust.UnprotectedRole,
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

func TestBrokerCapableRuntimeStartsEmptyAndProvisionsFirstFileProvider(t *testing.T) {
	dir := shortTempDir(t)
	native := newTestNative(nil)
	config := testConfig(dir, "broker-capable", native)
	configureTestBroker(&config)
	config.catalogService = nil
	config.CatalogAuthorizer = testCatalogAuthorizer{}
	broker, ok := config.Plan.Broker()
	if !ok {
		t.Fatal("File Provider test plan has no broker")
	}
	brokerRecord := proc.Record{
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
	brokerSession, err := graph.broker.OpenBroker(t.Context(), catalogservice.Identity{Peer: wire.Peer{
		PID: brokerRecord.PID, StartTime: brokerRecord.StartTime, Boot: brokerRecord.Boot,
		Executable: broker.Deployment.Executable,
	}}, "principal")
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

	client, err := mountservice.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(filepath.Join(dir, "fusekit.sock")), WireBuild: transportproto.WireBuild,
		Role: trust.UnprotectedRole,
	})
	if err != nil {
		t.Fatal(err)
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
	if response, err := client.ProvisionTenant(t.Context(), "acct-18", definition); err != nil || response.Code != mountproto.ErrorCodeOk {
		t.Fatalf("first File Provider ProvisionTenant = %#v, %v", response, err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	specs := graph.tenants.Specs()
	if len(specs) != 1 || !specs[0].Traits.Presentations.Has(catalog.PresentationFileProvider) ||
		!specs[0].FileProvider.Enabled || specs[0].FileProvider.PresentationInstanceID != "instance-18" {
		t.Fatalf("provisioned tenant fleet = %#v", specs)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		graph.topology.mu.Lock()
		current, wake := graph.topology.current, graph.topology.wake
		terminalErr, stopped := graph.topology.err, graph.topology.stopped
		graph.topology.mu.Unlock()
		if len(current.Tenants) == 1 && current.Tenants[0].Tenant == "acct-18" &&
			current.Tenants[0].Presentations.Has(catalog.PresentationFileProvider) {
			break
		}
		if terminalErr != nil || stopped {
			t.Fatalf("topology controller stopped before first File Provider tenant: %v", terminalErr)
		}
		select {
		case <-wake:
		case err := <-brokerErr:
			t.Fatalf("broker reconciliation: %v", err)
		case <-deadline.C:
			t.Fatal("topology controller did not observe first File Provider tenant")
		}
	}
	closeRuntime(t, runtime, done)
}

func TestRuntimeOwnerRecoveryIDFollowsImmutableSourceCapability(t *testing.T) {
	for _, test := range []struct {
		name          string
		sourceCapable bool
		want          proc.RecoveryID
	}{
		{name: "mount-only holder", want: recoveryid.Holder},
		{name: "empty source-capable owner", sourceCapable: true, want: recoveryid.SourceOwner},
	} {
		t.Run(test.name, func(t *testing.T) {
			identity, err := proc.CurrentIdentity()
			if err != nil {
				t.Skipf("authenticated current process identity unavailable: %v", err)
			}
			dir := shortTempDir(t)
			native := newTestNative(nil)
			config := testConfig(dir, "owner-class", native)
			if test.sourceCapable {
				configureTestSourceFleet(&config, testSourceAuthoritySpec("source"))
			}
			config.currentIdentity = func() (proc.Identity, error) { return identity, nil }
			checked := false
			config.catalogManager = func(
				ctx context.Context,
				managerConfig catalogworker.ManagerConfig,
			) (*catalogworker.Manager, error) {
				records, loadErr := (&proc.FileStore{Path: config.Plan.Paths().ProcessStore}).Load(ctx)
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
	native.onStart = func(ctx context.Context) error {
		client, err := wire.NewClient(ctx, wire.ClientConfig{
			Dial: wire.UnixDialer(filepath.Join(dir, "fusekit.sock")), WireBuild: transportproto.WireBuild,
			Role: trust.UnprotectedRole,
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
	health, err := runtime.Health(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if health.State != daemon.StateHealthy || health.Busy || !health.Ready ||
		health.ProcessGeneration == (proc.OwnerGeneration{}) || health.PID <= 0 {
		t.Fatalf("post-bootstrap health = %#v, want healthy and ready", health)
	}
	client := openMountClientEventually(t, filepath.Join(dir, "fusekit.sock"))
	idle, err := client.RuntimeHealth(t.Context())
	if err != nil {
		t.Fatalf("idle RuntimeHealth: %v", err)
	}
	graph := publishedRuntimeGraph(runtime)
	if graph == nil {
		t.Fatal("runtime graph was not published")
	}
	if idle.RuntimeBuild != "v1.0.0" || idle.RuntimeProtocol != mountproto.RuntimeProtocolVersion ||
		idle.RuntimePID != int64(health.PID) || idle.ProcessGeneration != health.ProcessGeneration.String() ||
		idle.ActivationGeneration != "health-test-activation" ||
		idle.State != mountproto.RuntimeStateHealthy || idle.Draining || idle.Busy || !idle.Ready ||
		idle.ReadinessPhase != mountproto.ReadinessPhaseReady || idle.ReadinessStep != mountproto.ReadinessStepPublished ||
		idle.NativePhase != mountproto.NativePhaseIdle || idle.NativeMount != nil ||
		idle.BrokerPhase != mountproto.BrokerPhaseDisabled {
		t.Fatalf("idle RuntimeHealth = %#v", idle)
	}
	definition := mountproto.TenantDefinition{
		Mount:       &mountproto.MountSpec{PresentationRoot: filepath.Join(testPresentationRoot(dir), "acct-18")},
		BackingRoot: filepath.Join(dir, "backing"), ContentSourceID: "source",
		AccessMode: mountproto.AccessModeReadWrite, CasePolicy: mountproto.CasePolicySensitive,
		Presentations: []mountproto.Presentation{mountproto.PresentationMount}, Generation: 1,
	}
	if response, err := client.ProvisionTenant(t.Context(), "acct-18", definition); err != nil || response.Code != mountproto.ErrorCodeOk {
		t.Fatalf("pre-presentation ProvisionTenant = %#v, %v", response, err)
	}
	nativeReady := make(chan error, 1)
	go func() { nativeReady <- graph.presentations.EnsureNative(context.Background()) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("runtime stopped before native presentation start: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("native presentation did not begin")
	}
	starting, err := client.RuntimeHealth(t.Context())
	if err != nil {
		t.Fatalf("starting RuntimeHealth: %v", err)
	}
	if starting.RuntimeBuild != "v1.0.0" || starting.RuntimeProtocol != mountproto.RuntimeProtocolVersion ||
		starting.RuntimePID != int64(health.PID) || starting.ProcessGeneration != health.ProcessGeneration.String() ||
		starting.ActivationGeneration != "health-test-activation" ||
		starting.State != mountproto.RuntimeStateHealthy || starting.Draining || starting.Busy || !starting.Ready ||
		starting.ReadinessPhase != mountproto.ReadinessPhaseReady || starting.ReadinessStep != mountproto.ReadinessStepPublished ||
		starting.NativePhase != mountproto.NativePhaseStarting || starting.NativeMount != nil ||
		starting.BrokerPhase != mountproto.BrokerPhaseDisabled {
		t.Fatalf("starting RuntimeHealth = %#v", starting)
	}
	close(release)
	select {
	case err := <-nativeReady:
		if err != nil {
			t.Fatalf("native presentation readiness: %v", err)
		}
	case err := <-done:
		t.Fatalf("runtime stopped before native presentation readiness: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("native presentation did not become ready")
	}
	published, err := runtime.Health(t.Context())
	if err != nil {
		t.Fatalf("published daemon health: %v", err)
	}
	if !published.Ready || published.ProcessGeneration != health.ProcessGeneration {
		t.Fatalf("published daemon health = %#v", published)
	}
	readyHealth, err := client.RuntimeHealth(t.Context())
	if err != nil {
		t.Fatalf("ready RuntimeHealth: %v", err)
	}
	if readyHealth.RuntimeBuild != "v1.0.0" || readyHealth.RuntimeProtocol != mountproto.RuntimeProtocolVersion ||
		readyHealth.RuntimePID != starting.RuntimePID || readyHealth.ProcessGeneration != starting.ProcessGeneration ||
		readyHealth.ActivationGeneration != starting.ActivationGeneration ||
		readyHealth.State != mountproto.RuntimeStateHealthy || readyHealth.Draining || readyHealth.Busy || !readyHealth.Ready ||
		readyHealth.ReadinessPhase != mountproto.ReadinessPhaseReady || readyHealth.ReadinessStep != mountproto.ReadinessStepPublished ||
		readyHealth.NativePhase != mountproto.NativePhaseLive || readyHealth.NativeMount == nil ||
		readyHealth.BrokerPhase != mountproto.BrokerPhaseDisabled {
		t.Fatalf("ready RuntimeHealth = %#v", readyHealth)
	}
	if err := runtime.daemon.Drain(); err != nil {
		t.Fatal(err)
	}
	drainingHealth, err := client.RuntimeHealth(t.Context())
	if err != nil {
		t.Fatalf("draining RuntimeHealth: %v", err)
	}
	if !drainingHealth.Draining || drainingHealth.State != mountproto.RuntimeStateDraining ||
		drainingHealth.Busy || drainingHealth.Ready ||
		drainingHealth.ReadinessPhase != mountproto.ReadinessPhaseDraining ||
		drainingHealth.ReadinessStep != mountproto.ReadinessStepPublished {
		t.Fatalf("draining RuntimeHealth = %#v", drainingHealth)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	closeRuntime(t, runtime, done)
	wantReadinessLog := []string{
		"step=listener result=starting",
		"step=listener result=live",
		"step=receipts result=settling",
		"step=receipts result=settled",
		"step=published result=publishing",
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
	if !strings.Contains(logOutput, `runtime_build="v1.0.0" activation_generation="`) {
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

func TestHolderRequiresConsumerStopControlStore(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.StopControlStore = nil
	if _, err := New(t.Context(), config); err == nil || !strings.Contains(err.Error(), "stop-control store is required") {
		t.Fatalf("New without stop-control store = %v", err)
	}
}

func TestHolderReservesObserverAndDisposableWorkerCapacity(t *testing.T) {
	config := testConfig(shortTempDir(t), "v1.0.0", newTestNative(nil))
	config.planner = nil
	configureTestSourceFleet(&config,
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
	configureTestSourceFleet(&config,
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
	broker, ok := config.Plan.Broker()
	if !ok {
		t.Fatal("File Provider test plan has no broker")
	}
	brokerRecord := proc.Record{
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
	if err := publishedRuntimeGraph(runtime).presentations.EnsureNative(t.Context()); err != nil {
		t.Fatalf("native demand: %v", err)
	}
	select {
	case <-brokerRecorded:
	case err := <-done:
		t.Fatalf("runtime stopped before broker registration: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker process was not durably registered")
	}
	graph := publishedRuntimeGraph(runtime)
	if graph == nil || graph.engine == nil || graph.broker == nil {
		t.Fatal("production convergence runtime was not composed")
	}
	session, err := graph.broker.OpenBroker(t.Context(), catalogservice.Identity{Peer: wire.Peer{
		PID: brokerRecord.PID, StartTime: brokerRecord.StartTime, Boot: brokerRecord.Boot,
		Executable: broker.Deployment.Executable,
	}}, "principal")
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
	waitRuntimeReady(t, runtime, done)
	client := openMountClientEventually(t, config.Plan.Paths().Socket)
	brokerHealth, err := client.RuntimeHealth(t.Context())
	if err != nil {
		t.Fatalf("broker RuntimeHealth after reconciliation: %v", err)
	}
	daemonHealth, err := runtime.Health(t.Context())
	if err != nil {
		t.Fatalf("broker daemon health: %v", err)
	}
	if brokerHealth.ReadinessPhase != mountproto.ReadinessPhaseReady ||
		brokerHealth.ReadinessStep != mountproto.ReadinessStepPublished ||
		brokerHealth.BrokerPhase != mountproto.BrokerPhaseLive ||
		brokerHealth.RuntimeProtocol != mountproto.RuntimeProtocolVersion || brokerHealth.RuntimePID <= 0 ||
		brokerHealth.ProcessGeneration != daemonHealth.ProcessGeneration.String() || brokerHealth.ActivationGeneration != graph.runtimeOwnerRecord.Generation.String() ||
		brokerHealth.State != mountproto.RuntimeStateHealthy || brokerHealth.Draining || brokerHealth.Busy || !brokerHealth.Ready {
		t.Fatalf("broker RuntimeHealth after reconciliation = %#v", brokerHealth)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
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

	broker, ok := config.Plan.Broker()
	if !ok {
		t.Fatal("File Provider-only plan has no broker")
	}
	brokerRecord := proc.Record{
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
	select {
	case <-brokerRecorded:
	case err := <-done:
		t.Fatalf("runtime stopped before broker registration: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker process was not durably registered")
	}
	graph := publishedRuntimeGraph(runtime)
	if graph == nil || graph.broker == nil || graph.mount != nil || graph.native != nil {
		t.Fatalf("File Provider-only runtime graph = %#v", graph)
	}
	session, err := graph.broker.OpenBroker(t.Context(), catalogservice.Identity{Peer: wire.Peer{
		PID: brokerRecord.PID, StartTime: brokerRecord.StartTime, Boot: brokerRecord.Boot,
		Executable: broker.Deployment.Executable,
	}}, "principal")
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
	waitRuntimeReady(t, runtime, done)
	client := openMountClientEventually(t, config.Plan.Paths().Socket)
	health, err := client.RuntimeHealth(t.Context())
	if err != nil {
		t.Fatal(err)
	}
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
	if _, err := client.ProvisionTenant(t.Context(), "file-provider-only", definition); err != nil {
		t.Fatalf("File Provider-only lifecycle provision: %v", err)
	}
	if starts, _ := native.counts(); starts != 0 {
		t.Fatalf("File Provider-only runtime started native %d times", starts)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
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
	config.ShutdownTimeout = 10 * time.Millisecond
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
	closed := make(chan error, 1)
	go func() { closed <- runtime.Close(context.Background()) }()
	<-native.closeEntered
	<-authority.done
	closeErr := <-closed
	if !errors.Is(closeErr, context.DeadlineExceeded) || errors.Is(closeErr, nativeFailure) {
		t.Fatalf("Close error = %v, want deadline before native terminal failure", closeErr)
	}
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nativeFailure) {
		t.Fatalf("Run error = %v, want deadline before native terminal failure", err)
	}
	close(native.closeRelease)
	if err := runtime.Close(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("replayed Close error = %v, want daemon terminal deadline", err)
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
	waitRuntimeEvent(t, startEntered, done, "native startup")
	ready := make(chan error, 1)
	go func() { ready <- runtime.WaitReady(context.Background()) }()
	select {
	case err := <-ready:
		t.Fatalf("WaitReady returned before native startup settled: %v", err)
	default:
	}
	close(startRelease)
	if err := <-ready; err != nil {
		t.Fatalf("WaitReady = %v", err)
	}
	closeRuntime(t, runtime, done)
}

func TestHolderWaitReadyReplaysActivationFailure(t *testing.T) {
	activationErr := errors.New("native activation failed")
	native := newTestNative(nil)
	startEntered := make(chan struct{})
	native.onStart = func(context.Context) error {
		close(startEntered)
		return activationErr
	}
	runtime, err := New(t.Context(), testConfig(shortTempDir(t), "v1.0.0", native))
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeEvent(t, startEntered, done, "native startup")
	if err := runtime.WaitReady(t.Context()); !errors.Is(err, daemon.ErrRuntimeNotReady) ||
		!errors.Is(err, activationErr) {
		t.Fatalf("WaitReady = %v, want readiness and activation failures", err)
	}
	if err := <-done; !errors.Is(err, activationErr) {
		t.Fatalf("Run = %v, want activation failure", err)
	}
	if err := runtime.Wait(context.Background()); !errors.Is(err, activationErr) {
		t.Fatalf("Wait replay = %v, want activation failure", err)
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
	if err := runtime.WaitReady(t.Context()); err != nil {
		t.Fatal(err)
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
		"Run":   done,
	} {
		if err := <-result; !errors.Is(err, terminalErr) {
			t.Fatalf("%s = %v, want terminal failure", operation, err)
		}
	}
	if err := runtime.Wait(context.Background()); !errors.Is(err, terminalErr) {
		t.Fatalf("replayed Wait = %v, want terminal failure", err)
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

func TestStopControlKeepsCapacityWithNativeBrokerAndOrdinarySaturated(t *testing.T) {
	dir := shortTempDir(t)
	socket := filepath.Join(dir, "fusekit.sock")
	oldNative := newTestNative(nil)
	oldConfig := testConfig(dir, "v1.0.0", oldNative)
	configureTestSourceFleet(&oldConfig)
	configureTestBroker(&oldConfig)
	oldConfig.catalogService = nil
	oldConfig.CatalogAuthorizer = testCatalogAuthorizer{}
	broker, ok := oldConfig.Plan.Broker()
	if !ok {
		t.Fatal("source-capable capacity plan has no broker")
	}
	brokerRecord := proc.Record{
		RecoveryID: recoveryid.Broker,
		PID:        42_419, StartTime: "broker-start", Boot: "broker-boot",
		Generation: holderOwnerGeneration("broker-generation"), ProcessGroup: true, SessionID: 42_419,
	}
	brokerProcess := newFakeManagedProcess(brokerRecord)
	brokerRecorded := make(chan struct{})
	oldConfig.brokerStart = testBrokerProcessStart(brokerProcess, brokerRecorded)
	oldConfig.wireMaxSessions = 4
	var verifierCalls atomic.Int64
	oldConfig.protectedPeer = func(_ context.Context, peer wire.Peer) error {
		verifierCalls.Add(1)
		if peer.PID != os.Getpid() {
			return fmt.Errorf("%w: ordinary test peer", trust.ErrUntrustedPeer)
		}
		return nil
	}
	oldRuntime, err := New(t.Context(), oldConfig)
	if err != nil {
		t.Fatal(err)
	}
	oldDone := runRuntime(t, oldRuntime)
	waitRuntimeReady(t, oldRuntime, oldDone)
	graph := publishedRuntimeGraph(oldRuntime)
	if graph == nil {
		t.Fatal("old runtime graph was not published")
	}
	presentationsReady := make(chan error, 1)
	go func() { presentationsReady <- graph.presentations.Ensure(context.Background(), true, true) }()
	waitNativeStart(t, oldNative, oldDone)
	select {
	case <-brokerRecorded:
	case err := <-oldDone:
		t.Fatalf("old runtime stopped before broker registration: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("old runtime did not register broker")
	}
	brokerSession, err := graph.broker.OpenBroker(t.Context(), catalogservice.Identity{Peer: wire.Peer{
		PID: brokerRecord.PID, StartTime: brokerRecord.StartTime, Boot: brokerRecord.Boot,
		Executable: broker.Deployment.Executable,
	}}, "principal")
	if err != nil {
		t.Fatal(err)
	}
	var command catalogproto.BrokerCommand
	select {
	case command = <-brokerSession.Commands():
	case err := <-oldDone:
		t.Fatalf("old runtime stopped before broker reconciliation: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("broker did not request initial reconciliation")
	}
	emptyDomains := []catalogproto.RegisteredDomain{}
	if err := brokerSession.AcceptResult(t.Context(), catalogproto.BrokerResult{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
		CommandID: command.CommandID, Kind: command.Kind, Domains: observedHolderDomainPage(emptyDomains),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-presentationsReady:
		if err != nil {
			t.Fatalf("start protected presentations: %v", err)
		}
	case err := <-oldDone:
		t.Fatalf("old runtime stopped before protected presentation readiness: %v", err)
	case <-time.After(holderTestEventTimeout):
		t.Fatal("protected presentations did not become ready")
	}
	ordinary := startHolderIdleSessionHelper(t, socket)
	defer ordinary.close(t)
	if calls := verifierCalls.Load(); calls != 0 {
		t.Fatalf("ordinary executable mismatch spawned %d verifier tasks", calls)
	}

	nativeClient, err := wire.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: transportproto.WireBuild, Role: trustroles.NativeChild,
	})
	if err != nil {
		t.Fatalf("open protected native session: %v", err)
	}
	defer func() { _ = nativeClient.Close() }()

	brokerClient, err := wire.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: transportproto.WireBuild, Role: trustroles.Broker,
	})
	if err != nil {
		t.Fatalf("open protected broker session: %v", err)
	}
	defer func() { _ = brokerClient.Close() }()
	expectHolderOrdinarySessionRejected(t, socket)

	stopClient, err := wire.NewClient(t.Context(), wire.ClientConfig{
		Dial: wire.UnixDialer(socket), WireBuild: transportproto.WireBuild, Role: trustroles.StopController,
	})
	if err != nil {
		t.Fatalf("open reserved stop-control session: %v", err)
	}
	result, err := stopClient.Call(
		t.Context(), wire.Op("daemon.control.stop"), "", []byte(`{"version":1}`),
	)
	if err != nil {
		t.Fatalf("stop control call: %v", err)
	}
	if result.Outcome != wire.Delivered || result.Response.Err != "" {
		t.Fatalf("stop control result = %#v", result)
	}
	_ = stopClient.Close()
	if err := waitRuntime(oldDone); err != nil {
		t.Fatalf("stopped bootstrap runtime: %v", err)
	}

	newNative := newTestNative(nil)
	newConfig := testConfig(dir, "v1.1.0", newNative)
	configureTestSourceRuntime(&newConfig)
	configureTestBroker(&newConfig)
	newConfig.catalogService = nil
	newConfig.CatalogAuthorizer = testCatalogAuthorizer{}
	newRuntime, err := New(t.Context(), newConfig)
	if err != nil {
		t.Fatal(err)
	}
	newDone := runRuntime(t, newRuntime)
	waitRuntimeReady(t, newRuntime, newDone)
	closeRuntime(t, newRuntime, newDone)
}

type holderIdleSessionHelper struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
}

func startHolderIdleSessionHelper(t *testing.T, socket string) *holderIdleSessionHelper {
	t.Helper()
	helper, status := startHolderSessionHelper(t, socket, "ordinary-client")
	if status != "ready" {
		t.Fatalf("idle helper = %q, want ready", status)
	}
	return helper
}

func expectHolderOrdinarySessionRejected(t *testing.T, socket string) {
	t.Helper()
	helper, status := startHolderSessionHelper(t, socket, "ordinary-client-rejected")
	if helper != nil {
		helper.close(t)
	}
	if status != "rejected" {
		t.Fatalf("saturated ordinary helper = %q, want rejected", status)
	}
}

func startHolderSessionHelper(t *testing.T, socket, name string) (*holderIdleSessionHelper, string) {
	t.Helper()
	executable := filepath.Join(filepath.Dir(socket), name)
	copyExecutable(t, executable)
	cmd := exec.Command(executable, "-test.run=^TestHolderIdleOrdinarySessionHelper$")
	cmd.Env = append(os.Environ(), "FUSEKIT_IDLE_SESSION_HELPER=1", "FUSEKIT_IDLE_SESSION_SOCKET="+socket)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatalf("read idle helper status: %v", err)
	}
	status := strings.TrimSpace(line)
	if status != "ready" {
		_ = stdin.Close()
		if err := cmd.Wait(); err != nil {
			t.Fatalf("session helper: %v", err)
		}
		return nil, status
	}
	return &holderIdleSessionHelper{cmd: cmd, stdin: stdin}, status
}

func copyExecutable(t *testing.T, destination string) {
	t.Helper()
	source, err := os.Open(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	target, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(target, source); err != nil {
		_ = target.Close()
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
}

func (h *holderIdleSessionHelper) close(t *testing.T) {
	t.Helper()
	if h == nil || h.cmd == nil {
		return
	}
	_ = h.stdin.Close()
	if err := h.cmd.Wait(); err != nil {
		t.Fatalf("idle helper: %v", err)
	}
	h.cmd = nil
}

func TestHolderIdleOrdinarySessionHelper(_ *testing.T) {
	if os.Getenv("FUSEKIT_IDLE_SESSION_HELPER") != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Dial: wire.UnixDialer(os.Getenv("FUSEKIT_IDLE_SESSION_SOCKET")), WireBuild: transportproto.WireBuild,
		Role: trust.UnprotectedRole,
	})
	if err != nil {
		_, _ = os.Stdout.WriteString("rejected\n")
		return
	}
	defer func() { _ = client.Close() }()
	mountClient, err := mountservice.NewClientOn(client)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "mount client: %v\n", err)
		return
	}
	if _, err := mountClient.BindNative(ctx); err == nil {
		_, _ = os.Stdout.WriteString("native accepted\n")
		return
	}
	catalogClient, err := catalogservice.NewClientOn(client)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stdout, "catalog client: %v\n", err)
		return
	}
	_, err = catalogClient.Head(ctx, "acct-18", 1)
	var remote *catalogservice.RemoteError
	if !errors.Is(err, wire.ErrNotReady) &&
		(!errors.As(err, &remote) || remote.Code != catalogproto.ErrorCodeNotFound) {
		_, _ = fmt.Fprintf(os.Stdout, "catalog bootstrap result: %v\n", err)
		return
	}
	_, _ = os.Stdout.WriteString("ready\n")
	_, _ = io.Copy(io.Discard, os.Stdin)
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

type testProcessRecoveryStep struct{ testRecoveryStep }

func (s testProcessRecoveryStep) Recover(context.Context) error {
	*s.events = append(*s.events, s.name)
	return s.err
}

func TestBrokerRecoveryRequiresCompletedProcessRecovery(t *testing.T) {
	events := []string{}
	processes := testProcessRecoveryStep{testRecoveryStep{name: "processes", events: &events}}
	broker := testRecoveryStep{name: "broker", events: &events}
	proof, err := recoverProcessGeneration(t.Context(), processes)
	if err != nil {
		t.Fatal(err)
	}
	if err := recoverBrokerAfterProcesses(t.Context(), proof, broker); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"processes", "broker"}) {
		t.Fatalf("recovery order = %v", events)
	}

	events = nil
	if err := recoverBrokerAfterProcesses(t.Context(), processRecoveryProof{}, broker); err == nil {
		t.Fatal("broker recovery accepted missing process settlement proof")
	}
	if len(events) != 0 {
		t.Fatalf("broker recovery ran without proof: %v", events)
	}

	processFailure := errors.New("process recovery failed")
	processes.err = processFailure
	proof, err = recoverProcessGeneration(t.Context(), processes)
	if !errors.Is(err, processFailure) || proof.complete {
		t.Fatalf("failed process recovery = %#v, %v", proof, err)
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
	protectedExecutable, err := os.Executable()
	if err != nil {
		panic(err)
	}
	protectedExecutable, err = filepath.EvalSymlinks(protectedExecutable)
	if err != nil {
		panic(err)
	}
	runtimeIdentity, err := proc.Probe(os.Getpid())
	if err != nil {
		panic(err)
	}
	return Config{
		Plan: plan, RuntimeBuild: build, Owner: "holder-test",
		TrustRequirements: RuntimeTrustRequirements{
			StopController:      testProcessRequirement("stop-controller"),
			ReceiptController:   testProcessRequirement("receipt-controller"),
			ReadinessController: testProcessRequirement("readiness-controller"),
		},
		StopControlStore: &proc.FileStore{Path: filepath.Join(dir, "stop-control.db")},
		planner:          testPlanner{}, native: native,
		fleetTransitions: testFleetTransitions{},
		Authorizer:       testMountAuthorizer{}, protectedPeer: func(context.Context, wire.Peer) error { return nil },
		protectedExecutable:     protectedExecutable,
		currentIdentity:         func() (proc.Identity, error) { return runtimeIdentity, nil },
		catalogService:          testCatalogService,
		catalogManager:          testCatalogManager,
		allowUnprotected:        true,
		CatalogReadinessTimeout: 30 * time.Second,
		CatalogOperationTimeout: 30 * time.Second,
		ShutdownTimeout:         5 * time.Second,
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
	config.TrustRequirements.FileProviderExtension = testProcessRequirement("file-provider-extension")
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
	return catalogworker.NewManager(ctx, managerConfig)
}

type testFleetTransitions struct{}

func (testFleetTransitions) Prepare(context.Context, tenant.FleetTransition) error { return nil }
func (testFleetTransitions) Commit(context.Context, tenant.FleetTransition) error  { return nil }
func (testFleetTransitions) Abort(context.Context, tenant.FleetTransition) error   { return nil }

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
	healthState  daemon.State
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
func (n *testNative) HealthState() daemon.State {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.healthState == "" {
		return daemon.StateHealthy
	}
	return n.healthState
}

func (n *testNative) setHealthState(state daemon.State) {
	n.mu.Lock()
	n.healthState = state
	n.mu.Unlock()
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
	go func() { done <- runtime.Run(context.Background()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = runtime.Close(ctx)
	})
	return done
}

func publishedRuntimeGraph(runtime *Runtime) *runtimeGraph {
	graph, ok := runtime.graphs.Load()
	if !ok {
		return nil
	}
	return graph
}

func waitNativeStart(t *testing.T, native *testNative, done <-chan error) {
	t.Helper()
	select {
	case <-native.started:
	case err := <-done:
		t.Fatalf("runtime stopped before native root start: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("native root did not start")
	}
}

func openMountClientEventually(t *testing.T, socket string) *mountservice.Client {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		client, err := mountservice.NewClient(t.Context(), wire.ClientConfig{
			Dial: wire.UnixDialer(socket), WireBuild: transportproto.WireBuild,
			Role: trust.UnprotectedRole,
		})
		if err == nil {
			return client
		}
		if time.Now().After(deadline) {
			t.Fatalf("open mount client: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
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
	case <-time.After(holderTestEventTimeout):
		return errors.New("runtime did not stop")
	}
}

func waitRuntimeReady(t *testing.T, runtime *Runtime, done <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
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

type testRegistry struct{}

func (testRegistry) TrackGroup(context.Context, int, proc.RecoveryID) (proc.Record, error) {
	return proc.Record{}, errors.New("unexpected worker")
}
func (testRegistry) Untrack(context.Context, proc.Record) error { return nil }
func (testRegistry) Owns(proc.Record) (bool, error)             { return false, nil }
func (testRegistry) Reap(context.Context) error                 { return nil }
func (testRegistry) TerminateWithin(context.Context, proc.Record, time.Duration) error {
	return errors.New("unexpected worker termination")
}

func testBrokerProcessStart(process managedProcess, prepared chan<- struct{}) brokerProcessStart {
	return func(
		_ context.Context,
		config proc.SpawnConfig,
		role trust.PeerRole,
		_ io.Writer,
		_ io.Writer,
	) (managedProcess, error) {
		if process == nil || config.RecoveryID != recoveryid.Broker || role != trustroles.Broker {
			return nil, errors.New("invalid broker process preparation")
		}
		close(prepared)
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

func (testMountAuthorizer) AuthorizeObservation(context.Context, mountservice.ObservationIdentity, mountproto.Operation) error {
	return nil
}

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
