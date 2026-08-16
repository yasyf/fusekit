package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

type testManagedBrokerProcess struct {
	record  catalog.ProcessRecord
	stops   atomic.Int32
	stopErr error
	settled bool
	start   func(context.Context) error
	stop    func()
}

func testBrokerChannelServe(context.Context, managedProcess) error { return nil }

func (p *testManagedBrokerProcess) Record() catalog.ProcessRecord { return p.record }

func (p *testManagedBrokerProcess) Start(ctx context.Context) error {
	if p.start != nil {
		return p.start(ctx)
	}
	return nil
}

func (*testManagedBrokerProcess) Done() <-chan struct{} { return make(chan struct{}) }

func (*testManagedBrokerProcess) Exit() (daemonkit.Exit, bool) { return daemonkit.Exit{}, false }

func (p *testManagedBrokerProcess) Settled() bool { return p.settled }

func (p *testManagedBrokerProcess) Stop(context.Context) error {
	p.stops.Add(1)
	if p.stop != nil {
		p.stop()
	}
	return p.stopErr
}

func TestBrokerProcessOwnerBindsAndRetiresOnlyExpectedExactProcess(t *testing.T) {
	record := testBrokerRecord(42, "start-1", "generation-1")
	process := &testManagedBrokerProcess{record: record}
	var bound catalog.BrokerProcessIdentity
	var owner *brokerProcessOwner
	var output io.Writer
	bind, script := brokerProcessBindScript(func(ctx context.Context) error {
		peerCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()
		if _, err := owner.BindBroker(peerCtx, daemonkit.Caller{
			UID: uint32(os.Getuid()), PID: 43,
		}); !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("opportunistic peer bind = %v, want the launch deadline", err)
		}
		var err error
		bound, err = owner.BindBroker(ctx, testBrokerPeer(record))
		if err != nil {
			return err
		}
		if _, err := owner.BindBroker(ctx, testBrokerPeer(record)); err == nil {
			return errors.New("duplicate broker bind was accepted")
		}
		return nil
	})
	start := func(_ managedSpawnConfig, stderr io.Writer) (managedProcess, error) {
		output = stderr
		process.start = bind
		return process, nil
	}
	plan := testBrokerProcessPlan(t)
	owner, err := newBrokerProcessOwner(plan, t.Context(), testBrokerChannelServe, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.StartBroker(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-script; err != nil {
		t.Fatal(err)
	}
	want := brokerCatalogProcessIdentity(record)
	if bound != want {
		t.Fatalf("BindBroker = %+v, want %+v", bound, want)
	}
	brokerProcessAssertOwnedLog(t, plan, output)
	substituted := want
	substituted.StartTime = "reused-pid"
	if err := owner.RetireBroker(t.Context(), substituted); err == nil {
		t.Fatal("RetireBroker accepted substituted process identity")
	}
	if process.stops.Load() != 0 || !owner.available() {
		t.Fatalf("identity mismatch touched process: stops %d, available %t", process.stops.Load(), owner.available())
	}
	if err := owner.RetireBroker(t.Context(), want); err != nil {
		t.Fatal(err)
	}
	if owner.available() || process.stops.Load() != 1 {
		t.Fatalf("retirement = available %t, stops %d", owner.available(), process.stops.Load())
	}
}

func TestBrokerProcessOwnerNeverReleasesCapacityWithoutReapProof(t *testing.T) {
	record := testBrokerRecord(42, "start-1", "generation-1")
	process := &testManagedBrokerProcess{record: record, stopErr: errors.New("unsettled")}
	var owner *brokerProcessOwner
	starts := 0
	bind, script := brokerProcessBindScript(func(ctx context.Context) error {
		_, err := owner.BindBroker(ctx, testBrokerPeer(record))
		return err
	})
	start := func(managedSpawnConfig, io.Writer) (managedProcess, error) {
		starts++
		process.start = bind
		return process, nil
	}
	var err error
	owner, err = brokerProcessTestOwner(t, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.StartBroker(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-script; err != nil {
		t.Fatal(err)
	}
	identity := brokerCatalogProcessIdentity(record)
	if err := owner.RetireBroker(t.Context(), identity); err == nil {
		t.Fatal("RetireBroker succeeded without reap proof")
	}
	if err := owner.StartBroker(t.Context()); err != nil {
		t.Fatal(err)
	}
	if starts != 1 || !owner.available() {
		t.Fatalf("failed retirement released capacity: starts %d, available %t", starts, owner.available())
	}
}

func TestBrokerProcessOwnerReleasesReapedCapacityAfterOutputError(t *testing.T) {
	record := testBrokerRecord(42, "start-output", "generation-output")
	outputErr := errors.New("close broker log")
	process := &testManagedBrokerProcess{record: record, stopErr: outputErr, settled: true}
	var owner *brokerProcessOwner
	bind, script := brokerProcessBindScript(func(ctx context.Context) error {
		_, err := owner.BindBroker(ctx, testBrokerPeer(record))
		return err
	})
	start := func(managedSpawnConfig, io.Writer) (managedProcess, error) {
		process.start = bind
		return process, nil
	}
	var err error
	owner, err = brokerProcessTestOwner(t, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.StartBroker(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-script; err != nil {
		t.Fatal(err)
	}
	if err := owner.RetireBroker(t.Context(), brokerCatalogProcessIdentity(record)); !errors.Is(err, outputErr) {
		t.Fatalf("RetireBroker = %v, want output error", err)
	}
	if owner.available() {
		t.Fatal("reaped broker retained capacity after output error")
	}
}

func TestBrokerProcessOwnerSerializesRelaunchUntilExactBinding(t *testing.T) {
	record := testBrokerRecord(42, "start-1", "generation-1")
	process := &testManagedBrokerProcess{record: record}
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var owner *brokerProcessOwner
	bind, script := brokerProcessBindScript(func(ctx context.Context) error {
		_, err := owner.BindBroker(ctx, testBrokerPeer(record))
		return err
	})
	start := func(managedSpawnConfig, io.Writer) (managedProcess, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		process.start = bind
		return process, nil
	}
	var err error
	owner, err = brokerProcessTestOwner(t, start)
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- owner.StartBroker(t.Context()) }()
	<-entered
	go func() { second <- owner.StartBroker(t.Context()) }()
	select {
	case err := <-second:
		t.Fatalf("second relaunch bypassed the in-flight launch: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if err := <-script; err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("signed app launches = %d, want 1", got)
	}
}

func TestBrokerProcessRetirementDeadlineJoinsLaunchAndExactStop(t *testing.T) {
	record := testBrokerRecord(42, "start-deadline", "generation-deadline")
	process := &testManagedBrokerProcess{record: record}
	publish := make(chan struct{})
	var owner *brokerProcessOwner
	bind, script := brokerProcessBindScript(func(ctx context.Context) error {
		<-publish
		_, err := owner.BindBroker(ctx, testBrokerPeer(record))
		return err
	})
	start := func(managedSpawnConfig, io.Writer) (managedProcess, error) {
		process.start = bind
		return process, nil
	}
	var err error
	owner, err = brokerProcessTestOwner(t, start)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- owner.StartBroker(t.Context()) }()
	brokerProcessAwaitExpectation(owner)

	deadlineCtx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
	defer cancel()
	<-deadlineCtx.Done()
	retired := make(chan error, 1)
	go func() {
		retired <- owner.RetireBroker(deadlineCtx, brokerCatalogProcessIdentity(record))
	}()
	select {
	case err := <-retired:
		t.Fatalf("RetireBroker returned before launch settlement: %v", err)
	default:
	}
	close(publish)
	if err := <-started; err != nil {
		t.Fatalf("StartBroker: %v", err)
	}
	if err := <-script; err != nil {
		t.Fatalf("broker bind: %v", err)
	}
	if err := <-retired; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RetireBroker = %v, want deadline after exact settlement", err)
	}
	if stops := process.stops.Load(); stops != 1 {
		t.Fatalf("broker stop calls = %d, want 1", stops)
	}
	if owner.available() {
		t.Fatal("retired broker retained capacity after exact stop")
	}
}

func TestBrokerProcessOwnerBindsOnlyRelaunchedRecordAfterPreBindCrash(t *testing.T) {
	first := testBrokerRecord(42, "start-1", "generation-1")
	second := testBrokerRecord(42, "start-2", "generation-2")
	crash := errors.New("crash before bind")
	process := &testManagedBrokerProcess{record: second}
	var owner *brokerProcessOwner
	var bound catalog.BrokerProcessIdentity
	starts := 0
	bind, script := brokerProcessBindScript(func(ctx context.Context) error {
		var err error
		bound, err = owner.BindBroker(ctx, testBrokerPeer(second))
		return err
	})
	start := func(managedSpawnConfig, io.Writer) (managedProcess, error) {
		starts++
		if starts == 1 {
			return nil, crash
		}
		process.start = bind
		return process, nil
	}
	var err error
	owner, err = brokerProcessTestOwner(t, start)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.StartBroker(t.Context()); !errors.Is(err, crash) {
		t.Fatalf("first StartBroker error = %v, want %v", err, crash)
	}
	if _, err := owner.BindBroker(t.Context(), testBrokerPeer(first)); err == nil {
		t.Fatal("untracked late process bound after failed launch")
	}
	if err := owner.StartBroker(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-script; err != nil {
		t.Fatal(err)
	}
	if starts != 2 {
		t.Fatalf("launches = %d, want 2", starts)
	}
	if want := brokerCatalogProcessIdentity(second); bound != want {
		t.Fatalf("bound identity = %+v, want the relaunched record %+v", bound, want)
	}
	if err := owner.RetireBroker(t.Context(), brokerCatalogProcessIdentity(first)); err == nil {
		t.Fatal("crashed launch identity stayed owned across the relaunch")
	}
	if process.stops.Load() != 0 || !owner.available() {
		t.Fatalf(
			"crashed identity touched the relaunch: stops %d, available %t",
			process.stops.Load(), owner.available(),
		)
	}
	if err := owner.RetireBroker(t.Context(), brokerCatalogProcessIdentity(second)); err != nil {
		t.Fatal(err)
	}
	if owner.available() || process.stops.Load() != 1 {
		t.Fatalf("retirement = available %t, stops %d", owner.available(), process.stops.Load())
	}
}

func TestBrokerProcessOwnerSettlesTeardownBindWithoutDualOwnership(t *testing.T) {
	record := testBrokerRecord(42, "start-1", "generation-1")
	process := &testManagedBrokerProcess{record: record}
	lateBind := make(chan error, 1)
	var owner *brokerProcessOwner
	// The child's bind lands while the owner is already tearing the timed-out
	// launch down: Stop is the one point production offers between the
	// abandoned await and the settlement decision.
	process.stop = func() {
		_, err := owner.BindBroker(context.Background(), testBrokerPeer(record))
		lateBind <- err
	}
	start := func(managedSpawnConfig, io.Writer) (managedProcess, error) { return process, nil }
	var err error
	owner, err = brokerProcessTestOwner(t, start)
	if err != nil {
		t.Fatal(err)
	}
	deadlineCtx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
	defer cancel()
	<-deadlineCtx.Done()
	if err := owner.StartBroker(deadlineCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StartBroker error = %v, want the launch deadline", err)
	}
	if err := <-lateBind; err != nil {
		t.Fatalf("bind during teardown: %v", err)
	}
	if err := owner.RetireBroker(t.Context(), brokerCatalogProcessIdentity(record)); err != nil {
		t.Fatalf("RetireBroker after teardown bind: %v", err)
	}
	if err := owner.RetireBroker(t.Context(), brokerCatalogProcessIdentity(record)); err == nil {
		t.Fatal("settled identity was retired twice")
	}
	if owner.available() || process.stops.Load() != 1 {
		t.Fatalf("settlement = available %t, stops %d", owner.available(), process.stops.Load())
	}
}

func TestBrokerProcessOwnerRejectsTypedNilProcessWithoutLosingSettlement(t *testing.T) {
	record := testBrokerRecord(42, "start-1", "generation-1")
	launchFailure := errors.New("launcher failed after process publication")
	tests := []struct {
		name      string
		startErr  error
		wantError error
	}{
		{name: "failed launch", startErr: launchFailure, wantError: launchFailure},
		{name: "successful launch without process", wantError: errMissingBrokerProcess},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			start := func(managedSpawnConfig, io.Writer) (managedProcess, error) {
				var process *testManagedBrokerProcess
				return process, test.startErr
			}
			owner, err := brokerProcessTestOwner(t, start)
			if err != nil {
				t.Fatal(err)
			}
			if err := owner.StartBroker(t.Context()); !errors.Is(err, test.wantError) {
				t.Fatalf("StartBroker error = %v, want %v", err, test.wantError)
			}
			if err := owner.RetireBroker(t.Context(), brokerCatalogProcessIdentity(record)); err == nil {
				t.Fatal("typed-nil prepared process retained an identity")
			}
			if owner.available() {
				t.Fatal("typed-nil launch result retained ownership")
			}
		})
	}
}

func TestBrokerProcessOwnerRetainsProcessAfterUnboundLaunchAndStopError(t *testing.T) {
	record := testBrokerRecord(42, "start-1", "generation-1")
	stopFailure := errors.New("process reap was not proven")
	process := &testManagedBrokerProcess{record: record, stopErr: stopFailure}
	start := func(managedSpawnConfig, io.Writer) (managedProcess, error) { return process, nil }
	owner, err := brokerProcessTestOwner(t, start)
	if err != nil {
		t.Fatal(err)
	}
	deadlineCtx, cancel := context.WithTimeout(t.Context(), time.Nanosecond)
	defer cancel()
	<-deadlineCtx.Done()
	err = owner.StartBroker(deadlineCtx)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, stopFailure) {
		t.Fatalf("StartBroker error = %v, want joined await and stop failures", err)
	}
	if process.stops.Load() != 1 || !owner.available() {
		t.Fatalf(
			"failed stop released ownership: stops %d, available %t",
			process.stops.Load(),
			owner.available(),
		)
	}
	process.stopErr = nil
	if err := owner.RetireBroker(t.Context(), brokerCatalogProcessIdentity(record)); err != nil {
		t.Fatalf("RetireBroker retry: %v", err)
	}
	if process.stops.Load() != 2 || owner.available() {
		t.Fatalf(
			"successful retry did not settle ownership: stops %d, available %t",
			process.stops.Load(),
			owner.available(),
		)
	}
}

func TestBrokerProcessSpecUsesFixedSignedBundleExecutableAndExactChildArguments(t *testing.T) {
	t.Setenv("CGOFUSE_LIBFUSE_PATH", "/usr/local/lib/libfuse-t.dylib")
	t.Setenv("FUSEKIT_CHILD_ENV_SENTINEL", "preserved")
	plan := testBrokerProcessPlan(t)
	config, err := brokerProcessSpec(plan)
	if err != nil {
		t.Fatal(err)
	}
	broker, ok := plan.Broker()
	if !ok {
		t.Fatal("test broker plan is disabled")
	}
	wantArguments := []string{brokerChildModeArgument}
	if !reflect.DeepEqual(config.cmd.Args, wantArguments) {
		t.Fatalf("arguments = %q, want %q", config.cmd.Args, wantArguments)
	}
	for _, argument := range config.cmd.Args {
		if strings.ContainsAny(argument, "/") || strings.Contains(argument, plan.Paths().Socket) {
			t.Fatalf("argument %q carries a path; the broker child learns no socket path at all", argument)
		}
	}
	if config.cmd.Path != broker.Deployment.Executable {
		t.Fatalf("executable = %q, want fixed signed executable %q", config.cmd.Path, broker.Deployment.Executable)
	}
	if config.id != recoveryid.Broker {
		t.Fatalf("recovery ID = %q, want broker", config.id)
	}
	if want := sanitizedChildEnvironment(os.Environ()); !reflect.DeepEqual(config.cmd.Env, want) {
		t.Fatalf("environment = %q, want %q", config.cmd.Env, want)
	}
	if got := filepath.Clean(config.cmd.Path); got != filepath.Join(
		plan.Application().AppPath, "Contents", "MacOS", plan.Application().Broker.ExecutableName,
	) {
		t.Fatalf("bundle executable = %q", got)
	}
	requirement := broker.Requirement
	if requirement.SigningIdentifier != plan.Application().Broker.SigningIdentifier ||
		!reflect.DeepEqual(requirement.RequiredEntitlements, testEntitlementPolicy().RequiredEntitlements) {
		t.Fatalf("broker process requirement = %#v, want plan broker role", requirement)
	}
	if !reflect.DeepEqual(config.cmd.Exec, daemonkit.ServingSigned(requirement)) {
		t.Fatalf("broker exec posture = %#v, want the signed broker requirement", config.cmd.Exec)
	}
	if !config.cmd.Session || config.channel != daemonkit.ChannelHandoff {
		t.Fatalf(
			"broker spawn = session %t channel %d, want a dedicated session on the handoff channel",
			config.cmd.Session, config.channel,
		)
	}
}

func brokerProcessAssertOwnedLog(t *testing.T, plan RuntimePlan, output io.Writer) {
	t.Helper()
	owned, ok := output.(*ownedProcessWriter)
	if !ok || owned.closer == nil {
		t.Fatalf("broker output = %#v, want an owned closable writer", output)
	}
	marker := "broker-output-" + t.Name()
	if _, err := owned.Write([]byte(marker + "\n")); err != nil {
		t.Fatalf("write broker output: %v", err)
	}
	logged, err := os.ReadFile(filepath.Join(plan.Paths().Directory, "broker.log"))
	if err != nil {
		t.Fatalf("read broker log: %v", err)
	}
	if !strings.Contains(string(logged), marker) {
		t.Fatalf("broker log = %q, want the owned writer's output", string(logged))
	}
}

// brokerProcessBindScript defers a bind script to its own goroutine so it races
// StartBroker's awaitBound instead of running inside Start: the owner registers
// the launched record only once Start has returned, so an in-Start bind would
// wait on a record that cannot exist yet. BindBroker's retry loop absorbs either
// arrival order, and the returned channel carries the script's verdict.
func brokerProcessBindScript(
	script func(context.Context) error,
) (func(context.Context) error, <-chan error) {
	result := make(chan error, 1)
	return func(ctx context.Context) error {
		go func() { result <- script(ctx) }()
		return nil
	}, result
}

func brokerProcessAwaitExpectation(owner *brokerProcessOwner) {
	for {
		owner.mu.Lock()
		expected := len(owner.records) != 0
		changed := owner.changed
		owner.mu.Unlock()
		if expected {
			return
		}
		<-changed
	}
}

func testBrokerRecord(pid int, start, generation string) catalog.ProcessRecord {
	return catalog.ProcessRecord{
		PID: pid, StartTime: start, Boot: "boot-1", Generation: holderOwnerGeneration(generation),
		ProcessGroup: true, SessionID: pid, RecoveryID: recoveryid.Broker,
	}
}

func testBrokerPeer(record catalog.ProcessRecord) daemonkit.Caller {
	return daemonkit.Caller{UID: uint32(os.Getuid()), PID: record.PID}
}

func brokerProcessTestOwner(t *testing.T, start brokerProcessStart) (*brokerProcessOwner, error) {
	t.Helper()
	plan := testBrokerProcessPlan(t)
	return newBrokerProcessOwner(plan, t.Context(), testBrokerChannelServe, start)
}

func testBrokerProcessPlan(t *testing.T) RuntimePlan {
	t.Helper()
	home := shortTempDir(t)
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.product", "ProductHelper"),
		RuntimeDirectory: filepath.Join(home, "runtime"),
		Native:           testNativeRuntimeSpec(filepath.Join(home, "presentation")),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     testEntitlementPolicy(), RuntimePolicy: testEntitlementPolicy(),
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(plan.Paths().Directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return plan
}
