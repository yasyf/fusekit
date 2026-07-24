package holder

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/trustroles"
)

type testManagedBrokerProcess struct {
	record  proc.Record
	stops   atomic.Int32
	stopErr error
	settled bool
	start   func(context.Context) error
}

func (p *testManagedBrokerProcess) Record() proc.Record { return p.record }

func (p *testManagedBrokerProcess) Start(ctx context.Context) error {
	if p.start != nil {
		return p.start(ctx)
	}
	return nil
}

func (*testManagedBrokerProcess) Done() <-chan struct{} { return make(chan struct{}) }

func (*testManagedBrokerProcess) Exit() (proc.ProcessExit, bool) { return proc.ProcessExit{}, false }

func (p *testManagedBrokerProcess) Settled() bool { return p.settled }

func (p *testManagedBrokerProcess) Stop(context.Context) error {
	p.stops.Add(1)
	return p.stopErr
}

func TestBrokerProcessOwnerBindsAndRetiresOnlyExpectedExactProcess(t *testing.T) {
	record := testBrokerRecord(42, "start-1", "generation-1")
	process := &testManagedBrokerProcess{record: record}
	var bound catalog.BrokerProcessIdentity
	var owner *brokerProcessOwner
	start := func(_ context.Context, _ proc.SpawnConfig, role trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
		if role != trustroles.Broker {
			return nil, errors.New("wrong broker role")
		}
		process.start = func(ctx context.Context) error {
			if _, err := owner.BindBroker(ctx, wire.Peer{
				PID: 43, StartTime: record.StartTime, Boot: record.Boot,
			}); err == nil {
				return errors.New("opportunistic peer was accepted")
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
		}
		return process, nil
	}
	var err error
	owner, err = newBrokerProcessOwner(testBrokerProcessPlan(t), start)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.StartBroker(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := brokerCatalogProcessIdentity(record)
	if bound != want {
		t.Fatalf("BindBroker = %+v, want %+v", bound, want)
	}
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
	start := func(_ context.Context, _ proc.SpawnConfig, _ trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
		starts++
		process.start = func(ctx context.Context) error {
			_, err := owner.BindBroker(ctx, testBrokerPeer(record))
			return err
		}
		return process, nil
	}
	var err error
	owner, err = newBrokerProcessOwner(testBrokerProcessPlan(t), start)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.StartBroker(t.Context()); err != nil {
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
	start := func(_ context.Context, _ proc.SpawnConfig, _ trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
		process.start = func(ctx context.Context) error {
			_, err := owner.BindBroker(ctx, testBrokerPeer(record))
			return err
		}
		return process, nil
	}
	var err error
	owner, err = newBrokerProcessOwner(testBrokerProcessPlan(t), start)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.StartBroker(t.Context()); err != nil {
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
	start := func(_ context.Context, _ proc.SpawnConfig, _ trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		process.start = func(ctx context.Context) error {
			_, err := owner.BindBroker(ctx, testBrokerPeer(record))
			return err
		}
		return process, nil
	}
	var err error
	owner, err = newBrokerProcessOwner(testBrokerProcessPlan(t), start)
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
	if got := calls.Load(); got != 1 {
		t.Fatalf("signed app launches = %d, want 1", got)
	}
}

func TestBrokerProcessRetirementDeadlineJoinsLaunchAndExactStop(t *testing.T) {
	t.Parallel()
	record := testBrokerRecord(42, "start-deadline", "generation-deadline")
	process := &testManagedBrokerProcess{record: record}
	bound := make(chan struct{})
	publish := make(chan struct{})
	var owner *brokerProcessOwner
	start := func(_ context.Context, _ proc.SpawnConfig, _ trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
		process.start = func(ctx context.Context) error {
			if _, err := owner.BindBroker(ctx, testBrokerPeer(record)); err != nil {
				return err
			}
			close(bound)
			<-publish
			return nil
		}
		return process, nil
	}
	var err error
	owner, err = newBrokerProcessOwner(testBrokerProcessPlan(t), start)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan error, 1)
	go func() { started <- owner.StartBroker(t.Context()) }()
	<-bound

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

func TestBrokerProcessOwnerRejectsLateBindAfterPreBindCrashAndPIDReuse(t *testing.T) {
	first := testBrokerRecord(42, "start-1", "generation-1")
	second := testBrokerRecord(42, "start-2", "generation-2")
	crash := errors.New("crash before bind")
	var owner *brokerProcessOwner
	starts := 0
	start := func(_ context.Context, _ proc.SpawnConfig, _ trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
		starts++
		record := first
		if starts == 2 {
			record = second
		}
		if starts == 1 {
			return nil, crash
		}
		process := &testManagedBrokerProcess{record: record}
		process.start = func(ctx context.Context) error {
			if _, err := owner.BindBroker(ctx, testBrokerPeer(first)); err == nil {
				return errors.New("late stale process bind was accepted")
			}
			_, err := owner.BindBroker(ctx, testBrokerPeer(record))
			return err
		}
		return process, nil
	}
	var err error
	owner, err = newBrokerProcessOwner(testBrokerProcessPlan(t), start)
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
	if starts != 2 {
		t.Fatalf("launches = %d, want 2", starts)
	}
}

func TestBrokerProcessOwnerSettlesCrashAfterBindWithoutDualOwnership(t *testing.T) {
	record := testBrokerRecord(42, "start-1", "generation-1")
	crash := errors.New("crash after bind")
	var owner *brokerProcessOwner
	process := &testManagedBrokerProcess{record: record}
	start := func(_ context.Context, _ proc.SpawnConfig, _ trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
		process.start = func(ctx context.Context) error {
			if _, err := owner.BindBroker(ctx, testBrokerPeer(record)); err != nil {
				return err
			}
			return crash
		}
		return process, nil
	}
	var err error
	owner, err = newBrokerProcessOwner(testBrokerProcessPlan(t), start)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.StartBroker(t.Context()); !errors.Is(err, crash) {
		t.Fatalf("StartBroker error = %v, want %v", err, crash)
	}
	if err := owner.RetireBroker(t.Context(), brokerCatalogProcessIdentity(record)); err != nil {
		t.Fatalf("RetireBroker after supervised crash: %v", err)
	}
	if owner.available() {
		t.Fatal("settled crashed broker retained ownership")
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
			var owner *brokerProcessOwner
			start := func(_ context.Context, _ proc.SpawnConfig, _ trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
				var process *testManagedBrokerProcess
				return process, test.startErr
			}
			var err error
			owner, err = newBrokerProcessOwner(testBrokerProcessPlan(t), start)
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

func TestBrokerProcessOwnerRetainsProcessReturnedWithStartAndStopErrors(t *testing.T) {
	record := testBrokerRecord(42, "start-1", "generation-1")
	crash := errors.New("launcher failed after process publication")
	stopFailure := errors.New("process reap was not proven")
	process := &testManagedBrokerProcess{record: record, stopErr: stopFailure}
	var owner *brokerProcessOwner
	start := func(_ context.Context, _ proc.SpawnConfig, _ trust.PeerRole, _, _ io.Writer) (managedProcess, error) {
		process.start = func(ctx context.Context) error {
			if _, err := owner.BindBroker(ctx, testBrokerPeer(record)); err != nil {
				return err
			}
			return crash
		}
		return process, nil
	}
	var err error
	owner, err = newBrokerProcessOwner(testBrokerProcessPlan(t), start)
	if err != nil {
		t.Fatal(err)
	}
	err = owner.StartBroker(t.Context())
	if !errors.Is(err, crash) || !errors.Is(err, stopFailure) {
		t.Fatalf("StartBroker error = %v, want joined launch and stop failures", err)
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
	spec, err := brokerProcessSpec(plan)
	if err != nil {
		t.Fatal(err)
	}
	broker, ok := plan.Broker()
	if !ok {
		t.Fatal("test broker plan is disabled")
	}
	wantArguments := []string{
		brokerChildModeArgument,
		brokerDaemonSocketArgument,
		plan.Paths().Socket,
	}
	if !reflect.DeepEqual(spec.Args, wantArguments) {
		t.Fatalf("arguments = %q, want %q", spec.Args, wantArguments)
	}
	if spec.Executable != broker.Deployment.Executable {
		t.Fatalf("executable = %q, want fixed signed executable %q", spec.Executable, broker.Deployment.Executable)
	}
	if spec.RecoveryID != recoveryid.Broker {
		t.Fatalf("recovery ID = %q, want broker", spec.RecoveryID)
	}
	if want := sanitizedChildEnvironment(os.Environ()); !reflect.DeepEqual(spec.Env, want) {
		t.Fatalf("environment = %q, want %q", spec.Env, want)
	}
	if got := filepath.Clean(spec.Executable); got != filepath.Join(
		plan.Application().AppPath, "Contents", "MacOS", plan.Application().Broker.ExecutableName,
	) {
		t.Fatalf("bundle executable = %q", got)
	}
	requirement := broker.Requirement
	if requirement.SigningIdentifier != plan.Application().Broker.SigningIdentifier ||
		!reflect.DeepEqual(requirement.RequiredEntitlements, testEntitlementPolicy().RequiredEntitlements) {
		t.Fatalf("broker process requirement = %#v, want plan broker role", requirement)
	}
	digest, err := requirement.ValidationDigest()
	if err != nil {
		t.Fatal(err)
	}
	if !spec.RequiresPeerFence || spec.ExpectedSignature == nil ||
		*spec.ExpectedSignature != (proc.SignatureDigest)(digest) {
		t.Fatalf("broker fence = required %t signature %v", spec.RequiresPeerFence, spec.ExpectedSignature)
	}
	if spec.Stdin != proc.StdioNull || spec.Stdout != proc.StdioPipe || spec.Stderr != proc.StdioPipe {
		t.Fatalf("broker stdio = %d/%d/%d, want null/pipe/pipe", spec.Stdin, spec.Stdout, spec.Stderr)
	}
}

func testBrokerRecord(pid int, start, generation string) proc.Record {
	return proc.Record{
		PID: pid, StartTime: start, Boot: "boot-1", Generation: holderOwnerGeneration(generation),
		ProcessGroup: true, SessionID: pid, RecoveryID: recoveryid.Broker,
	}
}

func testBrokerPeer(record proc.Record) wire.Peer {
	return wire.Peer{
		PID: record.PID, StartTime: record.StartTime, Boot: record.Boot,
		Comm: "ProductHelper", Executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
	}
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
