package holder

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/sourceauthority"
)

type fakeManagedSessionProcess struct {
	managedProcess
	conn net.Conn
}

func (p *fakeManagedSessionProcess) Conn() net.Conn { return p.conn }

func testHolderManagedSessionPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	childInput, parentInput, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	parentOutput, childOutput, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	parent, err := wire.NewDuplexConn(parentOutput, parentInput)
	if err != nil {
		t.Fatal(err)
	}
	child, err := wire.NewDuplexConn(childInput, childOutput)
	if err != nil {
		t.Fatal(err)
	}
	return parent, child
}

func TestSourceChildManagedSessionCanBeClaimedExactlyOnce(t *testing.T) {
	parent, child := testHolderManagedSessionPair(t)
	t.Cleanup(func() { _ = parent.Close(); _ = child.Close() })
	identity, err := proc.Probe(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	process := newSourceChildProcess(&fakeManagedSessionProcess{
		managedProcess: newFakeManagedProcess(sourceObserverTestRecord(identity)), conn: parent,
	})
	conn, err := process.Dial(t.Context())
	if err != nil {
		t.Fatalf("managed session rejected: %v", err)
	}
	if conn != parent {
		t.Fatal("source child substituted its daemonkit-managed session")
	}
	if second, err := process.Dial(t.Context()); err == nil || second != nil {
		t.Fatal("source child session was claimed twice")
	}
}

func TestSourceObserverLauncherUsesManagedSessionAndFixedExecutable(t *testing.T) {
	t.Setenv("CGOFUSE_LIBFUSE_PATH", "/usr/local/lib/libfuse-t.dylib")
	t.Setenv("FUSEKIT_CHILD_ENV_SENTINEL", "preserved")
	record := proc.Record{
		PID: 42, StartTime: "start", Boot: "boot", Generation: "generation",
		ProcessGroup: true, SessionID: 42, RecoveryClass: proc.RecoveryObserver,
	}
	parent, child := testHolderManagedSessionPair(t)
	t.Cleanup(func() { _ = parent.Close(); _ = child.Close() })
	managed := &fakeManagedSessionProcess{managedProcess: newFakeManagedProcess(record), conn: parent}
	var capturedPath string
	var capturedClass proc.RecoveryClass
	var capturedEnv []string
	launcher := sourceProcessLauncher{
		executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		startSession: func(_ context.Context, spec supervise.SessionProcessSpec) (managedSessionProcess, error) {
			capturedPath = spec.Path
			capturedClass = spec.RecoveryClass
			capturedEnv = append([]string(nil), spec.Env...)
			return managed, nil
		},
	}
	process, err := launcher.LaunchSourceObserver(t.Context(), sourceauthority.ObserverProcessSpec{
		Arguments: []string{"--fusekit-source-observer-child"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if process == nil || capturedPath != launcher.executable {
		t.Fatalf("source observer launch = %T path %q", process, capturedPath)
	}
	if capturedClass != proc.RecoveryObserver {
		t.Fatalf("source observer recovery class = %d", capturedClass)
	}
	assertSanitizedChildEnvironment(t, capturedEnv)
}

func assertSanitizedChildEnvironment(t *testing.T, environment []string) {
	t.Helper()
	foundSentinel := false
	for _, entry := range environment {
		if strings.HasPrefix(entry, "CGOFUSE_LIBFUSE_PATH=") {
			t.Fatalf("native-only loader path leaked into non-native child: %q", entry)
		}
		foundSentinel = foundSentinel || entry == "FUSEKIT_CHILD_ENV_SENTINEL=preserved"
	}
	if !foundSentinel {
		t.Fatal("unrelated child environment was not preserved")
	}
}

func TestSourceChildInvalidManagedIdentityJoinsExactReapingStop(t *testing.T) {
	invalidRecord := proc.Record{PID: 42, RecoveryClass: proc.RecoveryObserver}
	stopErr := errors.New("source child stop failure")
	managed := &gatedManagedProcess{
		record: invalidRecord, entered: make(chan struct{}), release: make(chan struct{}),
		done: make(chan struct{}), stopErr: stopErr,
	}
	parent, child := testHolderManagedSessionPair(t)
	t.Cleanup(func() { _ = parent.Close(); _ = child.Close() })
	session := &fakeManagedSessionProcess{managedProcess: managed, conn: parent}
	launcher := sourceProcessLauncher{
		executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		startSession: func(context.Context, supervise.SessionProcessSpec) (managedSessionProcess, error) {
			return session, nil
		},
	}
	result := make(chan error, 1)
	go func() {
		_, err := launcher.LaunchSourceObserver(t.Context(), sourceauthority.ObserverProcessSpec{
			Arguments: []string{"--fusekit-source-observer-child"},
		})
		result <- err
	}()
	<-managed.entered
	select {
	case err := <-result:
		t.Fatalf("invalid-identity launch returned before process settlement: %v", err)
	default:
	}
	close(managed.release)
	if err := <-result; !strings.Contains(err.Error(), "process identity") || !errors.Is(err, stopErr) {
		t.Fatalf("invalid-identity launch = %v, want identity and stop failures", err)
	}
	if managed.stops.Load() != 1 {
		t.Fatalf("cleanup stops=%d, want 1", managed.stops.Load())
	}
}

func TestSourceChildLaunchErrorStopsReturnedProcessBeforeReturn(t *testing.T) {
	startErr := errors.New("source child launch failed after process creation")
	stopErr := errors.New("source child stop failed")
	record := proc.Record{
		PID: 42, StartTime: "start", Boot: "boot", Generation: "generation",
		ProcessGroup: true, SessionID: 42, RecoveryClass: proc.RecoveryTask,
	}
	managed := &gatedManagedProcess{
		record: record, entered: make(chan struct{}), release: make(chan struct{}),
		done: make(chan struct{}), stopErr: stopErr,
	}
	parent, childConn := testHolderManagedSessionPair(t)
	t.Cleanup(func() { _ = parent.Close(); _ = childConn.Close() })
	session := &fakeManagedSessionProcess{managedProcess: managed, conn: parent}
	launcher := sourceProcessLauncher{
		executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		startSession: func(_ context.Context, spec supervise.SessionProcessSpec) (managedSessionProcess, error) {
			if spec.RecoveryClass != proc.RecoveryTask {
				t.Fatalf("source task recovery class = %d", spec.RecoveryClass)
			}
			return session, startErr
		},
	}
	result := make(chan error, 1)
	go func() {
		_, err := launcher.LaunchSourceTask(t.Context(), sourceauthority.SourceTaskProcessSpec{
			Arguments: []string{"--fusekit-source-task-child"},
		})
		result <- err
	}()
	<-managed.entered
	select {
	case err := <-result:
		t.Fatalf("failed launch returned before process settlement: %v", err)
	default:
	}
	close(managed.release)
	if err := <-result; !errors.Is(err, startErr) || !errors.Is(err, stopErr) {
		t.Fatalf("failed launch = %v, want launch and settlement failures", err)
	}
	if managed.stops.Load() != 1 {
		t.Fatalf("cleanup stops=%d, want 1", managed.stops.Load())
	}
}

func TestSourceChildRejectsTypedNilProcess(t *testing.T) {
	startErr := errors.New("source child typed-nil launch failed")
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "failure", err: startErr},
		{name: "success"},
	} {
		t.Run(test.name, func(t *testing.T) {
			launcher := sourceProcessLauncher{
				executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
				startSession: func(context.Context, supervise.SessionProcessSpec) (managedSessionProcess, error) {
					var process *fakeManagedSessionProcess
					return process, test.err
				},
			}
			_, err := launcher.LaunchSourceTask(t.Context(), sourceauthority.SourceTaskProcessSpec{
				Arguments: []string{"--fusekit-source-task-child"},
			})
			if test.err != nil {
				if !errors.Is(err, test.err) {
					t.Fatalf("LaunchSourceTask = %v, want %v", err, test.err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "returned no process") {
				t.Fatalf("LaunchSourceTask = %v, want missing process rejection", err)
			}
		})
	}
}

func TestSourceChildStopJoinsOnceAndReplaysTerminalResult(t *testing.T) {
	terminalErr := errors.New("source child terminal failure")
	managed := &gatedManagedProcess{
		entered: make(chan struct{}), release: make(chan struct{}),
		done: make(chan struct{}), stopErr: terminalErr,
	}
	parent, childConn := testHolderManagedSessionPair(t)
	t.Cleanup(func() { _ = parent.Close(); _ = childConn.Close() })
	child := newSourceChildProcess(&fakeManagedSessionProcess{managedProcess: managed, conn: parent})
	ctx, cancel := context.WithCancel(t.Context())
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- child.Stop(ctx) }()
	go func() { second <- child.Stop(context.Background()) }()
	<-managed.entered
	cancel()
	select {
	case err := <-first:
		t.Fatalf("canceled Stop returned before exact settlement: %v", err)
	case err := <-second:
		t.Fatalf("concurrent Stop returned before exact settlement: %v", err)
	default:
	}
	close(managed.release)
	if err := <-first; !errors.Is(err, context.Canceled) || !errors.Is(err, terminalErr) {
		t.Fatalf("canceled Stop = %v, want cancellation and terminal failure", err)
	}
	if err := <-second; errors.Is(err, context.Canceled) || !errors.Is(err, terminalErr) {
		t.Fatalf("concurrent Stop = %v, want terminal failure", err)
	}
	if err := child.Stop(context.Background()); !errors.Is(err, terminalErr) {
		t.Fatalf("repeated Stop = %v, want terminal failure", err)
	}
	if err := child.Wait(context.Background()); !errors.Is(err, terminalErr) {
		t.Fatalf("Wait = %v, want cached terminal failure", err)
	}
	if managed.stops.Load() != 1 {
		t.Fatalf("physical stops=%d, want 1", managed.stops.Load())
	}
}

func sourceObserverTestRecord(identity proc.Identity) proc.Record {
	return proc.Record{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Generation: "generation", ProcessGroup: true, SessionID: identity.PID,
		RecoveryClass: proc.RecoveryObserver,
	}
}
