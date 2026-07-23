package holder

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/fusekit/sourceauthority"
)

func TestSourceObserverProcessRequiresExactTrackedPeer(t *testing.T) {
	directory := shortTempDir(t)
	socket := filepath.Join(directory, "observer.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 2)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()

	identity, err := proc.Probe(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	record := sourceObserverTestRecord(identity)
	process := newSourceChildProcess(
		newFakeManagedProcess(record),
		func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	)
	conn, err := process.Dial(t.Context())
	if err != nil {
		t.Fatalf("exact observer peer rejected: %v", err)
	}
	_ = conn.Close()
	_ = (<-accepted).Close()

	wrong := record
	wrong.PID++
	process.process = newFakeManagedProcess(wrong)
	if conn, err := process.Dial(t.Context()); err == nil {
		_ = conn.Close()
		t.Fatal("wrong observer PID was trusted")
	}
	_ = (<-accepted).Close()
}

func TestSourceObserverProcessRejectsSocketReplacement(t *testing.T) {
	directory := shortTempDir(t)
	socket := filepath.Join(directory, "observer.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	identity, err := proc.Probe(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	original := sourceObserverTestRecord(identity)
	original.StartTime += "-replaced"
	process := newSourceChildProcess(
		newFakeManagedProcess(original),
		func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		},
	)
	if conn, err := process.Dial(t.Context()); err == nil {
		_ = conn.Close()
		t.Fatal("replacement socket owner was trusted as the original process")
	}
	_ = (<-accepted).Close()
}

func TestSourceObserverLauncherWaitsForSocketAndUsesFixedExecutable(t *testing.T) {
	t.Setenv("CGOFUSE_LIBFUSE_PATH", "/usr/local/lib/libfuse-t.dylib")
	t.Setenv("FUSEKIT_CHILD_ENV_SENTINEL", "preserved")
	directory := shortTempDir(t)
	socket := filepath.Join(directory, "observer.sock")
	record := proc.Record{
		PID: 42, StartTime: "start", Boot: "boot", Generation: "generation",
		ProcessGroup: true, SessionID: 42, RecoveryClass: proc.RecoveryObserver,
	}
	managed := newFakeManagedProcess(record)
	var capturedPath string
	var capturedClass proc.RecoveryClass
	var capturedEnv []string
	launcher := sourceProcessLauncher{
		executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		start: func(ctx context.Context, spec supervise.ProcessSpec) (managedProcess, error) {
			capturedPath = spec.Path
			capturedClass = spec.RecoveryClass
			capturedEnv = append([]string(nil), spec.Env...)
			listener, err := net.Listen("unix", socket)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() { _ = listener.Close() })
			if err := spec.Ready(ctx, record); err != nil {
				return nil, err
			}
			return managed, nil
		},
	}
	process, err := launcher.LaunchSourceObserver(t.Context(), sourceauthority.ObserverProcessSpec{
		Socket: socket, Arguments: []string{"--fusekit-source-observer-child", socket},
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

func TestSourceChildIdentityChangeJoinsExactReapingStop(t *testing.T) {
	directory := shortTempDir(t)
	socket := filepath.Join(directory, "observer.sock")
	readyRecord := proc.Record{
		PID: 42, StartTime: "start", Boot: "boot", Generation: "generation",
		ProcessGroup: true, SessionID: 42, RecoveryClass: proc.RecoveryObserver,
	}
	returnedRecord := readyRecord
	returnedRecord.StartTime = "replacement"
	stopErr := errors.New("source child stop failure")
	managed := &gatedManagedProcess{
		record: returnedRecord, entered: make(chan struct{}), release: make(chan struct{}),
		done: make(chan struct{}), stopErr: stopErr,
	}
	launcher := sourceProcessLauncher{
		executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		start: func(ctx context.Context, spec supervise.ProcessSpec) (managedProcess, error) {
			listener, err := net.Listen("unix", socket)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() { _ = listener.Close() })
			if err := spec.Ready(ctx, readyRecord); err != nil {
				return nil, err
			}
			return managed, nil
		},
	}
	result := make(chan error, 1)
	go func() {
		_, err := launcher.LaunchSourceObserver(t.Context(), sourceauthority.ObserverProcessSpec{
			Socket: socket, Arguments: []string{"--fusekit-source-observer-child", socket},
		})
		result <- err
	}()
	<-managed.entered
	select {
	case err := <-result:
		t.Fatalf("identity-changing launch returned before process settlement: %v", err)
	default:
	}
	close(managed.release)
	if err := <-result; !strings.Contains(err.Error(), "identity changed") || !errors.Is(err, stopErr) {
		t.Fatalf("identity-changing launch = %v, want identity and stop failures", err)
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
	launcher := sourceProcessLauncher{
		executable: "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
		start: func(_ context.Context, spec supervise.ProcessSpec) (managedProcess, error) {
			if spec.RecoveryClass != proc.RecoveryTask {
				t.Fatalf("source task recovery class = %d", spec.RecoveryClass)
			}
			return managed, startErr
		},
	}
	socket := filepath.Join(shortTempDir(t), "task.sock")
	result := make(chan error, 1)
	go func() {
		_, err := launcher.LaunchSourceTask(t.Context(), sourceauthority.SourceTaskProcessSpec{
			Socket: socket, Arguments: []string{"--fusekit-source-task-child"},
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
				start: func(context.Context, supervise.ProcessSpec) (managedProcess, error) {
					var process *fakeManagedProcess
					return process, test.err
				},
			}
			_, err := launcher.LaunchSourceTask(t.Context(), sourceauthority.SourceTaskProcessSpec{
				Socket:    filepath.Join(shortTempDir(t), "task.sock"),
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
	child := newSourceChildProcess(managed, nil)
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

func TestWaitForSourceSocketRejectsNonSocketAndDeadline(t *testing.T) {
	directory := shortTempDir(t)
	path := filepath.Join(directory, "observer.sock")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := waitForSourceSocket(t.Context(), path); err == nil {
		t.Fatal("regular readiness file accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := waitForSourceSocket(ctx, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error = %v, want deadline", err)
	}
}

func sourceObserverTestRecord(identity proc.Identity) proc.Record {
	return proc.Record{
		PID: identity.PID, StartTime: identity.StartTime, Boot: identity.Boot,
		Generation: "generation", ProcessGroup: true, SessionID: identity.PID,
		RecoveryClass: proc.RecoveryObserver,
	}
}
