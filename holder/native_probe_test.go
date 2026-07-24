package holder

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/fusekit/mountmux"
)

type recordingWorkerRunner struct {
	tasks []worker.CommandRequest
	err   error
}

func (r *recordingWorkerRunner) Run(_ context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	r.tasks = append(r.tasks, task)
	return worker.CommandResult{}, r.err
}

func TestRunNativeMountProbeUsesKillableDisposableWorker(t *testing.T) {
	runner := &recordingWorkerRunner{}
	executable := "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper"
	root := "/Users/test/.cc-pool/accounts"
	var readinessLog bytes.Buffer
	if err := runNativeMountProbe(t.Context(), runner, executable, root, nativeProbeTestToken(), &readinessLog); err != nil {
		t.Fatalf("runNativeMountProbe: %v", err)
	}
	if len(runner.tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(runner.tasks))
	}
	task := runner.tasks[0]
	wantArgs, err := mountmux.NativeProbeChildArguments(mountmux.NativeProbeChildConfig{Root: root, Token: nativeProbeTestToken()})
	if err != nil {
		t.Fatal(err)
	}
	if task.Path != executable || task.Dir != "/" || task.TotalTimeout != nativeProbeTotalTimeout ||
		!reflect.DeepEqual(task.Args, wantArgs) {
		t.Fatalf("task = %#v", task)
	}
	for _, entry := range task.Env {
		if strings.HasPrefix(entry, "PATH=") || strings.HasPrefix(entry, "LANG=") || strings.HasPrefix(entry, "CGOFUSE_LIBFUSE_PATH=") {
			t.Fatalf("reserved worker environment leaked: %q", entry)
		}
	}
	probeID, err := mountmux.NativeProbeID(nativeProbeTestToken())
	if err != nil {
		t.Fatal(err)
	}
	logged := readinessLog.String()
	if !strings.Contains(logged, "phase=probe_task_dispatch probe_id="+probeID+" result=begin") ||
		!strings.Contains(logged, "phase=probe_task_settled probe_id="+probeID+" result=ok") ||
		strings.Contains(logged, nativeProbeTestToken()) {
		t.Fatalf("readiness log = %q", logged)
	}
}

func TestRunNativeMountProbeRejectsInvalidInputAndReturnsWorkerFailure(t *testing.T) {
	runner := &recordingWorkerRunner{}
	executable := "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper"
	if err := runNativeMountProbe(t.Context(), runner, executable, "relative", nativeProbeTestToken(), io.Discard); err == nil {
		t.Fatal("relative probe root succeeded")
	}
	if len(runner.tasks) != 0 {
		t.Fatalf("invalid root ran %d tasks", len(runner.tasks))
	}

	want := errors.New("probe failed")
	runner.err = want
	if err := runNativeMountProbe(t.Context(), runner, executable, "/Volumes/FuseKit", nativeProbeTestToken(), io.Discard); !errors.Is(err, want) {
		t.Fatalf("runNativeMountProbe error = %v, want %v", err, want)
	}
	if err := runNativeMountProbe(t.Context(), nil, executable, "/Volumes/FuseKit", nativeProbeTestToken(), io.Discard); err == nil {
		t.Fatal("nil runner succeeded")
	}
	if err := runNativeMountProbe(t.Context(), runner, "relative", "/Volumes/FuseKit", nativeProbeTestToken(), io.Discard); err == nil {
		t.Fatal("relative executable succeeded")
	}
}

func TestRunChildDispatchesExactNativeProbeMode(t *testing.T) {
	config := mountmux.NativeProbeChildConfig{Root: t.TempDir(), Token: nativeProbeTestToken()}
	arguments, err := mountmux.NativeProbeChildArguments(config)
	if err != nil {
		t.Fatal(err)
	}
	handled, err := RunChild(t.Context(), arguments, ChildConfig{})
	if err != nil || !handled {
		t.Fatalf("RunChild(native probe) = %t, %v", handled, err)
	}
	if handled, err := RunChild(t.Context(), []string{"consumer-mode"}, ChildConfig{}); err != nil || handled {
		t.Fatalf("RunChild(unrelated) = %t, %v", handled, err)
	}
}

func TestRunNativeMountProbeWaitsForCanceledTaskSettlement(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	runner := workerRunnerFunc(func(ctx context.Context, _ worker.CommandRequest) (worker.CommandResult, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return worker.CommandResult{}, ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		result <- runNativeMountProbe(
			ctx, runner, "/Users/example/Applications/ProductHelper.app/Contents/MacOS/ProductHelper",
			"/Volumes/FuseKit", nativeProbeTestToken(), io.Discard,
		)
	}()
	<-entered
	cancel()
	select {
	case err := <-result:
		t.Fatalf("probe returned before task settlement: %v", err)
	default:
	}
	close(release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("settled cancellation = %v", err)
	}
}

type workerRunnerFunc func(context.Context, worker.CommandRequest) (worker.CommandResult, error)

func (f workerRunnerFunc) Run(ctx context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	return f(ctx, task)
}

func nativeProbeTestToken() string { return strings.Repeat("a", 64) }
