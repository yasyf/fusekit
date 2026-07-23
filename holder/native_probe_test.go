package holder

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/fusekit/mountmux"
)

type recordingTaskRunner struct {
	tasks []supervise.Task
	err   error
}

func (r *recordingTaskRunner) Run(_ context.Context, task supervise.Task) error {
	r.tasks = append(r.tasks, task)
	return r.err
}

func TestRunNativeMountProbeUsesKillableDisposableWorker(t *testing.T) {
	runner := &recordingTaskRunner{}
	executable := "/Applications/FuseKit.app/Contents/MacOS/FuseKit"
	root := "/Users/test/.cc-pool/accounts"
	if err := runNativeMountProbe(t.Context(), runner, executable, root, testNativeProbeToken()); err != nil {
		t.Fatalf("runNativeMountProbe: %v", err)
	}
	if len(runner.tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(runner.tasks))
	}
	task := runner.tasks[0]
	wantArgs, err := mountmux.NativeProbeChildArguments(mountmux.NativeProbeChildConfig{Root: root, Token: testNativeProbeToken()})
	if err != nil {
		t.Fatal(err)
	}
	if task.RecoveryClass != proc.RecoveryTask || task.Path != executable ||
		!reflect.DeepEqual(task.Args, wantArgs) {
		t.Fatalf("task = %#v", task)
	}
}

func TestRunNativeMountProbeRejectsInvalidInputAndReturnsWorkerFailure(t *testing.T) {
	runner := &recordingTaskRunner{}
	executable := "/Applications/FuseKit.app/Contents/MacOS/FuseKit"
	if err := runNativeMountProbe(t.Context(), runner, executable, "relative", testNativeProbeToken()); err == nil {
		t.Fatal("relative probe root succeeded")
	}
	if len(runner.tasks) != 0 {
		t.Fatalf("invalid root ran %d tasks", len(runner.tasks))
	}

	want := errors.New("probe failed")
	runner.err = want
	if err := runNativeMountProbe(t.Context(), runner, executable, "/Volumes/FuseKit", testNativeProbeToken()); !errors.Is(err, want) {
		t.Fatalf("runNativeMountProbe error = %v, want %v", err, want)
	}
	if err := runNativeMountProbe(t.Context(), nil, executable, "/Volumes/FuseKit", testNativeProbeToken()); err == nil {
		t.Fatal("nil runner succeeded")
	}
	if err := runNativeMountProbe(t.Context(), runner, "relative", "/Volumes/FuseKit", testNativeProbeToken()); err == nil {
		t.Fatal("relative executable succeeded")
	}
}

func TestRunChildDispatchesExactNativeProbeMode(t *testing.T) {
	config := mountmux.NativeProbeChildConfig{Root: t.TempDir(), Token: testNativeProbeToken()}
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
	runner := taskRunnerFunc(func(ctx context.Context, _ supervise.Task) error {
		close(entered)
		<-ctx.Done()
		<-release
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		result <- runNativeMountProbe(
			ctx, runner, "/Applications/FuseKit.app/Contents/MacOS/FuseKit",
			"/Volumes/FuseKit", testNativeProbeToken(),
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

type taskRunnerFunc func(context.Context, supervise.Task) error

func (f taskRunnerFunc) Run(ctx context.Context, task supervise.Task) error { return f(ctx, task) }
