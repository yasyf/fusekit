package holder

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
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
	root := "/Users/test/.cc-pool/accounts"
	if err := runNativeMountProbe(t.Context(), runner, root); err != nil {
		t.Fatalf("runNativeMountProbe: %v", err)
	}
	if len(runner.tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(runner.tasks))
	}
	task := runner.tasks[0]
	if task.RecoveryClass != proc.RecoveryTask || task.Path != nativeProbeExecutable ||
		!reflect.DeepEqual(task.Args, []string{"-A", "--", root}) {
		t.Fatalf("task = %#v", task)
	}
}

func TestRunNativeMountProbeRejectsInvalidInputAndReturnsWorkerFailure(t *testing.T) {
	runner := &recordingTaskRunner{}
	if err := runNativeMountProbe(t.Context(), runner, "relative"); err == nil {
		t.Fatal("relative probe root succeeded")
	}
	if len(runner.tasks) != 0 {
		t.Fatalf("invalid root ran %d tasks", len(runner.tasks))
	}

	want := errors.New("probe failed")
	runner.err = want
	if err := runNativeMountProbe(t.Context(), runner, "/Volumes/FuseKit"); !errors.Is(err, want) {
		t.Fatalf("runNativeMountProbe error = %v, want %v", err, want)
	}
	if err := runNativeMountProbe(t.Context(), nil, "/Volumes/FuseKit"); err == nil {
		t.Fatal("nil runner succeeded")
	}
}
