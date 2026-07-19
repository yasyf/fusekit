package fuset

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

func TestInstalledStatsThePath(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "libfuse-t.dylib")
	if err := os.WriteFile(present, []byte("stub"), 0o644); err != nil {
		t.Fatal(err)
	}
	absent := filepath.Join(dir, "missing.dylib")

	if !installed(present) {
		t.Errorf("installed(%q) = false, want true for an existing file", present)
	}
	if installed(absent) {
		t.Errorf("installed(%q) = true, want false for a missing file", absent)
	}
}

func TestInstalledBrokenSymlinkIsAbsent(t *testing.T) {
	dir := t.TempDir()
	link := filepath.Join(dir, "libfuse-t.dylib")
	if err := os.Symlink(filepath.Join(dir, "nonexistent"), link); err != nil {
		t.Fatal(err)
	}
	if installed(link) {
		t.Errorf("installed(broken symlink) = true, want false")
	}
}

func TestConstantsAreTheFuseTFacts(t *testing.T) {
	if Cask != "macos-fuse-t/homebrew-cask/fuse-t" {
		t.Errorf("Cask = %q", Cask)
	}
	if CaskVersion != "1.2.7" {
		t.Errorf("CaskVersion = %q", CaskVersion)
	}
	if CaskDylib != "/usr/local/lib/libfuse-t-1.2.7.dylib" {
		t.Errorf("CaskDylib = %q", CaskDylib)
	}
}

func TestInstallRequiresHolderRunnerAndPropagatesCancellation(t *testing.T) {
	if err := Install(t.Context(), nil, nil, nil); err == nil {
		t.Fatal("Install accepted a nil disposable task runner")
	}
	if _, err := exec.LookPath("brew"); err != nil {
		t.Skipf("brew unavailable: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	runner := &installTaskRunner{}
	if err := Install(ctx, runner, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Install cancellation = %v, want context canceled", err)
	}
	if runner.calls != 1 || !slices.Equal(runner.task.Args, []string{"install", "-y", "--cask", Cask}) {
		t.Fatalf("Install task = calls %d args %v", runner.calls, runner.task.Args)
	}
	if runner.task.RecoveryClass != proc.RecoveryTask {
		t.Fatalf("Install recovery class = %d, want task", runner.task.RecoveryClass)
	}
}

func TestInstallBoundsEachOutputStream(t *testing.T) {
	t.Setenv("CGOFUSE_LIBFUSE_PATH", "/tmp/foreign-libfuse.dylib")
	payload := bytes.Repeat([]byte("x"), installOutputLimit+1)
	runner := installTaskRunner{run: func(task supervise.Task) error {
		if _, err := task.Stdout.Write(payload); err != nil {
			return err
		}
		_, err := task.Stderr.Write(payload)
		return err
	}}
	var stdout, stderr bytes.Buffer
	err := install(t.Context(), &runner, "/opt/homebrew/bin/brew", &stdout, &stderr)
	if !errors.Is(err, errInstallOutputLimit) {
		t.Fatalf("Install output overflow = %v", err)
	}
	if stdout.Len() != installOutputLimit || stderr.Len() != installOutputLimit {
		t.Fatalf("bounded output lengths = %d, %d", stdout.Len(), stderr.Len())
	}
	if runner.task.Path != "/opt/homebrew/bin/brew" || runner.task.RecoveryClass != proc.RecoveryTask {
		t.Fatalf("Install task = %+v", runner.task)
	}
	if slices.Contains(runner.task.Env, "CGOFUSE_LIBFUSE_PATH=/tmp/foreign-libfuse.dylib") {
		t.Fatalf("install task inherited native-only environment: %v", runner.task.Env)
	}
}

type installTaskRunner struct {
	calls int
	task  supervise.Task
	run   func(supervise.Task) error
}

func (r *installTaskRunner) Run(ctx context.Context, task supervise.Task) error {
	r.calls++
	r.task = task
	if r.run != nil {
		return r.run(task)
	}
	return ctx.Err()
}
