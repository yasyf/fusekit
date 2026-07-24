package fuset

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/worker"
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
	runner := &installWorkerRunner{}
	if err := Install(ctx, runner, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Install cancellation = %v, want context canceled", err)
	}
	if runner.calls != 1 || !slices.Equal(runner.request.Args, []string{"install", "-y", "--cask", Cask}) {
		t.Fatalf("Install task = calls %d args %v", runner.calls, runner.request.Args)
	}
	if runner.request.Dir != "/" || runner.request.TotalTimeout != installTotalTimeout {
		t.Fatalf("Install worker policy = dir %q timeout %s", runner.request.Dir, runner.request.TotalTimeout)
	}
}

func TestInstallBoundsEachOutputStream(t *testing.T) {
	t.Setenv("CGOFUSE_LIBFUSE_PATH", "/tmp/foreign-libfuse.dylib")
	payload := bytes.Repeat([]byte("x"), installOutputLimit+1)
	runner := installWorkerRunner{run: func(worker.CommandRequest) (worker.CommandResult, error) {
		return worker.CommandResult{Stdout: payload, Stderr: payload}, nil
	}}
	var stdout, stderr bytes.Buffer
	err := install(t.Context(), &runner, "/opt/homebrew/bin/brew", &stdout, &stderr)
	if !errors.Is(err, errInstallOutputLimit) {
		t.Fatalf("Install output overflow = %v", err)
	}
	if stdout.Len() != installOutputLimit || stderr.Len() != installOutputLimit {
		t.Fatalf("bounded output lengths = %d, %d", stdout.Len(), stderr.Len())
	}
	if runner.request.Path != "/opt/homebrew/bin/brew" || runner.request.Dir != "/" ||
		runner.request.TotalTimeout != installTotalTimeout {
		t.Fatalf("Install request = %+v", runner.request)
	}
	for _, entry := range runner.request.Env {
		if strings.HasPrefix(entry, "PATH=") || strings.HasPrefix(entry, "LANG=") ||
			strings.HasPrefix(entry, "CGOFUSE_LIBFUSE_PATH=") {
			t.Fatalf("install worker inherited reserved environment: %v", runner.request.Env)
		}
	}
}

func TestInstallMapsWorkerOutputLimitAndPreservesCapturedOutput(t *testing.T) {
	runner := installWorkerRunner{run: func(worker.CommandRequest) (worker.CommandResult, error) {
		return worker.CommandResult{Stdout: []byte("partial")}, worker.ErrOutputLimit
	}}
	var stdout bytes.Buffer
	err := install(t.Context(), &runner, "/opt/homebrew/bin/brew", &stdout, nil)
	if !errors.Is(err, worker.ErrOutputLimit) || !errors.Is(err, errInstallOutputLimit) {
		t.Fatalf("Install worker output limit = %v", err)
	}
	if stdout.String() != "partial" {
		t.Fatalf("captured stdout = %q", stdout.String())
	}
}

type installWorkerRunner struct {
	calls   int
	request worker.CommandRequest
	run     func(worker.CommandRequest) (worker.CommandResult, error)
}

func (r *installWorkerRunner) Run(ctx context.Context, request worker.CommandRequest) (worker.CommandResult, error) {
	r.calls++
	r.request = request
	if r.run != nil {
		return r.run(request)
	}
	return worker.CommandResult{}, ctx.Err()
}
