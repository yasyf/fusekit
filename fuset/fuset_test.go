package fuset

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
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

func TestInstallRequiresToolPoolAndPropagatesCancellation(t *testing.T) {
	if err := Install(t.Context(), nil, nil, nil); err == nil {
		t.Fatal("Install accepted a nil FUSE tool pool")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	runner := &installRunner{}
	if err := install(ctx, runner, "/opt/homebrew/bin/brew", nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Install cancellation = %v, want context canceled", err)
	}
	if runner.calls != 1 || !slices.Equal(runner.request.Args, []string{"install", "-y", "--cask", Cask}) {
		t.Fatalf("Install task = calls %d args %v", runner.calls, runner.request.Args)
	}
	if runner.request.Dir != "/" || runner.timeout != installTotalTimeout {
		t.Fatalf("Install runner policy = dir %q timeout %s", runner.request.Dir, runner.timeout)
	}
}

func TestInstallBoundsEachOutputStream(t *testing.T) {
	t.Setenv("CGOFUSE_LIBFUSE_PATH", "/tmp/foreign-libfuse.dylib")
	payload := bytes.Repeat([]byte("x"), installOutputLimit+1)
	runner := installRunner{run: func(daemonkit.Cmd) (daemonkit.RunResult, error) {
		return daemonkit.RunResult{Stdout: payload, Stderr: payload}, nil
	}}
	var stdout, stderr bytes.Buffer
	err := install(context.Background(), &runner, "/opt/homebrew/bin/brew", &stdout, &stderr)
	if !errors.Is(err, errInstallOutputLimit) {
		t.Fatalf("Install output overflow = %v", err)
	}
	if stdout.Len() != installOutputLimit || stderr.Len() != installOutputLimit {
		t.Fatalf("bounded output lengths = %d, %d", stdout.Len(), stderr.Len())
	}
	if runner.request.Path != "/opt/homebrew/bin/brew" || runner.request.Dir != "/" ||
		runner.timeout != installTotalTimeout {
		t.Fatalf("Install request = %+v", runner.request)
	}
	for _, entry := range runner.request.Env {
		if strings.HasPrefix(entry, "PATH=") || strings.HasPrefix(entry, "LANG=") ||
			strings.HasPrefix(entry, "CGOFUSE_LIBFUSE_PATH=") {
			t.Fatalf("install runner inherited reserved environment: %v", runner.request.Env)
		}
	}
}

func TestInstallMapsTruncatedOutputAndPreservesCapturedOutput(t *testing.T) {
	runner := installRunner{run: func(daemonkit.Cmd) (daemonkit.RunResult, error) {
		return daemonkit.RunResult{Stdout: []byte("partial")}, daemonkit.ErrTruncated
	}}
	var stdout bytes.Buffer
	err := install(context.Background(), &runner, "/opt/homebrew/bin/brew", &stdout, nil)
	if !errors.Is(err, daemonkit.ErrTruncated) || !errors.Is(err, errInstallOutputLimit) {
		t.Fatalf("Install truncated output = %v", err)
	}
	if stdout.String() != "partial" {
		t.Fatalf("captured stdout = %q", stdout.String())
	}
}

type installRunner struct {
	calls   int
	request daemonkit.Cmd
	timeout time.Duration
	run     func(daemonkit.Cmd) (daemonkit.RunResult, error)
}

func (r *installRunner) Run(ctx context.Context, request daemonkit.Cmd) (daemonkit.RunResult, error) {
	r.calls++
	r.request = request
	deadline, ok := ctx.Deadline()
	if ok {
		r.timeout = time.Until(deadline).Round(time.Second)
	}
	if r.run != nil {
		return r.run(request)
	}
	return daemonkit.RunResult{}, ctx.Err()
}
