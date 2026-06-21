package proc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeHolderEnv flips the spawned test binary into a fast-failing child, so the
// spawn-path tests exercise the real fork without the child recursively running
// this suite (and re-spawning grandchildren).
const fakeHolderEnv = "FUSEKIT_PROC_TEST_FAKE_HOLDER"

// childArgs is the argv a Spawn would pass for a stand-in subcommand. The
// package is consumer-agnostic, so the tests pick a representative argv.
var childArgs = func(socket string) []string { return []string{"mount-holder", "--socket", socket} }

// alwaysHost is a CanHost that permits the spawn (the fuse-build equivalent).
func alwaysHost() error { return nil }

// dialAvailable reports whether socket accepts a connection — the production
// Available probe a consumer supplies.
func dialAvailable(socket string) func() bool {
	return func() bool {
		conn, err := net.DialTimeout("unix", socket, 200*time.Millisecond)
		if err != nil {
			return false
		}
		conn.Close()
		return true
	}
}

// shortSockDir returns a fresh dir under /tmp for the socket: macOS caps
// sun_path at 104 bytes and t.TempDir() paths exceed it.
func shortSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-proc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// TestMain doubles as the spawned child when Spawn's real spawn path is under
// test: childCmd execs THIS test binary, and the env var turns it into a process
// that dies before ever serving the socket.
func TestMain(m *testing.M) {
	if os.Getenv(fakeHolderEnv) == "1" {
		fmt.Fprintln(os.Stderr, "fake holder: exiting without serving")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestChildCmd(t *testing.T) {
	socket := "/tmp/ccp-test/m.sock"
	logPath := filepath.Join(t.TempDir(), "holder.log")

	cmd, logFile, err := Spawn{Socket: socket, LogPath: logPath, Args: childArgs(socket)}.childCmd()
	if err != nil {
		t.Fatalf("childCmd: %v", err)
	}
	defer logFile.Close()

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	wantArgs := []string{exe, "mount-holder", "--socket", socket}
	if !reflect.DeepEqual(cmd.Args, wantArgs) {
		t.Errorf("argv = %q, want %q", cmd.Args, wantArgs)
	}
	if cmd.SysProcAttr == nil || !cmd.SysProcAttr.Setsid {
		t.Errorf("SysProcAttr = %+v, want Setsid", cmd.SysProcAttr)
	}
	if cmd.Stdin != nil {
		t.Errorf("Stdin = %v, want nil", cmd.Stdin)
	}
	if cmd.Stdout != logFile || cmd.Stderr != logFile {
		t.Errorf("Stdout/Stderr = %v/%v, want the log file %v", cmd.Stdout, cmd.Stderr, logFile)
	}
	if logFile.Name() != logPath {
		t.Errorf("log file = %q, want %q", logFile.Name(), logPath)
	}
	fi, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("stat log: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("log perms = %o, want 0600", got)
	}
}

func TestChildCmdUnopenableLog(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")
	socket := "/tmp/ccp-test/m.sock"
	if _, _, err := (Spawn{Socket: socket, LogPath: logPath, Args: childArgs(socket)}).childCmd(); err == nil {
		t.Fatal("childCmd with an unopenable log path succeeded, want error")
	}
}

func TestSpawnTimeoutDefault(t *testing.T) {
	if got := (Spawn{}).timeout(); got != DefaultSpawnTimeout {
		t.Errorf("zero Timeout = %v, want %v", got, DefaultSpawnTimeout)
	}
	if got := (Spawn{Timeout: time.Second}).timeout(); got != time.Second {
		t.Errorf("explicit Timeout = %v, want 1s", got)
	}
}

func TestEnsureRunningShortCircuitsWhenAvailable(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// The log path is deliberately unopenable: any spawn attempt would fail
	// loudly inside childCmd, and CanHost would panic if consulted — a nil
	// return proves the short-circuit ran before either.
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")
	err = Spawn{
		Socket:    socket,
		LogPath:   logPath,
		Args:      childArgs(socket),
		Timeout:   time.Second,
		Available: dialAvailable(socket),
		CanHost:   func() error { t.Fatal("CanHost consulted despite a live socket"); return nil },
	}.EnsureRunning()
	if err != nil {
		t.Fatalf("EnsureRunning with a live socket = %v, want nil", err)
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Errorf("log file stat = %v, want not-exist (no spawn)", err)
	}
}

// TestEnsureRunningCanHostRefusalUnwrapped pins that a CanHost refusal is
// returned as-is — never wrapped in ErrHolderUnavailable — and no spawn is
// attempted. The non-match is load-bearing: it drives a consumer's permanent
// retreat (vs. transient retry).
func TestEnsureRunningCanHostRefusalUnwrapped(t *testing.T) {
	refusal := errors.New("this binary cannot host: install the fuse build")
	socket := filepath.Join(shortSockDir(t), "m.sock") // nothing listening
	logPath := filepath.Join(t.TempDir(), "holder.log")

	err := Spawn{
		Socket:    socket,
		LogPath:   logPath,
		Args:      childArgs(socket),
		Timeout:   time.Second,
		Available: dialAvailable(socket),
		CanHost:   func() error { return refusal },
	}.EnsureRunning()
	if !errors.Is(err, refusal) {
		t.Errorf("error = %v, want the CanHost refusal returned as-is", err)
	}
	if errors.Is(err, ErrHolderUnavailable) {
		t.Errorf("error = %v, want the CanHost refusal NOT wrapped in ErrHolderUnavailable", err)
	}
	if _, statErr := os.Stat(logPath); !os.IsNotExist(statErr) {
		t.Errorf("log file stat = %v, want not-exist (no spawn attempted)", statErr)
	}
}

// TestEnsureRunningSpawnFailureClassifiedHolderUnavailable pins the spawn-leg
// failure class: a spawn that cannot even be assembled (here an unopenable log
// path inside childCmd) wraps ErrHolderUnavailable, not the CanHost refusal.
func TestEnsureRunningSpawnFailureClassifiedHolderUnavailable(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock") // nothing listening
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")

	err := Spawn{
		Socket:    socket,
		LogPath:   logPath,
		Args:      childArgs(socket),
		Timeout:   time.Second,
		Available: dialAvailable(socket),
		CanHost:   alwaysHost,
	}.EnsureRunning()
	if err == nil {
		t.Fatal("EnsureRunning with an unopenable log path succeeded, want error")
	}
	if !errors.Is(err, ErrHolderUnavailable) {
		t.Errorf("error = %v, want errors.Is ErrHolderUnavailable", err)
	}
}

func TestEnsureRunningSpawnTimesOutOnFastFailingChild(t *testing.T) {
	t.Setenv(fakeHolderEnv, "1") // the child (this test binary) dies before serving
	socket := filepath.Join(shortSockDir(t), "m.sock")
	logPath := filepath.Join(t.TempDir(), "holder.log")

	err := Spawn{
		Socket:    socket,
		LogPath:   logPath,
		Args:      childArgs(socket),
		Timeout:   time.Second,
		Available: dialAvailable(socket),
		CanHost:   alwaysHost,
	}.EnsureRunning()
	if err == nil {
		t.Fatal("EnsureRunning with a child that dies before serving succeeded, want timeout error")
	}
	if !errors.Is(err, ErrHolderUnavailable) {
		t.Errorf("error = %v, want errors.Is ErrHolderUnavailable", err)
	}
	if !strings.Contains(err.Error(), "did not come up on "+socket) {
		t.Errorf("error = %q, want the did-not-come-up copy naming the socket", err)
	}
	if !strings.Contains(err.Error(), "check "+logPath) {
		t.Errorf("error = %q, want it to point at the log %s", err, logPath)
	}
	// The spawn really happened with stderr redirected: the fake child's parting
	// line must land in the log (poll — the detached child races us).
	deadline := time.Now().Add(5 * time.Second)
	var logData []byte
	for time.Now().Before(deadline) {
		logData, _ = os.ReadFile(logPath)
		if strings.Contains(string(logData), "fake holder") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !strings.Contains(string(logData), "fake holder") {
		t.Errorf("child log = %q, want the fake child's stderr line", logData)
	}
}

// TestSpawnedChildReaped pins the zombie fix: a spawned child that exits must be
// reaped (waited out), not merely Released. A reaped child disappears from the
// process table — kill(pid, 0) reports ESRCH — while an unreaped one would sit
// as a signalable zombie until the spawner's process exits, so the poll below
// would never see ESRCH.
func TestSpawnedChildReaped(t *testing.T) {
	t.Setenv(fakeHolderEnv, "1") // the child (this test binary) exits immediately
	socket := filepath.Join(shortSockDir(t), "m.sock")
	logPath := filepath.Join(t.TempDir(), "holder.log")
	cmd, logFile, err := Spawn{Socket: socket, LogPath: logPath, Args: childArgs(socket)}.childCmd()
	if err != nil {
		t.Fatalf("childCmd: %v", err)
	}
	defer logFile.Close()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	pid := cmd.Process.Pid
	reap(cmd)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return // reaped: gone from the process table
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("spawned child pid %d still in the process table: exited child not reaped (zombie)", pid)
}

// writeExe writes content to a fresh executable file under dir and returns its
// path; modTime, when non-zero, stamps it so staleness checks are deterministic.
func writeExe(t *testing.T, dir, name, content string, modTime time.Time) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
		t.Fatalf("write %s: %v", p, err)
	}
	if !modTime.IsZero() {
		if err := os.Chtimes(p, modTime, modTime); err != nil {
			t.Fatalf("chtimes %s: %v", p, err)
		}
	}
	return p
}

func TestMaterializeStableExe(t *testing.T) {
	base := time.Date(2026, 6, 20, 12, 0, 0, 0, time.UTC)

	t.Run("fresh copy creates an executable matching src", func(t *testing.T) {
		srcDir, dstDir := t.TempDir(), t.TempDir()
		src := writeExe(t, srcDir, "src", "hello-holder", base)

		target, err := materializeStableExe(src, dstDir, "holder")
		if err != nil {
			t.Fatalf("materializeStableExe: %v", err)
		}
		if want := filepath.Join(dstDir, "holder"); target != want {
			t.Errorf("target = %q, want %q", target, want)
		}
		fi, err := os.Stat(target)
		if err != nil {
			t.Fatalf("stat target: %v", err)
		}
		if fi.Mode()&0o111 == 0 {
			t.Errorf("target mode = %v, want executable", fi.Mode())
		}
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("read target: %v", err)
		}
		if string(got) != "hello-holder" {
			t.Errorf("target content = %q, want %q", got, "hello-holder")
		}
	})

	t.Run("stale src is recopied", func(t *testing.T) {
		srcDir, dstDir := t.TempDir(), t.TempDir()
		src := writeExe(t, srcDir, "src", "v1", base)
		if _, err := materializeStableExe(src, dstDir, "holder"); err != nil {
			t.Fatalf("first materialize: %v", err)
		}
		// Newer + different size: a strictly stale source.
		writeExe(t, srcDir, "src", "v2-longer", base.Add(time.Hour))

		target, err := materializeStableExe(src, dstDir, "holder")
		if err != nil {
			t.Fatalf("second materialize: %v", err)
		}
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("read target: %v", err)
		}
		if string(got) != "v2-longer" {
			t.Errorf("target content = %q, want refreshed %q", got, "v2-longer")
		}
	})

	t.Run("up-to-date target is skipped", func(t *testing.T) {
		srcDir, dstDir := t.TempDir(), t.TempDir()
		src := writeExe(t, srcDir, "src", "same-bytes", base)

		target, err := materializeStableExe(src, dstDir, "holder")
		if err != nil {
			t.Fatalf("first materialize: %v", err)
		}
		before, err := os.Stat(target)
		if err != nil {
			t.Fatalf("stat target: %v", err)
		}
		beforeIno := before.Sys().(*syscall.Stat_t).Ino

		if _, err := materializeStableExe(src, dstDir, "holder"); err != nil {
			t.Fatalf("second materialize: %v", err)
		}
		after, err := os.Stat(target)
		if err != nil {
			t.Fatalf("re-stat target: %v", err)
		}
		afterIno := after.Sys().(*syscall.Stat_t).Ino
		// A skipped copy leaves the same inode and modtime: no rewrite happened.
		if afterIno != beforeIno {
			t.Errorf("inode = %d, want unchanged %d (no rewrite)", afterIno, beforeIno)
		}
		if !after.ModTime().Equal(before.ModTime()) {
			t.Errorf("modtime = %v, want unchanged %v (no rewrite)", after.ModTime(), before.ModTime())
		}
	})

	t.Run("same-size older different-content src is recopied", func(t *testing.T) {
		srcDir, dstDir := t.TempDir(), t.TempDir()
		src := writeExe(t, srcDir, "src", "AAAA", base)
		if _, err := materializeStableExe(src, dstDir, "holder"); err != nil {
			t.Fatalf("first materialize: %v", err)
		}
		// Same size (4 bytes), different content, and — the trap a size+mtime
		// heuristic would skip on — an OLDER modtime than the existing copy, as
		// a tar-preserved release mtime can be. A content compare still refreshes.
		writeExe(t, srcDir, "src", "BBBB", base.Add(-time.Hour))

		target, err := materializeStableExe(src, dstDir, "holder")
		if err != nil {
			t.Fatalf("second materialize: %v", err)
		}
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("read target: %v", err)
		}
		if string(got) != "BBBB" {
			t.Errorf("target content = %q, want refreshed %q", got, "BBBB")
		}
	})

	t.Run("existing target is replaced atomically", func(t *testing.T) {
		srcDir, dstDir := t.TempDir(), t.TempDir()
		src := writeExe(t, srcDir, "src", "new-content", base.Add(time.Hour))
		// A pre-existing, different, OLDER target must be overwritten.
		writeExe(t, dstDir, "holder", "old-content", base)

		target, err := materializeStableExe(src, dstDir, "holder")
		if err != nil {
			t.Fatalf("materializeStableExe: %v", err)
		}
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("read target: %v", err)
		}
		if string(got) != "new-content" {
			t.Errorf("target content = %q, want replaced %q", got, "new-content")
		}
	})

	t.Run("missing src is a wrapped error", func(t *testing.T) {
		if _, err := materializeStableExe(filepath.Join(t.TempDir(), "nope"), t.TempDir(), "holder"); err == nil {
			t.Fatal("materializeStableExe with a missing source succeeded, want error")
		}
	})
}

func TestChildExeName(t *testing.T) {
	cases := []struct {
		id   string
		args []string
		want string
	}{
		{id: "subcommand argv", args: []string{"n", "--socket", "x"}, want: "n"},
		{id: "nil argv falls back", args: nil, want: "holder"},
		{id: "path is based", args: []string{"/a/b/c"}, want: "c"},
		{id: "empty first arg falls back", args: []string{""}, want: "holder"},
	}
	for _, tc := range cases {
		t.Run(tc.id, func(t *testing.T) {
			if got := childExeName(tc.args); got != tc.want {
				t.Errorf("childExeName(%q) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}
