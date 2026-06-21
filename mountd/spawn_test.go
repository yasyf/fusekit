package mountd

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

	"github.com/yasyf/fusekit"
)

// fakeHolderEnv flips the spawned test binary into a fast-failing holder, so
// TestEnsureRunningSpawnTimesOut can exercise the real fork without the child
// recursively running this suite (and re-spawning grandchildren).
const fakeHolderEnv = "FUSEKIT_MOUNTD_TEST_FAKE_HOLDER"

// holderArgs is the argv a Spawn would pass for a stand-in holder subcommand.
// The package is consumer-agnostic, so the tests pick a representative argv.
var holderArgs = func(socket string) []string { return []string{"mount-holder", "--socket", socket} }

// testHostHint is the pure-build refusal guidance a consumer would supply; the
// ErrCannotHost test asserts it survives onto the error.
const testHostHint = "install fuse-t (brew install macos-fuse-t/cask/fuse-t) then reinstall to get the fuse build"

// TestMain doubles as the spawned mount-holder when Spawn's real spawn path is
// under test: holderCmd execs THIS test binary, and the env var turns it into a
// holder that dies before ever serving the socket.
func TestMain(m *testing.M) {
	if os.Getenv(fakeHolderEnv) == "1" {
		fmt.Fprintln(os.Stderr, "fake holder: exiting without serving")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestHolderCmd(t *testing.T) {
	socket := "/tmp/ccp-test/m.sock"
	logPath := filepath.Join(t.TempDir(), "holder.log")

	cmd, logFile, err := Spawn{Socket: socket, LogPath: logPath, Args: holderArgs(socket)}.holderCmd()
	if err != nil {
		t.Fatalf("holderCmd: %v", err)
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

func TestHolderCmdUnopenableLog(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")
	socket := "/tmp/ccp-test/m.sock"
	if _, _, err := (Spawn{Socket: socket, LogPath: logPath, Args: holderArgs(socket)}).holderCmd(); err == nil {
		t.Fatal("holderCmd with an unopenable log path succeeded, want error")
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
	// loudly inside holderCmd, so a nil return proves the short-circuit.
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")
	if err := (Spawn{Socket: socket, LogPath: logPath, Args: holderArgs(socket), Timeout: time.Second}).EnsureRunning(); err != nil {
		t.Fatalf("EnsureRunning with a live socket = %v, want nil", err)
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Errorf("log file stat = %v, want not-exist (no spawn)", err)
	}
}

// TestEnsureRunningPureBuildErrCannotHost pins the pure-build refusal: with no
// fuse host built in (fusekit.Built()==false — the case for the untagged mountd
// suite), EnsureRunning refuses with ErrCannotHost, carries the consumer hint,
// and — load-bearing — does NOT errors.Is-match ErrHolderUnavailable, since the
// non-match is what drives a consumer's permanent retreat (vs. transient retry)
// and must never be confused.
func TestEnsureRunningPureBuildErrCannotHost(t *testing.T) {
	if fusekit.Built() {
		t.Skip("fuse build can spawn a holder; the ErrCannotHost refusal is pure-build only")
	}
	socket := filepath.Join(shortSockDir(t), "m.sock") // nothing listening
	logPath := filepath.Join(t.TempDir(), "holder.log")

	err := (Spawn{Socket: socket, LogPath: logPath, Args: holderArgs(socket), Timeout: time.Second, CannotHostHint: testHostHint}).EnsureRunning()
	if err == nil {
		t.Fatal("EnsureRunning in a pure build with no holder succeeded, want error")
	}
	if !errors.Is(err, ErrCannotHost) {
		t.Errorf("error = %v, want errors.Is ErrCannotHost", err)
	}
	// Deliberately NOT ErrHolderUnavailable: a binary that can never host or
	// spawn a holder is a permanent condition.
	if errors.Is(err, ErrHolderUnavailable) {
		t.Errorf("error = %v, want the pure-build refusal NOT classified as holder-unavailable", err)
	}
	if !strings.Contains(err.Error(), testHostHint) {
		t.Errorf("error = %q, want the consumer's cannot-host hint %q", err, testHostHint)
	}
	if _, statErr := os.Stat(logPath); !os.IsNotExist(statErr) {
		t.Errorf("log file stat = %v, want not-exist (no spawn attempted)", statErr)
	}
}

// TestEnsureRunningSpawnFailureClassifiedHolderUnavailable pins the spawn-leg
// failure class: a spawn that cannot even be assembled (here an unopenable log
// path inside holderCmd) is a holder-availability condition, not a mount
// verdict.
func TestEnsureRunningSpawnFailureClassifiedHolderUnavailable(t *testing.T) {
	if !fusekit.Built() {
		t.Skip("pure build refuses before reaching holderCmd; the spawn leg is fuse-build only")
	}
	socket := filepath.Join(shortSockDir(t), "m.sock") // nothing listening
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")

	err := (Spawn{Socket: socket, LogPath: logPath, Args: holderArgs(socket), Timeout: time.Second}).EnsureRunning()
	if err == nil {
		t.Fatal("EnsureRunning with an unopenable log path succeeded, want error")
	}
	if !errors.Is(err, ErrHolderUnavailable) {
		t.Errorf("error = %v, want errors.Is ErrHolderUnavailable", err)
	}
}

// TestSpawnedHolderReaped pins the zombie fix: a spawned holder that exits
// must be reaped (waited out), not merely Released. A reaped child disappears
// from the process table — kill(pid, 0) reports ESRCH — while an unreaped one
// would sit as a signalable zombie until this (in production: the spawner's)
// process exits, so the poll below would never see ESRCH.
func TestSpawnedHolderReaped(t *testing.T) {
	t.Setenv(fakeHolderEnv, "1") // the child (this test binary) exits immediately
	socket := filepath.Join(shortSockDir(t), "m.sock")
	logPath := filepath.Join(t.TempDir(), "holder.log")
	cmd, logFile, err := Spawn{Socket: socket, LogPath: logPath, Args: holderArgs(socket)}.holderCmd()
	if err != nil {
		t.Fatalf("holderCmd: %v", err)
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
	t.Fatalf("spawned holder pid %d still in the process table: exited child not reaped (zombie)", pid)
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

func TestHolderExeName(t *testing.T) {
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
			if got := holderExeName(tc.args); got != tc.want {
				t.Errorf("holderExeName(%q) = %q, want %q", tc.args, got, tc.want)
			}
		})
	}
}

func TestEnsureRunningSpawnTimesOutOnFastFailingHolder(t *testing.T) {
	if !fusekit.Built() {
		t.Skip("pure build refuses before spawning; the real spawn path is fuse-build only")
	}
	t.Setenv(fakeHolderEnv, "1") // the child (this test binary) dies before serving
	socket := filepath.Join(shortSockDir(t), "m.sock")
	logPath := filepath.Join(t.TempDir(), "holder.log")

	err := (Spawn{Socket: socket, LogPath: logPath, Args: holderArgs(socket), Timeout: time.Second}).EnsureRunning()
	if err == nil {
		t.Fatal("EnsureRunning with a holder that dies before serving succeeded, want timeout error")
	}
	// The timeout is a holder-availability condition, never a mount verdict:
	// without the sentinel, healFuse's default arm would auto-convert a fuse
	// account to symlink on a slow holder start.
	if !errors.Is(err, ErrHolderUnavailable) {
		t.Errorf("error = %v, want errors.Is ErrHolderUnavailable", err)
	}
	if !strings.Contains(err.Error(), "did not come up on "+socket) {
		t.Errorf("error = %q, want the did-not-come-up copy naming the socket", err)
	}
	if !strings.Contains(err.Error(), "check "+logPath) {
		t.Errorf("error = %q, want it to point at the log %s", err, logPath)
	}
	// The spawn really happened with stderr redirected: the fake holder's
	// parting line must land in the log (poll — the detached child races us).
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
		t.Errorf("holder log = %q, want the fake holder's stderr line", logData)
	}
}
