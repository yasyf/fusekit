package mountd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
)

// fakeHolderEnv flips the spawned test binary into a fast-failing holder, so
// TestEnsureRunningSpawnTimesOut can exercise the real fork without the child
// recursively running this suite (and re-spawning grandchildren).
const fakeHolderEnv = "FUSEKIT_MOUNTD_TEST_FAKE_HOLDER"

// holderArgs is a representative holder argv; the package is consumer-agnostic.
var holderArgs = func(socket string) []string { return []string{"mount-holder", "--socket", socket} }

// testHostHint is a consumer's cannot-host hint; the ErrCannotHost test asserts
// it survives onto the error.
const testHostHint = "install fuse-t (brew install macos-fuse-t/cask/fuse-t) then reinstall to get the fuse build"

// TestMain doubles as the spawned mount-holder when Spawn's real spawn path is
// under test: the spawn execs THIS test binary, and the env var turns it into a
// holder that dies before ever serving the socket.
func TestMain(m *testing.M) {
	if os.Getenv(fakeHolderEnv) == "1" {
		fmt.Fprintln(os.Stderr, "fake holder: exiting without serving")
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestEnsureRunningShortCircuitsWhenAvailable(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	// The log path is deliberately unopenable: any spawn attempt would fail
	// loudly inside the spawn, so a nil return proves the short-circuit.
	logPath := filepath.Join(t.TempDir(), "missing", "holder.log")
	if err := (Spawn{Socket: socket, LogPath: logPath, Args: holderArgs(socket), Timeout: time.Second}).EnsureRunning(); err != nil {
		t.Fatalf("EnsureRunning with a live socket = %v, want nil", err)
	}
	if _, err := os.Stat(logPath); !os.IsNotExist(err) {
		t.Errorf("log file stat = %v, want not-exist (no spawn)", err)
	}
}

// TestEnsureRunningPureBuildErrCannotHost pins the pure-build refusal (no fuse
// host built in, fusekit.Built()==false): EnsureRunning refuses with
// ErrCannotHost carrying the consumer hint, and — load-bearing — does NOT
// errors.Is-match ErrHolderUnavailable, since that non-match drives a consumer's
// permanent retreat (vs. transient retry) and must never be confused.
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
// failure class: a spawn that cannot be assembled (here an unopenable log path)
// is a holder-availability condition, not a mount verdict.
func TestEnsureRunningSpawnFailureClassifiedHolderUnavailable(t *testing.T) {
	if !fusekit.Built() {
		t.Skip("pure build refuses before reaching the spawn; the spawn leg is fuse-build only")
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

func TestAppBundle(t *testing.T) {
	cases := []struct{ name, exec, want string }{
		{"cask holder inner binary", HolderExe, HolderApp},
		{"explicit bundle inner exe", "/Applications/Foo.app/Contents/MacOS/foo", "/Applications/Foo.app"},
		{"nested .app picks the nearest", "/a/Outer.app/Contents/Inner.app/Contents/MacOS/x", "/a/Outer.app/Contents/Inner.app"},
		{"bare dev binary has no bundle", "/Users/me/code/fusekit/holder", ""},
		{"empty path", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := appBundle(tc.exec); got != tc.want {
				t.Errorf("appBundle(%q) = %q, want %q", tc.exec, got, tc.want)
			}
		})
	}
}

// TestEnsureRunningAppBundleLaunchesViaOpenG pins the cask-holder launch path: an
// ExecPath inside a .app bundle must be started via the LaunchServices seam
// (`open -g` the BUNDLE), never direct-exec'd as its inner Mach-O — a launchd
// daemon's direct exec runs outside the GUI session and never comes up.
func TestEnsureRunningAppBundleLaunchesViaOpenG(t *testing.T) {
	bundle := filepath.Join(t.TempDir(), "fusekit-holder.app")
	exe := filepath.Join(bundle, "Contents", "MacOS", "fusekit-holder")
	if err := os.MkdirAll(filepath.Dir(exe), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, []byte("dev stub"), 0o755); err != nil {
		t.Fatal(err)
	}

	socket := filepath.Join(shortSockDir(t), "m.sock")
	logPath := filepath.Join(t.TempDir(), "holder.log")

	var launchedWith string
	orig := launchApp
	launchApp = func(_ context.Context, app string) error {
		launchedWith = app
		ln, err := net.Listen("unix", socket) // the "holder" binds its socket
		if err != nil {
			return err
		}
		t.Cleanup(func() { _ = ln.Close() })
		return nil
	}
	defer func() { launchApp = orig }()

	err := (Spawn{Socket: socket, LogPath: logPath, Args: holderArgs(socket), ExecPath: exe, Timeout: 2 * time.Second}).EnsureRunning()
	if err != nil {
		t.Fatalf("EnsureRunning(.app holder) = %v, want nil", err)
	}
	if launchedWith != bundle {
		t.Errorf("launchApp launched %q, want the .app BUNDLE %q (open -g the bundle, not the inner exe)", launchedWith, bundle)
	}
	if _, statErr := os.Stat(logPath); !os.IsNotExist(statErr) {
		t.Errorf("holder log stat = %v, want not-exist (open -g, never a direct-exec spawn)", statErr)
	}
}

// TestEnsureRunningAppBundleRefusesWhenCaskMissing pins that the .app path still
// honors canHost: a missing bundle ExecPath (cask not installed) refuses with
// ErrCannotHost before any launch.
func TestEnsureRunningAppBundleRefusesWhenCaskMissing(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "fusekit-holder.app", "Contents", "MacOS", "fusekit-holder") // never created
	socket := filepath.Join(shortSockDir(t), "m.sock")

	launched := false
	orig := launchApp
	launchApp = func(_ context.Context, _ string) error { launched = true; return nil }
	defer func() { launchApp = orig }()

	err := (Spawn{Socket: socket, Args: holderArgs(socket), ExecPath: exe, CannotHostHint: testHostHint, Timeout: time.Second}).EnsureRunning()
	if !errors.Is(err, ErrCannotHost) {
		t.Errorf("error = %v, want errors.Is ErrCannotHost (uninstalled cask)", err)
	}
	if launched {
		t.Error("launchApp ran despite a missing cask; canHost must gate the launch")
	}
}
