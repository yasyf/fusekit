package mountd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/proc"
)

// DefaultSpawnTimeout re-exports proc.DefaultSpawnTimeout: the spawn-wait bound
// a zero Spawn.Timeout / RemoteHost.SpawnTimeout falls back to. It stays in the
// mountd surface so consumers that named it keep compiling.
const DefaultSpawnTimeout = proc.DefaultSpawnTimeout

// appLaunchTimeout bounds waiting for a cask .app holder to bind its socket after
// an `open -g` launch. It is well above proc.DefaultSpawnTimeout (5s) because the
// first LaunchServices launch absorbs Gatekeeper assessment of the freshly
// installed, notarized bundle before the holder's serve loop ever runs.
const appLaunchTimeout = 20 * time.Second

// ErrCannotHost is the pure-build (no fuse tag) refusal: this binary has no
// in-process fuse host, so it can neither serve mounts itself nor spawn a holder
// that could. It is a DISTINCT sentinel that must never errors.Is-match
// ErrHolderUnavailable — and is never wrapped in it. A could-not-reach-a-holder
// condition (ErrHolderUnavailable) is transient and drives retry; a binary that
// can never host is permanent and drives a consumer's gated retreat (cc-pool's
// fuse→symlink fallback, cc-notes' exit-code mapping). Collapsing the two would
// make additive holder blips trigger the one irreversible action.
var ErrCannotHost = errors.New("this binary cannot host fuse mounts")

// Spawn ensures a detached mount holder is serving a socket, auto-spawning one
// (in its own session) and waiting for its socket to come up. The consumer
// supplies the holder argv (Args), so one Spawn shape drives any consumer's
// `<binary> mount-holder --socket <sock>` subcommand. A running holder is usable
// by ANY build — the mounts live in the holder process — so only the spawn path
// requires the fuse build (fusekit.Built); a second spawn racing a starting
// holder is harmless, since the holder refuses to start if the socket is owned.
type Spawn struct {
	// Socket is the holder's unix socket path.
	Socket string
	// LogPath receives a spawned holder's stdout and stderr.
	LogPath string
	// Args is the spawned process's argv after the executable, e.g.
	// ["mount-holder", "--socket", socket]. The consumer owns the subcommand
	// name and flag spelling.
	Args []string
	// Timeout bounds waiting for a freshly spawned holder's socket. Zero means
	// DefaultSpawnTimeout.
	Timeout time.Duration
	// CannotHostHint is the user-facing guidance appended to ErrCannotHost on a
	// pure-build refusal (each consumer's brew/install text).
	CannotHostHint string
	// StableExecDir, when non-empty, makes the holder binary materialize as a
	// copy under this directory and spawn from there instead of os.Executable()
	// directly; this gives the holder a stable resolved path so the macOS
	// volume-access TCC grant survives version upgrades (the embedded
	// Developer-ID designated requirement survives the copy). Empty preserves
	// the os.Executable() default.
	StableExecDir string
	// ExecPath, when set, is the holder binary the child execs (forwarded to
	// proc.Spawn); canHost then gates on it existing, not fusekit.Built().
	ExecPath string
}

// EnsureRunning makes sure a holder serves Socket, returning nil once one is
// reachable. If none is, a pure build refuses with ErrCannotHost (carrying
// CannotHostHint) — deliberately NOT wrapped in ErrHolderUnavailable — while a
// fuse build spawns a detached holder and waits up to the timeout.
//
// Failure classes: every could-not-start-or-reach-a-holder leg (a spawn that
// fails to assemble/start, or whose socket never comes up) wraps
// ErrHolderUnavailable — a holder-availability condition, never a mount verdict,
// so drivers retry instead of converting the account. The pure-build refusal
// alone is unwrapped (ErrCannotHost): a binary that can never host or spawn a
// holder is a permanent condition.
func (s Spawn) EnsureRunning() error {
	cl := NewClient(s.Socket)
	ps := proc.Spawn{
		Socket:        s.Socket,
		LogPath:       s.LogPath,
		Args:          s.Args,
		Timeout:       s.Timeout,
		StableExecDir: s.StableExecDir,
		ExecPath:      s.ExecPath,
		Available:     cl.Available,
		CanHost:       s.canHost,
	}
	// A cask holder lives inside a signed .app bundle and MUST be launched via
	// LaunchServices (`open -g`), never direct-exec'd: a launchd-daemon exec of
	// the bundle's inner Mach-O runs outside the user GUI session that fuse-t's
	// volume bring-up + the OS volume-access grant need, and never comes up. A dev
	// holder (a bare `go build ./cmd/holder` binary, or os.Executable()) has no
	// .app ancestor and keeps proc.Spawn's direct-exec + Setsid path.
	if app := appBundle(s.ExecPath); app != "" {
		ps.Override = func() error { return s.launchAppHolder(cl, app) }
	}
	return ps.EnsureRunning()
}

// launchAppHolder is the proc.Spawn Override for a cask .app holder: gate on the
// cask being installed (canHost), `open -g` the bundle, then poll the socket
// until it binds or appLaunchTimeout elapses. proc.Spawn already short-circuited
// on Available, so a holder already serving is never relaunched — multi-tenant:
// a consumer must never version-replace a holder another consumer shares.
//
// `open -g` launches the bundle with NO argv (Args is ignored on this path), so
// the cask holder binds its DefaultHolderSocket; a .app holder therefore requires
// Spawn.Socket == DefaultHolderSocket() (cc-pool sets exactly that).
func (s Spawn) launchAppHolder(cl *Client, app string) error {
	if err := s.canHost(); err != nil {
		return err
	}
	if err := launchApp(context.Background(), app); err != nil {
		return fmt.Errorf("%w: launch holder app %s: %w", proc.ErrChildUnavailable, app, err)
	}
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = appLaunchTimeout
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cl.Available() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%w: holder app %s did not bind %s within %s", proc.ErrChildUnavailable, app, s.Socket, timeout)
}

// launchApp is the LaunchServices `open -g` seam, a var so a spawn test records
// the launch (and binds a canned holder) without shelling out to a real app.
var launchApp = proc.LaunchApp

// appBundle returns the nearest ancestor of execPath that ends in ".app", or ""
// when execPath is not inside an app bundle (a dev bare-binary holder). The cask
// holder's inner Mach-O is at <bundle>/Contents/MacOS/<exe>.
func appBundle(execPath string) string {
	if execPath == "" {
		return ""
	}
	for p := execPath; ; {
		dir := filepath.Dir(p)
		if dir == p {
			return ""
		}
		if strings.HasSuffix(dir, ".app") {
			return dir
		}
		p = dir
	}
}

// canHost gates the spawn on ExecPath existing when set, else on fusekit.Built();
// the pure-build refusal is ErrCannotHost, unwrapped for the consumer's retreat.
func (s Spawn) canHost() error {
	if s.ExecPath != "" {
		if _, err := os.Stat(s.ExecPath); err != nil {
			return fmt.Errorf("%w: holder not installed at %s (try `brew install --cask %s`): %s", ErrCannotHost, s.ExecPath, HolderCask, s.CannotHostHint)
		}
		return nil
	}
	if fusekit.Built() {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrCannotHost, s.CannotHostHint)
}
