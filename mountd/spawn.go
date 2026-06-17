package mountd

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/yasyf/fusekit"
)

// DefaultSpawnTimeout bounds how long callers wait for a freshly spawned
// holder's socket to come up.
const DefaultSpawnTimeout = 5 * time.Second

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
	if cl.Available() {
		return nil
	}
	if !fusekit.Built() {
		return fmt.Errorf("%w: %s", ErrCannotHost, s.CannotHostHint)
	}
	cmd, logFile, err := s.holderCmd()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrHolderUnavailable, err)
	}
	// The child holds its own descriptor once started; this one is ours.
	defer logFile.Close()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%w: spawn mount holder: %w", ErrHolderUnavailable, err)
	}
	reap(cmd)

	timeout := s.timeout()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cl.Available() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%w: mount holder did not come up on %s within %s; check %s", ErrHolderUnavailable, s.Socket, timeout, s.LogPath)
}

// timeout resolves the spawn-wait bound, defaulting a zero Timeout to
// DefaultSpawnTimeout.
func (s Spawn) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return DefaultSpawnTimeout
}

// holderCmd builds the detached mount-holder command: this same binary run with
// Args in its own session, stdout and stderr appended to LogPath.
func (s Spawn) holderCmd() (*exec.Cmd, *os.File, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, nil, fmt.Errorf("resolve executable: %w", err)
	}
	logFile, err := os.OpenFile(s.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open mount holder log: %w", err)
	}
	cmd := exec.Command(exe, s.Args...)
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach from our session
	return cmd, logFile, nil
}

// reap waits out a started detached child in the background, so its exit never
// strands a zombie in the spawner's process table. Setsid detaches the session,
// not the parent-child link: a long-lived daemon spawns holders from every
// supervise revival and skew replace, and Process.Release alone would leave one
// defunct entry per exited child (a flock-refusal loser, a crash-at-startup
// backoff attempt, every replaced holder) until the spawner itself exits. The
// goroutine's exit is the child's.
func reap(cmd *exec.Cmd) {
	go func() { _ = cmd.Wait() }()
}
