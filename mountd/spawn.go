package mountd

import (
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/proc"
)

// DefaultSpawnTimeout re-exports proc.DefaultSpawnTimeout: the spawn-wait bound
// a zero Spawn.Timeout / RemoteHost.SpawnTimeout falls back to. It stays in the
// mountd surface so consumers that named it keep compiling.
const DefaultSpawnTimeout = proc.DefaultSpawnTimeout

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
	return proc.Spawn{
		Socket:        s.Socket,
		LogPath:       s.LogPath,
		Args:          s.Args,
		Timeout:       s.Timeout,
		StableExecDir: s.StableExecDir,
		Available:     cl.Available,
		CanHost: func() error {
			if fusekit.Built() {
				return nil
			}
			return fmt.Errorf("%w: %s", ErrCannotHost, s.CannotHostHint)
		},
	}.EnsureRunning()
}
