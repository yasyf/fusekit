package mountd

import (
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit"
)

// RemoteHost drives the detached mount-holder over its socket, so the mounts
// outlive the daemon and CLI processes that ask for them. It is the wire/lifecycle
// half of cc-pool's RemoteProvider — Setup/Teardown/Sync/Health — with the
// consumer-specific Kind()/PrivateRoot() adapter left to each app. It compiles
// in every build variant: a running holder is usable by any build, and only the
// spawn path (Spawn.EnsureRunning) requires the fuse build.
type RemoteHost struct {
	// Socket is the mount-holder's unix socket path.
	Socket string
	// LogPath receives a spawned holder's stdout and stderr.
	LogPath string
	// Args is the holder argv passed to Spawn (e.g. ["mount-holder",
	// "--socket", socket]); the consumer owns the subcommand name.
	Args []string
	// SpawnTimeout bounds waiting for a freshly spawned holder's socket. Zero
	// means DefaultSpawnTimeout.
	SpawnTimeout time.Duration
	// CannotHostHint is the pure-build refusal guidance passed to Spawn.
	CannotHostHint string
	// StableExecDir is forwarded to Spawn: when non-empty, the holder is
	// materialized as a copy under this directory and spawned from there, so the
	// holder's resolved executable path stays stable across version upgrades and
	// the macOS "Network Volumes" TCC grant persists (the embedded Developer-ID
	// designated requirement survives the copy). Empty preserves the
	// os.Executable() default.
	StableExecDir string
}

// localState reports the local-kernel (mounted, alive) pair for a (base, dir):
// mounted is dir's non-blocking mountpoint check, alive is base's contents
// visible through it (only meaningful when mounted, so AND-ed). RemoteHost uses
// it for its zero-RPC adopt precheck, post-teardown re-verify, and Health — all
// local truth, no RPC. A var so tests fake kernel state without real mounts.
var localState = func(base, dir string) (mounted, alive bool) {
	m := fusekit.Mounted(dir)
	return m, m && fusekit.MountAlive(base, dir)
}

// ensureRunning spawns the holder if needed, via the consumer-supplied argv.
func (h *RemoteHost) ensureRunning() error {
	return Spawn{
		Socket:         h.Socket,
		LogPath:        h.LogPath,
		Args:           h.Args,
		Timeout:        h.SpawnTimeout,
		CannotHostHint: h.CannotHostHint,
		StableExecDir:  h.StableExecDir,
	}.EnsureRunning()
}

// overlayClass dual-wraps a wire sentinel with its fusekit root equivalent
// (multi-%w): a caller holding a RemoteHost classifies with the fusekit
// sentinels no matter which process detected the condition — the in-process
// host and this remote one must be errors.Is-identical. The wire identity stays
// in the chain for mountd-aware callers. Exact mapping: ErrTCCDenied →
// ErrMountNotLive, ErrMountTimeout → ErrMountTimeout, ErrMountFailed →
// ErrMountFailed, ErrUnmountWedged → ErrUnmountWedged; everything else passes
// through.
func overlayClass(err error) error {
	switch {
	case errors.Is(err, ErrTCCDenied):
		return fmt.Errorf("%w: %w", fusekit.ErrMountNotLive, err)
	case errors.Is(err, ErrMountTimeout):
		return fmt.Errorf("%w: %w", fusekit.ErrMountTimeout, err)
	case errors.Is(err, ErrMountFailed):
		return fmt.Errorf("%w: %w", fusekit.ErrMountFailed, err)
	case errors.Is(err, ErrUnmountWedged):
		return fmt.Errorf("%w: %w", fusekit.ErrUnmountWedged, err)
	default:
		return err
	}
}

// Setup ensures a live mirror of base at accountDir. A mirror that is already
// mounted and shallow-live is adopted with zero RPC — the holder kept serving
// it across a daemon restart; the adoption stat is bounded (probeMount), so a
// wedged mirror reads not-adoptable and routes to the holder instead of
// hanging the caller. A partial wedge (shallow-alive, bulk reads hang) is NOT
// distinguished here — detecting and healing it is the daemon's job: it
// deep-probes the live mirror and, on a wedge, tears it down so the dir reads
// not-mounted by the time this Setup runs and falls through to a fresh Mount.
// Otherwise the holder is spawned if needed and asked to mount (ensure-mounted
// holder-side: a mirror the holder still holds but that died is remounted). A
// dead HOLDER's carcass — accountDir still a mountpoint but absent from the
// fresh holder's registry — fails with ErrForeignMount by design (the holder
// never stacks mounts): callers must Teardown(base, accountDir) to clear it,
// then retry Setup.
func (h *RemoteHost) Setup(base, accountDir string) error {
	if st, ok := probeMount(localState, base, accountDir); ok && st.mounted && st.alive {
		return nil
	}
	if err := h.ensureRunning(); err != nil {
		return fmt.Errorf("mount %s: %w", accountDir, err)
	}
	if err := NewClient(h.Socket).Mount(base, accountDir); err != nil {
		return fmt.Errorf("mount %s: %w", accountDir, overlayClass(err))
	}
	return nil
}

// Teardown unmounts the mirror at accountDir. Nothing mounted is an immediate
// no-op with no holder contact (this absorbs the pure build's "retreat from a
// fuse account" semantics). Otherwise the holder is spawned if needed — a
// fresh holder clears a dead holder's carcass via its registry-miss path —
// and asked to unmount; an OK reply is then re-verified against the local
// kernel state, because honesty across the RPC boundary requires it (a lost
// response or skewed holder must not read as a clean teardown). Both stats
// are bounded and fail closed: a probe that does not answer reads
// still-mounted — the pre-check proceeds to the holder rather than skipping
// the teardown, and the re-verify reports the wedge rather than vouching for
// a teardown it cannot see, so callers never RemoveAll through a live mount.
func (h *RemoteHost) Teardown(base, accountDir string) error {
	if st, ok := probeMount(localState, base, accountDir); ok && !st.mounted {
		return nil
	}
	if err := h.ensureRunning(); err != nil {
		return fmt.Errorf("unmount %s: %w", accountDir, err)
	}
	if err := NewClient(h.Socket).Unmount(base, accountDir); err != nil {
		return fmt.Errorf("unmount %s: %w", accountDir, overlayClass(err))
	}
	switch st, ok := probeMount(localState, base, accountDir); {
	case !ok:
		return fmt.Errorf("unmount %s: holder reported success but the mountpoint stat did not answer within %s (wedged mirror?): %w", accountDir, liveProbeTimeout, fusekit.ErrUnmountWedged)
	case st.mounted:
		return fmt.Errorf("unmount %s: holder reported success but it is still a mountpoint: %w", accountDir, fusekit.ErrUnmountWedged)
	}
	return nil
}

// Sync re-asserts the overlay. The fuse mirror is live by construction, so
// there is nothing to repair — Sync is Health: report the mirror's state.
func (h *RemoteHost) Sync(base, accountDir string) error {
	return h.Health(base, accountDir)
}

// Health reports whether accountDir is a live mirror of base. It is local
// kernel truth only — zero RPC — because it sits on the daemon's poll hot
// path, and it is bounded (probeMount): a wedged mirror's stats never return,
// and an unanswered probe reads dead so the caller routes the dir into the
// bounded teardown→remount recovery instead of blocking the scheduler with
// the account's poll claim held.
func (h *RemoteHost) Health(base, accountDir string) error {
	st, ok := probeMount(localState, base, accountDir)
	switch {
	case !ok:
		return fmt.Errorf("mount at %s did not answer a liveness stat within %s; treating it as dead (wedged mirror?)", accountDir, liveProbeTimeout)
	case !st.mounted:
		return fmt.Errorf("%s is not a mountpoint", accountDir)
	case !st.alive:
		return fmt.Errorf("mount at %s is dead: %s's contents are not visible through it", accountDir, base)
	}
	return nil
}
