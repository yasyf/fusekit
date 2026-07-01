package mountd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit"
)

// RemoteHost drives the detached mount-holder over its socket, so mounts outlive
// the daemon and CLI. Compiles in every build variant; only the spawn path
// (Spawn.EnsureRunning) needs the fuse build.
type RemoteHost struct {
	// Socket is the mount-holder's unix socket path.
	Socket string
	// LogPath receives a spawned holder's stdout and stderr.
	LogPath string
	// Args is the holder argv passed to Spawn.
	Args []string
	// SpawnTimeout bounds waiting for a freshly spawned holder's socket. Zero
	// means DefaultSpawnTimeout.
	SpawnTimeout time.Duration
	// CannotHostHint is the pure-build refusal guidance passed to Spawn.
	CannotHostHint string
	// StableExecDir is forwarded to Spawn: a stable holder exec path, so the
	// macOS volume-access TCC grant survives upgrades.
	StableExecDir string
	// ExecPath is forwarded to Spawn: the holder is the cask binary at this path.
	ExecPath string
	Owner    string
	// Version is the consumer's wire version — the Server.Version the holder
	// reports through OpHealth.
	Version string
}

// localState reports the kernel's (mounted, alive) view of (base, dir); a var
// so tests fake kernel state without real mounts.
var localState = func(base, dir string) (mounted, alive bool) {
	m := fusekit.Mounted(dir)
	return m, m && fusekit.MountAlive(base, dir)
}

func (h *RemoteHost) ensureRunning() error {
	return Spawn{
		Socket:         h.Socket,
		LogPath:        h.LogPath,
		Args:           h.Args,
		Timeout:        h.SpawnTimeout,
		CannotHostHint: h.CannotHostHint,
		StableExecDir:  h.StableExecDir,
		ExecPath:       h.ExecPath,
	}.EnsureRunning()
}

func (h *RemoteHost) client() *Client { return &Client{Socket: h.Socket, Owner: h.Owner} }

// spawnHolder is a var so a converge test binds a canned successor instead of
// exec'ing a real holder.
var spawnHolder = func(h *RemoteHost) error { return h.ensureRunning() }

// convergeForceUnmount fills RetirePlan.ForceUnmount; a var so a converge test
// records the calls without a real unmount.
var convergeForceUnmount = func(dir string) { _ = fusekit.ForceUnmount(dir) }

// overlayClass dual-wraps a wire sentinel with its fusekit equivalent: the
// in-process host and this remote one must be errors.Is-identical, and the
// wire identity must stay in the chain for mountd-aware callers.
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

// Setup ensures a live mirror of base at accountDir. An already-mounted,
// shallow-live mirror is adopted with zero RPC; the adoption stat is bounded,
// so a wedged mirror reads not-adoptable and routes to the holder instead of
// hanging the caller. A partial wedge (shallow-alive) is the daemon's to
// deep-probe and tear down, not Setup's (see MountInfo). Otherwise the holder
// is spawned if needed and asked to mount. A dead holder's carcass — a
// mountpoint the fresh holder has no registry row for — fails with
// ErrForeignMount by design (the holder never stacks mounts); callers must
// Teardown(base, accountDir), then retry.
func (h *RemoteHost) Setup(base, accountDir string) error {
	if st, ok := probeMount(localState, base, accountDir); ok && st.mounted && st.alive {
		return nil
	}
	if err := h.ensureRunning(); err != nil {
		return fmt.Errorf("mount %s: %w", accountDir, err)
	}
	if err := h.client().Mount(base, accountDir); err != nil {
		return fmt.Errorf("mount %s: %w", accountDir, overlayClass(err))
	}
	return nil
}

// AddMount is the content-aware Setup: same local-liveness short-circuit and
// holder-spawn, but it re-registers a synth-serving mount over RPC, carrying
// spec's bridge wiring. Consumers driving the shared holder's content mounts
// call this instead of Setup.
func (h *RemoteHost) AddMount(spec fusekit.MountSpec) error {
	if st, ok := probeMount(localState, spec.Base, spec.Dir); ok && st.mounted && st.alive {
		return nil
	}
	if err := h.ensureRunning(); err != nil {
		return fmt.Errorf("mount %s: %w", spec.Dir, err)
	}
	if err := h.client().AddMount(spec); err != nil {
		return fmt.Errorf("mount %s: %w", spec.Dir, overlayClass(err))
	}
	return nil
}

// convergeWaitGone and convergeKillWait bound the retired holder's socket
// release: a graceful wait after an acked Shutdown, then — if the socket lingers
// — a shorter wait after a peer-gated reap. Vars, not consts, so a test can
// shrink them off the multi-second wedged path.
var (
	convergeWaitGone = 5 * time.Second
	convergeKillWait = 2 * time.Second
)

// Converge replaces a holder reporting a version other than h.Version, so a
// consumer upgrade takes effect on the shared multi-mount holder without a
// manual restart. It is a separate, explicit call — Setup's zero-RPC adopt fast
// path never invokes it — meant to run once at session start before Setup.
//
// Empty h.Version disables converge. An unreachable holder is not an error
// (the caller's subsequent Setup spawns a fresh one), nor is a holder whose
// version is unknown (a degraded/discarded reading the next call re-checks).
// On confirmed skew the stale holder is retired, the consumer's binary is
// respawned, and every mount the shared holder served is remounted — so the
// OTHER repos that holder hosted come back. A single failed remount does not
// fail the whole converge (that dir's own next Setup heals it); the joined
// remount error is returned only for the caller's log.
func (h *RemoteHost) Converge(ctx context.Context) error {
	if h.Version == "" {
		return nil
	}
	c := h.client()
	// Route on Poll's verdict booleans, not its error (a degraded holder is
	// Reachable with a non-nil List error); keying the unreachable arm on
	// !Reachable lets a reachable-but-degraded holder fall through to its own arm.
	poll, _ := c.Poll()
	switch {
	case !poll.Reachable:
		return nil
	case poll.Version == "":
		// Reachable holder reports no version — unknown, not skew evidence; the
		// next call re-checks.
		return nil
	case poll.Version == h.Version:
		return nil
	case poll.Degraded:
		// A degraded holder is alive at a known skewed version, but its live-mount
		// set could not be read. Retiring it would lose the (base, dir) pairs we
		// must remount to bring the other shared repos back, so spare it and leave
		// the converge for the next invocation, when List may answer.
		return nil
	}

	// Capture the wedged holder's pid while it still holds the socket, BEFORE
	// Shutdown: a successor that rebinds during the graceful wait is then refused
	// by KillPeer, not shot. A PeerPID error disables the reap.
	wedgedPID, pidErr := c.PeerPID()
	mounts := poll.Mounts
	if err := Retire(ctx, RetirePlan{
		Client:         c,
		CapturedPID:    wedgedPID,
		CapturedPIDErr: pidErr,
		WaitGone:       convergeWaitGone,
		KillWait:       convergeKillWait,
		Mounts:         mounts,
		ForceUnmount:   convergeForceUnmount,
		Spawn:          func() error { return spawnHolder(h) },
		Remount: func() error {
			var remountErr error
			for _, m := range mounts {
				if err := h.client().Mount(m.Base, m.Dir); err != nil {
					remountErr = errors.Join(remountErr, fmt.Errorf("converge: remount %s: %w", m.Dir, overlayClass(err)))
				}
			}
			return remountErr
		},
	}); err != nil {
		return fmt.Errorf("converge: %w", err)
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
	if err := h.client().Unmount(base, accountDir); err != nil {
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
// path, and it is bounded (probeMount): a wedged mirror's stats never return.
// The error DISTINGUISHES the two failure shapes so a caller need not remount
// on the first blip: a probe that did not answer within liveProbeTimeout wraps
// fusekit.ErrLivenessTimeout (the mirror is unresponsive but NOT proven dead —
// the holder may be saturated; debounce it), while a definitive dead reading
// (no longer a mountpoint, or base invisible through it) answers fast and stays
// a plain error (remount now). A caller that ignores the sentinel still sees a
// non-nil error and routes the dir into the bounded teardown→remount recovery.
func (h *RemoteHost) Health(base, accountDir string) error {
	st, ok := probeMount(localState, base, accountDir)
	switch {
	case !ok:
		return fmt.Errorf("%w: mount at %s did not answer a liveness stat within %s (holder may be saturated)", fusekit.ErrLivenessTimeout, accountDir, liveProbeTimeout)
	case !st.mounted:
		return fmt.Errorf("%s is not a mountpoint", accountDir)
	case !st.alive:
		return fmt.Errorf("mount at %s is dead: %s's contents are not visible through it", accountDir, base)
	}
	return nil
}
