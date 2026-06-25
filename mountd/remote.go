package mountd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit"
)

// RemoteHost drives the detached mount-holder over its socket, so the mounts
// outlive the daemon and CLI processes that ask for them. It is the wire/lifecycle
// half an overlay fuse provider embeds — Setup/Teardown/Sync/Health — with the
// consumer-specific Backend()/PrivateRoot() adapter left to each app (see
// overlay.RemoteFuseProvider). It compiles in every build variant: a running
// holder is usable by any build, and only the spawn path (Spawn.EnsureRunning)
// requires the fuse build.
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
	// the macOS volume-access TCC grant persists (the embedded Developer-ID
	// designated requirement survives the copy). Empty preserves the
	// os.Executable() default.
	StableExecDir string
	// ExecPath is forwarded to Spawn: the holder is the cask binary at this path.
	ExecPath string
	Owner    string
	// Version is the consumer's wire version — the value the holder reports
	// through OpHealth (the Server.Version this consumer set). When set, Converge
	// replaces a holder reporting a different version so a consumer upgrade takes
	// effect on the shared multi-mount holder without a manual restart. Empty
	// disables Converge.
	Version string
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
		ExecPath:       h.ExecPath,
	}.EnsureRunning()
}

func (h *RemoteHost) client() *Client { return &Client{Socket: h.Socket, Owner: h.Owner} }

// spawnHolder brings up a holder serving h.Socket at the consumer's version. A
// var so a converge test binds a canned successor instead of exec'ing a real holder.
var spawnHolder = func(h *RemoteHost) error { return h.ensureRunning() }

// convergeForceUnmount force-unmounts one orphaned carcass dir before a converge
// remount (the wedged-NFS kill-9 hazard). A var so a converge test records the
// calls without a real unmount, the spawnHolder/localState seam idiom.
var convergeForceUnmount = func(dir string) { _ = fusekit.ForceUnmount(dir) }

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
// release: first a graceful wait after an acked Shutdown, then — if the socket
// lingers — a shorter wait after a peer-gated reap. They mirror cc-notes'
// runMountShutdown timeouts (5s then 2s). Vars, not consts, so a test can shrink
// them off the multi-second wedged path (the spawnHolder/localState seam idiom).
var (
	convergeWaitGone = 5 * time.Second
	convergeKillWait = 2 * time.Second
)

// Converge replaces a holder reporting a version other than h.Version, so a
// consumer upgrade takes effect on the shared multi-mount holder without a
// manual restart. It is a separate, explicit call — Setup's zero-RPC adopt fast
// path never invokes it — meant to run once at session start before Setup: the
// common case is the cheap no-op where the holder already serves h.Version.
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
	// Poll's contract is to route on the verdict booleans and read the error only
	// for context (a degraded holder reports Reachable AND a non-nil List error),
	// so the unreachable arm keys on !Reachable — never on the error — which lets
	// a reachable-but-degraded holder fall through to its own explicit arm below.
	poll, _ := c.Poll()
	switch {
	case !poll.Reachable:
		return nil
	case poll.Version == "":
		// Reachable holder reports no version — unknown, not skew evidence; the
		// next call re-checks (mirrors proc.Supervisor.isSkew treating "" as not-skew).
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
	// Shutdown (improvement #1): a successor that rebinds during the graceful wait
	// is then refused by KillPeer, not shot. A PeerPID error disables the reap.
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
// non-nil error and routes the dir into the bounded teardown→remount recovery
// exactly as before.
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
