package mountd

import (
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit"
)

// RemoteHost drives the detached mount-holder over its socket, so mounts outlive
// the daemon and CLI. Compiles in every build variant; only the spawn path
// (Spawn.EnsureRunning) needs the fuse build. There is no consumer-driven
// retire/converge: the holder self-retires on version skew, lease-gated.
//
// Holder-binary contract: RemoteHost lazily fork+execs the holder from an
// arbitrary consumer process, so the child inherits every non-CLOEXEC
// descriptor — a session's lease fd included, which would pin that lease for
// the holder's whole lifetime. Any binary served as a holder MUST call
// proc.CloseInheritedFDs before any other work (cmd/holder complies); pure
// Go cannot enforce this from the spawner.
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
	// ExecPath is forwarded to Spawn: the holder is the cask binary at this path.
	ExecPath string
	Owner    string
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
		ExecPath:       h.ExecPath,
	}.EnsureRunning()
}

func (h *RemoteHost) client() *Client { return &Client{Socket: h.Socket, Owner: h.Owner} }

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
	case errors.Is(err, ErrMuxMismatch):
		return fmt.Errorf("%w: %w", fusekit.ErrMuxMismatch, err)
	default:
		return err
	}
}

// Setup ensures a live mirror of base at accountDir. The Mount RPC is ALWAYS
// sent — no local-liveness short-circuit — because it is idempotent, local,
// and the holder's idempotent path refreshes the journal row: after a
// journal-write failure the live mount's row can be stale, and only the
// retried RPC heals it. The holder is spawned if needed. A dead holder's
// carcass — a mountpoint the fresh holder has no registry row for — fails
// with ErrForeignMount by design (the holder never stacks mounts); callers
// must Teardown(base, accountDir), then retry.
func (h *RemoteHost) Setup(base, accountDir string) error {
	if err := h.ensureRunning(); err != nil {
		return fmt.Errorf("mount %s: %w", accountDir, err)
	}
	if err := h.client().Mount(base, accountDir); err != nil {
		return fmt.Errorf("mount %s: %w", accountDir, overlayClass(err))
	}
	return nil
}

// AddMount is the content-aware Setup: the same always-sent idempotent RPC
// and holder-spawn, carrying spec's bridge wiring for a synth-serving mount.
// Consumers driving the shared holder's content mounts call this instead of
// Setup.
func (h *RemoteHost) AddMount(spec fusekit.MountSpec) error {
	if err := h.ensureRunning(); err != nil {
		return fmt.Errorf("mount %s: %w", spec.Dir, err)
	}
	if err := h.client().AddMount(spec); err != nil {
		return fmt.Errorf("mount %s: %w", spec.Dir, overlayClass(err))
	}
	return nil
}

// AddBridge asks the shared holder to host this owner's File-Provider-facing
// content bridge: it spawns the holder if absent, then binds bridgeSocket and
// relays it to contentSocket. Idempotent adopt for the same owner. ErrForeignBridge
// surfaces a foreign owner already bound on bridgeSocket; consumers gate on
// OpHello's FeatureBridge.
func (h *RemoteHost) AddBridge(bridgeSocket, contentSocket string, privatePrefixes []string) error {
	if err := h.ensureRunning(); err != nil {
		return fmt.Errorf("add bridge %s: %w", bridgeSocket, err)
	}
	// The persist-warning is the owning driver's Client-level concern; this
	// seam reports the bridge's live state only.
	if _, _, err := h.client().AddBridge(bridgeSocket, contentSocket, privatePrefixes); err != nil {
		return fmt.Errorf("add bridge %s: %w", bridgeSocket, err)
	}
	return nil
}

// RemoveBridge stops and drains this owner's hosted bridge. Like RemoveMount it
// never spawns a holder — an unreachable holder means the bridge is already gone
// with it, a no-op success; the durable spool survives on disk regardless.
func (h *RemoteHost) RemoveBridge() error {
	if _, _, err := h.client().RemoveBridge(); err != nil {
		if errors.Is(err, ErrHolderUnavailable) {
			return nil
		}
		return fmt.Errorf("remove bridge: %w", err)
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
	// The persist-warning is the driving consumer's Client-level concern;
	// Teardown's contract is kernel truth, re-verified below.
	if _, err := h.client().Unmount(base, accountDir); err != nil {
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

// RemoveMount detaches a mux subtree from its shared native mount via the
// holder. Unlike Teardown it never short-circuits on local kernel state: a mux
// subtree is a logical entry in the holder's tree index, never an independent
// kernel mountpoint, so Mounted(dir) is always false and would make Teardown's
// pre-check a spurious no-op that never reaches the holder. It also never spawns
// a holder — a subtree exists only while a holder hosts its native root, so an
// unreachable holder means the root (and the subtree with it) is already gone,
// a no-op success. The last-child native unmount is re-verified holder-side
// against the ROOT; a wedge there surfaces as fusekit.ErrUnmountWedged.
func (h *RemoteHost) RemoveMount(base, dir string) error {
	if _, err := h.client().Unmount(base, dir); err != nil {
		if errors.Is(err, ErrHolderUnavailable) {
			return nil
		}
		return fmt.Errorf("detach %s: %w", dir, overlayClass(err))
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
