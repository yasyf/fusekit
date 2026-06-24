package mountd

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefaultRetireWaitGone / DefaultRetireKillWait mirror cc-notes' runMountShutdown
// timeouts (5s then 2s) and Converge's historical convergeWaitGone/KillWait. Vars,
// not consts, so a test shrinks them off the multi-second wedged path.
var (
	DefaultRetireWaitGone = 5 * time.Second
	DefaultRetireKillWait = 2 * time.Second
)

// RetirePlan is the input to Retire: the already-decided retire-and-remount of
// one wedged/skewed holder. The caller has ALREADY polled, decided to retire,
// snapshotted the mounts, and (improvement #1) captured the holder's pid at gate
// time — BEFORE Shutdown. Retire owns only the mechanics from Shutdown onward.
type RetirePlan struct {
	// Client drives the retiring holder's socket (Shutdown / WaitGone / KillPeer).
	Client *Client
	// CapturedPID is the retiring holder's pid, resolved by the CALLER at gate
	// time (before Shutdown) so the peer-gated reap lands only on this exact
	// process, never on a successor that rebound the socket during the graceful
	// wait. CapturedPIDErr is the capture's error: a non-nil value DISABLES the
	// reap (no identity to gate on) — the wedged holder is left for the caller's
	// next invocation, never name-killed.
	CapturedPID    int
	CapturedPIDErr error
	// WaitGone bounds the graceful post-Shutdown socket-release wait; KillWait
	// bounds the post-reap wait. Zero falls back to the Default* vars.
	WaitGone time.Duration
	KillWait time.Duration
	// Mounts is the live-mount set the caller snapshotted BEFORE Shutdown — the
	// (base, dir) pairs to carcass-clear then remount.
	Mounts []MountInfo
	// ForceUnmount force-unmounts one orphaned carcass dir directly (bounded,
	// bypassing the dead holder). REQUIRED. Run on every Mounts dir BEFORE the
	// remount — the wedged-NFS kill-9 whole-machine hazard.
	ForceUnmount func(dir string)
	// Spawn brings the successor up at the caller's version. REQUIRED.
	Spawn func() error
	// Remount re-establishes the snapshot after the successor is up (improvement
	// #3's carried-vs-row hook; default: remount all Mounts). Returns a joined
	// best-effort error. REQUIRED.
	Remount func() error
}

func (p RetirePlan) waitGone() time.Duration {
	if p.WaitGone > 0 {
		return p.WaitGone
	}
	return DefaultRetireWaitGone
}

func (p RetirePlan) killWait() time.Duration {
	if p.KillWait > 0 {
		return p.KillWait
	}
	return DefaultRetireKillWait
}

// Retire runs the shared retire-and-remount mechanics for one already-decided
// holder replacement: graceful Shutdown, a peer-gated reap of the gate-time pid
// if the socket lingers, the successor Spawn, then — INVARIANT — the carcass
// force-unmount BEFORE the Remount. The caller owns every skip-decision and the
// gate-time pid capture; Retire owns nothing but the order. ctx bounds the
// socket-release waits. Returns the Remount's joined best-effort error only (for
// the caller's log); a failed Shutdown the holder still acted on, a refused reap,
// and a single failed remount are all non-fatal — that dir's next Setup heals it.
func Retire(ctx context.Context, p RetirePlan) error {
	if _, err := p.Client.Shutdown(); err != nil && !errors.Is(err, ErrHolderUnavailable) {
		return fmt.Errorf("retire holder: %w", err)
	}
	// Peer-gated reap: only if the socket lingers AND the pid was captured at
	// gate time. KillPeer signals ONLY when the socket's current peer still
	// matches the captured pid, so a successor that rebound during the graceful
	// wait is refused, not shot (meltdown-critical). A capture error disables the
	// reap entirely — never a name kill.
	if !p.Client.WaitGoneContext(ctx, p.waitGone()) && p.CapturedPIDErr == nil {
		_, _ = p.Client.KillPeer(p.CapturedPID)
		p.Client.WaitGoneContext(ctx, p.killWait())
	}
	if err := p.Spawn(); err != nil {
		return fmt.Errorf("retire holder: respawn: %w", err)
	}
	// INVARIANT: carcass-clear BEFORE remount. A retired holder's mounts are dead
	// carcasses; force-unmount each before the successor remounts it, or a wedged
	// NFS carcass re-wedges the kernel (the kill-9 whole-machine hazard).
	for _, m := range p.Mounts {
		p.ForceUnmount(m.Dir)
	}
	return p.Remount()
}
