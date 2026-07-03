package mountd

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// DefaultRetireWaitGone / DefaultRetireKillWait bound Retire's graceful and
// post-reap socket-release waits. Vars, not consts, so tests can shrink them.
var (
	DefaultRetireWaitGone = 5 * time.Second
	DefaultRetireKillWait = 2 * time.Second
)

// RetirePlan is the input to Retire: one already-decided retire-and-remount of a
// wedged/skewed holder. The caller decides, snapshots Mounts, and captures the
// pid BEFORE Shutdown; Retire owns only the mechanics from Shutdown onward.
type RetirePlan struct {
	// Client drives the retiring holder's socket.
	Client *Client
	// CapturedPID is the retiring holder's pid, captured before Shutdown: the reap
	// is peer-gated on it so a rebound successor is never shot. A non-nil
	// CapturedPIDErr DISABLES the reap — never a name kill.
	CapturedPID    int
	CapturedPIDErr error
	// WaitGone bounds the graceful post-Shutdown socket-release wait; KillWait
	// bounds the post-reap wait. Zero falls back to the Default* vars.
	WaitGone time.Duration
	KillWait time.Duration
	// Mounts is the set snapshotted BEFORE Shutdown, to carcass-clear then remount.
	Mounts []MountInfo
	// ForceUnmount force-unmounts one carcass dir directly, bypassing the dead
	// holder. REQUIRED.
	ForceUnmount func(dir string)
	// Spawn brings the successor up at the caller's version. REQUIRED.
	Spawn func() error
	// Remount re-establishes the snapshot once the successor is up; returns a
	// joined best-effort error. REQUIRED.
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

// Retire runs one already-decided holder replacement: graceful Shutdown, a
// peer-gated reap of the gate-time pid if the socket lingers, Spawn, then
// carcass force-unmount BEFORE Remount. ctx bounds the socket-release waits.
// Returns Remount's joined best-effort error only; an already-gone holder, a
// refused reap, and a failed remount are non-fatal — the dir's next Setup heals.
func Retire(ctx context.Context, p RetirePlan) error {
	if _, err := p.Client.Shutdown(); err != nil && !errors.Is(err, ErrHolderUnavailable) {
		return fmt.Errorf("retire holder: %w", err)
	}
	if !p.Client.WaitGoneContext(ctx, p.waitGone()) && p.CapturedPIDErr == nil {
		_, _ = p.Client.KillPeer(p.CapturedPID)
		p.Client.WaitGoneContext(ctx, p.killWait())
	}
	if err := p.Spawn(); err != nil {
		return fmt.Errorf("retire holder: respawn: %w", err)
	}
	// INVARIANT: carcass-clear BEFORE remount, or a wedged NFS carcass re-wedges
	// the kernel (the kill-9 whole-machine hazard). Clear the distinct NATIVE
	// roots only: a mux subtree (MuxRoot set) is no kernel mount of its own — its
	// carcass lives at the shared MuxRoot, and MNT_FORCE is a root-only operation
	// — so subtree Dirs dedupe onto their MuxRoot while a plain row clears its own
	// Dir. Force-unmounting a subtree path would miss the real carcass at the root
	// and leave every mux remount refused fail-closed.
	cleared := map[string]bool{}
	for _, m := range p.Mounts {
		root := m.Dir
		if m.MuxRoot != "" {
			root = m.MuxRoot
		}
		if cleared[root] {
			continue
		}
		cleared[root] = true
		p.ForceUnmount(root)
	}
	return p.Remount()
}
