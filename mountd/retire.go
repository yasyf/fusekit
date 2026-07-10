package mountd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/proc"
)

// DefaultRetireWaitGone / DefaultRetireKillWait bound Retire's graceful and
// post-reap socket-release waits; DefaultRetireSpawnWait bounds the successor
// spawn's transient-failure retries. Vars, not consts, so tests can shrink them.
var (
	DefaultRetireWaitGone  = 5 * time.Second
	DefaultRetireKillWait  = 2 * time.Second
	DefaultRetireSpawnWait = 30 * time.Second
)

// retireSpawnRetryPause paces the transient-spawn retries, matching
// EnsureRunning's socket-poll cadence.
const retireSpawnRetryPause = 100 * time.Millisecond

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
	// bounds the post-reap wait; SpawnWait bounds Spawn's transient-failure
	// retries. Zero falls back to the Default* vars.
	WaitGone  time.Duration
	KillWait  time.Duration
	SpawnWait time.Duration
	// Mounts is the set snapshotted BEFORE Shutdown, to carcass-clear then remount.
	Mounts []MountInfo
	// ClearCarcass clears one CONFIRMED-DEAD carcass at a kernel root,
	// bypassing the dead holder (fusekit.ClearCarcass). It must never
	// force-unmount a live mount — MNT_FORCE on a live/busy NFS mount panics
	// the Apple NFS kext; a root that did not clear surfaces through Remount's
	// error. REQUIRED.
	ClearCarcass func(dir string)
	// Spawn brings the successor up at the caller's version. Transient failures
	// (proc.ErrChildUnavailable, proc.ErrPeerStarting) are retried within
	// SpawnWait — the retiree frees its socket flock only at process exit, so a
	// prompt successor can lose the flock race; any other error is permanent.
	// REQUIRED.
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

func (p RetirePlan) spawnWait() time.Duration {
	if p.SpawnWait > 0 {
		return p.SpawnWait
	}
	return DefaultRetireSpawnWait
}

// spawn retries p.Spawn on the flock-race transients (see RetirePlan.Spawn)
// until SpawnWait elapses or ctx ends; a permanent error returns immediately.
func (p RetirePlan) spawn(ctx context.Context) error {
	deadline := time.Now().Add(p.spawnWait())
	for {
		err := p.Spawn()
		if err == nil {
			return nil
		}
		if !errors.Is(err, proc.ErrChildUnavailable) && !errors.Is(err, proc.ErrPeerStarting) {
			return err
		}
		if time.Now().After(deadline) {
			return err
		}
		select {
		case <-ctx.Done():
			return err
		case <-time.After(retireSpawnRetryPause):
		}
	}
}

// Retire runs one already-decided holder replacement: graceful Shutdown, a
// peer-gated reap of the gate-time pid if the socket lingers, a Spawn retried
// on flock-race transients, then a confirmed-dead carcass clear BEFORE
// Remount. ctx bounds the socket-release waits and the spawn retries. Returns
// Remount's joined best-effort error only; an already-gone holder, a refused
// reap, and a failed remount are non-fatal — the dir's next Setup heals.
func Retire(ctx context.Context, p RetirePlan) error {
	if _, err := p.Client.Shutdown(); err != nil && !errors.Is(err, ErrHolderUnavailable) {
		return fmt.Errorf("retire holder: %w", err)
	}
	if !p.Client.WaitGoneContext(ctx, p.waitGone()) && p.CapturedPIDErr == nil {
		_, _ = p.Client.KillPeer(p.CapturedPID)
		p.Client.WaitGoneContext(ctx, p.killWait())
	}
	if err := p.spawn(ctx); err != nil {
		return fmt.Errorf("retire holder: respawn: %w", err)
	}
	// INVARIANT: carcass-clear BEFORE remount, or a wedged NFS carcass re-wedges
	// the kernel (the kill-9 whole-machine hazard). Clear the distinct NATIVE
	// roots only: a mux subtree (MuxRoot set) is no kernel mount of its own — its
	// carcass lives at the shared MuxRoot, and MNT_FORCE is a root-only operation
	// — so subtree Dirs dedupe onto their MuxRoot while a plain row clears its own
	// Dir. Force-unmounting a subtree path would miss the real carcass at the root
	// and leave every mux remount refused fail-closed. A root ANY row declares
	// CarcassPolicy "defer" for is never cleared (as in the holder's own replay):
	// its carcass stays for the consumer, and the remount surfaces it loudly as
	// ErrForeignMount.
	forced := map[string]bool{}
	var roots []string
	for _, m := range p.Mounts {
		root := m.Dir
		if m.MuxRoot != "" {
			root = m.MuxRoot
		}
		if _, ok := forced[root]; !ok {
			forced[root] = true
			roots = append(roots, root)
		}
		if m.CarcassPolicy == fusekit.CarcassPolicyDefer {
			forced[root] = false
		}
	}
	for _, root := range roots {
		if forced[root] {
			p.ClearCarcass(root)
		}
	}
	return p.Remount()
}
