package mountd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit"
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
	// ClearCarcass clears one CONFIRMED-DEAD carcass at a kernel root,
	// bypassing the dead holder (fusekit.ClearCarcass). It must never
	// force-unmount a live mount — MNT_FORCE on a live/busy NFS mount panics
	// the Apple NFS kext; a root that did not clear surfaces through Remount's
	// error. REQUIRED.
	ClearCarcass func(dir string)
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
// peer-gated reap of the gate-time pid if the socket lingers, Spawn, then a
// confirmed-dead carcass clear BEFORE Remount. ctx bounds the socket-release
// waits. Returns Remount's joined best-effort error only; an already-gone
// holder, a refused reap, and a failed remount are non-fatal — the dir's next
// Setup heals.
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
