package mountd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/proc"
)

// RetirePolicy supplies the child-control half of proc.Policy (Shutdown /
// WaitGone / Kill / Reconcile) over a holder Client. The consumer must
// SetCapturedPID at ReplaceSafe gate time so Kill signals only that process.
type RetirePolicy struct {
	Client *Client
	// KillPeer overrides the peer-gated reap; nil uses Client.KillPeer. An
	// override must keep the guarantee: signal ONLY a matching peer (refuse a
	// rebound successor), ErrUnreachable when the socket has no peer.
	KillPeer func(wantPID int) (int, error)
	// OnShutdown, when non-nil, runs after the Shutdown RPC; the RPC error is
	// returned regardless.
	OnShutdown func(failed []MountInfo, err error)
	// Consumer per-transition hooks.
	OnChildDied        func(ctx context.Context)
	OnRespawned        func(ctx context.Context)
	OnReplaceSucceeded func(ctx context.Context)
	OnReplaceAborted   func(ctx context.Context)

	capturedPID    int
	capturedPIDErr error
}

// SetCapturedPID records the holder pid (and capture error) resolved at
// ReplaceSafe gate time; Kill signals only that pid.
func (a *RetirePolicy) SetCapturedPID(pid int, err error) {
	a.capturedPID, a.capturedPIDErr = pid, err
}

// Shutdown asks the holder to step down. proc routes on the RPC error only.
func (a *RetirePolicy) Shutdown(_ context.Context) error {
	failed, err := a.Client.Shutdown()
	if a.OnShutdown != nil {
		a.OnShutdown(failed, err)
	}
	return err
}

// WaitGone reports whether the retiring holder released its socket within d.
func (a *RetirePolicy) WaitGone(ctx context.Context, d time.Duration) bool {
	return a.Client.WaitGoneContext(ctx, d)
}

// Kill signals ONLY the pid captured at gate time. A vanished peer
// (ErrUnreachable) maps to proc.ErrChildUnavailable so proc reads "nothing to
// kill, socket free."
func (a *RetirePolicy) Kill() (int, error) {
	if a.capturedPIDErr != nil {
		return 0, fmt.Errorf("holder pid not captured at gate time: %w", a.capturedPIDErr)
	}
	kp := a.KillPeer
	if kp == nil {
		kp = a.Client.KillPeer
	}
	pid, err := kp(a.capturedPID)
	if errors.Is(err, ErrUnreachable) {
		return 0, fmt.Errorf("%w: %w", proc.ErrChildUnavailable, err)
	}
	return pid, err
}

// Reconcile routes proc's transition to the matching callback. proc fires
// ChildDied (carcass-clear) BEFORE Respawned (remount).
func (a *RetirePolicy) Reconcile(ctx context.Context, ev proc.ReconcileEvent) {
	switch ev.Kind {
	case proc.ChildDied:
		if a.OnChildDied != nil {
			a.OnChildDied(ctx)
		}
	case proc.Respawned:
		if a.OnRespawned != nil {
			a.OnRespawned(ctx)
		}
	case proc.ReplaceSucceeded:
		if a.OnReplaceSucceeded != nil {
			a.OnReplaceSucceeded(ctx)
		}
	case proc.ReplaceAborted:
		if a.OnReplaceAborted != nil {
			a.OnReplaceAborted(ctx)
		}
	}
}
