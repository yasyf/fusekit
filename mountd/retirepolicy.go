package mountd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/proc"
)

// RetirePolicy supplies the child-control half of proc.Policy for a mount-holder
// consumer — Shutdown / WaitGone / Kill (peer-gated, captured-pid) / Reconcile —
// over consumer callbacks, so the same mechanics that back RemoteHost.Converge
// also back a supervised holder. It does NOT implement the consumer-judgement
// hooks (Probe / PeerAlive / ReplaceSafe / Retreat): those carry app state (claim
// gates, degraded debounce, retreat) and stay the consumer's. The consumer
// captures the holder's pid at ReplaceSafe gate time and stores it via
// SetCapturedPID so Kill lands only on that process.
type RetirePolicy struct {
	// Client drives the holder socket (Shutdown / WaitGone / KillPeer).
	Client *Client
	// KillPeer overrides the peer-gated reap. A consumer with its own kill seam —
	// a daemon that fakes the signal in tests, since the package-level
	// peerPIDFn/killProc are unexported and unreachable across packages — wires it
	// here so Kill routes through that seam. nil uses Client.KillPeer. It is given
	// the pid captured at gate time and must keep the peer-gated guarantee: signal
	// ONLY a peer matching that pid (refuse a rebound successor), and report
	// ErrUnreachable when the socket has no peer.
	KillPeer func(wantPID int) (int, error)
	// OnShutdown, when non-nil, runs after the holder acks Shutdown (e.g. a cache
	// markUnhealthy + a would-not-unmount log). The Shutdown RPC error is proc's
	// routing truth and is returned regardless.
	OnShutdown func(failed []MountInfo, err error)
	// OnChildDied / OnRespawned / OnReplaceSucceeded / OnReplaceAborted are the
	// per-transition re-establishment the consumer owns (snapshot + carcass-clear,
	// remount, claim release). The adapter only routes the Kind.
	OnChildDied        func(ctx context.Context)
	OnRespawned        func(ctx context.Context)
	OnReplaceSucceeded func(ctx context.Context)
	OnReplaceAborted   func(ctx context.Context)

	// capturedPID / capturedPIDErr is the holder identity the consumer resolved at
	// gate time (SetCapturedPID), so Kill gates on this exact process.
	capturedPID    int
	capturedPIDErr error
}

// SetCapturedPID stores the holder pid the consumer resolved at ReplaceSafe gate
// time (and its capture error), so a later peer-gated Kill lands only on it.
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

// Kill is the peer-gated reap (force-Replace reap only): it signals ONLY the pid
// captured at gate time. An uncaptured identity refuses before the seam, and a
// vanished peer (ErrUnreachable) maps to proc.ErrChildUnavailable so proc's
// reapWedged reads "nothing to kill, socket free."
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

// Reconcile routes proc's transition to the consumer's callback. The
// carcass-clear belongs in the consumer's OnChildDied, which proc fires BEFORE
// the respawn — preserving carcass-clear-before-remount (OnRespawned remounts).
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
