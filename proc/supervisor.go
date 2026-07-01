package proc

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ReconcileKind names the transitions the Supervisor delivers through
// Policy.Reconcile.
type ReconcileKind int

const (
	// ChildDied means the child is genuinely gone (no peer) and about to be
	// revived; the consumer force-unmounts orphaned carcasses. Never fires for an
	// alive-but-wedged child.
	ChildDied ReconcileKind = iota
	// Respawned means a revived child came up and verified; the consumer
	// re-establishes what it must serve. Does NOT imply a preceding ChildDied (a
	// spared wedged child that recovers fires Respawned alone), so
	// ChildDied-stashed state is single-use.
	Respawned
	// ReplaceSucceeded is the Replace finalizer on a verified replacement; the
	// consumer remounts under its held claims, then releases them.
	ReplaceSucceeded
	// ReplaceAborted is the Replace finalizer on any non-success exit; the
	// consumer releases its held claims without remounting.
	ReplaceAborted
)

// Verdict is the consumer's reachability verdict for the child, produced by
// Policy.Probe.
type Verdict struct {
	// Reachable is true when the child answered its primary health check; false
	// means gone or wedged.
	Reachable bool
	// Degraded is true when the primary health check answered but a secondary
	// readiness check failed — alive, not fully ready.
	Degraded bool
	// Version is the child's reported version, set whenever Reachable.
	Version string
}

// ReconcileEvent is one transition delivered to Policy.Reconcile.
type ReconcileEvent struct {
	// Kind names the transition.
	Kind ReconcileKind
}

// Policy is the consumer-supplied behavior surface: the consumer's judgements
// (what "ready" means, when a replace is safe) plus its child-control effects.
// Every member is required; the one optional hook (OnSpawnError) is a
// Supervisor field.
type Policy interface {
	// Probe returns the consumer's reachability Verdict; the Supervisor routes on
	// it and never re-derives reachability.
	Probe() Verdict
	// PeerAlive reports whether the child's socket still has a live peer — the
	// meltdown gate consulted before every destructive arm: false means genuinely
	// dead (revive + ChildDied force-unmount); true means alive-but-wedged (spared).
	PeerAlive() bool
	// ReplaceSafe opens the consumer's claim critical section for a Replace: ""
	// means clear (the consumer now HOLDS its claims); a non-"" reason defers (no
	// claims held). force bypasses the softer legs, never the claim-safety legs.
	// Called once per Replace.
	ReplaceSafe(ctx context.Context, force bool) (reason string)
	// Retreat is the crash-loop breaker action: the Supervisor stops reviving and
	// the consumer falls back to an always-available path. reason names the
	// tripped breaker for the consumer's log line.
	Retreat(ctx context.Context, reason string)
	// Shutdown asks the child to step down for a graceful replace (ctx-aware).
	Shutdown(ctx context.Context) error
	// WaitGone reports whether the retiring child released its socket within d
	// (ctx-aware).
	WaitGone(ctx context.Context, d time.Duration) bool
	// Kill is the PEER-GATED force kill: it kills ONLY the pid captured at gate
	// time (a successor that bound the socket in between is refused), returning
	// the killed pid. ErrChildUnavailable means the peer already vanished (nothing
	// to kill, socket free). Reached only on the force-Replace reap path.
	Kill() (pid int, err error)
	// Reconcile delivers each transition (ChildDied / Respawned / the Replace
	// finalizers) so the consumer re-establishes desired state and releases its
	// claims.
	Reconcile(ctx context.Context, ev ReconcileEvent)
}

const (
	reviveBreakerReason     = "child crash-looped without ever returning at this version"
	spawnFailBreakerReason  = "child will not spawn"
	verifyFailBreakerReason = "child spawns but never passes its health check"
)

// Supervisor keeps a detached, versioned child alive at MyVersion: it revives a
// genuinely dead child (under spawn backoff and a crash-loop breaker), spares an
// alive-but-wedged one, and replaces a version-skewed one once the consumer's
// claim gate clears. The consumer supplies the supervision loop (Tick is the
// unit of work) and drives it from a single goroutine; the Supervisor owns no
// ticker.
type Supervisor struct {
	// Spawn brings a fresh child up on every revive and replace; its Timeout
	// bounds only the fresh child's come-up (retiring-child waits use GoneWait).
	Spawn Spawn
	// MyVersion is this supervisor's own version; the crash-loop breaker resets
	// ONLY on a healthy settle at MyVersion.
	MyVersion string
	// Policy is the consumer behavior surface. Required.
	Policy Policy
	// OnSpawnError, when non-nil, observes each spawn/verify failure booked
	// against the backoff, with the post-increment attempt count and next-retry
	// floor. Surface-only — it must take no irreversible action; the Supervisor
	// owns the retry policy. A benign ErrSkipSpawn is never booked or surfaced,
	// and a successful bring-up does NOT call it.
	OnSpawnError func(err error, attempt int, nextRetry time.Time)
	// GoneWait bounds each wait for a RETIRING child to release its socket (after
	// Shutdown, and after a reap SIGKILL) — distinct from Spawn.Timeout, the fresh
	// child's come-up bound. Zero falls back to Spawn.Timeout, then
	// DefaultSpawnTimeout.
	GoneWait time.Duration
	// HazardWindow bounds what counts as a CONSECUTIVE death for the crash-loop
	// breaker; deaths farther apart reset the count. Zero means defaultHazardWindow.
	HazardWindow time.Duration
	// SpawnBackoff bounds the respawn backoff after consecutive spawn failures.
	SpawnBackoff Backoff
	// ReviveBreaker is the crash-loop breaker threshold: after this many
	// CONSECUTIVE deaths (or failed spawns/verifies) without a settle at
	// MyVersion, the Supervisor stops reviving and Retreats. Zero disables it.
	ReviveBreaker int

	// failures counts consecutive spawn failures, driving SpawnBackoff.
	// Tick-local, as are all fields below: single-goroutine, no lock.
	failures int
	retryAt  time.Time
	// reviveHazard counts CONSECUTIVE deaths that never restored the child at
	// MyVersion; reset ONLY by a settle at MyVersion or a death past HazardWindow.
	reviveHazard int
	lastReviveAt time.Time
	// sawUnhealthy edge-triggers the death transition: ChildDied and the hazard
	// increment fire once per episode. A wedged tick deliberately leaves it unset,
	// so a spared child never advances the death count.
	sawUnhealthy bool
	// spawnedSkew is the version our own spawns produce when it differs from
	// MyVersion (binary swapped under a running supervisor). Tick treats it as
	// settled — never re-replaced — but it must NOT reset the crash-loop breaker.
	spawnedSkew string
}

const defaultHazardWindow = 30 * time.Minute

// Validate reports the first missing required field; call it once before driving
// the supervision loop.
func (sv *Supervisor) Validate() error {
	switch {
	case sv.Policy == nil:
		return errors.New("proc.Supervisor: Policy is required")
	case sv.MyVersion == "":
		return errors.New("proc.Supervisor: MyVersion is required")
	case sv.Spawn.Available == nil:
		return errors.New("proc.Supervisor: Spawn.Available is required")
	case sv.Spawn.CanHost == nil:
		return errors.New("proc.Supervisor: Spawn.CanHost is required")
	}
	return nil
}

// Tick runs one supervision pass: revive a dead child, spare a wedged one,
// replace a skewed one. The consumer calls it from its own supervision loop.
func (sv *Supervisor) Tick(ctx context.Context) {
	v := sv.Policy.Probe()
	if !v.Reachable {
		sv.revive(ctx)
		return
	}
	if v.Degraded {
		// Degraded is alive, not dead: clear the death-episode flag so a
		// dead->degraded->dead oscillation re-fires ChildDied. A skewed degraded
		// child is actively failing — converge it past the soft gate (force); an
		// our-version one is a transient the consumer's steady-state heal handles.
		sv.sawUnhealthy = false
		if sv.isSkew(v.Version) {
			sv.Replace(ctx, true)
		}
		return
	}
	sv.sawUnhealthy = false
	if v.Version == "" {
		// Version unknown (a discarded poll) is not skew evidence.
		return
	}
	if !sv.isSkew(v.Version) {
		// Settled: our version, or the one our own spawns mint (re-replacing would
		// mint it again forever). The breaker resets ONLY on a MyVersion settle — a
		// spawnedSkew settle is the stuck-old-child loop it exists to catch.
		sv.resetSpawnBackoff()
		if v.Version == sv.MyVersion {
			sv.reviveHazard = 0
		}
		return
	}
	sv.Replace(ctx, false)
}

func (sv *Supervisor) isSkew(ver string) bool {
	return ver != "" && ver != sv.MyVersion && ver != sv.spawnedSkew
}

// IsSkew reports whether ver is a version this Supervisor would replace:
// neither MyVersion nor SpawnedSkew, and never "" (an unknown poll). Safe to
// call only from the goroutine that drives Tick.
func (sv *Supervisor) IsSkew(ver string) bool {
	return sv.isSkew(ver)
}

// revive handles an unreachable child, split on PeerAlive — the gate before
// every destructive arm. No peer is genuinely dead: signal ChildDied, advance
// the breaker, respawn under backoff. A live peer is alive-but-wedged: SPARED —
// only the explicit force-Replace path may peer-gated-Kill it.
func (sv *Supervisor) revive(ctx context.Context) {
	alive := sv.Policy.PeerAlive()
	switch {
	case alive:
		// Wedged: spared — no death count, no ChildDied. Not a free pass: its held
		// socket defeats the Spawn's Available short-circuit, so the spawn below
		// "succeeds", the verify fails, and the breaker still retreats it if it
		// never recovers.
	case !sv.sawUnhealthy:
		sv.sawUnhealthy = true
		now := time.Now()
		if !sv.lastReviveAt.IsZero() && now.Sub(sv.lastReviveAt) > sv.hazardWindow() {
			sv.reviveHazard = 0
		}
		sv.lastReviveAt = now
		sv.reviveHazard++
		// Force-unmount the dead carcasses before the respawn remounts them.
		sv.Policy.Reconcile(ctx, ReconcileEvent{Kind: ChildDied})
	}
	if sv.ReviveBreaker > 0 && sv.reviveHazard >= sv.ReviveBreaker {
		// Crash loop: every revive loses the consumer's in-flight state — stop the
		// churn and retreat.
		sv.Policy.Retreat(ctx, reviveBreakerReason)
		return
	}
	if time.Now().Before(sv.retryAt) {
		return
	}
	if err := sv.Spawn.EnsureRunning(); err != nil {
		if errors.Is(err, ErrSkipSpawn) {
			// Intentionally nothing to serve: benign — no backoff, no breaker, no
			// OnSpawnError.
			return
		}
		sv.noteSpawnFailure(err)
		if sv.ReviveBreaker > 0 && sv.failures >= sv.ReviveBreaker {
			sv.Policy.Retreat(ctx, spawnFailBreakerReason)
		}
		return
	}
	if !sv.verifySpawned() {
		if sv.ReviveBreaker > 0 && sv.failures >= sv.ReviveBreaker {
			sv.Policy.Retreat(ctx, verifyFailBreakerReason)
		}
		return
	}
	sv.sawUnhealthy = false
	sv.Policy.Reconcile(ctx, ReconcileEvent{Kind: Respawned})
}

// Replace retires a skewed (or degraded-skewed, under force) child and brings a
// fresh one up at MyVersion: ReplaceSafe opens the consumer's claim critical
// section, then Shutdown -> WaitGone -> (peer-gated reap on a wedge) -> spawn ->
// verify. Once the gate opens it fires EXACTLY ONE terminal Reconcile on EVERY
// path (ReplaceSucceeded on a verified replacement, else ReplaceAborted), so the
// consumer always releases its held claims. A deferral (ReplaceSafe returns a
// reason) opens no gate, holds no claims, fires no finalizer, and returns true;
// every acted-on path returns false.
func (sv *Supervisor) Replace(ctx context.Context, force bool) (deferred bool) {
	if reason := sv.Policy.ReplaceSafe(ctx, force); reason != "" {
		return true
	}
	// The gate cleared: the consumer now holds its claims, so EXACTLY ONE
	// finalizer must fire. fired guards the single delivery; the defer is the
	// catch-all for every early/error/panic-free return path.
	fired := false
	finalize := func(kind ReconcileKind) {
		if fired {
			return
		}
		fired = true
		sv.Policy.Reconcile(ctx, ReconcileEvent{Kind: kind})
	}
	defer finalize(ReplaceAborted)

	if ctx.Err() != nil {
		return
	}
	if err := sv.Policy.Shutdown(ctx); err != nil {
		// The child sweeps before the Shutdown reply lands, so an errored RPC is
		// outcome-unknown, not nothing-happened: wait it out before deciding. A
		// non-force replace that finds the child still serving defers to the next
		// tick (it may never have received the Shutdown); a force replace falls
		// through to the reap (a child that will not ack is exactly the wedge the
		// peer-gated kill exists for).
		if !sv.Policy.WaitGone(ctx, sv.goneWait()) {
			if ctx.Err() != nil {
				return
			}
			if !force {
				return
			}
			if !sv.reapWedged(ctx) {
				return
			}
		}
	} else if !sv.Policy.WaitGone(ctx, sv.goneWait()) {
		if ctx.Err() != nil {
			return
		}
		if !sv.reapWedged(ctx) {
			return
		}
	}
	if ctx.Err() != nil {
		return
	}
	if err := sv.Spawn.EnsureRunning(); err != nil {
		// ErrSkipSpawn during a replace is a benign "nothing to serve" no-op — do not
		// book it as a failure.
		if !errors.Is(err, ErrSkipSpawn) {
			sv.noteSpawnFailure(err)
		}
		return
	}
	if !sv.verifySpawned() {
		return
	}
	finalize(ReplaceSucceeded)
	return false
}

// reapWedged is the SIGKILL escape hatch for a child that acked Shutdown but
// kept its socket past the gone-wait; the kill is peer-gated (see Policy.Kill).
// Reports whether the socket is now free for the successor spawn.
func (sv *Supervisor) reapWedged(ctx context.Context) bool {
	_, err := sv.Policy.Kill()
	switch {
	case errors.Is(err, ErrChildUnavailable):
		// Released between WaitGone's last probe and now — nothing to kill.
		return true
	case err != nil:
		// Unverifiable or changed peer: defer to the next tick.
		return false
	}
	return sv.Policy.WaitGone(ctx, sv.goneWait())
}

// verifySpawned re-probes after a spawn that reported success: a socket held open
// by an unresponsive process defeats the Spawn's Available short-circuit, and a
// child that answers Health but fails its secondary readiness check (Degraded)
// has not finished coming up. Believing success only on a Reachable, non-Degraded
// Probe means a child that spawns but never becomes ready trips the verify-fail
// breaker instead of the consumer remounting against a not-ready child.
func (sv *Supervisor) verifySpawned() bool {
	v := sv.Policy.Probe()
	if !v.Reachable || v.Degraded {
		sv.noteSpawnFailure(fmt.Errorf("%w: spawn reported success but the child on %s is not ready (unreachable or degraded — socket held by an unresponsive process, or a secondary readiness check still failing?)", ErrChildUnavailable, sv.Spawn.Socket))
		return false
	}
	sv.resetSpawnBackoff()
	sv.noteSpawnedVersion(v.Version)
	return true
}

// noteSpawnedVersion records the version a supervisor-initiated spawn settled at
// (the reverse-skew spawnedSkew when an upgrade swapped the binary under this
// running supervisor). An empty version (a discarded poll) is left for the next
// tick.
func (sv *Supervisor) noteSpawnedVersion(ver string) {
	switch {
	case ver == "":
	case ver == sv.MyVersion:
		sv.spawnedSkew = ""
	default:
		sv.spawnedSkew = ver
	}
}

// noteSpawnFailure books one failed spawn (or verify) attempt against the backoff
// and surfaces it through OnSpawnError.
func (sv *Supervisor) noteSpawnFailure(err error) {
	sv.failures++
	sv.retryAt = time.Now().Add(sv.SpawnBackoff.After(sv.failures))
	if sv.OnSpawnError != nil {
		sv.OnSpawnError(err, sv.failures, sv.retryAt)
	}
}

// SpawnedSkew reports the reverse-skew version the Supervisor's own spawns
// currently settle at — non-empty only when an upgrade swapped the on-disk binary
// under this running supervisor (Tick treats it as settled, never re-replaced). A
// consumer uses it to tell a TRUE reverse-skew settle (worth a "restart to
// converge" operator note) from a forward-skew child it is still actively trying
// to replace. Safe to call only from the goroutine that drives Tick.
func (sv *Supervisor) SpawnedSkew() string {
	return sv.spawnedSkew
}

// resetSpawnBackoff clears the respawn backoff after a verified bring-up.
func (sv *Supervisor) resetSpawnBackoff() {
	sv.failures = 0
	sv.retryAt = time.Time{}
}

// ClearBackoff drops the spawn backoff floor so the next Tick attempts a spawn
// immediately, without changing the crash-loop breaker count — for a caller that
// observed the host become available again. Safe to call only from the single
// goroutine that drives Tick.
func (sv *Supervisor) ClearBackoff() {
	sv.retryAt = time.Time{}
}

// hazardWindow resolves the consecutive-death window, defaulting a zero
// HazardWindow.
func (sv *Supervisor) hazardWindow() time.Duration {
	if sv.HazardWindow > 0 {
		return sv.HazardWindow
	}
	return defaultHazardWindow
}

// goneWait is the per-leg wait for a retiring child to release its socket (after
// Shutdown, and after a reap SIGKILL), distinct from the fresh child's come-up
// bound (Spawn.Timeout).
func (sv *Supervisor) goneWait() time.Duration {
	if sv.GoneWait > 0 {
		return sv.GoneWait
	}
	if sv.Spawn.Timeout > 0 {
		return sv.Spawn.Timeout
	}
	return DefaultSpawnTimeout
}
