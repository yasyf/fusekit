package proc

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ReconcileKind names the terminal/edge transitions the Supervisor reports to
// its consumer through Reconcile. The consumer re-establishes desired state
// (remounts, claim release) keyed on the kind — the Supervisor owns only the
// generic mechanism that decides WHEN each fires.
type ReconcileKind int

const (
	// ChildDied means the Supervisor concluded the child is genuinely gone (no
	// peer) and is about to revive it. The accompanying ReconcileEvent.Alive is
	// false on this path; the consumer force-unmounts orphaned carcasses / drops
	// stale state before the respawn. It NEVER fires for an alive-but-wedged
	// child (that one is spared — see contract 1).
	ChildDied ReconcileKind = iota
	// Respawned means a dead child was respawned and verified at a usable
	// version; the consumer re-establishes whatever the fresh child must serve.
	Respawned
	// ReplaceSucceeded is the Replace finalizer on a verified replacement: the
	// old child stepped down (or was reaped) and a fresh one came up. The
	// consumer remounts under its held claims, then releases them.
	ReplaceSucceeded
	// ReplaceAborted is the Replace finalizer on any non-success exit — an early
	// gate failure, an RPC error, a ctx cancellation, a failed spawn/verify. The
	// consumer releases its held claims without remounting. Exactly one of
	// ReplaceSucceeded / ReplaceAborted fires per Replace call (contract 3).
	ReplaceAborted
)

// Verdict is the consumer's reachability verdict for the child, produced by
// Policy.Probe. The Supervisor routes on it and NEVER re-derives reachability
// from raw call outcomes — the consumer owns the Health[+secondary] exchange
// (e.g. mountd.Client.Poll) and distills it here.
type Verdict struct {
	// Reachable is true when the child answered its primary health check: the
	// socket is live and the child is responsive. False means gone or wedged.
	Reachable bool
	// Degraded is true when the primary health check answered but a secondary
	// readiness check failed — alive at a known Version, but not fully ready
	// (e.g. mountd's List blew its deadline). False when the child has no
	// secondary readiness check, or it passed.
	Degraded bool
	// Version is the child's reported version, set whenever Reachable.
	Version string
}

// ReconcileEvent is one transition delivered to Policy-wired Reconcile.
type ReconcileEvent struct {
	// Kind names the transition.
	Kind ReconcileKind
	// Alive carries the dead-vs-wedged split for ChildDied: false means the
	// child was genuinely gone (no peer) when the Supervisor decided to revive.
	// Unused by the other kinds.
	Alive bool
}

// Policy is the consumer-supplied decision surface the Supervisor consults. It
// keeps the Supervisor generic: every consumer-specific judgement (what
// "ready" means, when a replace is safe, what retreat does) lives behind these
// hooks while the Supervisor owns only the generic state machine.
type Policy interface {
	// Probe returns the consumer's reachability Verdict for the child (one
	// Health[+secondary] exchange). The Supervisor routes on this and never
	// re-derives reachability itself.
	Probe() Verdict
	// PeerAlive reports whether the child's socket still has a live peer — the
	// dead(revive)-vs-wedged(spare) split. It is the meltdown gate consulted
	// before every destructive arm: false means genuinely dead (revive + the
	// ChildDied force-unmount signal); true means alive-but-wedged (spare it).
	PeerAlive() bool
	// ReplaceSafe opens the consumer's claim critical section for a Replace:
	// "" means clear (the consumer now HOLDS its claims and the Supervisor may
	// retire the child); a non-"" reason defers (no claims held). force lets the
	// consumer bypass its softer legs (e.g. a busy/uptime gate) while every
	// claim-safety leg still holds. Called once per Replace.
	ReplaceSafe(ctx context.Context, force bool) (reason string)
	// Retreat is the crash-loop breaker action: after the breaker trips, the
	// Supervisor stops reviving and calls this so the consumer falls back to an
	// always-available path (cc-pool: fuse->symlink; cc-squash: retrieve-only).
	// reason is the breaker context for the consumer's log line.
	Retreat(ctx context.Context, reason string)
}

// reviveBreakerReason / spawnFailBreakerReason / verifyFailBreakerReason are the
// Retreat reasons for each crash-loop dead end, so the consumer's log line
// names which one tripped.
const (
	reviveBreakerReason     = "child crash-looped without ever returning at this version"
	spawnFailBreakerReason  = "child will not spawn"
	verifyFailBreakerReason = "child spawns but never passes its health check"
)

// Supervisor watches a detached, versioned child reached over a socket and
// keeps it alive at MyVersion: it revives a genuinely dead child (under spawn
// backoff and a crash-loop breaker), spares an alive-but-wedged one, and
// replaces a version-skewed child once the consumer's claim gate clears. It
// owns ONLY the generic mechanism — every consumer-specific judgement is wired
// through Policy and the child-control callbacks, so one Supervisor drives both
// cc-pool's mount-holder and cc-squash's holder+proxy.
//
// The state machine is single-goroutine: Run's loop and the tests' direct
// Tick/Replace calls are the only mutators, so the bookkeeping fields carry no
// lock.
type Supervisor struct {
	// Spawn is the detached-child spawn (proc.Spawn). Used by every revive and
	// replace to bring a fresh child up.
	Spawn Spawn
	// MyVersion is this supervisor's own version. Skew is Verdict.Version !=
	// MyVersion; the crash-loop breaker resets ONLY on a healthy settle at
	// MyVersion (contract 2).
	MyVersion string
	// Policy is the consumer decision surface (Probe/PeerAlive/ReplaceSafe/
	// Retreat). Required.
	Policy Policy
	// Shutdown asks the child to step down for a graceful replace. ctx-aware so
	// a consumer shutdown never stalls behind it. Required.
	Shutdown func(ctx context.Context) error
	// WaitGone reports whether the child released its socket within d (ctx-aware).
	// Required.
	WaitGone func(ctx context.Context, d time.Duration) bool
	// Kill is the PEER-GATED force kill: the consumer captured the child's pid at
	// gate time and kills ONLY that pid (a successor that bound the socket in
	// between is refused), returning the killed pid. Reached only on the
	// force-Replace reap path. Required.
	Kill func() (pid int, err error)
	// Reconcile delivers each transition (ChildDied / Respawned / the Replace
	// finalizers) so the consumer re-establishes desired state and releases its
	// claims. Required.
	Reconcile func(ctx context.Context, ev ReconcileEvent)
	// OnSpawnError, when non-nil, is called with each Spawn.EnsureRunning /
	// verifySpawned failure the Supervisor books against its backoff. The
	// Supervisor owns the retry policy (backoff, breaker); this hook only lets
	// the consumer surface the failure (a status field, a once-per-text log) —
	// it must take no irreversible action. nil discards the failures. A
	// successful bring-up does NOT call it; the consumer clears its own surface
	// on the next settle/Respawned.
	OnSpawnError func(err error)
	// Interval is the supervision cadence for Run. Zero means a sensible default.
	Interval time.Duration
	// HazardWindow bounds what counts as a CONSECUTIVE death for the crash-loop
	// breaker: two deaths farther apart start a fresh cluster (the hazard counter
	// resets). Zero means a sensible default.
	HazardWindow time.Duration
	// SpawnBackoff bounds the respawn backoff after consecutive spawn failures.
	SpawnBackoff Backoff
	// ReviveBreaker is the crash-loop circuit-breaker threshold: after this many
	// CONSECUTIVE deaths (or failed spawns/verifies) without the child ever
	// settling at MyVersion, the Supervisor stops reviving and calls
	// Policy.Retreat. Zero disables the breaker.
	ReviveBreaker int

	// --- tick-local state (single-goroutine; no lock) ---

	// failures counts consecutive spawn failures, driving SpawnBackoff.
	failures int
	// retryAt is the backoff floor: the earliest next spawn attempt.
	retryAt time.Time
	// reviveHazard counts CONSECUTIVE deaths/failed-revives that never restored
	// the child at MyVersion — the crash-loop signal behind ReviveBreaker. Reset
	// ONLY by a settled tick at MyVersion, or a death past HazardWindow.
	reviveHazard int
	// lastReviveAt timestamps the most recent death transition, so a death past
	// HazardWindow starts a fresh cluster.
	lastReviveAt time.Time
	// sawUnhealthy records that the last tick found the child genuinely dead, so
	// the death transition (and its hazard increment) fires once per episode.
	sawUnhealthy bool
	// sawWedgedAlive records the alive-but-wedged episode, so the death counter
	// is never advanced by an alive child.
	sawWedgedAlive bool
	// spawnedSkew is the version the Supervisor's own spawns produce when it
	// differs from MyVersion (a binary swapped under a running supervisor): the
	// reverse-skew steady state Tick must never re-replace, and which must NOT
	// reset the crash-loop breaker (contract 2).
	spawnedSkew string
}

// defaultInterval / defaultHazardWindow back the zero-value Interval /
// HazardWindow.
const (
	defaultInterval     = 10 * time.Second
	defaultHazardWindow = 30 * time.Minute
)

// Run supervises the child until ctx is cancelled, ticking every Interval.
// Started after the consumer's initial reconcile so it never races the first
// bring-up.
func (sv *Supervisor) Run(ctx context.Context) {
	interval := sv.Interval
	if interval <= 0 {
		interval = defaultInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sv.Tick(ctx)
		}
	}
}

// Tick runs one supervision pass: route the child on Policy.Probe (never
// re-deriving reachability), reviving a dead child, sparing a wedged one, and
// replacing a skewed one. Split from Run's loop so tests drive ticks
// deterministically.
func (sv *Supervisor) Tick(ctx context.Context) {
	v := sv.Policy.Probe()
	if !v.Reachable {
		sv.revive(ctx)
		return
	}
	if v.Degraded {
		// Alive at a known version but not fully ready. A skewed degraded child is
		// actively failing, so converge it onto our version past the soft gate
		// (force); an our-version (or our-spawn) degraded child is a transient blip
		// the consumer's steady-state heal handles, never a replace.
		if sv.isSkew(v.Version) {
			sv.Replace(ctx, true)
		}
		return
	}
	sv.sawUnhealthy, sv.sawWedgedAlive = false, false
	if v.Version == "" {
		// Healthy but version unknown (a discarded poll): not skew evidence. The
		// next tick restores polled truth.
		return
	}
	if !sv.isSkew(v.Version) {
		// Steady state: healthy at our version, or at the version our own spawns
		// produce (re-replacing would mint the same version forever). Reset the
		// spawn backoff; clear the crash-loop breaker ONLY at MyVersion — a
		// spawnedSkew settle is the very stuck-old-child loop the breaker exists
		// for, so it must NOT reset the count (contract 2).
		sv.resetSpawnBackoff()
		if v.Version == sv.MyVersion {
			sv.reviveHazard = 0
		}
		return
	}
	sv.Replace(ctx, false)
}

// isSkew reports whether ver is a version the Supervisor should replace: any
// version that is neither MyVersion nor the version our own spawns settle at
// (spawnedSkew). An empty ver is never skew (it is an unknown poll).
func (sv *Supervisor) isSkew(ver string) bool {
	return ver != "" && ver != sv.MyVersion && ver != sv.spawnedSkew
}

// revive handles a child whose Probe came back unreachable. It splits on
// PeerAlive: no peer is a genuinely dead child — the consumer is signalled to
// force-unmount its carcasses (Reconcile ChildDied, Alive=false), the
// crash-loop breaker advances, and a fresh child is spawned under backoff — while
// a live peer is an alive-but-wedged child, which is SPARED (no destructive
// action, no death-count advance): only the explicit force-Replace path may
// peer-gated-Kill it. This is contract 1: PeerAlive gates EVERY destructive arm.
func (sv *Supervisor) revive(ctx context.Context) {
	alive := sv.Policy.PeerAlive()
	switch {
	case alive:
		// Alive but unresponsive: SPARE its destructive side effects. Mark the
		// episode so the death counter is never advanced for it, and never fire
		// the ChildDied force-unmount signal (no Reconcile, no Kill). It is NOT
		// given a free pass though — its held socket defeats the Spawn's Available
		// short-circuit, so the spawn below "succeeds", the verify fails, and the
		// spawn-fail breaker still retreats it if it never recovers. Only the
		// explicit force-Replace path may peer-gated-Kill a wedged child.
		sv.sawWedgedAlive = true
	case !sv.sawUnhealthy:
		sv.sawUnhealthy = true
		now := time.Now()
		if !sv.lastReviveAt.IsZero() && now.Sub(sv.lastReviveAt) > sv.hazardWindow() {
			// The previous death was long ago — not the same crash loop. Start a
			// fresh cluster so far-apart transient deaths never accumulate.
			sv.reviveHazard = 0
		}
		sv.lastReviveAt = now
		sv.reviveHazard++
		// The child is genuinely gone: signal the consumer to force-unmount the
		// dead carcasses before the respawn remounts them fresh. Done on the death
		// transition (not per tick) so a wedged-carcass hazard is cleared the
		// moment it appears. The alive branch never reaches here — contract 1.
		sv.Reconcile(ctx, ReconcileEvent{Kind: ChildDied, Alive: false})
	}
	if sv.ReviveBreaker > 0 && sv.reviveHazard >= sv.ReviveBreaker {
		// The child keeps dying without ever returning at our version: every revive
		// loses the consumer's in-flight state and the breaker exists to stop the
		// churn. Retreat instead of reviving again.
		sv.Policy.Retreat(ctx, reviveBreakerReason)
		return
	}
	if time.Now().Before(sv.retryAt) {
		return
	}
	if err := sv.Spawn.EnsureRunning(); err != nil {
		sv.noteSpawnFailure(err)
		if sv.ReviveBreaker > 0 && sv.failures >= sv.ReviveBreaker {
			// The child will not spawn at all (its host became unavailable): retreat
			// so the consumer keeps working without it.
			sv.Policy.Retreat(ctx, spawnFailBreakerReason)
		}
		return
	}
	if !sv.verifySpawned() {
		if sv.ReviveBreaker > 0 && sv.failures >= sv.ReviveBreaker {
			// Spawn reports success but the child never passes its health check (a
			// socket held by an unresponsive process): same dead end as a failed
			// spawn — retreat.
			sv.Policy.Retreat(ctx, verifyFailBreakerReason)
		}
		return
	}
	sv.sawUnhealthy, sv.sawWedgedAlive = false, false
	sv.Reconcile(ctx, ReconcileEvent{Kind: Respawned})
}

// Replace retires a skewed (or degraded-skewed, under force) child and brings a
// fresh one up at MyVersion: ReplaceSafe opens the consumer's claim critical
// section, then Shutdown -> WaitGone -> (peer-gated reap on a wedge) -> spawn ->
// verify. It fires EXACTLY ONE terminal Reconcile on EVERY path —
// ReplaceSucceeded on a verified replacement, ReplaceAborted on any early /
// error / ctx-cancel exit — so the consumer always releases its claims
// (contract 3). When ReplaceSafe defers (returns a reason) the gate never
// opened, no claims are held, and NO finalizer fires; Replace returns true to
// signal the deferral so the caller can run its steady-state heal.
//
// Returns true ONLY when the replace deferred on a blocked gate (the consumer
// holds no claims). Every acted-on path — a clean replace or any failure after
// the gate cleared — returns false.
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
		sv.Reconcile(ctx, ReconcileEvent{Kind: kind})
	}
	defer finalize(ReplaceAborted)

	if ctx.Err() != nil {
		return
	}
	if err := sv.Shutdown(ctx); err != nil {
		// The child sweeps before the Shutdown reply lands, so an errored RPC is
		// outcome-unknown, not nothing-happened: wait it out before deciding. A
		// non-force replace that finds the child still serving defers to the next
		// tick (it may never have received the Shutdown); a force replace falls
		// through to the reap (a child that will not ack is exactly the wedge the
		// peer-gated kill exists for).
		if !sv.WaitGone(ctx, sv.spawnTimeout()) {
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
	} else if !sv.WaitGone(ctx, sv.spawnTimeout()) {
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
		sv.noteSpawnFailure(err)
		return
	}
	if !sv.verifySpawned() {
		return
	}
	finalize(ReplaceSucceeded)
	return false
}

// reapWedged is the SIGKILL escape hatch for a child that acked Shutdown but
// kept its socket past the gone-wait. It is peer-gated through Kill: the
// consumer resolves the socket's current peer and kills only the pid captured
// at gate time — a successor that bound the socket in between is refused
// (ErrHolderUnavailable from an unreachable/changed peer means nothing to kill).
// Reports whether the socket is now free for the successor spawn.
func (sv *Supervisor) reapWedged(ctx context.Context) bool {
	_, err := sv.Kill()
	switch {
	case errors.Is(err, ErrHolderUnavailable):
		// Released between WaitGone's last probe and now — nothing to kill.
		return true
	case err != nil:
		// Unverifiable or changed peer: defer to the next tick.
		return false
	}
	return sv.WaitGone(ctx, sv.spawnTimeout())
}

// verifySpawned re-probes after a spawn that reported success and confirms the
// child actually answers: a socket held open by an unresponsive process defeats
// the Spawn's Available short-circuit, so success is believed only when the
// fresh Probe vouches for it. A genuine success resets the backoff and records
// the version the spawn settled at (noteSpawnedVersion); a failure books the
// attempt against the backoff.
func (sv *Supervisor) verifySpawned() bool {
	v := sv.Policy.Probe()
	if !v.Reachable {
		sv.noteSpawnFailure(fmt.Errorf("%w: spawn reported success but the child on %s failed its health check (socket held by an unresponsive process?)", ErrHolderUnavailable, sv.Spawn.Socket))
		return false
	}
	sv.resetSpawnBackoff()
	sv.noteSpawnedVersion(v.Version)
	return true
}

// noteSpawnedVersion records the version a supervisor-initiated spawn settled
// at. The spawn execs the binary at the child's install path, which an upgrade
// may have swapped under this still-running supervisor — so a fresh child can
// report a NEWER version than ours. That version is this supervisor's steady
// state, not grounds for another replace (re-replacing would exec the same
// binary, observe the same skew, and churn forever), so Tick treats a child at
// spawnedSkew as settled. An empty version (a discarded poll) is left for the
// next tick.
func (sv *Supervisor) noteSpawnedVersion(ver string) {
	switch {
	case ver == "":
	case ver == sv.MyVersion:
		sv.spawnedSkew = ""
	default:
		sv.spawnedSkew = ver
	}
}

// noteSpawnFailure books one failed spawn (or verify) attempt against the
// backoff: the failure count grows and the next attempt waits out the doubled
// window. The failing err is surfaced through OnSpawnError (when wired) so the
// consumer can render it; the Supervisor itself only books the backoff.
func (sv *Supervisor) noteSpawnFailure(err error) {
	sv.failures++
	sv.retryAt = time.Now().Add(sv.SpawnBackoff.After(sv.failures))
	if sv.OnSpawnError != nil {
		sv.OnSpawnError(err)
	}
}

// resetSpawnBackoff clears the respawn backoff after a verified bring-up.
func (sv *Supervisor) resetSpawnBackoff() {
	sv.failures = 0
	sv.retryAt = time.Time{}
}

// ClearBackoff drops the spawn backoff floor so the next Tick attempts a spawn
// immediately, without changing the crash-loop breaker count. It exists for
// callers (and tests) that need to force a retry now — e.g. a consumer that
// observed the host become available again — and is safe to call only from the
// single goroutine that drives Tick/Run.
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

// spawnTimeout is the per-leg WaitGone/spawn wait, reusing the Spawn's
// configured timeout (defaulting to DefaultSpawnTimeout).
func (sv *Supervisor) spawnTimeout() time.Duration {
	if sv.Spawn.Timeout > 0 {
		return sv.Spawn.Timeout
	}
	return DefaultSpawnTimeout
}
