package proc

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type fakeChild struct {
	verdict   Verdict
	peerAlive bool
	safe      string // ReplaceSafe reason ("" = clear)

	probes    int
	peers     int
	safes     int
	lastForce bool
	retreats  []string

	// onProbe runs before each Probe returns; flips the verdict mid-tick.
	onProbe func(c *fakeChild)
	// onSafe runs when ReplaceSafe decides to clear; cancels mid-critical-section.
	onSafe func()

	// Wired by newSupervisor.
	cl  *callLog
	rec *recorder
}

func (c *fakeChild) Probe() Verdict {
	c.probes++
	if c.onProbe != nil {
		c.onProbe(c)
	}
	return c.verdict
}

func (c *fakeChild) PeerAlive() bool {
	c.peers++
	return c.peerAlive
}

func (c *fakeChild) ReplaceSafe(ctx context.Context, force bool) string {
	c.safes++
	c.lastForce = force
	if c.safe == "" && c.onSafe != nil {
		c.onSafe()
	}
	return c.safe
}

func (c *fakeChild) Retreat(ctx context.Context, reason string) {
	c.retreats = append(c.retreats, reason)
}

func (c *fakeChild) Shutdown(ctx context.Context) error {
	c.cl.shutdowns++
	return c.cl.shutdownErr
}

func (c *fakeChild) WaitGone(ctx context.Context, d time.Duration) bool {
	c.cl.waitGones++
	if c.cl.waitGoneFn != nil {
		return c.cl.waitGoneFn()
	}
	return c.cl.gone
}

func (c *fakeChild) Kill() (int, error) {
	c.cl.kills++
	return 123, c.cl.killErr
}

func (c *fakeChild) Reconcile(ctx context.Context, ev ReconcileEvent) {
	c.rec.reconcile(ctx, ev)
}

// recorder captures every Reconcile event in order.
type recorder struct{ events []ReconcileEvent }

func (r *recorder) reconcile(ctx context.Context, ev ReconcileEvent) {
	r.events = append(r.events, ev)
}

func (r *recorder) count(kind ReconcileKind) int {
	n := 0
	for _, e := range r.events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// liveSocket binds a real unix listener so Spawn.EnsureRunning's Available probe
// short-circuits (a child is "already serving").
func liveSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-sup")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return socket
}

// newSupervisor wires a Supervisor whose Spawn always "succeeds" (a live socket
// makes Available short-circuit), with injectable child-control callbacks the
// tests count.
func newSupervisor(t *testing.T, c *fakeChild, rec *recorder) (*Supervisor, *callLog) {
	t.Helper()
	socket := liveSocket(t)
	cl := &callLog{}
	c.cl = cl
	c.rec = rec
	sv := &Supervisor{
		Spawn: Spawn{
			Socket:    socket,
			Available: func() bool { return true },
			CanHost:   func() error { return nil },
		},
		MyVersion:     "v2",
		Policy:        c,
		SpawnBackoff:  Backoff{Base: 10 * time.Second, Cap: 10 * time.Minute},
		HazardWindow:  30 * time.Minute,
		ReviveBreaker: 3,
	}
	return sv, cl
}

// callLog counts the child-control callbacks and programs their outcomes;
// fakeChild's Shutdown/WaitGone/Kill methods drive it.
type callLog struct {
	shutdowns   int
	waitGones   int
	kills       int
	shutdownErr error
	gone        bool // default WaitGone return
	killErr     error
	// waitGoneFn, when set, overrides gone (a toggling WaitGone: wedged, then free after a reap).
	waitGoneFn func() bool
}

func TestSpawnBackoffDoublingAndCap(t *testing.T) {
	sv := &Supervisor{SpawnBackoff: Backoff{Base: 10 * time.Second, Cap: 40 * time.Second}}
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 40 * time.Second}
	for i, w := range want {
		before := time.Now()
		sv.noteSpawnFailure(nil)
		got := sv.retryAt.Sub(before)
		// retryAt is now + After(failures); allow a little slack for the clock.
		if got < w-time.Second || got > w+time.Second {
			t.Errorf("failure %d: backoff ~%v, want ~%v", i+1, got, w)
		}
	}
}

// TestOnSpawnErrorSurfacesFailures pins that every booked spawn/verify failure
// reaches OnSpawnError, and a clean bring-up never calls it.
func TestOnSpawnErrorSurfacesFailures(t *testing.T) {
	t.Run("verify failure is surfaced", func(t *testing.T) {
		c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
		rec := &recorder{}
		sv, _ := newSupervisor(t, c, rec)
		var got []error
		sv.OnSpawnError = func(err error, attempt int, nextRetry time.Time) { got = append(got, err) }
		sv.Tick(context.Background())
		if len(got) != 1 {
			t.Fatalf("OnSpawnError fired %d times, want 1", len(got))
		}
		if !errors.Is(got[0], ErrChildUnavailable) {
			t.Fatalf("surfaced err = %v, want it to wrap ErrChildUnavailable", got[0])
		}
	})

	t.Run("clean bring-up never calls it", func(t *testing.T) {
		c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
		rec := &recorder{}
		sv, _ := newSupervisor(t, c, rec)
		c.onProbe = func(c *fakeChild) {
			if c.probes >= 2 {
				c.verdict = Verdict{Reachable: true, Version: "v2"}
			}
		}
		called := false
		sv.OnSpawnError = func(error, int, time.Time) { called = true }
		sv.Tick(context.Background())
		if called {
			t.Fatal("OnSpawnError fired on a clean bring-up")
		}
	})
}

// TestClearBackoffDropsFloorButNotBreaker pins that ClearBackoff drops the
// spawn-backoff floor (the next Tick retries at once) WITHOUT touching the
// crash-loop breaker.
func TestClearBackoffDropsFloorButNotBreaker(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	rec := &recorder{}
	sv, _ := newSupervisor(t, c, rec)
	// probes is the spawn-attempt detector: a gated tick runs only the route Probe;
	// a spawn adds a second verify probe.
	c.onProbe = func(c *fakeChild) { c.verdict = Verdict{Reachable: false} }

	sv.Tick(context.Background())
	if sv.failures != 1 {
		t.Fatalf("after one failing tick failures = %d, want 1", sv.failures)
	}
	if sv.reviveHazard != 1 {
		t.Fatalf("after one failing tick reviveHazard = %d, want 1", sv.reviveHazard)
	}
	if !time.Now().Before(sv.retryAt) {
		t.Fatalf("retryAt = %v is not in the future; backoff floor was not armed", sv.retryAt)
	}

	sv.sawUnhealthy = true // model the same death episode (no fresh ChildDied)
	before := c.probes
	sv.Tick(context.Background())
	if c.probes != before+1 {
		t.Fatalf("gated tick ran %d probes, want 1 (no spawn attempt while backed off)", c.probes-before)
	}
	if sv.failures != 1 {
		t.Fatalf("gated tick booked another failure (failures = %d, want 1); it should not have spawned", sv.failures)
	}

	failuresBefore, hazardBefore := sv.failures, sv.reviveHazard
	sv.ClearBackoff()
	if !sv.retryAt.IsZero() {
		t.Fatalf("ClearBackoff left retryAt = %v, want zero", sv.retryAt)
	}
	if sv.failures != failuresBefore {
		t.Fatalf("ClearBackoff changed the spawn-fail breaker count to %d, want %d", sv.failures, failuresBefore)
	}
	if sv.reviveHazard != hazardBefore {
		t.Fatalf("ClearBackoff changed the revive-hazard breaker count to %d, want %d", sv.reviveHazard, hazardBefore)
	}

	before = c.probes
	sv.Tick(context.Background())
	if c.probes != before+2 {
		t.Fatalf("post-ClearBackoff tick ran %d probes, want 2 (spawn attempted + verified)", c.probes-before)
	}
	if sv.failures != failuresBefore+1 {
		t.Fatalf("post-ClearBackoff tick failures = %d, want %d (a spawn was attempted)", sv.failures, failuresBefore+1)
	}

	for len(c.retreats) == 0 {
		sv.sawUnhealthy = false // a fresh death episode each tick
		sv.ClearBackoff()       // keep retrying immediately
		sv.Tick(context.Background())
		if sv.failures > sv.ReviveBreaker+3 {
			t.Fatalf("breaker never tripped after %d spawn failures (threshold %d); ClearBackoff wrongly reset it", sv.failures, sv.ReviveBreaker)
		}
	}
	if sv.failures < sv.ReviveBreaker {
		t.Fatalf("Retreat tripped at failures = %d, below the ReviveBreaker threshold %d", sv.failures, sv.ReviveBreaker)
	}
}

// TestHazardWindowStaleClusterResets pins that a death far enough after the
// previous one starts a FRESH crash-loop cluster (the hazard counter resets),
// so far-apart transient deaths never accumulate into a spurious breaker trip.
func TestHazardWindowStaleClusterResets(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	rec := &recorder{}
	sv, _ := newSupervisor(t, c, rec)
	sv.reviveHazard = sv.ReviveBreaker - 1
	sv.lastReviveAt = time.Now().Add(-sv.HazardWindow - time.Minute)
	sv.sawUnhealthy = false
	c.onProbe = func(c *fakeChild) {
		if c.probes >= 2 {
			c.verdict = Verdict{Reachable: true, Version: "v2"}
		}
	}

	sv.Tick(context.Background())

	if sv.reviveHazard != 1 {
		t.Fatalf("reviveHazard = %d, want 1 (a stale prior death starts a fresh cluster, not accumulates)", sv.reviveHazard)
	}
	if len(c.retreats) != 0 {
		t.Fatalf("a single fresh death after a stale cluster retreated %v, want none", c.retreats)
	}
	if rec.count(Respawned) != 1 {
		t.Fatalf("a single fresh death revived %d times, want 1 (not a fallback)", rec.count(Respawned))
	}
}

func TestReviveOnGenuinelyDead(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	rec := &recorder{}
	sv, _ := newSupervisor(t, c, rec)
	c.onProbe = func(c *fakeChild) {
		if c.probes >= 2 {
			c.verdict = Verdict{Reachable: true, Version: "v2"}
		}
	}
	sv.Tick(context.Background())
	if rec.count(ChildDied) != 1 {
		t.Fatalf("dead child: ChildDied fired %d times, want 1", rec.count(ChildDied))
	}
	if rec.count(Respawned) != 1 {
		t.Fatalf("dead child: Respawned fired %d times, want 1", rec.count(Respawned))
	}
}

// TestContract1SpareWedgedChild pins that an alive-but-unresponsive child
// (PeerAlive true, Probe unreachable) is NOT force-killed and fires no ChildDied
// on a normal Tick — the PeerAlive gate keeps the revive arm from treating it as
// dead.
func TestContract1SpareWedgedChild(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: true}
	rec := &recorder{}
	sv, cl := newSupervisor(t, c, rec)
	sv.Tick(context.Background())

	if rec.count(ChildDied) != 0 {
		t.Errorf("wedged-alive child fired ChildDied %d times, want 0 (contract 1)", rec.count(ChildDied))
	}
	if cl.kills != 0 {
		t.Errorf("wedged-alive child was killed %d times on a normal Tick, want 0 (contract 1)", cl.kills)
	}
	if sv.reviveHazard != 0 {
		t.Errorf("wedged-alive child advanced the crash-loop breaker to %d, want 0 (contract 1)", sv.reviveHazard)
	}
	if c.peers == 0 {
		t.Error("PeerAlive was never consulted — the meltdown gate was bypassed (contract 1)")
	}
}

// TestContract1ManyTicksNeverDestructive pins that a child staying wedged-alive
// across many ticks is never force-killed and never fires ChildDied.
func TestContract1ManyTicksNeverDestructive(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: true}
	rec := &recorder{}
	sv, cl := newSupervisor(t, c, rec)
	// Force spawn-verify to fail so the spawn-fail breaker path runs too — still no Kill, no ChildDied.
	c.onProbe = func(c *fakeChild) { c.verdict = Verdict{Reachable: false} }
	for range 10 {
		sv.Tick(context.Background())
		sv.retryAt = time.Time{} // clear backoff so each tick attempts a spawn
	}
	if rec.count(ChildDied) != 0 {
		t.Errorf("wedged-alive child fired ChildDied across ticks, want 0 (contract 1)")
	}
	if cl.kills != 0 {
		t.Errorf("wedged-alive child was killed across ticks, want 0 (contract 1)")
	}
}

func TestReviveBreakerTripsAndRetreats(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	rec := &recorder{}
	sv, _ := newSupervisor(t, c, rec)
	for range sv.ReviveBreaker {
		sv.sawUnhealthy = false // model a fresh death-transition each tick
		sv.retryAt = time.Time{}
		sv.Tick(context.Background())
	}
	if len(c.retreats) == 0 {
		t.Fatalf("breaker never retreated after %d consecutive deaths", sv.ReviveBreaker)
	}
}

// TestContract2BreakerNotResetBySkewedSettle pins that a child repeatedly
// settling at a skewed version (!= MyVersion) and dying still trips ReviveBreaker
// and Retreats — a naive reset-on-any-healthy would clear the counter each skewed
// settle and loop forever.
func TestContract2BreakerNotResetBySkewedSettle(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{peerAlive: false}
	sv, _ := newSupervisor(t, c, rec)
	c.onProbe = func(c *fakeChild) {
		// First probe each tick = death; the verify probe settles skewed-healthy.
		if c.probes%2 == 0 {
			c.verdict = Verdict{Reachable: true, Version: "v1"} // skewed settle
		} else {
			c.verdict = Verdict{Reachable: false} // death
		}
	}
	for range sv.ReviveBreaker {
		c.verdict = Verdict{Reachable: false}
		sv.sawUnhealthy = false
		sv.retryAt = time.Time{}
		sv.revive(context.Background())
	}
	if len(c.retreats) == 0 {
		t.Fatal("breaker never retreated: a skewed settle wrongly reset the crash-loop counter (contract 2)")
	}
}

// TestContract2HealthySettleAtMyVersionResets is the positive control: a genuine
// settle at MyVersion DOES reset the breaker, so a normal respawn never false-trips it.
func TestContract2HealthySettleAtMyVersionResets(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{peerAlive: false}
	sv, _ := newSupervisor(t, c, rec)
	sv.reviveHazard = 2
	c.verdict = Verdict{Reachable: true, Version: "v2"} // healthy at MyVersion
	sv.Tick(context.Background())
	if sv.reviveHazard != 0 {
		t.Fatalf("a settle at MyVersion left reviveHazard=%d, want 0 (contract 2 positive control)", sv.reviveHazard)
	}
}

func TestSkewTriggersReplaceGatedBySafe(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: "live sessions"}
	sv, cl := newSupervisor(t, c, rec)
	deferred := sv.Tick // Tick calls Replace internally
	_ = deferred
	sv.Tick(context.Background())
	if c.safes == 0 {
		t.Fatal("skewed child never consulted ReplaceSafe")
	}
	if cl.shutdowns != 0 {
		t.Fatal("a deferred ReplaceSafe still shut the child down")
	}
	if rec.count(ReplaceSucceeded)+rec.count(ReplaceAborted) != 0 {
		t.Fatal("a deferred replace fired a finalizer (no claims were held)")
	}

	rec2 := &recorder{}
	c2 := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	sv2, cl2 := newSupervisor(t, c2, rec2)
	cl2.gone = true
	c2.onProbe = func(c *fakeChild) {
		if c.probes >= 2 {
			c.verdict = Verdict{Reachable: true, Version: "v2"}
		}
	}
	sv2.Tick(context.Background())
	if cl2.shutdowns != 1 {
		t.Fatalf("cleared gate: Shutdown fired %d times, want 1", cl2.shutdowns)
	}
	if rec2.count(ReplaceSucceeded) != 1 {
		t.Fatalf("cleared gate: ReplaceSucceeded fired %d times, want 1", rec2.count(ReplaceSucceeded))
	}
}

// TestContract3ReplaceFinalizerExactlyOnceOnCancel pins that cancelling ctx
// mid-Replace fires EXACTLY ONE ReplaceAborted — never zero (claims leak forever),
// never two (a double finalizer).
func TestContract3ReplaceFinalizerExactlyOnceOnCancel(t *testing.T) {
	rec := &recorder{}
	ctx, cancel := context.WithCancel(context.Background())
	c := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	c.onSafe = func() { cancel() } // cancel inside the claim critical section
	sv, _ := newSupervisor(t, c, rec)

	sv.Replace(ctx, false)

	aborted, succeeded := rec.count(ReplaceAborted), rec.count(ReplaceSucceeded)
	if aborted != 1 {
		t.Fatalf("ctx-cancel mid-Replace fired %d ReplaceAborted, want exactly 1 (contract 3)", aborted)
	}
	if succeeded != 0 {
		t.Fatalf("ctx-cancel mid-Replace also fired %d ReplaceSucceeded, want 0 (contract 3)", succeeded)
	}
}

// TestContract3FinalizerFiresOnEveryPath sweeps every Replace exit and asserts
// exactly one terminal finalizer fires each time — never zero, never both.
func TestContract3FinalizerFiresOnEveryPath(t *testing.T) {
	mkChild := func() *fakeChild {
		return &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	}
	cases := []struct {
		name  string
		setup func(c *fakeChild, cl *callLog)
		force bool
		want  ReconcileKind
	}{
		{
			name: "clean replace succeeds",
			setup: func(c *fakeChild, cl *callLog) {
				cl.gone = true
				c.onProbe = func(c *fakeChild) {
					if c.probes >= 1 {
						c.verdict = Verdict{Reachable: true, Version: "v2"}
					}
				}
			},
			want: ReplaceSucceeded,
		},
		{
			name: "shutdown-ok but child never goes away (non-force) aborts",
			setup: func(c *fakeChild, cl *callLog) {
				cl.gone = false // WaitGone never observes the socket free
			},
			want: ReplaceAborted,
		},
		{
			name: "spawn-verify never comes up aborts",
			setup: func(c *fakeChild, cl *callLog) {
				cl.gone = true
				c.onProbe = func(c *fakeChild) { c.verdict = Verdict{Reachable: false} }
			},
			want: ReplaceAborted,
		},
		{
			name: "force reap kills then verifies succeeds",
			setup: func(c *fakeChild, cl *callLog) {
				cl.shutdownErr = nil
				calls := 0
				orig := cl.gone
				_ = orig
				c.onProbe = func(c *fakeChild) {
					if c.probes >= 1 {
						c.verdict = Verdict{Reachable: true, Version: "v2"}
					}
				}
				_ = calls
			},
			force: true,
			want:  ReplaceSucceeded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			c := mkChild()
			sv, cl := newSupervisor(t, c, rec)
			if tc.name == "force reap kills then verifies succeeds" {
				// WaitGone: first call false (wedged), reap Kill ok, second call true.
				n := 0
				cl.waitGoneFn = func() bool {
					n++
					return n >= 2
				}
			}
			tc.setup(c, cl)
			sv.Replace(context.Background(), tc.force)
			total := rec.count(ReplaceSucceeded) + rec.count(ReplaceAborted)
			if total != 1 {
				t.Fatalf("%s: %d terminal finalizers fired, want exactly 1 (contract 3)", tc.name, total)
			}
			if got := rec.count(tc.want); got != 1 {
				t.Fatalf("%s: %v fired %d times, want 1", tc.name, tc.want, got)
			}
		})
	}
}

// TestPeerGatedKillOnlyOnForce pins that a non-force replace whose child acks
// Shutdown but keeps its socket (WaitGone false) must NOT Kill — it defers to the
// next tick; only a force replace reaches the peer-gated reap.
func TestPeerGatedKillOnlyOnForce(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	sv, cl := newSupervisor(t, c, rec)
	cl.shutdownErr = context.DeadlineExceeded
	cl.gone = false
	sv.Replace(context.Background(), false)
	if cl.kills != 0 {
		t.Fatalf("non-force replace killed the child %d times, want 0", cl.kills)
	}
	if rec.count(ReplaceAborted) != 1 {
		t.Fatalf("non-force defer fired %d ReplaceAborted, want 1", rec.count(ReplaceAborted))
	}

	rec2 := &recorder{}
	c2 := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	sv2, cl2 := newSupervisor(t, c2, rec2)
	cl2.shutdownErr = context.DeadlineExceeded
	// WaitGone: first false (wedged) -> reap; Kill ok; second true (socket freed).
	n := 0
	cl2.waitGoneFn = func() bool {
		n++
		return n >= 2
	}
	c2.onProbe = func(c *fakeChild) {
		if c.probes >= 1 {
			c.verdict = Verdict{Reachable: true, Version: "v2"}
		}
	}
	sv2.Replace(context.Background(), true)
	if cl2.kills != 1 {
		t.Fatalf("force reap killed the peer %d times, want 1", cl2.kills)
	}
	if rec2.count(ReplaceSucceeded) != 1 {
		t.Fatalf("force reap then spawn fired %d ReplaceSucceeded, want 1", rec2.count(ReplaceSucceeded))
	}
}

// TestReapPeerChangedDefers pins that on the force path a Kill reporting
// ErrChildUnavailable (nothing to kill / a successor in the way) makes the reap
// report the socket free; any other Kill error defers.
func TestReapPeerChangedDefers(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	sv, cl := newSupervisor(t, c, rec)
	cl.shutdownErr = context.DeadlineExceeded
	cl.gone = false                        // WaitGone always false
	cl.killErr = context.Canceled          // an unverifiable/changed peer (not ErrChildUnavailable)
	sv.Replace(context.Background(), true) // force path reaches the reap
	if cl.kills != 1 {
		t.Fatalf("force reap consulted Kill %d times, want 1", cl.kills)
	}
	if rec.count(ReplaceAborted) != 1 {
		t.Fatalf("a changed-peer reap fired %d ReplaceAborted, want 1 (defer + finalizer)", rec.count(ReplaceAborted))
	}
}

// TestTickDegradedRouting pins Tick's degraded branch: a degraded SKEWED child is
// force-converged (ReplaceSafe consulted with force=true); a degraded child at
// MyVersion, at spawnedSkew, or with an unknown version is left alone (no Replace).
func TestTickDegradedRouting(t *testing.T) {
	cases := []struct {
		name        string
		version     string
		spawnedSkew string
		wantReplace bool
	}{
		{"degraded skew force-converges", "v1", "", true},
		{"degraded at MyVersion is left alone", "v2", "", false},
		{"degraded at spawnedSkew is left alone", "v9", "v9", false},
		{"degraded with unknown version is left alone", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recorder{}
			// safe != "" so a triggered Replace defers at once (no spawn churn); assert only whether ReplaceSafe was consulted, and its force.
			c := &fakeChild{verdict: Verdict{Reachable: true, Degraded: true, Version: tc.version}, safe: "live sessions"}
			sv, _ := newSupervisor(t, c, rec)
			sv.spawnedSkew = tc.spawnedSkew
			sv.Tick(context.Background())
			if tc.wantReplace {
				if c.safes != 1 {
					t.Fatalf("degraded skew consulted ReplaceSafe %d times, want 1 (forced converge)", c.safes)
				}
				if !c.lastForce {
					t.Fatal("degraded skew converge used force=false, want force=true")
				}
			} else if c.safes != 0 {
				t.Fatalf("degraded non-skew consulted ReplaceSafe %d times, want 0 (no replace)", c.safes)
			}
		})
	}
}

// TestDeadDegradedDeadRefiresChildDied pins that a dead->degraded->dead
// oscillation fires ChildDied on BOTH deaths: the degraded branch must clear the
// death-episode flags, else the second death sees sawUnhealthy still set, never
// re-fires ChildDied, and its carcasses go uncleared.
func TestDeadDegradedDeadRefiresChildDied(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	sv, _ := newSupervisor(t, c, rec)

	// Tick 1: genuinely dead -> ChildDied #1.
	c.onProbe = func(c *fakeChild) { c.verdict = Verdict{Reachable: false} }
	sv.Tick(context.Background())

	// Tick 2: degraded (alive at MyVersion) -> must clear the death episode.
	c.onProbe = nil
	c.verdict = Verdict{Reachable: true, Degraded: true, Version: "v2"}
	sv.retryAt = time.Time{}
	sv.Tick(context.Background())

	// Tick 3: dead again -> ChildDied #2 only if the degraded tick cleared the flag.
	c.verdict = Verdict{Reachable: false}
	c.onProbe = func(c *fakeChild) { c.verdict = Verdict{Reachable: false} }
	sv.retryAt = time.Time{}
	sv.Tick(context.Background())

	if got := rec.count(ChildDied); got != 2 {
		t.Fatalf("dead->degraded->dead fired ChildDied %d times, want 2 (degraded must clear the death episode)", got)
	}
}

// TestVerifySpawnedRejectsDegraded pins that a freshly spawned child coming up
// Reachable-but-Degraded is NOT a verified bring-up — no Respawned, the attempt
// is booked against the backoff, so a never-ready child trips the verify-fail
// breaker instead of the consumer remounting against a not-ready child.
func TestVerifySpawnedRejectsDegraded(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	sv, _ := newSupervisor(t, c, rec)
	c.onProbe = func(c *fakeChild) {
		if c.probes >= 2 {
			c.verdict = Verdict{Reachable: true, Degraded: true, Version: "v2"}
		}
	}
	sv.Tick(context.Background())
	if rec.count(Respawned) != 0 {
		t.Fatalf("a degraded verify fired Respawned %d times, want 0 (degraded is not a verified bring-up)", rec.count(Respawned))
	}
	if sv.failures == 0 {
		t.Fatal("a degraded verify booked no spawn failure, want it counted toward the backoff/breaker")
	}
}

// TestErrSkipSpawnIsBenignNoOp pins that a Spawn.Override returning ErrSkipSpawn
// ("nothing to serve") is a no-op — no spawn failure is booked, the backoff
// floor is untouched, OnSpawnError never fires, and no Retreat happens.
func TestErrSkipSpawnIsBenignNoOp(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	sv, _ := newSupervisor(t, c, rec)
	sv.Spawn.Available = func() bool { return false } // so the Override body runs
	sv.Spawn.Override = func() error { return ErrSkipSpawn }
	surfaced := 0
	sv.OnSpawnError = func(error, int, time.Time) { surfaced++ }

	// Model the empty-pool flow: the child stays dead, every tick's spawn returns
	// ErrSkipSpawn, and only the backoff floor is cleared between ticks (so the
	// death transition fires once).
	for range sv.ReviveBreaker + 2 {
		sv.retryAt = time.Time{}
		sv.Tick(context.Background())
	}
	if surfaced != 0 {
		t.Fatalf("ErrSkipSpawn surfaced via OnSpawnError %d times, want 0", surfaced)
	}
	if sv.failures != 0 {
		t.Fatalf("ErrSkipSpawn booked %d spawn failures, want 0 (benign no-op)", sv.failures)
	}
	if len(c.retreats) != 0 {
		t.Fatalf("ErrSkipSpawn triggered Retreat %v, want none (an empty desired state is not a crash loop)", c.retreats)
	}
}

// TestErrSkipSpawnDuringReplaceAbortsCleanly pins ErrSkipSpawn on the Replace
// path: if the successor spawn returns ErrSkipSpawn after the gate cleared, no
// spawn failure is booked and exactly one ReplaceAborted finalizer fires (so the
// consumer still releases its held claims — contract 3).
func TestErrSkipSpawnDuringReplaceAbortsCleanly(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	sv, cl := newSupervisor(t, c, rec)
	cl.gone = true // the old child steps down cleanly on Shutdown+WaitGone
	sv.Spawn.Available = func() bool { return false }
	sv.Spawn.Override = func() error { return ErrSkipSpawn }
	surfaced := 0
	sv.OnSpawnError = func(error, int, time.Time) { surfaced++ }

	sv.Replace(context.Background(), false)

	if got := rec.count(ReplaceAborted); got != 1 {
		t.Fatalf("ErrSkipSpawn mid-Replace fired %d ReplaceAborted, want exactly 1 (claims released)", got)
	}
	if got := rec.count(ReplaceSucceeded); got != 0 {
		t.Fatalf("ErrSkipSpawn mid-Replace fired %d ReplaceSucceeded, want 0", got)
	}
	if sv.failures != 0 {
		t.Fatalf("ErrSkipSpawn mid-Replace booked %d spawn failures, want 0 (benign no-op)", sv.failures)
	}
	if surfaced != 0 {
		t.Fatalf("ErrSkipSpawn mid-Replace surfaced %d errors via OnSpawnError, want 0", surfaced)
	}
}

// TestOnSpawnErrorCarriesAttemptAndNextRetry pins that OnSpawnError receives the
// post-increment attempt count and the next-retry floor (for "attempt N, next in
// W" operator detail).
func TestOnSpawnErrorCarriesAttemptAndNextRetry(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	sv, _ := newSupervisor(t, c, rec)
	c.onProbe = func(c *fakeChild) { c.verdict = Verdict{Reachable: false} } // verify always fails
	var gotAttempt int
	var gotNext time.Time
	sv.OnSpawnError = func(err error, attempt int, nextRetry time.Time) {
		gotAttempt, gotNext = attempt, nextRetry
	}
	before := time.Now()
	sv.Tick(context.Background())
	if gotAttempt != 1 {
		t.Fatalf("OnSpawnError attempt = %d, want 1", gotAttempt)
	}
	if !gotNext.After(before) {
		t.Fatalf("OnSpawnError nextRetry = %v, want after %v (a backoff floor was armed)", gotNext, before)
	}
}

// TestSpawnedSkewGetter pins that SpawnedSkew surfaces the reverse-skew version
// the Supervisor's own spawns settle at, so the consumer can tell a true
// reverse-skew settle from a forward-skew child it is still trying to replace.
func TestSpawnedSkewGetter(t *testing.T) {
	sv := &Supervisor{MyVersion: "v2"}
	if got := sv.SpawnedSkew(); got != "" {
		t.Fatalf("fresh SpawnedSkew = %q, want empty", got)
	}
	sv.noteSpawnedVersion("v9") // an upgrade swapped the on-disk binary
	if got := sv.SpawnedSkew(); got != "v9" {
		t.Fatalf("SpawnedSkew after a v9 spawn = %q, want v9", got)
	}
	sv.noteSpawnedVersion("v2") // a later spawn back at MyVersion clears it
	if got := sv.SpawnedSkew(); got != "" {
		t.Fatalf("SpawnedSkew after a MyVersion spawn = %q, want empty", got)
	}
}

// TestSupervisorIsSkew pins IsSkew: an empty version is never skew (an unknown
// poll), MyVersion is never skew, the version our own spawns settle at
// (spawnedSkew) is never skew, and any other version IS skew.
func TestSupervisorIsSkew(t *testing.T) {
	sv := &Supervisor{MyVersion: "v2"}
	if sv.IsSkew("") {
		t.Error(`IsSkew("") = true, want false (an empty version is an unknown poll, never skew)`)
	}
	if sv.IsSkew("v2") {
		t.Error(`IsSkew(MyVersion) = true, want false`)
	}
	sv.noteSpawnedVersion("v9") // an upgrade swapped the on-disk binary: our spawns now settle at v9
	if got := sv.SpawnedSkew(); got != "v9" {
		t.Fatalf("setup: SpawnedSkew = %q, want v9 after noteSpawnedVersion(v9)", got)
	}
	if sv.IsSkew("v9") {
		t.Error(`IsSkew(spawnedSkew) = true, want false (our own spawns produce it; re-replacing would churn forever)`)
	}
	if !sv.IsSkew("v3") {
		t.Error(`IsSkew("v3") = false, want true (a foreign version we would replace)`)
	}
}

// TestGoneWait pins that the retiring-child gone-wait prefers GoneWait, falling
// back to Spawn.Timeout, then DefaultSpawnTimeout — distinct from the fresh
// child's come-up bound.
func TestGoneWait(t *testing.T) {
	if got := (&Supervisor{GoneWait: 9 * time.Second, Spawn: Spawn{Timeout: 2 * time.Second}}).goneWait(); got != 9*time.Second {
		t.Errorf("goneWait with GoneWait set = %v, want 9s", got)
	}
	if got := (&Supervisor{Spawn: Spawn{Timeout: 2 * time.Second}}).goneWait(); got != 2*time.Second {
		t.Errorf("goneWait falling back to Spawn.Timeout = %v, want 2s", got)
	}
	if got := (&Supervisor{}).goneWait(); got != DefaultSpawnTimeout {
		t.Errorf("goneWait with neither set = %v, want %v", got, DefaultSpawnTimeout)
	}
}

// TestValidate pins the fail-loud wire check: a fully-wired Supervisor passes,
// and each missing Required field is named rather than nil-panicking later.
func TestValidate(t *testing.T) {
	base := func() *Supervisor {
		return &Supervisor{
			MyVersion: "v2",
			Policy:    &fakeChild{},
			Spawn:     Spawn{Available: func() bool { return true }, CanHost: func() error { return nil }},
		}
	}
	if err := base().Validate(); err != nil {
		t.Fatalf("a fully-wired supervisor failed Validate: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*Supervisor)
	}{
		{"nil Policy", func(sv *Supervisor) { sv.Policy = nil }},
		{"empty MyVersion", func(sv *Supervisor) { sv.MyVersion = "" }},
		{"nil Spawn.Available", func(sv *Supervisor) { sv.Spawn.Available = nil }},
		{"nil Spawn.CanHost", func(sv *Supervisor) { sv.Spawn.CanHost = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sv := base()
			tc.mutate(sv)
			if err := sv.Validate(); err == nil {
				t.Fatalf("%s passed Validate, want an error naming the missing field", tc.name)
			}
		})
	}
}
