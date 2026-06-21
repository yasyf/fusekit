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

// fakeChild is the injectable test double for the supervised child: a
// programmable Probe verdict, a peer-alive flag, a ReplaceSafe gate, and call
// counters for every destructive arm. It mirrors cc-pool holder_test.go's
// injection so the state machine is unit-testable without a real process.
type fakeChild struct {
	verdict   Verdict // what Probe returns this tick
	peerAlive bool    // what PeerAlive returns
	safe      string  // ReplaceSafe reason ("" = clear)

	probes   int
	peers    int
	safes    int
	retreats []string

	// onProbe, when set, runs before each Probe returns — lets a test flip the
	// verdict to model a spawn bringing the child up at some version.
	onProbe func(c *fakeChild)
	// onSafe, when set, runs inside ReplaceSafe after it decides to clear — the
	// hook a ctx-cancel test uses to cancel mid-critical-section.
	onSafe func()
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
	if c.safe == "" && c.onSafe != nil {
		c.onSafe()
	}
	return c.safe
}

func (c *fakeChild) Retreat(ctx context.Context, reason string) {
	c.retreats = append(c.retreats, reason)
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
// short-circuits (a child is "already serving"). Returns the socket path.
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
	sv := &Supervisor{
		Spawn: Spawn{
			Socket:    socket,
			Available: func() bool { return true },
			CanHost:   func() error { return nil },
		},
		MyVersion:     "v2",
		Policy:        c,
		Shutdown:      func(ctx context.Context) error { cl.shutdowns++; return cl.shutdownErr },
		WaitGone:      func(ctx context.Context, d time.Duration) bool { cl.waitGones++; return cl.gone },
		Kill:          func() (int, error) { cl.kills++; return 123, cl.killErr },
		Reconcile:     rec.reconcile,
		SpawnBackoff:  Backoff{Base: 10 * time.Second, Cap: 10 * time.Minute},
		HazardWindow:  30 * time.Minute,
		ReviveBreaker: 3,
	}
	return sv, cl
}

// callLog counts the child-control callbacks and programs their outcomes.
type callLog struct {
	shutdowns   int
	waitGones   int
	kills       int
	shutdownErr error
	gone        bool // WaitGone return
	killErr     error
}

// --- spawn backoff doubling/cap ---

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

// TestOnSpawnErrorSurfacesFailures pins the consumer-facing spawn-error hook:
// every booked spawn/verify failure is delivered to OnSpawnError (so the
// consumer can surface it), and a clean bring-up never calls it. The first case
// is a spawn that will not assemble; the second a spawn that "succeeds" but
// whose verify probe never comes up (the zombie-socket shape).
func TestOnSpawnErrorSurfacesFailures(t *testing.T) {
	t.Run("verify failure is surfaced", func(t *testing.T) {
		c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
		rec := &recorder{}
		sv, _ := newSupervisor(t, c, rec)
		// The spawn "succeeds" (Available short-circuits) but the verify probe stays
		// unreachable — booked as a failure, surfaced through OnSpawnError.
		var got []error
		sv.OnSpawnError = func(err error) { got = append(got, err) }
		sv.Tick(context.Background())
		if len(got) != 1 {
			t.Fatalf("OnSpawnError fired %d times, want 1", len(got))
		}
		if !errors.Is(got[0], ErrHolderUnavailable) {
			t.Fatalf("surfaced err = %v, want it to wrap ErrHolderUnavailable", got[0])
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
		sv.OnSpawnError = func(error) { called = true }
		sv.Tick(context.Background())
		if called {
			t.Fatal("OnSpawnError fired on a clean bring-up")
		}
	})
}

// TestClearBackoffDropsFloorButNotBreaker pins ClearBackoff: it drops the
// spawn-backoff floor so the next Tick retries the spawn immediately, WITHOUT
// touching the crash-loop breaker. The supervisor is first driven into backoff
// by a failing spawn (verify probe stays unreachable, booking retryAt in the
// future); the next Tick must NOT attempt a spawn (still gated). After
// ClearBackoff the next Tick DOES attempt one — and a child that keeps looping
// still trips Retreat at the same ReviveBreaker threshold, proving ClearBackoff
// left the breaker count untouched.
func TestClearBackoffDropsFloorButNotBreaker(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	rec := &recorder{}
	sv, _ := newSupervisor(t, c, rec)
	// The spawn "succeeds" (Available short-circuits) but verify never comes up:
	// every attempt is a booked spawn failure that arms the backoff and advances
	// the breaker. probes is the spawn-attempt detector — a gated tick only runs
	// the initial route Probe (no verify second probe).
	c.onProbe = func(c *fakeChild) { c.verdict = Verdict{Reachable: false} }

	// One failing tick: a death revive that books a future retryAt and advances
	// the breaker to 1. The verify probe makes this 2 probes.
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

	// A tick while still gated must NOT attempt a spawn: only the route Probe runs,
	// so the probe count advances by exactly 1 (no verify probe).
	sv.sawUnhealthy = true // model the same death episode (no fresh ChildDied)
	before := c.probes
	sv.Tick(context.Background())
	if c.probes != before+1 {
		t.Fatalf("gated tick ran %d probes, want 1 (no spawn attempt while backed off)", c.probes-before)
	}
	if sv.failures != 1 {
		t.Fatalf("gated tick booked another failure (failures = %d, want 1); it should not have spawned", sv.failures)
	}

	// Drop the floor: ClearBackoff zeroes ONLY retryAt — the crash-loop breaker
	// counters (failures, reviveHazard) are left exactly as they were.
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

	// With the floor dropped, the very next Tick DOES attempt a spawn: route Probe
	// + verify probe = 2 probes, and the failed verify books another failure.
	before = c.probes
	sv.Tick(context.Background())
	if c.probes != before+2 {
		t.Fatalf("post-ClearBackoff tick ran %d probes, want 2 (spawn attempted + verified)", c.probes-before)
	}
	if sv.failures != failuresBefore+1 {
		t.Fatalf("post-ClearBackoff tick failures = %d, want %d (a spawn was attempted)", sv.failures, failuresBefore+1)
	}

	// The child still loops: keep clearing the floor so each Tick retries at once,
	// and the spawn-fail breaker must still trip Retreat once failures reaches the
	// untouched ReviveBreaker threshold — ClearBackoff never reset it.
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

// TestHazardWindowStaleClusterResets pins HazardWindow: a death far enough after
// the previous one starts a FRESH crash-loop cluster (the hazard counter
// resets), so occasional far-apart transient deaths never accumulate into a
// spurious breaker trip.
func TestHazardWindowStaleClusterResets(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	rec := &recorder{}
	sv, _ := newSupervisor(t, c, rec)
	// Two deaths' worth of accumulated hazard — but the last one was long ago.
	sv.reviveHazard = sv.ReviveBreaker - 1
	sv.lastReviveAt = time.Now().Add(-sv.HazardWindow - time.Minute)
	sv.sawUnhealthy = false
	// The spawn brings the child up at our version on the verify probe, so this
	// fresh death revives normally rather than retreating.
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

// --- revive on dead ---

func TestReviveOnGenuinelyDead(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	rec := &recorder{}
	sv, _ := newSupervisor(t, c, rec)
	// The spawn brings the child up at our version on the verify probe.
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

// --- spare on wedged (CONTRACT 1) ---
//
// TestContract1SpareWedgedChild: an alive-but-unresponsive child (PeerAlive
// true, Probe unreachable) must NOT be force-killed or have the ChildDied
// force-unmount signal fired on a normal Tick. Without the PeerAlive gate the
// revive arm would treat it as dead — fire ChildDied and advance the crash-loop
// breaker — so this asserts zero ChildDied, zero Kill, and no breaker advance.
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

// TestContract1ManyTicksNeverDestructive hardens contract 1: a child that stays
// wedged-alive across many ticks is never force-killed and never fires
// ChildDied, no matter how long it persists.
func TestContract1ManyTicksNeverDestructive(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: true}
	rec := &recorder{}
	sv, cl := newSupervisor(t, c, rec)
	// Make the spawn-verify fail so we exercise the spawn-fail breaker path too —
	// it must retreat, but still never Kill or fire ChildDied.
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

// --- breaker trips + Retreat after N consecutive deaths ---

func TestReviveBreakerTripsAndRetreats(t *testing.T) {
	c := &fakeChild{verdict: Verdict{Reachable: false}, peerAlive: false}
	rec := &recorder{}
	sv, _ := newSupervisor(t, c, rec)
	// The spawn never brings the child up (verify always unreachable): each tick
	// is a fresh genuine death, advancing the breaker.
	for range sv.ReviveBreaker {
		sv.sawUnhealthy = false // model a fresh death-transition each tick
		sv.retryAt = time.Time{}
		sv.Tick(context.Background())
	}
	if len(c.retreats) == 0 {
		t.Fatalf("breaker never retreated after %d consecutive deaths", sv.ReviveBreaker)
	}
}

// --- breaker NOT reset by skewed settle (CONTRACT 2) ---
//
// TestContract2BreakerNotResetBySkewedSettle: a child that repeatedly comes up
// at a version != MyVersion and dies must still trip ReviveBreaker and call
// Retreat. A naive reset-on-any-healthy would clear the counter on every skewed
// settle and loop forever, never retreating — so this asserts Retreat DOES fire
// despite the interleaved healthy-but-skewed settles.
func TestContract2BreakerNotResetBySkewedSettle(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{peerAlive: false}
	sv, _ := newSupervisor(t, c, rec)
	// Each "cycle" is: a death tick (Reachable false) where the spawn settles the
	// child healthy at a SKEWED version (v1, not MyVersion v2). The settle must
	// NOT reset reviveHazard. After ReviveBreaker cycles, the breaker trips.
	c.onProbe = func(c *fakeChild) {
		// First probe of a tick is the death verdict; the verify probe after the
		// spawn settles skewed-healthy.
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

// TestContract2HealthySettleAtMyVersionResets is the positive control for
// contract 2: a genuine settle at MyVersion DOES reset the breaker, so a normal
// respawn never false-trips it.
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

// --- version-skew triggers Replace gated by ReplaceSafe ---

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

	// Now clear the gate: the replace runs end to end.
	rec2 := &recorder{}
	c2 := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	sv2, cl2 := newSupervisor(t, c2, rec2)
	cl2.gone = true
	c2.onProbe = func(c *fakeChild) {
		// After the spawn, the verify probe shows the fresh child at MyVersion.
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

// --- Replace finalizer exactly-once incl ctx-cancel (CONTRACT 3) ---
//
// TestContract3ReplaceFinalizerExactlyOnceOnCancel: cancelling ctx mid-Replace
// must fire EXACTLY ONE ReplaceAborted — never zero (the consumer would leak its
// claims forever), never two (a double finalizer). The cancel happens inside the
// ReplaceSafe critical section via onSafe, so the very next ctx.Err() check
// bails — and the deferred finalizer is the only thing that can fire.
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
				// First WaitGone (after Shutdown) fails; reap kills; second WaitGone
				// succeeds. Model with a toggling gone flag.
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
				sv.WaitGone = func(ctx context.Context, d time.Duration) bool {
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

// --- peer-gated Kill only on force ---
//
// TestPeerGatedKillOnlyOnForce: a non-force replace whose child acks Shutdown
// but keeps its socket (WaitGone false) must NOT Kill — it defers to the next
// tick (the child may never have received the Shutdown). Only a force replace
// reaches the peer-gated reap.
func TestPeerGatedKillOnlyOnForce(t *testing.T) {
	// Non-force: Shutdown errors AND WaitGone false => defer, never Kill.
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

	// Force: same wedge reaches the reap and kills the peer.
	rec2 := &recorder{}
	c2 := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	sv2, cl2 := newSupervisor(t, c2, rec2)
	cl2.shutdownErr = context.DeadlineExceeded
	// WaitGone: first false (wedged) -> reap; Kill ok; second true (socket freed).
	n := 0
	sv2.WaitGone = func(ctx context.Context, d time.Duration) bool {
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

// TestReapPeerChangedDefers pins the peer-gate refusal: a Kill that reports the
// peer unreachable/changed (ErrHolderUnavailable) on the force path means
// nothing to kill or a successor in the way — the reap reports the socket free
// (released) when ErrHolderUnavailable, and defers on any other Kill error.
func TestReapPeerChangedDefers(t *testing.T) {
	rec := &recorder{}
	c := &fakeChild{verdict: Verdict{Reachable: true, Version: "v1"}, safe: ""}
	sv, cl := newSupervisor(t, c, rec)
	cl.shutdownErr = context.DeadlineExceeded
	cl.gone = false                        // WaitGone always false
	cl.killErr = context.Canceled          // an unverifiable/changed peer (not ErrHolderUnavailable)
	sv.Replace(context.Background(), true) // force path reaches the reap
	if cl.kills != 1 {
		t.Fatalf("force reap consulted Kill %d times, want 1", cl.kills)
	}
	if rec.count(ReplaceAborted) != 1 {
		t.Fatalf("a changed-peer reap fired %d ReplaceAborted, want 1 (defer + finalizer)", rec.count(ReplaceAborted))
	}
}
