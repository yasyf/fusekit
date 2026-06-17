package fusekit

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// swapMountAlive seams mountAliveFn for one test, restoring it on cleanup.
// Tests using it must not run in parallel.
func swapMountAlive(t *testing.T, fn func(base, accountDir string) bool) {
	t.Helper()
	prev := mountAliveFn
	mountAliveFn = fn
	t.Cleanup(func() { mountAliveFn = prev })
}

// swapStatProbeTimeout seams statProbeTimeout for one test, restoring it on
// cleanup. Tests using it must not run in parallel.
func swapStatProbeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := statProbeTimeout
	statProbeTimeout = d
	t.Cleanup(func() { statProbeTimeout = prev })
}

// TestMountAliveWithin pins the bounded liveness probe: a healthy stat's
// verdict passes through both polarities, and a parked stat (a wedged
// mirror's uninterruptible sleep) reads NOT alive within the bound — even
// when the stat would eventually answer alive — instead of hanging the
// caller forever.
func TestMountAliveWithin(t *testing.T) {
	cases := []struct {
		name string
		dir  string // unique per case: aliveProbes joins in-flight probes by dir
		park bool   // stat blocks until the test releases it
		stat bool   // the stat's eventual verdict
		want bool
	}{
		{name: "healthy live probe reads alive", dir: "/probe/acct-live", stat: true, want: true},
		{name: "healthy dead probe reads not alive", dir: "/probe/acct-dead", stat: false, want: false},
		{name: "parked probe reads not alive within the bound", dir: "/probe/acct-parked", park: true, stat: true, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swapStatProbeTimeout(t, 20*time.Millisecond)
			release := make(chan struct{})
			swapMountAlive(t, func(base, accountDir string) bool {
				if base != "base" || accountDir != tc.dir {
					t.Errorf("probe got (%q, %q), want (\"base\", %q)", base, accountDir, tc.dir)
				}
				if tc.park {
					<-release
				}
				return tc.stat
			})
			// Drain the parked probe body before swapMountAlive's cleanup
			// restores the seam it reads (cleanups run LIFO).
			t.Cleanup(func() {
				close(release)
				deadline := time.Now().Add(5 * time.Second)
				for aliveProbes.Inflight() != 0 {
					if time.Now().After(deadline) {
						t.Error("parked probe body never drained")
						return
					}
					time.Sleep(time.Millisecond)
				}
			})

			start := time.Now()
			got := MountAliveWithin("base", tc.dir)
			elapsed := time.Since(start)
			if got != tc.want {
				t.Errorf("MountAliveWithin = %v, want %v", got, tc.want)
			}
			// Well under the 2s production bound: returning at all while the
			// stat is parked is the property; the margin keeps it unflaky.
			if tc.park && elapsed >= time.Second {
				t.Errorf("MountAliveWithin returned after %v with a parked stat; the %v bound did not hold", elapsed, statProbeTimeout)
			}
		})
	}
}

// TestMountWaitErrSentinels pins the proven/unproven TCC split mountWaitErr
// composes: an unproven mount-up timeout reads as the one-time "Network Volumes"
// grant (wrapping ErrMountNotLive, carrying the System-Settings walkthrough), a
// proven one reads as transient fuse-t slowness (wrapping ErrMountTimeout, never
// surfacing TCC guidance). mountWaitErr is defined pure in errors.go even though
// Mount only calls it under the fuse tag, so it is unit-testable here without a
// real mount.
func TestMountWaitErrSentinels(t *testing.T) {
	const tccPhrase = "grant Network Volumes access once"
	const dir = "/pool/accounts/acct-01"
	const waited = 8 * time.Second
	cases := []struct {
		name          string
		proven        bool
		wantIs        error
		wantNotIs     error
		wantTCCPhrase bool
		wantWaited    bool
	}{
		{
			name:          "unproven grant reads as TCC with walkthrough",
			proven:        false,
			wantIs:        ErrMountNotLive,
			wantNotIs:     ErrMountTimeout,
			wantTCCPhrase: true,
		},
		{
			name:       "proven grant reads as transient timeout without TCC guidance",
			proven:     true,
			wantIs:     ErrMountTimeout,
			wantNotIs:  ErrMountNotLive,
			wantWaited: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mountWaitErr(dir, waited, tc.proven)
			if err == nil {
				t.Fatal("mountWaitErr returned nil")
			}
			if !errors.Is(err, tc.wantIs) {
				t.Errorf("errors.Is(err, %v) = false, want true; err = %v", tc.wantIs, err)
			}
			if errors.Is(err, tc.wantNotIs) {
				t.Errorf("errors.Is(err, %v) = true, want false; err = %v", tc.wantNotIs, err)
			}
			msg := err.Error()
			if got := strings.Contains(msg, tccPhrase); got != tc.wantTCCPhrase {
				t.Errorf("message contains %q = %v, want %v; msg = %q", tccPhrase, got, tc.wantTCCPhrase, msg)
			}
			// The System Settings pointer is TCC guidance: present iff the
			// grant is unproven. ErrMountTimeout's godoc forbids surfacing it.
			if got := strings.Contains(msg, "System Settings"); got != tc.wantTCCPhrase {
				t.Errorf("message contains \"System Settings\" = %v, want %v; msg = %q", got, tc.wantTCCPhrase, msg)
			}
			if strings.Contains(msg, "symlink is used until then") {
				t.Errorf("message carries the stale symlink-fallback claim; msg = %q", msg)
			}
			if !strings.Contains(msg, dir) {
				t.Errorf("message does not name the account dir %q; msg = %q", dir, msg)
			}
			if tc.wantWaited && !strings.Contains(msg, waited.String()) {
				t.Errorf("message does not mention the %v wait; msg = %q", waited, msg)
			}
		})
	}
}

// TestStatProbesJoinsConcurrentDo pins the wedge-containment contract StatProbes
// exists for: concurrent Do calls for the SAME key collapse onto ONE in-flight
// stat goroutine. Joiners find the in-flight entry and share its result (or its
// timeout) instead of each spawning their own stat — so a wedged carcass parks
// at most one goroutine, never one per caller. cc-pool's live_test.go has no
// standalone StatProbes join test, so this is written fresh against the core
// primitive; it also pins Inflight() (1 while parked, 0 after the probe exits).
func TestStatProbesJoinsConcurrentDo(t *testing.T) {
	var probes StatProbes[int]
	const key = "/probe/wedged"

	release := make(chan struct{})
	// A guarded, close-once release plus a drain cleanup, so a Fatalf can never
	// strand the parked probe goroutine (the happy path also closes release).
	var closeOnce sync.Once
	closeRelease := func() { closeOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		closeRelease()
		deadline := time.Now().Add(5 * time.Second)
		for probes.Inflight() != 0 {
			if time.Now().After(deadline) {
				t.Error("parked probe never drained")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})

	var firstCalls atomic.Int32
	started := make(chan struct{})
	firstOK := make(chan bool, 1)
	verdict := make(chan int, 1)
	// The first Do spawns the one probe goroutine and parks its stat on release.
	go func() {
		v, ok := probes.Do(key, 5*time.Second, func() int {
			firstCalls.Add(1)
			close(started)
			<-release // a wedged carcass: the stat parks until released
			return 7
		})
		firstOK <- ok
		verdict <- v
	}()
	<-started // the probe is in-flight and parked under key

	if got := probes.Inflight(); got != 1 {
		t.Fatalf("Inflight while parked = %d, want 1", got)
	}

	// Concurrent same-key callers JOIN the parked probe: Do finds the in-flight
	// entry and waits on its verdict, so each joiner's OWN stat func is never
	// invoked. With a short bound they time out ON the shared probe (ok=false),
	// because the parked stat has not answered — they never start a second.
	const joiners = 8
	var joinerStat atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < joiners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := probes.Do(key, 20*time.Millisecond, func() int {
				joinerStat.Add(1) // must NEVER run: joiners share the parked probe
				return 99
			}); ok {
				t.Errorf("joiner Do ok=true, want false: the parked probe never answered within the bound")
			}
		}()
	}
	wg.Wait()

	if got := joinerStat.Load(); got != 0 {
		t.Fatalf("joiner stat invocations = %d, want 0 — concurrent same-key Do must join the parked probe, not spawn its own", got)
	}
	if got := firstCalls.Load(); got != 1 {
		t.Fatalf("underlying stat invocations = %d, want exactly 1 (joined, not re-spawned)", got)
	}
	if got := probes.Inflight(); got != 1 {
		t.Fatalf("Inflight after the joiners timed out = %d, want still 1 (the one parked probe)", got)
	}

	// Release the parked stat: the first Do now gets the real verdict, ok=true,
	// and the probe goroutine exits, draining Inflight back to 0.
	closeRelease()
	if ok := <-firstOK; !ok {
		t.Fatal("first Do ok=false, want true: the released stat answered within its bound")
	}
	if got := <-verdict; got != 7 {
		t.Fatalf("first Do verdict = %d, want 7 (the stat's return value)", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for probes.Inflight() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("Inflight after release = %d, want 0 (probe goroutine exited)", probes.Inflight())
		}
		time.Sleep(time.Millisecond)
	}
}
