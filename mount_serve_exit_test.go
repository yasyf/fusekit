//go:build fuse && cgo

// This file pins the bail-the-mount-wait-on-serve-exit fix (cc-pool d5f358a) at
// the waitReady control-flow level, WITHOUT a real fuse-t mount: it drives the
// readiness-wait helper directly with a Ready that never (or only finally)
// reports live and a serve-exit channel, asserting waitReady bails promptly
// instead of burning the full Wait. It needs neither FUSEKIT_LIVE nor a real
// mount (Mount itself is never called — it would dlopen libfuse-t and attempt a
// real fuse-t mount). The downstream proven/unproven → ErrMountTimeout/
// ErrMountNotLive mapping that Mount layers on a false waitReady is pinned
// separately by TestMountWaitErrSentinels (live_probe_test.go).

package fusekit

import (
	"sync/atomic"
	"testing"
	"time"
)

// swapMountPollInterval seams mountPollInterval (the poll cadence shared by the
// pure waitMounted and the fuse-tagged waitReady) for one test, restoring it on
// cleanup. It lives in this fuse-tagged file because waitReady — the only
// consumer these tests drive — is fuse-tagged. Tests using it must not run in
// parallel.
func swapMountPollInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := mountPollInterval
	mountPollInterval = d
	t.Cleanup(func() { mountPollInterval = prev })
}

// TestWaitReadyBailsOnServeExit pins cc-pool d5f358a: when the serving goroutine
// exits before the mount comes live (a hard mount(2) failure that will never
// come up), waitReady bails after one final probe instead of burning the rest
// of the Wait. Both cases are constructed without a real mount — Ready and the
// serve-exit channel are driven directly.
func TestWaitReadyBailsOnServeExit(t *testing.T) {
	t.Run("serve exit, never live -> bail false well before timeout", func(t *testing.T) {
		// A poll interval far longer than the assertion ceiling: the only way
		// waitReady can return inside the ceiling is the serve-exit select case
		// firing — not the poll timer, and certainly not the Wait deadline.
		swapMountPollInterval(t, 2*time.Second)
		const timeout = 10 * time.Second
		ready := func() bool { return false } // the mount never comes live
		exited := make(chan struct{})
		go func() {
			time.Sleep(20 * time.Millisecond) // the serve goroutine returns shortly
			close(exited)
		}()

		start := time.Now()
		live, hard := waitReady(ready, timeout, exited)
		if live {
			t.Fatal("waitReady live = true, want false: the serve goroutine exited and the dir never came live")
		}
		if !hard {
			t.Fatal("waitReady exited = false, want true: a serve-exit with no live mount is a hard mount failure")
		}
		if waited := time.Since(start); waited >= time.Second {
			t.Fatalf("waitReady waited %v; it did not bail on the serve-exit channel (poll interval 2s, timeout %v)", waited, timeout)
		}
	})

	t.Run("serve exit, final probe sees the mount -> kept", func(t *testing.T) {
		swapMountPollInterval(t, 50*time.Millisecond)
		var calls atomic.Int32
		ready := func() bool {
			// Top-of-loop probe misses; the one final probe after serve-exit
			// catches a mount that landed in the same instant.
			return calls.Add(1) > 1
		}
		exited := make(chan struct{})
		close(exited) // the serve goroutine already returned

		live, hard := waitReady(ready, 10*time.Second, exited)
		if !live {
			t.Fatal("waitReady live = false, want true: the final probe after serve-exit saw a live mount")
		}
		if hard {
			t.Fatal("waitReady exited = true, want false: the final probe saw the mount live, so it is not a hard failure")
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("ready calls = %d, want 2 (top probe + one final probe on serve-exit)", got)
		}
	})
}

// The TestWaitMounted* cases below pin the PURE waitMounted (live.go) deadline
// edges — the at-deadline probe contract and the late-mount keep — that the
// fuse-tagged waitReady serve-exit cases above do not exercise. Ported from
// cc-pool's overlay/live_test.go when waitMounted moved into fusekit; they drive
// the mountAliveFn seam (swapMountAlive, live_probe_test.go) directly, no mount.
// They live in this fuse-tagged file to share swapMountPollInterval with the
// waitReady cases; waitMounted itself is pure, so the contract holds in every
// build.

// TestWaitMountedChecksAtDeadline pins that a zero timeout still runs exactly
// one at-deadline probe and keeps a live mount it sees.
func TestWaitMountedChecksAtDeadline(t *testing.T) {
	var calls atomic.Int32
	swapMountAlive(t, func(base, accountDir string) bool {
		calls.Add(1)
		if base != "base" || accountDir != "acct" {
			t.Errorf("probe got (%q, %q), want (\"base\", \"acct\")", base, accountDir)
		}
		return true
	})
	if !waitMounted("base", "acct", 0, nil) {
		t.Fatal("waitMounted = false, want true: a zero timeout must still run one at-deadline probe")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("probe calls = %d, want exactly 1 for timeout=0", got)
	}
}

// TestWaitMountedTimesOutBounded pins that a zero timeout against a dead dir
// probes exactly once and returns false (no extra polls past the deadline).
func TestWaitMountedTimesOutBounded(t *testing.T) {
	var calls atomic.Int32
	swapMountAlive(t, func(base, accountDir string) bool {
		calls.Add(1)
		return false
	})
	if waitMounted("base", "acct", 0, nil) {
		t.Fatal("waitMounted = true, want false: the probe never saw a live mount")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("probe calls = %d, want exactly 1 for timeout=0 (no extra polls past the deadline)", got)
	}
}

// TestWaitMountedLateMountKept pins that a mount landing at/after the deadline
// (without any serve-exit) is kept by the final at-deadline probe.
func TestWaitMountedLateMountKept(t *testing.T) {
	swapMountPollInterval(t, time.Millisecond)
	const timeout = 25 * time.Millisecond
	start := time.Now()
	// flipAt is the earliest instant waitMounted's internal deadline can be; the
	// real one lands the call-overhead nanoseconds later. Flipping at flipAt is
	// deterministic against the probe-at-deadline contract (every probe deciding
	// at/after the real deadline sees live) and catches a check-deadline-first
	// implementation in all but the nanosecond-scale skew window between flipAt
	// and the real deadline.
	flipAt := start.Add(timeout)
	swapMountAlive(t, func(base, accountDir string) bool {
		return !time.Now().Before(flipAt)
	})
	if !waitMounted("base", "acct", timeout, nil) {
		t.Fatal("waitMounted = false, want true: a mount landing after the deadline must be kept by the final at-deadline probe")
	}
	if waited := time.Since(start); waited < timeout {
		t.Fatalf("waitMounted returned after %v, before the %v deadline — the late-flip path was not exercised", waited, timeout)
	}
}
