//go:build fuse && cgo

package fusekit

import (
	"sync/atomic"
	"testing"
	"time"
)

// swapMountPollInterval swaps a global; callers must not run in parallel.
func swapMountPollInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := mountPollInterval
	mountPollInterval = d
	t.Cleanup(func() { mountPollInterval = prev })
}

// TestWaitReadyBailsOnServeExit pins that a serve-goroutine exit before the
// mount comes live makes waitReady bail after one final probe, not burn the Wait.
func TestWaitReadyBailsOnServeExit(t *testing.T) {
	t.Run("serve exit, never live -> bail false well before timeout", func(t *testing.T) {
		// Poll interval past the assertion ceiling: only serve-exit returns in time.
		swapMountPollInterval(t, 2*time.Second)
		const timeout = 10 * time.Second
		ready := func() bool { return false }
		exited := make(chan struct{})
		go func() {
			time.Sleep(20 * time.Millisecond)
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
			// First (top-of-loop) probe misses; the final post-serve-exit probe hits.
			return calls.Add(1) > 1
		}
		exited := make(chan struct{})
		close(exited)

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

// TestWaitMounted* pin waitMounted (live.go); kept in this fuse-tagged file
// to share swapMountPollInterval.

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
// probes exactly once and returns false.
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
	// flipAt is at or before the internal deadline (set call-overhead ns after
	// start): the at-deadline probe sees live; check-deadline-first would fail.
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
