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
		if waitReady(ready, timeout, exited) {
			t.Fatal("waitReady = true, want false: the serve goroutine exited and the dir never came live")
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

		if !waitReady(ready, 10*time.Second, exited) {
			t.Fatal("waitReady = false, want true: the final probe after serve-exit saw a live mount")
		}
		if got := calls.Load(); got != 2 {
			t.Fatalf("ready calls = %d, want 2 (top probe + one final probe on serve-exit)", got)
		}
	})
}
