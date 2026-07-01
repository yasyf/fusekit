package fusekit

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// drainForceUnmountProbes drains parked syscall bodies so none outlives the
// test's unmountFn swap. Register FIRST — cleanups run LIFO.
func drainForceUnmountProbes(t *testing.T, release chan struct{}) {
	t.Helper()
	t.Cleanup(func() {
		close(release)
		deadline := time.Now().Add(5 * time.Second)
		for forceUnmountProbes.Inflight() != 0 {
			if time.Now().After(deadline) {
				t.Error("parked force-unmount body never drained")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})
}

func swapUnmountFn(t *testing.T, fn func(dir string) error) {
	t.Helper()
	prev := unmountFn
	unmountFn = fn
	t.Cleanup(func() { unmountFn = prev })
}

func swapForceUnmountTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := forceUnmountTimeout
	forceUnmountTimeout = d
	t.Cleanup(func() { forceUnmountTimeout = prev })
}

func TestForceUnmountCleanResult(t *testing.T) {
	var (
		mu        sync.Mutex
		gotTarget string
	)
	swapUnmountFn(t, func(target string) error {
		mu.Lock()
		gotTarget = target
		mu.Unlock()
		return nil
	})

	if err := ForceUnmount("/pool/acct-01"); err != nil {
		t.Fatalf("ForceUnmount clean = %v, want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotTarget != "/pool/acct-01" {
		t.Fatalf("unmount target = %q, want /pool/acct-01", gotTarget)
	}
}

func TestForceUnmountWrapsSyscallError(t *testing.T) {
	sentinel := errors.New("device busy")
	swapUnmountFn(t, func(string) error { return sentinel })

	err := ForceUnmount("/pool/acct-01")
	if !errors.Is(err, sentinel) {
		t.Fatalf("ForceUnmount err = %v, want it to wrap the syscall error", err)
	}
}

// TestForceUnmountBounded pins that a wedged, never-returning unmount cannot
// hang the caller: ErrForceUnmountTimeout at the bound, syscall left parked.
func TestForceUnmountBounded(t *testing.T) {
	release := make(chan struct{})
	drainForceUnmountProbes(t, release)
	started := make(chan struct{})
	swapUnmountFn(t, func(string) error {
		close(started)
		<-release
		return nil
	})
	swapForceUnmountTimeout(t, 50*time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- ForceUnmount("/pool/acct-01") }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrForceUnmountTimeout) {
			t.Fatalf("wedged unmount err = %v, want ErrForceUnmountTimeout", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ForceUnmount hung on a wedged unmount instead of returning at the timeout")
	}
	<-started // the syscall goroutine did run
}

// TestForceUnmountJoinsRepeatedWedged pins the leak bound: re-issues against
// the same wedged dir join the one parked syscall goroutine, not one per call.
func TestForceUnmountJoinsRepeatedWedged(t *testing.T) {
	release := make(chan struct{})
	drainForceUnmountProbes(t, release)
	var calls atomic.Int32
	swapUnmountFn(t, func(string) error {
		calls.Add(1)
		<-release
		return nil
	})
	swapForceUnmountTimeout(t, 20*time.Millisecond)

	const (
		dir      = "/pool/acct-wedged"
		reissues = 5
	)
	for i := 0; i < reissues; i++ {
		if err := ForceUnmount(dir); !errors.Is(err, ErrForceUnmountTimeout) {
			t.Fatalf("re-issue %d: ForceUnmount = %v, want ErrForceUnmountTimeout", i, err)
		}
	}
	if got := forceUnmountProbes.Inflight(); got != 1 {
		t.Fatalf("parked probes after %d re-issues = %d, want exactly 1 (joined, not leaked)", reissues, got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("syscall invocations = %d, want exactly 1 — repeated force-unmounts must join the parked goroutine, not re-spawn", got)
	}
}
