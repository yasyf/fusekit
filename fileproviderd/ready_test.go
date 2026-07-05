package fileproviderd

import (
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// swapReadDir seams readDirFn for one test; callers must not run in parallel
// (readDirFn is a package var).
func swapReadDir(t *testing.T, fn func(string) ([]os.DirEntry, error)) {
	t.Helper()
	prev := readDirFn
	readDirFn = fn
	t.Cleanup(func() { readDirFn = prev })
}

// swapReadyPollInterval seams readyPollInterval for one test; callers must not
// run in parallel.
func swapReadyPollInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := readyPollInterval
	readyPollInterval = d
	t.Cleanup(func() { readyPollInterval = prev })
}

// TestWaitDomainServes pins the readiness gate: a domain already serving returns
// nil on the first probe, a root that never serves returns ErrDomainNotServing at
// the deadline, and a root that starts serving after a couple of intervals returns
// nil once it does.
func TestWaitDomainServes(t *testing.T) {
	const root = "/cloud/acct-01"
	tests := []struct {
		name       string
		serveAfter int32 // failing probes before the root serves; -1 = never
		timeout    time.Duration
		wantErr    error
		wantProbes int32 // exact probe count expected once WaitDomainServes returns
	}{
		{name: "ready root serves on the first probe", serveAfter: 0, timeout: 5 * time.Second, wantErr: nil, wantProbes: 1},
		{name: "absent root never serves and hits the deadline", serveAfter: -1, timeout: 60 * time.Millisecond, wantErr: ErrDomainNotServing},
		{name: "root that appears after two intervals serves", serveAfter: 2, timeout: 5 * time.Second, wantErr: nil, wantProbes: 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			swapReadyPollInterval(t, 5*time.Millisecond)
			var calls atomic.Int32
			swapReadDir(t, func(name string) ([]os.DirEntry, error) {
				if name != root {
					t.Errorf("readDirFn got %q, want %q", name, root)
				}
				n := calls.Add(1)
				if tc.serveAfter >= 0 && n > tc.serveAfter {
					return nil, nil // the appex has materialized: enumeration succeeds
				}
				return nil, os.ErrNotExist // still materializing
			})

			err := WaitDomainServes(root, tc.timeout)
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("WaitDomainServes = %v, want nil", err)
				}
			} else if !errors.Is(err, tc.wantErr) {
				t.Fatalf("WaitDomainServes = %v, want errors.Is %v", err, tc.wantErr)
			}
			if tc.wantProbes > 0 && calls.Load() != tc.wantProbes {
				t.Errorf("readDirFn called %d times, want exactly %d", calls.Load(), tc.wantProbes)
			}
		})
	}
}

// TestWaitDomainServesRequiresRoot pins the fail-loud guard on an empty root.
func TestWaitDomainServesRequiresRoot(t *testing.T) {
	if err := WaitDomainServes("", time.Second); err == nil {
		t.Fatal("WaitDomainServes(\"\") = nil, want a loud required-arg failure")
	}
}

// TestWaitDomainServesHungProbeExits pins that a readDirFn parked inside a
// materializing appex does not block the caller — WaitDomainServes returns
// ErrDomainNotServing at the deadline — and that the worker goroutine drains and
// exits once the kernel finally answers, never looping back into readDirFn.
func TestWaitDomainServesHungProbeExits(t *testing.T) {
	swapReadyPollInterval(t, 5*time.Millisecond)
	release := make(chan struct{})
	probeReturned := make(chan struct{}, 4)
	var calls atomic.Int32
	swapReadDir(t, func(string) ([]os.DirEntry, error) {
		if calls.Add(1) == 1 {
			<-release // the first probe parks inside the "materializing appex"
		}
		probeReturned <- struct{}{}
		return nil, nil // serves once unblocked
	})

	start := time.Now()
	err := WaitDomainServes("/cloud/acct-01", 40*time.Millisecond)
	if !errors.Is(err, ErrDomainNotServing) {
		t.Fatalf("WaitDomainServes with a parked probe = %v, want ErrDomainNotServing", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("caller blocked %v behind the parked probe; the worker must not block the caller", elapsed)
	}

	// The worker is still parked in readDirFn; nothing has returned yet.
	select {
	case <-probeReturned:
		t.Fatal("readDirFn returned before release; the probe was supposed to be parked")
	default:
	}

	close(release) // the kernel finally answers
	select {
	case <-probeReturned:
	case <-time.After(2 * time.Second):
		t.Fatal("parked probe never drained after release; the worker goroutine leaked")
	}

	// A success on the (only) probe drives the worker down its buffered-send exit;
	// it must never loop back into readDirFn.
	time.Sleep(10 * readyPollInterval)
	if got := calls.Load(); got != 1 {
		t.Fatalf("readDirFn called %d times, want exactly 1 — the worker looped instead of exiting on the serving probe", got)
	}
}
