//go:build fuse && cgo

package fusekit

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakeHost stubs the unmounter seam; busy-vs-idle is driven by Handle.done,
// never by the host.
type fakeHost struct {
	calls atomic.Int32
}

func (f *fakeHost) Unmount() bool {
	f.calls.Add(1)
	return true
}

func swapUnmountGraces(t *testing.T, unmount, force time.Duration) {
	t.Helper()
	pu, pf := unmountGrace, forceGrace
	unmountGrace, forceGrace = unmount, force
	t.Cleanup(func() { unmountGrace, forceGrace = pu, pf })
}

func swapMountedFn(t *testing.T, fn func(string) bool) {
	t.Helper()
	prev := mountedFn
	mountedFn = fn
	t.Cleanup(func() { mountedFn = prev })
}

// swapReapServers stubs the orphan-server reaper: tests must never run the
// real process scan.
func swapReapServers(t *testing.T, fn func(string)) {
	t.Helper()
	prev := reapServers
	reapServers = fn
	t.Cleanup(func() { reapServers = prev })
}

// TestHandleUnmountForceDecision pins graceful-only teardown: busy escalates
// to the force seam only with ForceOnWedge (ErrUnmountWedged either way); idle
// never forces. Seam-injected — no real mount, no holder.
func TestHandleUnmountForceDecision(t *testing.T) {
	cases := []struct {
		name         string
		dir          string
		forceOnWedge bool
		doneClosed   bool // idle: serving goroutine already returned
		mounted      bool // post-teardown mountedFn verdict
		wantErr      error
		wantForce    bool
	}{
		{
			name:    "busy_no_force_reports_wedged",
			dir:     "/fake/busy-no-force",
			mounted: true,
			wantErr: ErrUnmountWedged,
		},
		{
			name:         "busy_force_escalates",
			dir:          "/fake/busy-force",
			forceOnWedge: true,
			mounted:      true,
			wantErr:      ErrUnmountWedged,
			wantForce:    true,
		},
		{
			name:       "idle_no_force_clean",
			dir:        "/fake/idle-no-force",
			doneClosed: true,
		},
		{
			name:         "idle_force_clean",
			dir:          "/fake/idle-force",
			forceOnWedge: true,
			doneClosed:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swapUnmountGraces(t, 20*time.Millisecond, 20*time.Millisecond)
			swapMountedFn(t, func(string) bool { return tc.mounted })
			swapReapServers(t, func(string) {})

			// Buffered: the async force goroutine must never block sending after
			// Unmount returns.
			forceTarget := make(chan string, 1)
			swapUnmountFn(t, func(dir string) error {
				forceTarget <- dir
				return nil
			})

			host := &fakeHost{}
			done := make(chan struct{})
			if tc.doneClosed {
				close(done)
			}
			h := &Handle{host: host, dir: tc.dir, done: done, forceOnWedge: tc.forceOnWedge}

			err := h.Unmount()
			if tc.wantErr == nil {
				if err != nil {
					t.Fatalf("Unmount() = %v, want nil (clean teardown)", err)
				}
			} else if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Unmount() = %v, want %v", err, tc.wantErr)
			}

			// Idle can return before its goroutine runs; assert the graceful call
			// only in the deterministic busy case, which waits out unmountGrace.
			if !tc.doneClosed && host.calls.Load() == 0 {
				t.Error("graceful host.Unmount was never issued")
			}

			if tc.wantForce {
				select {
				case got := <-forceTarget:
					if got != tc.dir {
						t.Fatalf("forced-unmount target = %q, want %q", got, tc.dir)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("ForceOnWedge=true on a busy mount: the forced-unmount seam was never called")
				}
				return
			}
			// Give any stray async force a beat to surface.
			select {
			case got := <-forceTarget:
				t.Fatalf("forced-unmount seam called for %q, want never (graceful-only)", got)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}
