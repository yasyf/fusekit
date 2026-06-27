//go:build fuse && cgo

package fusekit

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakeHost stands in for *fuse.FileSystemHost in Handle.Unmount tests: its
// graceful Unmount records that it was issued but NEVER closes the serving
// goroutine's done channel, so a test drives busy-vs-idle entirely through that
// channel. It satisfies the unmounter seam.
type fakeHost struct {
	calls atomic.Int32
}

func (f *fakeHost) Unmount() bool {
	f.calls.Add(1)
	return true
}

// swapUnmountGraces shrinks the teardown grace timers for one test so a busy
// mount's escalation decision is reached in milliseconds, not seconds.
func swapUnmountGraces(t *testing.T, unmount, force time.Duration) {
	t.Helper()
	pu, pf := unmountGrace, forceGrace
	unmountGrace, forceGrace = unmount, force
	t.Cleanup(func() { unmountGrace, forceGrace = pu, pf })
}

// swapMountedFn replaces the post-teardown mountpoint predicate for one test, so
// Handle.Unmount's wedged-vs-clean verdict is exercised without a real mount.
func swapMountedFn(t *testing.T, fn func(string) bool) {
	t.Helper()
	prev := mountedFn
	mountedFn = fn
	t.Cleanup(func() { mountedFn = prev })
}

// swapReapServers stubs the orphan-server reaper for one test so the idle
// teardown path never runs the real darwin process scan.
func swapReapServers(t *testing.T, fn func(string)) {
	t.Helper()
	prev := reapServers
	reapServers = fn
	t.Cleanup(func() { reapServers = prev })
}

// TestHandleUnmountForceDecision pins the graceful-only-by-default teardown. A
// busy mount (the graceful host.Unmount never closes done) escalates to the
// forced-unmount seam ONLY when ForceOnWedge is set, and either way reports
// ErrUnmountWedged while the mount is still up; an idle mount (done pre-closed,
// no longer a mountpoint) tears down cleanly and never forces, regardless of the
// flag. Pure seam injection — no real mount, no holder, never run live.
func TestHandleUnmountForceDecision(t *testing.T) {
	cases := []struct {
		name         string
		dir          string
		forceOnWedge bool
		doneClosed   bool  // idle: the serving goroutine already returned
		mounted      bool  // what the post-teardown Mounted seam reports
		wantErr      error // nil means a clean teardown
		wantForce    bool  // whether the forced-unmount seam must be called
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

			// forceTarget receives the dir handed to the forced-unmount seam.
			// Buffered so the (async) force goroutine never blocks delivering it,
			// even if Handle.Unmount has already returned.
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

			// The busy path waits out unmountGrace before deciding, so the
			// graceful host.Unmount has provably been issued by the time Unmount
			// returns; the idle path may return before its goroutine is scheduled,
			// so only assert it where it is deterministic.
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
			// No code path may issue the force; give any stray goroutine a beat,
			// then assert the seam stayed untouched.
			select {
			case got := <-forceTarget:
				t.Fatalf("forced-unmount seam called for %q, want never (graceful-only)", got)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}
