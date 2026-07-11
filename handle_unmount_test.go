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

func swapUnmountGrace(t *testing.T, d time.Duration) {
	t.Helper()
	prev := unmountGrace
	unmountGrace = d
	t.Cleanup(func() { unmountGrace = prev })
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

// TestHandleUnmountOutcomes pins graceful-only teardown with outcome honesty:
// there is no force escalation of any kind, a final wedge reads
// ErrUnmountWedged, and a teardown still in flight past the grace reads
// ErrTeardownPending (wrapping ErrUnmountWedged) so a fence holder knows the
// outcome is unknown. Seam-injected — no real mount, no holder.
func TestHandleUnmountOutcomes(t *testing.T) {
	cases := []struct {
		name        string
		doneClosed  bool // serving goroutine already returned
		mounted     bool // post-teardown mountedFn verdict
		wantErr     error
		wantPending bool
	}{
		{name: "returned_and_unmounted_is_clean", doneClosed: true},
		{name: "returned_but_still_mounted_is_final_wedge", doneClosed: true, mounted: true, wantErr: ErrUnmountWedged},
		{name: "in_flight_and_unmounted_is_clean", mounted: false},
		{name: "in_flight_and_still_mounted_is_pending", mounted: true, wantErr: ErrUnmountWedged, wantPending: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swapUnmountGrace(t, 20*time.Millisecond)
			swapMountedFn(t, func(string) bool { return tc.mounted })
			reaped := make(chan string, 1)
			swapReapServers(t, func(dir string) { reaped <- dir })

			host := &fakeHost{}
			done := make(chan struct{})
			if tc.doneClosed {
				close(done)
			}
			h := &Handle{host: host, dir: "/fake/mnt", done: done}

			err := h.Unmount()
			switch {
			case tc.wantErr == nil:
				if err != nil {
					t.Fatalf("Unmount() = %v, want nil (clean teardown)", err)
				}
				select {
				case <-reaped:
				default:
					t.Error("clean teardown never reaped the dir's own prior server")
				}
			case !errors.Is(err, tc.wantErr):
				t.Fatalf("Unmount() = %v, want %v", err, tc.wantErr)
			}
			if got := errors.Is(err, ErrTeardownPending); got != tc.wantPending {
				t.Fatalf("errors.Is(err, ErrTeardownPending) = %v, want %v (err=%v)", got, tc.wantPending, err)
			}
			if tc.wantErr != nil {
				select {
				case dir := <-reaped:
					t.Fatalf("wedged/pending teardown reaped %q — the mount may still be live", dir)
				default:
				}
			}
			// The busy cases wait out unmountGrace, so the graceful call is
			// deterministically issued; idle can return before its goroutine runs.
			if !tc.doneClosed && host.calls.Load() == 0 {
				t.Error("graceful host.Unmount was never issued")
			}
		})
	}
}

// TestHandleUnmountNeverForces pins that a wedged teardown leaves no force
// side effects: Handle carries no force path at all, so the only observable
// actions are the graceful host.Unmount and — on a clean outcome only — the
// own-server reap.
func TestHandleUnmountNeverForces(t *testing.T) {
	swapUnmountGrace(t, 20*time.Millisecond)
	swapMountedFn(t, func(string) bool { return true })
	swapReapServers(t, func(dir string) { t.Errorf("reaped %q under a still-mounted dir", dir) })

	h := &Handle{host: &fakeHost{}, dir: "/fake/wedged", done: make(chan struct{})}
	if err := h.Unmount(); !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("Unmount() = %v, want ErrUnmountWedged", err)
	}
}
