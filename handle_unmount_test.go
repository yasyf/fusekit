//go:build fuse && cgo

package fusekit

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// fakeHost stubs the unmounter seam; busy-vs-idle is driven by block (the
// unmount CALL in flight), never by the serve-loop channel.
type fakeHost struct {
	calls atomic.Int32
	ret   bool
	block chan struct{} // non-nil: Unmount parks on it (call in flight)
}

func (f *fakeHost) Unmount() bool {
	f.calls.Add(1)
	if f.block != nil {
		<-f.block
	}
	return f.ret
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

// TestHandleUnmountOutcomes pins graceful-only teardown with outcome honesty
// keyed on the UNMOUNT CALL, not the serve loop: no force escalation of any
// kind; a returned call with the mount still up — a prompt cgofuse refusal
// included, even with the serve loop still open — is a FINAL wedge
// (ErrUnmountWedged, never a park); only a call still in flight past the
// grace with the mount still up reads ErrTeardownPending. Seam-injected — no
// real mount, no holder.
func TestHandleUnmountOutcomes(t *testing.T) {
	cases := []struct {
		name        string
		opInFlight  bool // host.Unmount parks past the grace
		opRet       bool // host.Unmount's bool when it returns promptly
		mounted     bool // post-teardown mountedFn verdict
		wantErr     error
		wantPending bool
	}{
		{name: "returned_and_unmounted_is_clean", opRet: true},
		{name: "returned_true_but_still_mounted_is_final_wedge", opRet: true, mounted: true, wantErr: ErrUnmountWedged},
		{name: "prompt_false_return_still_mounted_is_final_wedge_never_pending", opRet: false, mounted: true, wantErr: ErrUnmountWedged},
		{name: "in_flight_and_unmounted_is_clean", opInFlight: true},
		{name: "in_flight_and_still_mounted_is_pending", opInFlight: true, mounted: true, wantErr: ErrUnmountWedged, wantPending: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swapUnmountGrace(t, 20*time.Millisecond)
			swapMountedFn(t, func(string) bool { return tc.mounted })
			reaped := make(chan string, 1)
			swapReapServers(t, func(dir string) { reaped <- dir })

			host := &fakeHost{ret: tc.opRet}
			if tc.opInFlight {
				host.block = make(chan struct{})
				t.Cleanup(func() { close(host.block) })
			}
			h := &Handle{host: host, dir: "/fake/mnt", done: make(chan struct{})}

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
			if host.calls.Load() == 0 {
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

	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	h := &Handle{host: &fakeHost{block: block}, dir: "/fake/wedged", done: make(chan struct{})}
	if err := h.Unmount(); !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("Unmount() = %v, want ErrUnmountWedged", err)
	}
}
