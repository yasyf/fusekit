//go:build fuse && cgo

package fusekit

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeHost stubs the unmounter seam ONLY to pin that Handle.Unmount never
// invokes it: cgofuse's Unmount is unconditionally MNT_FORCE on Darwin, so a
// single call is a force-unmount escape.
type fakeHost struct {
	calls atomic.Int32
}

func (f *fakeHost) Unmount() bool {
	f.calls.Add(1)
	return false
}

// unmountCall is one recorded unmountFn invocation.
type unmountCall struct {
	dir   string
	flags int
}

// fakeUnmount drives the unmountFn seam: records each call, optionally parks
// on block (the call in flight), then returns err.
type fakeUnmount struct {
	mu    sync.Mutex
	calls []unmountCall
	block chan struct{} // non-nil: the call parks on it
	err   error
}

func (f *fakeUnmount) unmount(dir string, flags int) error {
	f.mu.Lock()
	f.calls = append(f.calls, unmountCall{dir: dir, flags: flags})
	blk := f.block
	f.mu.Unlock()
	if blk != nil {
		<-blk
	}
	return f.err
}

func (f *fakeUnmount) recorded() []unmountCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]unmountCall(nil), f.calls...)
}

func swapUnmountFn(t *testing.T, fn func(dir string, flags int) error) {
	t.Helper()
	prev := unmountFn
	unmountFn = fn
	t.Cleanup(func() { unmountFn = prev })
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
// keyed on the UNMOUNT CALL, not the serve loop: the teardown is fusekit's
// OWN unmount(2) with flags=0 — cgofuse's forcing Unmount is NEVER invoked; a
// returned call with the mount still up — a prompt EBUSY refusal included,
// even with the serve loop still open — is a FINAL wedge (ErrUnmountWedged,
// never a park); only a call still in flight past the grace with the mount
// still up reads ErrTeardownPending, and UnmountDone resolves when THAT call
// returns. Seam-injected — no real mount, no holder.
func TestHandleUnmountOutcomes(t *testing.T) {
	cases := []struct {
		name        string
		opInFlight  bool  // the unmount(2) call parks past the grace
		opErr       error // the call's prompt error
		mounted     bool  // post-teardown mountedFn verdict
		wantErr     error
		wantPending bool
	}{
		{name: "returned_and_unmounted_is_clean"},
		{name: "errored_but_unmounted_is_clean_kernel_truth_wins", opErr: errors.New("EINVAL: not mounted")},
		{name: "prompt_ebusy_still_mounted_is_final_wedge_never_pending", opErr: errors.New("EBUSY"), mounted: true, wantErr: ErrUnmountWedged},
		{name: "returned_nil_but_still_mounted_is_final_wedge", mounted: true, wantErr: ErrUnmountWedged},
		{name: "in_flight_and_unmounted_is_clean", opInFlight: true},
		{name: "in_flight_and_still_mounted_is_pending", opInFlight: true, mounted: true, wantErr: ErrUnmountWedged, wantPending: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swapUnmountGrace(t, 20*time.Millisecond)
			swapMountedFn(t, func(string) bool { return tc.mounted })
			reaped := make(chan string, 1)
			swapReapServers(t, func(dir string) { reaped <- dir })

			fu := &fakeUnmount{err: tc.opErr}
			var blk chan struct{}
			if tc.opInFlight {
				blk = make(chan struct{})
				fu.block = blk
			}
			swapUnmountFn(t, fu.unmount)

			host := &fakeHost{}
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
			if host.calls.Load() != 0 {
				t.Fatal("Handle.Unmount invoked cgofuse's Unmount — that call is MNT_FORCE on Darwin")
			}
			calls := fu.recorded()
			if len(calls) != 1 || calls[0].dir != "/fake/mnt" || calls[0].flags != 0 {
				t.Fatalf("unmount(2) calls = %+v, want exactly one for /fake/mnt with flags=0", calls)
			}

			// UnmountDone is the CALL's resolution channel: open while the call
			// is parked, closed once it returns.
			ch := h.UnmountDone()
			if ch == nil {
				t.Fatal("UnmountDone = nil after an Unmount call")
			}
			if tc.opInFlight {
				select {
				case <-ch:
					t.Fatal("UnmountDone closed while the call is still parked")
				default:
				}
				close(blk)
			}
			select {
			case <-ch:
			case <-time.After(2 * time.Second):
				t.Fatal("UnmountDone never closed after the call returned")
			}
		})
	}
}

// TestHandleUnmountNeverForces pins that a wedged teardown leaves no force
// side effects: Handle carries no force path at all — cgofuse's forcing
// Unmount is never issued, the kernel call is flags=0 — so the only
// observable actions are that graceful call and, on a clean outcome only,
// the own-server reap.
func TestHandleUnmountNeverForces(t *testing.T) {
	swapUnmountGrace(t, 20*time.Millisecond)
	swapMountedFn(t, func(string) bool { return true })
	swapReapServers(t, func(dir string) { t.Errorf("reaped %q under a still-mounted dir", dir) })

	fu := &fakeUnmount{block: make(chan struct{})}
	t.Cleanup(func() { close(fu.block) })
	swapUnmountFn(t, fu.unmount)

	host := &fakeHost{}
	h := &Handle{host: host, dir: "/fake/wedged", done: make(chan struct{})}
	if err := h.Unmount(); !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("Unmount() = %v, want ErrUnmountWedged", err)
	}
	if host.calls.Load() != 0 {
		t.Fatal("wedged teardown escalated into cgofuse's forcing Unmount")
	}
	for _, c := range fu.recorded() {
		if c.flags != 0 {
			t.Fatalf("unmount(2) issued with flags=%d, want 0 (graceful only)", c.flags)
		}
	}
}
