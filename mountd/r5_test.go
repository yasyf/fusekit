package mountd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/lease"
)

func swapWedgeRecheck(t *testing.T, d time.Duration) {
	t.Helper()
	prev := wedgeRecheck
	wedgeRecheck = d
	t.Cleanup(func() { wedgeRecheck = prev })
}

// wedgeServer drives a park to a FINAL WEDGE: a pending teardown whose call
// returns with the mount still reading mounted.
func wedgeServer(t *testing.T) (s *Server, fake *fakeHost, dir string) {
	t.Helper()
	const base = "/pool/base"
	dir = "/pool/acct-01"
	done := make(chan struct{})
	fake = &fakeHost{teardownFn: func(string, string) error { return pendingErr() }}
	host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{dir: done}}
	s = &Server{Host: host, Version: testVersion, Log: log.New(io.Discard, "", 0), LeaseDir: t.TempDir()}
	// A cancellable runCtx bounds the wedge-clear watcher to the test.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.runCtx = ctx
	s.initState()
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	if resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"}); resp.OK || resp.ErrClass != ClassWedged {
		t.Fatalf("pending unmount = (ok=%v class=%q), want wedged", resp.OK, resp.ErrClass)
	}
	// The call returns with the mount STILL up (fake.live[dir] stays true).
	close(done)
	if !s.awaitPark(dir, 2*time.Second) {
		t.Fatal("park never resolved")
	}
	return s, fake, dir
}

// TestResolvedWedgeFenceSurvivesGC pins R5-2: a park resolved to a final
// wedge stores its lease fence in server state — a strong reference — so the
// os.File finalizer can never silently close the fence fd (dropping LOCK_EX)
// while the in-memory claim remains; health surfaces the wedged dir.
func TestResolvedWedgeFenceSurvivesGC(t *testing.T) {
	s, _, dir := wedgeServer(t)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runtime.GC() // run finalizers: an unreferenced fence would close its fd
		if f, err := lease.Seize(s.LeaseDir, dir); err == nil {
			_ = f.Release()
			t.Fatal("Seize succeeded after GC — the wedged park's fence fd was finalized away")
		} else if !errors.Is(err, lease.ErrBusy) {
			t.Fatalf("Seize = %v, want ErrBusy", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := s.claim(dir); ok {
		t.Fatal("claim free under a resolved-to-wedge park")
	}

	resp := s.dispatch(Request{Op: OpHealth})
	if !resp.OK {
		t.Fatalf("health: %s", resp.Error)
	}
	if len(resp.WedgedDirs) != 1 || resp.WedgedDirs[0] != dir {
		t.Fatalf("health WedgedDirs = %v, want [%s]", resp.WedgedDirs, dir)
	}
}

// TestWedgeClearReleasesFenceAndClaims pins the wedge-clear path: once the
// mount is observed gone (an operator's external unmount), the stored fence
// and claims release, the wedge leaves health, and the dir is usable again.
func TestWedgeClearReleasesFenceAndClaims(t *testing.T) {
	swapWedgeRecheck(t, 20*time.Millisecond)
	s, fake, dir := wedgeServer(t)

	if resp := s.dispatch(Request{Op: OpHealth}); len(resp.WedgedDirs) != 1 {
		t.Fatalf("health WedgedDirs = %v, want the wedged dir", resp.WedgedDirs)
	}
	fake.setLive(dir, false) // the mount finally goes away

	deadline := time.Now().Add(2 * time.Second)
	for {
		if f, err := lease.Seize(s.LeaseDir, dir); err == nil {
			_ = f.Release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fence still held after the wedge cleared")
		}
		time.Sleep(5 * time.Millisecond)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		if release, ok := s.claim(dir); ok {
			release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("claim still held after the wedge cleared")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if resp := s.dispatch(Request{Op: OpHealth}); len(resp.WedgedDirs) != 0 {
		t.Fatalf("health WedgedDirs = %v, want empty after the clear", resp.WedgedDirs)
	}
}

// TestRetireParkCleanResolutionDropsRegistryRowKeepsJournal pins R5-9: a
// plain pending retire whose parked unmount later lands CLEAN drops the
// stale registry row (the mount is gone) while preserving the journal row —
// the successor's replay intent.
func TestRetireParkCleanResolutionDropsRegistryRowKeepsJournal(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	done := make(chan struct{})
	fake := &fakeHost{teardownFn: func(string, string) error { return pendingErr() }}
	host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{dir: done}}
	s := newHandlerServer(t, host)
	s.journal = newJournal(filepath.Join(t.TempDir(), "journal.json"))
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}

	if s.retireSweep() {
		t.Fatal("retireSweep = true, want an aborted sweep on the pending teardown")
	}
	if _, ok := s.registered(dir); !ok {
		t.Fatal("plain pending retire dropped the registry row before resolution")
	}

	// The parked call lands and the mount is genuinely gone.
	fake.setLive(dir, false)
	close(done)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, ok := s.registered(dir); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stale registry row survived a clean retire-park resolution")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, ok := s.journal.mount(dir); !ok {
		t.Fatal("journal row dropped by a retire park — the successor's replay intent is gone")
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		if f, err := lease.Seize(s.LeaseDir, dir); err == nil {
			_ = f.Release()
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("fence still held after the clean resolution")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestParkResolutionProbeNeverJoinsStaleProbe pins R5-7: the watcher's
// at-resolution kernel re-read is a FRESH probe. A wedged pre-resolution
// probe for the same dir is still in flight with a stale mounted=true
// verdict; joining it would manufacture a final wedge after the mount is
// gone.
func TestParkResolutionProbeNeverJoinsStaleProbe(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-stale"
	done := make(chan struct{})
	fake := &fakeHost{teardownFn: func(string, string) error { return pendingErr() }}
	host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{dir: done}}
	s := &Server{Host: host, Version: testVersion, Log: log.New(io.Discard, "", 0), LeaseDir: t.TempDir()}
	s.initState()
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}

	// Arm the stale probe: the FIRST State call parks holding mounted=true;
	// later calls answer the truth (gone).
	staleRelease := make(chan struct{})
	t.Cleanup(func() { close(staleRelease) })
	var calls atomic.Int32
	setState(fake, func(string) bool {
		if calls.Add(1) == 1 {
			<-staleRelease
			return true
		}
		return false
	}, func(string, string) bool { return false })
	go probeMount(s.Host.State, base, dir) // the in-flight probe a join would latch onto
	waitForCond(t, "stale probe in flight", func() bool { return calls.Load() == 1 })

	if resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"}); resp.OK || resp.ErrClass != ClassWedged {
		t.Fatalf("pending unmount = (ok=%v class=%q), want wedged", resp.OK, resp.ErrClass)
	}
	close(done) // the call returns; the mount is gone (fresh probes read false)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if f, err := lease.Seize(s.LeaseDir, dir); err == nil {
			_ = f.Release()
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("fence never released — the resolution probe joined the stale in-flight probe")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestUnmountEBusyClasses pins R5-4: a prompt EBUSY refusal
// (fusekit.ErrMountBusy) crosses the wire as retryable ClassBusy — the
// frozen protocol contract — while every other wedge stays ClassWedged; a
// busy refusal is final for this call, so the fence releases with the reply.
func TestUnmountEBusyClasses(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		wantClass string
	}{
		{name: "prompt EBUSY is retryable busy", err: fmt.Errorf("%w: %w: unmount answered EBUSY", fusekit.ErrUnmountWedged, fusekit.ErrMountBusy), wantClass: ClassBusy},
		{name: "plain wedge stays wedged", err: fmt.Errorf("%w: still mounted", fusekit.ErrUnmountWedged), wantClass: ClassWedged},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const base, dir = "/pool/base", "/pool/acct-01"
			fake := &fakeHost{teardownFn: func(string, string) error { return tc.err }}
			s := newHandlerServer(t, fake)
			if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
				t.Fatalf("mount: %s", resp.Error)
			}
			resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"})
			if resp.OK || resp.ErrClass != tc.wantClass {
				t.Fatalf("unmount = (ok=%v class=%q %q), want class %q", resp.OK, resp.ErrClass, resp.Error, tc.wantClass)
			}
			f, err := lease.Seize(s.LeaseDir, dir)
			if err != nil {
				t.Fatalf("Seize after a final refusal = %v, want free (no park)", err)
			}
			_ = f.Release()
		})
	}
}

// TestUnmountErrorCarriesJournalWarning pins R5-8: a wedged unmount already
// dropped the row and journal entry, so a failed journal write must surface
// on the ERROR reply's Warning too — never silently swallowed.
func TestUnmountErrorCarriesJournalWarning(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{teardownFn: func(string, string) error { return fusekit.ErrUnmountWedged }}
	s := newHandlerServer(t, fake)
	s.journal = newJournal(filepath.Join(t.TempDir(), "journal.json"))
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}

	// Break the journal under the live entry: the drop's save now fails.
	ro := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(ro, 0o700) })
	s.journal.path = filepath.Join(ro, "sub", "journal.json")

	resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassWedged {
		t.Fatalf("unmount = (ok=%v class=%q), want wedged", resp.OK, resp.ErrClass)
	}
	if !strings.Contains(resp.Warning, "journal") {
		t.Fatalf("Warning = %q, want the journal persist-warning on the error reply", resp.Warning)
	}
}

// TestSetupSpecCarriesReArmSignals pins R5-1(b): every spec the server
// mounts rides the server's own signal re-arm hook, so fusekit.Mount's
// post-ready signal.Reset is immediately followed by the holder
// re-registering its shutdown handler.
func TestSetupSpecCarriesReArmSignals(t *testing.T) {
	fake := &fakeHost{}
	s := newHandlerServer(t, fake)
	var armed atomic.Int32
	s.reArmSignals = func() { armed.Add(1) }
	if resp := s.dispatch(Request{Op: OpMount, Base: "/pool/base", Dir: "/pool/acct-01", Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	specs := fake.capturedSpecs()
	if len(specs) != 1 {
		t.Fatalf("Setup specs = %d, want 1", len(specs))
	}
	if specs[0].ReArmSignals == nil {
		t.Fatal("MountSpec.ReArmSignals = nil — the server's re-arm hook is not riding the spec")
	}
	specs[0].ReArmSignals()
	if got := armed.Load(); got != 1 {
		t.Fatalf("re-arm hook invocations = %d, want 1", got)
	}
}
