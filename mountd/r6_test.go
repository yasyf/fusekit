package mountd

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/lease"
)

// TestProbePassesReArmSignals pins R6-1: OpProbe hands the server's re-arm
// hook to the probe — its throwaway mount defuses cgofuse's signal handler
// like any other mount, and a probe without the hook would strip the
// holder's own SIGTERM subscription (default-terminate under live mounts).
func TestProbePassesReArmSignals(t *testing.T) {
	s := newHandlerServer(t, &fakeHost{})
	var armed atomic.Int32
	s.reArmSignals = func() { armed.Add(1) }
	var got func()
	s.Probe = func(reArm func()) (bool, error) {
		got = reArm
		return true, nil
	}
	if resp := s.dispatch(Request{Op: OpProbe}); !resp.OK || !resp.FuseOK {
		t.Fatalf("probe = %+v, want OK with FuseOK=true", resp)
	}
	if got == nil {
		t.Fatal("OpProbe passed a nil re-arm hook — the probe mount would strip the holder's SIGTERM subscription")
	}
	got()
	if armed.Load() != 1 {
		t.Fatal("the hook passed to Probe is not the server's reArmSignals")
	}
}

// TestContractViolationWedgeIsPermanent pins R6-9: a nil-channel pending
// verdict has no proof the original call ever returned, so its wedge is
// PERMANENT — health marks it, and even an observed-gone mount never
// auto-releases the fence or claims; only a holder restart clears it.
func TestContractViolationWedgeIsPermanent(t *testing.T) {
	swapWedgeRecheck(t, 10*time.Millisecond)
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{teardownFn: func(string, string) error { return pendingErr() }}
	// No resolution channel for dir: TeardownDone answers nil.
	host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{}}
	s := &Server{Host: host, Version: testVersion, Log: log.New(io.Discard, "", 0), LeaseDir: t.TempDir()}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.runCtx = ctx
	s.initState()
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	if resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"}); resp.OK || resp.ErrClass != ClassWedged {
		t.Fatalf("nil-channel unmount = (ok=%v class=%q), want wedged", resp.OK, resp.ErrClass)
	}

	resp := s.dispatch(Request{Op: OpHealth})
	want := dir + WedgeContractViolation
	if len(resp.WedgedDirs) != 1 || resp.WedgedDirs[0] != want {
		t.Fatalf("health WedgedDirs = %v, want [%q]", resp.WedgedDirs, want)
	}

	// The mount is observed gone — and the wedge must STILL hold: no
	// watcher may release a contract violation.
	fake.setLive(dir, false)
	time.Sleep(100 * time.Millisecond) // ~10 recheck intervals
	if f, err := lease.Seize(s.LeaseDir, dir); err == nil {
		_ = f.Release()
		t.Fatal("fence released on an observed-gone mount — a contract-violation wedge must be permanent")
	} else if !errors.Is(err, lease.ErrBusy) {
		t.Fatalf("Seize = %v, want ErrBusy", err)
	}
	if _, ok := s.claim(dir); ok {
		t.Fatal("claim free under a permanent contract-violation wedge")
	}
	if resp := s.dispatch(Request{Op: OpHealth}); len(resp.WedgedDirs) != 1 || resp.WedgedDirs[0] != want {
		t.Fatalf("health WedgedDirs = %v, want the permanent entry %q", resp.WedgedDirs, want)
	}
}

// TestWedgeClearSingleFlightProbe pins R6-5: the wedge-clear watcher never
// launches a new probe while the previous one is still blocked in
// Host.State — a permanently wedged State costs ONE stuck goroutine total,
// not one per tick.
func TestWedgeClearSingleFlightProbe(t *testing.T) {
	swapWedgeRecheck(t, 5*time.Millisecond)
	_, fake, _ := wedgeServer(t)

	var calls atomic.Int32
	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	setState(fake, func(string) bool {
		calls.Add(1)
		<-block
		return true
	}, func(string, string) bool { return true })

	waitForCond(t, "the first blocked probe", func() bool { return calls.Load() >= 1 })
	time.Sleep(50 * time.Millisecond) // ~10 ticks: the old code would stack ~10 probes
	if got := calls.Load(); got != 1 {
		t.Fatalf("probes launched while one is still in flight = %d, want 1 (single-flight)", got)
	}
}

// TestResolveGoneJournalDropFailureRetainedInHealth pins R6-8: a park
// resolution's late journal drop has no reply left to carry its warning, so
// it must land in health's Warning (not just the log) — and a later clean
// resolution of the same dir clears it.
func TestResolveGoneJournalDropFailureRetainedInHealth(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	s := newHandlerServer(t, &fakeHost{})
	s.journal = newJournal(filepath.Join(t.TempDir(), "journal.json"))
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}

	// Break the journal under the live entry: the resolution's drop fails.
	ro := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(ro, 0o700) })
	s.journal.path = filepath.Join(ro, "sub", "journal.json")

	s.resolveGone("unmount (in-flight teardown)", dir, true, nil)
	if w := s.dispatch(Request{Op: OpHealth}).Warning; !strings.Contains(w, "journal drop failed") {
		t.Fatalf("health Warning = %q, want the late journal-drop failure retained", w)
	}

	// A later clean resolution of the same dir clears the stale warning.
	s.resolveGone("unmount (in-flight teardown)", dir, true, nil)
	if w := s.dispatch(Request{Op: OpHealth}).Warning; strings.Contains(w, "journal drop failed") {
		t.Fatalf("health Warning = %q, want the warning cleared by the clean resolution", w)
	}
}

// TestReclaimJournalOnlyFailureRetainsWarning pins R6-8: a rowless journal
// entry whose reclaim unmount FAILS surfaces only its dir to the caller —
// the failed reply's persist-warning must be retained in health, never
// discarded with the reply.
func TestReclaimJournalOnlyFailureRetainsWarning(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{teardownFn: func(string, string) error { return fusekit.ErrUnmountWedged }}
	s := newHandlerServer(t, fake)
	s.journal = newJournal(filepath.Join(t.TempDir(), "journal.json"))
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	// Rowless journal entry: the lease-deferred-replay shape.
	s.mu.Lock()
	delete(s.registry, dir)
	s.mu.Unlock()

	ro := filepath.Join(t.TempDir(), "ro")
	if err := os.Mkdir(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(ro, 0o700) })
	s.journal.path = filepath.Join(ro, "sub", "journal.json")

	resp := s.dispatch(Request{Op: OpReclaim, Owner: "cc-pool"})
	if !resp.OK || len(resp.Mounts) != 1 || resp.Mounts[0].Dir != dir {
		t.Fatalf("reclaim = %+v, want OK with %s reported failed", resp, dir)
	}
	health := s.dispatch(Request{Op: OpHealth})
	if !strings.Contains(health.Warning, dir) || !strings.Contains(health.Warning, "journal") {
		t.Fatalf("health Warning = %q, want the failed reclaim's journal persist-warning retained", health.Warning)
	}
}
