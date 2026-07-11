package mountd

import (
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/internal/carcass"
	"github.com/yasyf/fusekit/lease"
)

// pendingHost wraps fakeHost with the TeardownPender capability.
type pendingHost struct {
	*fakeHost
	mu      sync.Mutex
	pending map[string]<-chan struct{}
}

func (p *pendingHost) TeardownDone(dir string) <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	ch := p.pending[dir]
	delete(p.pending, dir)
	return ch
}

func pendingErr() error {
	return fmt.Errorf("%w: %w: still in flight", fusekit.ErrUnmountWedged, fusekit.ErrTeardownPending)
}

// TestUnmountParksFenceOnPendingTeardown pins P-8: when the provider reports
// the graceful unmount STILL IN FLIGHT, the server does NOT release the lease
// fence or the dir claim with its wedged response — the dir stays busy and
// fenced until the teardown resolves, so no new session can acquire under a
// parked unmount that may land at any moment.
func TestUnmountParksFenceOnPendingTeardown(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	done := make(chan struct{})
	fake := &fakeHost{teardownFn: func(string, string) error { return pendingErr() }}
	host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{dir: done}}
	s := &Server{Host: host, Version: testVersion, Log: log.New(io.Discard, "", 0), LeaseDir: t.TempDir()}
	s.initState()
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}

	resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassWedged {
		t.Fatalf("pending unmount = (ok=%v class=%q %q), want wedged", resp.OK, resp.ErrClass, resp.Error)
	}

	// The claim is parked: a second op on the dir bounces busy.
	if resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"}); resp.OK || resp.ErrClass != ClassBusy {
		t.Fatalf("op during parked teardown = (ok=%v class=%q), want busy", resp.OK, resp.ErrClass)
	}
	// The fence is parked: no session (or fence) can take the lease.
	if _, err := lease.Seize(s.LeaseDir, dir); !errors.Is(err, lease.ErrBusy) {
		t.Fatalf("Seize during parked teardown = %v, want ErrBusy — the fence was released under a live parked unmount", err)
	}

	// The parked call lands (the mount actually came down), then resolution
	// releases fence and claim — the watcher re-reads kernel truth first.
	fake.setLive(dir, false)
	close(done)
	deadline := time.Now().Add(2 * time.Second)
	for {
		f, err := lease.Seize(s.LeaseDir, dir)
		if err == nil {
			_ = f.Release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fence still held after resolution: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	deadline = time.Now().Add(2 * time.Second)
	for {
		if _, ok := s.claim(dir); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("claim still held after resolution")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestParkResolvedToWedgeKeepsFenceAndClaim pins R4-3's park resolution
// re-evaluation: when the parked unmount CALL returns but the mountpoint is
// STILL up (a final refusal after the grace), the watcher resolves the park
// — no silent infinite park — but keeps the dir claimed and fenced, so a new
// session can never land on a mount in an unknowable state.
func TestParkResolvedToWedgeKeepsFenceAndClaim(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	done := make(chan struct{})
	fake := &fakeHost{teardownFn: func(string, string) error { return pendingErr() }}
	host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{dir: done}}
	s := &Server{Host: host, Version: testVersion, Log: log.New(io.Discard, "", 0), LeaseDir: t.TempDir()}
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
		t.Fatal("park never resolved — a final refusal must resolve the park, not park forever")
	}
	// Resolved TO A WEDGE: fence and claim stay held.
	if _, err := lease.Seize(s.LeaseDir, dir); !errors.Is(err, lease.ErrBusy) {
		t.Fatalf("Seize after a resolved-to-wedge park = %v, want ErrBusy (the fence must stay held)", err)
	}
	if _, ok := s.claim(dir); ok {
		t.Fatal("claim free after a resolved-to-wedge park — a new session could land on the wedged mount")
	}
	// The shutdown wait does not hang on it: the watcher completed.
	waitDone := make(chan struct{})
	go func() { s.parkWG.Wait(); close(waitDone) }()
	select {
	case <-waitDone:
	case <-time.After(2 * time.Second):
		t.Fatal("parkWG still pending after the park resolved to a wedge")
	}
}

// TestUnmountFinalWedgeReleasesImmediately is the negative control: a FINAL
// wedge (no pending) must release the fence with the response — parking is
// only for unknown outcomes.
func TestUnmountFinalWedgeReleasesImmediately(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{teardownFn: func(string, string) error { return fusekit.ErrUnmountWedged }}
	host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{}}
	s := &Server{Host: host, Version: testVersion, Log: log.New(io.Discard, "", 0), LeaseDir: t.TempDir()}
	s.initState()
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}

	resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassWedged {
		t.Fatalf("final wedge = (ok=%v class=%q), want wedged", resp.OK, resp.ErrClass)
	}
	f, err := lease.Seize(s.LeaseDir, dir)
	if err != nil {
		t.Fatalf("Seize after final wedge = %v, want free (outcome known — no park)", err)
	}
	_ = f.Release()
	release, ok := s.claim(dir)
	if !ok {
		t.Fatal("claim still held after a final wedge")
	}
	release()
}

// TestMountParksClaimsOnPendingForce pins R3's wedged-force parking: a
// carcass force that timed out (carcass.PendingForce) may still land, so the
// pre-mount clear transfers the dir's fence and claim to a watcher keyed on
// the force process's exit — never released while the late unmount can land
// under a fresh session.
func TestMountParksClaimsOnPendingForce(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{}
	var corpse atomic.Bool
	corpse.Store(true) // the carcass mountpoint; cleared when the late force lands
	setState(fake, func(string) bool { return corpse.Load() }, func(string, string) bool { return false })
	s := newHandlerServer(t, fake)

	forceDone := make(chan struct{})
	prev := clearCarcass
	clearCarcass = func(d string, _ func() error) error {
		return &carcass.PendingForce{Dir: d, Done: forceDone}
	}
	t.Cleanup(func() { clearCarcass = prev })

	resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassWedged {
		t.Fatalf("mount over a pending force = (ok=%v class=%q %q), want wedged", resp.OK, resp.ErrClass, resp.Error)
	}
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); resp.OK || resp.ErrClass != ClassBusy {
		t.Fatalf("op during parked force = (ok=%v class=%q), want busy — the claim was released under an unresolved force", resp.OK, resp.ErrClass)
	}
	if _, err := lease.Seize(s.LeaseDir, dir); !errors.Is(err, lease.ErrBusy) {
		t.Fatalf("Seize during parked force = %v, want ErrBusy — the fence was released under an unresolved force", err)
	}

	// The late force lands (the carcass is gone), then the watcher releases.
	corpse.Store(false)
	close(forceDone)
	deadline := time.Now().Add(2 * time.Second)
	for {
		f, err := lease.Seize(s.LeaseDir, dir)
		if err == nil {
			_ = f.Release()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fence still held after the force resolved: %v", err)
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
			t.Fatal("claim still held after the force resolved")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestMountParksClaimsOnPendingFirstChildSetup pins R3's failed-first-child
// routing: a mux tenant whose Setup failed with the empty root's unmount
// still in flight (ErrTeardownPending via the unwind) parks the dir AND root
// claims on the pending-teardown machinery instead of releasing them — a
// retry must not stack onto the in-flight root unmount.
func TestMountParksClaimsOnPendingFirstChildSetup(t *testing.T) {
	const base, root, dir = "/pool/base", "/pool/mnt", "/pool/mnt/acct-01"
	done := make(chan struct{})
	fake := &fakeHost{setupFn: func(string, string) error {
		return fmt.Errorf("first tenant build failed: %w", pendingErr())
	}}
	host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{dir: done}}
	s := newHandlerServer(t, host)

	resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, MuxRoot: root, Owner: "cc-pool"})
	if resp.OK {
		t.Fatalf("mount with a pending first-child unwind = OK, want error")
	}
	if _, ok := s.claim(dir); ok {
		t.Fatal("dir claim released under a pending first-child root unwind")
	}
	if _, ok := s.claim(root); ok {
		t.Fatal("root claim released under a pending first-child root unwind")
	}

	close(done)
	deadline := time.Now().Add(2 * time.Second)
	for {
		if release, ok := s.claim(dir); ok {
			release()
			if rootRelease, rok := s.claim(root); rok {
				rootRelease()
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("claims still held after the pending unwind resolved")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestShutdownWaitsOnUnresolvedParks pins R4-4: Run must not return — and so
// the process must not exit, dropping every EX-fence fd — while a park
// watcher's unmount call is still in flight; it returns once the park
// resolves.
func TestShutdownWaitsOnUnresolvedParks(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	done := make(chan struct{})
	fake := &fakeHost{teardownFn: func(string, string) error { return pendingErr() }}
	host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{dir: done}}
	socket := filepath.Join(shortSockDir(t), "m.sock")
	_, cl, runDone, cancel := runServer(t, &Server{Socket: socket, Host: host, Version: testVersion, Log: log.New(io.Discard, "", 0)})

	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("mount: %v", err)
	}
	if _, err := cl.Unmount(base, dir); !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("Unmount = %v, want the pending wedge", err)
	}

	cancel()
	select {
	case err := <-runDone:
		t.Fatalf("Run returned (%v) with a park unresolved — process exit would drop the EX fence mid-flight", err)
	case <-time.After(300 * time.Millisecond):
	}

	fake.setLive(dir, false)
	close(done)
	select {
	case <-runDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Run never returned after the park resolved")
	}
}

// penderlessHost implements Host WITHOUT TeardownPender — Validate must
// refuse it structurally, not at error time.
type penderlessHost struct{}

func (penderlessHost) Setup(fusekit.MountSpec) error              { return nil }
func (penderlessHost) Teardown(string, string) error              { return nil }
func (penderlessHost) State(string, string) (mounted, alive bool) { return false, false }

func TestValidateRequiresTeardownPender(t *testing.T) {
	s := &Server{Host: penderlessHost{}, LeaseDir: t.TempDir()}
	err := s.Validate()
	if err == nil || !strings.Contains(err.Error(), "TeardownPender") {
		t.Fatalf("Validate(penderless host) = %v, want a TeardownPender refusal", err)
	}
	// Negative leg: a pender-capable host validates clean.
	if err := (&Server{Host: &fakeHost{}, LeaseDir: t.TempDir()}).Validate(); err != nil {
		t.Fatalf("Validate(fakeHost) = %v, want nil", err)
	}
}
