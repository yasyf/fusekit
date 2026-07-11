package mountd

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
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

	// Resolution releases fence and claim.
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
