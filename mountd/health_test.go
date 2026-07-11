package mountd

import (
	"errors"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/lease"
)

func healthResp(t *testing.T, s *Server) Response {
	t.Helper()
	resp := s.dispatch(Request{Op: OpHealth})
	if !resp.OK {
		t.Fatalf("health: %s", resp.Error)
	}
	return resp
}

func TestHealthStatusFields(t *testing.T) {
	t.Run("journal-less server reports version-only zeros", func(t *testing.T) {
		resp := healthResp(t, newHandlerServer(t, &fakeHost{}))
		if resp.Version != testVersion {
			t.Errorf("Version = %q, want %q", resp.Version, testVersion)
		}
		if resp.Retiring || resp.ParkedUntil != 0 || resp.JournalMounts != 0 || resp.JournalBridges != 0 || len(resp.RetireStrikes) != 0 || resp.RetireDeferredDir != "" || resp.RetireDeferredReason != "" {
			t.Errorf("zero server leaked status: %+v", resp)
		}
	})

	t.Run("journal entry counts cover mounts and bridges", func(t *testing.T) {
		s, _ := newJournaledHandlerServer(t, &fakeHost{})
		mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
		mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool"})
		if err := s.journal.putBridge(bridgeEntry{Owner: "cc-pool", BridgeSocket: "/tmp/b.sock", ContentSocket: "/tmp/c.sock"}); err != nil {
			t.Fatal(err)
		}
		resp := healthResp(t, s)
		if resp.JournalMounts != 2 || resp.JournalBridges != 1 {
			t.Errorf("journal counts = %d/%d, want 2/1", resp.JournalMounts, resp.JournalBridges)
		}
		// An unmount co-updates the count — never a stale snapshot.
		if r := s.dispatch(Request{Op: OpUnmount, Base: "/b/b", Dir: "/m/b", Owner: "cc-pool"}); !r.OK {
			t.Fatalf("unmount: %s", r.Error)
		}
		if resp := healthResp(t, s); resp.JournalMounts != 1 {
			t.Errorf("JournalMounts after unmount = %d, want 1", resp.JournalMounts)
		}
	})

	t.Run("lease-gate deferral surfaces dir and reason, then clears", func(t *testing.T) {
		fake := &fakeHost{}
		s, _, _, _ := skewedServer(t, fake)
		mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
		h, err := lease.Acquire(s.LeaseDir, "/m/a", "cc-pool")
		if err != nil {
			t.Fatal(err)
		}

		r := newRetirer(s)
		if r.tick(time.Now()) {
			t.Fatal("lease-held tick retired")
		}
		resp := healthResp(t, s)
		if resp.RetireDeferredDir != "/m/a" {
			t.Errorf("RetireDeferredDir = %q, want /m/a", resp.RetireDeferredDir)
		}
		if !strings.Contains(resp.RetireDeferredReason, "v9.9.10") {
			t.Errorf("RetireDeferredReason = %q, want the skew reason", resp.RetireDeferredReason)
		}
		if resp.Retiring {
			t.Error("deferred holder reported Retiring; it serves normally")
		}

		if err := h.Close(); err != nil {
			t.Fatal(err)
		}
		if !r.tick(time.Now()) {
			t.Fatal("lease-free tick did not retire")
		}
		if resp := healthResp(t, s); resp.RetireDeferredDir != "" || resp.RetireDeferredReason != "" {
			t.Errorf("deferral survived the drain: %+v", resp)
		}
	})

	t.Run("lease summary counts total and held", func(t *testing.T) {
		s := newHandlerServer(t, &fakeHost{})
		h, err := lease.Acquire(s.LeaseDir, "/m/held", "cc-pool")
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		free, err := lease.Acquire(s.LeaseDir, "/m/free", "cc-pool")
		if err != nil {
			t.Fatal(err)
		}
		free.Close()
		resp := healthResp(t, s)
		if resp.LeasesTotal != 2 || resp.LeasesHeld != 1 {
			t.Errorf("lease summary = %d/%d, want total 2 held 1", resp.LeasesTotal, resp.LeasesHeld)
		}
	})

	t.Run("retiring flag mirrors the drain gate", func(t *testing.T) {
		s, _ := newJournaledHandlerServer(t, &fakeHost{})
		s.retiring.Store(true)
		if resp := healthResp(t, s); !resp.Retiring {
			t.Error("Retiring = false while the holder bounces new work")
		}
	})

	t.Run("storm-breaker park surfaces deadline and strike history", func(t *testing.T) {
		fake := &fakeHost{}
		s, _, _, _ := skewedServer(t, fake)
		mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
		release, ok := s.claim("/m/a") // every sweep aborts busy
		if !ok {
			t.Fatal("claim /m/a")
		}
		defer release()

		base := time.Now()
		r := newRetirer(s)
		for i, at := range []time.Duration{0, time.Minute, 2 * time.Minute} {
			if r.tick(base.Add(at)) {
				t.Fatalf("tick %d retired", i)
			}
		}
		resp := healthResp(t, s)
		if want := base.Add(2 * time.Minute).Add(retireParkLadder[0]).Unix(); resp.ParkedUntil != want {
			t.Errorf("ParkedUntil = %d, want %d", resp.ParkedUntil, want)
		}
		if resp.Retiring {
			t.Error("parked holder reported Retiring; it serves normally")
		}
		want := []int64{base.Unix(), base.Add(time.Minute).Unix(), base.Add(2 * time.Minute).Unix()}
		if !reflect.DeepEqual(resp.RetireStrikes, want) {
			t.Errorf("RetireStrikes = %v, want %v", resp.RetireStrikes, want)
		}
	})
}

// TestHealthConcurrentWithRetireTick races OpHealth snapshots against retire
// ticks; the race detector fails it if the snapshot bypasses the tick's locks.
func TestHealthConcurrentWithRetireTick(t *testing.T) {
	fake := &fakeHost{}
	s, _, _, _ := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	release, ok := s.claim("/m/a")
	if !ok {
		t.Fatal("claim /m/a")
	}
	defer release()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 100 {
			if resp := s.dispatch(Request{Op: OpHealth}); !resp.OK {
				t.Errorf("health: %s", resp.Error)
				return
			}
		}
	}()
	base := time.Now()
	r := newRetirer(s)
	for i := range 20 {
		r.tick(base.Add(time.Duration(i) * time.Second))
	}
	wg.Wait()
}

// TestHealthOpDeadlineBelowClientTimeout pins opDeadline's coupling rule for
// OpHealth: the server deadline binds, so a contended holder answers slow
// instead of blowing the client timeout into ErrHolderUnavailable.
func TestHealthOpDeadlineBelowClientTimeout(t *testing.T) {
	if got := opDeadline(OpHealth); got != time.Second {
		t.Fatalf("opDeadline(health) = %s, want 1s", got)
	}
	if opDeadline(OpHealth) >= healthClientTimeout {
		t.Fatalf("opDeadline(health) = %s, must sit BELOW the client timeout %s", opDeadline(OpHealth), healthClientTimeout)
	}
}

// TestStatusOverTheWire pins the client Status round-trip against a real server.
func TestStatusOverTheWire(t *testing.T) {
	fake := &fakeHost{}
	s := &Server{
		Socket:  filepath.Join(shortSockDir(t), "m.sock"),
		Host:    fake,
		Version: testVersion,
		Log:     log.New(io.Discard, "", 0),
	}
	_, cl, _, _ := runServer(t, s)
	st, err := cl.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Version != testVersion || st.Retiring || !st.ParkedUntil.IsZero() {
		t.Fatalf("Status = %+v, want a quiet holder at %q", st, testVersion)
	}
	h, err := cl.Hello()
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if h.Version != testVersion || !reflect.DeepEqual(h.Features, HolderFeatures) {
		t.Fatalf("Hello = %+v, want version %q features %v", h, testVersion, HolderFeatures)
	}
}

func TestClientStatusDecodesFields(t *testing.T) {
	socket, requests := startRawHolder(t, func(string) string {
		return `{"proto":2,"ok":true,"version":"v1.2.3","retiring":true,"parked_until":1765500000,"journal_mounts":2,"journal_bridges":1,"retire_strikes":[1765490000,1765499000],"retire_deferred_dir":"/m/a","retire_deferred_reason":"installed bundle is v1.2.4"}`
	})
	st, err := NewClient(socket).Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	want := &HealthStatus{
		Version:              "v1.2.3",
		Retiring:             true,
		ParkedUntil:          time.Unix(1765500000, 0),
		JournalMounts:        2,
		JournalBridges:       1,
		RetireStrikes:        []time.Time{time.Unix(1765490000, 0), time.Unix(1765499000, 0)},
		RetireDeferredDir:    "/m/a",
		RetireDeferredReason: "installed bundle is v1.2.4",
	}
	if !reflect.DeepEqual(st, want) {
		t.Fatalf("Status = %+v, want %+v", st, want)
	}
	if want := `{"proto":2,"op":"health"}`; requests()[0] != want {
		t.Fatalf("request = %s, want %s (frozen wire artifact)", requests()[0], want)
	}
}

// TestClientRefusesProtoOneHolder pins backward skew: a proto-1 holder's
// reply reads ErrProtoMismatch naming the cask upgrade — never a decode of
// stale fields.
func TestClientRefusesProtoOneHolder(t *testing.T) {
	socket, _ := startRawHolder(t, func(string) string {
		return `{"proto":1,"ok":true,"version":"v0.38.4"}`
	})
	_, err := NewClient(socket).Status()
	if !errors.Is(err, ErrProtoMismatch) {
		t.Fatalf("Status against a proto-1 holder = %v, want ErrProtoMismatch", err)
	}
	if !strings.Contains(err.Error(), "brew upgrade --cask fusekit-holder") {
		t.Fatalf("proto-mismatch error %q must name the cask upgrade", err)
	}
}
