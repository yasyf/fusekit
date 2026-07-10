package mountd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
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
		resp := healthResp(t, newHandlerServer(&fakeHost{}))
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

	t.Run("idle-gate deferral surfaces dir and reason, then clears", func(t *testing.T) {
		fake := &fakeHost{}
		s, _, _, _ := skewedServer(t, fake)
		mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}) // IdlePolicy absent = attest, unattested

		r := newRetirer(s)
		if r.tick(time.Now()) {
			t.Fatal("unattested tick retired")
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

		attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
		if !r.tick(time.Now()) {
			t.Fatal("attested tick did not retire")
		}
		if resp := healthResp(t, s); resp.RetireDeferredDir != "" || resp.RetireDeferredReason != "" {
			t.Errorf("deferral survived the drain: %+v", resp)
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
		mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
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
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
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

func TestListDomainsHandler(t *testing.T) {
	domains := []DomainInfo{
		{Domain: "cc-pool-acct-01", DisplayName: "acct-01"},
		{Domain: "cc-pool-acct-02"},
	}
	cases := []struct {
		name        string
		source      func(ctx context.Context) ([]DomainInfo, error)
		wantOK      bool
		wantErr     string
		wantDomains []DomainInfo
	}{
		{
			name:    "nil source fails loudly, never an empty list",
			source:  nil,
			wantErr: "no File Provider domain source",
		},
		{
			name:    "source error propagates",
			source:  func(context.Context) ([]DomainInfo, error) { return nil, errors.New("app control socket gone") },
			wantErr: "listdomains: app control socket gone",
		},
		{
			name:   "no registered domains is OK-empty",
			source: func(context.Context) ([]DomainInfo, error) { return nil, nil },
			wantOK: true,
		},
		{
			name:        "registered domains return typed",
			source:      func(context.Context) ([]DomainInfo, error) { return domains, nil },
			wantOK:      true,
			wantDomains: domains,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newHandlerServer(&fakeHost{})
			s.DomainSource = tc.source
			resp := s.dispatch(Request{Op: OpListDomains})
			if resp.OK != tc.wantOK {
				t.Fatalf("OK = %v (%s), want %v", resp.OK, resp.Error, tc.wantOK)
			}
			if resp.ErrClass != "" {
				t.Errorf("ErrClass = %q, want none (plain error, never a mount class)", resp.ErrClass)
			}
			if tc.wantErr != "" && !strings.Contains(resp.Error, tc.wantErr) {
				t.Errorf("Error = %q, want it to contain %q", resp.Error, tc.wantErr)
			}
			if !reflect.DeepEqual(resp.Domains, tc.wantDomains) {
				t.Errorf("Domains = %+v, want %+v", resp.Domains, tc.wantDomains)
			}
		})
	}
}

func TestListDomainsSourceGetsBoundedContext(t *testing.T) {
	s := newHandlerServer(&fakeHost{})
	s.DomainSource = func(ctx context.Context) ([]DomainInfo, error) {
		d, ok := ctx.Deadline()
		if !ok {
			return nil, errors.New("no deadline on the source context")
		}
		if remaining := time.Until(d); remaining > domainSourceTimeout {
			return nil, fmt.Errorf("deadline %s exceeds domainSourceTimeout", remaining)
		}
		return nil, nil
	}
	if resp := s.dispatch(Request{Op: OpListDomains}); !resp.OK {
		t.Fatal(resp.Error)
	}
}

func TestListDomainsOpDeadline(t *testing.T) {
	if got := opDeadline(OpListDomains); got != 15*time.Second {
		t.Fatalf("opDeadline(listdomains) = %s, want 15s", got)
	}
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

func TestListDomainsOverTheWire(t *testing.T) {
	fake := &fakeHost{}
	want := []DomainInfo{{Domain: "cc-pool-acct-01", DisplayName: "acct-01"}}
	s := &Server{
		Socket:       filepath.Join(shortSockDir(t), "m.sock"),
		Host:         fake,
		Version:      testVersion,
		Log:          log.New(io.Discard, "", 0),
		DomainSource: func(context.Context) ([]DomainInfo, error) { return want, nil },
	}
	_, cl, _, _ := runServer(t, s)
	got, err := cl.ListDomains()
	if err != nil {
		t.Fatalf("ListDomains: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ListDomains = %+v, want %+v", got, want)
	}
	st, err := cl.Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Version != testVersion || st.Retiring || !st.ParkedUntil.IsZero() {
		t.Fatalf("Status = %+v, want a quiet holder at %q", st, testVersion)
	}
}

// TestClientListDomainsAgainstOldHolder pins the forward-skew mapping: a
// holder that predates the op answers its unknown-op default arm, which the
// client surfaces as an IsUnknownOp error — never a crash, never a class.
func TestClientListDomainsAgainstOldHolder(t *testing.T) {
	socket, requests := startRawHolder(t, func(string) string {
		return `{"proto":1,"ok":false,"error":"unknown op: listdomains"}`
	})
	_, err := NewClient(socket).ListDomains()
	if err == nil {
		t.Fatal("ListDomains against an old holder succeeded")
	}
	if !IsUnknownOp(err) {
		t.Fatalf("err = %v, want IsUnknownOp to match", err)
	}
	if errors.Is(err, ErrUnknownClass) {
		t.Fatalf("err = %v; a classless unknown-op reply must not read as ErrUnknownClass", err)
	}
	if want := `{"proto":1,"op":"listdomains"}`; requests()[0] != want {
		t.Fatalf("request = %s, want %s (frozen wire artifact)", requests()[0], want)
	}
}

func TestClientStatusDecodesFields(t *testing.T) {
	socket, requests := startRawHolder(t, func(string) string {
		return `{"proto":1,"ok":true,"version":"v1.2.3","retiring":true,"parked_until":1765500000,"journal_mounts":2,"journal_bridges":1,"retire_strikes":[1765490000,1765499000],"retire_deferred_dir":"/m/a","retire_deferred_reason":"installed bundle is v1.2.4"}`
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
	if want := `{"proto":1,"op":"health"}`; requests()[0] != want {
		t.Fatalf("request = %s, want %s (frozen wire artifact)", requests()[0], want)
	}
}

// TestClientStatusOldHolderZeros pins backward skew: a holder that predates
// the status fields answers version-only, which decodes as all-zeros.
func TestClientStatusOldHolderZeros(t *testing.T) {
	socket, _ := startRawHolder(t, func(string) string {
		return `{"proto":1,"ok":true,"version":"v0.37.0"}`
	})
	st, err := NewClient(socket).Status()
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Version != "v0.37.0" || st.Retiring || !st.ParkedUntil.IsZero() || st.JournalMounts != 0 || st.JournalBridges != 0 || len(st.RetireStrikes) != 0 || st.RetireDeferredDir != "" || st.RetireDeferredReason != "" {
		t.Fatalf("Status = %+v, want version-only zeros from an old holder", st)
	}
}
