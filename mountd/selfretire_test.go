package mountd

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
)

func mountForTest(t *testing.T, s *Server, spec fusekit.MountSpec) {
	t.Helper()
	if resp := s.dispatch(mountEntryOf(spec).mountRequest()); !resp.OK {
		t.Fatalf("mount %s: %s", spec.Dir, resp.Error)
	}
}

func attestForTest(t *testing.T, s *Server, owner string, dirs []string, ttl time.Duration) {
	t.Helper()
	if resp := s.dispatch(Request{Op: OpAttestIdle, Owner: owner, Dirs: dirs, TTL: ttl}); !resp.OK {
		t.Fatalf("attestidle: %s", resp.Error)
	}
}

// skewedServer arms a journaled handler server with a controllable skew
// verdict and a shutdown recorder.
func skewedServer(t *testing.T, fake *fakeHost) (s *Server, journalPath string, skewed *bool, shutdowns *int) {
	t.Helper()
	s, journalPath = newJournaledHandlerServer(t, fake)
	sk, sd := true, 0
	s.RetireSkew = func() (bool, string, error) { return sk, "installed bundle is v9.9.10, this holder is v9.9.9", nil }
	s.triggerShutdown = func() { sd++ }
	return s, journalPath, &sk, &sd
}

func strikeFilePath(s *Server) string {
	return filepath.Join(filepath.Dir(s.journal.path), "holder-retires.json")
}

func strikeCount(t *testing.T, s *Server) int {
	t.Helper()
	times, err := loadStrikeTimes(strikeFilePath(s))
	if err != nil {
		t.Fatalf("load strikes: %v", err)
	}
	return len(times)
}

func TestAttestIdleHandler(t *testing.T) {
	newServer := func(t *testing.T) *Server {
		s, _ := newJournaledHandlerServer(t, &fakeHost{})
		mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
		return s
	}
	cases := []struct {
		name      string
		req       Request
		wantClass string
		wantErr   string // substring of Error when no class applies
	}{
		{name: "traversal owner", req: Request{Op: OpAttestIdle, Owner: "x/../y", Dirs: []string{"/m/a"}, TTL: time.Minute}, wantClass: ClassInvalidOwner},
		{name: "empty owner", req: Request{Op: OpAttestIdle, Dirs: []string{"/m/a"}, TTL: time.Minute}, wantClass: ClassInvalidOwner},
		{name: "no dirs", req: Request{Op: OpAttestIdle, Owner: "cc-pool", TTL: time.Minute}, wantErr: "dirs are required"},
		{name: "zero ttl", req: Request{Op: OpAttestIdle, Owner: "cc-pool", Dirs: []string{"/m/a"}}, wantErr: "ttl"},
		{name: "negative ttl", req: Request{Op: OpAttestIdle, Owner: "cc-pool", Dirs: []string{"/m/a"}, TTL: -time.Second}, wantErr: "ttl"},
		{name: "ttl over cap", req: Request{Op: OpAttestIdle, Owner: "cc-pool", Dirs: []string{"/m/a"}, TTL: MaxAttestTTL + time.Second}, wantErr: "ttl"},
		{name: "relative dir", req: Request{Op: OpAttestIdle, Owner: "cc-pool", Dirs: []string{"m/a"}, TTL: time.Minute}, wantErr: "must be absolute"},
		{name: "foreign-owned dir", req: Request{Op: OpAttestIdle, Owner: "other", Dirs: []string{"/m/a"}, TTL: time.Minute}, wantClass: ClassForeignMount},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newServer(t)
			resp := s.dispatch(tc.req)
			if resp.OK {
				t.Fatalf("attestidle succeeded, want refusal")
			}
			if resp.ErrClass != tc.wantClass {
				t.Fatalf("class = %q, want %q (error: %s)", resp.ErrClass, tc.wantClass, resp.Error)
			}
			if tc.wantErr != "" && !strings.Contains(resp.Error, tc.wantErr) {
				t.Fatalf("error = %q, want substring %q", resp.Error, tc.wantErr)
			}
			// A refused attest never records freshness.
			if s.attestFresh("/m/a", tc.req.Owner, time.Now()) {
				t.Fatal("refused attest recorded an attestation")
			}
		})
	}

	t.Run("records and expires", func(t *testing.T) {
		s := newServer(t)
		attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
		now := time.Now()
		if !s.attestFresh("/m/a", "cc-pool", now) {
			t.Fatal("fresh attest not recorded")
		}
		if s.attestFresh("/m/a", "other", now) {
			t.Fatal("attest matched a different owner")
		}
		if s.attestFresh("/m/a", "cc-pool", now.Add(2*time.Minute)) {
			t.Fatal("attest survived its TTL")
		}
		if s.attestFresh("/m/other", "cc-pool", now) {
			t.Fatal("unattested dir read fresh")
		}
	})

	t.Run("unregistered dir is recorded but gate re-matches owner", func(t *testing.T) {
		s := newServer(t)
		attestForTest(t, s, "other", []string{"/m/free"}, time.Minute)
		now := time.Now()
		if !s.attestFresh("/m/free", "other", now) {
			t.Fatal("unregistered-dir attest not recorded")
		}
		// A pre-mount attest by a foreign owner can never satisfy the gate
		// for the eventual owner's mount.
		if s.attestFresh("/m/free", "cc-pool", now) {
			t.Fatal("foreign pre-mount attest satisfied the owner gate")
		}
	})
}

// TestAttestClearedOnRegistration pins Fix B: every mount (re)registration
// invalidates a pre-existing attestation for its dir — a pre-mount "idle"
// verdict must never gate a drain of a mount created after it.
func TestAttestClearedOnRegistration(t *testing.T) {
	spec := fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}
	cases := []struct {
		name string
		run  func(t *testing.T, s *Server)
		want bool // attestFresh("/m/a", "cc-pool") afterward
	}{
		{name: "pre-mount attest is cleared by the mount", run: func(t *testing.T, s *Server) {
			attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
			mountForTest(t, s, spec)
		}, want: false},
		{name: "unmount plus remount clears an attested mount", run: func(t *testing.T, s *Server) {
			mountForTest(t, s, spec)
			attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
			if resp := s.dispatch(Request{Op: OpUnmount, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}); !resp.OK {
				t.Fatalf("unmount: %s", resp.Error)
			}
			mountForTest(t, s, spec)
		}, want: false},
		{name: "attest after mount stays fresh", run: func(t *testing.T, s *Server) {
			mountForTest(t, s, spec)
			attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
		}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newJournaledHandlerServer(t, &fakeHost{})
			tc.run(t, s)
			if got := s.attestFresh("/m/a", "cc-pool", time.Now()); got != tc.want {
				t.Fatalf("attestFresh after %s = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestRevokeIdleHandler(t *testing.T) {
	refusals := []struct {
		name      string
		req       Request
		wantClass string
		wantErr   string
	}{
		{name: "traversal owner", req: Request{Op: OpRevokeIdle, Owner: "x/../y", Dirs: []string{"/m/a"}}, wantClass: ClassInvalidOwner},
		{name: "empty owner", req: Request{Op: OpRevokeIdle, Dirs: []string{"/m/a"}}, wantClass: ClassInvalidOwner},
		{name: "no dirs", req: Request{Op: OpRevokeIdle, Owner: "cc-pool"}, wantErr: "dirs are required"},
		{name: "relative dir", req: Request{Op: OpRevokeIdle, Owner: "cc-pool", Dirs: []string{"m/a"}}, wantErr: "must be absolute"},
	}
	for _, tc := range refusals {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newJournaledHandlerServer(t, &fakeHost{})
			mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
			attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
			resp := s.dispatch(tc.req)
			if resp.OK {
				t.Fatal("revokeidle succeeded, want refusal")
			}
			if resp.ErrClass != tc.wantClass {
				t.Fatalf("class = %q, want %q (error: %s)", resp.ErrClass, tc.wantClass, resp.Error)
			}
			if tc.wantErr != "" && !strings.Contains(resp.Error, tc.wantErr) {
				t.Fatalf("error = %q, want substring %q", resp.Error, tc.wantErr)
			}
			// A refused revoke must not have touched the attestation.
			if !s.attestFresh("/m/a", "cc-pool", time.Now()) {
				t.Fatal("refused revoke cleared the attestation")
			}
		})
	}

	behaviors := []struct {
		name string
		run  func(t *testing.T, s *Server)
		want bool // attestFresh("/m/a", "cc-pool") afterward
	}{
		{name: "revoke clears freshness synchronously", run: func(t *testing.T, s *Server) {
			attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
			if resp := s.dispatch(Request{Op: OpRevokeIdle, Owner: "cc-pool", Dirs: []string{"/m/a"}}); !resp.OK {
				t.Fatalf("revoke: %s", resp.Error)
			}
		}, want: false},
		{name: "a later attest re-establishes freshness", run: func(t *testing.T, s *Server) {
			attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
			if resp := s.dispatch(Request{Op: OpRevokeIdle, Owner: "cc-pool", Dirs: []string{"/m/a"}}); !resp.OK {
				t.Fatalf("revoke: %s", resp.Error)
			}
			attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
		}, want: true},
		{name: "revoke without an attestation is an idempotent no-op", run: func(t *testing.T, s *Server) {
			if resp := s.dispatch(Request{Op: OpRevokeIdle, Owner: "cc-pool", Dirs: []string{"/m/a"}}); !resp.OK {
				t.Fatalf("revoke: %s", resp.Error)
			}
		}, want: false},
	}
	for _, tc := range behaviors {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := newJournaledHandlerServer(t, &fakeHost{})
			mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
			tc.run(t, s)
			if got := s.attestFresh("/m/a", "cc-pool", time.Now()); got != tc.want {
				t.Fatalf("attestFresh = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("revoke removes only the owner's own attestation", func(t *testing.T) {
		s, _ := newJournaledHandlerServer(t, &fakeHost{})
		attestForTest(t, s, "other", []string{"/m/free"}, time.Minute)
		if resp := s.dispatch(Request{Op: OpRevokeIdle, Owner: "cc-pool", Dirs: []string{"/m/free"}}); !resp.OK {
			t.Fatalf("revoke: %s", resp.Error)
		}
		if !s.attestFresh("/m/free", "other", time.Now()) {
			t.Fatal("a foreign owner's revoke removed another consumer's attestation")
		}
	})
}

// TestRevokeIdleBlocksRetire pins the select path's contract: a revoke landed
// before the tick means the drain defers, and a fresh re-attest lets it retire.
func TestRevokeIdleBlocksRetire(t *testing.T) {
	fake := &fakeHost{}
	s, _, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
	if resp := s.dispatch(Request{Op: OpRevokeIdle, Owner: "cc-pool", Dirs: []string{"/m/a"}}); !resp.OK {
		t.Fatalf("revoke: %s", resp.Error)
	}

	r := newRetirer(s)
	if r.tick(time.Now()) {
		t.Fatal("tick retired a revoked mount")
	}
	if _, teardowns := fake.calls(); len(teardowns) != 0 {
		t.Fatalf("revoked mount was drained: %v", teardowns)
	}
	if *shutdowns != 0 {
		t.Fatal("shutdown triggered after a revoke")
	}

	attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)
	if !r.tick(time.Now()) {
		t.Fatal("tick did not retire after a fresh re-attest")
	}
	if *shutdowns != 1 {
		t.Fatalf("shutdowns = %d, want 1", *shutdowns)
	}
}

func TestAttestIdleOpDeadline(t *testing.T) {
	for _, op := range []Op{OpAttestIdle, OpRevokeIdle} {
		if got := opDeadline(op); got != 5*time.Second {
			t.Fatalf("opDeadline(%s) = %s, want 5s", op, got)
		}
	}
}

func TestMountRejectsUnknownIdlePolicy(t *testing.T) {
	s := newHandlerServer(&fakeHost{})
	resp := s.dispatch(Request{Op: OpMount, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: "bogus"})
	if resp.OK || !strings.Contains(resp.Error, "idle_policy") {
		t.Fatalf("bogus idle_policy = (ok=%v, %q), want refusal naming idle_policy", resp.OK, resp.Error)
	}
}

func TestRetiringGateBouncesOnlyNewWork(t *testing.T) {
	s := newHandlerServer(&fakeHost{})
	s.retiring.Store(true)

	for _, op := range []Op{OpMount, OpAddBridge} {
		req := Request{Op: op, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", BridgeSocket: "/g/b.sock", ContentSocket: "/u/c.sock"}
		resp := s.dispatch(req)
		if resp.OK || resp.ErrClass != ClassBusy {
			t.Fatalf("%s while retiring = (ok=%v class=%q), want retryable busy", op, resp.OK, resp.ErrClass)
		}
	}
	// Ops that help the drain still serve.
	if resp := s.dispatch(Request{Op: OpUnmount, Base: "/b/a", Dir: "/m/a"}); !resp.OK {
		t.Fatalf("unmount while retiring: %s", resp.Error)
	}
	if resp := s.dispatch(Request{Op: OpAttestIdle, Owner: "cc-pool", Dirs: []string{"/m/a"}, TTL: time.Minute}); !resp.OK {
		t.Fatalf("attestidle while retiring: %s", resp.Error)
	}
	// A revoke ABORTS a drain, so it must serve while retiring.
	if resp := s.dispatch(Request{Op: OpRevokeIdle, Owner: "cc-pool", Dirs: []string{"/m/a"}}); !resp.OK {
		t.Fatalf("revokeidle while retiring: %s", resp.Error)
	}
	if resp := s.dispatch(Request{Op: OpHealth}); !resp.OK {
		t.Fatal("health while retiring")
	}
}

func TestRetireDefersFailClosedWithoutAttest(t *testing.T) {
	fake := &fakeHost{}
	s, path, _, shutdowns := skewedServer(t, fake)
	// Absent IdlePolicy means "attest" — fail-closed.
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})

	r := newRetirer(s)
	// Two deferring ticks: the second pins the lastDefer dedup path and that
	// deferral never accumulates state toward a sweep.
	for i := range 2 {
		if r.tick(time.Now()) {
			t.Fatalf("tick %d retired an unattested mount", i)
		}
	}
	if _, teardowns := fake.calls(); len(teardowns) != 0 {
		t.Fatalf("unattested defer tore down %v", teardowns)
	}
	// Deferred is NOT retiring: the holder serves while it waits for
	// idleness, so a busy consumer can't wedge it into bouncing all work.
	if s.retiring.Load() {
		t.Fatal("deferral left the holder retiring; new work would bounce busy")
	}
	if resp := s.dispatch(Request{Op: OpMount, Base: "/b/n", Dir: "/m/n", Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount during deferral: %s", resp.Error)
	}
	if *shutdowns != 0 {
		t.Fatal("shutdown triggered on a deferred retire")
	}
	// A quiet defer is not a retire attempt: no strike recorded, on disk or in memory.
	if _, err := os.Stat(strikeFilePath(s)); !os.IsNotExist(err) {
		t.Fatalf("deferred tick persisted a strike: stat err = %v", err)
	}
	if times := s.strikes.Times(); len(times) != 0 {
		t.Fatalf("deferred ticks struck in memory: %v", times)
	}
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/a", "/m/n"}) {
		t.Fatalf("journal = %v, want [/m/a /m/n]", dirs)
	}
}

func TestRetireCleanSweepExitsAndKeepsJournal(t *testing.T) {
	fake := &fakeHost{}
	s, path, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/p", Dir: "/m/p", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Minute)

	r := newRetirer(s)
	if !r.tick(time.Now()) {
		t.Fatal("tick did not retire with a fresh attest")
	}
	if *shutdowns != 1 {
		t.Fatalf("shutdown triggered %d times, want 1", *shutdowns)
	}
	_, teardowns := fake.calls()
	if want := []hostCall{{"/b/a", "/m/a"}, {"/b/p", "/m/p"}}; !reflect.DeepEqual(teardowns, want) {
		t.Fatalf("teardowns = %v, want %v", teardowns, want)
	}
	if reg := s.snapshotRegistry(); len(reg) != 0 {
		t.Fatalf("registry after clean sweep = %v, want empty", reg)
	}
	// The journal SURVIVES a self-retire — the successor replays it.
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/a", "/m/p"}) {
		t.Fatalf("journal after retire = %v, want both mounts", dirs)
	}
	if got := strikeCount(t, s); got != 1 {
		t.Fatalf("strike history = %d entries, want 1", got)
	}
	if !s.retiring.Load() {
		t.Fatal("retiring cleared before exit; a late mount could land")
	}
}

func TestRetireBusyClaimAbortsAndRemountsPrefix(t *testing.T) {
	fake := &fakeHost{}
	s, path, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	release, ok := s.claim("/m/b")
	if !ok {
		t.Fatal("claim /m/b")
	}
	defer release()

	r := newRetirer(s)
	if r.tick(time.Now()) {
		t.Fatal("tick retired past a busy mount")
	}
	setups, teardowns := fake.calls()
	if want := []hostCall{{"/b/a", "/m/a"}}; !reflect.DeepEqual(teardowns, want) {
		t.Fatalf("teardowns = %v, want only the swept prefix %v", teardowns, want)
	}
	// Initial two mounts plus the abort's remount of the swept prefix.
	if want := []hostCall{{"/b/a", "/m/a"}, {"/b/b", "/m/b"}, {"/b/a", "/m/a"}}; !reflect.DeepEqual(setups, want) {
		t.Fatalf("setups = %v, want %v (remounted prefix)", setups, want)
	}
	if bases := registryBases(s); !reflect.DeepEqual(bases, map[string]string{"/m/a": "/b/a", "/m/b": "/b/b"}) {
		t.Fatalf("registry after abort = %v, want both rows back", bases)
	}
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/a", "/m/b"}) {
		t.Fatalf("journal after abort = %v, want both", dirs)
	}
	if *shutdowns != 0 {
		t.Fatal("shutdown triggered on an aborted sweep")
	}
	// An aborted sweep IS a retire attempt: one strike.
	if got := strikeCount(t, s); got != 1 {
		t.Fatalf("strike history = %d entries, want 1", got)
	}
}

// TestRetireAbortedSweepServesNormally pins that an aborted sweep is not a
// drain: the retiring flag clears, so new mounts land between attempts
// instead of bouncing ClassBusy until the storm breaker parks. (A SUCCESSFUL
// sweep keeps retiring set through the exit —
// TestRetireCleanSweepExitsAndKeepsJournal.)
func TestRetireAbortedSweepServesNormally(t *testing.T) {
	fake := &fakeHost{}
	s, _, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	release, ok := s.claim("/m/b") // aborts the sweep mid-drain
	if !ok {
		t.Fatal("claim /m/b")
	}
	defer release()

	r := newRetirer(s)
	if r.tick(time.Now()) {
		t.Fatal("tick retired past a busy mount")
	}
	if s.retiring.Load() {
		t.Fatal("aborted sweep left the holder retiring; new work would bounce busy between ticks")
	}
	if resp := s.dispatch(Request{Op: OpMount, Base: "/b/n", Dir: "/m/n", Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount after an aborted sweep bounced: %s", resp.Error)
	}
	if *shutdowns != 0 {
		t.Fatal("shutdown triggered on an aborted sweep")
	}
}

func TestRetireWedgedTeardownAbortsKeepsRow(t *testing.T) {
	fake := &fakeHost{}
	s, _, _, _ := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	fake.mu.Lock()
	fake.teardownFn = func(base, dir string) error {
		if dir == "/m/b" {
			return fusekit.ErrUnmountWedged
		}
		return nil
	}
	fake.mu.Unlock()

	r := newRetirer(s)
	if r.tick(time.Now()) {
		t.Fatal("tick retired past a wedged unmount")
	}
	// The graceful-only invariant: the wedged (EBUSY) mount keeps its row —
	// it is still serving a consumer — and the swept prefix is remounted.
	if _, ok := s.registered("/m/b"); !ok {
		t.Fatal("wedged mount lost its registry row")
	}
	setups, _ := fake.calls()
	if want := []hostCall{{"/b/a", "/m/a"}, {"/b/b", "/m/b"}, {"/b/a", "/m/a"}}; !reflect.DeepEqual(setups, want) {
		t.Fatalf("setups = %v, want remounted prefix %v", setups, want)
	}
}

// TestRetireWedgedMuxTenantReattachesIntoSurvivingRoot pins the mux half of
// the wedged-sweep contract: the only mux-tenant teardown error source is the
// last-child native-root unmount, AFTER the tenant detached — so its row is a
// lie and must go, and the abort re-attaches the tenant into the surviving
// root (the provider still holds it: HoldsMuxRoot).
func TestRetireWedgedMuxTenantReattachesIntoSurvivingRoot(t *testing.T) {
	fake := &fakeHost{muxRootsHeld: map[string]bool{"/mux": true}}
	s, path, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/t1", Dir: "/mux/t1", Owner: "cc-pool", MuxRoot: "/mux", IdlePolicy: fusekit.IdlePolicyProbe})
	fake.mu.Lock()
	fake.teardownFn = func(_, dir string) error {
		if dir == "/mux/t1" {
			return fusekit.ErrUnmountWedged // last-child root unmount wedged
		}
		return nil
	}
	fake.mu.Unlock()

	r := newRetirer(s)
	if r.tick(time.Now()) {
		t.Fatal("tick retired past a wedged mux-root unmount")
	}
	if *shutdowns != 0 {
		t.Fatal("shutdown triggered on an aborted sweep")
	}
	// Re-attached, not orphaned: a second Setup ran and the row is back with
	// its MuxRoot — never a lying row for a detached tenant.
	setups, _ := fake.calls()
	if want := []hostCall{{"/b/t1", "/mux/t1"}, {"/b/t1", "/mux/t1"}}; !reflect.DeepEqual(setups, want) {
		t.Fatalf("setups = %v, want the abort's re-attach %v", setups, want)
	}
	if mux := registryMux(s); !reflect.DeepEqual(mux, map[string]string{"/mux/t1": "/mux"}) {
		t.Fatalf("registry after abort = %v, want the tenant re-attached", mux)
	}
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/mux/t1"}) {
		t.Fatalf("journal = %v, want intact", dirs)
	}
	// An aborted sweep is still a retire attempt: one strike.
	if got := strikeCount(t, s); got != 1 {
		t.Fatalf("strike history = %d entries, want 1", got)
	}
}

// TestRetireStrikePersistFailureDefersSweep pins the breaker-integrity rule:
// a generation that cannot RECORD its retire attempt never sweeps (else a
// broken install kill-cycles successors past the cross-generation breaker);
// the in-memory strikes still park it by the third tick.
func TestRetireStrikePersistFailureDefersSweep(t *testing.T) {
	fake := &fakeHost{}
	s, path, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})

	r := newRetirer(s)
	// A FILE where the state dir should be fails every AtomicWrite.
	blocker := filepath.Join(t.TempDir(), "blocker")
	if err := os.WriteFile(blocker, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	r.strikesPath = filepath.Join(blocker, "holder-retires.json")

	base := time.Now()
	for i, at := range []time.Duration{0, time.Minute} {
		if r.tick(base.Add(at)) {
			t.Fatalf("tick %d retired without a persisted strike", i)
		}
		if _, teardowns := fake.calls(); len(teardowns) != 0 {
			t.Fatalf("tick %d swept despite the persist failure: %v", i, teardowns)
		}
		if s.retiring.Load() {
			t.Fatalf("tick %d left the holder retiring", i)
		}
	}
	// Third tick: the in-memory window parks — bounded spam, still no sweep.
	if r.tick(base.Add(2 * time.Minute)) {
		t.Fatal("breaker tick retired")
	}
	if want := base.Add(2 * time.Minute).Add(retireParkLadder[0]); !s.parkedUntil.Equal(want) {
		t.Fatalf("parkedUntil = %v, want %v (in-memory strikes must still park)", s.parkedUntil, want)
	}
	if _, teardowns := fake.calls(); len(teardowns) != 0 {
		t.Fatalf("parked tick swept: %v", teardowns)
	}
	if s.retiring.Load() {
		t.Fatal("parked holder still bounces new work")
	}
	if *shutdowns != 0 {
		t.Fatal("persist-failing generation exited")
	}
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/a"}) {
		t.Fatalf("journal = %v, want intact", dirs)
	}
}

func TestRetireAttestExpiryMidSweepAborts(t *testing.T) {
	fake := &fakeHost{}
	s, _, _, _ := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	gateNow := time.Now()
	// 1ns TTL: fresh at the gate's (earlier) now, expired by the sweep's
	// own re-check — the sweep must abort before any teardown.
	attestForTest(t, s, "cc-pool", []string{"/m/a"}, time.Nanosecond)

	r := newRetirer(s)
	if r.tick(gateNow) {
		t.Fatal("tick retired on an attest that expired mid-sweep")
	}
	if _, teardowns := fake.calls(); len(teardowns) != 0 {
		t.Fatalf("mid-sweep expiry still tore down %v", teardowns)
	}
	if _, ok := s.registered("/m/a"); !ok {
		t.Fatal("mount lost its row on an aborted sweep")
	}
}

func TestRetireStormBreakerParksLoudly(t *testing.T) {
	fake := &fakeHost{}
	s, _, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	release, ok := s.claim("/m/b") // every sweep aborts busy
	if !ok {
		t.Fatal("claim /m/b")
	}
	defer release()

	base := time.Now()
	r := newRetirer(s)
	for i, at := range []time.Duration{0, time.Minute} {
		if r.tick(base.Add(at)) {
			t.Fatalf("tick %d retired", i)
		}
	}
	if got := strikeCount(t, s); got != 2 {
		t.Fatalf("strikes after two aborts = %d, want 2", got)
	}
	teardownsBefore := func() int { _, tds := fake.calls(); return len(tds) }()

	// Third attempt inside the window trips the breaker: parked, NO sweep,
	// retiring cleared so the holder serves normally, and no exit.
	if r.tick(base.Add(2 * time.Minute)) {
		t.Fatal("breaker tick retired")
	}
	if got := strikeCount(t, s); got != 3 {
		t.Fatalf("strikes after breaker = %d, want 3", got)
	}
	if _, tds := fake.calls(); len(tds) != teardownsBefore {
		t.Fatalf("parked tick still swept: teardowns %d -> %d", teardownsBefore, len(tds))
	}
	if s.retiring.Load() {
		t.Fatal("parked holder still bounces new work")
	}
	if *shutdowns != 0 {
		t.Fatal("breaker exited instead of parking")
	}
	if want := base.Add(2 * time.Minute).Add(retireParkLadder[0]); !s.parkedUntil.Equal(want) {
		t.Fatalf("parkedUntil = %v, want %v (first ladder step)", s.parkedUntil, want)
	}
	// While parked, ticks are inert — not even a skew evaluation's strike.
	if r.tick(base.Add(3 * time.Minute)) {
		t.Fatal("parked tick retired")
	}
	if got := strikeCount(t, s); got != 3 {
		t.Fatalf("parked tick recorded a strike: %d", got)
	}
}

func TestRetireStrikeHistoryPersistsAcrossGenerations(t *testing.T) {
	fake := &fakeHost{}
	s, path, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})

	// Two prior generations retired within the window; this generation's
	// attempt is the third — it must park, not kill-cycle.
	now := time.Now()
	hist, err := json.Marshal([]time.Time{now.Add(-2 * time.Minute), now.Add(-time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(strikeFilePath(s), hist, 0o600); err != nil {
		t.Fatal(err)
	}

	r := newRetirer(s)
	if r.tick(now) {
		t.Fatal("third-generation tick retired instead of parking")
	}
	if _, teardowns := fake.calls(); len(teardowns) != 0 {
		t.Fatalf("parked generation swept: %v", teardowns)
	}
	if *shutdowns != 0 {
		t.Fatal("parked generation exited")
	}
	if got := strikeCount(t, s); got != 3 {
		t.Fatalf("persisted strikes = %d, want 3", got)
	}
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/a"}) {
		t.Fatalf("journal = %v, want intact", dirs)
	}

	// Corrupt history is loud but never blocks retirement forever.
	if err := os.WriteFile(strikeFilePath(s), []byte("{nope"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadStrikeTimes(strikeFilePath(s)); err == nil {
		t.Fatal("corrupt strike history loaded without error")
	}
	newRetirer(s) // logs and starts fresh
	attestGone := time.Now()
	if s.strikes.Struck(attestGone) {
		t.Fatal("corrupt history restored as struck")
	}
}

func TestRetireSkewTransitions(t *testing.T) {
	fake := &fakeHost{}
	s, _, skewed, _ := skewedServer(t, fake)
	// A gate-passing tick whose sweep aborts busy: probe-policy mounts (no
	// attest needed) with one claim held.
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe})
	release, ok := s.claim("/m/b")
	if !ok {
		t.Fatal("claim /m/b")
	}
	defer release()

	r := newRetirer(s)
	if r.tick(time.Now()) {
		t.Fatal("aborted-sweep tick retired")
	}
	if s.retiring.Load() {
		t.Fatal("aborted sweep left the holder retiring")
	}
	// A drain in flight when the skew clears (an install rolled back
	// mid-sweep) also returns to serving normally.
	s.retiring.Store(true)
	*skewed = false
	if r.tick(time.Now()) {
		t.Fatal("unskewed tick retired")
	}
	if s.retiring.Load() {
		t.Fatal("cleared skew left the holder retiring")
	}
	if resp := s.dispatch(Request{Op: OpMount, Base: "/b/x", Dir: "/m/x", Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount after skew cleared: %s", resp.Error)
	}
}

func TestRetireSkewCheckErrorIsInertAndDeduped(t *testing.T) {
	fake := &fakeHost{}
	s, _ := newJournaledHandlerServer(t, fake)
	calls := 0
	s.RetireSkew = func() (bool, string, error) { calls++; return false, "", errors.New("boom") }
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})

	r := newRetirer(s)
	for i := range 3 {
		if r.tick(time.Now()) {
			t.Fatalf("erroring tick %d retired", i)
		}
	}
	if calls != 3 {
		t.Fatalf("skew check ran %d times, want 3", calls)
	}
	if s.retiring.Load() {
		t.Fatal("erroring skew check set retiring")
	}
	if _, teardowns := fake.calls(); len(teardowns) != 0 {
		t.Fatalf("erroring skew check swept: %v", teardowns)
	}
}

func TestAttestIdleAndIdlePolicyOverTheWire(t *testing.T) {
	fake := &fakeHost{}
	sockDir := shortSockDir(t)
	s, _, _, _ := runServer(t, &Server{Socket: filepath.Join(sockDir, "holder.sock"), Host: fake, Version: testVersion, Log: log.New(io.Discard, "", 0)})

	cl := &Client{Socket: s.Socket, Owner: "cc-pool"}
	spec := fusekit.MountSpec{Base: "/b/w", Dir: "/m/w", Owner: "cc-pool", IdlePolicy: fusekit.IdlePolicyProbe}
	if err := cl.AddMount(spec); err != nil {
		t.Fatalf("AddMount: %v", err)
	}
	specs := fake.capturedSpecs()
	if len(specs) != 1 || specs[0].IdlePolicy != fusekit.IdlePolicyProbe {
		t.Fatalf("IdlePolicy did not survive the wire: %+v", specs)
	}
	if err := cl.AttestIdle([]string{"/m/w"}, time.Minute); err != nil {
		t.Fatalf("AttestIdle: %v", err)
	}
	if !s.attestFresh("/m/w", "cc-pool", time.Now()) {
		t.Fatal("wire attest not recorded")
	}

	bad := &Client{Socket: s.Socket, Owner: "x/../y"}
	if err := bad.AttestIdle([]string{"/m/w"}, time.Minute); !errors.Is(err, ErrInvalidOwner) {
		t.Fatalf("traversal owner error = %v, want ErrInvalidOwner", err)
	}
	foreign := &Client{Socket: s.Socket, Owner: "other"}
	if err := foreign.AttestIdle([]string{"/m/w"}, time.Minute); !errors.Is(err, ErrForeignMount) {
		t.Fatalf("foreign attest error = %v, want ErrForeignMount", err)
	}

	h := &RemoteHost{Socket: s.Socket, Owner: "cc-pool"}
	if err := h.AttestIdle([]string{"/m/w"}, time.Minute); err != nil {
		t.Fatalf("RemoteHost.AttestIdle: %v", err)
	}
	// An unreachable holder is a no-op: nothing to retire.
	gone := &RemoteHost{Socket: filepath.Join(sockDir, "nope.sock"), Owner: "cc-pool"}
	if err := gone.AttestIdle([]string{"/m/w"}, time.Minute); err != nil {
		t.Fatalf("AttestIdle against a dead holder = %v, want nil", err)
	}
}

const releasePlist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>CFBundleIdentifier</key><string>com.yasyf.fusekit-holder</string>
  <key>CFBundleName</key><string>fusekit-holder</string>
  <key>CFBundleShortVersionString</key><string>0.38.0</string>
  <key>CFBundleVersion</key><string>123</string>
  <key>LSBackgroundOnly</key><true/>
</dict></plist>
`

func writePlist(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "Info.plist")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadBundleShortVersion(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string
		wantErr bool
	}{
		{name: "release format", content: releasePlist, want: "0.38.0"},
		{name: "key after other strings", content: `<plist><dict><key>A</key><string>x</string><key>CFBundleShortVersionString</key><string>1.2.3</string></dict></plist>`, want: "1.2.3"},
		{name: "missing key", content: `<plist><dict><key>A</key><string>x</string></dict></plist>`, wantErr: true},
		{name: "non-string value after key", content: `<plist><dict><key>CFBundleShortVersionString</key><true/><key>B</key><string>x</string></dict></plist>`, wantErr: true},
		{name: "binary junk", content: "bplist00\x00\x01\x02", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := readBundleShortVersion(writePlist(t, tc.content))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got %q, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("version = %q, want %q", got, tc.want)
			}
		})
	}
	if _, err := readBundleShortVersion(filepath.Join(t.TempDir(), "missing.plist")); err == nil {
		t.Fatal("missing plist read without error")
	}
}

func TestPlistSkew(t *testing.T) {
	path := writePlist(t, releasePlist) // installed 0.38.0
	cases := []struct {
		name     string
		compiled string
		want     bool
	}{
		{name: "same version v-prefixed", compiled: "v0.38.0", want: false},
		{name: "same version bare", compiled: "0.38.0", want: false},
		{name: "older build", compiled: "v0.37.0", want: true},
		{name: "dev build never skews", compiled: "dev", want: false},
		{name: "empty never skews", compiled: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			skewed, reason, err := plistSkew(tc.compiled, path)()
			if err != nil {
				t.Fatal(err)
			}
			if skewed != tc.want {
				t.Fatalf("skewed = %v, want %v", skewed, tc.want)
			}
			if skewed && (!strings.Contains(reason, "0.38.0") || !strings.Contains(reason, "0.37.0")) {
				t.Fatalf("reason %q does not name both versions", reason)
			}
		})
	}
	// A dev build must not even need the plist.
	if skewed, _, err := plistSkew("dev", filepath.Join(t.TempDir(), "missing.plist"))(); skewed || err != nil {
		t.Fatalf("dev against a missing plist = (%v, %v), want inert", skewed, err)
	}
	// An unreadable plist fails safe: an error, never a retire.
	if skewed, _, err := plistSkew("v0.38.0", filepath.Join(t.TempDir(), "missing.plist"))(); skewed || err == nil {
		t.Fatalf("missing plist = (%v, %v), want (false, error)", skewed, err)
	}
}

func TestExeHashSkew(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "holder")
	if err := os.WriteFile(exe, []byte("generation-one"), 0o700); err != nil {
		t.Fatal(err)
	}
	check := exeHashSkew(exe)
	if skewed, _, err := check(); skewed || err != nil {
		t.Fatalf("unchanged binary = (%v, %v), want (false, nil)", skewed, err)
	}
	if err := os.WriteFile(exe, []byte("generation-two"), 0o700); err != nil {
		t.Fatal(err)
	}
	skewed, reason, err := check()
	if err != nil {
		t.Fatal(err)
	}
	if !skewed || !strings.Contains(reason, exe) {
		t.Fatalf("replaced binary = (%v, %q), want skew naming the path", skewed, reason)
	}

	// A missing baseline fails safe on every call.
	gone := exeHashSkew(filepath.Join(t.TempDir(), "missing"))
	if skewed, _, err := gone(); skewed || err == nil {
		t.Fatalf("missing baseline = (%v, %v), want (false, error)", skewed, err)
	}
}
