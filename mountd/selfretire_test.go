package mountd

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/lease"
)

func mountForTest(t *testing.T, s *Server, spec fusekit.MountSpec) {
	t.Helper()
	if resp := s.dispatch(mountEntryOf(spec).mountRequest()); !resp.OK {
		t.Fatalf("mount %s: %s", spec.Dir, resp.Error)
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

func TestRetiringGateBouncesOnlyNewWork(t *testing.T) {
	s := newHandlerServer(t, &fakeHost{})
	s.retiring.Store(true)

	for _, op := range []Op{OpMount, OpAddBridge} {
		req := Request{Op: op, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", BridgeSocket: "/g/b.sock", ContentSocket: "/u/c.sock"}
		resp := s.dispatch(req)
		if resp.OK || resp.ErrClass != ClassBusy {
			t.Fatalf("%s while retiring = (ok=%v class=%q), want retryable busy", op, resp.OK, resp.ErrClass)
		}
	}
	// Ops that help the drain still serve.
	if resp := s.dispatch(Request{Op: OpUnmount, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("unmount while retiring: %s", resp.Error)
	}
	if resp := s.dispatch(Request{Op: OpHealth}); !resp.OK {
		t.Fatal("health while retiring")
	}
}

func TestRetireDefersOnHeldLease(t *testing.T) {
	fake := &fakeHost{}
	s, path, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	h, err := lease.Acquire(s.LeaseDir, "/m/a", "cc-pool")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	r := newRetirer(s)
	// Two deferring ticks: the second pins the lastDefer dedup path and that
	// deferral never accumulates state toward a sweep.
	for i := range 2 {
		if r.tick(time.Now()) {
			t.Fatalf("tick %d retired a lease-held mount", i)
		}
	}
	if _, teardowns := fake.calls(); len(teardowns) != 0 {
		t.Fatalf("lease-held defer tore down %v", teardowns)
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
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/p", Dir: "/m/p", Owner: "cc-pool"})

	r := newRetirer(s)
	if !r.tick(time.Now()) {
		t.Fatal("tick did not retire with every lease free")
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
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool"})
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
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool"})
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
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool"})
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
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/t1", Dir: "/mux/t1", Owner: "cc-pool", MuxRoot: "/mux"})
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
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})

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

// TestRetireLeaseSeizeMidSweepAborts pins that the sweep's own Seize is the
// authoritative busy re-check: a lease acquired after the gate passed (here,
// before retireSweep runs directly) aborts the sweep before any teardown.
func TestRetireLeaseSeizeMidSweepAborts(t *testing.T) {
	fake := &fakeHost{}
	s, _, _, _ := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	h, err := lease.Acquire(s.LeaseDir, "/m/a", "cc-pool")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	if s.retireSweep() {
		t.Fatal("sweep drained under a held lease")
	}
	if _, teardowns := fake.calls(); len(teardowns) != 0 {
		t.Fatalf("lease-held sweep still tore down %v", teardowns)
	}
	if _, ok := s.registered("/m/a"); !ok {
		t.Fatal("mount lost its row on an aborted sweep")
	}
}

func TestRetireStormBreakerParksLoudly(t *testing.T) {
	fake := &fakeHost{}
	s, _, _, shutdowns := skewedServer(t, fake)
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool"})
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
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})

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
	if s.strikes.Struck(time.Now()) {
		t.Fatal("corrupt history restored as struck")
	}
}

func TestRetireSkewTransitions(t *testing.T) {
	fake := &fakeHost{}
	s, _, skewed, _ := skewedServer(t, fake)
	// A gate-passing tick whose sweep aborts busy: free leases with one
	// in-process claim held.
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	mountForTest(t, s, fusekit.MountSpec{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool"})
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

// TestRetireAbortDuringPendingTeardown pins R3's park-aware abort: a sweep
// whose mux tenant teardown left the last-root unmount IN FLIGHT parks the
// tenant's claims; the abort must wait that park out before its re-attach —
// a blind remount would bounce off its own parked claim and lose the tenant.
// The bounded negative leg: an unresolved park leaves the tenant journaled
// (never re-attached under a live parked unmount).
func TestRetireAbortDuringPendingTeardown(t *testing.T) {
	build := func(t *testing.T, done <-chan struct{}) (*Server, *fakeHost, string, *int) {
		t.Helper()
		fake := &fakeHost{muxRootsHeld: map[string]bool{"/mux": true}}
		host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{"/mux/t1": done}}
		s, path := newJournaledHandlerServer(t, host)
		sd := 0
		s.RetireSkew = func() (bool, string, error) { return true, "skewed", nil }
		s.triggerShutdown = func() { sd++ }
		mountForTest(t, s, fusekit.MountSpec{Base: "/b/t1", Dir: "/mux/t1", Owner: "cc-pool", MuxRoot: "/mux"})
		fake.mu.Lock()
		fake.teardownFn = func(string, string) error { return pendingErr() }
		fake.mu.Unlock()
		return s, fake, path, &sd
	}

	t.Run("park resolves: the abort re-attaches the tenant", func(t *testing.T) {
		done := make(chan struct{})
		close(done) // the parked unmount resolves as soon as the watcher starts
		s, fake, path, shutdowns := build(t, done)
		if newRetirer(s).tick(time.Now()) {
			t.Fatal("tick retired past a pending mux-root unmount")
		}
		if *shutdowns != 0 {
			t.Fatal("shutdown triggered on an aborted sweep")
		}
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
	})

	t.Run("park unresolved: bounded wait, tenant left journaled", func(t *testing.T) {
		prev := retireAbortParkWait
		retireAbortParkWait = 50 * time.Millisecond
		t.Cleanup(func() { retireAbortParkWait = prev })
		done := make(chan struct{}) // never resolves
		t.Cleanup(func() { close(done) })
		s, fake, path, _ := build(t, done)
		if newRetirer(s).tick(time.Now()) {
			t.Fatal("tick retired past a pending mux-root unmount")
		}
		setups, _ := fake.calls()
		if len(setups) != 1 {
			t.Fatalf("setups = %v, want NO re-attach under a live parked unmount", setups)
		}
		if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/mux/t1"}) {
			t.Fatalf("journal = %v, want the tenant kept for the consumer or a successor", dirs)
		}
		if _, ok := s.claim("/mux/t1"); ok {
			t.Fatal("claim free under an unresolved parked unmount")
		}
	})
}

// TestRetireAbortAwaitsSiblingRootPark pins R4-7: with TWO tenants on one
// mux root, the LAST tenant's swept teardown parks the shared root's claim
// under ITS dir — the abort's remount of the OTHER tenant collides with that
// root claim, so the abort must await the park through the ROOT key before
// re-attaching. The unresolved leg is the bounded, journaled-loud fallback.
func TestRetireAbortAwaitsSiblingRootPark(t *testing.T) {
	build := func(t *testing.T, done <-chan struct{}) (*Server, *fakeHost, string, *int) {
		t.Helper()
		fake := &fakeHost{muxRootsHeld: map[string]bool{"/mux": true}}
		host := &pendingHost{fakeHost: fake, pending: map[string]<-chan struct{}{"/mux/t2": done}}
		s, path := newJournaledHandlerServer(t, host)
		sd := 0
		s.RetireSkew = func() (bool, string, error) { return true, "skewed", nil }
		s.triggerShutdown = func() { sd++ }
		for _, name := range []string{"t1", "t2"} {
			mountForTest(t, s, fusekit.MountSpec{Base: "/b/" + name, Dir: "/mux/" + name, Owner: "cc-pool", MuxRoot: "/mux"})
		}
		// Only the LAST child's teardown (the native root unmount) pends;
		// sibling detaches stay clean.
		fake.mu.Lock()
		fake.teardownFn = func(_, dir string) error {
			if dir == "/mux/t2" {
				return pendingErr()
			}
			return nil
		}
		fake.mu.Unlock()
		return s, fake, path, &sd
	}

	t.Run("root park resolves: both tenants re-attach", func(t *testing.T) {
		done := make(chan struct{})
		close(done)
		s, _, path, shutdowns := build(t, done)
		if newRetirer(s).tick(time.Now()) {
			t.Fatal("tick retired past a pending shared-root unmount")
		}
		if *shutdowns != 0 {
			t.Fatal("shutdown triggered on an aborted sweep")
		}
		if mux := registryMux(s); !reflect.DeepEqual(mux, map[string]string{"/mux/t1": "/mux", "/mux/t2": "/mux"}) {
			t.Fatalf("registry after abort = %v, want BOTH tenants re-attached (t1 must not bounce off t2's root park)", mux)
		}
		if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/mux/t1", "/mux/t2"}) {
			t.Fatalf("journal = %v, want both tenants intact", dirs)
		}
	})

	t.Run("root park unresolved: bounded wait, tenants left journaled", func(t *testing.T) {
		prev := retireAbortParkWait
		retireAbortParkWait = 50 * time.Millisecond
		t.Cleanup(func() { retireAbortParkWait = prev })
		done := make(chan struct{}) // never resolves
		t.Cleanup(func() { close(done) })
		s, fake, path, _ := build(t, done)
		if newRetirer(s).tick(time.Now()) {
			t.Fatal("tick retired past a pending shared-root unmount")
		}
		setups, _ := fake.calls()
		if len(setups) != 2 {
			t.Fatalf("setups = %v, want NO re-attach while the shared root's unmount is in flight", setups)
		}
		if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/mux/t1", "/mux/t2"}) {
			t.Fatalf("journal = %v, want both tenants kept for the consumer or a successor", dirs)
		}
	})
}
