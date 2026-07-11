package mountd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/proc"
)

// fullSpec sets every MountSpec field the journal must persist; DeepEqual
// against it catches a field the journal silently drops.
func fullSpec() fusekit.MountSpec {
	return fusekit.MountSpec{
		Base:             "/b/acct",
		Dir:              "/m/acct",
		Owner:            "cc-pool",
		ContentSocket:    "/s/c.sock",
		Domain:           "dom",
		PrivateRoot:      "/p",
		ContentMode:      "source",
		ProbePath:        ".probe",
		PrivatePrefixes:  []string{".credentials.json"},
		AttrCache:        true,
		AttrCacheTimeout: 2 * time.Second,
	}
}

func muxSpec(name string) fusekit.MountSpec {
	return fusekit.MountSpec{Base: "/b/" + name, Dir: "/mux/" + name, Owner: "cc-pool", MuxRoot: "/mux", ContentMode: "mux"}
}

func testBridgeEntry() bridgeEntry {
	return bridgeEntry{Owner: "cc-pool", BridgeSocket: "/grp/b.sock", ContentSocket: "/up/c.sock", PrivatePrefixes: []string{"secret"}}
}

func readJournalFile(t *testing.T, path string) journalFile {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return journalFile{}
	}
	if err != nil {
		t.Fatal(err)
	}
	var f journalFile
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("journal file unparseable: %v\n%s", err, data)
	}
	return f
}

func journaledMountDirs(t *testing.T, path string) []string {
	t.Helper()
	f := readJournalFile(t, path)
	dirs := make([]string, 0, len(f.Mounts))
	for _, m := range f.Mounts {
		dirs = append(dirs, m.Dir)
	}
	return dirs
}

func TestDefaultJournalPath(t *testing.T) {
	if got, want := DefaultJournalPath("/x/y/holder.sock"), "/x/y/holder-specs.json"; got != want {
		t.Fatalf("DefaultJournalPath = %q, want %q", got, want)
	}
}

func TestJournalRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holder-specs.json")
	j := newJournal(path)
	plain, tenant, bridge := fullSpec(), muxSpec("t1"), testBridgeEntry()
	if err := j.putMount(plain); err != nil {
		t.Fatal(err)
	}
	if err := j.putMount(tenant); err != nil {
		t.Fatal(err)
	}
	if err := j.putBridge(bridge); err != nil {
		t.Fatal(err)
	}

	re, err := openJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	mounts, bridges := re.snapshot()
	wantMounts := []mountEntry{mountEntryOf(plain), mountEntryOf(tenant)} // sorted by dir
	if !reflect.DeepEqual(mounts, wantMounts) {
		t.Fatalf("reloaded mounts = %+v, want %+v", mounts, wantMounts)
	}
	if want := []bridgeEntry{bridge}; !reflect.DeepEqual(bridges, want) {
		t.Fatalf("reloaded bridges = %+v, want %+v", bridges, want)
	}

	if err := j.dropMount(tenant.Dir); err != nil {
		t.Fatal(err)
	}
	if err := j.dropBridge(bridge.Owner); err != nil {
		t.Fatal(err)
	}
	re2, err := openJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	mounts, bridges = re2.snapshot()
	if want := []mountEntry{mountEntryOf(plain)}; !reflect.DeepEqual(mounts, want) {
		t.Fatalf("post-drop mounts = %+v, want %+v", mounts, want)
	}
	if len(bridges) != 0 {
		t.Fatalf("post-drop bridges = %+v, want none", bridges)
	}
}

// TestJournalGoldenFormat freezes the on-disk shape: a successor holder
// generation parses this file, so a failing golden is a durable-format break —
// additive-only, like the wire protocol.
func TestJournalGoldenFormat(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holder-specs.json")
	j := newJournal(path)
	if err := j.putMount(fullSpec()); err != nil {
		t.Fatal(err)
	}
	if err := j.putBridge(testBridgeEntry()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := `{
  "mounts": [
    {
      "base": "/b/acct",
      "dir": "/m/acct",
      "owner": "cc-pool",
      "content_socket": "/s/c.sock",
      "domain": "dom",
      "private_root": "/p",
      "content_mode": "source",
      "probe_path": ".probe",
      "private_prefixes": [
        ".credentials.json"
      ],
      "attr_cache": true,
      "attr_cache_timeout": 2000000000
    }
  ],
  "bridges": [
    {
      "owner": "cc-pool",
      "bridge_socket": "/grp/b.sock",
      "content_socket": "/up/c.sock",
      "private_prefixes": [
        "secret"
      ]
    }
  ]
}`
	if string(got) != want {
		t.Fatalf("journal file = %s\nwant %s\n(frozen durable-format artifact)", got, want)
	}
}

func TestJournalDropAbsentWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holder-specs.json")
	j := newJournal(path)
	if err := j.dropMount("/m/none"); err != nil {
		t.Fatal(err)
	}
	if err := j.dropBridge("nobody"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("absent drops touched the journal file: stat err = %v", err)
	}
}

func TestOpenJournalMissingAndCorrupt(t *testing.T) {
	j, err := openJournal(filepath.Join(t.TempDir(), "nope.json"))
	if err != nil {
		t.Fatalf("missing journal: %v", err)
	}
	if mounts, bridges := j.snapshot(); len(mounts) != 0 || len(bridges) != 0 {
		t.Fatalf("missing journal not empty: %v %v", mounts, bridges)
	}

	path := filepath.Join(t.TempDir(), "holder-specs.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openJournal(path); err == nil {
		t.Fatal("corrupt journal opened without error")
	}
}

func newJournaledHandlerServer(t *testing.T, f *fakeHost) (*Server, string) {
	t.Helper()
	s := newHandlerServer(t, f)
	path := filepath.Join(t.TempDir(), "holder-specs.json")
	s.journal = newJournal(path)
	return s, path
}

func TestMountLifecycleCoUpdatesJournal(t *testing.T) {
	fake := &fakeHost{}
	s, path := newJournaledHandlerServer(t, fake)
	spec := fullSpec()
	req := mountEntryOf(spec).mountRequest()

	if resp := s.dispatch(req); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	f := readJournalFile(t, path)
	if want := []mountEntry{mountEntryOf(spec)}; !reflect.DeepEqual(f.Mounts, want) {
		t.Fatalf("journaled mounts = %+v, want %+v", f.Mounts, want)
	}

	// A failed Setup must never journal: the registry has no row for it.
	fake.mu.Lock()
	fake.setupFn = func(base, dir string) error {
		if dir == "/m/bad" {
			return errors.New("boom")
		}
		return nil
	}
	fake.mu.Unlock()
	if resp := s.dispatch(Request{Op: OpMount, Base: "/b/bad", Dir: "/m/bad", Owner: "cc-pool"}); resp.OK {
		t.Fatal("bad mount succeeded")
	}
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{spec.Dir}) {
		t.Fatalf("journal after failed mount = %v, want [%s]", dirs, spec.Dir)
	}

	if resp := s.dispatch(Request{Op: OpUnmount, Base: spec.Base, Dir: spec.Dir, Owner: spec.Owner}); !resp.OK {
		t.Fatalf("unmount: %s", resp.Error)
	}
	if dirs := journaledMountDirs(t, path); len(dirs) != 0 {
		t.Fatalf("journal after unmount = %v, want empty", dirs)
	}
}

func TestWedgedUnmountDropsJournalEntry(t *testing.T) {
	fake := &fakeHost{}
	s, path := newJournaledHandlerServer(t, fake)
	if resp := s.dispatch(Request{Op: OpMount, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	fake.mu.Lock()
	fake.teardownFn = func(base, dir string) error { return fusekit.ErrUnmountWedged }
	fake.mu.Unlock()
	resp := s.dispatch(Request{Op: OpUnmount, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassWedged {
		t.Fatalf("wedged unmount = (ok=%v class=%q), want wedged failure", resp.OK, resp.ErrClass)
	}
	// Registry honesty extends to the journal: the row is dropped either way.
	if dirs := journaledMountDirs(t, path); len(dirs) != 0 {
		t.Fatalf("journal after wedged unmount = %v, want empty", dirs)
	}
}

// TestUnmountRowlessNotMountedDropsJournalEntry pins Fix C: a retire sweep
// leaves a mount rowless, journaled, and kernel-unmounted; an owner's acked
// Unmount in that state must drop the journal entry too, or the successor
// resurrects a mount the owner explicitly removed.
func TestUnmountRowlessNotMountedDropsJournalEntry(t *testing.T) {
	fake := &fakeHost{}
	s, path := newJournaledHandlerServer(t, fake)
	for _, name := range []string{"a", "b"} {
		if resp := s.dispatch(Request{Op: OpMount, Base: "/b/" + name, Dir: "/m/" + name, Owner: "cc-pool"}); !resp.OK {
			t.Fatalf("mount %s: %s", name, resp.Error)
		}
	}
	// Mimic the post-sweep state for /m/a: row dropped, journal kept, kernel
	// unmounted.
	s.mu.Lock()
	delete(s.registry, "/m/a")
	s.mu.Unlock()
	fake.setLive("/m/a", false)

	if resp := s.dispatch(Request{Op: OpUnmount, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("rowless unmount: %s", resp.Error)
	}
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/b"}) {
		t.Fatalf("journal after rowless unmount = %v, want only /m/b (the ack must drop /m/a)", dirs)
	}
	// The no-op stayed a no-op: nothing was torn down for the absent mount.
	if _, teardowns := fake.calls(); len(teardowns) != 0 {
		t.Fatalf("rowless not-mounted unmount tore down %v", teardowns)
	}
}

func TestSweepCoUpdatesJournalPerDir(t *testing.T) {
	fake := &fakeHost{}
	s, path := newJournaledHandlerServer(t, fake)
	for _, name := range []string{"a", "b"} {
		if resp := s.dispatch(Request{Op: OpMount, Base: "/b/" + name, Dir: "/m/" + name, Owner: "cc-pool"}); !resp.OK {
			t.Fatalf("mount %s: %s", name, resp.Error)
		}
	}
	release, ok := s.claim("/m/a")
	if !ok {
		t.Fatal("claim /m/a")
	}
	failed := s.unmountAll()
	if len(failed) != 1 || failed[0].Dir != "/m/a" {
		t.Fatalf("sweep failed = %+v, want the busy /m/a only", failed)
	}
	// The busy dir stays journaled (still registered); the swept one is gone.
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/a"}) {
		t.Fatalf("journal after partial sweep = %v, want [/m/a]", dirs)
	}
	release()
	if failed := s.unmountAll(); len(failed) != 0 {
		t.Fatalf("second sweep failed = %+v", failed)
	}
	if dirs := journaledMountDirs(t, path); len(dirs) != 0 {
		t.Fatalf("journal after full sweep = %v, want empty", dirs)
	}
}

// TestSweepWedgeKeepsRowAndJournalEntry pins the sweep half of journal
// convergence: a failed teardown keeps the journal entry for the successor,
// keeps a plain mount's row (restored handle) so a later sweep converges, and
// drops only a detached mux tenant's lying row.
func TestSweepWedgeKeepsRowAndJournalEntry(t *testing.T) {
	t.Run("plain wedge keeps row and entry then converges", func(t *testing.T) {
		fake := &fakeHost{}
		s, path := newJournaledHandlerServer(t, fake)
		for _, name := range []string{"a", "b"} {
			if resp := s.dispatch(Request{Op: OpMount, Base: "/b/" + name, Dir: "/m/" + name, Owner: "cc-pool"}); !resp.OK {
				t.Fatalf("mount %s: %s", name, resp.Error)
			}
		}
		fake.mu.Lock()
		fake.teardownFn = func(_, dir string) error {
			if dir == "/m/a" {
				return fusekit.ErrUnmountWedged
			}
			return nil
		}
		fake.mu.Unlock()

		failed := s.unmountAll()
		if len(failed) != 1 || failed[0].Dir != "/m/a" || !failed[0].Live {
			t.Fatalf("sweep failed = %+v, want live /m/a only", failed)
		}
		if _, ok := s.registered("/m/a"); !ok {
			t.Fatal("wedged plain mount lost its registry row")
		}
		if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/a"}) {
			t.Fatalf("journal after wedged sweep = %v, want [/m/a] kept for the successor", dirs)
		}
		// The wedge clears; the retry sweep converges to empty.
		fake.mu.Lock()
		fake.teardownFn = nil
		fake.mu.Unlock()
		if failed := s.unmountAll(); len(failed) != 0 {
			t.Fatalf("retry sweep failed = %+v", failed)
		}
		if dirs := journaledMountDirs(t, path); len(dirs) != 0 {
			t.Fatalf("journal after retry sweep = %v, want empty", dirs)
		}
		if reg := s.snapshotRegistry(); len(reg) != 0 {
			t.Fatalf("registry after retry sweep = %v, want empty", reg)
		}
	})

	t.Run("mux tenant wedge drops the lying row and keeps the entry", func(t *testing.T) {
		fake := &fakeHost{}
		s, path := newJournaledHandlerServer(t, fake)
		if resp := s.dispatch(Request{Op: OpMount, Base: "/b/t1", Dir: "/mux/t1", Owner: "cc-pool", MuxRoot: "/mux"}); !resp.OK {
			t.Fatalf("mount: %s", resp.Error)
		}
		fake.mu.Lock()
		fake.teardownFn = func(string, string) error { return fusekit.ErrUnmountWedged }
		fake.mu.Unlock()

		failed := s.unmountAll()
		if len(failed) != 1 || failed[0].Dir != "/mux/t1" {
			t.Fatalf("sweep failed = %+v, want /mux/t1", failed)
		}
		if _, ok := s.registered("/mux/t1"); ok {
			t.Fatal("detached mux tenant kept a lying registry row")
		}
		if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/mux/t1"}) {
			t.Fatalf("journal after wedged mux sweep = %v, want [/mux/t1] kept for the successor", dirs)
		}
	})
}

func TestBridgeLifecycleCoUpdatesJournal(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	fake := &fakeHost{}
	s, path := newJournaledHandlerServer(t, fake)

	if resp := addBridge(t, s, "cc-pool", "/grp/b.sock", "/up/a.sock", []string{"secret"}); !resp.OK {
		t.Fatalf("add: %s", resp.Error)
	}
	f := readJournalFile(t, path)
	want := []bridgeEntry{{Owner: "cc-pool", BridgeSocket: "/grp/b.sock", ContentSocket: "/up/a.sock", PrivatePrefixes: []string{"secret"}}}
	if !reflect.DeepEqual(f.Bridges, want) {
		t.Fatalf("journaled bridges = %+v, want %+v", f.Bridges, want)
	}

	// Adopt-in-place must re-journal the new upstream and prefixes.
	if resp := addBridge(t, s, "cc-pool", "/grp/b.sock", "/up/b.sock", []string{"secret", "other"}); !resp.OK {
		t.Fatalf("adopt: %s", resp.Error)
	}
	f = readJournalFile(t, path)
	want = []bridgeEntry{{Owner: "cc-pool", BridgeSocket: "/grp/b.sock", ContentSocket: "/up/b.sock", PrivatePrefixes: []string{"secret", "other"}}}
	if !reflect.DeepEqual(f.Bridges, want) {
		t.Fatalf("journal after adopt = %+v, want %+v", f.Bridges, want)
	}

	// A refused add must not journal.
	if resp := s.dispatch(Request{Op: OpAddBridge, Owner: "x/../victim", BridgeSocket: "/grp/v.sock", ContentSocket: "/up/v.sock"}); resp.OK {
		t.Fatal("hostile owner accepted")
	}
	if f = readJournalFile(t, path); len(f.Bridges) != 1 {
		t.Fatalf("journal after refused add = %+v", f.Bridges)
	}

	if resp := s.dispatch(Request{Op: OpRemoveBridge, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("remove: %s", resp.Error)
	}
	if f = readJournalFile(t, path); len(f.Bridges) != 0 {
		t.Fatalf("journal after remove = %+v, want empty", f.Bridges)
	}

	// Reclaim tears the bridge down through the same path and must co-delete.
	if resp := addBridge(t, s, "cc-pool", "/grp/b.sock", "/up/a.sock", nil); !resp.OK {
		t.Fatalf("re-add: %s", resp.Error)
	}
	if resp := s.dispatch(Request{Op: OpReclaim, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("reclaim: %s", resp.Error)
	}
	if f = readJournalFile(t, path); len(f.Bridges) != 0 {
		t.Fatalf("journal after reclaim = %+v, want empty", f.Bridges)
	}
}

// TestBridgeJournalRegistryOrderUnderChurn hammers same-owner add/remove races
// and then pins the journal file to the registry outcome: a journal write
// ordered against a concurrent removal the wrong way around would leave a
// phantom entry for the next generation to resurrect — or lose a live bridge.
func TestBridgeJournalRegistryOrderUnderChurn(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s, path := newJournaledHandlerServer(t, &fakeHost{})

	const workers, iters = 4, 40
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				addBridge(t, s, "cc-pool", "/grp/b.sock", "/up/a.sock", nil)
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				s.dispatch(Request{Op: OpRemoveBridge, Owner: "cc-pool"})
			}
		}()
	}
	wg.Wait()

	// Quiesce on a removed bridge: the journal must not hold a phantom.
	if resp := s.dispatch(Request{Op: OpRemoveBridge, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("final remove: %s", resp.Error)
	}
	if f := readJournalFile(t, path); len(f.Bridges) != 0 {
		t.Fatalf("journal after remove churn = %+v, want empty", f.Bridges)
	}

	// Quiesce on a live bridge: the journal must not have lost it.
	if resp := addBridge(t, s, "cc-pool", "/grp/b.sock", "/up/a.sock", nil); !resp.OK {
		t.Fatalf("final add: %s", resp.Error)
	}
	f := readJournalFile(t, path)
	want := []bridgeEntry{{Owner: "cc-pool", BridgeSocket: "/grp/b.sock", ContentSocket: "/up/a.sock"}}
	if !reflect.DeepEqual(f.Bridges, want) {
		t.Fatalf("journal after add churn = %+v, want %+v", f.Bridges, want)
	}
}

func shrinkReplayRetries(t *testing.T) {
	t.Helper()
	prevA, prevB := replayAttempts, replayBackoff
	replayAttempts = 2
	replayBackoff = proc.Backoff{Base: time.Millisecond, Cap: time.Millisecond}
	t.Cleanup(func() { replayAttempts, replayBackoff = prevA, prevB })
}

// reapCapture records the replay's carcass-clear and orphan-reap calls; its
// mutex makes the cross-goroutine reads race-clean.
type reapCapture struct {
	mu      sync.Mutex
	cleared []string
	reaps   [][]string
}

func (c *reapCapture) snapshot() (cleared []string, reaps [][]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.cleared...), append([][]string(nil), c.reaps...)
}

func captureReapSeams(t *testing.T) *reapCapture {
	t.Helper()
	c := &reapCapture{}
	prevClear, prevReap := clearCarcass, reapOrphans
	clearCarcass = func(dir string) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.cleared = append(c.cleared, dir)
		return nil
	}
	reapOrphans = func(roots []string) []int {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.reaps = append(c.reaps, append([]string(nil), roots...))
		return nil
	}
	t.Cleanup(func() { clearCarcass, reapOrphans = prevClear, prevReap })
	return c
}

func startJournaledServer(t *testing.T, fake *fakeHost, socket, journalPath string) (*Server, *Client) {
	t.Helper()
	s, cl, _, _ := runServer(t, &Server{Socket: socket, Host: fake, Version: testVersion, Log: log.New(io.Discard, "", 0), JournalPath: journalPath})
	return s, cl
}

// listTopology projects the wire List into dir->base and dir->muxroot maps.
func listTopology(t *testing.T, cl *Client) (bases, mux map[string]string) {
	t.Helper()
	mounts, err := cl.List()
	if err != nil {
		t.Fatal(err)
	}
	bases, mux = map[string]string{}, map[string]string{}
	for _, m := range mounts {
		bases[m.Dir] = m.Base
		mux[m.Dir] = m.MuxRoot
	}
	return bases, mux
}

func TestReplayRestoresJournaledState(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	capture := captureReapSeams(t)
	sockDir := shortSockDir(t)
	socket := filepath.Join(sockDir, "m.sock")
	jpath := DefaultJournalPath(socket)

	plain, t1, t2 := fullSpec(), muxSpec("t1"), muxSpec("t2")
	bridge := bridgeEntry{Owner: "cc-pool", BridgeSocket: filepath.Join(sockDir, "b.sock"), ContentSocket: "/up/c.sock", PrivatePrefixes: []string{"secret"}}
	seed := newJournal(jpath)
	for _, spec := range []fusekit.MountSpec{plain, t1, t2} {
		if err := seed.putMount(spec); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.putBridge(bridge); err != nil {
		t.Fatal(err)
	}

	fake := &fakeHost{}
	_, cl := startJournaledServer(t, fake, socket, jpath)

	// Assert over the wire: the Run goroutine owns the registry, so wire reads
	// are both the real consumer surface and race-clean.
	gotBases, gotMux := listTopology(t, cl)
	wantBases := map[string]string{"/m/acct": "/b/acct", "/mux/t1": "/b/t1", "/mux/t2": "/b/t2"}
	if !reflect.DeepEqual(gotBases, wantBases) {
		t.Fatalf("replayed registry = %v, want %v", gotBases, wantBases)
	}
	wantMux := map[string]string{"/m/acct": "", "/mux/t1": "/mux", "/mux/t2": "/mux"}
	if !reflect.DeepEqual(gotMux, wantMux) {
		t.Fatalf("replayed mux topology = %v, want %v", gotMux, wantMux)
	}

	// Full-fidelity re-Setup: the plain mount's spec survives the journal intact.
	var replayed *fusekit.MountSpec
	for _, spec := range fake.capturedSpecs() {
		if spec.Dir == plain.Dir {
			spec := spec
			replayed = &spec
		}
	}
	if replayed == nil || !reflect.DeepEqual(*replayed, plain) {
		t.Fatalf("replayed spec = %+v, want %+v", replayed, plain)
	}

	cl.Owner = "cc-pool"
	infos, err := cl.Bridges()
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[0].Socket != bridge.BridgeSocket || infos[0].Upstream != bridge.ContentSocket {
		t.Fatalf("replayed bridge = %+v", infos)
	}

	// Carcass-clear and reap ran once each over the deduped kernel roots: the
	// mux tenants collapse to their shared root, never the logical dirs.
	wantRoots := []string{"/m/acct", "/mux"}
	cleared, reaps := capture.snapshot()
	if !reflect.DeepEqual(cleared, wantRoots) {
		t.Fatalf("carcass-cleared roots = %v, want %v", cleared, wantRoots)
	}
	if len(reaps) != 1 || !reflect.DeepEqual(reaps[0], wantRoots) {
		t.Fatalf("reap calls = %v, want one over %v", reaps, wantRoots)
	}

	// The journal still holds everything it replayed.
	f := readJournalFile(t, jpath)
	if dirs := journaledMountDirs(t, jpath); !reflect.DeepEqual(dirs, []string{"/m/acct", "/mux/t1", "/mux/t2"}) {
		t.Fatalf("post-replay journal mounts = %v", dirs)
	}
	if want := []bridgeEntry{bridge}; !reflect.DeepEqual(f.Bridges, want) {
		t.Fatalf("post-replay journal bridges = %+v, want %+v", f.Bridges, want)
	}
}

func TestReplayDropsDeadEntryAndServes(t *testing.T) {
	shrinkReplayRetries(t)
	captureReapSeams(t)
	sockDir := shortSockDir(t)
	socket := filepath.Join(sockDir, "m.sock")
	jpath := DefaultJournalPath(socket)

	seed := newJournal(jpath)
	for _, spec := range []fusekit.MountSpec{
		{Base: "/b/bad", Dir: "/m/bad", Owner: "cc-pool"},
		{Base: "/b/good", Dir: "/m/good", Owner: "cc-pool"},
	} {
		if err := seed.putMount(spec); err != nil {
			t.Fatal(err)
		}
	}

	fake := &fakeHost{setupFn: func(base, dir string) error {
		if dir == "/m/bad" {
			return errors.New("boom")
		}
		return nil
	}}
	_, cl := startJournaledServer(t, fake, socket, jpath)

	gotBases, _ := listTopology(t, cl)
	if want := map[string]string{"/m/good": "/b/good"}; !reflect.DeepEqual(gotBases, want) {
		t.Fatalf("registry = %v, want %v", gotBases, want)
	}
	if dirs := journaledMountDirs(t, jpath); !reflect.DeepEqual(dirs, []string{"/m/good"}) {
		t.Fatalf("journal = %v, want the dead entry dropped", dirs)
	}
	// Each attempt retried the dead entry exactly replayAttempts times.
	setups, _ := fake.calls()
	badSetups := 0
	for _, c := range setups {
		if c.dir == "/m/bad" {
			badSetups++
		}
	}
	if badSetups != replayAttempts {
		t.Fatalf("dead entry setups = %d, want %d", badSetups, replayAttempts)
	}

	// The holder serves after a partial replay, and new mounts journal normally.
	cl.Owner = "cc-pool"
	if err := cl.AddMount(fusekit.MountSpec{Base: "/b/new", Dir: "/m/new", Owner: "cc-pool"}); err != nil {
		t.Fatalf("post-replay mount: %v", err)
	}
	if dirs := journaledMountDirs(t, jpath); !reflect.DeepEqual(dirs, []string{"/m/good", "/m/new"}) {
		t.Fatalf("journal after wire mount = %v", dirs)
	}
}

func TestReplayCorruptJournalStartsEmptyAndServes(t *testing.T) {
	captureReapSeams(t)
	sockDir := shortSockDir(t)
	socket := filepath.Join(sockDir, "m.sock")
	jpath := DefaultJournalPath(socket)
	if err := os.WriteFile(jpath, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	fake := &fakeHost{}
	_, cl := startJournaledServer(t, fake, socket, jpath)
	if gotBases, _ := listTopology(t, cl); len(gotBases) != 0 {
		t.Fatalf("registry after corrupt journal = %v, want empty", gotBases)
	}

	// Serving continues, and the first mount rebuilds a valid journal.
	cl.Owner = "cc-pool"
	if err := cl.AddMount(fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}); err != nil {
		t.Fatalf("mount over corrupt journal: %v", err)
	}
	if dirs := journaledMountDirs(t, jpath); !reflect.DeepEqual(dirs, []string{"/m/a"}) {
		t.Fatalf("rebuilt journal = %v, want [/m/a]", dirs)
	}
}

// TestReplayEstablishesBridgesBeforeMounts pins the replay order: a tree-mode
// mount replays through its owner's bridge, so mount-first would burn its
// retries against a not-yet-up bridge and drop it from the journal for good.
func TestReplayEstablishesBridgesBeforeMounts(t *testing.T) {
	redirectSpool(t)
	captureReapSeams(t)
	var mu sync.Mutex
	var order []string
	record := func(what string) {
		mu.Lock()
		order = append(order, what)
		mu.Unlock()
	}
	prev := startBridge
	startBridge = func(_ *Server, row *bridgeRow) {
		record("bridge:" + row.owner)
		close(row.done)
	}
	t.Cleanup(func() { startBridge = prev })

	sockDir := shortSockDir(t)
	socket := filepath.Join(sockDir, "m.sock")
	jpath := DefaultJournalPath(socket)
	seed := newJournal(jpath)
	for _, spec := range []fusekit.MountSpec{
		{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"},
		{Base: "/b/b", Dir: "/m/b", Owner: "cc-pool"},
	} {
		if err := seed.putMount(spec); err != nil {
			t.Fatal(err)
		}
	}
	if err := seed.putBridge(bridgeEntry{Owner: "cc-pool", BridgeSocket: filepath.Join(sockDir, "b.sock"), ContentSocket: "/up/c.sock"}); err != nil {
		t.Fatal(err)
	}

	fake := &fakeHost{setupFn: func(_, dir string) error {
		record("mount:" + dir)
		return nil
	}}
	startJournaledServer(t, fake, socket, jpath)

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	want := []string{"bridge:cc-pool", "mount:/m/a", "mount:/m/b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("replay order = %v, want bridges strictly before mounts %v", got, want)
	}
}

// TestShutdownJournalDrainVsRetirePreserve pins the journal's lifecycle
// contract. A clean external shutdown (bootout/logout/reboot SIGTERM, here
// ctx cancel) drains bridges and cleanly-swept mounts — consumers
// re-establish — but a mount whose teardown WEDGED keeps its entry: its
// kernel mount outlives the process as a carcass the successor's replay
// clears (carcass proof v2) or surfaces. A self-retire exit preserves the
// whole journal for the successor's replay.
func TestShutdownJournalDrainVsRetirePreserve(t *testing.T) {
	t.Run("clean shutdown drains all but the wedged mount", func(t *testing.T) {
		stubStartBridge(t)
		redirectSpool(t)
		sockDir := shortSockDir(t)
		socket := filepath.Join(sockDir, "m.sock")
		jpath := DefaultJournalPath(socket)

		// A wedged unmount must not block the exit or the drain.
		fake := &fakeHost{teardownFn: func(_, dir string) error {
			if dir == "/m/wedged" {
				return fusekit.ErrUnmountWedged
			}
			return nil
		}}
		s, cl, done, cancel := runServer(t, &Server{Socket: socket, Host: fake, Version: testVersion, Log: log.New(io.Discard, "", 0), JournalPath: jpath})
		cl.Owner = "cc-pool"
		for _, dir := range []string{"/m/clean", "/m/wedged"} {
			if err := cl.AddMount(fusekit.MountSpec{Base: "/b" + dir, Dir: dir, Owner: "cc-pool"}); err != nil {
				t.Fatalf("mount %s: %v", dir, err)
			}
		}
		if _, err := cl.AddBridge(filepath.Join(sockDir, "b.sock"), "/up/c.sock", nil); err != nil {
			t.Fatalf("add bridge: %v", err)
		}
		f := readJournalFile(t, jpath)
		if len(f.Mounts) != 2 || len(f.Bridges) != 1 {
			t.Fatalf("pre-shutdown journal = %+v, want 2 mounts and 1 bridge", f)
		}

		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not exit on cancel")
		}
		f = readJournalFile(t, jpath)
		if len(f.Bridges) != 0 {
			t.Fatalf("post-shutdown bridges = %+v, want drained", f.Bridges)
		}
		// The wedged mount's entry survives for the successor; the clean mount
		// drained.
		if len(f.Mounts) != 1 || f.Mounts[0].Dir != "/m/wedged" {
			t.Fatalf("post-shutdown mounts = %+v, want only the wedged mount", f.Mounts)
		}
		if s.retired.Load() {
			t.Fatal("an external shutdown read as a retire")
		}
	})

	t.Run("self-retire preserves the journal for the successor", func(t *testing.T) {
		prevTick := retireTick
		retireTick = 20 * time.Millisecond
		t.Cleanup(func() { retireTick = prevTick })
		captureReapSeams(t)
		sockDir := shortSockDir(t)
		socket := filepath.Join(sockDir, "m.sock")
		jpath := DefaultJournalPath(socket)
		seed := newJournal(jpath)
		spec := fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}
		if err := seed.putMount(spec); err != nil {
			t.Fatal(err)
		}

		s := &Server{
			Socket: socket, Host: &fakeHost{}, Version: testVersion,
			Log: log.New(io.Discard, "", 0), JournalPath: jpath, LeaseDir: t.TempDir(),
			RetireSkew: func() (bool, string, error) { return true, "test skew", nil },
		}
		done := make(chan error, 1)
		go func() { done <- s.Run(context.Background()) }()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("holder did not self-retire")
		}
		if !s.retired.Load() {
			t.Fatal("retire exit did not mark retired")
		}
		f := readJournalFile(t, jpath)
		if want := []mountEntry{mountEntryOf(spec)}; !reflect.DeepEqual(f.Mounts, want) {
			t.Fatalf("post-retire journal = %+v, want %+v preserved for the successor", f.Mounts, want)
		}
	})
}
