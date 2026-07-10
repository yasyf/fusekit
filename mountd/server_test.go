package mountd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
)

// testVersion pins OpHealth's version: the consumer's Server.Version, never fusekit's.
const testVersion = "v9.9.9 (test1234)"

type hostCall struct{ base, dir string }

// fakeHost stubs Host so the suite runs without fuse-t.
type fakeHost struct {
	mu        sync.Mutex
	setups    []hostCall
	specs     []fusekit.MountSpec // full specs passed to Setup, for wire-fidelity assertions
	teardowns []hostCall
	// teardownPolicies records each Teardown's carcassPolicy, parallel to teardowns.
	teardownPolicies []string
	live             map[string]bool
	// muxRootsHeld models MuxRootHolder: native mux roots the provider still holds
	// even without a registry row (a wedged last-child unmount's leftover).
	muxRootsHeld map[string]bool
	// Hooks run outside the lock so tests may block in them.
	setupFn    func(base, dir string) error
	teardownFn func(base, dir string) error
	mountedFn  func(dir string) bool
	aliveFn    func(base, dir string) bool
}

var (
	_ Host          = (*fakeHost)(nil)
	_ MuxRootHolder = (*fakeHost)(nil)
)

func (f *fakeHost) Setup(spec fusekit.MountSpec) error {
	base, dir := spec.Base, spec.Dir
	f.mu.Lock()
	f.setups = append(f.setups, hostCall{base, dir})
	f.specs = append(f.specs, spec)
	fn := f.setupFn
	f.mu.Unlock()
	if fn != nil {
		if err := fn(base, dir); err != nil {
			return err
		}
	}
	f.setLive(dir, true)
	return nil
}

func (f *fakeHost) Teardown(base, dir, carcassPolicy string) error {
	f.mu.Lock()
	f.teardowns = append(f.teardowns, hostCall{base, dir})
	f.teardownPolicies = append(f.teardownPolicies, carcassPolicy)
	fn := f.teardownFn
	f.mu.Unlock()
	if fn != nil {
		if err := fn(base, dir); err != nil {
			return err
		}
	}
	f.setLive(dir, false)
	return nil
}

func (f *fakeHost) State(base, dir string) (mounted, alive bool) {
	f.mu.Lock()
	mf, af := f.mountedFn, f.aliveFn
	f.mu.Unlock()
	if mf == nil && af == nil {
		live := f.isLive(dir)
		return live, live
	}
	if mf != nil {
		mounted = mf(dir)
	}
	if af != nil {
		alive = af(base, dir)
	}
	return mounted, alive
}

func (f *fakeHost) HoldsMuxRoot(root string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.muxRootsHeld[root]
}

func (f *fakeHost) setLive(dir string, live bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.live == nil {
		f.live = map[string]bool{}
	}
	if live {
		f.live[dir] = true
		return
	}
	delete(f.live, dir)
}

func (f *fakeHost) isLive(dir string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.live[dir]
}

func (f *fakeHost) calls() (setups, teardowns []hostCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]hostCall(nil), f.setups...), append([]hostCall(nil), f.teardowns...)
}

func (f *fakeHost) capturedTeardownPolicies() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.teardownPolicies...)
}

func (f *fakeHost) capturedSpecs() []fusekit.MountSpec {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]fusekit.MountSpec(nil), f.specs...)
}

func setState(f *fakeHost, mounted func(dir string) bool, alive func(base, dir string) bool) {
	f.mu.Lock()
	f.mountedFn, f.aliveFn = mounted, alive
	f.mu.Unlock()
}

// shortSockDir: macOS caps sun_path at 104 bytes and t.TempDir() paths exceed it.
func shortSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-mountd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// done is buffered so tests that never read it still let Run finish.
func startServerAt(t *testing.T, fake *fakeHost, socket string) (s *Server, cl *Client, done chan error, cancel context.CancelFunc) {
	t.Helper()
	return runServer(t, &Server{Socket: socket, Host: fake, Version: testVersion, Log: log.New(io.Discard, "", 0)})
}

// runServer runs a caller-built Server (journal tests set JournalPath) and
// waits until its socket answers.
func runServer(t *testing.T, s *Server) (out *Server, cl *Client, done chan error, cancel context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		done <- s.Run(ctx)
		close(stopped)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("holder did not stop on ctx cancel")
		}
	})
	cl = NewClient(s.Socket)
	waitAvailable(t, cl)
	return s, cl, done, cancel
}

func startServer(t *testing.T, fake *fakeHost) (s *Server, cl *Client, done chan error, cancel context.CancelFunc) {
	t.Helper()
	return startServerAt(t, fake, filepath.Join(shortSockDir(t), "m.sock"))
}

func waitAvailable(t *testing.T, cl *Client) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cl.Available() {
		if time.Now().After(deadline) {
			t.Fatal("holder socket never came up")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func newHandlerServer(f *fakeHost) *Server {
	s := &Server{Host: f, Version: testVersion, Log: log.New(io.Discard, "", 0)}
	s.initState()
	return s
}

// registryBases drops Epoch/MountedAt; TestListReportsEpochMountedAt pins them.
func registryBases(s *Server) map[string]string {
	bases := map[string]string{}
	for dir, row := range s.snapshotRegistry() {
		bases[dir] = row.Base
	}
	return bases
}

// registryMux projects each registry row to its MuxRoot (empty for a plain row),
// so mux tests pin the topology recorded per dir.
func registryMux(s *Server) map[string]string {
	mux := map[string]string{}
	for dir, row := range s.snapshotRegistry() {
		mux[dir] = row.MuxRoot
	}
	return mux
}

func TestHandleMount(t *testing.T) {
	const (
		base = "/pool/base"
		dir  = "/pool/acct-01"
	)
	tests := []struct {
		name        string
		base, dir   string
		seed        map[string]string // pre-existing registry rows
		inflight    []string          // dirs whose claim is already held
		mountedAt   map[string]bool   // State: dirs that look like mountpoints
		aliveAt     map[string]bool   // State: dirs whose mirror shows base's contents
		setupErr    error             // returned by the fake's Setup
		teardownErr error             // returned by the fake's Teardown
		wantOK      bool
		wantClass   string
		wantErr     string // required substring of Error when wantOK is false
		wantSetup   []hostCall
		wantTear    []hostCall
		wantReg     map[string]string
	}{
		{
			name: "fresh mount registers",
			base: base, dir: dir,
			wantOK:    true,
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{dir: base},
		},
		{
			name: "repeat mount of the same LIVE pair is idempotent and skips Setup",
			base: base, dir: dir,
			seed:      map[string]string{dir: base},
			mountedAt: map[string]bool{dir: true},
			aliveAt:   map[string]bool{dir: true},
			wantOK:    true,
			wantReg:   map[string]string{dir: base},
		},
		{
			name: "registered dir with a different base classifies base-mismatch",
			base: base, dir: dir,
			seed:      map[string]string{dir: "/pool/other"},
			wantOK:    false,
			wantClass: ClassBaseMismatch,
			wantErr:   "already mirrors",
			wantReg:   map[string]string{dir: "/pool/other"},
		},
		{
			// ensure-mounted: external umount left the registered mirror a non-mountpoint.
			name: "dead mirror (not a mountpoint) is torn down and remounted",
			base: base, dir: dir,
			seed:      map[string]string{dir: base},
			wantOK:    true,
			wantTear:  []hostCall{{base, dir}},
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{dir: base},
		},
		{
			// same recovery, wedged fuse daemon: still a mountpoint but base no longer shows through.
			name: "dead mirror (mountpoint, base not visible) is torn down and remounted",
			base: base, dir: dir,
			seed:      map[string]string{dir: base},
			mountedAt: map[string]bool{dir: true},
			wantOK:    true,
			wantTear:  []hostCall{{base, dir}},
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{dir: base},
		},
		{
			name: "dead mirror whose teardown wedges classifies wedged, deregisters, never re-Setups",
			base: base, dir: dir,
			seed:        map[string]string{dir: base},
			teardownErr: fmt.Errorf("%w: %s; refusing to treat it as torn down", fusekit.ErrUnmountWedged, dir),
			wantOK:      false,
			wantClass:   ClassWedged,
			wantErr:     "refusing to treat it as torn down",
			wantTear:    []hostCall{{base, dir}},
			wantReg:     map[string]string{},
		},
		{
			name: "dead mirror whose teardown fails plainly classifies mount-failed",
			base: base, dir: dir,
			seed:        map[string]string{dir: base},
			teardownErr: errors.New("umount: EBUSY"),
			wantOK:      false,
			wantClass:   ClassMountFailed,
			wantErr:     "EBUSY",
			wantTear:    []hostCall{{base, dir}},
			wantReg:     map[string]string{},
		},
		{
			name: "setup failure classifies mount-failed and does not register",
			base: base, dir: dir,
			setupErr:  errors.New("mount_fuset: exec format error"),
			wantOK:    false,
			wantClass: ClassMountFailed,
			wantErr:   "exec format error",
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{},
		},
		{
			name: "setup wrapping ErrMountNotLive classifies tcc and does not register",
			base: base, dir: dir,
			setupErr:  fmt.Errorf("%w: %s never became live; a one-time OS volume-access grant is pending", fusekit.ErrMountNotLive, dir),
			wantOK:    false,
			wantClass: ClassTCC,
			wantErr:   "one-time OS volume-access grant",
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{},
		},
		{
			name: "setup wrapping ErrMountTimeout classifies mount-timeout and does not register",
			base: base, dir: dir,
			setupErr:  fmt.Errorf("%w: %s after 8s; the OS grant is proven — transient fuse-t slowness, retrying", fusekit.ErrMountTimeout, dir),
			wantOK:    false,
			wantClass: ClassMountTimeout,
			wantErr:   "transient fuse-t slowness",
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{},
		},
		{
			// A content bridge unreachable at Build must classify content-unavailable
			// (retryable), never mount-failed — else a driver irreversibly demotes the account.
			name: "setup wrapping content.ErrBridgeUnavailable classifies content-unavailable and does not register",
			base: base, dir: dir,
			setupErr:  fmt.Errorf("holderfs: manifest for %s: %w", dir, content.ErrBridgeUnavailable),
			wantOK:    false,
			wantClass: ClassContentUnavailable,
			wantErr:   "bridge data socket not reachable",
			wantSetup: []hostCall{{base, dir}},
			wantReg:   map[string]string{},
		},
		{
			name: "foreign mountpoint is refused before Setup",
			base: base, dir: dir,
			mountedAt: map[string]bool{dir: true},
			wantOK:    false,
			wantClass: ClassForeignMount,
			wantErr:   "unmount it first",
			wantReg:   map[string]string{},
		},
		{
			name: "empty base refused",
			base: "", dir: dir,
			wantOK:  false,
			wantErr: "base and dir are required",
			wantReg: map[string]string{},
		},
		{
			name: "empty dir refused",
			base: base, dir: "",
			wantOK:  false,
			wantErr: "base and dir are required",
			wantReg: map[string]string{},
		},
		{
			name: "dir equal to base refused",
			base: base, dir: base,
			wantOK:  false,
			wantErr: "refusing dir == base",
			wantReg: map[string]string{},
		},
		{
			name: "in-flight dir is busy and never reaches the provider",
			base: base, dir: dir,
			inflight:  []string{dir},
			wantOK:    false,
			wantClass: ClassBusy,
			wantErr:   "busy",
			wantReg:   map[string]string{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mountedAt, aliveAt := tc.mountedAt, tc.aliveAt
			fake := &fakeHost{
				setupFn:    func(string, string) error { return tc.setupErr },
				teardownFn: func(string, string) error { return tc.teardownErr },
				mountedFn:  func(d string) bool { return mountedAt[d] },
				aliveFn:    func(_, d string) bool { return aliveAt[d] },
			}
			s := newHandlerServer(fake)
			for d, b := range tc.seed {
				s.registry[d] = mountRow{Base: b}
			}
			for _, d := range tc.inflight {
				s.inflight[d] = true
			}

			resp := s.dispatch(Request{Op: OpMount, Base: tc.base, Dir: tc.dir})

			assertResp(t, resp, tc.wantOK, tc.wantClass, tc.wantErr)
			setups, tears := fake.calls()
			if !reflect.DeepEqual(setups, tc.wantSetup) {
				t.Errorf("Setup calls = %v, want %v", setups, tc.wantSetup)
			}
			if !reflect.DeepEqual(tears, tc.wantTear) {
				t.Errorf("Teardown calls = %v, want %v", tears, tc.wantTear)
			}
			if got := registryBases(s); !reflect.DeepEqual(got, tc.wantReg) {
				t.Errorf("registry = %v, want %v", got, tc.wantReg)
			}
			assertClaimsReleased(t, s, len(tc.inflight))
		})
	}
}

// TestHandleMountTreeModeDirBaseRules pins tree mode's Base contract on the
// wire: Base is a NOMINAL identity key the served tree never reads, but it
// still keys the registry, teardown, and unmount — so a non-empty Base stays
// required and dir == base stays refused in EVERY mode. A tree mount over its
// own base would shadow the consumer's backing tree from the consumer itself
// and could never come down through OpUnmount (handleUnmount refuses
// dir == base); tree tenants mount at a dedicated dir.
func TestHandleMountTreeModeDirBaseRules(t *testing.T) {
	const (
		root = "/repo/notes"
		mnt  = "/mnt/notes"
	)
	tests := []struct {
		name      string
		mode      string
		base, dir string
		wantOK    bool
		wantErr   string
		wantSetup []hostCall
	}{
		{
			name: "tree mode mounts at a dedicated dir",
			mode: fusekit.ContentModeTree, base: root, dir: mnt,
			wantOK:    true,
			wantSetup: []hostCall{{root, mnt}},
		},
		{
			name: "tree mode refuses dir == base",
			mode: fusekit.ContentModeTree, base: root, dir: root,
			wantErr: "refusing dir == base",
		},
		{
			name: "source mode refuses dir == base",
			mode: fusekit.ContentModeSource, base: root, dir: root,
			wantErr: "refusing dir == base",
		},
		{
			name: "mode-less refuses dir == base",
			mode: "", base: root, dir: root,
			wantErr: "refusing dir == base",
		},
		{
			name: "tree mode still requires a base",
			mode: fusekit.ContentModeTree, base: "", dir: mnt,
			wantErr: "base and dir are required",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHost{}
			s := newHandlerServer(fake)
			resp := s.dispatch(Request{Op: OpMount, Base: tc.base, Dir: tc.dir, ContentMode: tc.mode})
			assertResp(t, resp, tc.wantOK, "", tc.wantErr)
			setups, _ := fake.calls()
			if !reflect.DeepEqual(setups, tc.wantSetup) {
				t.Errorf("Setup calls = %v, want %v", setups, tc.wantSetup)
			}
		})
	}
}

func TestHandleUnmount(t *testing.T) {
	const (
		base = "/pool/base"
		dir  = "/pool/acct-01"
	)
	tests := []struct {
		name        string
		base, dir   string
		seed        map[string]string
		inflight    []string
		mountedAt   map[string]bool
		teardownErr error
		wantOK      bool
		wantClass   string
		wantErr     string
		wantTear    []hostCall
		wantReg     map[string]string
	}{
		{
			name: "registered dir unmounts and deregisters",
			base: base, dir: dir,
			seed:     map[string]string{dir: base},
			wantOK:   true,
			wantTear: []hostCall{{base, dir}},
			wantReg:  map[string]string{},
		},
		{
			name: "registry base wins over the request base",
			base: "/pool/lies", dir: dir,
			seed:     map[string]string{dir: base},
			wantOK:   true,
			wantTear: []hostCall{{base, dir}},
			wantReg:  map[string]string{},
		},
		{
			name: "wedged teardown classifies wedged and STILL deregisters",
			base: base, dir: dir,
			seed:        map[string]string{dir: base},
			teardownErr: fmt.Errorf("%w: %s; refusing to treat it as torn down", fusekit.ErrUnmountWedged, dir),
			wantOK:      false,
			wantClass:   ClassWedged,
			wantErr:     "refusing to treat it as torn down",
			wantTear:    []hostCall{{base, dir}},
			wantReg:     map[string]string{},
		},
		{
			name: "plain teardown failure carries no class and still deregisters",
			base: base, dir: dir,
			seed:        map[string]string{dir: base},
			teardownErr: errors.New("umount: EBUSY"),
			wantOK:      false,
			wantErr:     "EBUSY",
			wantTear:    []hostCall{{base, dir}},
			wantReg:     map[string]string{},
		},
		{
			name: "unknown unmounted dir is an OK no-op without Teardown",
			base: base, dir: dir,
			wantOK:  true,
			wantReg: map[string]string{},
		},
		{
			name: "carcass: unknown mountpoint is torn down with the request base",
			base: base, dir: dir,
			mountedAt: map[string]bool{dir: true},
			wantOK:    true,
			wantTear:  []hostCall{{base, dir}},
			wantReg:   map[string]string{},
		},
		{
			name: "empty base refused even though the registry could supply it",
			base: "", dir: dir,
			seed:    map[string]string{dir: base},
			wantOK:  false,
			wantErr: "base and dir are required",
			wantReg: map[string]string{dir: base},
		},
		{
			name: "empty dir refused",
			base: base, dir: "",
			wantOK:  false,
			wantErr: "base and dir are required",
			wantReg: map[string]string{},
		},
		{
			name: "dir equal to base refused",
			base: base, dir: base,
			wantOK:  false,
			wantErr: "refusing dir == base",
			wantReg: map[string]string{},
		},
		{
			name: "in-flight dir is busy and stays registered",
			base: base, dir: dir,
			seed:      map[string]string{dir: base},
			inflight:  []string{dir},
			wantOK:    false,
			wantClass: ClassBusy,
			wantErr:   "busy",
			wantReg:   map[string]string{dir: base},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mountedAt := tc.mountedAt
			fake := &fakeHost{
				teardownFn: func(string, string) error { return tc.teardownErr },
				mountedFn:  func(d string) bool { return mountedAt[d] },
				aliveFn:    func(string, string) bool { return false },
			}
			s := newHandlerServer(fake)
			for d, b := range tc.seed {
				s.registry[d] = mountRow{Base: b}
			}
			for _, d := range tc.inflight {
				s.inflight[d] = true
			}

			resp := s.dispatch(Request{Op: OpUnmount, Base: tc.base, Dir: tc.dir})

			assertResp(t, resp, tc.wantOK, tc.wantClass, tc.wantErr)
			if _, tears := fake.calls(); !reflect.DeepEqual(tears, tc.wantTear) {
				t.Errorf("Teardown calls = %v, want %v", tears, tc.wantTear)
			}
			if got := registryBases(s); !reflect.DeepEqual(got, tc.wantReg) {
				t.Errorf("registry = %v, want %v", got, tc.wantReg)
			}
			assertClaimsReleased(t, s, len(tc.inflight))
		})
	}
}

// assertResp checks the OK/ErrClass/Error triple of one response. Failing
// cases must pin an error substring so a wrong-reason failure cannot pass.
func assertResp(t *testing.T, resp Response, wantOK bool, wantClass, wantErr string) {
	t.Helper()
	if resp.OK != wantOK {
		t.Fatalf("OK = %v (error %q), want %v", resp.OK, resp.Error, wantOK)
	}
	if resp.ErrClass != wantClass {
		t.Errorf("ErrClass = %q, want %q", resp.ErrClass, wantClass)
	}
	if wantOK {
		if resp.Error != "" {
			t.Errorf("Error = %q on an OK response", resp.Error)
		}
		return
	}
	if wantErr == "" {
		t.Fatal("test bug: a failing case must pin an error substring")
	}
	if !strings.Contains(resp.Error, wantErr) {
		t.Errorf("Error = %q, want substring %q", resp.Error, wantErr)
	}
}

// assertClaimsReleased verifies a handler returned its in-flight claim; only
// the claims the test itself seeded may remain.
func assertClaimsReleased(t *testing.T, s *Server, seeded int) {
	t.Helper()
	s.mu.Lock()
	held := len(s.inflight)
	s.mu.Unlock()
	if held != seeded {
		t.Errorf("in-flight gate leaked: %d claims held, want %d", held, seeded)
	}
}

func TestHandleList(t *testing.T) {
	t.Run("Live needs BOTH the mountpoint and base visibility, sorted by dir", func(t *testing.T) {
		fake := &fakeHost{}
		s := newHandlerServer(fake)
		s.registry["/pool/acct-01"] = mountRow{Base: "/pool/base"}
		s.registry["/pool/acct-02"] = mountRow{Base: "/pool/base"}
		s.registry["/pool/acct-03"] = mountRow{Base: "/pool/base"}
		// acct-02 is mounted-not-alive, acct-03 alive-not-mounted (its underlying
		// dir shadows base's entries): Live needs BOTH halves, so either alone reads
		// dead — a false Live would permanently mask a dead mirror from remount.
		setState(fake,
			func(dir string) bool {
				return dir == "/pool/acct-01" || dir == "/pool/acct-02"
			},
			func(base, dir string) bool {
				return base == "/pool/base" && (dir == "/pool/acct-01" || dir == "/pool/acct-03")
			})
		resp := s.dispatch(Request{Op: OpList})
		if !resp.OK {
			t.Fatalf("list failed: %q", resp.Error)
		}
		want := []MountInfo{
			{Dir: "/pool/acct-01", Base: "/pool/base", Live: true},
			{Dir: "/pool/acct-02", Base: "/pool/base", Live: false},
			{Dir: "/pool/acct-03", Base: "/pool/base", Live: false},
		}
		if !reflect.DeepEqual(resp.Mounts, want) {
			t.Fatalf("list = %+v, want %+v", resp.Mounts, want)
		}
	})
	t.Run("empty registry lists nothing", func(t *testing.T) {
		resp := newHandlerServer(&fakeHost{}).dispatch(Request{Op: OpList})
		if !resp.OK || len(resp.Mounts) != 0 {
			t.Fatalf("list = %+v (ok %v), want empty OK", resp.Mounts, resp.OK)
		}
	})
}

// shrinkLiveProbeTimeout shortens the liveness probe bound for one test (mutates a package global — no t.Parallel).
func shrinkLiveProbeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := liveProbeTimeout
	liveProbeTimeout = d
	t.Cleanup(func() { liveProbeTimeout = prev })
}

// releaseProbes unblocks and drains in-flight liveness probes so a blocked aliveFn
// does not leak a goroutine reading the test-owned fake past the test.
func releaseProbes(t *testing.T, block chan struct{}) {
	t.Helper()
	close(block)
	deadline := time.Now().Add(5 * time.Second)
	for liveProbes.Inflight() > 0 {
		if time.Now().After(deadline) {
			t.Error("in-flight liveness probes never drained")
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestHandleListWedgedMirrorBounded pins the bounded liveness probe: fuse-t's
// NFS backend has no timeout knobs, so a wedged mirror's stats block forever.
// Unbounded, one wedged mirror would blow the client's List deadline and
// un-vouch EVERY fuse account pool-wide; a second List joins the stuck probe
// instead of stacking a goroutine per refresh.
func TestHandleListWedgedMirrorBounded(t *testing.T) {
	shrinkLiveProbeTimeout(t, 100*time.Millisecond)
	fake := &fakeHost{}
	s := newHandlerServer(fake)
	s.registry["/pool/acct-01"] = mountRow{Base: "/pool/base"}
	s.registry["/pool/acct-02"] = mountRow{Base: "/pool/base"}

	block := make(chan struct{})
	var wedgedStats atomic.Int32
	setState(fake,
		func(string) bool { return true },
		func(_, dir string) bool {
			if dir == "/pool/acct-01" {
				wedgedStats.Add(1)
				<-block // the wedged mirror: this stat never returns
			}
			return true
		})
	t.Cleanup(func() { releaseProbes(t, block) })

	want := []MountInfo{
		{Dir: "/pool/acct-01", Base: "/pool/base", Live: false},
		{Dir: "/pool/acct-02", Base: "/pool/base", Live: true},
	}
	start := time.Now()
	resp := s.dispatch(Request{Op: OpList})
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("list took %s, want bounded by the probe timeout", elapsed)
	}
	if !resp.OK {
		t.Fatalf("list failed: %q", resp.Error)
	}
	if !reflect.DeepEqual(resp.Mounts, want) {
		t.Fatalf("list = %+v, want %+v", resp.Mounts, want)
	}

	// Second List joins the still-stuck probe (one wedged stat total), entry still dead.
	resp = s.dispatch(Request{Op: OpList})
	if !resp.OK || !reflect.DeepEqual(resp.Mounts, want) {
		t.Fatalf("second list = %+v (ok %v), want %+v", resp.Mounts, resp.OK, want)
	}
	if got := wedgedStats.Load(); got != 1 {
		t.Errorf("wedged dir probed %d times, want 1 (joiners must not stack stuck goroutines)", got)
	}
}

// TestHandleMountWedgedRegisteredMirrorRemounted pins the same bound on
// handleMount's idempotency check: a registered mirror whose liveness stats
// wedge reads dead within the bound and takes the recovery (bounded forced
// teardown, then remount) instead of hanging past the op deadline.
func TestHandleMountWedgedRegisteredMirrorRemounted(t *testing.T) {
	shrinkLiveProbeTimeout(t, 100*time.Millisecond)
	fake := &fakeHost{}
	s := newHandlerServer(fake)
	s.registry["/pool/acct-01"] = mountRow{Base: "/pool/base"}

	block := make(chan struct{})
	setState(fake,
		func(string) bool { return true },
		func(string, string) bool { <-block; return true })
	t.Cleanup(func() { releaseProbes(t, block) })

	resp := s.dispatch(Request{Op: OpMount, Base: "/pool/base", Dir: "/pool/acct-01"})
	if !resp.OK {
		t.Fatalf("mount over a wedged registered mirror = %+v, want the teardown+remount recovery", resp)
	}
	setups, tears := fake.calls()
	if !reflect.DeepEqual(tears, []hostCall{{"/pool/base", "/pool/acct-01"}}) {
		t.Errorf("Teardown calls = %v, want the wedged mirror torn down", tears)
	}
	if !reflect.DeepEqual(setups, []hostCall{{"/pool/base", "/pool/acct-01"}}) {
		t.Errorf("Setup calls = %v, want the mirror remounted", setups)
	}
	assertClaimsReleased(t, s, 0)
}

func TestHandleHealthAndProbe(t *testing.T) {
	s := newHandlerServer(&fakeHost{})

	health := s.dispatch(Request{Op: OpHealth})
	if !health.OK || health.Version != testVersion {
		t.Fatalf("health = %+v, want OK with version %q", health, testVersion)
	}

	if resp := s.dispatch(Request{Op: OpProbe}); !resp.OK || resp.FuseOK {
		t.Fatalf("probe with nil Probe = %+v, want OK with FuseOK=false", resp)
	}
	s.Probe = func() (bool, error) { return true, nil }
	if resp := s.dispatch(Request{Op: OpProbe}); !resp.OK || !resp.FuseOK || resp.ErrClass != "" {
		t.Fatalf("probe = %+v, want FuseOK=true and no ErrClass", resp)
	}
	s.Probe = func() (bool, error) { return false, nil }
	if resp := s.dispatch(Request{Op: OpProbe}); !resp.OK || resp.FuseOK || resp.ErrClass != "" {
		t.Fatalf("probe = %+v, want FuseOK=false and no ErrClass", resp)
	}

	// A failing probe carries the mount's classification so the driver distinguishes
	// a hard ErrMountFailed (fuse unavailable here) from a pending ErrMountNotLive
	// (the grant may still land); the RPC still succeeds (OK=true, FuseOK=false).
	s.Probe = func() (bool, error) { return false, fmt.Errorf("rejected: %w", fusekit.ErrMountFailed) }
	if resp := s.dispatch(Request{Op: OpProbe}); !resp.OK || resp.FuseOK || resp.ErrClass != ClassMountFailed {
		t.Fatalf("probe (hard failure) = %+v, want OK, FuseOK=false, ErrClass=%q", resp, ClassMountFailed)
	}
	s.Probe = func() (bool, error) { return false, fmt.Errorf("pending: %w", fusekit.ErrMountNotLive) }
	if resp := s.dispatch(Request{Op: OpProbe}); !resp.OK || resp.FuseOK || resp.ErrClass != ClassTCC {
		t.Fatalf("probe (pending grant) = %+v, want OK, FuseOK=false, ErrClass=%q", resp, ClassTCC)
	}
}

// TestServerMountUnmountHappyPath drives the holder end-to-end over a real unix
// socket: mount, idempotent repeat (no second Setup), list live, unmount, clean shutdown.
func TestServerMountUnmountHappyPath(t *testing.T) {
	fake := &fakeHost{}
	_, cl, done, _ := startServer(t, fake)

	root := t.TempDir()
	base := filepath.Join(root, "base")
	dir := filepath.Join(root, "acct-01")

	before := time.Now().Unix()
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("mount: %v", err)
	}
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("repeat mount should be idempotent OK, got %v", err)
	}
	if setups, _ := fake.calls(); !reflect.DeepEqual(setups, []hostCall{{base, dir}}) {
		t.Fatalf("Setup calls = %v, want exactly one for %s (repeat mount of a live pair must not re-Setup)", setups, dir)
	}

	mounts, err := cl.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(mounts) != 1 {
		t.Fatalf("list = %+v, want one entry", mounts)
	}
	m := mounts[0]
	if m.Dir != dir || m.Base != base || !m.Live {
		t.Fatalf("list entry = %+v, want live %s <- %s", m, dir, base)
	}
	if m.Epoch != 1 {
		t.Errorf("Epoch = %d, want 1 for the holder's first mount of %s", m.Epoch, dir)
	}
	if m.MountedAt < before || m.MountedAt > time.Now().Unix() {
		t.Errorf("MountedAt = %d, want within [%d, now]", m.MountedAt, before)
	}

	if err := cl.Unmount(base, dir); err != nil {
		t.Fatalf("unmount: %v", err)
	}
	if _, tears := fake.calls(); !reflect.DeepEqual(tears, []hostCall{{base, dir}}) {
		t.Fatalf("Teardown calls = %v, want exactly one for %s", tears, dir)
	}
	if mounts, err := cl.List(); err != nil || len(mounts) != 0 {
		t.Fatalf("list after unmount = %+v (err %v), want empty", mounts, err)
	}

	failed, err := cl.Shutdown()
	if err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if len(failed) != 0 {
		t.Fatalf("shutdown reported failed dirs %+v, want none", failed)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after OpShutdown")
	}
	if !cl.WaitGone(2 * time.Second) {
		t.Fatal("socket still live after shutdown")
	}
}

func TestShutdownReportsFailedDirs(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "base")
	dirA := filepath.Join(root, "acct-01")
	dirB := filepath.Join(root, "acct-02")

	fake := &fakeHost{teardownFn: func(_, dir string) error {
		if dir == dirA {
			return fmt.Errorf("%w: %s; refusing to treat it as torn down", fusekit.ErrUnmountWedged, dir)
		}
		return nil
	}}
	_, cl, done, _ := startServer(t, fake)

	if err := cl.Mount(base, dirA); err != nil {
		t.Fatalf("mount A: %v", err)
	}
	if err := cl.Mount(base, dirB); err != nil {
		t.Fatalf("mount B: %v", err)
	}

	failed, err := cl.Shutdown()
	if err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if want := []MountInfo{{Dir: dirA, Base: base, Live: true}}; !reflect.DeepEqual(failed, want) {
		t.Fatalf("shutdown failed dirs = %+v, want %+v", failed, want)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after OpShutdown")
	}
	// The wedged dir keeps its row, so the post-drain sweep retries it — once,
	// gracefully; the clean dir is swept exactly once.
	if _, tears := fake.calls(); !reflect.DeepEqual(tears, []hostCall{{base, dirA}, {base, dirB}, {base, dirA}}) {
		t.Fatalf("Teardown calls = %v, want the wedged dir retried by the post-drain sweep", tears)
	}
	if !cl.WaitGone(2 * time.Second) {
		t.Fatal("socket still live after shutdown")
	}
}

// TestRunCtxCancelSweepsMounts is the SIGTERM-equivalent path: signal.NotifyContext
// wraps Run's ctx, so cancelling it drives the same exit — drain, unmount all, release socket.
func TestRunCtxCancelSweepsMounts(t *testing.T) {
	fake := &fakeHost{}
	_, cl, done, cancel := startServer(t, fake)

	root := t.TempDir()
	base := filepath.Join(root, "base")
	dir := filepath.Join(root, "acct-01")
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("mount: %v", err)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not exit on ctx cancel")
	}
	if _, tears := fake.calls(); !reflect.DeepEqual(tears, []hostCall{{base, dir}}) {
		t.Fatalf("ctx cancel must sweep mounts down; Teardown calls = %v", tears)
	}
	if !cl.WaitGone(2 * time.Second) {
		t.Fatal("socket still live after ctx-cancel sweep")
	}
}

func TestSecondRunRefusedAgainstLiveHolder(t *testing.T) {
	a, cl, _, _ := startServer(t, &fakeHost{})

	b := &Server{Socket: a.Socket, Host: &fakeHost{}, Version: testVersion, Log: log.New(io.Discard, "", 0)}
	err := b.Run(context.Background())
	if err == nil {
		t.Fatal("second holder must refuse to start against a live socket")
	}
	if !strings.Contains(err.Error(), "refusing to start") {
		t.Fatalf("refusal error = %q, want it to say it is refusing to start", err)
	}
	if !strings.Contains(err.Error(), testVersion) {
		t.Fatalf("refusal error = %q, want it to name the live holder's version %q", err, testVersion)
	}

	// The loser must not have disturbed the winner: socket intact, still serving.
	if ver, herr := cl.Health(); herr != nil || ver != testVersion {
		t.Fatalf("first holder unhealthy after refused start: version %q, err %v", ver, herr)
	}
}

func TestStaleSocketRemovedAndRebound(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")

	// Manufacture a stale socket: bind, keep the file on close.
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(socket); err != nil {
		t.Fatalf("precondition: stale socket file should remain after close: %v", err)
	}

	_, cl, _, _ := startServerAt(t, &fakeHost{}, socket)
	if ver, err := cl.Health(); err != nil || ver != testVersion {
		t.Fatalf("holder over a reclaimed stale socket: version %q, err %v", ver, err)
	}
}

// TestRunRefusedWhileLockHeld pins the flock that closes the start race: a holder
// that cannot take Socket+".lock" must refuse WITHOUT touching the socket path —
// its os.Remove on a believed-stale (actually live) socket is the hazard the lock prevents.
func TestRunRefusedWhileLockHeld(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	lock, err := os.OpenFile(socket+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	// flock contends between two open file descriptions even within one process, so
	// holding it here stands in for a racing holder that won the lock but has not bound.
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	s := &Server{Socket: socket, Host: &fakeHost{}, Version: testVersion, Log: log.New(io.Discard, "", 0)}
	runErr := s.Run(context.Background())
	if runErr == nil || !strings.Contains(runErr.Error(), "refusing to start") {
		t.Fatalf("Run with the holder lock held = %v, want a refusing-to-start error", runErr)
	}
	if _, statErr := os.Stat(socket); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("a losing holder must not create (or have removed) the socket; stat err = %v", statErr)
	}
}

// TestCrashedHolderLockAndSocketReclaimed: a crash leaves both the lock and socket
// files behind, but the flock died with the process, so a fresh holder reclaims both.
func TestCrashedHolderLockAndSocketReclaimed(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	if err := os.WriteFile(socket+".lock", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ln.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	_, cl, _, _ := startServerAt(t, &fakeHost{}, socket)
	if ver, err := cl.Health(); err != nil || ver != testVersion {
		t.Fatalf("holder over a crashed holder's leavings: version %q, err %v", ver, err)
	}
}

func TestRunNilHostRefused(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "m.sock")
	s := &Server{Socket: socket}
	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "cannot host fuse mounts") {
		t.Fatalf("Run with nil Host = %v, want a loud cannot-host refusal", err)
	}
	if _, statErr := os.Stat(socket); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("nil-Host Run must not create the socket; stat err = %v", statErr)
	}
}

func TestConcurrentSameDirMountsSerialize(t *testing.T) {
	fake := &fakeHost{}
	entered := make(chan string, 2)
	release := make(chan struct{})
	fake.setupFn = func(_, dir string) error {
		entered <- dir
		<-release
		return nil
	}
	_, cl, _, _ := startServer(t, fake)

	root := t.TempDir()
	base := filepath.Join(root, "base")
	dir := filepath.Join(root, "acct-01")

	first := make(chan error, 1)
	go func() { first <- cl.Mount(base, dir) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first mount never reached Setup")
	}

	// The first mount is parked inside Setup holding the dir's claim, so a
	// second same-dir mount must bounce as busy without reaching the provider.
	err := cl.Mount(base, dir)
	if err == nil {
		t.Fatal("same-dir mount during an in-flight mount must be refused busy")
	}
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("second mount err = %v, want errors.Is ErrBusy", err)
	}
	if !strings.Contains(err.Error(), "busy") {
		t.Fatalf("second mount err = %v, want a busy refusal", err)
	}
	if errors.Is(err, ErrMountFailed) || errors.Is(err, ErrTCCDenied) || errors.Is(err, ErrForeignMount) {
		t.Fatalf("busy must not carry a failure class: %v", err)
	}

	close(release)
	if err := <-first; err != nil {
		t.Fatalf("first mount: %v", err)
	}
	if setups, _ := fake.calls(); len(setups) != 1 {
		t.Fatalf("Setup ran %d times, want exactly 1 — the busy op must never reach the provider", len(setups))
	}
	// The claim is back: the same mount now lands on the idempotent path.
	if err := cl.Mount(base, dir); err != nil {
		t.Fatalf("mount after claim release: %v", err)
	}
	if setups, _ := fake.calls(); len(setups) != 1 {
		t.Fatalf("Setup ran %d times after idempotent re-mount, want still 1", len(setups))
	}
}

func TestConcurrentDifferentDirMountsProceed(t *testing.T) {
	fake := &fakeHost{}
	entered := make(chan string, 2)
	release := make(chan struct{})
	fake.setupFn = func(_, dir string) error {
		entered <- dir
		<-release
		return nil
	}
	_, cl, _, _ := startServer(t, fake)

	root := t.TempDir()
	base := filepath.Join(root, "base")
	dirA := filepath.Join(root, "acct-01")
	dirB := filepath.Join(root, "acct-02")

	errs := make(chan error, 2)
	go func() { errs <- cl.Mount(base, dirA) }()
	go func() { errs <- cl.Mount(base, dirB) }()

	// Neither Setup has been released, so both entering proves the dirs mount
	// concurrently; a serialized holder would never produce the second entry.
	inFlight := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case d := <-entered:
			inFlight[d] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only %v reached Setup; different dirs must mount concurrently", inFlight)
		}
	}
	if !inFlight[dirA] || !inFlight[dirB] {
		t.Fatalf("in-flight Setups = %v, want both %s and %s", inFlight, dirA, dirB)
	}
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("mount: %v", err)
		}
	}
	mounts, err := cl.List()
	if err != nil || len(mounts) != 2 {
		t.Fatalf("list = %+v (err %v), want both mounts registered", mounts, err)
	}
}

func TestBadRequestsOverTheWire(t *testing.T) {
	_, cl, _, _ := startServer(t, &fakeHost{})

	t.Run("malformed JSON gets an error response, not a hangup", func(t *testing.T) {
		conn, err := net.DialTimeout("unix", cl.Socket, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := io.WriteString(conn, "{this is not json}\n"); err != nil {
			t.Fatal(err)
		}
		var resp Response
		if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
			t.Fatalf("no response to malformed JSON: %v", err)
		}
		if resp.OK {
			t.Fatal("malformed JSON must not be OK")
		}
		if !strings.Contains(resp.Error, "bad request") {
			t.Errorf("Error = %q, want a bad-request message", resp.Error)
		}
		if resp.Proto != MountProtoVersion {
			t.Errorf("Proto = %d, want %d on every response", resp.Proto, MountProtoVersion)
		}
	})

	t.Run("unknown op reads as not-supported, never as holder failure", func(t *testing.T) {
		resp, err := cl.do(Request{Op: Op("balance-quota")}, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if resp.OK {
			t.Fatal("unknown op must not be OK")
		}
		// Drivers detect not-supported by this exact prefix, so it is part of the frozen wire surface.
		if resp.Error != "unknown op: balance-quota" {
			t.Errorf("Error = %q, want %q", resp.Error, "unknown op: balance-quota")
		}
		if resp.ErrClass != "" {
			t.Errorf("unknown op must not carry an error class, got %q", resp.ErrClass)
		}
		if resp.Proto != MountProtoVersion {
			t.Errorf("Proto = %d, want %d on every response", resp.Proto, MountProtoVersion)
		}
	})
}

func listOne(t *testing.T, s *Server) MountInfo {
	t.Helper()
	resp := s.dispatch(Request{Op: OpList})
	if !resp.OK || len(resp.Mounts) != 1 {
		t.Fatalf("list = %+v, want OK with one entry", resp)
	}
	return resp.Mounts[0]
}

// TestListReportsEpochMountedAt pins the additive List fields: Epoch starts at
// 1, bumps on remount, and MountedAt stamps the current mount.
func TestListReportsEpochMountedAt(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{}
	s := newHandlerServer(fake)

	before := time.Now().Unix()
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir}); !resp.OK {
		t.Fatalf("mount: %+v", resp)
	}
	m := listOne(t, s)
	if !m.Live {
		t.Errorf("list entry = %+v, want live", m)
	}
	if m.Epoch != 1 {
		t.Errorf("Epoch = %d, want 1 for the holder's first mount", m.Epoch)
	}
	if m.MountedAt < before || m.MountedAt > time.Now().Unix() {
		t.Errorf("MountedAt = %d, want within [%d, now]", m.MountedAt, before)
	}
	first := m.MountedAt

	// Kill the mirror so the ensure-mounted path remounts it: the epoch must
	// bump and MountedAt must restamp.
	fake.setLive(dir, false)
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir}); !resp.OK {
		t.Fatalf("remount: %+v", resp)
	}
	m = listOne(t, s)
	if m.Epoch != 2 {
		t.Errorf("Epoch after remount = %d, want 2", m.Epoch)
	}
	if m.MountedAt < first || m.MountedAt > time.Now().Unix() {
		t.Errorf("MountedAt after remount = %d, want within [%d, now]", m.MountedAt, first)
	}
	if !m.Live {
		t.Errorf("list entry after remount = %+v, want live", m)
	}
}

// TestAddMountCarriesAttrCacheOverWire drives AddMount end to end over a real
// socket and asserts the per-mount attr-cache knobs survive the client→wire→
// server→MountSpec path intact: present values reach the holder-side Setup
// spec, and an absent (default) spec decodes to false/zero — exactly today's
// noattrcache behavior — proving the additive field is backward-safe.
func TestAddMountCarriesAttrCacheOverWire(t *testing.T) {
	tests := []struct {
		name            string
		attrCache       bool
		timeout         time.Duration
		wantAttrCache   bool
		wantAttrTimeout time.Duration
	}{
		{name: "absent decodes to false/zero (today's behavior)", attrCache: false, timeout: 0, wantAttrCache: false, wantAttrTimeout: 0},
		{name: "present survives to the holder-side MountSpec", attrCache: true, timeout: 30 * time.Second, wantAttrCache: true, wantAttrTimeout: 30 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHost{}
			_, cl, _, _ := startServer(t, fake)
			root := t.TempDir()
			base := filepath.Join(root, "base")
			dir := filepath.Join(root, "acct-01")
			if err := cl.AddMount(fusekit.MountSpec{
				Base: base, Dir: dir,
				AttrCache:        tc.attrCache,
				AttrCacheTimeout: tc.timeout,
			}); err != nil {
				t.Fatalf("AddMount: %v", err)
			}
			specs := fake.capturedSpecs()
			if len(specs) != 1 {
				t.Fatalf("Setup specs = %d, want exactly 1", len(specs))
			}
			if specs[0].AttrCache != tc.wantAttrCache {
				t.Errorf("holder-side AttrCache = %v, want %v", specs[0].AttrCache, tc.wantAttrCache)
			}
			if specs[0].AttrCacheTimeout != tc.wantAttrTimeout {
				t.Errorf("holder-side AttrCacheTimeout = %v, want %v", specs[0].AttrCacheTimeout, tc.wantAttrTimeout)
			}
		})
	}
}

// TestRequestAttrCacheOmitemptyContract pins the additive-wire invariant: a
// default Request omits both attr-cache fields, so an OLD holder that predates
// them sees byte-identical JSON and its absent-field decode is false/zero
// (today's noattrcache). A present request round-trips through mountSpec.
func TestRequestAttrCacheOmitemptyContract(t *testing.T) {
	dflt, err := json.Marshal(Request{Op: OpMount, Base: "/b", Dir: "/d"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(dflt), "attr_cache") {
		t.Errorf("default Request JSON = %s, want no attr_cache fields (old holders must see identical bytes)", dflt)
	}

	var absent Request
	if err := json.Unmarshal([]byte(`{"proto":1,"op":"mount","base":"/b","dir":"/d"}`), &absent); err != nil {
		t.Fatal(err)
	}
	if spec := mountSpec(absent); spec.AttrCache || spec.AttrCacheTimeout != 0 {
		t.Errorf("absent fields decoded to AttrCache=%v timeout=%v, want false/0", spec.AttrCache, spec.AttrCacheTimeout)
	}

	raw, err := json.Marshal(Request{Op: OpMount, Base: "/b", Dir: "/d", AttrCache: true, AttrCacheTimeout: 45 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var got Request
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if spec := mountSpec(got); !spec.AttrCache || spec.AttrCacheTimeout != 45*time.Second {
		t.Errorf("round-tripped MountSpec = {AttrCache:%v Timeout:%v}, want {true 45s}", spec.AttrCache, spec.AttrCacheTimeout)
	}
}

// TestRequestMuxRootOmitempty pins the additive-wire invariant for MuxRoot: a
// default Request omits it (old holders see byte-identical JSON and serve a
// plain per-dir mount), and a present value round-trips through mountSpec.
func TestRequestMuxRootOmitempty(t *testing.T) {
	dflt, err := json.Marshal(Request{Op: OpMount, Base: "/b", Dir: "/d"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(dflt), "mux_root") {
		t.Errorf("default Request JSON = %s, want no mux_root (old holders must see identical bytes)", dflt)
	}
	raw, err := json.Marshal(Request{Op: OpMount, Base: "/b", Dir: "/mnt/d", MuxRoot: "/mnt"})
	if err != nil {
		t.Fatal(err)
	}
	var got Request
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if spec := mountSpec(got); spec.MuxRoot != "/mnt" {
		t.Errorf("round-tripped MountSpec.MuxRoot = %q, want /mnt", spec.MuxRoot)
	}
}

// TestHandleMountMux pins the mux dispatch surface at the handler level: static
// shape validation, plain/mux collisions, the registered-topology mismatch, the
// first-attach-only foreign-root probe, idempotency, and dead-subtree recovery.
// The row records its MuxRoot for the per-root claim and the collision checks.
func TestHandleMountMux(t *testing.T) {
	const (
		base = "/pool/base"        // shared base (~/.claude)
		root = "/pool/mnt"         // mux native root
		a1   = "/pool/mnt/acct-01" // subtree
		a2   = "/pool/mnt/acct-02" // sibling subtree
	)
	tests := []struct {
		name       string
		base, dir  string
		muxRoot    string
		seed       map[string]mountRow
		mountedAt  map[string]bool
		aliveAt    map[string]bool
		holdsRoots []string // native mux roots the provider still holds (no row)
		wantOK     bool
		wantClass  string
		wantErr    string
		wantSetup  []hostCall
		wantTear   []hostCall
		wantReg    map[string]string // dir -> row.MuxRoot after dispatch
	}{
		{
			name: "fresh mux subtree registers under its root",
			base: base, dir: a1, muxRoot: root,
			wantOK:    true,
			wantSetup: []hostCall{{base, a1}},
			wantReg:   map[string]string{a1: root},
		},
		{
			name: "non-absolute mux root refused (malformed, no class)",
			base: base, dir: a1, muxRoot: "relative/mnt",
			wantErr: "must be absolute",
			wantReg: map[string]string{},
		},
		{
			name: "dir not a direct child of the root refused",
			base: base, dir: "/pool/mnt/sub/acct", muxRoot: root,
			wantErr: "direct child",
			wantReg: map[string]string{},
		},
		{
			name: "mux root equal to base refused",
			base: base, dir: "/pool/base/acct", muxRoot: base,
			wantErr: "must not be the base",
			wantReg: map[string]string{},
		},
		{
			name: "mux root under base refused",
			base: base, dir: "/pool/base/sub/acct", muxRoot: "/pool/base/sub",
			wantErr: "under it",
			wantReg: map[string]string{},
		},
		{
			name: "plain mount over a registered mux root is a mismatch",
			base: base, dir: root, muxRoot: "",
			seed:      map[string]mountRow{a1: {Base: base, MuxRoot: root}},
			wantOK:    false,
			wantClass: ClassMuxMismatch,
			wantErr:   "serves mux subtrees",
			wantReg:   map[string]string{a1: root},
		},
		{
			name: "mux mount whose root is a registered plain mount is a mismatch",
			base: base, dir: a1, muxRoot: root,
			seed:      map[string]mountRow{root: {Base: base}},
			wantOK:    false,
			wantClass: ClassMuxMismatch,
			wantErr:   "already a plain mount",
			wantReg:   map[string]string{root: ""},
		},
		{
			name: "registered plain dir re-requested as mux is a mismatch",
			base: base, dir: a1, muxRoot: root,
			seed:      map[string]mountRow{a1: {Base: base}},
			wantOK:    false,
			wantClass: ClassMuxMismatch,
			wantErr:   "registered as a plain mount",
			wantReg:   map[string]string{a1: ""},
		},
		{
			name: "first attach over a foreign root mountpoint is refused",
			base: base, dir: a1, muxRoot: root,
			mountedAt: map[string]bool{root: true},
			wantOK:    false,
			wantClass: ClassForeignMount,
			wantErr:   "mux root",
			wantReg:   map[string]string{},
		},
		{
			name: "later tenant skips the foreign-root probe (the root is ours)",
			base: base, dir: a2, muxRoot: root,
			seed:      map[string]mountRow{a1: {Base: base, MuxRoot: root}},
			mountedAt: map[string]bool{root: true}, // mounted, but by us
			wantOK:    true,
			wantSetup: []hostCall{{base, a2}},
			wantReg:   map[string]string{a1: root, a2: root},
		},
		{
			name: "idempotent live mux subtree skips Setup",
			base: base, dir: a1, muxRoot: root,
			seed:      map[string]mountRow{a1: {Base: base, MuxRoot: root}},
			mountedAt: map[string]bool{a1: true},
			aliveAt:   map[string]bool{a1: true},
			wantOK:    true,
			wantReg:   map[string]string{a1: root},
		},
		{
			name: "dead mux subtree is detached and re-attached",
			base: base, dir: a1, muxRoot: root,
			seed:      map[string]mountRow{a1: {Base: base, MuxRoot: root}},
			wantOK:    true,
			wantTear:  []hostCall{{base, a1}},
			wantSetup: []hostCall{{base, a1}},
			wantReg:   map[string]string{a1: root},
		},
		{
			// A wedged last-child unmount deregistered the row but the provider still
			// holds the native mount (still a mountpoint). A later tenant must re-attach
			// to the surviving root via the MuxRootHolder capability — NOT bounce
			// ClassForeignMount over a root this holder still owns.
			name: "held wedged root re-attaches without a foreign-root probe",
			base: base, dir: a1, muxRoot: root,
			holdsRoots: []string{root},
			mountedAt:  map[string]bool{root: true}, // still a mountpoint, but ours
			wantOK:     true,
			wantSetup:  []hostCall{{base, a1}},
			wantReg:    map[string]string{a1: root},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mountedAt, aliveAt := tc.mountedAt, tc.aliveAt
			fake := &fakeHost{
				mountedFn: func(d string) bool { return mountedAt[d] },
				aliveFn:   func(_, d string) bool { return aliveAt[d] },
			}
			if len(tc.holdsRoots) > 0 {
				fake.muxRootsHeld = map[string]bool{}
				for _, r := range tc.holdsRoots {
					fake.muxRootsHeld[r] = true
				}
			}
			s := newHandlerServer(fake)
			for d, row := range tc.seed {
				s.registry[d] = row
			}

			resp := s.dispatch(Request{Op: OpMount, Base: tc.base, Dir: tc.dir, MuxRoot: tc.muxRoot})

			assertResp(t, resp, tc.wantOK, tc.wantClass, tc.wantErr)
			setups, tears := fake.calls()
			if !reflect.DeepEqual(setups, tc.wantSetup) {
				t.Errorf("Setup calls = %v, want %v", setups, tc.wantSetup)
			}
			if !reflect.DeepEqual(tears, tc.wantTear) {
				t.Errorf("Teardown calls = %v, want %v", tears, tc.wantTear)
			}
			if got := registryMux(s); !reflect.DeepEqual(got, tc.wantReg) {
				t.Errorf("registry dir->mux = %v, want %v", got, tc.wantReg)
			}
			// Both the Dir claim and any MuxRoot claim must be released.
			assertClaimsReleased(t, s, 0)
			// The spec forwarded to the provider carries the MuxRoot verbatim.
			if len(setups) > 0 {
				for _, spec := range fake.capturedSpecs() {
					if spec.MuxRoot != tc.muxRoot {
						t.Errorf("holder-side spec MuxRoot = %q, want %q", spec.MuxRoot, tc.muxRoot)
					}
				}
			}
		})
	}
}

// TestHandleUnmountMuxDetach pins that unmounting a registered mux subtree
// tears it down (a logical detach at the Host seam) and deregisters, and that a
// wedged last-child native unmount surfaces ClassWedged while still dropping the
// row (the kernel truth is the error, never a lying registry).
func TestHandleUnmountMuxDetach(t *testing.T) {
	const (
		base = "/pool/base"
		root = "/pool/mnt"
		a1   = "/pool/mnt/acct-01"
	)
	tests := []struct {
		name        string
		teardownErr error
		wantOK      bool
		wantClass   string
		wantErr     string
	}{
		{
			name:   "clean detach",
			wantOK: true,
		},
		{
			name:        "wedged last-child native unmount classifies wedged",
			teardownErr: fmt.Errorf("%w: %s; refusing to treat it as torn down", fusekit.ErrUnmountWedged, root),
			wantOK:      false,
			wantClass:   ClassWedged,
			wantErr:     "refusing to treat it as torn down",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHost{
				teardownFn: func(string, string) error { return tc.teardownErr },
				aliveFn:    func(string, string) bool { return false },
			}
			s := newHandlerServer(fake)
			s.registry[a1] = mountRow{Base: base, MuxRoot: root}

			resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: a1})

			assertResp(t, resp, tc.wantOK, tc.wantClass, tc.wantErr)
			if _, tears := fake.calls(); !reflect.DeepEqual(tears, []hostCall{{base, a1}}) {
				t.Errorf("Teardown calls = %v, want the subtree detached once", tears)
			}
			if got := registryMux(s); len(got) != 0 {
				t.Errorf("registry = %v, want empty after detach (row dropped regardless of wedge)", got)
			}
			// The Dir claim and the MuxRoot claim are both released.
			assertClaimsReleased(t, s, 0)
		})
	}
}

// TestReclaimSweepsMuxRows pins that reclaim (and, by the same sweep, shutdown)
// tears down mux subtree rows exactly like plain ones: each row's Teardown is a
// logical detach, and the last one takes the native root down through the same
// path. Owner scoping is honored.
func TestReclaimSweepsMuxRows(t *testing.T) {
	const (
		base = "/pool/base"
		root = "/pool/mnt"
	)
	fake := &fakeHost{aliveFn: func(string, string) bool { return false }}
	s := newHandlerServer(fake)
	s.registry["/pool/mnt/acct-01"] = mountRow{Base: base, Owner: "o", MuxRoot: root}
	s.registry["/pool/mnt/acct-02"] = mountRow{Base: base, Owner: "o", MuxRoot: root}
	s.registry["/pool/mnt/acct-03"] = mountRow{Base: base, Owner: "other", MuxRoot: root}

	resp := s.dispatch(Request{Op: OpReclaim, Owner: "o"})
	if !resp.OK {
		t.Fatalf("reclaim: %+v", resp)
	}
	if len(resp.Mounts) != 0 {
		t.Fatalf("reclaim reported failed dirs %+v, want a clean sweep", resp.Mounts)
	}
	_, tears := fake.calls()
	want := []hostCall{{base, "/pool/mnt/acct-01"}, {base, "/pool/mnt/acct-02"}}
	if !reflect.DeepEqual(tears, want) {
		t.Fatalf("Teardown calls = %v, want only owner o's subtrees in dir order", tears)
	}
	if got := registryMux(s); !reflect.DeepEqual(got, map[string]string{"/pool/mnt/acct-03": root}) {
		t.Fatalf("registry after reclaim = %v, want only the other owner's row", got)
	}
}

// TestSweepSerializesWithSameRootMount pins the sweep's MuxRoot claim: a
// reclaim/shutdown sweep claims each mux row's MuxRoot (not just its dir), in the
// fixed dir-then-root order handleMount takes, so a concurrent same-root mount
// bounces ClassBusy instead of racing the sweep's last-child native unmount into
// a dying muxTree.
func TestSweepSerializesWithSameRootMount(t *testing.T) {
	const (
		base = "/pool/base"
		root = "/pool/mnt"
		a1   = "/pool/mnt/acct-01"
		a2   = "/pool/mnt/acct-02"
	)
	entered := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeHost{
		teardownFn: func(string, string) error {
			close(entered)
			<-release
			return nil
		},
		aliveFn: func(string, string) bool { return false },
	}
	s := newHandlerServer(fake)
	s.registry[a1] = mountRow{Base: base, Owner: "o", MuxRoot: root}

	swept := make(chan Response, 1)
	go func() { swept <- s.dispatch(Request{Op: OpReclaim, Owner: "o"}) }()

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sweep never reached Teardown")
	}

	// The sweep is parked inside Teardown holding a1's dir claim AND the shared
	// MuxRoot claim. A sibling mount under the same root must bounce busy on the
	// root — never proceed into the tree the sweep is tearing down.
	resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: a2, MuxRoot: root})
	if resp.OK || resp.ErrClass != ClassBusy {
		t.Fatalf("same-root mount during a sweep = %+v, want ClassBusy", resp)
	}
	if !strings.Contains(resp.Error, root) {
		t.Errorf("busy error = %q, want it to name the contended mux root %s", resp.Error, root)
	}
	if setups, _ := fake.calls(); len(setups) != 0 {
		t.Errorf("Setup ran %d times, want 0 — the busy mount must not reach the provider", len(setups))
	}

	close(release)
	if r := <-swept; !r.OK {
		t.Fatalf("reclaim = %+v, want a clean sweep", r)
	}
	if got := registryMux(s); len(got) != 0 {
		t.Errorf("registry after sweep = %v, want empty", got)
	}
	assertClaimsReleased(t, s, 0)
}

// TestHandleListCarriesMuxRoot pins that List surfaces each row's MuxRoot and
// that a subtree's Live is the tree-index verdict (root mounted ∧ attached ∧
// subtree probe), reused verbatim from the plain liveness path.
func TestHandleListCarriesMuxRoot(t *testing.T) {
	const (
		base = "/pool/base"
		root = "/pool/mnt"
		a1   = "/pool/mnt/acct-01"
	)
	fake := &fakeHost{}
	s := newHandlerServer(fake)
	s.registry[a1] = mountRow{Base: base, MuxRoot: root, Epoch: 1}
	setState(fake,
		func(d string) bool { return d == a1 },
		func(_, d string) bool { return d == a1 })

	resp := s.dispatch(Request{Op: OpList})
	if !resp.OK || len(resp.Mounts) != 1 {
		t.Fatalf("list = %+v, want one entry", resp)
	}
	m := resp.Mounts[0]
	if m.Dir != a1 || m.Base != base || m.MuxRoot != root || !m.Live {
		t.Fatalf("list entry = %+v, want live subtree %s of root %s", m, a1, root)
	}
}
