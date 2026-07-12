//go:build fuse && cgo

package fusekit

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

// subtreeFSStub is a mux native-root stand-in implementing SubtreeHost: it
// records the attach/detach sequence and current membership.
type subtreeFSStub struct {
	fuse.FileSystemBase
	mu       sync.Mutex
	members  map[string]bool
	attaches []string
	detaches []string
}

func newSubtreeFSStub() *subtreeFSStub {
	return &subtreeFSStub{members: map[string]bool{}}
}

func (f *subtreeFSStub) Attach(name string, _ fuse.FileSystemInterface) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.members[name] {
		return fmt.Errorf("stub: %q already attached", name)
	}
	f.members[name] = true
	f.attaches = append(f.attaches, name)
	return nil
}

func (f *subtreeFSStub) Detach(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.members, name)
	f.detaches = append(f.detaches, name)
}

func (f *subtreeFSStub) Attached(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.members[name]
}

func (f *subtreeFSStub) attachCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.attaches)
}

func (f *subtreeFSStub) detachCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.detaches)
}

// muxChildStub is a tenant filesystem stand-in; it implements Flusher and
// HandleReleaser so Drain registration and the post-detach release+flush are
// observable, recording their call order.
type muxChildStub struct {
	fuse.FileSystemBase
	flushes  atomic.Int32
	releases atomic.Int32

	mu    sync.Mutex
	order []string // "release"/"flush" in call order
}

func (f *muxChildStub) FlushWithin(time.Duration) bool {
	f.flushes.Add(1)
	f.mu.Lock()
	f.order = append(f.order, "flush")
	f.mu.Unlock()
	return true
}

func (f *muxChildStub) ReleaseAll() {
	f.releases.Add(1)
	f.mu.Lock()
	f.order = append(f.order, "release")
	f.mu.Unlock()
}

func (f *muxChildStub) callOrder() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

func swapMountFn(t *testing.T, fn func(Config) (*Handle, error)) {
	t.Helper()
	prev := mountFn
	mountFn = fn
	t.Cleanup(func() { mountFn = prev })
}

// muxRig wires a MountSet whose Build dispatches ContentModeMux to a fresh
// subtreeFSStub per native root and everything else to a fresh muxChildStub,
// with mountFn faked to a rigHost-backed Handle — no kernel mounts anywhere.
// The rig keeps a truthful per-dir mount table: mountFn marks a dir mounted,
// a graceful fake unmount clears it, and mountedFn reads it — so code that
// consults kernel mount truth (State, the dead-root reap) sees the same
// lifecycle a real mount has. setMounted(dir, false) without an unmount is
// the external force-unmount: the mount vanishes while every handle survives.
type muxRig struct {
	set        *MountSet
	rootFS     *subtreeFSStub // the CURRENT native root's host (fresh per root mount)
	rootHost   *unmountLedger // counts graceful unmount(2) calls through the seam
	mountDirs  []string       // dirs mountFn was invoked for, in order
	children   map[string]*muxChildStub
	childReady func() bool
	stateCalls []string
	preAttach  string // name pre-occupied on each fresh root (attach-conflict arming)

	mu            sync.Mutex
	mounted       map[string]bool     // kernel mount-table truth per dir, kept by the fakes
	hosts         map[string]*rigHost // dir -> its latest fake Handle's serve-side record
	wedged        bool                // when set, fake unmounts don't take (the mount survives)
	unmountBlock  chan struct{}       // non-nil: fake unmount calls PARK on it (in-flight teardown)
	childBuildErr error               // non-nil: Build fails for non-mux (child) specs
}

func (r *muxRig) setHost(dir string, h *rigHost) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hosts[dir] = h
}

func (r *muxRig) hostFor(dir string) *rigHost {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.hosts[dir]
}

func (r *muxRig) setUnmountBlock(ch chan struct{}) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unmountBlock = ch
}

func (r *muxRig) getUnmountBlock() chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.unmountBlock
}

func (r *muxRig) setChildBuildErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.childBuildErr = err
}

func (r *muxRig) getChildBuildErr() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.childBuildErr
}

func (r *muxRig) setMounted(dir string, v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mounted[dir] = v
}

func (r *muxRig) isMounted(dir string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mounted[dir]
}

func (r *muxRig) setWedged(v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.wedged = v
}

func (r *muxRig) isWedged() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.wedged
}

// unmountLedger counts graceful unmount(2) calls issued through the
// unmountFn seam; the cgofuse host is structurally unreachable from a Handle.
type unmountLedger struct{ calls atomic.Int32 }

// rigHost is one fake Handle's serve-side record (done = the serve loop's
// exit); Handle.Unmount goes through the kernel unmountFn seam
// (rig.kernelUnmount).
type rigHost struct {
	rig  *muxRig
	dir  string
	done chan struct{}
	once sync.Once
}

// kernelUnmount is the rig's unmountFn: a graceful unmount that takes clears
// the dir from the rig's mount table and closes the Handle's done (the serve
// goroutine exiting), mirroring a real external unmount; a wedged rig leaves
// both untouched so Handle.Unmount reads its wedge verdict. Calls count on
// the shared fakeHost ledger.
func (r *muxRig) kernelUnmount(dir string, flags int) error {
	r.rootHost.calls.Add(1)
	if flags != 0 {
		panic(fmt.Sprintf("rig: unmount(2) issued with flags=%d, want 0 (graceful only)", flags))
	}
	if blk := r.getUnmountBlock(); blk != nil {
		<-blk // an in-flight teardown: the call parks until the test resolves it
	}
	if r.isWedged() {
		return errors.New("EBUSY (rig wedged)")
	}
	if h := r.hostFor(dir); h != nil {
		h.once.Do(func() {
			r.setMounted(dir, false)
			close(h.done)
		})
		return nil
	}
	r.setMounted(dir, false)
	return nil
}

func newMuxRig(t *testing.T) *muxRig {
	t.Helper()
	rig := &muxRig{
		rootFS:     newSubtreeFSStub(),
		rootHost:   &unmountLedger{},
		children:   map[string]*muxChildStub{},
		childReady: func() bool { return true },
		mounted:    map[string]bool{},
		hosts:      map[string]*rigHost{},
	}
	rig.set = &MountSet{
		Build: func(spec MountSpec) (Config, error) {
			if spec.ContentMode == ContentModeMux {
				root := newSubtreeFSStub()
				if rig.preAttach != "" {
					if err := root.Attach(rig.preAttach, &muxChildStub{}); err != nil {
						return Config{}, err
					}
				}
				rig.rootFS = root
				return Config{Base: spec.Base, Dir: spec.Dir, FS: root}, nil
			}
			if err := rig.getChildBuildErr(); err != nil {
				return Config{}, err
			}
			child := &muxChildStub{}
			rig.children[spec.Dir] = child
			return Config{
				Base:  spec.Base,
				Dir:   spec.Dir,
				FS:    child,
				Ready: func() bool { return rig.childReady() },
				Wait:  30 * time.Millisecond,
			}, nil
		},
		StateFn: func(base, dir string) (mounted, alive bool) {
			rig.stateCalls = append(rig.stateCalls, dir)
			return false, false
		},
	}
	swapMountFn(t, func(cfg Config) (*Handle, error) {
		rig.mountDirs = append(rig.mountDirs, cfg.Dir)
		rig.setMounted(cfg.Dir, true)
		h := &rigHost{rig: rig, dir: cfg.Dir, done: make(chan struct{})}
		rig.setHost(cfg.Dir, h)
		return &Handle{dir: cfg.Dir, done: h.done}, nil
	})
	swapUnmountFn(t, rig.kernelUnmount)
	swapUnmountGrace(t, 20*time.Millisecond)
	swapMountedFn(t, rig.isMounted)
	swapReapServers(t, func(string) {})
	return rig
}

func muxSpec(root, name string) MountSpec {
	return MountSpec{
		Base:    "/fake/base",
		Dir:     filepath.Join(root, name),
		Owner:   "test",
		MuxRoot: root,
	}
}

// waitFor polls cond up to a second; the graceful unmount call is issued on a
// goroutine, so assertions on it must not race.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestMountSetMuxSharesOneNativeMount pins the whole point: N Setups sharing
// a MuxRoot produce ONE native mount, N logical attaches, and per-dir
// idempotency.
func TestMountSetMuxSharesOneNativeMount(t *testing.T) {
	rig := newMuxRig(t)
	root := "/fake/mux"
	for _, name := range []string{"a", "b", "c"} {
		if err := rig.set.Setup(muxSpec(root, name)); err != nil {
			t.Fatalf("Setup(%s) = %v, want nil", name, err)
		}
	}
	if len(rig.mountDirs) != 1 || rig.mountDirs[0] != root {
		t.Fatalf("native mounts = %v, want exactly [%s]", rig.mountDirs, root)
	}
	if got := rig.rootFS.attachCount(); got != 3 {
		t.Fatalf("attaches = %d, want 3", got)
	}
	for _, name := range []string{"a", "b", "c"} {
		if !rig.rootFS.Attached(name) {
			t.Errorf("Attached(%s) = false, want true", name)
		}
	}
	// Idempotent per dir: a repeat Setup neither mounts nor re-attaches.
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("repeat Setup(a) = %v, want nil", err)
	}
	if len(rig.mountDirs) != 1 || rig.rootFS.attachCount() != 3 {
		t.Fatalf("repeat Setup mounted or attached again: mounts=%v attaches=%d", rig.mountDirs, rig.rootFS.attachCount())
	}
}

// TestMountSetMuxOptionMismatch pins ErrMuxMismatch: a later tenant
// disagreeing with the root's AttrCache options is refused, never silently
// folded in.
func TestMountSetMuxOptionMismatch(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*MountSpec)
	}{
		{"attrcache", func(s *MountSpec) { s.AttrCache = true }},
		{"attrcache_timeout", func(s *MountSpec) { s.AttrCache = false; s.AttrCacheTimeout = 2 * time.Second }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newMuxRig(t)
			root := "/fake/mux"
			if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
				t.Fatalf("first Setup = %v, want nil", err)
			}
			spec := muxSpec(root, "b")
			tc.mutate(&spec)
			err := rig.set.Setup(spec)
			if !errors.Is(err, ErrMuxMismatch) {
				t.Fatalf("mismatched Setup = %v, want ErrMuxMismatch", err)
			}
			if rig.rootFS.Attached("b") {
				t.Error("mismatched tenant was attached anyway")
			}
			if len(rig.mountDirs) != 1 {
				t.Errorf("native mounts = %v, want the one original", rig.mountDirs)
			}
		})
	}
}

// TestMountSetMuxTeardown pins the detach lifecycle: non-last teardown only
// detaches; the last child gracefully unmounts the native root; the next
// Setup after empty re-establishes it.
func TestMountSetMuxTeardown(t *testing.T) {
	rig := newMuxRig(t)
	root := "/fake/mux"
	for _, name := range []string{"a", "b"} {
		if err := rig.set.Setup(muxSpec(root, name)); err != nil {
			t.Fatalf("Setup(%s) = %v", name, err)
		}
	}

	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "a")); err != nil {
		t.Fatalf("Teardown(a) = %v, want nil", err)
	}
	if rig.rootFS.Attached("a") || !rig.rootFS.Attached("b") {
		t.Fatal("Teardown(a) must detach only a")
	}
	if got := rig.rootHost.calls.Load(); got != 0 {
		t.Fatalf("root unmounted after non-last teardown (%d calls)", got)
	}

	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "b")); err != nil {
		t.Fatalf("Teardown(b) = %v, want nil", err)
	}
	if rig.rootFS.Attached("b") {
		t.Fatal("last teardown left b attached")
	}
	waitFor(t, "graceful root unmount", func() bool { return rig.rootHost.calls.Load() >= 1 })

	// The tree is gone: the next Setup mounts a fresh native root.
	if err := rig.set.Setup(muxSpec(root, "c")); err != nil {
		t.Fatalf("Setup(c) after empty = %v, want nil", err)
	}
	if len(rig.mountDirs) != 2 {
		t.Fatalf("native mounts = %v, want a remount after unmount-on-empty", rig.mountDirs)
	}
}

// TestMountSetMuxTeardownWedgedRootKeepsTree pins wedge honesty: a wedged
// last-child unmount surfaces ErrUnmountWedged and the root stays registered
// — the kernel still holds the mount, so the registry must keep saying so.
func TestMountSetMuxTeardownWedgedRootKeepsTree(t *testing.T) {
	rig := newMuxRig(t)
	swapMountedFn(t, func(string) bool { return true }) // unmount never takes
	root := "/fake/mux"
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("Setup(a) = %v", err)
	}
	err := rig.set.Teardown("/fake/base", filepath.Join(root, "a"))
	if !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("wedged last teardown = %v, want ErrUnmountWedged", err)
	}
	// The still-mounted root is reused, never stacked over.
	if err := rig.set.Setup(muxSpec(root, "b")); err != nil {
		t.Fatalf("Setup(b) after wedge = %v, want nil", err)
	}
	if len(rig.mountDirs) != 1 {
		t.Fatalf("native mounts = %v, want no second mount over a wedged root", rig.mountDirs)
	}
}

// TestMountSetMuxDeadRootReassembly pins the force-unmount recovery contract
// (the 2026-07-03 validate-mux reassembly livelock): the native root is
// removed out from under the holder while the tree still carries siblings.
// The next re-issued tenant Setup must reap the carcass — release every
// stranded child and unmount the dead handle, which reaps the orphaned
// server — and mount a FRESH native root; the siblings' re-issued Setups
// re-attach through it. Without the reap the tree survives (siblings keep it
// non-empty, so the last-child unmount never fires), every re-issued Setup
// attaches into the carcass, and nothing ever comes kernel-visible again.
func TestMountSetMuxDeadRootReassembly(t *testing.T) {
	rig := newMuxRig(t)
	root := "/fake/mux"
	dirs := map[string]string{}
	for _, name := range []string{"a", "b", "c"} {
		dirs[name] = filepath.Join(root, name)
		if err := rig.set.Setup(muxSpec(root, name)); err != nil {
			t.Fatalf("Setup(%s) = %v", name, err)
		}
	}
	stranded := map[string]*muxChildStub{}
	for name, dir := range dirs {
		stranded[name] = rig.children[dir]
	}

	// The kernel mount vanishes out from under the holder (external umount -f).
	rig.setMounted(root, false)

	// The holder's remount path detaches the dead row first (Host.Teardown),
	// then re-issues Setup. The teardown is non-last — siblings remain — so
	// only the Setup's reap can retire the carcass.
	if err := rig.set.Teardown("/fake/base", dirs["a"]); err != nil {
		t.Fatalf("Teardown(a) over the dead root = %v, want nil", err)
	}
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("Setup(a) over the dead root = %v, want a reap + fresh remount", err)
	}
	if len(rig.mountDirs) != 2 || rig.mountDirs[1] != root {
		t.Fatalf("native mounts = %v, want a fresh root remount after the reap", rig.mountDirs)
	}
	if got := rig.rootHost.calls.Load(); got != 1 {
		t.Fatalf("unmount calls = %d, want exactly 1 (the dead handle's reap)", got)
	}
	// The reap released and drained the stranded siblings' handles; tenant a's
	// own release happened via its explicit Teardown.
	for _, name := range []string{"a", "b", "c"} {
		if got := stranded[name].releases.Load(); got != 1 {
			t.Errorf("stranded %s releases = %d, want 1", name, got)
		}
		if got := stranded[name].flushes.Load(); got != 1 {
			t.Errorf("stranded %s flushes = %d, want 1", name, got)
		}
	}

	// Siblings reassemble: their subtree index entries dropped with the reap,
	// so their teardown is the under-live-root no-op and Setup re-attaches
	// through the fresh root — never a second native mount.
	for _, name := range []string{"b", "c"} {
		if err := rig.set.Teardown("/fake/base", dirs[name]); err != nil {
			t.Fatalf("Teardown(%s) = %v, want nil no-op", name, err)
		}
		if err := rig.set.Setup(muxSpec(root, name)); err != nil {
			t.Fatalf("Setup(%s) after the reap = %v", name, err)
		}
	}
	if len(rig.mountDirs) != 2 {
		t.Fatalf("native mounts = %v, want the root remounted exactly once for the whole reassembly", rig.mountDirs)
	}
	for _, name := range []string{"a", "b", "c"} {
		if !rig.rootFS.Attached(name) {
			t.Errorf("%s not attached to the fresh root", name)
		}
	}
	if got := rig.rootHost.calls.Load(); got != 1 {
		t.Fatalf("unmount calls after reassembly = %d, want still 1 (the live root is never unmounted)", got)
	}
}

// TestMountSetMuxDeadRootReapPending pins R5-6: a dead-root reap whose
// unmount call is still in flight past the grace registers its resolution
// under the tenant dir driving the reap — so the server's park confirms the
// call's return before the final fence release — and the resolution reaps
// the orphaned server only once the call has landed.
func TestMountSetMuxDeadRootReapPending(t *testing.T) {
	rig := newMuxRig(t)
	reaped := make(chan string, 2)
	swapReapServers(t, func(dir string) { reaped <- dir })
	root := "/fake/mux"
	dirA := filepath.Join(root, "a")
	for _, name := range []string{"a", "b"} {
		if err := rig.set.Setup(muxSpec(root, name)); err != nil {
			t.Fatalf("Setup(%s) = %v", name, err)
		}
	}

	// External force-unmount carcass; a is re-issued (teardown is non-last, so
	// only the Setup's reap can retire the dead tree) with the reap's unmount
	// call parked in flight.
	rig.setMounted(root, false)
	if err := rig.set.Teardown("/fake/base", dirA); err != nil {
		t.Fatalf("Teardown(a) over the dead root = %v", err)
	}
	blk := make(chan struct{})
	rig.setUnmountBlock(blk)

	err := rig.set.Setup(muxSpec(root, "a"))
	if err == nil || !errors.Is(err, ErrTeardownPending) {
		t.Fatalf("Setup over a dead root with the reap in flight = %v, want ErrTeardownPending", err)
	}
	ch := rig.set.TeardownDone(dirA)
	if ch == nil {
		t.Fatal("TeardownDone(a) = nil — the pending dead-root reap registered no resolution channel")
	}
	select {
	case dir := <-reaped:
		t.Fatalf("reaped %q before the parked unmount call returned", dir)
	default:
	}

	close(blk)
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("resolution channel never closed after the parked call returned")
	}
	select {
	case dir := <-reaped:
		if dir != root {
			t.Fatalf("reaped %q, want the dead root %q", dir, root)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resolution never reaped the dead root's server")
	}
}

// TestMountSetHoldsMuxRoot pins the MuxRootHolder capability the holder consults
// when no registry row references a root: false before any mount, true while the
// native mount is held — INCLUDING after a wedged last-child unmount keeps the
// tree — and false again once it comes cleanly down.
func TestMountSetHoldsMuxRoot(t *testing.T) {
	rig := newMuxRig(t)
	root := "/fake/mux"
	if rig.set.HoldsMuxRoot(root) {
		t.Fatal("HoldsMuxRoot before any mount = true, want false")
	}
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("Setup(a) = %v", err)
	}
	if !rig.set.HoldsMuxRoot(root) {
		t.Fatal("HoldsMuxRoot with a live tenant = false, want true")
	}

	// A wedged last-child unmount keeps the tree — the kernel mount survived —
	// so the root is still held even though its last subtree row is gone.
	rig.setWedged(true)
	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "a")); !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("wedged last teardown = %v, want ErrUnmountWedged", err)
	}
	if !rig.set.HoldsMuxRoot(root) {
		t.Fatal("HoldsMuxRoot after a wedged last-detach = false, want true (kernel mount survived)")
	}

	// A later tenant re-attaches to the surviving root: the kernel mount is
	// still up, so there is no reap and no second native mount.
	rig.setWedged(false)
	if err := rig.set.Setup(muxSpec(root, "b")); err != nil {
		t.Fatalf("Setup(b) after wedge = %v", err)
	}
	if len(rig.mountDirs) != 1 {
		t.Fatalf("native mounts = %v, want re-attach to the surviving root, no remount", rig.mountDirs)
	}
	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "b")); err != nil {
		t.Fatalf("clean last teardown = %v", err)
	}
	waitFor(t, "graceful root unmount", func() bool { return rig.rootHost.calls.Load() >= 1 })
	if rig.set.HoldsMuxRoot(root) {
		t.Fatal("HoldsMuxRoot after a clean last-detach = true, want false")
	}
}

// TestMountSetMuxTeardownUnregisteredSubtree pins the no-op: an unregistered
// dir under a live mux root is not a mountpoint and must never take the
// forced-unmount carcass path through the native mount.
func TestMountSetMuxTeardownUnregisteredSubtree(t *testing.T) {
	rig := newMuxRig(t)
	root := "/fake/mux"
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("Setup(a) = %v", err)
	}
	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "ghost")); err != nil {
		t.Fatalf("Teardown(ghost) = %v, want nil no-op", err)
	}
	if got := rig.rootFS.detachCount(); got != 0 {
		t.Errorf("detaches = %d, want 0", got)
	}
	if got := rig.rootHost.calls.Load(); got != 0 {
		t.Errorf("root unmount calls = %d, want 0", got)
	}
	if !rig.rootFS.Attached("a") {
		t.Error("Teardown(ghost) disturbed tenant a")
	}
}

// TestMountSetMuxChildReadyFailure pins the fail-loud attach: a subtree that
// never comes live is detached again, and — as the only child — takes the
// native root down with it.
func TestMountSetMuxChildReadyFailure(t *testing.T) {
	rig := newMuxRig(t)
	rig.childReady = func() bool { return false }
	root := "/fake/mux"
	err := rig.set.Setup(muxSpec(root, "a"))
	if err == nil {
		t.Fatal("Setup with a never-ready subtree succeeded, want failure")
	}
	if !errors.Is(err, ErrMountNotLive) && !errors.Is(err, ErrMountTimeout) {
		t.Fatalf("Setup = %v, want a mount-liveness error", err)
	}
	if rig.rootFS.Attached("a") {
		t.Error("failed subtree left attached")
	}
	waitFor(t, "empty root unmount", func() bool { return rig.rootHost.calls.Load() >= 1 })

	// The registry healed: a later Setup with a ready child works from scratch.
	rig.childReady = func() bool { return true }
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("Setup after recovery = %v, want nil", err)
	}
	if !rig.rootFS.Attached("a") {
		t.Error("recovered Setup did not attach")
	}
}

// TestMountSetMuxRootBuildFailures pins the loud refusals establishing a
// native root: Build errors pass through, and a root filesystem that cannot
// host subtrees is ErrMuxMismatch.
func TestMountSetMuxRootBuildFailures(t *testing.T) {
	t.Run("build_error", func(t *testing.T) {
		rig := newMuxRig(t)
		boom := errors.New("boom")
		rig.set.Build = func(spec MountSpec) (Config, error) { return Config{}, boom }
		if err := rig.set.Setup(muxSpec("/fake/mux", "a")); !errors.Is(err, boom) {
			t.Fatalf("Setup = %v, want the Build error", err)
		}
		if len(rig.mountDirs) != 0 {
			t.Errorf("native mounts = %v, want none", rig.mountDirs)
		}
	})
	t.Run("no_subtree_host", func(t *testing.T) {
		rig := newMuxRig(t)
		rig.set.Build = func(spec MountSpec) (Config, error) {
			return Config{Base: spec.Base, Dir: spec.Dir, FS: &muxChildStub{}}, nil
		}
		if err := rig.set.Setup(muxSpec("/fake/mux", "a")); !errors.Is(err, ErrMuxMismatch) {
			t.Fatalf("Setup = %v, want ErrMuxMismatch for a non-SubtreeHost root", err)
		}
		if len(rig.mountDirs) != 0 {
			t.Errorf("native mounts = %v, want none (refused before mounting)", rig.mountDirs)
		}
	})
}

// TestMountSetMuxFirstTenantFailureUnwindsRoot pins the first-tenant unwind: a
// child Build (or Attach) failure AFTER the native root is established tears the
// childless root back down — never leaving a stranded, registry-rowless mount
// that later mounts refuse as foreign and no sweep can reach — and a clean retry
// re-mounts from scratch.
func TestMountSetMuxFirstTenantFailureUnwindsRoot(t *testing.T) {
	boom := errors.New("first-tenant boom")
	cases := []struct {
		name string
		// arm makes the FIRST tenant's Build or Attach fail and returns a repair
		// that undoes the failure so the retry can succeed.
		arm func(t *testing.T, rig *muxRig) (repair func())
	}{
		{
			name: "child_build_error",
			arm: func(t *testing.T, rig *muxRig) func() {
				good := rig.set.Build
				rig.set.Build = func(spec MountSpec) (Config, error) {
					if spec.ContentMode == ContentModeMux {
						return good(spec) // the native root still builds and mounts
					}
					return Config{}, boom // ...but the first child's build fails
				}
				return func() { rig.set.Build = good }
			},
		},
		{
			name: "child_attach_error",
			arm: func(t *testing.T, rig *muxRig) func() {
				// Occupy the tenant name on the freshly built root so the host's
				// Attach refuses the child.
				rig.preAttach = "a"
				return func() { rig.preAttach = "" }
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newMuxRig(t)
			root := "/fake/mux"
			repair := tc.arm(t, rig)

			if err := rig.set.Setup(muxSpec(root, "a")); err == nil {
				t.Fatal("first-tenant failure Setup succeeded, want an error")
			}
			// The root mounted once to host the tenant...
			if len(rig.mountDirs) != 1 || rig.mountDirs[0] != root {
				t.Fatalf("native mounts = %v, want the root mounted exactly once", rig.mountDirs)
			}
			// ...then was torn back down: the childless carcass never lingers.
			waitFor(t, "empty root unmount after a failed first tenant", func() bool { return rig.rootHost.calls.Load() >= 1 })

			// The registry healed: a clean retry mounts a FRESH native root (proving
			// m.trees[root] was cleared) and attaches the tenant.
			repair()
			if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
				t.Fatalf("retry Setup after a failed first tenant = %v, want nil", err)
			}
			if len(rig.mountDirs) != 2 {
				t.Fatalf("native mounts = %v, want a fresh remount on the retry", rig.mountDirs)
			}
			if !rig.rootFS.Attached("a") {
				t.Error("retry did not attach the tenant")
			}
		})
	}
}

// TestMountSetMuxLaterTenantFailureKeepsRoot pins the flip side: a child failure
// on a LATER tenant (the root already carries a sibling) must NOT unwind the
// shared native root — the sibling still owns it.
func TestMountSetMuxLaterTenantFailureKeepsRoot(t *testing.T) {
	rig := newMuxRig(t)
	root := "/fake/mux"
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("first Setup = %v, want nil", err)
	}
	boom := errors.New("later-tenant boom")
	good := rig.set.Build
	rig.set.Build = func(spec MountSpec) (Config, error) {
		if filepath.Base(spec.Dir) == "b" {
			return Config{}, boom
		}
		return good(spec)
	}
	if err := rig.set.Setup(muxSpec(root, "b")); !errors.Is(err, boom) {
		t.Fatalf("later-tenant Setup = %v, want the Build error", err)
	}
	// The shared root is untouched: no unmount, sibling a still attached, no remount.
	if got := rig.rootHost.calls.Load(); got != 0 {
		t.Errorf("native root unmounted after a later-tenant failure (%d calls)", got)
	}
	if len(rig.mountDirs) != 1 {
		t.Errorf("native mounts = %v, want the one original (no remount)", rig.mountDirs)
	}
	if !rig.rootFS.Attached("a") {
		t.Error("later-tenant failure disturbed sibling a")
	}
	if rig.rootFS.Attached("b") {
		t.Error("failed later tenant was attached anyway")
	}
}

// TestMountSetMuxState pins the tree-index State: mounted = root mounted ∧
// attached; alive adds the bounded subtree opendir; unregistered dirs fall
// through to StateFn.
func TestMountSetMuxState(t *testing.T) {
	rig := newMuxRig(t)
	root := t.TempDir()
	dir := filepath.Join(root, "a")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	swapMountedFn(t, func(d string) bool { return d == root })
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("Setup = %v", err)
	}

	if mounted, alive := rig.set.State("/fake/base", dir); !mounted || !alive {
		t.Fatalf("State = (%v, %v), want (true, true)", mounted, alive)
	}

	// Detached tenant: registered dir, live root, no membership.
	rig.rootFS.Detach("a")
	if mounted, alive := rig.set.State("/fake/base", dir); mounted || alive {
		t.Fatalf("State after detach = (%v, %v), want (false, false)", mounted, alive)
	}
	if err := rig.rootFS.Attach("a", &muxChildStub{}); err != nil {
		t.Fatal(err)
	}

	// Root not a kernel mountpoint: dead regardless of attachment.
	swapMountedFn(t, func(string) bool { return false })
	if mounted, alive := rig.set.State("/fake/base", dir); mounted || alive {
		t.Fatalf("State with unmounted root = (%v, %v), want (false, false)", mounted, alive)
	}

	// Unregistered dirs are StateFn's, untouched by the tree index.
	if len(rig.stateCalls) != 0 {
		t.Fatalf("StateFn called for subtree dirs: %v", rig.stateCalls)
	}
	rig.set.State("/fake/base", "/fake/elsewhere")
	if len(rig.stateCalls) != 1 || rig.stateCalls[0] != "/fake/elsewhere" {
		t.Fatalf("StateFn calls = %v, want [/fake/elsewhere]", rig.stateCalls)
	}
}

// TestMountSetMuxDrain pins that subtree children register their Flushers
// under their own dir, so the pre-teardown drain reaches them.
func TestMountSetMuxDrain(t *testing.T) {
	rig := newMuxRig(t)
	root := "/fake/mux"
	dir := filepath.Join(root, "a")
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("Setup = %v", err)
	}
	rig.set.Drain(dir, time.Second)
	if got := rig.children[dir].flushes.Load(); got != 1 {
		t.Fatalf("child flushes = %d, want 1", got)
	}
	// Teardown detaches the child and drains its handles once (release + flush)
	// before deregistering it — so a later Drain can no longer reach it.
	if err := rig.set.Teardown("/fake/base", dir); err != nil {
		t.Fatalf("Teardown = %v", err)
	}
	afterTeardown := rig.children[dir].flushes.Load()
	if afterTeardown != 2 {
		t.Fatalf("child flushes after teardown = %d, want 2 (explicit Drain + teardown flush)", afterTeardown)
	}
	rig.set.Drain(dir, time.Second)
	if got := rig.children[dir].flushes.Load(); got != afterTeardown {
		t.Fatalf("post-teardown drain reached the deregistered child (flushes = %d, want %d)", got, afterTeardown)
	}
}

// TestMountSetMuxTeardownReleasesChildHandles pins the post-detach handle
// release: teardownMux invokes the child's ReleaseAll (closing its kernel fds
// and scheduling any dirty synth write-through) BEFORE draining it with
// FlushWithin, so a detached child never leaks fds or drops an in-flight commit.
func TestMountSetMuxTeardownReleasesChildHandles(t *testing.T) {
	rig := newMuxRig(t)
	root := "/fake/mux"
	dir := filepath.Join(root, "a")
	if err := rig.set.Setup(muxSpec(root, "a")); err != nil {
		t.Fatalf("Setup = %v", err)
	}
	child := rig.children[dir]
	if err := rig.set.Teardown("/fake/base", dir); err != nil {
		t.Fatalf("Teardown = %v", err)
	}
	if got := child.releases.Load(); got != 1 {
		t.Fatalf("ReleaseAll called %d times, want exactly 1", got)
	}
	if order := child.callOrder(); len(order) != 2 || order[0] != "release" || order[1] != "flush" {
		t.Fatalf("teardown op order = %v, want [release flush] (schedule write-throughs, then drain them)", order)
	}
}

// TestMountSetPlainPathUnchanged pins that specs WITHOUT a MuxRoot keep
// today's one-mount-per-dir behavior end to end.
func TestMountSetPlainPathUnchanged(t *testing.T) {
	rig := newMuxRig(t)
	spec := MountSpec{Base: "/fake/base", Dir: "/fake/plain", Owner: "test"}
	if err := rig.set.Setup(spec); err != nil {
		t.Fatalf("Setup = %v", err)
	}
	if len(rig.mountDirs) != 1 || rig.mountDirs[0] != "/fake/plain" {
		t.Fatalf("mounts = %v, want [/fake/plain]", rig.mountDirs)
	}
	if rig.rootFS.attachCount() != 0 {
		t.Fatal("plain Setup touched the subtree host")
	}
	if err := rig.set.Setup(spec); err != nil || len(rig.mountDirs) != 1 {
		t.Fatalf("repeat Setup = (%v, mounts %v), want idempotent no-op", err, rig.mountDirs)
	}
	if err := rig.set.Teardown("/fake/base", "/fake/plain"); err != nil {
		t.Fatalf("Teardown = %v", err)
	}
	waitFor(t, "plain graceful unmount", func() bool { return rig.rootHost.calls.Load() >= 1 })
}

// TestMountSetTeardownWedgeRestoresHandle pins Fix A's kernel-panic half: a
// wedged graceful unmount RESTORES the handle and flusher, so the provider
// stays truthful (still mounted ⟺ still has the handle) and the retry tears
// down gracefully through the restored handle — the handle-less MNT_FORCE
// path is never reached for a mount this holder owns.
func TestMountSetTeardownWedgeRestoresHandle(t *testing.T) {
	rig := newMuxRig(t)
	spec := MountSpec{Base: "/fake/base", Dir: "/fake/plain", Owner: "test"}
	if err := rig.set.Setup(spec); err != nil {
		t.Fatalf("Setup = %v", err)
	}

	rig.setWedged(true)
	if err := rig.set.Teardown("/fake/base", "/fake/plain"); !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("wedged Teardown = %v, want ErrUnmountWedged", err)
	}
	// The flusher came back with the handle: a retry's pre-teardown drain
	// still reaches the filesystem.
	rig.set.Drain("/fake/plain", time.Millisecond)
	if rig.children["/fake/plain"].flushes.Load() == 0 {
		t.Fatal("wedged Teardown dropped the flusher; the retry's drain went nowhere")
	}

	rig.setWedged(false)
	if err := rig.set.Teardown("/fake/base", "/fake/plain"); err != nil {
		t.Fatalf("retry Teardown = %v, want graceful success via the restored handle", err)
	}
	if got := rig.rootHost.calls.Load(); got < 2 {
		t.Fatalf("graceful unmount calls = %d, want the retry to go through the restored handle", got)
	}
	if rig.isMounted("/fake/plain") {
		t.Fatal("mount survived the graceful retry")
	}
}

// TestMountSetTeardownCarcassGracefulOnly pins the handle-less carcass
// branch: Teardown NEVER force-unmounts — a still-present carcass surfaces as
// ErrUnmountWedged for the pre-mount/replay clear (the only force sites), and
// an absent one is a clean no-op.
func TestMountSetTeardownCarcassGracefulOnly(t *testing.T) {
	cases := []struct {
		name    string
		mounted bool // mountedFn verdict (carcass presence)
		wantErr error
	}{
		{name: "present carcass is left in place and surfaced", mounted: true, wantErr: ErrUnmountWedged},
		{name: "nothing mounted is a clean no-op", mounted: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swapMountedFn(t, func(string) bool { return tc.mounted })
			m := &MountSet{StateFn: func(_, _ string) (bool, bool) { return tc.mounted, false }}

			err := m.Teardown("/fake/base", "/fake/carcass")
			if tc.wantErr == nil && err != nil {
				t.Fatalf("Teardown = %v, want nil", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("Teardown = %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestMountSetTeardownPendingLifecycle pins the provider side of P-8: a
// teardown still in flight past the grace surfaces ErrTeardownPending (also
// ErrUnmountWedged), registers a resolution channel TeardownDone pops exactly
// once, keeps the restored handle while the outcome is unknown, and — when
// the parked unmount lands late — drops the handle and reaps the server.
func TestMountSetTeardownPendingLifecycle(t *testing.T) {
	swapUnmountGrace(t, 20*time.Millisecond)
	var mu sync.Mutex
	mounted := true
	swapMountedFn(t, func(string) bool { mu.Lock(); defer mu.Unlock(); return mounted })
	reaped := make(chan string, 4)
	swapReapServers(t, func(d string) { reaped <- d })

	done := make(chan struct{})
	blk := make(chan struct{}) // the unmount CALL parks on it (genuinely in flight)
	fu := &fakeUnmount{block: blk}
	swapUnmountFn(t, fu.unmount)
	swapMountFn(t, func(cfg Config) (*Handle, error) {
		return &Handle{dir: cfg.Dir, done: done}, nil
	})
	set := &MountSet{
		Build:   func(spec MountSpec) (Config, error) { return Config{Base: spec.Base, Dir: spec.Dir}, nil },
		StateFn: func(base, dir string) (m, a bool) { return false, false },
	}
	const base, dir = "/b/acct", "/m/acct"
	if err := set.Setup(MountSpec{Base: base, Dir: dir, Owner: "test"}); err != nil {
		t.Fatal(err)
	}

	err := set.Teardown(base, dir)
	if !errors.Is(err, ErrTeardownPending) || !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("Teardown(in flight) = %v, want ErrTeardownPending wrapping ErrUnmountWedged", err)
	}
	ch := set.TeardownDone(dir)
	if ch == nil {
		t.Fatal("TeardownDone = nil after a pending teardown")
	}
	if set.TeardownDone(dir) != nil {
		t.Fatal("TeardownDone popped twice; the channel must transfer exactly once")
	}
	set.mu.Lock()
	restored := set.mounts[dir] != nil
	set.mu.Unlock()
	if !restored {
		t.Fatal("handle not restored while the outcome is unknown")
	}

	// The parked unmount lands late: the registry self-heals and reaps.
	mu.Lock()
	mounted = false
	mu.Unlock()
	close(blk)
	close(done)
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("resolution channel never closed")
	}
	// Drain to OUR dir: a prior test's late resolution goroutine may fire its
	// own reap into the freshly swapped seam.
	drainDeadline := time.After(2 * time.Second)
	for got := ""; got != dir; {
		select {
		case got = <-reaped:
		case <-drainDeadline:
			t.Fatal("late-landing unmount never reaped")
		}
	}
	waitFor(t, "restored handle dropped", func() bool {
		set.mu.Lock()
		defer set.mu.Unlock()
		return set.mounts[dir] == nil
	})
}

// TestTeardownDoneClosesOnlyAfterReconcile pins the one-release-owner
// sequencing: the channel TeardownDone hands out closes only AFTER the
// registry reconciliation ran, so a fence holder waiting on it can never
// release while a stale restored handle is still visible (the
// adopt-then-delete race).
func TestTeardownDoneClosesOnlyAfterReconcile(t *testing.T) {
	rig := newMuxRig(t)
	spec := MountSpec{Base: "/fake/base", Dir: "/fake/mnt/a"}
	if err := rig.set.Setup(spec); err != nil {
		t.Fatalf("Setup: %v", err)
	}

	blk := make(chan struct{})
	rig.setUnmountBlock(blk)
	err := rig.set.Teardown(spec.Base, spec.Dir)
	if !errors.Is(err, ErrTeardownPending) {
		t.Fatalf("Teardown = %v, want ErrTeardownPending (call parked past the grace)", err)
	}
	ch := rig.set.TeardownDone(spec.Dir)
	if ch == nil {
		t.Fatal("TeardownDone = nil for a pending teardown")
	}

	close(blk) // the parked unmount lands
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("resolution channel never closed")
	}
	// The sequencing pin: at the instant ch fires, reconciliation already
	// dropped the landed teardown's handle — no window where a new Setup can
	// adopt it and reconciliation then deletes it.
	rig.set.mu.Lock()
	_, held := rig.set.mounts[spec.Dir]
	rig.set.mu.Unlock()
	if held {
		t.Fatal("resolution fired BEFORE reconciliation: the stale handle is still registered")
	}
}

// TestFailedFirstChildUnwindRegistersPending pins the failed-first-child
// routing: when the empty root's unwind unmount is still in flight, the
// error carries ErrTeardownPending and the pending-teardown machinery is
// registered under the child's dir — same as every other pending case — so
// the holder parks instead of letting a retry stack onto the in-flight
// unmount. Once resolved, a retry assembles a fresh root.
func TestFailedFirstChildUnwindRegistersPending(t *testing.T) {
	rig := newMuxRig(t)
	const root = "/fake/mux"
	spec := muxSpec(root, "acct-01")
	rig.setChildBuildErr(errors.New("bridge blip"))

	blk := make(chan struct{})
	rig.setUnmountBlock(blk)
	err := rig.set.Setup(spec)
	if err == nil || !errors.Is(err, ErrTeardownPending) {
		t.Fatalf("Setup = %v, want the cause joined with ErrTeardownPending", err)
	}
	ch := rig.set.TeardownDone(spec.Dir)
	if ch == nil {
		t.Fatal("TeardownDone = nil: the pending root unwind was never registered")
	}

	close(blk)
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("pending unwind never resolved")
	}
	rig.set.mu.Lock()
	_, held := rig.set.trees[root]
	rig.set.mu.Unlock()
	if held {
		t.Fatal("landed root unwind left the dead tree registered")
	}

	// Retry self-assembles a fresh root.
	rig.setUnmountBlock(nil)
	rig.setChildBuildErr(nil)
	if err := rig.set.Setup(spec); err != nil {
		t.Fatalf("retry Setup = %v, want a fresh root", err)
	}
}
