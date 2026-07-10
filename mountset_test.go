//go:build fuse && cgo

package fusekit

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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
	rootHost   *fakeHost      // shared unmount-call ledger across every fake Handle
	mountDirs  []string       // dirs mountFn was invoked for, in order
	children   map[string]*muxChildStub
	childReady func() bool
	stateCalls []string
	preAttach  string // name pre-occupied on each fresh root (attach-conflict arming)

	mu      sync.Mutex
	mounted map[string]bool // kernel mount-table truth per dir, kept by the fakes
	wedged  bool            // when set, fake unmounts don't take (the mount survives)
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

// rigHost is the per-mount unmounter fake: a graceful unmount that takes
// clears the dir from the rig's mount table and closes the Handle's done (the
// serve goroutine exiting), mirroring a real teardown; a wedged rig leaves
// both untouched so Handle.Unmount times out into its wedge verdict. Calls
// count through the shared fakeHost so tests keep one unmount ledger.
type rigHost struct {
	rig  *muxRig
	dir  string
	done chan struct{}
	once sync.Once
}

func (h *rigHost) Unmount() bool {
	h.rig.rootHost.calls.Add(1)
	if h.rig.isWedged() {
		return false
	}
	h.once.Do(func() {
		h.rig.setMounted(h.dir, false)
		close(h.done)
	})
	return true
}

func newMuxRig(t *testing.T) *muxRig {
	t.Helper()
	rig := &muxRig{
		rootFS:     newSubtreeFSStub(),
		rootHost:   &fakeHost{},
		children:   map[string]*muxChildStub{},
		childReady: func() bool { return true },
		mounted:    map[string]bool{},
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
		return &Handle{host: h, dir: cfg.Dir, done: h.done}, nil
	})
	swapUnmountGraces(t, 20*time.Millisecond, 20*time.Millisecond)
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

	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "a"), CarcassPolicyForce); err != nil {
		t.Fatalf("Teardown(a) = %v, want nil", err)
	}
	if rig.rootFS.Attached("a") || !rig.rootFS.Attached("b") {
		t.Fatal("Teardown(a) must detach only a")
	}
	if got := rig.rootHost.calls.Load(); got != 0 {
		t.Fatalf("root unmounted after non-last teardown (%d calls)", got)
	}

	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "b"), CarcassPolicyForce); err != nil {
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
	err := rig.set.Teardown("/fake/base", filepath.Join(root, "a"), CarcassPolicyForce)
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
	if err := rig.set.Teardown("/fake/base", dirs["a"], CarcassPolicyForce); err != nil {
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
		if err := rig.set.Teardown("/fake/base", dirs[name], CarcassPolicyForce); err != nil {
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
	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "a"), CarcassPolicyForce); !errors.Is(err, ErrUnmountWedged) {
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
	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "b"), CarcassPolicyForce); err != nil {
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
	if err := rig.set.Teardown("/fake/base", filepath.Join(root, "ghost"), CarcassPolicyForce); err != nil {
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
	if err := rig.set.Teardown("/fake/base", dir, CarcassPolicyForce); err != nil {
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
	if err := rig.set.Teardown("/fake/base", dir, CarcassPolicyForce); err != nil {
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
	if err := rig.set.Teardown("/fake/base", "/fake/plain", CarcassPolicyForce); err != nil {
		t.Fatalf("Teardown = %v", err)
	}
	waitFor(t, "plain graceful unmount", func() bool { return rig.rootHost.calls.Load() >= 1 })
}

func swapForceUnmountFn(t *testing.T, fn func(string)) {
	t.Helper()
	prev := forceUnmountFn
	forceUnmountFn = fn
	t.Cleanup(func() { forceUnmountFn = prev })
}

// TestMountSetTeardownWedgeRestoresHandle pins Fix A's kernel-panic half: a
// wedged graceful unmount RESTORES the handle and flusher, so the provider
// stays truthful (still mounted ⟺ still has the handle) and the retry tears
// down gracefully through the restored handle — the handle-less MNT_FORCE
// path is never reached for a mount this holder owns.
func TestMountSetTeardownWedgeRestoresHandle(t *testing.T) {
	rig := newMuxRig(t)
	var forced []string
	swapForceUnmountFn(t, func(dir string) { forced = append(forced, dir) })
	spec := MountSpec{Base: "/fake/base", Dir: "/fake/plain", Owner: "test"}
	if err := rig.set.Setup(spec); err != nil {
		t.Fatalf("Setup = %v", err)
	}

	rig.setWedged(true)
	if err := rig.set.Teardown("/fake/base", "/fake/plain", CarcassPolicyForce); !errors.Is(err, ErrUnmountWedged) {
		t.Fatalf("wedged Teardown = %v, want ErrUnmountWedged", err)
	}
	// The flusher came back with the handle: a retry's pre-teardown drain
	// still reaches the filesystem.
	rig.set.Drain("/fake/plain", time.Millisecond)
	if rig.children["/fake/plain"].flushes.Load() == 0 {
		t.Fatal("wedged Teardown dropped the flusher; the retry's drain went nowhere")
	}

	rig.setWedged(false)
	if err := rig.set.Teardown("/fake/base", "/fake/plain", CarcassPolicyForce); err != nil {
		t.Fatalf("retry Teardown = %v, want graceful success via the restored handle", err)
	}
	if got := rig.rootHost.calls.Load(); got < 2 {
		t.Fatalf("graceful unmount calls = %d, want the retry to go through the restored handle", got)
	}
	if rig.isMounted("/fake/plain") {
		t.Fatal("mount survived the graceful retry")
	}
	// The load-bearing negative: no MNT_FORCE, ever, for a handle-backed mount.
	if len(forced) != 0 {
		t.Fatalf("force-unmount path hit %v, want never (graceful-only for owned mounts)", forced)
	}
}

// TestMountSetTeardownCarcassPolicy pins the handle-less carcass branch:
// force (or absent) policy force-unmounts then re-checks; defer NEVER touches
// the force path — it surfaces a present carcass as ErrUnmountWedged and
// no-ops on an absent one.
func TestMountSetTeardownCarcassPolicy(t *testing.T) {
	cases := []struct {
		name      string
		policy    string
		mounted   bool // mountedFn verdict (carcass presence / post-force re-check)
		wantErr   error
		wantForce bool
	}{
		{name: "force clears a carcass", policy: CarcassPolicyForce, mounted: false, wantForce: true},
		{name: "absent policy means force", policy: "", mounted: false, wantForce: true},
		{name: "force on a wedged carcass surfaces wedged", policy: CarcassPolicyForce, mounted: true, wantErr: ErrUnmountWedged, wantForce: true},
		{name: "defer leaves a present carcass and surfaces it", policy: CarcassPolicyDefer, mounted: true, wantErr: ErrUnmountWedged, wantForce: false},
		{name: "defer with nothing mounted is a clean no-op", policy: CarcassPolicyDefer, mounted: false, wantForce: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swapMountedFn(t, func(string) bool { return tc.mounted })
			var forced []string
			swapForceUnmountFn(t, func(dir string) { forced = append(forced, dir) })
			m := &MountSet{StateFn: func(_, _ string) (bool, bool) { return tc.mounted, false }}

			err := m.Teardown("/fake/base", "/fake/carcass", tc.policy)
			if tc.wantErr == nil && err != nil {
				t.Fatalf("Teardown = %v, want nil", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("Teardown = %v, want %v", err, tc.wantErr)
			}
			if tc.wantForce && !reflect.DeepEqual(forced, []string{"/fake/carcass"}) {
				t.Fatalf("force calls = %v, want exactly the carcass dir", forced)
			}
			if !tc.wantForce && len(forced) != 0 {
				t.Fatalf("force calls = %v, want NONE — defer must never force-unmount", forced)
			}
		})
	}
}
