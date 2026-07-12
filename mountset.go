//go:build fuse && cgo

package fusekit

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

// SubtreeHost is the capability a mux native root's filesystem exposes for
// live tenant lifecycle: Attach serves fs under the root's depth-1 name with
// no kernel involvement, Detach removes it (later lookups ENOENT; in-flight
// ops complete), Attached reports membership. Config.FS built for
// ContentModeMux must implement it. Attached children never receive
// Init/Destroy — the fuse host delivers those to the root exactly once.
type SubtreeHost interface {
	Attach(name string, fs fuse.FileSystemInterface) error
	Detach(name string)
	Attached(name string) bool
}

// HandleReleaser is an optional child-filesystem capability the mux teardown
// invokes right after Detach: close every kernel-visible handle the child still
// holds. Once detached the child is unroutable through the mux root, so the
// kernel's later Release/Releasedir/Flush on its open fds would ENOENT — leaking
// the raw backing fd for the holder's lifetime and dropping a dirty synth
// entry's scheduled write-through. ReleaseAll closes each tracked fd through the
// child's own Release path so those write-throughs still schedule; the caller
// then drains them with FlushWithin. A child that mints no kernel handles (or
// none survive a detach) need not implement it.
type HandleReleaser interface {
	ReleaseAll()
}

// muxDetachDrain bounds teardownMux's post-Detach write-through drain: after
// ReleaseAll schedules a still-open dirty synth handle's write-through, the
// child's FlushWithin waits up to this for it to reach the consumer. Best-effort
// — the worker persists past the bound and the durable backing file remains the
// source of truth — and sized so the pre-teardown Drain (drainGrace) plus this
// plus the last-child unmountGrace stay within the holder's OpUnmount budget.
// Var so tests can shrink it.
var muxDetachDrain = 5 * time.Second

// muxTree is one native mux mount: the root handle, its SubtreeHost, the
// option fingerprint every later tenant must match, and the attached child
// filesystems keyed by their subtree dir.
type muxTree struct {
	handle           *Handle
	host             SubtreeHost
	attrCache        bool
	attrCacheTimeout time.Duration
	children         map[string]fuse.FileSystemInterface // subtree dir -> attached child FS
}

// MountSet is a registry of in-process mounts satisfying mountd.Host. Specs
// with a MuxRoot share ONE native mount per root and register their dirs as
// logical subtrees of it; everything else gets its own kernel mount, exactly
// as before.
type MountSet struct {
	// Build returns the Config to mount for a spec; an error fails the mount
	// loudly (a fallback passthrough would serve the wrong bytes).
	Build func(spec MountSpec) (Config, error)

	// StateFn reports whether dir is a mountpoint (mounted) and whether it is
	// serving (alive); the holder keys on each half — never collapse to one bool.
	// Subtree dirs are answered from the tree index instead (State).
	StateFn func(base, dir string) (mounted, alive bool)

	mu       sync.Mutex
	mounts   map[string]*Handle
	flushers map[string]Flusher
	trees    map[string]*muxTree // MuxRoot -> native mount
	treeDirs map[string]string   // subtree dir -> its MuxRoot
	// pending maps a dir whose graceful unmount is still in flight
	// (ErrTeardownPending) to its resolution channel; TeardownDone pops it.
	pending map[string]<-chan struct{}
}

// mountFn seams Mount so registry tests exercise the tree lifecycle without
// kernel mounts.
var mountFn = Mount

// subtreeProbes joins bounded subtree liveness opendirs per dir: the probe
// traverses the native NFS mount, which can wedge, and a wedged subtree must
// cost one detached goroutine — never a parked State caller.
var subtreeProbes StatProbes[bool]

// initLocked lazily allocates the registry maps. Caller holds mu.
func (m *MountSet) initLocked() {
	if m.mounts == nil {
		m.mounts = map[string]*Handle{}
		m.flushers = map[string]Flusher{}
		m.trees = map[string]*muxTree{}
		m.treeDirs = map[string]string{}
	}
}

// Setup mounts base at dir and registers the handle; an already-mounted dir is
// a no-op. The live mount proves the process TCC grant. A spec with a MuxRoot
// routes into the tree lifecycle instead (setupMux). The mutex drops across
// Mount, so per-dir single-flight is the holder's claim gate, not this method.
func (m *MountSet) Setup(spec MountSpec) error {
	if spec.MuxRoot != "" {
		return m.setupMux(spec)
	}
	m.mu.Lock()
	m.initLocked()
	if _, ok := m.mounts[spec.Dir]; ok {
		m.mu.Unlock()
		return nil // already mounted
	}
	m.mu.Unlock()

	cfg, err := m.Build(spec)
	if err != nil {
		return err
	}
	if cfg.ReArmSignals == nil {
		cfg.ReArmSignals = spec.ReArmSignals
	}
	h, err := mountFn(cfg)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.mounts[spec.Dir] = h
	if f, ok := cfg.FS.(Flusher); ok {
		m.flushers[spec.Dir] = f
	}
	m.mu.Unlock()
	return nil
}

// setupMux registers spec.Dir as a subtree of its MuxRoot's ONE native mount:
// find-or-mount the root (a synthesized ContentModeMux spec through the same
// Build dispatch), refuse an option disagreement with ErrMuxMismatch (a
// tenant's options are never silently dropped), build the child through Build,
// attach it under its depth-1 name, and wait bounded on the child's own Ready
// — which probes through spec.Dir, verifying the attach end-to-end. As with
// Setup, per-dir (and per-root) single-flight is the holder's claim gate.
func (m *MountSet) setupMux(spec MountSpec) error {
	m.mu.Lock()
	m.initLocked()
	if _, ok := m.treeDirs[spec.Dir]; ok {
		m.mu.Unlock()
		return nil // already attached
	}
	t := m.trees[spec.MuxRoot]
	m.mu.Unlock()

	// A registered tree whose root is no longer a kernel mountpoint is a
	// force-unmount carcass: attaching into it can never come kernel-visible,
	// and with siblings still registered the last-child unmount never fires —
	// every re-issued tenant Setup would time out against it forever. Reap it
	// and fall through to a fresh root: re-issuing each tenant's Mount then
	// reassembles the pool, the self-reassembly contract the heal loop relies
	// on. (The option-mismatch check below deliberately runs only against a
	// LIVE root — a dead root's options die with it.)
	if t != nil && !mountedFn(spec.MuxRoot) {
		if err := m.reapDeadRoot(spec.MuxRoot, spec.Dir, t); err != nil {
			return err
		}
		t = nil
	}

	// attrCache/attrCacheTimeout are immutable after mountMuxRoot, so the
	// lock-free read is safe.
	if t != nil && (t.attrCache != spec.AttrCache || t.attrCacheTimeout != spec.AttrCacheTimeout) {
		return fmt.Errorf("%w: %s wants attrcache=%v timeout=%s but root %s is mounted with attrcache=%v timeout=%s; unmount the root to change options",
			ErrMuxMismatch, spec.Dir, spec.AttrCache, spec.AttrCacheTimeout, spec.MuxRoot, t.attrCache, t.attrCacheTimeout)
	}

	if t == nil {
		var err error
		if t, err = m.mountMuxRoot(spec); err != nil {
			return err
		}
	}

	cfg, err := m.Build(spec)
	if err != nil {
		return m.unwindEmptyRoot(spec.MuxRoot, spec.Dir, err)
	}
	name := filepath.Base(spec.Dir)
	if err := t.host.Attach(name, cfg.FS); err != nil {
		return m.unwindEmptyRoot(spec.MuxRoot, spec.Dir, err)
	}
	m.mu.Lock()
	t.children[spec.Dir] = cfg.FS
	m.treeDirs[spec.Dir] = spec.MuxRoot
	if f, ok := cfg.FS.(Flusher); ok {
		m.flushers[spec.Dir] = f
	}
	m.mu.Unlock()

	ready := cfg.Ready
	if ready == nil {
		ready = func() bool { return MountAlive(cfg.Base, cfg.Dir) }
	}
	start := time.Now()
	if live, _ := waitReady(ready, cfg.Wait, nil); !live {
		// Fail loud and leave no half-attached tenant: detach, and drop the
		// root too when this was its only child (teardownMux's own path).
		if terr := m.teardownMux(spec.MuxRoot, spec.Dir); terr != nil {
			return fmt.Errorf("subtree %s never came live and its teardown failed: %w", spec.Dir, terr)
		}
		return mountWaitErr(spec.Dir, time.Since(start), mountProven())
	}
	return nil
}

// mountMuxRoot establishes the ONE native mount for spec's MuxRoot,
// synthesizing its root MountSpec (ContentModeMux; AttrCache options from
// this first tenant's spec) through the same Build dispatch.
func (m *MountSet) mountMuxRoot(spec MountSpec) (*muxTree, error) {
	rootSpec := MountSpec{
		Base:             filepath.Dir(spec.MuxRoot),
		Dir:              spec.MuxRoot,
		Owner:            spec.Owner,
		ContentMode:      ContentModeMux,
		AttrCache:        spec.AttrCache,
		AttrCacheTimeout: spec.AttrCacheTimeout,
		ReArmSignals:     spec.ReArmSignals,
	}
	cfg, err := m.Build(rootSpec)
	if err != nil {
		return nil, err
	}
	if cfg.ReArmSignals == nil {
		cfg.ReArmSignals = rootSpec.ReArmSignals
	}
	host, ok := cfg.FS.(SubtreeHost)
	if !ok {
		return nil, fmt.Errorf("%w: Build(%s) returned %T, which does not host subtrees", ErrMuxMismatch, spec.MuxRoot, cfg.FS)
	}
	h, err := mountFn(cfg)
	if err != nil {
		return nil, err
	}
	t := &muxTree{
		handle:           h,
		host:             host,
		attrCache:        spec.AttrCache,
		attrCacheTimeout: spec.AttrCacheTimeout,
		children:         map[string]fuse.FileSystemInterface{},
	}
	m.mu.Lock()
	m.trees[spec.MuxRoot] = t
	m.mu.Unlock()
	return t, nil
}

// unwindEmptyRoot tears a just-established native root back down when the first
// tenant's Build or Attach failed before any child registered, so a transient
// failure — a content-bridge blip during that first manifest fetch — never
// strands a childless, registry-rowless mux root: one that later mounts refuse
// as a foreign mountpoint and that neither sweep nor unmountAll (they iterate
// the registry only) can ever reach. A root that already carries siblings is
// left up; their own teardown owns it. The caller holds the MuxRoot claim, so
// the child set is stable across the check. The original cause is returned,
// joined with any teardown failure (the ready-timeout path's precedent); a
// PENDING root unmount registers with the pending-teardown machinery under
// dir, exactly like every other pending case, so the server parks the claims
// instead of letting a retry stack onto the in-flight unmount.
func (m *MountSet) unwindEmptyRoot(root, dir string, cause error) error {
	m.mu.Lock()
	t := m.trees[root]
	empty := t != nil && len(t.children) == 0
	m.mu.Unlock()
	if !empty {
		return cause
	}
	if err := t.handle.Unmount(); err != nil {
		if errors.Is(err, ErrTeardownPending) {
			m.registerPending(dir, t.handle.UnmountDone(), func() {
				if mountedFn(root) {
					return // final wedge: the tree stays registered; a later tenant's unwind retries
				}
				m.mu.Lock()
				if m.trees[root] == t {
					delete(m.trees, root)
				}
				m.mu.Unlock()
				reapServers(root)
			})
		}
		return errors.Join(cause, fmt.Errorf("tear down empty mux root %s after a failed first tenant: %w", root, err))
	}
	m.mu.Lock()
	delete(m.trees, root)
	m.mu.Unlock()
	return cause
}

// reapDeadRoot tears down the in-memory remains of a native mux root whose
// kernel mount was removed externally (a force-unmount out from under the
// holder). The tree is dropped with its subtree index entries first, each
// stranded child is released (the kernel can no longer deliver Release through
// the dead mount, so tracked fds and pending write-through are drained here),
// and the handle's unmount reaps the orphaned server process. An unmount error
// is returned loud — but the tree stays dropped either way: our mount is gone,
// so the registry must never keep saying otherwise. A PENDING unmount
// registers its resolution under dir — the tenant Setup driving this reap —
// so the server's park confirms the call's return before the final fence
// release; resolution only reaps the orphaned server (the tree is already
// dropped). The caller holds the per-root single-flight claim, so the tree
// cannot change under the reap.
func (m *MountSet) reapDeadRoot(root, dir string, t *muxTree) error {
	m.mu.Lock()
	delete(m.trees, root)
	var children []fuse.FileSystemInterface
	for d, r := range m.treeDirs {
		if r != root {
			continue
		}
		children = append(children, t.children[d])
		delete(m.treeDirs, d)
		delete(m.flushers, d)
	}
	m.mu.Unlock()
	for _, child := range children {
		releaseDetachedChild(child)
	}
	if err := t.handle.Unmount(); err != nil {
		if errors.Is(err, ErrTeardownPending) {
			m.registerPending(dir, t.handle.UnmountDone(), func() {
				if mountedFn(root) {
					return // reappeared mounted: leave it to the next probe
				}
				reapServers(root)
			})
		}
		return fmt.Errorf("reap dead mux root %s: %w", root, err)
	}
	return nil
}

// Drain blocks up to grace for dir's filesystem to flush pending background
// write-through before teardown.
func (m *MountSet) Drain(dir string, grace time.Duration) {
	m.mu.Lock()
	f := m.flushers[dir]
	m.mu.Unlock()
	if f != nil {
		f.FlushWithin(grace)
	}
}

// Teardown unmounts dir's registered mount, GRACEFULLY ONLY — it never
// force-unmounts (the only force in the fleet is the holder's fenced, proven-dead
// pre-mount/replay clear). A registered subtree detaches logically
// (teardownMux); an unregistered dir that is a direct child of a live mux
// root is a not-mounted no-op. A registered plain mount unmounts gracefully
// through its handle; a wedged unmount (error ⟺ still mounted) RESTORES the
// handle and flusher so the provider stays truthful — still mounted ⟺ still
// has the handle — and the next teardown retries gracefully. An unregistered,
// handle-less dir that is still a mountpoint (a prior-run carcass) is left in
// place and surfaced as ErrUnmountWedged, so a live mount is never treated as
// torn down.
func (m *MountSet) Teardown(base, dir string) error {
	m.mu.Lock()
	if root, ok := m.treeDirs[dir]; ok {
		m.mu.Unlock()
		return m.teardownMux(root, dir)
	}
	_, underLiveRoot := m.trees[filepath.Dir(dir)]
	h, ok := m.mounts[dir]
	f := m.flushers[dir]
	delete(m.mounts, dir)
	delete(m.flushers, dir)
	m.mu.Unlock()
	if ok {
		err := h.Unmount()
		if err != nil {
			// Blind restore is race-free: the holder's per-dir claim
			// single-flights Teardown against Setup (see Setup).
			m.mu.Lock()
			m.mounts[dir] = h
			if f != nil {
				m.flushers[dir] = f
			}
			m.mu.Unlock()
			if errors.Is(err, ErrTeardownPending) {
				m.registerPending(dir, h.UnmountDone(), func() {
					if mountedFn(dir) {
						return // final wedge: the restored handle stays; the next teardown retries
					}
					// The parked unmount landed late: the restored handle is a lie now.
					m.mu.Lock()
					if m.mounts[dir] == h {
						delete(m.mounts, dir)
						delete(m.flushers, dir)
					}
					m.mu.Unlock()
					reapServers(dir)
				})
			}
		}
		return err
	}
	if underLiveRoot {
		return nil // not a mountpoint; the live native mount is its siblings'
	}
	if !mountedFn(dir) {
		return nil
	}
	return fmt.Errorf("%w: carcass at %s left in place (only the pre-mount/replay carcass clear may force)", ErrUnmountWedged, dir)
}

// teardownMux detaches dir from its native root; the last child gracefully
// unmounts the root itself (a wedge surfaces as ErrUnmountWedged and the tree
// stays registered — the root is still mounted, so the registry must keep
// saying so). A subtree is never MNT_FORCEd: detach has no kernel side.
func (m *MountSet) teardownMux(root, dir string) error {
	m.mu.Lock()
	t := m.trees[root]
	delete(m.treeDirs, dir)
	delete(m.flushers, dir)
	var child fuse.FileSystemInterface
	last := false
	if t != nil {
		child = t.children[dir]
		delete(t.children, dir)
		last = len(t.children) == 0
	}
	m.mu.Unlock()
	if t == nil {
		return nil
	}
	t.host.Detach(filepath.Base(dir))
	releaseDetachedChild(child)
	if !last {
		return nil
	}
	if err := t.handle.Unmount(); err != nil {
		if errors.Is(err, ErrTeardownPending) {
			m.registerPending(dir, t.handle.UnmountDone(), func() {
				if mountedFn(root) {
					return // final wedge: the tree stays registered; the next last-detach retries
				}
				m.mu.Lock()
				if m.trees[root] == t {
					delete(m.trees, root)
				}
				m.mu.Unlock()
				reapServers(root)
			})
		}
		return err
	}
	m.mu.Lock()
	delete(m.trees, root)
	m.mu.Unlock()
	return nil
}

// registerPending records dir's in-flight teardown for TeardownDone and
// resolves the registry once the parked unmount CALL returns (done is the
// handle's per-call UnmountDone channel — never the serve loop's, which can
// close while the call is still blocked) — dropping the restored handle (or
// tree) when the unmount landed late, so the provider stays truthful.
// TeardownDone hands out a channel that closes only AFTER resolve ran: there
// is exactly ONE release owner — the server's fence watcher is sequenced
// strictly after this reconciliation, so a new Setup can never adopt a stale
// handle that reconciliation then deletes.
func (m *MountSet) registerPending(dir string, done <-chan struct{}, resolve func()) {
	resolved := make(chan struct{})
	m.mu.Lock()
	if m.pending == nil {
		m.pending = map[string]<-chan struct{}{}
	}
	m.pending[dir] = resolved
	m.mu.Unlock()
	go func() {
		<-done
		resolve()
		close(resolved)
	}()
}

// TeardownDone pops dir's in-flight teardown resolution channel (nil when
// none is pending) — the mountd.TeardownPender capability the server parks
// its lease fence and claims on (P-8).
func (m *MountSet) TeardownDone(dir string) <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := m.pending[dir]
	delete(m.pending, dir)
	return ch
}

// releaseDetachedChild closes a just-detached child's kernel-visible handles
// before it becomes unreachable. Detach has already removed it from the mux
// root, so the kernel's later Release/Releasedir/Flush on its open fds would
// route to ENOENT — the raw backing fd would leak for the holder's lifetime and
// a dirty synth entry's write-through would never be scheduled. ReleaseAll
// closes each tracked fd through the child's own Release path (which schedules
// those write-throughs); FlushWithin then drains them, bounded, so an in-flight
// commit still lands before the mount goes away. A child with no such
// capability (it minted no kernel handles) is a no-op.
func releaseDetachedChild(child fuse.FileSystemInterface) {
	if r, ok := child.(HandleReleaser); ok {
		r.ReleaseAll()
	}
	if f, ok := child.(Flusher); ok {
		f.FlushWithin(muxDetachDrain)
	}
}

// State reports the (mounted, alive) pair. A registered subtree answers from
// the tree index: mounted = the native root is a kernel mountpoint AND the
// tenant is attached; alive additionally requires a bounded opendir of the
// subtree through the mount. Everything else defers to StateFn.
func (m *MountSet) State(base, dir string) (mounted, alive bool) {
	m.mu.Lock()
	root, isSubtree := m.treeDirs[dir]
	t := m.trees[root]
	m.mu.Unlock()
	if !isSubtree {
		return m.StateFn(base, dir)
	}
	if t == nil || !mountedFn(root) || !t.host.Attached(filepath.Base(dir)) {
		return false, false
	}
	return true, opendirWithin(dir)
}

// HoldsMuxRoot reports whether a native mux root is still held — its muxTree is
// registered — regardless of whether any subtree row references it. A wedged
// last-child unmount keeps the tree (teardownMux returns ErrUnmountWedged and
// leaves m.trees intact because the kernel mount survived) after the holder has
// already deregistered the subtree row. This is how the holder learns the root
// is still ours, so a later tenant re-attaches to the surviving mount instead of
// bouncing ClassForeignMount.
func (m *MountSet) HoldsMuxRoot(root string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.trees[root]
	return ok
}

// opendirWithin is the bounded subtree liveness probe: the dir answers an
// opendir + one readdir through the native mount within statProbeTimeout. A
// timed-out probe reads NOT alive — fail-closed, a wedged mount never passes
// for healthy.
func opendirWithin(dir string) bool {
	alive, ok := subtreeProbes.Do(dir, statProbeTimeout, func() bool {
		f, err := os.Open(dir)
		if err != nil {
			return false
		}
		defer func() { _ = f.Close() }()
		_, err = f.Readdirnames(1)
		return err == nil || errors.Is(err, io.EOF)
	})
	return ok && alive
}
