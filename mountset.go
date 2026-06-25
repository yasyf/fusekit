//go:build fuse && cgo

// This file holds MountSet, the registry that drives N in-process mounts behind
// the mount-holder's Host seam. It is the lifecycle half of a mirror provider
// (Setup/Teardown/the mounts registry + everMountedLive), reshaped so each
// consumer supplies its own per-(base,dir) Config (Build) and liveness pair
// (State), and fusekit owns the registry, the idempotent-remount, and the
// bounded teardown.

package fusekit

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// MountSet is a registry of in-process mounts that satisfies the mount-holder's
// host seam: its Setup, Teardown, and State methods match
// mountd.Host{ Setup(base,dir) error; Teardown(base,dir) error; State(base,dir)
// (mounted, alive bool) }. The compile-time interface assertion
// `var _ mountd.Host = (*MountSet)(nil)` lives in the mountd package
// (mountset_assert_fuse.go); fusekit cannot import mountd here without a
// dependency cycle, so the contract is held structurally on this side.
//
// Note: a *MountSet (pointer) satisfies the host seam, not a MountSet value —
// the registry mutex and map cannot be copied.
//
// Field naming: the host seam exposes State as a METHOD, so the consumer-
// supplied state function cannot also be a field named State (Go forbids a
// field and method sharing a name). The function is therefore the StateFn
// field, and the State method delegates to it.
type MountSet struct {
	// Build returns the Config to mount for a spec, or fails the mount loudly
	// (e.g. the consumer's content is unreachable, so a passthrough would serve
	// the wrong bytes). It is called once per first Setup of a dir; an
	// already-mounted dir is a no-op remount.
	Build func(spec MountSpec) (Config, error)

	// StateFn reports the (mounted, alive) state pair for a (base, dir): mounted
	// is whether dir is a mountpoint at all, alive whether it is serving. The
	// State method delegates to it. The pair is load-bearing — the holder keys
	// foreign-mount refusal on mounted alone, the unmount no-op on !mounted,
	// and idempotent mount/list on both — so both halves must be reported
	// independently, never collapsed to one bool.
	StateFn func(base, dir string) (mounted, alive bool)

	mu       sync.Mutex
	mounts   map[string]*Handle
	flushers map[string]Flusher // dir -> its FS, when it drains before teardown
}

// Setup mounts base at dir and registers the handle, or no-ops if dir is
// already mounted in this set (idempotent remount). It mirrors cc-pool's
// FuseProvider.Setup registry insert, and the live mount proves the process TCC
// grant via Mount. The registry mutex is dropped across the Mount I/O, so this
// does not by itself serialize two concurrent Setups of the same dir — the
// mount-holder's per-dir claim gate is what guarantees single-flight; MountSet
// is only ever driven from behind it.
func (m *MountSet) Setup(spec MountSpec) error {
	m.mu.Lock()
	if m.mounts == nil {
		m.mounts = map[string]*Handle{}
		m.flushers = map[string]Flusher{}
	}
	if _, ok := m.mounts[spec.Dir]; ok {
		m.mu.Unlock()
		return nil // already mounted
	}
	m.mu.Unlock()

	cfg, err := m.Build(spec)
	if err != nil {
		return err
	}
	h, err := Mount(cfg)
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

// Drain blocks up to grace for dir's filesystem to flush pending background
// write-through before teardown. A dir with no draining FS returns at once.
func (m *MountSet) Drain(dir string, grace time.Duration) {
	m.mu.Lock()
	f := m.flushers[dir]
	m.mu.Unlock()
	if f != nil {
		f.FlushWithin(grace)
	}
}

// Teardown unmounts dir's registered mount bounded (Handle.Unmount) and drops
// it from the registry. A dir not in the registry is torn down best-effort with
// a forced kernel unmount (it may be a carcass left by a prior run), then
// verified: an unmount that did not take returns ErrUnmountWedged so the caller
// never treats a live mount as gone. It mirrors cc-pool's FuseProvider.Teardown
// minus the mirror-specific write-through drain (that stays app-side).
func (m *MountSet) Teardown(base, dir string) error {
	m.mu.Lock()
	h, ok := m.mounts[dir]
	delete(m.mounts, dir)
	delete(m.flushers, dir)
	m.mu.Unlock()
	if ok {
		return h.Unmount()
	}
	// Not ours (e.g. left over from a prior run): forced best-effort unmount,
	// then an honest mountpoint re-check.
	_ = unix.Unmount(dir, unix.MNT_FORCE)
	if Mounted(dir) {
		return fmt.Errorf("%w: %s; refusing to treat it as torn down", ErrUnmountWedged, dir)
	}
	return nil
}

// State reports the (mounted, alive) pair for a (base, dir) by delegating to
// the StateFn field. It is the method the mount-holder's host seam requires;
// see the type doc for why the field is named StateFn rather than State.
func (m *MountSet) State(base, dir string) (mounted, alive bool) {
	return m.StateFn(base, dir)
}
