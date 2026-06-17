//go:build fuse && cgo

// This file holds MountSet, the registry that drives N in-process mounts behind
// the mount-holder's Host seam. It is the lifecycle half of cc-pool's
// FuseProvider (Setup/Teardown/the mounts registry + everMountedLive), reshaped
// so each consumer supplies its own per-(base,dir) Config (Build) and liveness
// pair (State), and fusekit owns the registry, the idempotent-remount, and the
// bounded teardown.

package fusekit

import (
	"fmt"
	"sync"

	"golang.org/x/sys/unix"
)

// MountSet is a registry of in-process mounts that satisfies the mount-holder's
// host seam: its Setup, Teardown, and State methods match
// mountd.Host{ Setup(base,dir) error; Teardown(base,dir) error; State(base,dir)
// (mounted, alive bool) }. Phase 2 (the mountd package) will add the
// compile-time interface assertion `var _ mountd.Host = (*MountSet)(nil)`;
// fusekit cannot import mountd here without a dependency cycle, so the contract
// is held structurally for now — the method set is exactly Setup/Teardown/State
// with those signatures.
//
// Note: a *MountSet (pointer) satisfies the host seam, not a MountSet value —
// the registry mutex and map cannot be copied.
//
// Field naming: the host seam exposes State as a METHOD, so the consumer-
// supplied state function cannot also be a field named State (Go forbids a
// field and method sharing a name). The function is therefore the Probe field,
// and the State method delegates to it.
type MountSet struct {
	// Build returns the Config to mount for a (base, dir). It is called once
	// per first Setup of a dir; an already-mounted dir is a no-op remount.
	Build func(base, dir string) Config

	// Probe reports the (mounted, alive) state pair for a (base, dir): mounted
	// is whether dir is a mountpoint at all, alive whether it is serving. The
	// State method delegates to it. The pair is load-bearing — the holder keys
	// foreign-mount refusal on mounted alone, the unmount no-op on !mounted,
	// and idempotent mount/list on both — so both halves must be reported
	// independently, never collapsed to one bool.
	Probe func(base, dir string) (mounted, alive bool)

	mu     sync.Mutex
	mounts map[string]*Handle
}

// Setup mounts base at dir and registers the handle, or no-ops if dir is
// already mounted in this set (idempotent remount). It mirrors cc-pool's
// FuseProvider.Setup registry insert: a concurrent Setup of the same dir is
// serialized by the registry mutex, and the live mount proves the process TCC
// grant via Mount.
func (m *MountSet) Setup(base, dir string) error {
	m.mu.Lock()
	if m.mounts == nil {
		m.mounts = map[string]*Handle{}
	}
	if _, ok := m.mounts[dir]; ok {
		m.mu.Unlock()
		return nil // already mounted
	}
	m.mu.Unlock()

	h, err := Mount(m.Build(base, dir))
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.mounts[dir] = h
	m.mu.Unlock()
	return nil
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
// the Probe field. It is the method the mount-holder's host seam requires; see
// the type doc for why the field is named Probe rather than State.
func (m *MountSet) State(base, dir string) (mounted, alive bool) {
	return m.Probe(base, dir)
}
