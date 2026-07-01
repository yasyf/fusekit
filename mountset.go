//go:build fuse && cgo

package fusekit

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// MountSet is a registry of in-process mounts satisfying mountd.Host.
type MountSet struct {
	// Build returns the Config to mount for a spec; an error fails the mount
	// loudly (a fallback passthrough would serve the wrong bytes).
	Build func(spec MountSpec) (Config, error)

	// StateFn reports whether dir is a mountpoint (mounted) and whether it is
	// serving (alive); the holder keys on each half — never collapse to one bool.
	StateFn func(base, dir string) (mounted, alive bool)

	mu       sync.Mutex
	mounts   map[string]*Handle
	flushers map[string]Flusher
}

// Setup mounts base at dir and registers the handle; an already-mounted dir is
// a no-op. The live mount proves the process TCC grant. The mutex drops across
// Mount, so per-dir single-flight is the holder's claim gate, not this method.
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
// write-through before teardown.
func (m *MountSet) Drain(dir string, grace time.Duration) {
	m.mu.Lock()
	f := m.flushers[dir]
	m.mu.Unlock()
	if f != nil {
		f.FlushWithin(grace)
	}
}

// Teardown unmounts dir's registered mount. An unregistered dir (a prior-run
// carcass) gets a forced unmount then a re-check; a wedged unmount returns
// ErrUnmountWedged so a live mount is never treated as torn down.
func (m *MountSet) Teardown(base, dir string) error {
	m.mu.Lock()
	h, ok := m.mounts[dir]
	delete(m.mounts, dir)
	delete(m.flushers, dir)
	m.mu.Unlock()
	if ok {
		return h.Unmount()
	}
	// Error ignored: the Mounted re-check is the verdict.
	_ = unix.Unmount(dir, unix.MNT_FORCE)
	if Mounted(dir) {
		return fmt.Errorf("%w: %s; refusing to treat it as torn down", ErrUnmountWedged, dir)
	}
	return nil
}

// State reports the (mounted, alive) pair via StateFn.
func (m *MountSet) State(base, dir string) (mounted, alive bool) {
	return m.StateFn(base, dir)
}
