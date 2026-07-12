//go:build fuse && cgo

package fusekit

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Mount-up bounds: an unproven TCC grant slows the first mount, so the probe
// waits the longer first-mount bound; proven grants get the shorter one.
const (
	probeWait      = 8 * time.Second
	probeFirstWait = 14 * time.Second
)

// HostProbe mounts, stats, and tears down a throwaway probe mount, reporting
// whether fuse works here. It must run in the process that will host real
// mounts: the fuse capability and the one-time macOS volume-access TCC grant
// are per-process, and a successful probe trips and proves the grant for every
// later mount. The returned error carries Mount's classification — hard
// ErrMountFailed vs presumed-TCC ErrMountNotLive. reArm rides the probe
// Config's ReArmSignals: the probe's post-ready signal.Reset strips the
// embedding app's subscriptions exactly like any other mount's, so no mount
// inside a holder process — probe included — may omit it.
func HostProbe(reArm func()) (bool, error) {
	tmp, err := os.MkdirTemp("", "fusekit-probe-")
	if err != nil {
		return false, fmt.Errorf("probe: make temp dir: %w", err)
	}
	defer os.RemoveAll(tmp)
	src := filepath.Join(tmp, "src")
	mnt := filepath.Join(tmp, "mnt")
	_ = os.MkdirAll(src, 0o700)
	_ = os.MkdirAll(mnt, 0o700)
	if err := os.WriteFile(filepath.Join(src, "probe"), []byte("ok"), 0o600); err != nil {
		return false, fmt.Errorf("probe: write probe file: %w", err)
	}

	h, err := Mount(probeConfig(src, mnt, reArm))
	if err != nil {
		return false, err
	}
	defer h.Unmount()
	if _, err := os.Stat(filepath.Join(mnt, "probe")); err != nil {
		return false, fmt.Errorf("probe mount came up but its stat failed: %w", err)
	}
	return true, nil
}

// probeConfig builds the throwaway probe mount's Config, force-stamping
// ReArmSignals — split out so the guard is testable without a kernel mount.
func probeConfig(src, mnt string, reArm func()) Config {
	return Config{
		Base:         src,
		Dir:          mnt,
		FS:           &probeFS{root: src},
		Options:      MountOptions{Volname: "fusekit-probe", NoBrowse: true}.Build(),
		Wait:         probeWait,
		FirstWait:    probeFirstWait,
		ReArmSignals: reArm,
	}
}
