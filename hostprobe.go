//go:build fuse && cgo

// This file holds HostProbe, the throwaway probe mount that confirms fuse works
// on this machine (and trips the one-time macOS "Network Volumes" privacy
// grant). It is the fuse build's port of cc-pool's probeFuse: capability and
// the TCC grant are per-process, so the probe must run in the process that will
// host real mounts.

package fusekit

import (
	"os"
	"path/filepath"
	"time"
)

// Probe mount-up bounds. The probe is (by construction) often the process's
// first mount, so it waits the longer first-mount bound for an unproven TCC
// grant; once proven, later probes use the shorter bound.
const (
	probeWait      = 8 * time.Second
	probeFirstWait = 14 * time.Second
)

// HostProbe attempts a throwaway in-process probe mount and reports whether it
// came up and served a stat. It mkdirs a temp src+mnt, writes a probe file into
// src, mounts a passthrough of src at mnt, stats the probe file through the
// mount, then tears the mount down. It must run in the process that will host
// real mounts: the fuse capability and the macOS "Network Volumes" TCC grant
// are per-process, and a successful probe proves the grant for every later
// mount in the process. Any failure (no fuse runtime, mkdir/mount/stat error)
// reads false.
func HostProbe() bool {
	tmp, err := os.MkdirTemp("", "fusekit-probe-")
	if err != nil {
		return false
	}
	defer os.RemoveAll(tmp)
	src := filepath.Join(tmp, "src")
	mnt := filepath.Join(tmp, "mnt")
	_ = os.MkdirAll(src, 0o700)
	_ = os.MkdirAll(mnt, 0o700)
	if err := os.WriteFile(filepath.Join(src, "probe"), []byte("ok"), 0o600); err != nil {
		return false
	}

	h, err := Mount(Config{
		Base:      src,
		Dir:       mnt,
		FS:        &probeFS{root: src},
		Options:   MountOptions{Volname: "fusekit-probe", NoBrowse: true}.Build(),
		Wait:      probeWait,
		FirstWait: probeFirstWait,
	})
	if err != nil {
		return false
	}
	defer h.Unmount()
	_, err = os.Stat(filepath.Join(mnt, "probe"))
	return err == nil
}
