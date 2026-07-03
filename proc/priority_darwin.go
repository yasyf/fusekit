//go:build darwin

package proc

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Nice lowers the calling process's scheduling priority to n — classic Unix
// nice(2) semantics, inherited by children (for a mount holder, the per-mount
// NFS servers fuse-t spawns). A soft weight, deliberately NOT the Darwin
// background band: the band's CPU throttle + lowest I/O tier starve a
// data-plane server under load, and a self-set band cannot be cleared from
// outside the process. One-way for unprivileged processes: lowering priority
// back requires root, so pick n once at startup.
func Nice(n int) error {
	if err := unix.Setpriority(unix.PRIO_PROCESS, 0, n); err != nil {
		return fmt.Errorf("set nice %d: %w", n, err)
	}
	return nil
}
