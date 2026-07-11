//go:build !darwin

package carcass

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// lookupMount pins dir's identity by device number: st_dev differs from the
// parent's iff a filesystem is mounted on it. Stat failure reads not-mounted.
func lookupMount(dir string) (mountID, bool) {
	dir = filepath.Clean(dir)
	var st, parent unix.Stat_t
	if err := unix.Stat(dir, &st); err != nil {
		return mountID{}, false
	}
	if err := unix.Stat(filepath.Dir(dir), &parent); err != nil {
		return mountID{}, false
	}
	if st.Dev == parent.Dev {
		return mountID{}, false
	}
	return mountID{fsidA: int64(st.Dev)}, true
}
