//go:build !darwin

package fusekit

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Mounted reports whether dir is a mountpoint: its st_dev differs from its parent's
// iff a filesystem is mounted on it. Stat failure reads as not mounted, matching darwin.
func Mounted(dir string) bool {
	dir = filepath.Clean(dir)
	var st, parent unix.Stat_t
	if err := unix.Stat(dir, &st); err != nil {
		return false
	}
	if err := unix.Stat(filepath.Dir(dir), &parent); err != nil {
		return false
	}
	return st.Dev != parent.Dev
}
