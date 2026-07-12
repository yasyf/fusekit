//go:build !darwin

package fusekit

import (
	"errors"
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// MountedCheck reports whether dir is a mountpoint: its st_dev differs from
// its parent's iff a filesystem is mounted on it. ENOENT is a definitive
// not-mounted; any other stat failure is returned — an UNDETERMINED verdict a
// teardown verification must fail closed on.
func MountedCheck(dir string) (bool, error) {
	dir = filepath.Clean(dir)
	var st, parent unix.Stat_t
	if err := unix.Stat(dir, &st); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return false, nil
		}
		return false, fmt.Errorf("stat %s: %w", dir, err)
	}
	if err := unix.Stat(filepath.Dir(dir), &parent); err != nil {
		return false, fmt.Errorf("stat %s: %w", filepath.Dir(dir), err)
	}
	return st.Dev != parent.Dev, nil
}

// Mounted is MountedCheck with the error collapsed to not-mounted — the
// liveness fail direction, matching darwin.
func Mounted(dir string) bool {
	mounted, err := MountedCheck(dir)
	return err == nil && mounted
}
