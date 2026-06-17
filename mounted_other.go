//go:build !darwin

package fusekit

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// Mounted reports whether dir is currently a mountpoint. On non-darwin
// (Linux fuse3) there is no Getfsstat-style cached mount table to consult, so
// it compares device ids: a directory's st_dev differs from its parent's iff
// a filesystem is mounted on it. Either stat failing reads false, matching the
// darwin implementation's error-as-not-mounted contract.
//
// This is the cc-notes device-id approach (devOf) reframed as a standalone
// predicate — instead of a pre-mount baseline it compares against the parent,
// which is the same observation a mountpoint makes: stat-ing the mountpoint
// resolves INTO the mounted fs, whose root device differs from the directory
// that backs it.
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
