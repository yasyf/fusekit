//go:build darwin

package fusekit

import (
	"fmt"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// MountedCheck reports whether dir is a mountpoint, by membership in the
// kernel's cached mount table. Getfsstat(MNT_NOWAIT) returns the in-kernel
// snapshot and cannot block — an lstat of dir would resolve INTO a fuse-t
// mirror's NFS-backed fs and hang forever on a wedged mount. dir is never
// statted, realpathed, or normalized (callers hash the exact dir string). A
// Getfsstat failure is returned, never collapsed: it is an UNDETERMINED
// verdict a teardown verification must fail closed on.
func MountedCheck(dir string) (bool, error) {
	n, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return false, fmt.Errorf("getfsstat: %w", err)
	}
	// +4 headroom for a mount racing in between the sizing call and the fill.
	table := make([]unix.Statfs_t, n+4)
	n, err = unix.Getfsstat(table, unix.MNT_NOWAIT)
	if err != nil {
		return false, fmt.Errorf("getfsstat: %w", err)
	}
	return mountpointIn(table[:n], mountCandidates(dir)), nil
}

// Mounted is MountedCheck with the error collapsed to not-mounted — the
// liveness fail direction. Teardown verification uses MountedCheck and fails
// toward wedged instead.
func Mounted(dir string) bool {
	mounted, err := MountedCheck(dir)
	return err == nil && mounted
}

// mountpointIn reports whether any entry's f_mntonname equals a candidate;
// f_mntfromname (the mount source) is deliberately never matched.
func mountpointIn(table []unix.Statfs_t, candidates []string) bool {
	for i := range table {
		name := unix.ByteSliceToString(table[i].Mntonname[:])
		for _, c := range candidates {
			if name == c {
				return true
			}
		}
	}
	return false
}

// mountCandidates returns the f_mntonname spellings to match: the cleaned dir,
// plus resolved-parent + base when the parent resolves differently (the kernel
// records a $TMPDIR mount under the firmlinked /private/var/... spelling).
// Only the parent is ever resolved — resolving dir would touch the mount — and
// the result is compare-only, never stored or hashed.
func mountCandidates(dir string) []string {
	dir = filepath.Clean(dir)
	candidates := []string{dir}
	if parent, err := filepath.EvalSymlinks(filepath.Dir(dir)); err == nil {
		if alt := filepath.Join(parent, filepath.Base(dir)); alt != dir {
			candidates = append(candidates, alt)
		}
	}
	return candidates
}
