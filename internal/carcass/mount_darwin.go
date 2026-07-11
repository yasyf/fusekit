//go:build darwin

package carcass

import (
	"path/filepath"

	"golang.org/x/sys/unix"
)

// lookupMount finds dir in the kernel's cached mount table and pins its
// identity. Getfsstat(MNT_NOWAIT) returns the in-kernel snapshot and cannot
// block — an lstat of dir would resolve INTO a dead mount and hang.
func lookupMount(dir string) (mountID, bool) {
	n, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return mountID{}, false
	}
	// +4 headroom for a mount racing in between the sizing call and the fill.
	table := make([]unix.Statfs_t, n+4)
	n, err = unix.Getfsstat(table, unix.MNT_NOWAIT)
	if err != nil {
		return mountID{}, false
	}
	for i := range table[:n] {
		name := unix.ByteSliceToString(table[i].Mntonname[:])
		for _, c := range mountCandidates(dir) {
			if name == c {
				return mountID{
					fsidA:  int64(table[i].Fsid.Val[0]),
					fsidB:  int64(table[i].Fsid.Val[1]),
					fstype: unix.ByteSliceToString(table[i].Fstypename[:]),
					source: unix.ByteSliceToString(table[i].Mntfromname[:]),
				}, true
			}
		}
	}
	return mountID{}, false
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
