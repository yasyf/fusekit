package fusekit

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// This file holds the untagged pre-mount carcass cleanup, folded in from
// cc-notes (clearCarcass). It compiles in every build variant: a fresh mount
// must clear the dead carcass a killed holder left behind before it can take
// over the mountpoint, and that cleanup needs no fuse runtime.

// ClearCarcass force-unmounts the dead mount a killed process left at dir: a
// stat that answers ENOTCONN/EIO — or does not answer at all (fuse-t's NFS
// backend has no soft/timeout knobs, so a dead server can hang the stat) —
// marks a carcass; the platform reaper (umount -f on darwin, fusermount3 -uz
// on other) clears it, verified by one retried stat. A healthy or absent path
// is a no-op (statAnswers treats ENOENT as healthy). Returns ErrUnmountWedged
// when the carcass does not clear.
func ClearCarcass(dir string) error {
	if statAnswers(dir) {
		return nil
	}
	forceReap(dir)
	if statAnswers(dir) {
		return nil
	}
	return fmt.Errorf("%w: dead mount at %s did not clear", ErrUnmountWedged, dir)
}

// statAnswers reports a healthy, bounded stat of p. ENOENT is healthy — the
// path simply does not exist, which the caller rejects on its own terms;
// ENOTCONN/EIO and a stat that never answers within statProbeTimeout are
// carcass signs. The probe goroutine's exit is the stat returning; for a truly
// wedged carcass that is never — exactly the condition the bound contains.
func statAnswers(p string) bool {
	ch := make(chan error, 1)
	go func() {
		_, err := os.Stat(p)
		ch <- err
	}()
	select {
	case err := <-ch:
		return !errors.Is(err, unix.ENOTCONN) && !errors.Is(err, unix.EIO)
	case <-time.After(statProbeTimeout):
		return false
	}
}
