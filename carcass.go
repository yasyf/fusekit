package fusekit

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// Deliberately untagged: pre-mount carcass clearing needs no fuse runtime and
// must build in every variant.

// ClearCarcass force-unmounts the dead-mount carcass a killed process left at
// dir; a healthy or absent path is a no-op, and ErrUnmountWedged means the
// carcass did not clear. A hanging stat marks a carcass too: fuse-t's NFS
// backend has no soft/timeout knobs.
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

// statAnswers reports a healthy, bounded stat of p: ENOENT is healthy (absent
// is not wedged); a carcass errno (carcassErr) or no answer within
// statProbeTimeout marks a carcass.
func statAnswers(p string) bool {
	ch := make(chan error, 1)
	go func() {
		_, err := os.Stat(p)
		ch <- err
	}()
	select {
	case err := <-ch:
		return !carcassErr(err)
	case <-time.After(statProbeTimeout):
		return false
	}
}

// carcassErr reports a dead-server stat errno: ENOTCONN/EIO (severed
// transport) or EPERM/EACCES (an orphaned go-nfsv4 whose holder died answers
// every op with a permission error — the dead-holder incident signature).
func carcassErr(err error) bool {
	return errors.Is(err, unix.ENOTCONN) || errors.Is(err, unix.EIO) ||
		errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES)
}
