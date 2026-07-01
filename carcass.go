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

// ClearCarcass force-unmounts the dead mount a killed process left at dir: a
// stat answering ENOTCONN/EIO — or hanging, since fuse-t's NFS backend has no
// soft/timeout knobs — marks a carcass; the platform reaper clears it,
// verified by one retried stat. A healthy or absent path is a no-op; returns
// ErrUnmountWedged when the carcass does not clear.
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
// is not wedged); ENOTCONN/EIO or no answer within statProbeTimeout mark a
// carcass. On a truly wedged carcass the probe goroutine never exits — the
// leak is exactly the condition the bound contains.
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
