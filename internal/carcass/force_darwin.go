//go:build darwin

package carcass

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

// forceTimeout bounds the umount(8) -f process; a var so tests shrink it.
var forceTimeout = 5 * time.Second

// force runs the bounded carcass force: umount -f (matches the holder's own
// fuse-t unmount path; see ccn doc 501ce12) under a context deadline that
// kills the process, plus a wait bound for a kill the kernel cannot deliver
// (D-state). A timeout surfaces PendingForce: the unmount may still land, so
// the caller keeps the fence held until the process exits — never released
// under an unresolved force. Exit status is deliberately ignored: the
// caller's retried stat and mount-table re-check are the verdict.
func force(dir string) error {
	ctx, cancel := context.WithTimeout(context.Background(), forceTimeout)
	cmd := exec.CommandContext(ctx, "umount", "-f", dir)
	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("%w: start umount -f %s: %v", ErrWedged, dir, err)
	}
	waited := make(chan struct{})
	go func() {
		defer cancel()
		_ = cmd.Wait()
		close(waited)
	}()
	select {
	case <-waited:
		return nil
	case <-time.After(forceTimeout + time.Second):
		return &PendingForce{Dir: dir, Done: waited}
	}
}
