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
// (D-state). A timeout keeps the carcass fenced — the caller defers and
// surfaces, never hangs replay. Exit status is deliberately ignored: the
// caller's retried stat and mount-table re-check are the verdict.
func force(dir string) error {
	ctx, cancel := context.WithTimeout(context.Background(), forceTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "umount", "-f", dir)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%w: start umount -f %s: %v", ErrWedged, dir, err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(forceTimeout + time.Second):
		return fmt.Errorf("%w: umount -f %s did not return within %s; keeping the carcass fenced", ErrWedged, dir, forceTimeout)
	}
}
