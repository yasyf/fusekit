//go:build !darwin

package carcass

import (
	"context"
	"fmt"
	"os/exec"
	"time"
)

// forceTimeout bounds the fusermount3 process; a var so tests shrink it.
var forceTimeout = 5 * time.Second

// force best-effort unmounts a carcass, bounded; the caller's retried stat
// and mount re-check are the verdict. A timeout surfaces PendingForce so the
// caller keeps the fence held until the process exits.
func force(dir string) error {
	ctx, cancel := context.WithTimeout(context.Background(), forceTimeout)
	cmd := exec.CommandContext(ctx, "fusermount3", "-uz", dir)
	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("%w: start fusermount3 -uz %s: %v", ErrWedged, dir, err)
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
