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
// and mount re-check are the verdict.
func force(dir string) error {
	ctx, cancel := context.WithTimeout(context.Background(), forceTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "fusermount3", "-uz", dir)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%w: start fusermount3 -uz %s: %v", ErrWedged, dir, err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
		return nil
	case <-time.After(forceTimeout + time.Second):
		return fmt.Errorf("%w: fusermount3 -uz %s did not return within %s; keeping the carcass fenced", ErrWedged, dir, forceTimeout)
	}
}
