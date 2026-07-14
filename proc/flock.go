package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sys/unix"
)

const flockPollInterval = 25 * time.Millisecond

// FlockHandle owns an acquired advisory lock.
type FlockHandle struct {
	f *os.File
}

// Release drops the lock; the lock file is left on disk on purpose: unlinking
// under flock races other processes that have it open.
func (h *FlockHandle) Release() {
	_ = unix.Flock(int(h.f.Fd()), unix.LOCK_UN)
	_ = h.f.Close()
}

// Flock takes an exclusive cross-process advisory lock on path. It polls
// rather than blocking in the syscall so ctx cancellation is observed and no
// goroutine leaks on a stuck holder.
func Flock(ctx context.Context, path string) (*FlockHandle, error) {
	//nolint:gosec // G703: callers pass lock paths they own, not user-tainted input
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create lock dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // G304: callers pass lock paths they own, not user input
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", path, err)
	}
	for {
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return &FlockHandle{f: f}, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) {
			_ = f.Close()
			return nil, fmt.Errorf("flock %s: %w", path, err)
		}
		select {
		case <-ctx.Done():
			_ = f.Close()
			return nil, fmt.Errorf("flock %s: %w", path, ctx.Err())
		case <-time.After(flockPollInterval):
		}
	}
}
