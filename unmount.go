package fusekit

import (
	"fmt"
	"time"
)

// Deliberately untagged: the pure-Go daemon build must be able to force-unmount
// a dead holder's carcasses itself.

// forceUnmountTimeout bounds one ForceUnmount call; a var so tests can shrink it.
var forceUnmountTimeout = 5 * time.Second

// forceUnmountProbes joins concurrent and repeated force-unmounts per dir.
// Never shared with aliveProbes/deepProbes: a parked MNT_FORCE on a wedged
// carcass must not block — or be answered by — a liveness stat. The join pins
// per-tick and per-breaker-window re-issues to the one already-parked
// goroutine, so a permanently-wedged carcass cannot leak a goroutine per tick.
var forceUnmountProbes StatProbes[error]

// ForceUnmount force-unmounts dir via the platform unmountFn, never through
// the mount holder — it exists for a dead holder's orphaned carcasses.
// A wedged NFS carcass can block the call forever in uninterruptible wait, so
// it runs behind forceUnmountProbes and returns ErrForceUnmountTimeout when
// forceUnmountTimeout elapses.
func ForceUnmount(dir string) error {
	err, ok := forceUnmountProbes.Do(dir, forceUnmountTimeout, func() error {
		return unmountFn(dir)
	})
	if !ok {
		return fmt.Errorf("%w: %s", ErrForceUnmountTimeout, dir)
	}
	if err != nil {
		return fmt.Errorf("force unmount %s: %w", dir, err)
	}
	return nil
}
