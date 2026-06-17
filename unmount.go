package fusekit

import (
	"fmt"
	"time"
)

// This file holds the untagged direct force-unmount primitive. It compiles in
// every build variant: the daemon (pure-Go default included) force-unmounts a
// dead holder's orphaned fuse carcasses ITSELF, without routing through the
// holder, the moment the holder dies — a wedged NFS carcass must never linger.
// The unmountFn seam it calls is platform-split (unmount_darwin.go /
// unmount_other.go); ErrForceUnmountTimeout lives in errors.go beside the
// other root sentinels.

// forceUnmountTimeout bounds one ForceUnmount syscall. A var, not a const, so
// tests can shrink it.
var forceUnmountTimeout = 5 * time.Second

// forceUnmountProbes joins concurrent and repeated force-unmounts per dir. Its
// own StatProbes instance, never shared with the stat or deep probes
// (aliveProbes/deepProbes): a parked MNT_FORCE against a
// permanently-wedged carcass must never block — or be answered by — a liveness
// stat behind its join, and vice versa. The join is what makes the
// at-most-one-parked-goroutine-per-carcass contract true: the daemon re-issues
// ForceUnmount against the same wedged dir every supervision tick
// (forceUnmountOrphans) and every breaker window (escalateWedgedRow), and each
// re-issue shares the single already-parked goroutine instead of spawning
// another, so a carcass the kernel will never MNT_FORCE cannot leak goroutines
// per tick.
var forceUnmountProbes StatProbes[error]

// ForceUnmount force-unmounts dir directly via the platform unmountFn
// (unix.Unmount(MNT_FORCE) on darwin, fusermount3 -uz on other), bounded by
// forceUnmountTimeout. It does NOT contact the mount holder: the daemon calls
// it on a holder's orphaned carcasses the moment the holder dies, when the dead
// holder can no longer perform the unmount itself. The call runs in a per-dir
// StatProbes goroutine behind the bound because a wedged NFS carcass can make
// the unmount block forever in uninterruptible wait — that must never hang the
// daemon's supervise goroutine, and repeated re-issues against the same carcass
// must share that one parked goroutine rather than each spawning one (see
// forceUnmountProbes). The probe goroutine exits if the call ever returns, even
// past the bound. Returns nil on a clean unmount, the wrapped error, or
// ErrForceUnmountTimeout when the bound elapses.
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
