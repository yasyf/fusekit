package fusekit

import (
	"errors"
	"fmt"
	"time"
)

// Untagged so every build variant compiles these sentinels: a non-fuse binary
// must errors.Is against errors that crossed the holder process boundary
// (mountd maps them onto wire error classes).
var (
	// ErrFuseUnavailable means a fuse-tagged binary could not load libfuse; Mount
	// recovers cgofuse's dlopen-failure panic into this sentinel.
	ErrFuseUnavailable = errors.New("fuse runtime unavailable")

	// ErrMountNotLive means a mount never came live in a process that has not yet
	// hosted one — on macOS almost always the missing one-time OS volume-access
	// grant.
	ErrMountNotLive = errors.New("fuse mount did not come up")

	// ErrMountTimeout means a mount timed out in a process that already hosted a
	// live one: the OS grant is proven, so this is transient fuse-t slowness that
	// callers retry — never convert the provider or surface the grant walkthrough.
	// Gap: no public API queries the grant, so a mid-process revocation still
	// reads as this until a holder restart resets the deduction.
	ErrMountTimeout = errors.New("fuse mount did not come up in time")

	// ErrMountFailed means the mount(2)/NFS call was rejected: the serving
	// goroutine exited before the mount came live. Never the OS grant — a pending
	// grant blocks the call rather than returning — so no grant walkthrough; the
	// cause is in the holder log.
	ErrMountFailed = errors.New("fuse mount failed")

	// ErrUnmountWedged means an unmount did not take: the dir is still a live
	// mountpoint and must not be treated as torn down (RemoveAll through it
	// would reach the backing base dir).
	ErrUnmountWedged = errors.New("unmount did not take")

	// ErrTeardownPending means a graceful unmount is STILL IN FLIGHT past its
	// grace: the outcome is unknown, and the parked call may land at any later
	// moment. Always wrapped alongside ErrUnmountWedged (the dir is still a
	// mountpoint). The holder keeps the dir's lease fence and claim until the
	// call resolves — never hand the dir to a new session mid-teardown.
	ErrTeardownPending = errors.New("graceful unmount still in flight; outcome unknown")

	// ErrLivenessTimeout means a bounded liveness stat of an existing mirror did
	// not answer in time: unresponsive but NOT proven dead. Fuse-t NFS can stall a
	// stat under load while the mirror is alive, so one timeout is not grounds to
	// remount over live sessions; definitive dead readings stay plain errors.
	ErrLivenessTimeout = errors.New("mirror liveness stat did not answer in time")

	// ErrMuxMismatch means a mux-mode spec cannot join its MuxRoot's already-
	// established native mount: its options (AttrCache/AttrCacheTimeout)
	// disagree with the ones the root was mounted with, or the root's built
	// filesystem does not host subtrees. Registry state, resolved by
	// unmounting the root and retrying — never a mount-liveness verdict, and
	// never silently ignored (a tenant's options must not be dropped).
	ErrMuxMismatch = errors.New("mux root options mismatch")
)

// Error text stays backend-neutral; surfacing the grant is the consumer's job
// (overlay.Backend.Enablement).
func mountWaitErr(accountDir string, waited time.Duration, proven bool) error {
	if !proven {
		return fmt.Errorf("%w: %s never became live; on macOS a process's first fuse mount is blocked pending a one-time OS volume-access grant that this failed attempt surfaces — mounts retry automatically once it is granted", ErrMountNotLive, accountDir)
	}
	return fmt.Errorf("%w: %s after %s; this process already hosts live mounts, so the OS grant is proven — transient fuse-t slowness, retrying", ErrMountTimeout, accountDir, waited)
}

func mountFailureErr(accountDir string, waited time.Duration, serveExited, proven bool) error {
	if serveExited {
		return fmt.Errorf("%w: %s (the mount call was rejected before the mirror came live — is fuse-t installed and loadable at CGOFUSE_LIBFUSE_PATH? the mount holder log carries the underlying cgofuse error)", ErrMountFailed, accountDir)
	}
	return mountWaitErr(accountDir, waited, proven)
}
