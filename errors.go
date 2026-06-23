package fusekit

import (
	"errors"
	"fmt"
	"time"
)

// This file holds untagged sentinels for the fuse failure modes callers must
// classify without string matching (the mount-holder server maps them onto
// wire error classes). They compile in every build variant so a non-fuse
// binary can still errors.Is against errors that crossed a process boundary.
var (
	// ErrFuseUnavailable means the binary was built with fuse support but the
	// fuse runtime could not be brought up: cgofuse failed to dlopen the
	// libfuse implementation (libfuse-t.dylib on macOS, libfuse3 on Linux), so
	// it panicked on the first fuse call. Mount recovers that panic and wraps
	// it with this sentinel. It is distinct from mountd.ErrCannotHost, which is
	// the pure-build (no fuse tag) refusal — that binary has no fuse runtime to
	// load at all.
	ErrFuseUnavailable = errors.New("fuse runtime unavailable")

	// ErrMountNotLive means a fuse mount was issued but never came live in a
	// process that has NOT yet hosted any live mount — on macOS almost always
	// the one-time "Network Volumes" TCC grant. FuseProvider.Setup wraps its
	// mount-timeout error with it only while the grant is unproven; once any
	// mount in the process has come live, timeouts wrap ErrMountTimeout
	// instead.
	ErrMountNotLive = errors.New("fuse mount did not come up")

	// ErrMountTimeout means a fuse mount timed out in a process that has
	// ALREADY hosted a live mount, so the macOS "Network Volumes" TCC grant is
	// proven and this is transient fuse-t slowness — never the TCC condition.
	// Callers retry; they must never convert the provider or surface TCC
	// guidance for it. Honest gap: a grant revoked mid-process still reads as
	// this — established NFS mounts survive revocation, there is no public TCC
	// query API for Network Volumes, and attempting a mount is the only
	// observable — and a holder restart resets the deduction.
	ErrMountTimeout = errors.New("fuse mount did not come up in time")

	// ErrMountFailed means a fuse mount was rejected outright: host.Mount
	// returned (its serving goroutine exited) before the mount ever came live,
	// so the mount(2)/NFS call itself failed — fuse-t not installed or not
	// loadable, the kernel refusing the mount, a bad CGOFUSE_LIBFUSE_PATH. It is
	// NEVER the one-time "Network Volumes" TCC grant: a pending grant keeps the
	// mount call BLOCKED with the serving goroutine alive (surfacing as a
	// timeout wrapping ErrMountNotLive), it does not return. Callers must not
	// surface the TCC walkthrough for it; the real cause is in the holder log.
	ErrMountFailed = errors.New("fuse mount failed")

	// ErrUnmountWedged means an unmount did not take: the dir is still a live
	// mountpoint and must not be treated as torn down (RemoveAll through it
	// would reach the backing ~/.claude). FuseProvider.Teardown wraps its
	// refusal with it.
	ErrUnmountWedged = errors.New("unmount did not take")

	// ErrForceUnmountTimeout means a forced unmount syscall did not return
	// within the bound: the carcass is so wedged the kernel will not even
	// complete MNT_FORCE in time. The syscall runs inside a per-dir StatProbes
	// join, so repeated force-unmounts of the same wedged carcass share the
	// single parked goroutine (it exits if the kernel ever answers) — a single
	// wedged carcass parks at most one goroutine, never the caller, no matter
	// how many ticks re-issue against it.
	ErrForceUnmountTimeout = errors.New("forced unmount did not return in time")
)

// mountWaitErr composes FuseProvider.Setup's mount-up timeout error. proven
// reports whether this process has already hosted a live mount: unproven
// timeouts presume the one-time "Network Volumes" TCC grant and carry the
// walkthrough (wrapping ErrMountNotLive); proven timeouts are transient
// fuse-t slowness (wrapping ErrMountTimeout) and must never carry TCC
// guidance.
func mountWaitErr(accountDir string, waited time.Duration, proven bool) error {
	if !proven {
		return fmt.Errorf("%w: %s (presumed missing macOS TCC grant: this failed attempt is what creates the toggle under System Settings ▸ Privacy & Security ▸ Network Volumes — grant Network Volumes access once and mounts retry automatically)", ErrMountNotLive, accountDir)
	}
	return fmt.Errorf("%w: %s after %s; this process already hosts live mounts, so the Network Volumes grant is proven — transient fuse-t slowness, retrying", ErrMountTimeout, accountDir, waited)
}

// mountFailureErr composes Mount's error for a mount that did not come live.
// serveExited reports whether host.Mount returned before the mount came up: a
// serve-exit is a hard mount(2) rejection (ErrMountFailed) — never a pending
// Network Volumes prompt, which keeps the call blocked — so it bypasses the
// proven/unproven TCC split entirely. A timeout with the serving goroutine
// still alive routes to mountWaitErr (presumed-TCC vs proven-slowness); proven
// is forwarded there.
func mountFailureErr(accountDir string, waited time.Duration, serveExited, proven bool) error {
	if serveExited {
		return fmt.Errorf("%w: %s (the mount call was rejected before the mirror came live — is fuse-t installed and loadable at CGOFUSE_LIBFUSE_PATH? the mount holder log carries the underlying cgofuse error)", ErrMountFailed, accountDir)
	}
	return mountWaitErr(accountDir, waited, proven)
}
