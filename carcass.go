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

// ErrCarcassUndetermined means a mount's liveness could not be proven either
// way — the stat neither answered healthy nor returned a dead errno within
// carcassProbeDeadline. A hanging stat is NEVER proof of death (an in-flight
// RPC, a reconnecting transport, or a wedged-but-live server all hang), so
// the caller must defer and surface, never force.
var ErrCarcassUndetermined = errors.New("mount state undetermined; refusing to force-unmount")

// carcassProbeDeadline is the hard bound for "the stat answered immediately".
// Rationale (carcass proof v2, NFS-343.100.5/xnu-12377.121.6 audit): the four
// dead errnos come from local vnode/transport state with no server round-trip
// and answer in microseconds; a stat still unanswered after this bound means
// in-flight I/O or a live-but-slow mount. 2s is generous headroom over
// scheduler noise while keeping the pre-mount and replay paths bounded. A var
// only so tests shrink it; production never changes it.
var carcassProbeDeadline = 2 * time.Second

type statVerdict int

const (
	statHealthy statVerdict = iota // answered without a dead errno (ENOENT included)
	statDead                       // answered with a dead-server errno
	statHung                       // did not answer within carcassProbeDeadline
)

// ClearCarcass force-unmounts the dead-mount carcass a killed holder left at
// dir. It is the fleet's ONLY force-unmount, reached from exactly two holder
// sites — the pre-mount clear (Mount's Config.ClearCarcass) and the journal
// replay clear — and it forces IFF carcass proof v2 holds:
//
//  1. dir's stat answers a dead errno (ENOTCONN/EIO/EPERM/EACCES) within
//     carcassProbeDeadline. A hanging stat returns ErrCarcassUndetermined —
//     never force-on-hang.
//  2. dir is a current kernel mountpoint (getfsstat, no server I/O), so the
//     errno provenance is the mount's own stat — a local permission or path
//     errno on a non-mountpoint is a no-op, not proof.
//  3. Death is revalidated immediately before the force syscall.
//  4. The caller holds dir's lease exclusively (lease.Seize) across the whole
//     call — the in-kernel fence against session acquire/rebind mid-force.
//     Enforced structurally by the mountd server (its carcass paths seize
//     before clearing); pinned by the server's tests.
//
// The force is umount(8) -f — the genuine MNT_FORCE -> dounmount -> vflush ->
// vclean path the panic audit proved invalidate-only for a dead mount — then
// an any-generation go-nfsv4 reap whose kill decision is re-confirmed at kill
// time (fresh comm + argv mountpoint re-read guards PID reuse).
//
// Deployment facts the proof also rests on (not checkable here): the running
// NFS client matches the audited tags or an audited equivalent; vnode drain
// is not bypassed (no bootarg_no_vnode_drain); no other fleet component
// issues MNT_FORCE; kernel buffer/UPL/UBC bookkeeping is intact. A healthy or
// absent path is a no-op; ErrUnmountWedged means the carcass did not clear.
func ClearCarcass(dir string) error {
	switch carcassProbe(dir) {
	case statHealthy:
		return nil
	case statHung:
		return fmt.Errorf("%w: stat of %s did not answer within %s (a hanging stat is never proof of death)", ErrCarcassUndetermined, dir, carcassProbeDeadline)
	}
	// Errno provenance: a dead errno off a non-mountpoint is local path or
	// permission state, and there is nothing to unmount.
	if !carcassMounted(dir) {
		return nil
	}
	// Revalidate death immediately before the force syscall.
	if carcassProbe(dir) != statDead {
		return fmt.Errorf("%w: %s stopped answering a dead errno between probe and force", ErrCarcassUndetermined, dir)
	}
	forceReapFn(dir)
	if carcassProbe(dir) != statHealthy || carcassMounted(dir) {
		return fmt.Errorf("%w: dead mount at %s did not clear", ErrUnmountWedged, dir)
	}
	return nil
}

// Test seams: fake mountpoint state (getfsstat — no server I/O), stat errno,
// and the force syscall without a real mount.
var (
	carcassMounted = Mounted
	carcassStat    = func(p string) error { _, err := os.Stat(p); return err }
	forceReapFn    = forceReap
)

// carcassProbe stats p bounded by carcassProbeDeadline. The stat runs in a
// goroutine; a hung one is abandoned (statHung) — the caller defers, it never
// forces under an abandoned probe.
func carcassProbe(p string) statVerdict {
	ch := make(chan error, 1)
	// Captured before the goroutine: an abandoned hung probe must not read
	// the seam var while a test cleanup restores it.
	stat := carcassStat
	go func() { ch <- stat(p) }()
	select {
	case err := <-ch:
		if carcassErr(err) {
			return statDead
		}
		return statHealthy
	case <-time.After(carcassProbeDeadline):
		return statHung
	}
}

// statAnswers reports a healthy, bounded stat of p: ENOENT is healthy (absent
// is not wedged); a carcass errno or no answer within statProbeTimeout reads
// false. Used by the orphan reaper's carcass gate, never as force proof.
func statAnswers(p string) bool {
	ch := make(chan error, 1)
	go func() {
		_, err := os.Stat(p)
		ch <- err
	}()
	select {
	case err := <-ch:
		return !carcassErr(err)
	case <-time.After(statProbeTimeout):
		return false
	}
}

// carcassErr reports a dead-server stat errno: ENOTCONN/EIO (severed
// transport) or EPERM/EACCES (an orphaned go-nfsv4 whose holder died answers
// every op with a permission error — the dead-holder incident signature).
func carcassErr(err error) bool {
	return errors.Is(err, unix.ENOTCONN) || errors.Is(err, unix.EIO) ||
		errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES)
}
