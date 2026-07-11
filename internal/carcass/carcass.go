// Package carcass is the fleet's ONLY force-unmount implementation, internal
// on purpose: no public fusekit API offers force. Force-unmount exists at
// EXACTLY two holder-internal sites — the mountd pre-mount carcass clear and
// the mountd journal-replay carcass clear — both executed under a seized
// lease EX fence with carcass proof v2:
//
//	(stat answers IMMEDIATELY with ENOTCONN/EIO/EPERM/EACCES)
//	∧ (mount identity pinned via the kernel mount table)
//	∧ (the mount's go-nfsv4 server proven dead BEFORE forcing, pid-reuse-proof)
//
// A hanging stat is NEVER proof, anywhere. (A Go consumer can always raw-
// syscall MNT_FORCE; the point is that this library hands out no force
// primitive.)
package carcass

import (
	"errors"
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// ErrUndetermined means a mount's state could not be proven safe to force —
// the stat hung, death did not revalidate, the mount identity moved, or a
// LIVE go-nfsv4 still serves the dir. The caller defers and surfaces, never
// forces.
var ErrUndetermined = errors.New("mount state undetermined; refusing to force-unmount")

// ErrWedged means the proven-dead carcass did not clear: the bounded force
// timed out or the mount survived it. The dir must stay fenced and surfaced.
var ErrWedged = errors.New("dead mount did not clear")

// ProbeDeadline is the hard bound for "the stat answered immediately".
// Rationale (carcass proof v2, NFS-343.100.5/xnu-12377.121.6 audit): the four
// dead errnos come from local vnode/transport state with no server round-trip
// and answer in microseconds; a stat still unanswered after this bound means
// in-flight I/O or a live-but-slow mount. 2s is generous headroom over
// scheduler noise while keeping the pre-mount and replay paths bounded. A var
// only so tests shrink it; production never changes it.
var ProbeDeadline = 2 * time.Second

// Verdict is a bounded stat probe's three-state answer. Hung is never death.
type Verdict int

const (
	Healthy Verdict = iota // answered without a dead errno (ENOENT included)
	Dead                   // answered with a dead-server errno
	Hung                   // did not answer within ProbeDeadline
)

// Test seams: the stat errno, the kernel mount-table identity, the server
// death proof, and the bounded force syscall — so the proof ladder is
// table-testable without a real mount.
var (
	statFn        = func(p string) error { _, err := os.Stat(p); return err }
	lookupMountFn = lookupMount
	serversDeadFn = ensureServersDead
	forceFn       = force
)

// Probe stats p bounded by ProbeDeadline. The stat runs in a goroutine; a
// hung one is abandoned (Hung) — the caller defers, it never forces under an
// abandoned probe.
func Probe(p string) Verdict {
	ch := make(chan error, 1)
	// Captured before the goroutine: an abandoned hung probe must not read
	// the seam var while a test cleanup restores it.
	stat := statFn
	go func() { ch <- stat(p) }()
	select {
	case err := <-ch:
		if DeadErrno(err) {
			return Dead
		}
		return Healthy
	case <-time.After(ProbeDeadline):
		return Hung
	}
}

// DeadErrno reports a dead-server stat errno: ENOTCONN/EIO (severed
// transport) or EPERM/EACCES (an orphaned go-nfsv4 whose holder died answers
// every op with a permission error — the dead-holder incident signature).
func DeadErrno(err error) bool {
	return errors.Is(err, unix.ENOTCONN) || errors.Is(err, unix.EIO) ||
		errors.Is(err, unix.EPERM) || errors.Is(err, unix.EACCES)
}

// mountID pins a mountpoint's kernel identity at proof time so the force can
// never land on a mount that replaced or covered the proven-dead one. Read
// from the kernel mount table (getfsstat on darwin — no server I/O).
type mountID struct {
	fsidA, fsidB int64
	fstype       string
	source       string
}

// Clear force-unmounts the dead-mount carcass at dir IFF carcass proof v2
// holds (package doc). The caller — one of the two mountd force sites — MUST
// hold dir's lease EX fence across the whole call. The ladder, in order:
//
//  1. dir's stat answers a dead errno within ProbeDeadline. Healthy (ENOENT
//     included) is a no-op; a hanging stat is ErrUndetermined — never
//     force-on-hang.
//  2. dir is a current kernel mountpoint and its identity (fsid, fstype,
//     source) is pinned. A dead errno off a non-mountpoint is local path or
//     permission state — a no-op, not proof.
//  3. The mount's go-nfsv4 server is proven dead BEFORE forcing: any process
//     still serving dir that is a live child of this holder means the server
//     is alive (a sandbox/MAC/auth denial, not a carcass) — ErrUndetermined;
//     a prior generation's orphan is killed with a pid-reuse-proof re-check
//     (fresh comm + argv + full start time) and its death confirmed. Servers
//     that outlive the kill defer too.
//  4. Death is revalidated and the pinned mount identity re-verified
//     immediately before the force; any drift aborts.
//  5. The force itself — umount(8) -f, the genuine MNT_FORCE → dounmount →
//     vflush → vclean path the panic audit proved invalidate-only for a dead
//     mount — is bounded by a context deadline plus process kill; a timeout
//     keeps the carcass fenced and surfaces ErrWedged, never hangs replay.
//
// Deployment facts the proof also rests on (not checkable here): the running
// NFS client matches the audited tags or an audited equivalent; vnode drain
// is not bypassed (no bootarg_no_vnode_drain); no other fleet component
// issues MNT_FORCE; kernel buffer/UPL/UBC bookkeeping is intact.
func Clear(dir string) error {
	switch Probe(dir) {
	case Healthy:
		return nil
	case Hung:
		return fmt.Errorf("%w: stat of %s did not answer within %s (a hanging stat is never proof of death)", ErrUndetermined, dir, ProbeDeadline)
	}
	// Errno provenance: a dead errno off a non-mountpoint is local path or
	// permission state, and there is nothing to unmount.
	id, mounted := lookupMountFn(dir)
	if !mounted {
		return nil
	}
	// The mount's server must be proven dead BEFORE the force (assertion #9):
	// a dead-errno stat with a live server is a denial, not a carcass.
	if err := serversDeadFn(dir); err != nil {
		return err
	}
	// Revalidate death and the pinned identity immediately before the force.
	if Probe(dir) != Dead {
		return fmt.Errorf("%w: %s stopped answering a dead errno between probe and force", ErrUndetermined, dir)
	}
	if id2, ok := lookupMountFn(dir); !ok || id2 != id {
		return fmt.Errorf("%w: mount identity at %s changed between proof and force (pinned %+v, now %+v mounted=%v)", ErrUndetermined, dir, id, id2, ok)
	}
	if err := forceFn(dir); err != nil {
		return err
	}
	if Probe(dir) != Healthy {
		return fmt.Errorf("%w: dead mount at %s did not clear", ErrWedged, dir)
	}
	if _, still := lookupMountFn(dir); still {
		return fmt.Errorf("%w: %s is still in the kernel mount table after the force", ErrWedged, dir)
	}
	return nil
}
