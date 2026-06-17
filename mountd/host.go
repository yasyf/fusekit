package mountd

import (
	"time"

	"github.com/yasyf/fusekit"
)

// Host is the in-process fuse host the mount holder drives. It is the narrow
// seam between the holder's protocol/registry mechanism and whatever actually
// performs the kernel mounts — cc-pool's ~/.claude mirror, cc-notes' synthetic
// tree, the throwaway capability probe. fusekit.MountSet satisfies it (the
// compile-time assertion lives in mountset_assert_fuse.go, behind the fuse tag,
// because MountSet embeds the cgofuse mount registry; this package stays pure).
//
// Setup ensures a live mirror of base at dir; Teardown unmounts it. State
// returns the two kernel-truth halves of mirror liveness as a PAIR: mounted
// (dir is a mountpoint at all) and alive (base's contents are visible through
// it). The pair is load-bearing — the holder keys foreign-mount refusal on
// mounted alone, the unmount no-op on !mounted, and idempotent mount/list on
// both — so the halves are reported independently and never collapsed to one
// bool.
type Host interface {
	Setup(base, dir string) error
	Teardown(base, dir string) error
	State(base, dir string) (mounted, alive bool)
}

// liveProbeTimeout bounds one kernel liveness probe (the Host.State pair stat).
// fuse-t's NFS backend has no soft/timeout mount options, so a wedged mirror —
// the serving-path fault this error taxonomy was built around — blocks those
// stats indefinitely. An unanswered probe reads dead: the driver then routes
// the dir through the bounded forced-teardown remount path, instead of one
// wedged mirror hanging List (and un-vouching every healthy sibling when the
// client's deadline blows). It must stay under the client's 3s List deadline.
// A var, not a const, so tests can shrink it.
var liveProbeTimeout = 2 * time.Second

// mountState is one bounded probe's verdict: the two kernel-truth halves of
// mirror liveness (the device-id mountpoint check and base's contents showing
// through it).
type mountState struct {
	mounted bool
	alive   bool
}

// liveProbes joins concurrent bounded liveness stats per dir, package-wide: the
// holder's handlers and the client-side RemoteHost both stat dirs that can
// wedge with their mirror, and a wedged dir must cost at most one stuck
// goroutine no matter how many callers ask.
var liveProbes fusekit.StatProbes[mountState]

// probeMount reports dir's kernel mount state — the (mounted, alive) pair
// returned by state — bounded by liveProbeTimeout (see fusekit.StatProbes for
// the join/detach semantics). The state func is the source of kernel truth: the
// server passes its Host.State (the in-process host's view), the client-side
// RemoteHost passes its local-kernel probe. ok=false means the stat did not
// answer within the bound (a wedged mirror) and the caller must fail toward its
// safe direction: dead for liveness checks, still-mounted for foreign-mount
// refusals and teardown verification.
func probeMount(state func(base, dir string) (mounted, alive bool), base, dir string) (st mountState, ok bool) {
	return liveProbes.Do(dir, liveProbeTimeout, func() mountState {
		m, a := state(base, dir)
		return mountState{mounted: m, alive: a}
	})
}
