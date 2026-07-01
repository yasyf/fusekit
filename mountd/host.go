package mountd

import (
	"time"

	"github.com/yasyf/fusekit"
)

// Host is the in-process fuse host the mount holder drives: the seam between the
// holder's protocol and whatever performs the kernel mounts. fusekit.MountSet
// satisfies it (assertion in mountset_assert_fuse.go behind the fuse tag; this
// package stays cgo-free). State's two bits are keyed independently and never
// collapse to one: mounted (dir is a mountpoint) drives foreign-mount refusal
// and unmount no-op; alive (base's contents visible through it) is liveness.
type Host interface {
	Setup(spec fusekit.MountSpec) error
	Teardown(base, dir string) error
	State(base, dir string) (mounted, alive bool)
}

// Drainer is an optional Host capability, invoked by the server before
// Teardown: drain dir's pending write-through within grace so an in-flight
// synth write lands before the mount goes away.
type Drainer interface {
	Drain(dir string, grace time.Duration)
}

// liveProbeTimeout bounds one kernel liveness stat: fuse-t's NFS backend has
// no soft/timeout mount options, so a wedged mirror blocks stats indefinitely
// and an unanswered probe must read dead. Must stay under the client's 3s List
// deadline. Var, not const, so tests can shrink it.
var liveProbeTimeout = 2 * time.Second

// mountState is one bounded probe's (mounted, alive) verdict.
type mountState struct {
	mounted bool
	alive   bool
}

// liveProbes joins concurrent bounded liveness stats per dir, package-wide: the
// holder's handlers and the client-side RemoteHost both stat dirs that can
// wedge with their mirror, and a wedged dir must cost at most one stuck
// goroutine no matter how many callers ask.
var liveProbes fusekit.StatProbes[mountState]

// probeMount reports dir's kernel mount state — the (mounted, alive) pair from
// state, bounded by liveProbeTimeout (see fusekit.StatProbes for join/detach).
// state is the source of kernel truth (the server's in-process Host.State, or
// RemoteHost's local-kernel probe). ok=false means the stat did not answer within
// the bound (a wedged mirror), so the caller must fail toward its safe direction:
// dead for liveness checks, still-mounted for foreign-mount refusals and teardown
// verification.
func probeMount(state func(base, dir string) (mounted, alive bool), base, dir string) (st mountState, ok bool) {
	return liveProbes.Do(dir, liveProbeTimeout, func() mountState {
		m, a := state(base, dir)
		return mountState{mounted: m, alive: a}
	})
}
