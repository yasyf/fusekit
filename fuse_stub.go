//go:build !(fuse && cgo)

package fusekit

// This file provides the non-fuse build's stubs for the fuse-only host surface.
// Only HostProbe is stubbed: it is the one capability query a pure-Go binary
// asks ("can this process host an in-process mount?"), and the honest answer
// without a fuse runtime is no. Mount, Serve, MountSet, and CacheDefeat are
// fuse-only types with no meaningful pure-build behavior, so they are NOT
// stubbed — consumers gate their use on the fuse build tag (or route through
// the detached mount-holder, which is itself a pure binary spawned to host).

// HostProbe reports whether this process can host an in-process fuse mount, and
// why not when it cannot. In a build without the fuse tag (or without cgo)
// there is no fuse runtime to drive, so it always reports (false, nil) — no
// capability, but no error to surface. The fuse build (hostprobe.go) attempts a
// real throwaway probe mount and returns its classified failure.
func HostProbe() (bool, error) { return false, nil }
