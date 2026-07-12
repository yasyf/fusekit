//go:build !(fuse && cgo)

package fusekit

// Only HostProbe is stubbed; Mount/Serve/MountSet/CacheDefeat deliberately aren't —
// consumers gate on the fuse tag or route through the mount-holder.

// HostProbe reports whether this process can host an in-process fuse mount;
// without a fuse runtime it always reports (false, nil).
func HostProbe(func()) (bool, error) { return false, nil }
