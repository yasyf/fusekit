//go:build !darwin

package fusekit

// reapOrphanedServers is a no-op: fuse-t's per-mount NFS servers are macOS-only.
func reapOrphanedServers(string) {}

// ReapOrphanedServers is a no-op off darwin: fuse-t is macOS-only.
func ReapOrphanedServers([]string) []int { return nil }

// reapDirServersAnyGen is a no-op off darwin: fuse-t is macOS-only.
func reapDirServersAnyGen(string) {}
