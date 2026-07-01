//go:build !darwin

package fusekit

// reapOrphanedServers is a no-op: fuse-t's per-mount NFS servers are macOS-only.
func reapOrphanedServers(string) {}
