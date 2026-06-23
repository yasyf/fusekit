//go:build !darwin

package fusekit

// reapOrphanedServers is a darwin-only concern: fuse-t and its per-mount
// go-nfsv4 NFS backend are macOS-only, so other platforms have no orphaned NFS
// server to reap after a teardown.
func reapOrphanedServers(string) {}
