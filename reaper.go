package fusekit

// reapServers seams the platform orphaned-NFS-server reaper (reapOrphanedServers
// — on darwin it kills a go-nfsv4 left bound to a torn-down dir; elsewhere a
// no-op) so tests assert the teardown and pre-mount reap calls without spawning
// a real server. Production callers go through this var.
var reapServers = reapOrphanedServers
