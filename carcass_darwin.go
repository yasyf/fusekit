//go:build darwin

package fusekit

import "os/exec"

// forceReap clears a dead-mount carcass. umount -f, not the unix.Unmount
// syscall ForceUnmount uses, so teardown matches the holder's own
// cgofuse/fuse-t unmount path. Best-effort: the caller's retried stat verifies.
// The umount does not guarantee the backing server exits, and a dead holder's
// orphan is no child of this process — kill any-generation servers bound to
// exactly this confirmed carcass too.
func forceReap(dir string) {
	_ = exec.Command("umount", "-f", dir).Run()
	reapDirServersAnyGen(dir)
}
