//go:build darwin

package fusekit

import "os/exec"

// forceReap clears a dead-mount carcass. umount -f, not the unix.Unmount
// syscall ForceUnmount uses, so teardown matches the holder's own
// cgofuse/fuse-t unmount path. Best-effort: the caller's retried stat verifies.
func forceReap(dir string) {
	_ = exec.Command("umount", "-f", dir).Run()
}
