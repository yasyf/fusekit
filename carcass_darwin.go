//go:build darwin

package fusekit

import "os/exec"

// forceReap clears a dead-mount carcass at dir. On darwin it shells out to
// umount -f (not the unix.Unmount syscall ForceUnmount uses) so cgofuse/fuse-t
// teardown semantics match the holder's own unmount path during pre-mount
// cleanup. Best-effort: a reap failure is verified — never trusted — by the
// caller's retried stat.
func forceReap(dir string) {
	_ = exec.Command("umount", "-f", dir).Run()
}
