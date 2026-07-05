//go:build darwin

package fusekit

import "os/exec"

// forceReap clears a dead-mount carcass: umount -f (matches the holder's own
// fuse-t unmount path), then an any-generation server reap. Best-effort — the
// caller's retried stat verifies. See ccn doc 501ce12.
func forceReap(dir string) {
	_ = exec.Command("umount", "-f", dir).Run()
	reapDirServersAnyGen(dir)
}
