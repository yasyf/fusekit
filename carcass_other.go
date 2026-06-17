//go:build !darwin

package fusekit

import "os/exec"

// forceReap clears a dead-mount carcass at dir. On non-darwin (Linux fuse3) it
// shells out to fusermount3 -uz, the lazy forced unmount the libfuse3 userspace
// helper provides. Best-effort: a reap failure is verified — never trusted —
// by the caller's retried stat.
func forceReap(dir string) {
	_ = exec.Command("fusermount3", "-uz", dir).Run()
}
