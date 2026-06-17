//go:build !darwin

package fusekit

import "os/exec"

// unmountFn seams the force-unmount call so ForceUnmount is unit-testable
// without a real mount. Tests swap it and restore via t.Cleanup. Production on
// non-darwin (Linux fuse3): fusermount3 -uz, the lazy forced unmount the
// libfuse3 userspace helper provides (there is no unprivileged MNT_FORCE
// syscall path for a fuse mount).
var unmountFn = func(dir string) error {
	return exec.Command("fusermount3", "-uz", dir).Run()
}
