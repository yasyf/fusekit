//go:build !darwin

package fusekit

import "os/exec"

// unmountFn seams force-unmount for tests; fusermount3 because Linux has no
// unprivileged MNT_FORCE path for a fuse mount.
var unmountFn = func(dir string) error {
	return exec.Command("fusermount3", "-uz", dir).Run()
}
