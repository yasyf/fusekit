//go:build darwin

package fusekit

import "golang.org/x/sys/unix"

// unmountFn seams force-unmount for tests.
var unmountFn = func(dir string) error {
	return unix.Unmount(dir, unix.MNT_FORCE)
}
