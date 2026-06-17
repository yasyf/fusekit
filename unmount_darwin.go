//go:build darwin

package fusekit

import "golang.org/x/sys/unix"

// unmountFn seams the force-unmount call so ForceUnmount is unit-testable
// without a real mount. Tests swap it and restore via t.Cleanup. Production on
// darwin: unix.Unmount with MNT_FORCE, the direct syscall cc-pool's daemon
// relies on to clear a dead holder's carcass without a fusermount helper.
var unmountFn = func(dir string) error {
	return unix.Unmount(dir, unix.MNT_FORCE)
}
