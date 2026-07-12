//go:build fuse && cgo && linux

package fusekit

import (
	"errors"

	"golang.org/x/sys/unix"
)

// unmountFn seams Handle.Unmount's kernel umount2(2) call. flags is ALWAYS 0
// — never MNT_DETACH (a lazy detach is a force by another name). umount2 of a
// FUSE mount needs root, so an ordinary user's EPERM falls back to the
// fusermount helpers — graceful -u only.
var unmountFn = func(dir string, flags int) error {
	err := unix.Unmount(dir, flags)
	if err == nil || !errors.Is(err, unix.EPERM) {
		return err
	}
	return fusermountUnmount(dir)
}
