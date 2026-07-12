//go:build fuse && cgo && !linux

package fusekit

import "golang.org/x/sys/unix"

// unmountFn seams Handle.Unmount's kernel unmount(2) call. flags is ALWAYS 0
// — never MNT_FORCE; the fleet's only force is the holder's fenced,
// proof-gated carcass clear.
var unmountFn = func(dir string, flags int) error { return unix.Unmount(dir, flags) }
