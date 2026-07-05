//go:build darwin

package proc

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// SuppressStdio re-points stdout and stderr at /dev/null so processes spawned
// from here — directly or by linked libraries, e.g. fuse-t's go-nfsv4 — inherit
// a harmless sink instead of this process's log file. Route logging and
// debug.SetCrashOutput at an O_CLOEXEC file first.
func SuppressStdio() error { return redirectDevNull(1, 2) }

func redirectDevNull(fds ...int) error {
	null, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open %s: %w", os.DevNull, err)
	}
	defer null.Close()
	for _, fd := range fds {
		if err := unix.Dup2(int(null.Fd()), fd); err != nil {
			return fmt.Errorf("dup2 %s onto fd %d: %w", os.DevNull, fd, err)
		}
	}
	return nil
}
