package proc

import (
	"fmt"
	"os"
	"runtime"
	"strconv"

	"golang.org/x/sys/unix"
)

// CloseInheritedFDs closes every inherited descriptor ≥3 — one whose
// FD_CLOEXEC flag is UNSET, which right after exec can only be a descriptor
// the parent deliberately kept inheritable. A consumer session's lease fd is
// exactly that: a lazily spawned holder inheriting it would pin the lease for
// its whole lifetime. The process's own runtime descriptors are CLOEXEC and
// untouched. Detached children spawned via Spawn MUST call this FIRST in
// main, before opening anything non-CLOEXEC.
func CloseInheritedFDs() error {
	dir := "/dev/fd"
	if runtime.GOOS == "linux" {
		dir = "/proc/self/fd"
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("list open fds: %w", err)
	}
	for _, e := range entries {
		fd, err := strconv.Atoi(e.Name())
		if err != nil || fd < 3 {
			continue
		}
		// ReadDir's own transient fd reads EBADF here and is skipped.
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC != 0 {
			continue
		}
		_ = unix.Close(fd)
	}
	return nil
}
