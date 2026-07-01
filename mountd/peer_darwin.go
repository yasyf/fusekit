//go:build darwin

package mountd

import (
	"fmt"
	"net"
	"time"

	"golang.org/x/sys/unix"
)

func peerPID(socketPath string) (int, error) {
	conn, err := net.DialTimeout("unix", socketPath, 500*time.Millisecond)
	if err != nil {
		return 0, ErrUnreachable
	}
	defer conn.Close()
	raw, err := conn.(*net.UnixConn).SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("syscall conn: %w", err)
	}
	var pid int
	var opErr error
	if err := raw.Control(func(fd uintptr) {
		pid, opErr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	}); err != nil {
		return 0, fmt.Errorf("control fd: %w", err)
	}
	if opErr != nil {
		return 0, fmt.Errorf("getsockopt LOCAL_PEERPID: %w", opErr)
	}
	return pid, nil
}
