//go:build darwin

package fusekit

import (
	"bytes"
	"encoding/binary"
	"os"

	"golang.org/x/sys/unix"
)

// nfsServerComm is fuse-t's NFS backend process name, one server per NATIVE
// mount. libfuse-t spawns it, so no Go handle exists; survivors are found by
// PID. Under mux the native mount is the shared mux root serving every tenant as
// a subtree, so one go-nfsv4 backs the whole pool and its argv mountpoint is that
// root — logical per-tenant detach spawns and reaps no server.
const nfsServerComm = "go-nfsv4"

// reapOrphanedServers force-kills any nfsServerComm child of this process
// serving dir: forced fuse-t unmount does not guarantee the backing NFS
// server exits, and a survivor answers stale stats and stacks a duplicate
// under a later mount. Call ONLY after confirming dir is no longer a
// mountpoint — a child still bound to it is then provably orphaned.
// Sysctl-only, so it never blocks on a wedged mount the way lsof does.
// Direct children only: a dead holder's orphans are left to ClearCarcass,
// never a cross-holder kill. Best effort — a failed signal is caught by the
// caller's mountpoint re-verify.
func reapOrphanedServers(dir string) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.ppid", os.Getpid())
	if err != nil {
		return
	}
	for _, pid := range orphanServerPIDs(procs, dir, serverMountpoint) {
		_ = unix.Kill(pid, unix.SIGKILL)
	}
}

// orphanServerPIDs is pure so the safety-critical kill decision is testable
// without real children or signals.
func orphanServerPIDs(procs []unix.KinfoProc, dir string, mountpointOf func(pid int) string) []int {
	var pids []int
	for i := range procs {
		p := &procs[i].Proc
		if commName(p.P_comm[:]) != nfsServerComm {
			continue
		}
		pid := int(p.P_pid)
		if mountpointOf(pid) == dir {
			pids = append(pids, pid)
		}
	}
	return pids
}

// commName trims a NUL-terminated, MAXCOMLEN-truncated kinfo_proc p_comm.
func commName(b []byte) string {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return string(b)
}

// serverMountpoint returns pid's last argv element — go-nfsv4's mountpoint,
// which for a mux server is the shared native/mux root reapServers already
// operates on — from the kernel's exec-time argv copy. "" on any failure, so the
// caller skips the pid rather than risk a wrong kill.
func serverMountpoint(pid int) string {
	buf, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return ""
	}
	return parseLastArg(buf)
}

// parseLastArg extracts the final argv string from a kern.procargs2 blob:
// uint32 argc, NUL-terminated exec path, NUL padding, then argc NUL-terminated
// argv strings (environment follows, ignored). "" on a malformed buffer.
func parseLastArg(buf []byte) string {
	if len(buf) < 4 {
		return ""
	}
	argc := int(binary.LittleEndian.Uint32(buf))
	if argc <= 0 {
		return ""
	}
	rest := buf[4:]
	i := bytes.IndexByte(rest, 0)
	if i < 0 {
		return ""
	}
	rest = rest[i:]
	for len(rest) > 0 && rest[0] == 0 {
		rest = rest[1:]
	}
	var last string
	for n := 0; n < argc && len(rest) > 0; n++ {
		j := bytes.IndexByte(rest, 0)
		if j < 0 {
			last = string(rest)
			break
		}
		last = string(rest[:j])
		rest = rest[j+1:]
	}
	return last
}
