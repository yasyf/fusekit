//go:build darwin

package fusekit

import (
	"bytes"
	"encoding/binary"
	"os"

	"golang.org/x/sys/unix"
)

// nfsServerComm is the process name fuse-t's NFS backend runs as: each fuse-t
// mount is served by exactly one such child of the mount-holder. libfuse-t
// spawns it inside cgofuse's host.Mount, so neither fusekit nor its consumers
// hold a Go handle on it — reaping a survivor means finding it by PID.
const nfsServerComm = "go-nfsv4"

// reapOrphanedServers force-kills any nfsServerComm child of THIS process whose
// mountpoint argument is dir. It is the honest completion of a teardown: a
// forced fuse-t unmount takes the mountpoint down without guaranteeing the
// backing NFS server exits, and a survivor then answers stale stats and lets a
// later mount stack a second server on the same dir (the observed duplicate
// go-nfsv4). Callers invoke it ONLY after confirming dir is no longer a
// mountpoint, so any child still bound to dir is provably orphaned.
//
// It is bounded and never touches the (possibly wedged) mount: the child set
// comes from a kern.proc.ppid sysctl read and each argv from the kernel's
// exec-time copy (kern.procargs2) — neither blocks on a hung fuse-t mount the
// way lsof (which opens every mountpoint) does. It is scoped to direct children
// of the current holder: a server orphaned by a *dead* holder (reparented to
// launchd) is left to ClearCarcass's mountpoint reap, never to a cross-holder
// kill. Best effort — a failed signal is caught by the caller's mountpoint
// re-verify.
func reapOrphanedServers(dir string) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.ppid", os.Getpid())
	if err != nil {
		return
	}
	for _, pid := range orphanServerPIDs(procs, dir, serverMountpoint) {
		_ = unix.Kill(pid, unix.SIGKILL)
	}
}

// orphanServerPIDs selects, from a parent's child procs, the go-nfsv4 servers
// whose mountpoint (via mountpointOf) equals dir. It is pure so the kill
// decision — the part that must never target a wrong process — is testable
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

// commName trims a kinfo_proc p_comm (a NUL-terminated, MAXCOMLEN-truncated
// process name) to a Go string.
func commName(b []byte) string {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return string(b)
}

// serverMountpoint returns pid's last argv element — for go-nfsv4 the mountpoint
// it serves. The argv comes from kern.procargs2, the kernel's saved exec-time
// copy, so the read never touches the process's current state or its mount. ""
// on any read/parse failure, so the caller skips the pid (fail safe — never a
// wrong kill).
func serverMountpoint(pid int) string {
	buf, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return ""
	}
	return parseLastArg(buf)
}

// parseLastArg extracts the final argv string from a kern.procargs2 blob, whose
// layout is: a uint32 argc, the NUL-terminated executable path, NUL padding,
// then argc NUL-terminated argv strings (the environment follows, ignored). It
// returns "" on a short or malformed buffer.
func parseLastArg(buf []byte) string {
	if len(buf) < 4 {
		return ""
	}
	argc := int(binary.LittleEndian.Uint32(buf))
	if argc <= 0 {
		return ""
	}
	rest := buf[4:]
	// Skip the exec path (NUL-terminated) and the NUL padding before argv[0].
	i := bytes.IndexByte(rest, 0)
	if i < 0 {
		return ""
	}
	rest = rest[i:]
	for len(rest) > 0 && rest[0] == 0 {
		rest = rest[1:]
	}
	// Walk argc NUL-terminated arguments; the last is go-nfsv4's mountpoint.
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
