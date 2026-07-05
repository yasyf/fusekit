//go:build darwin

package fusekit

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// nfsServerComm is fuse-t's NFS backend process name, one server per NATIVE
// mount. libfuse-t spawns it, so no Go handle exists; survivors are found by
// PID. Under mux the native mount is the shared mux root serving every tenant as
// a subtree, so one go-nfsv4 backs the whole pool and its argv mountpoint is that
// root — logical per-tenant detach spawns and reaps no server.
const nfsServerComm = "go-nfsv4"

// reapOrphanedServers force-kills any nfsServerComm child of this process
// serving dir; call ONLY after confirming dir is no longer a mountpoint.
// Safety argument (sysctl-only scan, direct-children scope): see ccn doc 501ce12.
func reapOrphanedServers(dir string) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.ppid", os.Getpid())
	if err != nil {
		return
	}
	for _, pid := range orphanServerPIDs(procs, dir, serverMountpoint) {
		// No carcass gate: the holder legitimately clears its own prior child, live or not.
		if pidStillServes(pid, dir, commOfPid, serverMountpoint) {
			_ = unix.Kill(pid, unix.SIGKILL)
		}
	}
}

// pidStillServes reports whether pid still reads comm go-nfsv4 with argv
// mountpoint dir — the kill-time PID-reuse guard.
func pidStillServes(pid int, dir string, commOf, mountpointOf func(pid int) string) bool {
	return commOf(pid) == nfsServerComm && mountpointOf(pid) == dir
}

// reapDirServersAnyGen force-kills every nfsServerComm process of ANY generation
// whose argv mountpoint is exactly dir. forceReap-only: dir is a confirmed carcass.
func reapDirServersAnyGen(dir string) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return
	}
	carcass := func(d string) bool { return !statAnswers(d) }
	for _, pid := range orphanServerPIDs(procs, dir, serverMountpoint) {
		if reconfirmOrphan(orphanCandidate{pid: pid, mp: dir}, commOfPid, serverMountpoint, carcass) {
			_ = unix.Kill(pid, unix.SIGKILL)
		}
	}
}

// ReapOrphanedServers force-kills orphaned nfsServerComm servers of ANY
// generation under roots — carcass-confirmed only, re-confirmed at kill time,
// never a live mount. Returns the PIDs killed. See ccn doc 501ce12.
func ReapOrphanedServers(roots []string) []int {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil
	}
	carcass := func(dir string) bool { return !statAnswers(dir) }
	cands := crossGenOrphanCandidates(procs, roots, serverMountpoint, carcass)
	var killed []int
	for _, c := range cands {
		if !reconfirmOrphan(c, commOfPid, serverMountpoint, carcass) {
			continue
		}
		_ = unix.Kill(c.pid, unix.SIGKILL)
		killed = append(killed, c.pid)
	}
	return killed
}

// orphanCandidate is one scan-time kill candidate: a go-nfsv4 pid and the
// argv mountpoint it serves.
type orphanCandidate struct {
	pid int
	mp  string
}

// crossGenOrphanCandidates is pure so the safety-critical cross-generation
// kill decision is testable without real processes or signals.
func crossGenOrphanCandidates(procs []unix.KinfoProc, roots []string, mountpointOf func(pid int) string, carcass func(dir string) bool) []orphanCandidate {
	verdicts := map[string]bool{}
	var cands []orphanCandidate
	for i := range procs {
		p := &procs[i].Proc
		if commName(p.P_comm[:]) != nfsServerComm {
			continue
		}
		mp := mountpointOf(int(p.P_pid))
		if mp == "" || !underAny(mp, roots) {
			continue
		}
		v, ok := verdicts[mp]
		if !ok {
			v = carcass(mp)
			verdicts[mp] = v
		}
		if v {
			cands = append(cands, orphanCandidate{pid: int(p.P_pid), mp: mp})
		}
	}
	return cands
}

// reconfirmOrphan re-validates one candidate immediately before its kill:
// pidStillServes AND a FRESH carcass re-stat (never memoized across the kill
// loop; see ccn doc 501ce12).
func reconfirmOrphan(c orphanCandidate, commOf, mountpointOf func(pid int) string, carcass func(dir string) bool) bool {
	return pidStillServes(c.pid, c.mp, commOf, mountpointOf) && carcass(c.mp)
}

// commOfPid reads pid's current comm from the kernel; "" when the pid is gone,
// so the caller spares it.
func commOfPid(pid int) string {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return ""
	}
	return commName(kp.Proc.P_comm[:])
}

// underAny reports whether mp equals a root or lies strictly under one.
func underAny(mp string, roots []string) bool {
	mp = filepath.Clean(mp)
	for _, r := range roots {
		if r == "" {
			continue
		}
		r = filepath.Clean(r)
		if mp == r || strings.HasPrefix(mp, r+string(filepath.Separator)) {
			return true
		}
	}
	return false
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
