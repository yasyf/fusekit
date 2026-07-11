//go:build darwin

package carcass

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// nfsServerComm is fuse-t's NFS backend process name, one server per NATIVE
// mount. libfuse-t spawns it, so no Go handle exists; survivors are found by
// PID. Under mux the native mount is the shared mux root serving every tenant
// as a subtree, so one go-nfsv4 backs the whole pool and its argv mountpoint
// is that root — logical per-tenant detach spawns and reaps no server.
const nfsServerComm = "go-nfsv4"

// serverKillWait bounds the post-SIGKILL death confirmation; a var so tests
// shrink it.
var serverKillWait = 2 * time.Second

// procStamp is a process's full kernel start time (sec + usec) — the
// pid-reuse anchor. One-second resolution is not enough: a same-second reuse
// by another same-shaped go-nfsv4 must never be shot as the original.
type procStamp struct {
	sec  int64
	usec int32
}

// orphanCandidate is one scan-time kill candidate: a go-nfsv4 pid, the argv
// mountpoint it serves, its parent, and the scan-time full start stamp the
// kill-time re-check compares so a reused pid is never shot.
type orphanCandidate struct {
	pid   int
	ppid  int
	mp    string
	start procStamp
}

// Test seams: the server scan and the kill signal, so the pre-force
// server-death proof is table-testable without real processes.
var (
	dirServersFn = dirServersAnyGen
	killFn       = func(pid int) { _ = unix.Kill(pid, unix.SIGKILL) }
)

// ensureServersDead proves dir's go-nfsv4 server dead before a force
// (assertion #9). FAIL-CLOSED: any enumeration failure — sysctl, or an
// unreadable argv on a go-nfsv4-shaped pid — is ErrUndetermined, never an
// empty candidate set; zero candidates prove death only off a FULL scan. A
// candidate that is a LIVE child of this process means the server is alive
// (the dead errno is a denial, not a carcass) and defers. Prior-generation
// orphans are killed under the pid-reuse-proof re-check and their death
// confirmed, bounded; a survivor defers.
func ensureServersDead(dir string) error {
	cands, err := dirServersFn(dir)
	if err != nil {
		return fmt.Errorf("%w: server scan for %s failed; death not provable: %v", ErrUndetermined, dir, err)
	}
	for _, c := range cands {
		if c.ppid == os.Getpid() {
			return fmt.Errorf("%w: go-nfsv4 pid %d serving %s is a live child of this holder — a dead-errno stat with a live server is not a carcass", ErrUndetermined, c.pid, dir)
		}
		if reconfirmOrphan(c, commOfPid, serverMountpoint, startOfPid, func(string) bool { return true }) {
			killFn(c.pid)
		}
	}
	deadline := time.Now().Add(serverKillWait)
	for {
		cands, err := dirServersFn(dir)
		if err != nil {
			return fmt.Errorf("%w: server re-scan for %s failed; death not provable: %v", ErrUndetermined, dir, err)
		}
		if len(cands) == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%w: go-nfsv4 still serving %s after SIGKILL; refusing to force", ErrUndetermined, dir)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// dirServersAnyGen scans every process of ANY generation whose comm is
// go-nfsv4 and whose argv mountpoint is exactly dir. Errors, never an empty
// set, on enumeration failure (the force gate fails closed on it).
func dirServersAnyGen(dir string) ([]orphanCandidate, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil, fmt.Errorf("enumerate processes: %w", err)
	}
	return dirServerCandidates(procs, dir, liveServerMountpoint)
}

// dirServerCandidates is pure so the pre-force server-death decision is
// testable without real processes. A mountpointOf error on a go-nfsv4 pid
// aborts the whole scan: an unreadable argv could hide a matching server.
func dirServerCandidates(procs []unix.KinfoProc, dir string, mountpointOf func(pid int) (string, error)) ([]orphanCandidate, error) {
	var cands []orphanCandidate
	for i := range procs {
		p := &procs[i].Proc
		if commName(p.P_comm[:]) != nfsServerComm {
			continue
		}
		pid := int(p.P_pid)
		mp, err := mountpointOf(pid)
		if err != nil {
			return nil, err
		}
		if mp != dir {
			continue
		}
		cands = append(cands, orphanCandidate{
			pid:   pid,
			ppid:  int(procs[i].Eproc.Ppid),
			mp:    dir,
			start: procStamp{sec: p.P_starttime.Sec, usec: p.P_starttime.Usec},
		})
	}
	return cands, nil
}

// liveServerMountpoint is the force gate's argv read: unlike serverMountpoint
// (whose "" skips a pid on a KILL decision — failing open there spares, the
// safe direction), any UNPROVEN state on the enumeration side is an error —
// it could hide a live server. Only a POSITIVE pid-gone signal (ESRCH), or a
// readable comm that is no longer go-nfsv4, drops the pid from the scan.
func liveServerMountpoint(pid int) (string, error) {
	buf, err := unix.SysctlRaw("kern.procargs2", pid)
	return liveMountpoint(pid, buf, err, pidGone, liveCommOfPid)
}

// liveMountpoint is liveServerMountpoint's pure verdict, seam-injected so the
// fail-closed double-failure ladder is table-testable.
func liveMountpoint(pid int, buf []byte, argsErr error, gone func(int) bool, commOf func(int) (string, error)) (string, error) {
	if argsErr != nil {
		if gone(pid) {
			return "", nil // positively exited since the snapshot: no candidate
		}
		comm, cerr := commOf(pid)
		if cerr != nil {
			return "", fmt.Errorf("procargs of pid %d unreadable (%v) and comm unreadable (%v); not provably exited", pid, argsErr, cerr)
		}
		if comm != nfsServerComm {
			return "", nil // exec'd into something else: no longer a server
		}
		return "", fmt.Errorf("procargs of go-nfsv4 pid %d unreadable: %w", pid, argsErr)
	}
	mp, err := parseLastArg(buf)
	if err != nil {
		return "", fmt.Errorf("procargs of go-nfsv4 pid %d malformed: %w", pid, err)
	}
	if mp == "" {
		return "", fmt.Errorf("procargs of go-nfsv4 pid %d yielded no mountpoint", pid)
	}
	return mp, nil
}

// pidGone reports a POSITIVE pid-exit signal: kill(pid, 0) answering ESRCH.
// Alive (nil/EPERM) or any undetermined errno is NOT gone.
func pidGone(pid int) bool {
	return errors.Is(unix.Kill(pid, 0), unix.ESRCH)
}

// liveCommOfPid is commOfPid with the lookup failure surfaced instead of
// folded into "" — the enumeration side must distinguish gone from unreadable.
func liveCommOfPid(pid int) (string, error) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return "", err
	}
	return commName(kp.Proc.P_comm[:]), nil
}

// ReapOwnChildren force-kills any go-nfsv4 child of this process serving dir;
// call ONLY after confirming dir is no longer a mountpoint. Safety argument
// (sysctl-only scan, direct-children scope): see ccn doc 501ce12.
func ReapOwnChildren(dir string) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.ppid", os.Getpid())
	if err != nil {
		return
	}
	for _, c := range ownChildCandidates(procs, dir, serverMountpoint) {
		// No carcass gate: the holder legitimately clears its own prior child, live or not.
		if pidStillServes(c.pid, dir, commOfPid, serverMountpoint) && startOfPid(c.pid) == c.start {
			_ = unix.Kill(c.pid, unix.SIGKILL)
		}
	}
}

// ownChildCandidates is pure so the safety-critical kill decision is testable
// without real children or signals.
func ownChildCandidates(procs []unix.KinfoProc, dir string, mountpointOf func(pid int) string) []orphanCandidate {
	var cands []orphanCandidate
	for i := range procs {
		p := &procs[i].Proc
		if commName(p.P_comm[:]) != nfsServerComm {
			continue
		}
		pid := int(p.P_pid)
		if mountpointOf(pid) == dir {
			cands = append(cands, orphanCandidate{pid: pid, mp: dir, start: procStamp{sec: p.P_starttime.Sec, usec: p.P_starttime.Usec}})
		}
	}
	return cands
}

// ReapOrphaned force-kills orphaned go-nfsv4 servers of ANY generation under
// roots — carcass-proven only (a root's stat answers a dead errno; a hanging
// or healthy stat is NEVER a carcass), re-confirmed at kill time, never a
// live mount's server. Returns the PIDs killed. See ccn doc 501ce12.
func ReapOrphaned(roots []string) []int {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.all")
	if err != nil {
		return nil
	}
	cands := crossGenOrphanCandidates(procs, roots, serverMountpoint, provenDead)
	var killed []int
	for _, c := range cands {
		if !reconfirmOrphan(c, commOfPid, serverMountpoint, startOfPid, provenDead) {
			continue
		}
		_ = unix.Kill(c.pid, unix.SIGKILL)
		killed = append(killed, c.pid)
	}
	return killed
}

// provenDead is the reaper's carcass gate — carcass proof v2's errno leg:
// only an immediately-answered dead errno reads dead. A hang (Hung) is never
// proof; ENOENT/healthy is not a carcass.
func provenDead(dir string) bool { return Probe(dir) == Dead }

// crossGenOrphanCandidates is pure so the safety-critical cross-generation
// kill decision is testable without real processes or signals.
func crossGenOrphanCandidates(procs []unix.KinfoProc, roots []string, mountpointOf func(pid int) string, dead func(dir string) bool) []orphanCandidate {
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
			v = dead(mp)
			verdicts[mp] = v
		}
		if v {
			cands = append(cands, orphanCandidate{pid: int(p.P_pid), mp: mp, start: procStamp{sec: p.P_starttime.Sec, usec: p.P_starttime.Usec}})
		}
	}
	return cands
}

// reconfirmOrphan re-validates one candidate immediately before its kill: a
// FRESH dead verdict (never memoized across the kill loop; see ccn doc
// 501ce12) FIRST — it can take ProbeDeadline — then pidStillServes and the
// scan-time full start stamp re-read fresh (a reused pid has a different
// stamp — never shot) last, so the identity check sits tight against the kill.
func reconfirmOrphan(c orphanCandidate, commOf, mountpointOf func(pid int) string, startOf func(pid int) procStamp, dead func(dir string) bool) bool {
	return dead(c.mp) && pidStillServes(c.pid, c.mp, commOf, mountpointOf) && startOf(c.pid) == c.start
}

// pidStillServes reports whether pid still reads comm go-nfsv4 with argv
// mountpoint dir — half the kill-time PID-reuse guard; the fresh full-start
// re-read is the other half.
func pidStillServes(pid int, dir string, commOf, mountpointOf func(pid int) string) bool {
	return commOf(pid) == nfsServerComm && mountpointOf(pid) == dir
}

// startOfPid reads pid's current full start stamp from the kernel; the zero
// stamp when the pid is gone, so a compare against a scan-time anchor spares it.
func startOfPid(pid int) procStamp {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return procStamp{}
	}
	return procStamp{sec: kp.Proc.P_starttime.Sec, usec: kp.Proc.P_starttime.Usec}
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

// commName trims a NUL-terminated, MAXCOMLEN-truncated kinfo_proc p_comm.
func commName(b []byte) string {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		b = b[:i]
	}
	return string(b)
}

// serverMountpoint returns pid's last argv element — go-nfsv4's mountpoint,
// which for a mux server is the shared native/mux root — from the kernel's
// exec-time argv copy. "" on any failure, so the caller skips the pid rather
// than risk a wrong kill.
func serverMountpoint(pid int) string {
	buf, err := unix.SysctlRaw("kern.procargs2", pid)
	if err != nil {
		return ""
	}
	mp, err := parseLastArg(buf)
	if err != nil {
		return ""
	}
	return mp
}

// parseLastArg extracts the final argv string from a kern.procargs2 blob:
// uint32 argc, NUL-terminated exec path, NUL padding, then argc NUL-terminated
// argv strings (environment follows, ignored). A short buffer, non-positive
// argc, fewer than argc decoded args, or an unterminated final arg is an
// ERROR — a truncated snapshot must never pass for a usable mountpoint (the
// force gate turns it into ErrUndetermined).
func parseLastArg(buf []byte) (string, error) {
	if len(buf) < 4 {
		return "", fmt.Errorf("procargs buffer too short (%d bytes)", len(buf))
	}
	argc := int(binary.LittleEndian.Uint32(buf))
	if argc <= 0 {
		return "", fmt.Errorf("procargs argc = %d", argc)
	}
	rest := buf[4:]
	i := bytes.IndexByte(rest, 0)
	if i < 0 {
		return "", errors.New("procargs exec path unterminated")
	}
	rest = rest[i:]
	for len(rest) > 0 && rest[0] == 0 {
		rest = rest[1:]
	}
	var last string
	for n := 0; n < argc; n++ {
		j := bytes.IndexByte(rest, 0)
		if j < 0 {
			return "", fmt.Errorf("procargs truncated: arg %d of %d missing or unterminated", n+1, argc)
		}
		last = string(rest[:j])
		rest = rest[j+1:]
	}
	return last, nil
}
