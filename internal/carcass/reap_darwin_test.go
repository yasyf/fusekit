//go:build darwin

package carcass

import (
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// procargs2 builds a kern.procargs2-shaped blob (layout: see parseLastArg),
// minus the kernel-appended trailing environ.
func procargs2(execPath string, argv ...string) []byte {
	var b []byte
	b = binary.LittleEndian.AppendUint32(b, uint32(len(argv)))
	b = append(b, execPath...)
	b = append(b, 0, 0) // exec-path terminator + one pad NUL before argv[0]
	for _, a := range argv {
		b = append(b, a...)
		b = append(b, 0)
	}
	return b
}

func TestParseLastArg(t *testing.T) {
	const mp = "/Users/x/.cc-pool/accounts/acct-01"
	full := procargs2("/usr/local/bin/go-nfsv4", "go-nfsv4", "--volname", "cc-pool-acct-01", "--attrcache=false", mp)
	tests := []struct {
		name    string
		buf     []byte
		want    string
		wantErr bool
	}{
		{
			name: "go-nfsv4 argv: mountpoint is the last arg",
			buf:  full,
			want: mp,
		},
		{name: "single arg", buf: procargs2("/bin/foo", "foo"), want: "foo"},
		{name: "too short", buf: []byte{1, 2}, wantErr: true},
		{name: "argc zero", buf: procargs2("/bin/foo"), wantErr: true},
		{name: "empty", buf: nil, wantErr: true},
		// R4-2a: a truncated snapshot is an ERROR, never a usable value — the
		// last decoded arg could be --volname while the real mountpoint arg
		// was cut off, and the force gate would then miss a live server.
		{name: "unterminated final arg is an error", buf: full[:len(full)-1], wantErr: true},
		{name: "fewer than argc args is an error", buf: full[:len(full)-len(mp)-1], wantErr: true},
		{name: "exec path unterminated is an error", buf: append(binary.LittleEndian.AppendUint32(nil, 1), 'x'), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseLastArg(tc.buf)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseLastArg = %q, want an error (truncated/malformed buffer)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseLastArg: %v", err)
			}
			if got != tc.want {
				t.Errorf("parseLastArg = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestLiveMountpointFailsClosed pins R4-2's enumeration ladder: only a
// POSITIVE pid-gone signal (or a readable non-go-nfsv4 comm) drops a pid from
// the force-gate scan; a procargs failure with an unreadable comm, a
// truncated buffer, or a still-go-nfsv4 comm is an ERROR the gate turns into
// ErrUndetermined — never a silent "process exited".
func TestLiveMountpointFailsClosed(t *testing.T) {
	const mp = "/pool/acct-01"
	good := procargs2("/usr/local/bin/go-nfsv4", "go-nfsv4", mp)
	argsErr := errors.New("sysctl procargs2: EINVAL")
	commErr := errors.New("sysctl kern.proc.pid: transient EPERM")
	tests := []struct {
		name    string
		buf     []byte
		argsErr error
		gone    bool
		comm    string
		commErr error
		want    string
		wantErr bool
	}{
		{name: "readable argv yields the mountpoint", buf: good, want: mp},
		{name: "truncated argv is an error", buf: good[:len(good)-1], wantErr: true},
		{name: "procargs failed but pid positively gone drops the pid", argsErr: argsErr, gone: true},
		{name: "procargs failed and comm morphed drops the pid", argsErr: argsErr, comm: "bash"},
		{name: "procargs failed with comm still go-nfsv4 is an error", argsErr: argsErr, comm: "go-nfsv4", wantErr: true},
		{name: "DOUBLE FAILURE (procargs and comm unreadable) is an error, not exited", argsErr: argsErr, commErr: commErr, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gone := func(int) bool { return tc.gone }
			commOf := func(int) (string, error) { return tc.comm, tc.commErr }
			got, err := liveMountpoint(42, tc.buf, tc.argsErr, gone, commOf)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("liveMountpoint = %q, want an error (enumeration must fail closed)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("liveMountpoint: %v", err)
			}
			if got != tc.want {
				t.Errorf("liveMountpoint = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPidGoneNeverPositiveForALivePid(t *testing.T) {
	if pidGone(os.Getpid()) {
		t.Fatal("pidGone(self) = true — a live pid must never read positively exited")
	}
}

func TestCommName(t *testing.T) {
	var c [17]byte
	copy(c[:], "go-nfsv4")
	if got := commName(c[:]); got != "go-nfsv4" {
		t.Errorf("commName = %q, want go-nfsv4", got)
	}
	// Max-length name: no NUL terminator.
	full := []byte("abcdefghijklmnop")
	if got := commName(full); got != "abcdefghijklmnop" {
		t.Errorf("commName(full) = %q, want the whole slice", got)
	}
}

func kproc(comm string, pid int32) unix.KinfoProc {
	var kp unix.KinfoProc
	copy(kp.Proc.P_comm[:], comm)
	kp.Proc.P_pid = pid
	return kp
}

// cpids projects candidate pids so cases can assert on pid lists while the
// candidate carries the mountpoint the kill-time reconfirm re-stats.
func cpids(cands []orphanCandidate) []int {
	var pids []int
	for _, c := range cands {
		pids = append(pids, c.pid)
	}
	return pids
}

func TestCrossGenOrphanCandidates(t *testing.T) {
	const (
		root    = "/pool/accounts"
		carcMP  = "/pool/accounts/acct-01"
		liveMP  = "/pool/accounts/acct-02"
		foreign = "/other/consumer/mnt"
	)
	dead := func(dir string) bool { return dir == carcMP || dir == foreign || dir == root }

	tests := []struct {
		name  string
		procs []unix.KinfoProc
		roots []string
		mpOf  map[int]string
		want  []int
	}{
		{
			name: "kills only proven-dead servers under the roots",
			procs: []unix.KinfoProc{
				kproc("go-nfsv4", 200), // carcass under root → reap
				kproc("go-nfsv4", 201), // LIVE mount under root → keep
				kproc("go-nfsv4", 202), // carcass but OUTSIDE roots → keep
				kproc("bash", 203),     // wrong comm, carcass under root → keep
				kproc("go-nfsv4", 204), // no argv mountpoint → keep
				kproc("go-nfsv4", 205), // second server on the same carcass → reap
			},
			roots: []string{root},
			mpOf:  map[int]string{200: carcMP, 201: liveMP, 202: foreign, 203: carcMP, 205: carcMP},
			want:  []int{200, 205},
		},
		{
			name:  "mountpoint equal to a root counts as under it",
			procs: []unix.KinfoProc{kproc("go-nfsv4", 210)},
			roots: []string{root},
			mpOf:  map[int]string{210: root},
			want:  []int{210},
		},
		{
			name:  "sibling path sharing the root as a string prefix is outside",
			procs: []unix.KinfoProc{kproc("go-nfsv4", 220)},
			roots: []string{"/pool/acct-01"},
			mpOf:  map[int]string{220: "/pool/acct-010"},
			want:  nil,
		},
		{
			name:  "no roots kills nothing",
			procs: []unix.KinfoProc{kproc("go-nfsv4", 230)},
			roots: nil,
			mpOf:  map[int]string{230: carcMP},
			want:  nil,
		},
		{
			name: "second of several roots matches",
			procs: []unix.KinfoProc{
				kproc("go-nfsv4", 240),
				kproc("go-nfsv4", 241), // live under the second root → keep
			},
			roots: []string{"/elsewhere", "/other/consumer"},
			mpOf:  map[int]string{240: foreign, 241: "/other/consumer/live"},
			want:  []int{240},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mpOf := func(pid int) string { return tc.mpOf[pid] }
			got := crossGenOrphanCandidates(tc.procs, tc.roots, mpOf, dead)
			if !reflect.DeepEqual(cpids(got), tc.want) {
				t.Errorf("crossGenOrphanCandidates = %v, want pids %v", got, tc.want)
			}
			for _, c := range got {
				if c.mp != tc.mpOf[c.pid] {
					t.Errorf("candidate %d carries mp %q, want its scanned mountpoint %q", c.pid, c.mp, tc.mpOf[c.pid])
				}
			}
		})
	}
}

func TestCrossGenOrphanCandidatesMemoizesDeadVerdict(t *testing.T) {
	const mp = "/pool/accounts/acct-01"
	procs := []unix.KinfoProc{
		kproc("go-nfsv4", 300),
		kproc("go-nfsv4", 301),
		kproc("go-nfsv4", 302),
	}
	calls := 0
	dead := func(string) bool { calls++; return true }
	got := crossGenOrphanCandidates(procs, []string{"/pool/accounts"}, func(int) string { return mp }, dead)
	if want := []int{300, 301, 302}; !reflect.DeepEqual(cpids(got), want) {
		t.Errorf("crossGenOrphanCandidates = %v, want pids %v", got, want)
	}
	if calls != 1 {
		t.Errorf("dead verdicts for one mountpoint = %d, want 1 (memoized — the stat can park %v)", calls, ProbeDeadline)
	}
}

// TestReconfirmOrphan pins the kill-time TOCTOU gate: between the scan and the
// SIGKILL a fresh server can mount over the same path and a PID can be reused,
// so each kill re-checks comm, argv mountpoint, AND the scan-time FULL start
// stamp (sec+usec), never trusting the scan-time verdict.
func TestReconfirmOrphan(t *testing.T) {
	const mp = "/pool/accounts/acct-01"
	scanStart := procStamp{sec: 123_456, usec: 250_000}
	tests := []struct {
		name     string
		comm     string
		nowMp    string    // argv mountpoint re-read at kill time; "" defaults to mp
		nowStart procStamp // full start stamp re-read at kill time; zero defaults to the scan's
		dead     bool
		want     bool
	}{
		{name: "still a go-nfsv4 on a still-dead mount is killed", comm: "go-nfsv4", dead: true, want: true},
		{name: "mountpoint healthy again at kill time is spared", comm: "go-nfsv4", dead: false, want: false},
		{name: "pid reused by another process is spared", comm: "bash", dead: true, want: false},
		{name: "pid gone (comm unreadable) is spared", comm: "", dead: true, want: false},
		{name: "pid reused by a go-nfsv4 on a DIFFERENT mount is spared", comm: "go-nfsv4", nowMp: "/pool/accounts/acct-02", dead: true, want: false},
		{name: "pid reused by a same-shaped go-nfsv4 with a DIFFERENT start second is spared", comm: "go-nfsv4", nowStart: procStamp{sec: 999_999, usec: 250_000}, dead: true, want: false},
		{name: "SAME-SECOND pid reuse differs only in usec and is spared", comm: "go-nfsv4", nowStart: procStamp{sec: 123_456, usec: 250_001}, dead: true, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := orphanCandidate{pid: 400, mp: mp, start: scanStart}
			commOf := func(pid int) string {
				if pid != 400 {
					t.Fatalf("comm re-check hit pid %d, want the candidate's 400", pid)
				}
				return tc.comm
			}
			mpOf := func(pid int) string {
				if pid != 400 {
					t.Fatalf("mountpoint re-read hit pid %d, want the candidate's 400", pid)
				}
				if tc.nowMp != "" {
					return tc.nowMp
				}
				return mp
			}
			startOf := func(pid int) procStamp {
				if pid != 400 {
					t.Fatalf("start re-read hit pid %d, want the candidate's 400", pid)
				}
				if tc.nowStart != (procStamp{}) {
					return tc.nowStart
				}
				return scanStart
			}
			dead := func(dir string) bool {
				if dir != mp {
					t.Fatalf("kill-time re-stat hit %q, want the candidate's mountpoint %q", dir, mp)
				}
				return tc.dead
			}
			if got := reconfirmOrphan(c, commOf, mpOf, startOf, dead); got != tc.want {
				t.Errorf("reconfirmOrphan = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestReconfirmOrphanStatsFreshPerCandidate pins the safety-critical invariant
// that the kill-time dead verdict is NOT memoized across the kill loop: two
// candidates scanned on the same mountpoint are each re-stat'd, so a mount that
// comes back live between the first kill and the second candidate spares the
// second (a fresh server on that path is never felled on a stale verdict).
func TestReconfirmOrphanStatsFreshPerCandidate(t *testing.T) {
	const mp = "/pool/accounts/acct-01"
	stats := 0
	live := false // flips to live after the first reconfirm
	dead := func(string) bool { stats++; c := !live; live = true; return c }
	comm := func(int) string { return "go-nfsv4" }
	mpOf := func(int) string { return mp }

	startOf := func(int) procStamp { return procStamp{} }
	if !reconfirmOrphan(orphanCandidate{pid: 500, mp: mp}, comm, mpOf, startOf, dead) {
		t.Fatal("first candidate on a dead mount was spared")
	}
	if reconfirmOrphan(orphanCandidate{pid: 501, mp: mp}, comm, mpOf, startOf, dead) {
		t.Fatal("second candidate killed on a mount that came back live — stale memoized verdict")
	}
	if stats != 2 {
		t.Errorf("dead re-stats = %d, want 2 (fresh per candidate, no cross-loop memoization)", stats)
	}
}

func TestUnderAny(t *testing.T) {
	tests := []struct {
		name  string
		mp    string
		roots []string
		want  bool
	}{
		{name: "direct child", mp: "/r/a", roots: []string{"/r"}, want: true},
		{name: "deep descendant", mp: "/r/a/b/c", roots: []string{"/r"}, want: true},
		{name: "equals the root", mp: "/r", roots: []string{"/r"}, want: true},
		{name: "unclean paths are normalized", mp: "/r//a/", roots: []string{"/r/"}, want: true},
		{name: "string-prefix sibling is outside", mp: "/root2", roots: []string{"/root"}, want: false},
		{name: "parent of the root is outside", mp: "/", roots: []string{"/r"}, want: false},
		{name: "empty root never matches", mp: "/r/a", roots: []string{""}, want: false},
		{name: "no roots", mp: "/r/a", roots: nil, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := underAny(tc.mp, tc.roots); got != tc.want {
				t.Errorf("underAny(%q, %v) = %v, want %v", tc.mp, tc.roots, got, tc.want)
			}
		})
	}
}

func TestOwnChildCandidates(t *testing.T) {
	const target = "/pool/acct-01"
	procs := []unix.KinfoProc{
		kproc("go-nfsv4", 100), // serves the target dir → reap
		kproc("go-nfsv4", 101), // serves a DIFFERENT dir → keep
		kproc("bash", 102),     // not an NFS server → keep, even on the dir
		kproc("go-nfsv4", 103), // serves the target dir → reap
	}
	mountpointOf := func(pid int) string {
		switch pid {
		case 100, 103:
			return target
		case 101:
			return "/pool/acct-02"
		case 102:
			return target
		}
		return ""
	}
	got := ownChildCandidates(procs, target, mountpointOf)
	want := []int{100, 103}
	if !reflect.DeepEqual(cpids(got), want) {
		t.Errorf("ownChildCandidates = %v, want pids %v (only go-nfsv4 children bound to the dir)", got, want)
	}
}

// TestDirServerCandidatesCarryPpid pins that the pre-force server scan carries
// the parent pid — the live-child (own live server) refusal keys on it.
func TestDirServerCandidatesCarryPpid(t *testing.T) {
	const target = "/pool/acct-01"
	kp := kproc("go-nfsv4", 600)
	kp.Eproc.Ppid = 555
	got, err := dirServerCandidates([]unix.KinfoProc{kp}, target, func(int) (string, error) { return target, nil })
	if err != nil {
		t.Fatalf("dirServerCandidates: %v", err)
	}
	if len(got) != 1 || got[0].ppid != 555 {
		t.Fatalf("dirServerCandidates = %+v, want one candidate with ppid 555", got)
	}
}

func swapServerSeams(t *testing.T, scan func(dir string) ([]orphanCandidate, error)) *[]int {
	t.Helper()
	prevScan, prevKill := dirServersFn, killFn
	killed := &[]int{}
	dirServersFn = scan
	killFn = func(pid int) { *killed = append(*killed, pid) }
	t.Cleanup(func() { dirServersFn, killFn = prevScan, prevKill })
	return killed
}

func swapServerKillWait(t *testing.T, d time.Duration) {
	t.Helper()
	prev := serverKillWait
	serverKillWait = d
	t.Cleanup(func() { serverKillWait = prev })
}

func TestEnsureServersDead(t *testing.T) {
	const dir = "/pool/acct-01"
	self := os.Getpid()
	t.Run("no servers is proven dead", func(t *testing.T) {
		killed := swapServerSeams(t, func(string) ([]orphanCandidate, error) { return nil, nil })
		if err := ensureServersDead(dir); err != nil {
			t.Fatalf("ensureServersDead(no servers) = %v, want nil", err)
		}
		if len(*killed) != 0 {
			t.Fatalf("killed %v with no candidates", *killed)
		}
	})
	t.Run("own live child means a live server — defer, never kill (assertions #6/#9)", func(t *testing.T) {
		killed := swapServerSeams(t, func(string) ([]orphanCandidate, error) {
			return []orphanCandidate{{pid: 700, ppid: self, mp: dir}}, nil
		})
		err := ensureServersDead(dir)
		if !errors.Is(err, ErrUndetermined) {
			t.Fatalf("ensureServersDead(own live child) = %v, want ErrUndetermined", err)
		}
		if len(*killed) != 0 {
			t.Fatalf("killed our own live child %v — the dead errno was a denial, not death", *killed)
		}
	})
	t.Run("orphan surviving the kill defers", func(t *testing.T) {
		swapServerKillWait(t, 50*time.Millisecond)
		// The orphan never dies: the scan keeps returning it. reconfirm fails
		// (no real pid 701 named go-nfsv4), so no kill is recorded, and the
		// confirm loop must still refuse the force.
		swapServerSeams(t, func(string) ([]orphanCandidate, error) {
			return []orphanCandidate{{pid: 701, ppid: 1, mp: dir}}, nil
		})
		err := ensureServersDead(dir)
		if !errors.Is(err, ErrUndetermined) {
			t.Fatalf("ensureServersDead(surviving server) = %v, want ErrUndetermined", err)
		}
	})
	t.Run("orphan that dies after the kill is proven dead", func(t *testing.T) {
		swapServerKillWait(t, time.Second)
		scans := 0
		swapServerSeams(t, func(string) ([]orphanCandidate, error) {
			scans++
			if scans == 1 {
				return []orphanCandidate{{pid: 702, ppid: 1, mp: dir}}, nil
			}
			return nil, nil
		})
		if err := ensureServersDead(dir); err != nil {
			t.Fatalf("ensureServersDead(orphan died) = %v, want nil", err)
		}
	})
}

// TestEnsureServersDeadFailsClosedOnScanError pins R3's fail-closed force
// gate: a process-enumeration failure is NEVER an empty candidate set — the
// force defers with ErrUndetermined and nothing is killed. Zero candidates
// prove death only off a full, successful scan (the passing leg above).
func TestEnsureServersDeadFailsClosedOnScanError(t *testing.T) {
	killed := swapServerSeams(t, func(string) ([]orphanCandidate, error) {
		return nil, errors.New("sysctl kern.proc.all: transient EPERM")
	})
	err := ensureServersDead("/pool/acct-01")
	if !errors.Is(err, ErrUndetermined) {
		t.Fatalf("ensureServersDead(scan error) = %v, want ErrUndetermined", err)
	}
	if len(*killed) != 0 {
		t.Fatalf("killed %v under a failed scan", *killed)
	}
}

// TestEnsureServersDeadFailsClosedOnRescanError covers the post-kill confirm
// loop: a scan that succeeds first but fails on the death re-check also
// defers — the confirm is part of the proof.
func TestEnsureServersDeadFailsClosedOnRescanError(t *testing.T) {
	swapServerKillWait(t, time.Second)
	scans := 0
	swapServerSeams(t, func(string) ([]orphanCandidate, error) {
		scans++
		if scans == 1 {
			return []orphanCandidate{{pid: 800, ppid: 1, mp: "/pool/acct-01"}}, nil
		}
		return nil, errors.New("sysctl went away mid-confirm")
	})
	if err := ensureServersDead("/pool/acct-01"); !errors.Is(err, ErrUndetermined) {
		t.Fatalf("ensureServersDead(re-scan error) = %v, want ErrUndetermined", err)
	}
}

// TestDirServerCandidatesFailClosedOnUnreadableArgv pins the second R3 leg:
// an unreadable argv on a go-nfsv4-shaped pid aborts the WHOLE scan (it could
// hide a matching server); a wrong-comm pid's argv is never read at all.
func TestDirServerCandidatesFailClosedOnUnreadableArgv(t *testing.T) {
	const target = "/pool/acct-01"
	procs := []unix.KinfoProc{
		kproc("go-nfsv4", 810), // readable, matches
		kproc("go-nfsv4", 811), // argv unreadable -> scan error
		kproc("bash", 812),     // wrong comm: argv must not be read
	}
	mpOf := func(pid int) (string, error) {
		switch pid {
		case 810:
			return target, nil
		case 811:
			return "", errors.New("procargs of go-nfsv4 pid 811 unreadable: EPERM")
		default:
			t.Fatalf("argv read for wrong-comm pid %d", pid)
			return "", nil
		}
	}
	cands, err := dirServerCandidates(procs, target, mpOf)
	if err == nil {
		t.Fatalf("dirServerCandidates = %v, want an error (unreadable argv could hide a matching server)", cands)
	}
	// Negative leg: all argvs readable -> the matching pid is the only candidate.
	okMp := func(pid int) (string, error) {
		if pid == 810 {
			return target, nil
		}
		return "/elsewhere", nil
	}
	cands, err = dirServerCandidates(procs[:2], target, okMp)
	if err != nil {
		t.Fatalf("dirServerCandidates(clean) = %v", err)
	}
	if want := []int{810}; !reflect.DeepEqual(cpids(cands), want) {
		t.Fatalf("candidates = %v, want %v", cands, want)
	}
}
