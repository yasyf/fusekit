//go:build darwin

package fusekit

import (
	"encoding/binary"
	"reflect"
	"testing"

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
	tests := []struct {
		name string
		buf  []byte
		want string
	}{
		{
			name: "go-nfsv4 argv: mountpoint is the last arg",
			buf:  procargs2("/usr/local/bin/go-nfsv4", "go-nfsv4", "--volname", "cc-pool-acct-01", "--attrcache=false", mp),
			want: mp,
		},
		{name: "single arg", buf: procargs2("/bin/foo", "foo"), want: "foo"},
		{name: "too short", buf: []byte{1, 2}, want: ""},
		{name: "argc zero", buf: procargs2("/bin/foo"), want: ""},
		{name: "empty", buf: nil, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseLastArg(tc.buf); got != tc.want {
				t.Errorf("parseLastArg = %q, want %q", got, tc.want)
			}
		})
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

func TestOrphanServerPIDs(t *testing.T) {
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
	got := orphanServerPIDs(procs, target, mountpointOf)
	want := []int{100, 103}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("orphanServerPIDs = %v, want %v (only go-nfsv4 children bound to the dir)", got, want)
	}
}
