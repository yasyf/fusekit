//go:build darwin

package fusekit

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

// statfsAt builds a mount-table entry; short names leave the fixed-size fields
// NUL-padded, as the kernel reports them.
func statfsAt(mntonname, mntfromname string) unix.Statfs_t {
	var s unix.Statfs_t
	copy(s.Mntonname[:], mntonname)
	copy(s.Mntfromname[:], mntfromname)
	return s
}

// TestMountpointIn pins byte-exact f_mntonname membership.
func TestMountpointIn(t *testing.T) {
	table := []unix.Statfs_t{
		statfsAt("/", "/dev/disk1s1"),
		statfsAt("/dev", "devfs"),
		statfsAt("/System/Volumes/Data", "/dev/disk1s2"),
	}
	cases := []struct {
		name       string
		table      []unix.Statfs_t
		candidates []string
		want       bool
	}{
		{
			name:       "exact match is a mountpoint",
			table:      table,
			candidates: []string{"/dev"},
			want:       true,
		},
		{
			name:       "absent path is not a mountpoint",
			table:      table,
			candidates: []string{"/no/such/mount"},
			want:       false,
		},
		{
			name:       "a later candidate matches when the first does not",
			table:      table,
			candidates: []string{"/raw/spelling", "/dev"},
			want:       true,
		},
		{
			name:       "a candidate that is a prefix of a mountpoint does not match",
			table:      []unix.Statfs_t{statfsAt("/var/folders/x/T/acct-01", "fuse")},
			candidates: []string{"/var"},
			want:       false,
		},
		{
			name:       "a longer prefix of a mountpoint still does not match",
			table:      []unix.Statfs_t{statfsAt("/var/folders/x/T/acct-01", "fuse")},
			candidates: []string{"/var/folders/x"},
			want:       false,
		},
		{
			name:       "a dir under a non-root mountpoint is not itself a mountpoint",
			table:      []unix.Statfs_t{statfsAt("/mnt/data", "/dev/disk2")},
			candidates: []string{"/mnt/data/sub"},
			want:       false,
		},
		{
			name:       "the parent-resolved candidate matches the kernel's firmlinked spelling",
			table:      []unix.Statfs_t{statfsAt("/private/var/folders/x/T/acct-01", "fuse")},
			candidates: []string{"/var/folders/x/T/acct-01", "/private/var/folders/x/T/acct-01"},
			want:       true,
		},
		{
			name:       "f_mntfromname is never consulted",
			table:      []unix.Statfs_t{statfsAt("/somewhere/else", "/p/acct-01")},
			candidates: []string{"/p/acct-01"},
			want:       false,
		},
		{
			name:       "the name is NUL-terminated and bytes after the first NUL are ignored",
			table:      []unix.Statfs_t{statfsAt("/p/acct-01\x00stale-leftover", "fuse")},
			candidates: []string{"/p/acct-01"},
			want:       true,
		},
		{
			name:       "an empty table never matches",
			table:      nil,
			candidates: []string{"/dev"},
			want:       false,
		},
		{
			name:       "no candidates never match",
			table:      table,
			candidates: nil,
			want:       false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mountpointIn(tc.table, tc.candidates); got != tc.want {
				t.Errorf("mountpointIn = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestMountCandidates pins the candidate spellings: raw dir first, resolved-parent second.
func TestMountCandidates(t *testing.T) {
	t.Run("clean is applied and an unresolvable parent degrades to raw-only", func(t *testing.T) {
		// /p does not exist, so the parent cannot resolve.
		got := mountCandidates("/p/x/../acct-01")
		want := []string{"/p/acct-01"}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("mountCandidates = %v, want %v", got, want)
		}
	})

	t.Run("an already-resolved parent yields a single candidate", func(t *testing.T) {
		realParent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(realParent, "child")
		got := mountCandidates(dir)
		want := []string{dir}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("mountCandidates = %v, want %v", got, want)
		}
	})

	t.Run("a symlinked parent adds the resolved spelling, raw first", func(t *testing.T) {
		realParent, err := filepath.EvalSymlinks(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(realParent, link); err != nil {
			t.Fatal(err)
		}
		dir := filepath.Join(link, "child")
		got := mountCandidates(dir)
		want := []string{dir, filepath.Join(realParent, "child")}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("mountCandidates = %v, want %v", got, want)
		}
		if got[0] != dir {
			t.Errorf("candidates[0] = %q, want the raw dir %q", got[0], dir)
		}
	})
}

// TestMountedRealTable exercises Mounted against the real mount table; a missing
// path answers false without blocking.
func TestMountedRealTable(t *testing.T) {
	if !Mounted("/") {
		t.Error("Mounted(/) = false; the root volume is always a mountpoint")
	}
	if !Mounted("/dev") {
		t.Error("Mounted(/dev) = false; devfs is always a mountpoint")
	}
	if Mounted(t.TempDir()) {
		t.Error("Mounted(tempdir) = true; a fresh temp dir is not a mountpoint")
	}
	missing := filepath.Join(t.TempDir(), "definitely-not-a-mountpoint")
	if Mounted(missing) {
		t.Errorf("Mounted(%q) = true; a missing path is not a mountpoint", missing)
	}
}
