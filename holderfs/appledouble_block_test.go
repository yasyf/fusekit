//go:build fuse && cgo && darwin

package holderfs

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/content"
)

// TestBuildOptionsOmitNamedattr pins the namedattr mitigation: the holder
// mounts WITHOUT the NFSv4 named-attribute option — that vnode path is
// implicated in macOS nfs_vinvalbuf2 kernel panics — and must never regress
// to setting it. Exact-slice equality also catches an accidental new option.
func TestBuildOptionsOmitNamedattr(t *testing.T) {
	base, dir := t.TempDir(), t.TempDir()
	cfg, err := Build(fusekit.MountSpec{Base: base, Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(cfg.Options, "namedattr") {
		t.Fatalf("Build options contain namedattr; got %v", cfg.Options)
	}
	want := []string{
		"-o", "volname=holder-" + filepath.Base(dir),
		"-o", "noattrcache",
		"-o", "nobrowse",
		"-o", "rwsize=1048576",
	}
	if !slices.Equal(cfg.Options, want) {
		t.Fatalf("Build options = %v, want %v", cfg.Options, want)
	}
}

// newLitteredFS builds a holderFS over real temp dirs with AppleDouble litter
// pre-planted on the backing store (Base top-level, a nested dir, and
// PrivateRoot), plus look-alike names that are NOT AppleDouble. The "._synth"
// entry models a hostile manifest name; its bridge client points at a
// nonexistent socket, so any accidental refresh fails cleanly.
func newLitteredFS(t *testing.T) (*holderFS, string, string) {
	t.Helper()
	base, priv := t.TempDir(), t.TempDir()
	for _, d := range []string{
		filepath.Join(base, "nested"),
		filepath.Join(base, "._litterdir"),
	} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for p, data := range map[string]string{
		filepath.Join(base, "._litter"):             "AD",
		filepath.Join(base, "real.json"):            "{}",
		filepath.Join(base, ".foo"):                 "dot",
		filepath.Join(base, "..data"):               "dotdot",
		filepath.Join(base, "x._y"):                 "interior",
		filepath.Join(base, "nested", "._litter"):   "AD",
		filepath.Join(base, "nested", "inner.json"): "{}",
		filepath.Join(priv, "._daemon"):             "AD", // sidecar of an exact-private name
		filepath.Join(priv, "._.claude.json"):       "AD", // sidecar of a prefix-private name
		filepath.Join(priv, "._synthback"):          "SYNTH",
	} {
		if err := os.WriteFile(p, []byte(data), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("elsewhere", filepath.Join(base, "._symlitter")); err != nil {
		t.Fatal(err)
	}
	fs := &holderFS{
		base:            base,
		privateRoot:     priv,
		privateExact:    map[string]bool{"daemon": true},
		privatePrefixes: []string{".claude.json"},
		shared:          map[string]sharedEntry{},
		synth: map[string]*synthView{
			"/._synth": newSynthView("._synth", "d",
				content.NewBridgeClient(filepath.Join(priv, "no.sock")),
				filepath.Join(priv, "._synthback"), nil),
		},
		synthFhs:    map[uint64]*synthHandle{},
		nextSynthFh: synthFhBase,
	}
	return fs, base, priv
}

// TestAppleDoubleCreatingOpsEACCES pins that every op which would bring a
// "._" basename into existence answers EACCES and leaves the backing store
// untouched, at any depth, on both the Base and PrivateRoot routes.
func TestAppleDoubleCreatingOpsEACCES(t *testing.T) {
	fs, base, priv := newLitteredFS(t)
	eacces := -int(syscall.EACCES)
	cases := []struct {
		name    string
		op      func() int
		absent  []string // must not exist on the backing store afterward
		present []string // must remain in place afterward
	}{
		{
			name: "Create top-level",
			op: func() int {
				rc, _ := fs.Create("/._new", syscall.O_WRONLY, 0o644)
				return rc
			},
			absent: []string{filepath.Join(base, "._new")},
		},
		{
			name: "Create nested",
			op: func() int {
				rc, _ := fs.Create("/nested/._new", syscall.O_WRONLY, 0o644)
				return rc
			},
			absent: []string{filepath.Join(base, "nested", "._new")},
		},
		{
			name: "Create sidecar of a private-prefix name lands nowhere",
			op: func() int {
				rc, _ := fs.Create("/._.claude.json.tmp1", syscall.O_WRONLY, 0o644)
				return rc
			},
			absent: []string{
				filepath.Join(priv, "._.claude.json.tmp1"),
				filepath.Join(base, "._.claude.json.tmp1"),
			},
		},
		{
			name: "Open with O_CREAT",
			op: func() int {
				rc, _ := fs.Open("/._new", syscall.O_CREAT|syscall.O_WRONLY)
				return rc
			},
			absent: []string{filepath.Join(base, "._new")},
		},
		{
			name: "Open with O_CREAT on existing litter",
			op: func() int {
				rc, _ := fs.Open("/._litter", syscall.O_CREAT|syscall.O_RDWR)
				return rc
			},
			present: []string{filepath.Join(base, "._litter")},
		},
		{
			name:   "Mkdir top-level",
			op:     func() int { return fs.Mkdir("/._newdir", 0o755) },
			absent: []string{filepath.Join(base, "._newdir")},
		},
		{
			name:   "Mkdir nested",
			op:     func() int { return fs.Mkdir("/nested/._newdir", 0o755) },
			absent: []string{filepath.Join(base, "nested", "._newdir")},
		},
		{
			name:    "Rename onto an AppleDouble destination",
			op:      func() int { return fs.Rename("/real.json", "/._real.json") },
			absent:  []string{filepath.Join(base, "._real.json")},
			present: []string{filepath.Join(base, "real.json")},
		},
		{
			name:    "Rename onto a nested AppleDouble destination",
			op:      func() int { return fs.Rename("/nested/inner.json", "/nested/._inner.json") },
			absent:  []string{filepath.Join(base, "nested", "._inner.json")},
			present: []string{filepath.Join(base, "nested", "inner.json")},
		},
		{
			name:    "Link to an AppleDouble destination",
			op:      func() int { return fs.Link("/real.json", "/._hardlink") },
			absent:  []string{filepath.Join(base, "._hardlink")},
			present: []string{filepath.Join(base, "real.json")},
		},
		{
			name:   "Symlink at an AppleDouble destination",
			op:     func() int { return fs.Symlink("real.json", "/._symlink") },
			absent: []string{filepath.Join(base, "._symlink")},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if rc := tc.op(); rc != eacces {
				t.Fatalf("rc = %d, want EACCES (%d)", rc, eacces)
			}
			for _, p := range tc.absent {
				if _, err := os.Lstat(p); !errors.Is(err, os.ErrNotExist) {
					t.Errorf("%s reached the backing store through a blocked op (lstat err = %v)", p, err)
				}
			}
			for _, p := range tc.present {
				if _, err := os.Lstat(p); err != nil {
					t.Errorf("%s was disturbed by a blocked op: %v", p, err)
				}
			}
		})
	}
}

// TestAppleDoubleResolutionOpsENOENT pins that a "._" basename never resolves
// through the mount — even with litter sitting on the backing store — and that
// the blocked ops leave that litter exactly as it was.
func TestAppleDoubleResolutionOpsENOENT(t *testing.T) {
	fs, base, priv := newLitteredFS(t)
	enoent := -int(syscall.ENOENT)
	getattr := func(p string) func() int {
		return func() int {
			var st fuse.Stat_t
			return fs.Getattr(p, &st, ^uint64(0))
		}
	}
	cases := []struct {
		name    string
		op      func() int
		present []string // pre-existing litter a blocked op must leave in place
	}{
		{"Getattr top-level litter", getattr("/._litter"), []string{filepath.Join(base, "._litter")}},
		{"Getattr nested litter", getattr("/nested/._litter"), []string{filepath.Join(base, "nested", "._litter")}},
		{"Getattr private-exact sidecar litter", getattr("/._daemon"), []string{filepath.Join(priv, "._daemon")}},
		{"Getattr private-prefix sidecar litter", getattr("/._.claude.json"), []string{filepath.Join(priv, "._.claude.json")}},
		{"Getattr hostile synth name", getattr("/._synth"), []string{filepath.Join(priv, "._synthback")}},
		{"Getattr litter dir", getattr("/._litterdir"), []string{filepath.Join(base, "._litterdir")}},
		{"Open existing litter read-only", func() int {
			rc, _ := fs.Open("/._litter", syscall.O_RDONLY)
			return rc
		}, nil},
		{"Open existing litter read-write without create", func() int {
			rc, _ := fs.Open("/._litter", syscall.O_RDWR)
			return rc
		}, nil},
		{"Opendir litter dir", func() int {
			rc, _ := fs.Opendir("/._litterdir")
			return rc
		}, nil},
		{"Readdir litter dir", func() int {
			return fs.Readdir("/._litterdir", func(string, *fuse.Stat_t, int64) bool { return true }, 0, 0)
		}, nil},
		{"Readlink litter symlink", func() int {
			rc, _ := fs.Readlink("/._symlitter")
			return rc
		}, []string{filepath.Join(base, "._symlitter")}},
		{"Unlink litter", func() int { return fs.Unlink("/._litter") }, []string{filepath.Join(base, "._litter")}},
		{"Unlink nested litter", func() int { return fs.Unlink("/nested/._litter") }, []string{filepath.Join(base, "nested", "._litter")}},
		{"Rmdir empty litter dir", func() int { return fs.Rmdir("/._litterdir") }, []string{filepath.Join(base, "._litterdir")}},
		{"Rename litter source away", func() int { return fs.Rename("/._litter", "/promoted.json") }, []string{filepath.Join(base, "._litter")}},
		{"Link litter source", func() int { return fs.Link("/._litter", "/promoted.json") }, []string{filepath.Join(base, "._litter")}},
		{"Truncate litter by path", func() int { return fs.Truncate("/._litter", 0, ^uint64(0)) }, []string{filepath.Join(base, "._litter")}},
		{"Chmod litter", func() int { return fs.Chmod("/._litter", 0o600) }, nil},
		{"Chown litter", func() int { return fs.Chown("/._litter", uint32(os.Getuid()), uint32(os.Getgid())) }, nil},
		{"Utimens litter", func() int { return fs.Utimens("/._litter", []fuse.Timespec{{}, {}}) }, nil},
		{"Statfs litter", func() int {
			var st fuse.Statfs_t
			return fs.Statfs("/._litter", &st)
		}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if rc := tc.op(); rc != enoent {
				t.Fatalf("rc = %d, want ENOENT (%d)", rc, enoent)
			}
			for _, p := range tc.present {
				if _, err := os.Lstat(p); err != nil {
					t.Errorf("pre-existing litter %s was disturbed: %v", p, err)
				}
			}
		})
	}
	if _, err := os.Lstat(filepath.Join(base, "promoted.json")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a blocked rename/link source materialized promoted.json (lstat err = %v)", err)
	}
	if data, err := os.ReadFile(filepath.Join(base, "._litter")); err != nil || string(data) != "AD" {
		t.Errorf("blocked ops reached ._litter: data = %q, err = %v", data, err)
	}
}

// TestAppleDoubleReaddirFilters pins that no "._" basename is ever listed:
// not from Base, not from the PrivateRoot merge (whose isPrivate sidecar
// colocation would otherwise surface "._daemon"), and not from a synth entry.
func TestAppleDoubleReaddirFilters(t *testing.T) {
	fs, _, _ := newLitteredFS(t)
	readNames := func(t *testing.T, path string) map[string]bool {
		t.Helper()
		names := map[string]bool{}
		rc := fs.Readdir(path, func(name string, _ *fuse.Stat_t, _ int64) bool {
			names[name] = true
			return true
		}, 0, 0)
		if rc != 0 {
			t.Fatalf("Readdir(%s) = %d, want 0", path, rc)
		}
		return names
	}
	cases := []struct {
		name    string
		path    string
		want    []string
		wantNot []string
	}{
		{
			name:    "root hides base, private-merge, and synth litter",
			path:    "/",
			want:    []string{"real.json", ".foo", "..data", "x._y", "nested"},
			wantNot: []string{"._litter", "._litterdir", "._symlitter", "._daemon", "._.claude.json", "._synth"},
		},
		{
			name:    "nested dir hides its litter",
			path:    "/nested",
			want:    []string{"inner.json"},
			wantNot: []string{"._litter"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			names := readNames(t, tc.path)
			for _, n := range tc.want {
				if !names[n] {
					t.Errorf("Readdir(%s) is missing %q; got %v", tc.path, n, names)
				}
			}
			for _, n := range tc.wantNot {
				if names[n] {
					t.Errorf("Readdir(%s) lists %q, want it filtered", tc.path, n)
				}
			}
			for n := range names {
				if isAppleDouble(n) {
					t.Errorf("Readdir(%s) leaked AppleDouble name %q", tc.path, n)
				}
			}
		})
	}
}

// TestAppleDoubleNegativeNamesUntouched pins that look-alike names — ".foo",
// "..data", "x._y" — pass every op unimpeded: the block must match ONLY a
// "._" basename prefix.
func TestAppleDoubleNegativeNamesUntouched(t *testing.T) {
	fs, base, _ := newLitteredFS(t)

	t.Run("resolution and reads work", func(t *testing.T) {
		for _, p := range []string{"/.foo", "/..data", "/x._y"} {
			var st fuse.Stat_t
			if rc := fs.Getattr(p, &st, ^uint64(0)); rc != 0 {
				t.Errorf("Getattr(%s) = %d, want 0", p, rc)
			}
			rc, fh := fs.Open(p, syscall.O_RDONLY)
			if rc != 0 {
				t.Errorf("Open(%s) = %d, want 0", p, rc)
				continue
			}
			fs.Release(p, fh)
		}
	})

	t.Run("creation works", func(t *testing.T) {
		cases := []struct {
			name string
			op   func() int
			path string // backing path that must exist afterward
		}{
			{
				name: "Create dotfile",
				op: func() int {
					rc, fh := fs.Create("/.bar", syscall.O_WRONLY, 0o644)
					if rc == 0 {
						fs.Release("/.bar", fh)
					}
					return rc
				},
				path: filepath.Join(base, ".bar"),
			},
			{
				name: "Create interior-._ name",
				op: func() int {
					rc, fh := fs.Create("/y._z", syscall.O_WRONLY, 0o644)
					if rc == 0 {
						fs.Release("/y._z", fh)
					}
					return rc
				},
				path: filepath.Join(base, "y._z"),
			},
			{
				name: "Mkdir double-dot name",
				op:   func() int { return fs.Mkdir("/..dir", 0o755) },
				path: filepath.Join(base, "..dir"),
			},
			{
				name: "Rename interior-._ name",
				op:   func() int { return fs.Rename("/x._y", "/x._renamed") },
				path: filepath.Join(base, "x._renamed"),
			},
			{
				name: "Symlink dotfile",
				op:   func() int { return fs.Symlink(".foo", "/.foolink") },
				path: filepath.Join(base, ".foolink"),
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if rc := tc.op(); rc != 0 {
					t.Fatalf("rc = %d, want 0", rc)
				}
				if _, err := os.Lstat(tc.path); err != nil {
					t.Errorf("%s missing after the op: %v", tc.path, err)
				}
			})
		}
	})

	t.Run("unlink works", func(t *testing.T) {
		if rc := fs.Unlink("/.foo"); rc != 0 {
			t.Fatalf("Unlink(/.foo) = %d, want 0", rc)
		}
		if _, err := os.Lstat(filepath.Join(base, ".foo")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf(".foo still on the backing store after Unlink (lstat err = %v)", err)
		}
	})
}
