//go:build fuse && cgo && darwin

package holderfs

import (
	"testing"

	"github.com/winfsp/cgofuse/fuse"
)

func newRoutingFS() *holderFS {
	return &holderFS{
		base:            "/base",
		privateRoot:     "/priv",
		privateExact:    map[string]bool{"daemon": true, "ide": true},
		privatePrefixes: []string{".claude.json", ".credentials.json"},
		shared: map[string]sharedEntry{
			"projects": {target: "/base/projects", stat: fuse.Stat_t{Mode: fuse.S_IFLNK | 0o777, Size: 14}},
		},
		synth:       map[string]*synthView{"/.claude.json": {writePath: "/priv/.claude.json"}},
		synthFhs:    map[uint64]*synthHandle{},
		nextSynthFh: synthFhBase,
	}
}

func TestRealRouting(t *testing.T) {
	fs := newRoutingFS()
	cases := []struct{ path, want string }{
		{"/projects/p.json", "/base/projects/p.json"},     // shared dir: nested passthrough to base
		{"/daemon/x", "/priv/daemon/x"},                   // exact private redirect
		{"/.claude.json.tmp7", "/priv/.claude.json.tmp7"}, // atomic-write temp: prefix → private
		{"/.credentials.json", "/priv/.credentials.json"}, // prefix → private
		{"/.claude.json", "/priv/.claude.json"},           // synth → its durable writePath
		{"/settings.json", "/base/settings.json"},         // unknown top-level → base passthrough
	}
	for _, c := range cases {
		if got := fs.real(c.path); got != c.want {
			t.Errorf("real(%q) = %q, want %q", c.path, got, c.want)
		}
	}
}

func TestIsPrivate(t *testing.T) {
	fs := newRoutingFS()
	for _, name := range []string{"daemon", "ide", ".claude.json", ".claude.json.tmpABC", "._daemon", "._.claude.json"} {
		if !fs.isPrivate(name) {
			t.Errorf("isPrivate(%q) = false, want true", name)
		}
	}
	for _, name := range []string{"projects", "settings.json", "history"} {
		if fs.isPrivate(name) {
			t.Errorf("isPrivate(%q) = true, want false", name)
		}
	}
}

func TestSharedSymlinkRouting(t *testing.T) {
	fs := newRoutingFS()
	if target, ok := fs.sharedLink("/projects"); !ok || target != "/base/projects" {
		t.Fatalf("sharedLink(/projects) = %q, %v; want /base/projects", target, ok)
	}
	if _, ok := fs.sharedLink("/projects/nested"); ok {
		t.Error("sharedLink(/projects/nested) = ok; only top-level entries are live symlinks")
	}
	rc, target := fs.Readlink("/projects")
	if rc != 0 || target != "/base/projects" {
		t.Fatalf("Readlink(/projects) = (%d, %q), want (0, /base/projects)", rc, target)
	}
	var stat fuse.Stat_t
	if rc := fs.Getattr("/projects", &stat, ^uint64(0)); rc != 0 || stat.Mode&fuse.S_IFLNK == 0 {
		t.Fatalf("Getattr(/projects) = rc %d, mode %#o; want a symlink", rc, stat.Mode)
	}
}
