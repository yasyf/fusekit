package fusekit

import (
	"slices"
	"testing"
	"time"
)

func TestMountOptionsBuild(t *testing.T) {
	tests := []struct {
		name string
		opts MountOptions
		want []string
	}{
		{
			// Byte-identical cc-pool v0.28.1 darwin option string; AttrCache is
			// zero, so the slice is platform-independent.
			name: "cc-pool darwin option string",
			opts: MountOptions{Volname: "cc-pool-x", NoBrowse: true, NamedAttr: true, Extra: []string{"rwsize=1048576"}},
			want: []string{
				"-o", "volname=cc-pool-x",
				"-o", "noattrcache",
				"-o", "nobrowse",
				"-o", "namedattr",
				"-o", "rwsize=1048576",
			},
		},
		{
			name: "cc-notes options: volname+noattrcache+nobrowse only",
			opts: MountOptions{Volname: "cc-notes-x", NoBrowse: true},
			want: []string{
				"-o", "volname=cc-notes-x",
				"-o", "noattrcache",
				"-o", "nobrowse",
			},
		},
		{
			name: "zero options still emit noattrcache",
			opts: MountOptions{},
			want: []string{"-o", "noattrcache"},
		},
		{
			name: "multiple Extra emitted in order after structured flags",
			opts: MountOptions{Volname: "v", Extra: []string{"a=1", "b=2"}},
			want: []string{"-o", "volname=v", "-o", "noattrcache", "-o", "a=1", "-o", "b=2"},
		},
		{
			// AttrCache opt-in with no timeout: no noattrcache, and no
			// attrcache-timeout (fuse-t keeps go-nfsv4's default TTL).
			name: "AttrCache on without a timeout omits both noattrcache and attrcache-timeout",
			opts: MountOptions{Volname: "v", NoBrowse: true, AttrCache: true},
			want: []string{"-o", "volname=v", "-o", "nobrowse"},
		},
		{
			name: "AttrCache on with a timeout emits attrcache-timeout in whole seconds",
			opts: MountOptions{Volname: "v", AttrCache: true, AttrCacheTimeout: 30 * time.Second},
			want: []string{"-o", "volname=v", "-o", "attrcache-timeout=30"},
		},
		{
			// go-nfsv4's --attrcache-timeout is integer seconds: a sub-second TTL
			// is not representable, so nothing is emitted rather than a bogus 0.
			name: "AttrCache on with a sub-second timeout emits no attrcache-timeout",
			opts: MountOptions{Volname: "v", AttrCache: true, AttrCacheTimeout: 500 * time.Millisecond},
			want: []string{"-o", "volname=v"},
		},
		{
			// A timeout is meaningless while the cache is off (noattrcache): it is
			// never emitted, and noattrcache still is.
			name: "AttrCache off ignores AttrCacheTimeout and still emits noattrcache",
			opts: MountOptions{Volname: "v", AttrCacheTimeout: 30 * time.Second},
			want: []string{"-o", "volname=v", "-o", "noattrcache"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := tc.opts.Build()
			if !slices.Equal(got, tc.want) {
				t.Errorf("Build() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestMountOptionsNoattrcacheRule pins the noattrcache opt-in contract on every
// GOOS: the zero value (default, cache off) always emits noattrcache, and
// AttrCache:true always omits it. The rule is platform-independent — the old
// darwin force is gone now that holderfs serves stabilized attrs.
func TestMountOptionsNoattrcacheRule(t *testing.T) {
	if def := (MountOptions{}).Build(); !slices.Contains(def, "noattrcache") {
		t.Fatalf("the zero value must emit noattrcache on every GOOS; got %v", def)
	}
	if off := (MountOptions{AttrCache: false}).Build(); !slices.Contains(off, "noattrcache") {
		t.Fatalf("AttrCache:false must emit noattrcache; got %v", off)
	}
	if on := (MountOptions{AttrCache: true}).Build(); slices.Contains(on, "noattrcache") {
		t.Fatalf("AttrCache:true must omit noattrcache; got %v", on)
	}
}
