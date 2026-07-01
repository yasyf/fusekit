package fusekit

import (
	"runtime"
	"slices"
	"testing"
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

// TestMountOptionsNoattrcacheRule pins the fuse-t-over-NFS torn-read invariant:
// darwin forces noattrcache even with AttrCache:true; non-darwin honors it.
func TestMountOptionsNoattrcacheRule(t *testing.T) {
	withCache := MountOptions{AttrCache: true}.Build()
	hasIt := slices.Contains(withCache, "noattrcache")
	if runtime.GOOS == "darwin" {
		if !hasIt {
			t.Fatalf("darwin must force noattrcache even with AttrCache:true; got %v", withCache)
		}
	} else if hasIt {
		t.Fatalf("non-darwin with AttrCache:true must omit noattrcache; got %v", withCache)
	}

	if withoutCache := (MountOptions{AttrCache: false}).Build(); !slices.Contains(withoutCache, "noattrcache") {
		t.Fatalf("AttrCache:false must always emit noattrcache; got %v", withoutCache)
	}
}
