package holderfs

import "testing"

func TestIsAppleDouble(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{"bare sidecar name", "._x", true},
		{"top-level sidecar path", "/._x", true},
		{"nested sidecar path", "/a/b/._x", true},
		{"sidecar of a dotfile", "/._.claude.json", true},
		{"bare prefix only", "/._", true},
		{"plain dotfile", "/.foo", false},
		{"double-dot name", "/..data", false},
		{"interior ._ in a name", "/x._y", false},
		// Deliberately final-component only: a "._" parent can never resolve,
		// so per-component lookup never descends to present such a path.
		{"sidecar component mid-path only", "/._dir/file", false},
		{"root", "/", false},
		{"empty", "", false},
		{"regular nested file", "/a/b/c.json", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isAppleDouble(tc.path); got != tc.want {
				t.Errorf("isAppleDouble(%q) = %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}
