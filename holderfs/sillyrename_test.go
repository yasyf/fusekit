package holderfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSillyRenamed(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"fuse_hidden placeholder", ".fuse_hidden0000000000001234", true},
		{"fuse_hidden bare prefix", ".fuse_hidden", true},
		{"nfs placeholder", ".nfs.0123456789abcdef", true},
		{"nfs bare prefix", ".nfs.", true},
		{"plain private dotfile", ".claude.json", false},
		{"nfs without trailing dot", ".nfsx", false},
		{"nfs missing dot before id", ".nfs0001", false},
		{"fuse_hidden truncated", ".fuse_hidde", false},
		{"interior match only", "x.fuse_hiddenY", false},
		{"regular file", "settings.json", false},
		{"empty", "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := sillyRenamed(tc.in); got != tc.want {
				t.Errorf("sillyRenamed(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestSweepSillyLitter(t *testing.T) {
	t.Run("removes silly litter, keeps everything else", func(t *testing.T) {
		priv := t.TempDir()
		keep := []string{".claude.json", "daemon", "settings.json", ".nfsx", ".fuse_hidde"}
		litter := []string{
			".fuse_hidden0000000000000001",
			".fuse_hidden0000000000000002",
			".nfs.abc123",
			".nfs.",
		}
		for _, n := range append(append([]string{}, keep...), litter...) {
			if err := os.WriteFile(filepath.Join(priv, n), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		sweepSillyLitter(priv)
		for _, n := range keep {
			if _, err := os.Stat(filepath.Join(priv, n)); err != nil {
				t.Errorf("sweep removed non-litter %q: %v", n, err)
			}
		}
		for _, n := range litter {
			if _, err := os.Stat(filepath.Join(priv, n)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("sweep left litter %q (stat err = %v)", n, err)
			}
		}
	})
	t.Run("missing privateRoot is a no-op", func(t *testing.T) {
		sweepSillyLitter(filepath.Join(t.TempDir(), "absent")) // must not panic or error out
	})
	t.Run("empty privateRoot is a no-op", func(t *testing.T) {
		sweepSillyLitter("") // must not panic
	})
}
