package overlay

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	cases := map[string]struct {
		want    Backend
		wantErr bool
	}{
		"symlink": {BackendSymlink, false},
		"nfs":     {BackendNFS, false},
		"fskit":   {BackendFSKit, false},
		"fuse":    {"", true}, // legacy value, not a backend
		"":        {"", true},
		"bogus":   {"", true},
		"Symlink": {"", true}, // case-sensitive
	}
	for s, tc := range cases {
		got, err := Parse(s)
		if tc.wantErr {
			if err == nil {
				t.Errorf("Parse(%q) = %q, nil; want ErrUnknownBackend", s, got)
				continue
			}
			if !errors.Is(err, ErrUnknownBackend) {
				t.Errorf("Parse(%q) error = %v; want errors.Is ErrUnknownBackend", s, err)
			}
			if !strings.Contains(err.Error(), s) && s != "" {
				t.Errorf("Parse(%q) error = %v; want it to name the bad value", s, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", s, err)
		}
		if got != tc.want {
			t.Errorf("Parse(%q) = %q, want %q", s, got, tc.want)
		}
	}
}

func TestIsFuse(t *testing.T) {
	cases := map[Backend]bool{
		BackendSymlink: false,
		BackendNFS:     true,
		BackendFSKit:   true,
	}
	for b, want := range cases {
		if got := b.IsFuse(); got != want {
			t.Errorf("%q.IsFuse() = %v, want %v", b, got, want)
		}
	}
}

// TestSymlinkAvailable pins symlink's always-available answer; nfs/fskit are
// environmental (fuse-t install + macOS version), not asserted.
func TestSymlinkAvailable(t *testing.T) {
	if !BackendSymlink.Available() {
		t.Error("BackendSymlink.Available() = false, want always true")
	}
}

func TestEnablement(t *testing.T) {
	if en := BackendSymlink.Enablement(); en.Needed {
		t.Errorf("symlink Enablement().Needed = true, want false")
	}
	nfs := BackendNFS.Enablement()
	if !nfs.Needed || nfs.Pane == "" || nfs.Guidance == "" || len(nfs.URLs) == 0 {
		t.Errorf("nfs Enablement() incomplete: %+v", nfs)
	}
	if !strings.Contains(nfs.Pane, "Network Volumes") {
		t.Errorf("nfs Enablement().Pane = %q, want it to name Network Volumes", nfs.Pane)
	}
	fskit := BackendFSKit.Enablement()
	if !fskit.Needed || fskit.Pane == "" || fskit.Guidance == "" || len(fskit.URLs) == 0 {
		t.Errorf("fskit Enablement() incomplete: %+v", fskit)
	}
	if !strings.Contains(fskit.Pane, "FSKit") {
		t.Errorf("fskit Enablement().Pane = %q, want it to name FSKit", fskit.Pane)
	}
}

// TestOpenSettingsTriesURLsInOrderUntilSuccess pins OpenSettings walking the
// backend's Enablement URLs in order, stopping on the first success, via the
// openRunner seam.
func TestOpenSettingsTriesURLsInOrderUntilSuccess(t *testing.T) {
	prev := openRunner
	defer func() { openRunner = prev }()

	var tried []string
	openRunner = func(_ context.Context, url string) error {
		tried = append(tried, url)
		if len(tried) < 2 {
			return errors.New("first anchor missing on this macOS")
		}
		return nil
	}
	if err := BackendNFS.OpenSettings(context.Background()); err != nil {
		t.Fatalf("OpenSettings: %v", err)
	}
	want := BackendNFS.Enablement().URLs[:2]
	if len(tried) != 2 || tried[0] != want[0] || tried[1] != want[1] {
		t.Errorf("tried URLs = %v, want the first two of %v in order", tried, BackendNFS.Enablement().URLs)
	}
}

// TestOpenSettingsAllFail pins that when every URL fails, OpenSettings wraps the
// last failure with %w.
func TestOpenSettingsAllFail(t *testing.T) {
	prev := openRunner
	defer func() { openRunner = prev }()

	boom := errors.New("open boom")
	openRunner = func(context.Context, string) error { return boom }
	err := BackendNFS.OpenSettings(context.Background())
	if err == nil || !errors.Is(err, boom) {
		t.Fatalf("OpenSettings all-fail error = %v, want it to wrap the last failure", err)
	}
}

// TestOpenSettingsSymlinkErrors pins that a backend with no grant (symlink) has
// nothing to open and errors.
func TestOpenSettingsSymlinkErrors(t *testing.T) {
	if err := BackendSymlink.OpenSettings(context.Background()); err == nil {
		t.Error("BackendSymlink.OpenSettings() = nil, want an error (no grant to open)")
	}
}

// TestFuseBackendDefaultsToNFS pins the safe default: a non-passthrough spec
// always lands on NFS regardless of FSKit availability.
func TestFuseBackendDefaultsToNFS(t *testing.T) {
	if got := FuseBackend(Spec{PassthroughOnly: false}); got != BackendNFS {
		t.Errorf("FuseBackend(non-passthrough) = %q, want %q", got, BackendNFS)
	}
	// Passthrough lands on fskit only when fuse-t's FSKit backend is available,
	// else NFS — both valid, so assert only a fuse backend, never symlink.
	if got := FuseBackend(Spec{PassthroughOnly: true}); !got.IsFuse() {
		t.Errorf("FuseBackend(passthrough) = %q, want a fuse backend", got)
	}
}
