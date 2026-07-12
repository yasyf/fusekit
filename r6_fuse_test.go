//go:build fuse && cgo

package fusekit

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// TestProbeConfigCarriesReArmSignals pins R6-1's structural guard: the
// throwaway host-probe mount is a fusekit.Mount inside the holder process
// like any other, so its Config force-stamps ReArmSignals.
func TestProbeConfigCarriesReArmSignals(t *testing.T) {
	var armed bool
	cfg := probeConfig("/src", "/mnt", func() { armed = true })
	if cfg.ReArmSignals == nil {
		t.Fatal("probe Config.ReArmSignals = nil — an OpProbe mount would defuse signals without re-arming the holder")
	}
	cfg.ReArmSignals()
	if !armed {
		t.Fatal("probe Config.ReArmSignals is not the caller's hook")
	}
}

// TestConfirmMounted pins the confirm op's plumbing: the default target is
// the mountpoint root, ProbePath overrides it, and a failed stat surfaces —
// the caller's cue to skip the signal defuse.
func TestConfirmMounted(t *testing.T) {
	dir := t.TempDir()
	if err := confirmMounted(Config{Dir: dir}); err != nil {
		t.Fatalf("confirm of a stat-able root = %v, want nil", err)
	}
	if err := confirmMounted(Config{Dir: dir, ProbePath: filepath.Join(dir, "missing")}); err == nil {
		t.Fatal("confirm of a missing ProbePath = nil, want an error")
	}
}

// TestUnmountVerdictWrapsCallErrno pins R6-6: the unmount CALL's errno rides
// the wedge verdict with %w, so errors.Is sees it through both the
// mounted-check-failed and still-mounted verdicts; a nil call error renders
// as "(nil)" without faking an errno.
func TestUnmountVerdictWrapsCallErrno(t *testing.T) {
	swap := func(t *testing.T, u func(string, int) error, m func(string) (bool, error)) {
		t.Helper()
		pu, pm := unmountFn, mountedCheckFn
		unmountFn, mountedCheckFn = u, m
		t.Cleanup(func() { unmountFn, mountedCheckFn = pu, pm })
	}
	cases := []struct {
		name    string
		callErr error
		mounted bool
		merr    error
		wantIs  []error
		wantSub string
	}{
		{
			name:    "mounted check failed carries the call errno",
			callErr: unix.EPERM,
			merr:    fmt.Errorf("getfsstat: %w", unix.EIO),
			wantIs:  []error{ErrUnmountWedged, unix.EPERM},
		},
		{
			name:    "still mounted carries the call errno",
			callErr: unix.EPERM,
			mounted: true,
			wantIs:  []error{ErrUnmountWedged, unix.EPERM},
		},
		{
			name:    "nil call error still mounted reads (nil)",
			mounted: true,
			wantIs:  []error{ErrUnmountWedged},
			wantSub: "(nil)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swap(t,
				func(string, int) error { return tc.callErr },
				func(string) (bool, error) { return tc.mounted, tc.merr },
			)
			h := &Handle{dir: "/r6/never-mounted"}
			err := h.Unmount()
			if err == nil {
				t.Fatal("Unmount = nil, want a wedge verdict")
			}
			for _, want := range tc.wantIs {
				if !errors.Is(err, want) {
					t.Errorf("errors.Is(%v, %v) = false, want true", err, want)
				}
			}
			if errors.Is(err, ErrMountBusy) {
				t.Errorf("verdict %v wraps ErrMountBusy, want only the call errno", err)
			}
			if tc.wantSub != "" && !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q does not contain %q", err, tc.wantSub)
			}
		})
	}
}
