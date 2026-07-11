package fusekit

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestCarcassErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil (healthy stat)", err: nil, want: false},
		{name: "ENOENT is healthy — absent is not wedged", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.ENOENT}, want: false},
		{name: "ENOTCONN", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.ENOTCONN}, want: true},
		{name: "EIO", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.EIO}, want: true},
		{name: "EPERM — orphaned-server signature", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.EPERM}, want: true},
		{name: "EACCES — orphaned-server signature", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.EACCES}, want: true},
		{name: "EBADF is not a carcass", err: &fs.PathError{Op: "stat", Path: "/x", Err: unix.EBADF}, want: false},
		{name: "plain error is not a carcass", err: errors.New("boom"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := carcassErr(tc.err); got != tc.want {
				t.Errorf("carcassErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestStatAnswers(t *testing.T) {
	dir := t.TempDir()
	if !statAnswers(dir) {
		t.Error("statAnswers(existing dir) = false, want true")
	}
	if !statAnswers(filepath.Join(dir, "absent")) {
		t.Error("statAnswers(ENOENT) = false, want true — absent is healthy, not a carcass")
	}
}

func TestStatAnswersPermissionDenied(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses permission checks")
	}
	dir := t.TempDir()
	locked := filepath.Join(dir, "locked")
	if err := os.MkdirAll(filepath.Join(locked, "mnt"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	if statAnswers(filepath.Join(locked, "mnt")) {
		t.Error("statAnswers(EACCES path) = true, want false — a permission refusal is the orphaned-server carcass signature")
	}
}

// swapCarcassSeams fakes the probe errno, the mount-table verdict, and the
// force syscall for one test; callers must not run in parallel.
func swapCarcassSeams(t *testing.T, stat func(string) error, mounted func(string) bool, force func(string)) *[]string {
	t.Helper()
	prevStat, prevMounted, prevForce := carcassStat, carcassMounted, forceReapFn
	forced := &[]string{}
	carcassStat = stat
	carcassMounted = mounted
	forceReapFn = func(dir string) {
		*forced = append(*forced, dir)
		if force != nil {
			force(dir)
		}
	}
	t.Cleanup(func() { carcassStat, carcassMounted, forceReapFn = prevStat, prevMounted, prevForce })
	return forced
}

func swapCarcassProbeDeadline(t *testing.T, d time.Duration) {
	t.Helper()
	prev := carcassProbeDeadline
	carcassProbeDeadline = d
	t.Cleanup(func() { carcassProbeDeadline = prev })
}

func deadErr(p string) error {
	return &fs.PathError{Op: "stat", Path: p, Err: unix.ENOTCONN}
}

// TestClearCarcassNeverForcesOnHang is the pinned regression for the shipped
// hang-as-carcass hole: a stat that does not answer within the probe deadline
// is NEVER proof of death — ClearCarcass defers with ErrCarcassUndetermined
// and the force path is untouched.
func TestClearCarcassNeverForcesOnHang(t *testing.T) {
	swapCarcassProbeDeadline(t, 30*time.Millisecond)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	forced := swapCarcassSeams(t,
		func(string) error { <-release; return nil },
		func(string) bool { return true },
		nil)

	err := ClearCarcass("/carcass/hung")
	if !errors.Is(err, ErrCarcassUndetermined) {
		t.Fatalf("ClearCarcass(hanging stat) = %v, want ErrCarcassUndetermined", err)
	}
	if len(*forced) != 0 {
		t.Fatalf("a hanging stat forced %v — force-on-hang is the kernel-panic hole", *forced)
	}
}

func TestClearCarcassProofV2(t *testing.T) {
	cases := []struct {
		name      string
		stat      func(calls *int, p string) error
		mounted   bool
		wantErr   error // nil means success
		wantForce int
	}{
		{
			name:    "healthy path is a no-op",
			stat:    func(*int, string) error { return nil },
			mounted: true,
		},
		{
			name:    "ENOENT is healthy — absent is not wedged",
			stat:    func(_ *int, p string) error { return &fs.PathError{Op: "stat", Path: p, Err: unix.ENOENT} },
			mounted: false,
		},
		{
			name: "dead errno off a non-mountpoint is local state, never forced",
			stat: func(_ *int, p string) error { return deadErr(p) },
			// errno provenance: not in the kernel mount table => nothing to unmount.
			mounted: false,
		},
		{
			name: "proven-dead mountpoint is forced and clears",
			stat: func(calls *int, p string) error {
				// Dead on the probe and the pre-force revalidation; healthy
				// (ENOENT) once the force unmounted it.
				if *calls <= 2 {
					return deadErr(p)
				}
				return &fs.PathError{Op: "stat", Path: p, Err: unix.ENOENT}
			},
			mounted:   true,
			wantForce: 1,
		},
		{
			name: "death not revalidated immediately before the force is never forced",
			stat: func(calls *int, p string) error {
				if *calls == 1 {
					return deadErr(p) // probe read dead …
				}
				return nil // … but the revalidation reads healthy (resurrection window)
			},
			mounted: true,
			wantErr: ErrCarcassUndetermined,
		},
		{
			name:      "a carcass the force cannot clear surfaces wedged",
			stat:      func(_ *int, p string) error { return deadErr(p) },
			mounted:   true,
			wantErr:   ErrUnmountWedged,
			wantForce: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			cleared := false
			forced := swapCarcassSeams(t,
				func(p string) error { calls++; return tc.stat(&calls, p) },
				func(string) bool { return tc.mounted && !cleared },
				func(string) {
					if tc.wantErr == nil {
						cleared = true
					}
				})
			err := ClearCarcass("/carcass/dir")
			if tc.wantErr == nil && err != nil {
				t.Fatalf("ClearCarcass = %v, want nil", err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("ClearCarcass = %v, want %v", err, tc.wantErr)
			}
			if len(*forced) != tc.wantForce {
				t.Fatalf("force calls = %v, want %d", *forced, tc.wantForce)
			}
		})
	}
}
