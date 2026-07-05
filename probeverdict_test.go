package fusekit

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestProbeOpenVerdict(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error // sentinel the verdict must wrap; nil means no verdict
	}{
		{name: "nil error carries no verdict", err: nil, want: nil},
		{name: "ENOENT is probe-missing (old holder)", err: &fs.PathError{Op: "open", Path: "/m/.probe", Err: unix.ENOENT}, want: ErrProbeMissing},
		{name: "EPERM is probe-denied (orphaned server)", err: &fs.PathError{Op: "open", Path: "/m/.probe", Err: unix.EPERM}, want: ErrProbeDenied},
		{name: "EACCES is probe-denied (orphaned server)", err: &fs.PathError{Op: "open", Path: "/m/.probe", Err: unix.EACCES}, want: ErrProbeDenied},
		{name: "EIO carries no verdict here", err: &fs.PathError{Op: "open", Path: "/m/.probe", Err: unix.EIO}, want: nil},
		{name: "plain error carries no verdict", err: errors.New("boom"), want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ProbeOpenVerdict(tc.err)
			if tc.want == nil {
				if got != nil {
					t.Fatalf("ProbeOpenVerdict(%v) = %v, want nil", tc.err, got)
				}
				return
			}
			if !errors.Is(got, tc.want) {
				t.Fatalf("ProbeOpenVerdict(%v) = %v, want errors.Is %v", tc.err, got, tc.want)
			}
			// The two sentinels must never alias: a denied probe folded into
			// missing is exactly the incident's misread.
			other := ErrProbeMissing
			if tc.want == ErrProbeMissing {
				other = ErrProbeDenied
			}
			if errors.Is(got, other) {
				t.Errorf("ProbeOpenVerdict(%v) also matches %v — the verdicts must stay distinct", tc.err, other)
			}
		})
	}
}

func TestProbeOpenVerdictRealFS(t *testing.T) {
	dir := t.TempDir()
	_, err := os.Open(filepath.Join(dir, "absent-probe"))
	if v := ProbeOpenVerdict(err); !errors.Is(v, ErrProbeMissing) {
		t.Errorf("open(absent) verdict = %v, want ErrProbeMissing", v)
	}

	if os.Geteuid() == 0 {
		t.Skip("root bypasses permission checks")
	}
	locked := filepath.Join(dir, "locked")
	if err := os.MkdirAll(locked, 0o700); err != nil {
		t.Fatal(err)
	}
	probe := filepath.Join(locked, "probe")
	if err := os.WriteFile(probe, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(locked, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o700) })
	_, err = os.Open(probe)
	if v := ProbeOpenVerdict(err); !errors.Is(v, ErrProbeDenied) {
		t.Errorf("open(EACCES) verdict = %v, want ErrProbeDenied", v)
	}
}
