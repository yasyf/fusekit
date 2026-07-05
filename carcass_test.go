package fusekit

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

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
