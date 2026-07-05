//go:build darwin

package proc

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestRedirectDevNull(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()

	fd := int(w.Fd())
	if err := redirectDevNull(fd); err != nil {
		t.Fatalf("redirectDevNull: %v", err)
	}

	// The fd now refers to /dev/null, not the pipe.
	var st, nullSt unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		t.Fatalf("fstat redirected fd: %v", err)
	}
	if err := unix.Stat(os.DevNull, &nullSt); err != nil {
		t.Fatal(err)
	}
	if st.Rdev != nullSt.Rdev || st.Mode&unix.S_IFMT != unix.S_IFCHR {
		t.Errorf("redirected fd rdev/mode = %#x/%#o, want %s's %#x/S_IFCHR", st.Rdev, st.Mode&unix.S_IFMT, os.DevNull, nullSt.Rdev)
	}

	// Writes to the redirected fd land in the sink, never the old pipe: the
	// reader must see immediate EOF (dup2 closed the pipe's only write end).
	if _, err := unix.Write(fd, []byte("swallowed")); err != nil {
		t.Errorf("write to redirected fd: %v", err)
	}
	buf := make([]byte, 16)
	n, err := r.Read(buf)
	if n != 0 || !errors.Is(err, io.EOF) {
		t.Errorf("pipe read after redirect = (%d, %v), want (0, EOF) — the original fd survived the redirect", n, err)
	}
}

func TestRedirectDevNullBadFd(t *testing.T) {
	if err := redirectDevNull(-1); err == nil {
		t.Fatal("redirectDevNull(-1) = nil, want error")
	}
}

// TestLogOpenIsCloexec pins the holder's --log guarantee: a Go os.OpenFile fd
// is O_CLOEXEC, so spawned go-nfsv4 servers can never inherit the log file.
func TestLogOpenIsCloexec(t *testing.T) {
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "holder.log"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if err != nil {
		t.Fatal(err)
	}
	if flags&unix.FD_CLOEXEC == 0 {
		t.Error("os.OpenFile log fd lacks FD_CLOEXEC — it would leak into spawned servers")
	}
}
