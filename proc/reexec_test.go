package proc

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"syscall"
	"testing"
	"time"
)

type execRecorder struct {
	called bool
	argv0  string
	argv   []string
	env    []string
	err    error
}

func (r *execRecorder) install(t *testing.T) {
	t.Helper()
	prev := execve
	execve = func(argv0 string, argv, env []string) error {
		r.called = true
		r.argv0, r.argv, r.env = argv0, argv, env
		return r.err
	}
	t.Cleanup(func() { execve = prev })
}

func inode(t *testing.T, path string) uint64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return fi.Sys().(*syscall.Stat_t).Ino
}

func TestReexecStable(t *testing.T) {
	t.Run("already at stable path", func(t *testing.T) {
		rec := &execRecorder{}
		rec.install(t)
		dir := t.TempDir()
		target := writeExe(t, dir, "holder", "self", time.Time{})
		beforeIno := inode(t, target)

		if err := reexecStable(target, dir, "holder"); err != nil {
			t.Fatalf("reexecStable = %v, want nil", err)
		}
		if rec.called {
			t.Error("execve called despite the running exe already at the stable path")
		}
		if got := inode(t, target); got != beforeIno {
			t.Errorf("inode = %d, want unchanged %d (no rewrite)", got, beforeIno)
		}
	})

	t.Run("relocates and execs", func(t *testing.T) {
		rec := &execRecorder{}
		rec.install(t)
		srcDir, dstDir := t.TempDir(), t.TempDir()
		src := writeExe(t, srcDir, "src", "current-bytes", time.Time{})

		if err := reexecStable(src, dstDir, "holder"); err != nil {
			t.Fatalf("reexecStable = %v, want nil", err)
		}
		target := filepath.Join(dstDir, "holder")
		fi, err := os.Stat(target)
		if err != nil {
			t.Fatalf("stat target: %v", err)
		}
		if got := fi.Mode().Perm(); got != 0o755 {
			t.Errorf("target perms = %o, want 0755", got)
		}
		got, err := os.ReadFile(target)
		if err != nil {
			t.Fatalf("read target: %v", err)
		}
		if string(got) != "current-bytes" {
			t.Errorf("target content = %q, want %q", got, "current-bytes")
		}
		if !rec.called {
			t.Fatal("execve not called after relocate")
		}
		if rec.argv0 != target {
			t.Errorf("execve argv0 = %q, want %q", rec.argv0, target)
		}
		if !reflect.DeepEqual(rec.argv, os.Args) {
			t.Errorf("execve argv = %q, want os.Args %q", rec.argv, os.Args)
		}
		if !reflect.DeepEqual(rec.env, os.Environ()) {
			t.Errorf("execve env = %q, want os.Environ() %q", rec.env, os.Environ())
		}
	})

	t.Run("refreshes stale copy", func(t *testing.T) {
		rec := &execRecorder{}
		rec.install(t)
		srcDir, dstDir := t.TempDir(), t.TempDir()
		src := writeExe(t, srcDir, "src", "v2-current", time.Time{})
		writeExe(t, dstDir, "holder", "v1-old", time.Time{})

		if err := reexecStable(src, dstDir, "holder"); err != nil {
			t.Fatalf("reexecStable = %v, want nil", err)
		}
		got, err := os.ReadFile(filepath.Join(dstDir, "holder"))
		if err != nil {
			t.Fatalf("read target: %v", err)
		}
		if string(got) != "v2-current" {
			t.Errorf("target content = %q, want refreshed %q", got, "v2-current")
		}
		if !rec.called {
			t.Error("execve not called after refreshing a stale copy")
		}
	})

	t.Run("identical copy not rewritten", func(t *testing.T) {
		rec := &execRecorder{}
		rec.install(t)
		srcDir, dstDir := t.TempDir(), t.TempDir()
		src := writeExe(t, srcDir, "src", "same-bytes", time.Time{})
		target := writeExe(t, dstDir, "holder", "same-bytes", time.Time{})
		beforeIno := inode(t, target)

		if err := reexecStable(src, dstDir, "holder"); err != nil {
			t.Fatalf("reexecStable = %v, want nil", err)
		}
		if got := inode(t, target); got != beforeIno {
			t.Errorf("inode = %d, want unchanged %d (byte-identical, no rewrite)", got, beforeIno)
		}
		if !rec.called {
			t.Error("execve not called for a byte-identical stable copy")
		}
	})

	t.Run("symlinked dir detected as stable", func(t *testing.T) {
		rec := &execRecorder{}
		rec.install(t)
		realDir := t.TempDir()
		resolved := writeExe(t, realDir, "holder", "self", time.Time{})
		linkDir := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(realDir, linkDir); err != nil {
			t.Fatalf("symlink: %v", err)
		}

		if err := reexecStable(resolved, linkDir, "holder"); err != nil {
			t.Fatalf("reexecStable = %v, want nil", err)
		}
		if rec.called {
			t.Error("execve called despite target being the same file via a symlinked dir (loop hazard)")
		}
	})

	t.Run("materialize failure surfaces", func(t *testing.T) {
		rec := &execRecorder{}
		rec.install(t)
		srcDir := t.TempDir()
		src := writeExe(t, srcDir, "src", "current", time.Time{})
		dstDir := filepath.Join(t.TempDir(), "ro")
		if err := os.Mkdir(dstDir, 0o500); err != nil {
			t.Fatalf("mkdir ro: %v", err)
		}
		t.Cleanup(func() { os.Chmod(dstDir, 0o700) })

		err := reexecStable(src, dstDir, "holder")
		if err == nil {
			t.Fatal("reexecStable into an unwritable dir succeeded, want error")
		}
		if rec.called {
			t.Error("execve called despite a materialize failure")
		}
	})

	t.Run("exec failure surfaces", func(t *testing.T) {
		sentinel := errors.New("execve refused")
		rec := &execRecorder{err: sentinel}
		rec.install(t)
		srcDir, dstDir := t.TempDir(), t.TempDir()
		src := writeExe(t, srcDir, "src", "current", time.Time{})

		err := reexecStable(src, dstDir, "holder")
		if !errors.Is(err, sentinel) {
			t.Fatalf("error = %v, want the execve error wrapped", err)
		}
		if !rec.called {
			t.Error("execve not called")
		}
	})
}

// ReexecStable resolves os.Executable and, pointed at its own dir, is a no-op.
func TestReexecStableResolvesSelf(t *testing.T) {
	rec := &execRecorder{}
	rec.install(t)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		t.Fatal(err)
	}
	if err := ReexecStable(filepath.Dir(resolved), filepath.Base(resolved)); err != nil {
		t.Fatalf("ReexecStable pointed at its own dir = %v, want nil", err)
	}
	if rec.called {
		t.Error("execve called despite the running exe already at the target path")
	}
}
