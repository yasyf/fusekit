package proc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// execve is the syscall.Exec seam; tests replace it so the real self-exec never runs.
var execve = syscall.Exec

// ReexecStable materializes the running executable at dir/name and re-execs it
// there so macOS TCC grants, keyed by resolved executable path, survive the
// per-version install path of a Homebrew keg. It returns nil when the running
// executable already resolves to dir/name (the post-exec pass), so callers
// invoke it unconditionally at startup; on success the re-exec replaces the
// process and never returns. Self-exec counterpart to Spawn.StableExecDir.
func ReexecStable(dir, name string) error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve executable: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		return fmt.Errorf("resolve executable symlinks: %w", err)
	}
	return reexecStable(resolved, dir, name)
}

func reexecStable(resolved, dir, name string) error {
	target := filepath.Join(dir, name)
	if resolved == target {
		return nil
	}
	// A symlinked spelling of dir makes target a different string but the same
	// inode as the running executable; without this SameFile guard the post-exec
	// pass would relocate-and-exec forever.
	ti, err := os.Stat(target)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return fmt.Errorf("stat stable executable %s: %w", target, err)
	default:
		si, err := os.Stat(resolved)
		if err != nil {
			return fmt.Errorf("stat resolved executable %s: %w", resolved, err)
		}
		if os.SameFile(si, ti) {
			return nil
		}
	}

	if _, err := materializeStableExe(resolved, dir, name); err != nil {
		return fmt.Errorf("materialize stable executable: %w", err)
	}
	if err := execve(target, os.Args, os.Environ()); err != nil {
		return fmt.Errorf("re-exec at %s: %w", target, err)
	}
	return nil
}
