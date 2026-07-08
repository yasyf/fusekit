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
// process and never returns. os.Args[1:] carries over verbatim while argv[0] is
// rewritten to the stable path, so a reader of argv[0] never sees a keg path
// Homebrew has since deleted. Concurrent starts of different versions are
// last-writer-wins on the stable copy; callers converge via their normal
// version eviction (no locking here by design). Self-exec counterpart to
// Spawn.StableExecDir.
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
	// The guard compares real paths, not inodes: resolved is symlink-free, so
	// equality proves the running executable IS the regular file at dir/name.
	// A hardlink or symlink leaf at target must NOT be blessed — the process
	// would keep (or exec onto) an unstable TCC identity.
	realDir, err := filepath.EvalSymlinks(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// dir not created yet — the running executable cannot reside there.
	case err != nil:
		return fmt.Errorf("resolve stable dir %s: %w", dir, err)
	case filepath.Join(realDir, name) == resolved:
		return nil
	}

	target := filepath.Join(dir, name)
	// A symlink leaf would survive materialize's byte-identical skip and the
	// kernel would exec through it, keying TCC to its destination.
	if fi, err := os.Lstat(target); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		if err := os.Remove(target); err != nil {
			return fmt.Errorf("remove symlink at stable path %s: %w", target, err)
		}
	}
	if _, err := materializeStableExe(resolved, dir, name); err != nil {
		return fmt.Errorf("materialize stable executable: %w", err)
	}
	if err := execve(target, append([]string{target}, os.Args[1:]...), os.Environ()); err != nil {
		return fmt.Errorf("re-exec at %s: %w", target, err)
	}
	return nil
}
