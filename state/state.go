// Package state owns a consumer's private per-user state directory: a ~/.<App>
// root, leaf paths under it, idempotent creation, and an atomic write for the
// out-of-process status mirror a status command or menu-bar widget reads. It is
// app-agnostic — the consumer supplies its App name — so multiple consumers
// share one layout primitive. Stdlib-only; it never imports the root fusekit
// package or mountd.
package state

import (
	"fmt"
	"os"
	"path/filepath"
)

// Home returns the current user's home directory. Callers resolve it lazily, not
// at init, so a missing HOME surfaces at the call site, not at process start.
func Home() (string, error) {
	h, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return h, nil
}

func mustHome() string {
	h, err := Home()
	if err != nil {
		panic(err)
	}
	return h
}

// Dir is a consumer's private state directory, ~/.<App> (App "cc-pool" yields
// ~/.cc-pool). It holds no handles, so the zero value plus App is safe to copy.
type Dir struct {
	// App is the directory basename without the leading dot. Required.
	App string
}

// Root is the absolute path of the state directory (~/.<App>). It panics if the
// home directory cannot be resolved.
func (d Dir) Root() string {
	return filepath.Join(mustHome(), "."+d.App)
}

// Path joins leaf onto the state directory root (Root()/leaf).
func (d Dir) Path(leaf string) string {
	return filepath.Join(d.Root(), leaf)
}

// Ensure creates the state directory with 0700 perms if missing.
func (d Dir) Ensure() error {
	return os.MkdirAll(d.Root(), 0o700)
}

// AtomicWrite writes data to path via a temp file in path's own directory plus
// a rename, so a concurrent reader never sees a torn file. path's parent dir is
// created 0700 if missing, and the temp is chmod'd to perm before the rename.
func AtomicWrite(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("ensure dir %s: %w", dir, err)
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp.*")
	if err != nil {
		return fmt.Errorf("create temp in %s: %w", dir, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }() // no-op after a successful rename
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("rename to %s: %w", path, err)
	}
	return nil
}
