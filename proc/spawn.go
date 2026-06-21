package proc

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
)

// DefaultSpawnTimeout bounds how long callers wait for a freshly spawned child's
// socket to come up.
const DefaultSpawnTimeout = 5 * time.Second

// Spawn ensures a detached child process is serving Socket, auto-spawning one
// (in its own session) and waiting for its socket to come up. The consumer
// supplies the child argv (Args), so one Spawn shape drives any consumer's
// `<binary> <subcommand> --socket <sock>` invocation. A running child is usable
// by ANY build — the work lives in the child process — so only the spawn path is
// gated (CanHost); a second spawn racing a starting child is harmless, since the
// child refuses to start if the socket is owned.
type Spawn struct {
	// Socket is the child's unix socket path.
	Socket string
	// LogPath receives a spawned child's stdout and stderr.
	LogPath string
	// Args is the spawned process's argv after the executable, e.g.
	// ["mount-holder", "--socket", socket]. The consumer owns the subcommand
	// name and flag spelling.
	Args []string
	// Timeout bounds waiting for a freshly spawned child's socket. Zero means
	// DefaultSpawnTimeout.
	Timeout time.Duration
	// StableExecDir, when non-empty, makes the child binary materialize as a copy
	// under this directory and spawn from there instead of os.Executable()
	// directly; this gives the child a stable resolved path so a macOS TCC grant
	// survives version upgrades (the embedded Developer-ID designated requirement
	// survives the copy). Empty preserves the os.Executable() default.
	StableExecDir string
	// Available reports whether a child is already serving Socket. Required; it
	// replaces a hard-coded socket dial so the caller owns the liveness probe
	// (e.g. mountd's NewClient(Socket).Available()).
	Available func() bool
	// CanHost gates the spawn: nil means this binary may spawn a child; a non-nil
	// error is a permanent refusal returned as-is, UNWRAPPED — never folded into
	// ErrHolderUnavailable, since a binary that can never host is a permanent
	// condition while an unreachable child is transient. Required.
	CanHost func() error
}

// EnsureRunning makes sure a child serves Socket, returning nil once one is
// reachable. If none is and CanHost refuses, that refusal is returned as-is
// (permanent); otherwise a detached child is spawned and waited up to the
// timeout.
//
// Failure classes: every could-not-start-or-reach leg (a spawn that fails to
// assemble/start, or whose socket never comes up) wraps ErrHolderUnavailable — a
// process-availability condition, never a domain verdict, so drivers retry. The
// CanHost refusal alone is unwrapped.
func (s Spawn) EnsureRunning() error {
	if s.Available() {
		return nil
	}
	if err := s.CanHost(); err != nil {
		return err
	}
	cmd, logFile, err := s.childCmd()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrHolderUnavailable, err)
	}
	// The child holds its own descriptor once started; this one is ours.
	defer logFile.Close()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%w: spawn mount holder: %w", ErrHolderUnavailable, err)
	}
	reap(cmd)

	timeout := s.timeout()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.Available() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%w: mount holder did not come up on %s within %s; check %s", ErrHolderUnavailable, s.Socket, timeout, s.LogPath)
}

// timeout resolves the spawn-wait bound, defaulting a zero Timeout to
// DefaultSpawnTimeout.
func (s Spawn) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return DefaultSpawnTimeout
}

// childExeName names the stable child copy after the consumer's subcommand
// (e.g. "n"), falling back to "holder" when Args is empty. filepath.Base guards
// against path separators in args[0].
func childExeName(args []string) string {
	if len(args) > 0 && args[0] != "" {
		return filepath.Base(args[0])
	}
	return "holder"
}

// stableExeMatches reports whether target already holds a byte-identical copy of
// the binary at srcPath. Size is the cheap first discriminator (a code change
// shifts a Go binary's size); on an equal size it falls through to a content
// hash, so an upgrade whose binary is coincidentally the same length — e.g. a
// patch that only bumps an equal-length version string — still refreshes the
// copy instead of leaving the child stale and version-skewed. mtime is
// deliberately NOT used: a release tarball preserves an archived build mtime
// that can predate an existing copy, which a mtime heuristic would misread as
// up-to-date. A missing target reports false (it must be materialized).
func stableExeMatches(srcPath, target string) (bool, error) {
	si, err := os.Stat(srcPath)
	if err != nil {
		return false, fmt.Errorf("stat holder source %s: %w", srcPath, err)
	}
	ti, err := os.Stat(target)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat stable holder %s: %w", target, err)
	}
	if si.Size() != ti.Size() {
		return false, nil
	}
	sh, err := fileSHA256(srcPath)
	if err != nil {
		return false, err
	}
	th, err := fileSHA256(target)
	if err != nil {
		return false, err
	}
	return sh == th, nil
}

// fileSHA256 returns the SHA-256 digest of the file at path.
func fileSHA256(path string) ([sha256.Size]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("hash %s: %w", path, err)
	}
	return [sha256.Size]byte(h.Sum(nil)), nil
}

// materializeStableExe copies the binary at srcPath into dir as a stable,
// executable file named name, atomically and only when stale, returning the
// target path. Atomic so a running old copy (which cannot be truncated:
// ETXTBSY) keeps its inode while the next spawn picks up the replacement.
func materializeStableExe(srcPath, dir, name string) (string, error) {
	target := filepath.Join(dir, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("create stable exec dir %s: %w", dir, err)
	}
	switch matched, err := stableExeMatches(srcPath, target); {
	case err != nil:
		return "", err
	case matched:
		return target, nil
	}

	in, err := os.Open(srcPath)
	if err != nil {
		return "", fmt.Errorf("open holder source %s: %w", srcPath, err)
	}
	defer in.Close()

	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create stable holder temp in %s: %w", dir, err)
	}
	// renamed is set only after a successful os.Rename so the cleanup does not
	// delete the freshly materialized target.
	renamed := false
	defer func() {
		if !renamed {
			os.Remove(tmp.Name())
		}
	}()
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		return "", fmt.Errorf("copy holder to %s: %w", tmp.Name(), err)
	}
	if err := tmp.Chmod(0o755); err != nil {
		tmp.Close()
		return "", fmt.Errorf("chmod stable holder %s: %w", tmp.Name(), err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close stable holder %s: %w", tmp.Name(), err)
	}
	if err := os.Rename(tmp.Name(), target); err != nil {
		return "", fmt.Errorf("rename stable holder into %s: %w", target, err)
	}
	renamed = true
	return target, nil
}

// childCmd builds the detached child command: this same binary run with Args in
// its own session, stdout and stderr appended to LogPath. When StableExecDir is
// set the binary is first materialized as a stable copy there.
func (s Spawn) childCmd() (*exec.Cmd, *os.File, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, nil, fmt.Errorf("resolve executable: %w", err)
	}
	if s.StableExecDir != "" {
		src, err := filepath.EvalSymlinks(exe)
		if err != nil {
			return nil, nil, fmt.Errorf("resolve executable symlinks: %w", err)
		}
		exe, err = materializeStableExe(src, s.StableExecDir, childExeName(s.Args))
		if err != nil {
			return nil, nil, fmt.Errorf("materialize stable holder: %w", err)
		}
	}
	logFile, err := os.OpenFile(s.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open mount holder log: %w", err)
	}
	cmd := exec.Command(exe, s.Args...)
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true} // detach from our session
	return cmd, logFile, nil
}

// reap waits out a started detached child in the background, so its exit never
// strands a zombie in the spawner's process table. Setsid detaches the session,
// not the parent-child link: a long-lived daemon spawns children from every
// supervise revival and skew replace, and Process.Release alone would leave one
// defunct entry per exited child (a flock-refusal loser, a crash-at-startup
// backoff attempt, every replaced child) until the spawner itself exits. The
// goroutine's exit is the child's.
func reap(cmd *exec.Cmd) {
	go func() { _ = cmd.Wait() }()
}
