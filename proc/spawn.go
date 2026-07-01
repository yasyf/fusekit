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

// DefaultSpawnTimeout bounds the wait for a freshly spawned child's socket.
const DefaultSpawnTimeout = 5 * time.Second

// Spawn ensures a detached child process is serving Socket, spawning one in
// its own session when needed. Racing spawns are harmless: the child refuses
// to start if the socket is already owned.
type Spawn struct {
	// Socket is the child's unix socket path.
	Socket string
	// LogPath receives a spawned child's stdout and stderr.
	LogPath string
	// Args is the child's argv after the executable.
	Args []string
	// Timeout bounds waiting for a freshly spawned child's socket. Zero means
	// DefaultSpawnTimeout.
	Timeout time.Duration
	// StableExecDir, when non-empty, spawns the child from a copy materialized
	// under this directory: a stable resolved path keeps a macOS TCC grant valid
	// across upgrades (the embedded Developer-ID designated requirement survives
	// the copy).
	StableExecDir string
	// ExecPath, when set, is the binary the child execs instead of os.Executable()/StableExecDir.
	ExecPath string
	// Available reports whether a child is already serving Socket. Required.
	Available func() bool
	// CanHost gates the spawn: a non-nil error is a permanent refusal, returned
	// unwrapped by EnsureRunning. Required.
	CanHost func() error
	// Override, when non-nil, replaces everything after the Available
	// short-circuit — the CanHost check included — and its error is returned
	// verbatim. ErrSkipSpawn signals a benign nothing-to-serve no-op.
	Override func() error
}

// EnsureRunning ensures a child serves Socket, spawning a detached one and
// waiting for its socket when needed. Every could-not-start-or-reach failure
// wraps ErrChildUnavailable (transient; drivers retry); a CanHost refusal is
// returned unwrapped (permanent).
func (s Spawn) EnsureRunning() error {
	if s.Available() {
		return nil
	}
	if s.Override != nil {
		return s.Override()
	}
	if err := s.CanHost(); err != nil {
		return err
	}
	cmd, logFile, err := s.childCmd()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrChildUnavailable, err)
	}
	// The child holds its own descriptor; this one is ours.
	defer logFile.Close()
	// Cap the child subtree's RLIMIT_NPROC across the fork so a runaway re-spawn
	// loop starves at EAGAIN instead of fork-bombing the host (darwin only).
	if err := withChildNprocCap(cmd.Start); err != nil {
		return fmt.Errorf("%w: spawn child: %w", ErrChildUnavailable, err)
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
	return fmt.Errorf("%w: child did not come up on %s within %s; check %s", ErrChildUnavailable, s.Socket, timeout, s.LogPath)
}

func (s Spawn) timeout() time.Duration {
	if s.Timeout > 0 {
		return s.Timeout
	}
	return DefaultSpawnTimeout
}

// filepath.Base guards against path separators in args[0].
func childExeName(args []string) string {
	if len(args) > 0 && args[0] != "" {
		return filepath.Base(args[0])
	}
	return "child"
}

// stableExeMatches reports whether target is a byte-identical copy of srcPath.
// Equal sizes still hash (equal-length version bumps); mtime is deliberately
// unused: release tarballs preserve archived mtimes that can predate an existing copy.
func stableExeMatches(srcPath, target string) (bool, error) {
	si, err := os.Stat(srcPath)
	if err != nil {
		return false, fmt.Errorf("stat child source %s: %w", srcPath, err)
	}
	ti, err := os.Stat(target)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat stable child %s: %w", target, err)
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

// materializeStableExe copies srcPath to dir/name when stale. Atomic rename:
// a running old copy cannot be truncated (ETXTBSY) and keeps its inode while
// the next spawn picks up the replacement.
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
		return "", fmt.Errorf("open child source %s: %w", srcPath, err)
	}
	defer in.Close()

	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("create stable child temp in %s: %w", dir, err)
	}
	renamed := false
	defer func() {
		if !renamed {
			os.Remove(tmp.Name())
		}
	}()
	if _, err := io.Copy(tmp, in); err != nil {
		tmp.Close()
		return "", fmt.Errorf("copy child to %s: %w", tmp.Name(), err)
	}
	if err := tmp.Chmod(0o755); err != nil {
		tmp.Close()
		return "", fmt.Errorf("chmod stable child %s: %w", tmp.Name(), err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close stable child %s: %w", tmp.Name(), err)
	}
	if err := os.Rename(tmp.Name(), target); err != nil {
		return "", fmt.Errorf("rename stable child into %s: %w", target, err)
	}
	renamed = true
	return target, nil
}

func (s Spawn) childCmd() (*exec.Cmd, *os.File, error) {
	exe := s.ExecPath
	if exe == "" {
		var err error
		exe, err = os.Executable()
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
				return nil, nil, fmt.Errorf("materialize stable child: %w", err)
			}
		}
	}
	logFile, err := os.OpenFile(s.LogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("open child log: %w", err)
	}
	cmd := exec.Command(exe, s.Args...)
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	return cmd, logFile, nil
}

// reap waits out the child so its exit never strands a zombie: Setsid detaches
// the session, not the parent-child link, and Process.Release would not reap.
func reap(cmd *exec.Cmd) {
	go func() { _ = cmd.Wait() }()
}
