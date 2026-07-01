package mountd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/proc"
)

// DefaultSpawnTimeout is the fallback for a zero Spawn.Timeout / RemoteHost.SpawnTimeout.
const DefaultSpawnTimeout = proc.DefaultSpawnTimeout

// appLaunchTimeout bounds a cask .app holder's socket bind after `open -g`, larger
// than proc.DefaultSpawnTimeout because the first LaunchServices launch absorbs
// Gatekeeper assessment.
const appLaunchTimeout = 20 * time.Second

// ErrCannotHost is the pure-build (no fuse tag) refusal to host or spawn. It must
// never errors.Is-match ErrHolderUnavailable: unavailable is transient (drives
// retry), cannot-host is permanent (drives the consumer's irreversible fallback).
var ErrCannotHost = errors.New("this binary cannot host fuse mounts")

// Spawn ensures a detached mount holder is serving a socket, auto-spawning one and
// waiting. Mounts live in the holder, so a running holder serves any build and only
// the spawn path needs the fuse build; racing spawns are harmless — the holder
// refuses an already-owned socket.
type Spawn struct {
	// Socket is the holder's unix socket path.
	Socket string
	// LogPath receives a spawned holder's stdout and stderr.
	LogPath string
	// Args is the spawned process's argv after the executable, e.g. ["mount-holder", "--socket", socket].
	Args []string
	// Timeout bounds the spawned holder's socket wait; zero means DefaultSpawnTimeout.
	Timeout time.Duration
	// CannotHostHint is the consumer's install guidance appended to ErrCannotHost.
	CannotHostHint string
	// StableExecDir, when non-empty, spawns the holder from a copy here instead of
	// os.Executable(): the stable path keeps the macOS volume-access TCC grant across
	// upgrades (the Developer-ID designated requirement survives the copy).
	StableExecDir string
	// ExecPath, when set, is the holder binary the child execs; canHost then
	// gates on it existing, not fusekit.Built().
	ExecPath string
}

// EnsureRunning ensures a holder serves Socket. Every could-not-start-or-reach leg
// wraps ErrHolderUnavailable; only the pure-build refusal returns ErrCannotHost
// unwrapped.
func (s Spawn) EnsureRunning() error {
	cl := NewClient(s.Socket)
	ps := proc.Spawn{
		Socket:        s.Socket,
		LogPath:       s.LogPath,
		Args:          s.Args,
		Timeout:       s.Timeout,
		StableExecDir: s.StableExecDir,
		ExecPath:      s.ExecPath,
		Available:     cl.Available,
		CanHost:       s.canHost,
	}
	// A cask .app holder MUST launch via LaunchServices (`open -g`): direct exec of
	// the bundle's Mach-O runs outside the GUI session that fuse-t volume bring-up and
	// the volume-access TCC grant need, and never comes up.
	if app := appBundle(s.ExecPath); app != "" {
		ps.Override = func() error { return s.launchAppHolder(cl, app) }
	}
	return ps.EnsureRunning()
}

// launchAppHolder is the proc.Spawn Override for a cask .app holder. proc.Spawn
// short-circuited on Available, so a serving holder is never relaunched — never
// version-replace a holder another consumer shares. `open -g` passes no argv (Args
// ignored), so the holder binds DefaultHolderSocket; Spawn.Socket must equal
// DefaultHolderSocket().
func (s Spawn) launchAppHolder(cl *Client, app string) error {
	if err := s.canHost(); err != nil {
		return err
	}
	if err := launchApp(context.Background(), app); err != nil {
		return fmt.Errorf("%w: launch holder app %s: %w", proc.ErrChildUnavailable, app, err)
	}
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = appLaunchTimeout
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cl.Available() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%w: holder app %s did not bind %s within %s", proc.ErrChildUnavailable, app, s.Socket, timeout)
}

// launchApp is a test seam over proc.LaunchApp.
var launchApp = proc.LaunchApp

// appBundle returns execPath's nearest ".app" ancestor, or "" outside a bundle.
func appBundle(execPath string) string {
	if execPath == "" {
		return ""
	}
	for p := execPath; ; {
		dir := filepath.Dir(p)
		if dir == p {
			return ""
		}
		if strings.HasSuffix(dir, ".app") {
			return dir
		}
		p = dir
	}
}

func (s Spawn) canHost() error {
	if s.ExecPath != "" {
		if _, err := os.Stat(s.ExecPath); err != nil {
			return fmt.Errorf("%w: holder not installed at %s (try `brew install --cask %s`): %s", ErrCannotHost, s.ExecPath, HolderCask, s.CannotHostHint)
		}
		return nil
	}
	if fusekit.Built() {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrCannotHost, s.CannotHostHint)
}
