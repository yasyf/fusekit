package mountd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit"
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
		Socket:    s.Socket,
		LogPath:   s.LogPath,
		Args:      s.Args,
		Timeout:   s.Timeout,
		ExecPath:  s.ExecPath,
		Available: cl.Available,
		CanHost:   s.canHost,
		// A cask .app holder MUST launch via LaunchServices (`open -g`): a direct
		// exec of the bundle's Mach-O runs outside the GUI session that fuse-t
		// volume bring-up and the volume-access TCC grant need, and never comes up.
		Launch: appLaunchStrategy(s.ExecPath),
	}
	// The LaunchServices path absorbs Gatekeeper assessment on first launch, so give
	// it the longer socket-bind bound when the caller left Timeout unset.
	if ps.Launch != nil && ps.Timeout <= 0 {
		ps.Timeout = appLaunchTimeout
	}
	err := ps.EnsureRunning(context.Background())
	if errors.Is(err, proc.ErrAppLaunchUnsupported) {
		return fmt.Errorf("%w: %w", ErrHolderUnavailable, err)
	}
	return err
}

// appLaunchStrategy selects the LaunchStrategy for a holder ExecPath: a cask .app
// bundle's inner binary launches via LaunchServices (`open -g` the bundle), which
// proc.AppLaunchNew drives, never a direct exec of the inner Mach-O; a bare
// (non-bundle) path returns nil for the default direct exec. proc.Spawn
// short-circuits on Available, so a serving holder another consumer shares is
// never relaunched.
func appLaunchStrategy(execPath string) proc.LaunchStrategy {
	if app := appBundle(execPath); app != "" {
		return proc.AppLaunchNew{App: app}
	}
	return nil
}

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
