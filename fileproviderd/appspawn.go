package fileproviderd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/proc"
)

// DefaultSpawnTimeout re-exports proc.DefaultSpawnTimeout: the spawn-wait bound
// a zero AppSpawn.Timeout / RemoteDomainHost.SpawnTimeout falls back to. It
// stays in the fileproviderd surface so consumers that named it keep compiling.
const DefaultSpawnTimeout = proc.DefaultSpawnTimeout

// ErrAppLaunchUnsupported is the non-darwin refusal: the File Provider companion
// app is launched via macOS `open`, which exists only on macOS. It is a DISTINCT
// sentinel that must never errors.Is-match ErrAppUnavailable — an unreachable
// app is transient and drives retry, while a platform that can never launch the
// app is permanent. It is NOT ErrCannotControl either: ErrCannotControl is a
// machine that has the app but lacks the entitlement (a capability "no" Select
// retreats on); this is a build/platform with no app-launch path at all.
var ErrAppLaunchUnsupported = errors.New("launching the File Provider companion app is only supported on macOS")

// launchApp opens the companion app at appPath detached (background, no
// activation). It is the platform seam: applaunch_darwin.go runs `open -g`,
// applaunch_other.go returns ErrAppLaunchUnsupported. A var so a spawn test
// records the launch without shelling out to a real app.
var launchApp = launchAppPlatform

// AppSpawn ensures the signed File Provider companion app is running and serving
// its control socket, launching it via macOS `open -g` if needed and waiting for
// the socket to appear. It wraps proc.Spawn — but unlike mountd's holder spawn
// (which execs THIS binary and supports StableExecDir for the per-process TCC
// grant), the companion app is a separate signed bundle: its entitlement lives
// in its code signature, not a per-process grant, so there is NO StableExecDir.
// The Override seam replaces proc.Spawn's exec-this-binary body with the `open`
// launch.
type AppSpawn struct {
	// AppPath is the companion app bundle path (e.g. /Applications/Foo.app),
	// passed to `open -g`. Required.
	AppPath string
	// ControlSocket is the control socket the app binds once launched; spawn
	// waits for it to accept a connection. Required.
	ControlSocket string
	// Timeout bounds waiting for a freshly launched app's control socket. Zero
	// means DefaultSpawnTimeout. The companion app's launch + File Provider
	// framework bring-up is heavier than a Go child, so consumers typically set
	// this well above the default.
	Timeout time.Duration
}

// EnsureRunning makes sure the companion app serves ControlSocket, returning nil
// once it is reachable. On a non-darwin build the launch leg refuses with
// ErrAppLaunchUnsupported (UNWRAPPED — a permanent platform condition, never the
// transient ErrAppUnavailable). On darwin it `open -g`s the app and waits up to
// the timeout; a socket that never comes up wraps ErrAppUnavailable so consumers
// retry rather than retreat.
func (s AppSpawn) EnsureRunning(ctx context.Context) error {
	if s.AppPath == "" || s.ControlSocket == "" {
		return fmt.Errorf("%w: AppSpawn requires AppPath and ControlSocket", ErrAppUnavailable)
	}
	cl := NewAppClient(s.ControlSocket)
	return proc.Spawn{
		Socket:    s.ControlSocket,
		Timeout:   s.Timeout,
		Available: cl.Available,
		// CanHost never refuses here: capability is gated downstream (the OS
		// refuses a domain register with ClassNoEntitlement, surfaced as
		// ErrCannotControl). proc.Spawn requires it, so supply a permissive one.
		CanHost: func() error { return nil },
		// Override replaces proc.Spawn's exec-this-binary detached spawn with the
		// `open -g` app launch, then proc.Spawn polls Available up to Timeout.
		// Returning nil here lets that poll run; a launch error wraps
		// ErrAppUnavailable (a non-darwin build returns ErrAppLaunchUnsupported,
		// which is NOT folded into ErrAppUnavailable — it is permanent).
		Override: func() error {
			if err := launchApp(ctx, s.AppPath); err != nil {
				if errors.Is(err, ErrAppLaunchUnsupported) {
					return err
				}
				return fmt.Errorf("%w: launch %s: %w", ErrAppUnavailable, s.AppPath, err)
			}
			return s.waitForSocket(cl)
		},
	}.EnsureRunning()
}

// waitForSocket polls the control socket until it accepts a connection or the
// timeout elapses. proc.Spawn's own post-Override poll also checks Available,
// but proc.Spawn's loop runs after Override RETURNS; doing the wait inside
// Override keeps a launch-then-never-serve into one ErrAppUnavailable error that
// names the socket, matching mountd's did-not-come-up shape.
func (s AppSpawn) waitForSocket(cl *AppClient) error {
	timeout := s.Timeout
	if timeout <= 0 {
		timeout = DefaultSpawnTimeout
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cl.Available() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("%w: companion app did not serve %s within %s", ErrAppUnavailable, s.ControlSocket, timeout)
}
