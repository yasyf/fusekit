package fileproviderd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// DefaultSpawnTimeout is the spawn-wait bound a zero AppSpawn.Timeout or
// RemoteDomainHost.SpawnTimeout falls back to; re-exported so consumers that
// named it keep compiling.
const DefaultSpawnTimeout = proc.DefaultSpawnTimeout

// defaultAppLaunchTimeout bounds the `open -g` launch itself when
// AppSpawn.LaunchTimeout is zero — the one otherwise-unbounded call in the Setup
// chain, which a wedged fileproviderd can hang forever.
const defaultAppLaunchTimeout = 30 * time.Second

// ErrAppLaunchUnsupported is the non-darwin refusal (the app launches via macOS
// `open`). A distinct permanent sentinel that must never errors.Is-match
// ErrAppUnavailable (transient, drives retry) nor ErrCannotControl (has the app
// but lacks the entitlement); this platform has no app-launch path at all.
var ErrAppLaunchUnsupported = errors.New("launching the File Provider companion app is only supported on macOS")

// launchApp opens the companion app detached: applaunch_darwin.go runs `open -g`,
// applaunch_other.go returns ErrAppLaunchUnsupported. A var so tests stub the
// launch without a real app.
var launchApp = launchAppPlatform

// AppSpawn ensures the signed File Provider companion app is running and serving
// its control socket, launching it via macOS `open -g` if needed and waiting for
// the socket. It wraps proc.Spawn, but the companion app is a separate signed
// bundle whose entitlement lives in its code signature (not a per-process TCC
// grant), so it never spawns from a stable copy; the Override seam swaps proc.Spawn's
// exec-this-binary body for the `open` launch.
type AppSpawn struct {
	// AppPath is the companion app bundle path, passed to `open -g`. Required.
	AppPath string
	// ControlSocket is the control socket the app binds once launched. Required.
	ControlSocket string
	// Timeout bounds waiting for a freshly launched app's control socket. Zero
	// means DefaultSpawnTimeout; consumers typically set it well above the
	// default since File Provider bring-up is heavier than a Go child.
	Timeout time.Duration
	// LaunchTimeout bounds the `open -g` launch itself, distinct from Timeout,
	// which waits for the socket: a stalled fileproviderd can hang the launch
	// indefinitely. Zero means defaultAppLaunchTimeout.
	LaunchTimeout time.Duration
}

// EnsureRunning makes sure the companion app serves ControlSocket, returning nil
// once reachable. Non-darwin refuses with ErrAppLaunchUnsupported; darwin
// `open -g`s the app and waits up to the timeout, wrapping ErrAppUnavailable if
// the socket never comes up.
func (s AppSpawn) EnsureRunning(ctx context.Context) error {
	if s.AppPath == "" || s.ControlSocket == "" {
		return fmt.Errorf("%w: AppSpawn requires AppPath and ControlSocket", ErrAppUnavailable)
	}
	cl := NewAppClient(s.ControlSocket)
	if cl.Available() {
		return nil
	}
	// The companion app is a separate signed bundle whose entitlement lives in its
	// code signature, so it never execs this binary — it launches via `open -g`,
	// bounded so a wedged fileproviderd cannot hang the launch forever. Capability is
	// gated downstream (the OS refuses a domain register with ClassNoEntitlement,
	// surfaced as ErrCannotControl), so there is no host-capability gate here.
	launchTimeout := s.launchTimeout()
	launchCtx, cancel := context.WithTimeout(ctx, launchTimeout)
	defer cancel()
	if err := launchApp(launchCtx, s.AppPath); err != nil {
		if errors.Is(err, ErrAppLaunchUnsupported) {
			return err
		}
		// Parent ctx done: the caller aborted, not our launch bound — keep its cause,
		// no launch-timeout copy (checked first: parent cancellation propagates into
		// launchCtx as DeadlineExceeded too).
		if ctx.Err() != nil {
			return fmt.Errorf("%w: launch %s: %w", ErrAppUnavailable, s.AppPath, ctx.Err())
		}
		// Our own launch deadline fired while the parent was still live.
		if errors.Is(launchCtx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("%w: launch %s: timed out after %s — fileproviderd may be stalled (Activity Monitor / reboot): %w", ErrAppUnavailable, s.AppPath, launchTimeout, err)
		}
		return fmt.Errorf("%w: launch %s: %w", ErrAppUnavailable, s.AppPath, err)
	}
	return s.waitForSocket(cl)
}

// launchTimeout is the `open -g` launch bound, defaulting to defaultAppLaunchTimeout.
func (s AppSpawn) launchTimeout() time.Duration {
	if s.LaunchTimeout > 0 {
		return s.LaunchTimeout
	}
	return defaultAppLaunchTimeout
}

// waitForSocket polls the control socket until it accepts or the timeout
// elapses. Waiting inside Override (proc.Spawn also polls afterward) folds a
// launch-then-never-serve into one ErrAppUnavailable that names the socket.
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
