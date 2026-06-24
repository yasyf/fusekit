package fileproviderd

import (
	"context"
	"fmt"
	"time"
)

// RemoteDomainHost drives the signed File Provider companion app over its
// control socket, so a domain — system-supervised and surviving process death —
// is registered, located, signalled, and removed from any Go process. It is the
// File-Provider analog of mountd.RemoteHost: the wire/lifecycle half a consumer
// overlay provider embeds (Ensure/Remove/Signal/State), with the consumer-
// specific Backend()/PrivateRoot() adapter left to the provider (see
// overlay.FileProviderProvider in Stage B). It composes AppSpawn (ensure the
// signed app is running) with AppClient (drive the domain ops).
//
// Unlike mountd.RemoteHost there is NO local kernel-state fast path: a File
// Provider domain's truth lives in NSFileProviderManager inside the signed app,
// not in a mountpoint this process can stat, so every op goes through the
// control socket. There is also no Converge/Retire skew machinery — a domain is
// OS-supervised, not a detached holder the consumer must version-replace.
type RemoteDomainHost struct {
	// AppPath is the companion app bundle path, passed to `open -g`. Required for
	// Ensure to spawn the app; State/Signal/Remove against an already-running app
	// do not need to spawn, but a non-running app makes them report unavailable.
	AppPath string
	// ControlSocket is the companion app's control socket path. Required.
	ControlSocket string
	// SpawnTimeout bounds waiting for a freshly launched app's control socket.
	// Zero means DefaultSpawnTimeout.
	SpawnTimeout time.Duration
}

// appSpawn builds the AppSpawn that brings the companion app up.
func (h *RemoteDomainHost) appSpawn() AppSpawn {
	return AppSpawn{
		AppPath:       h.AppPath,
		ControlSocket: h.ControlSocket,
		Timeout:       h.SpawnTimeout,
	}
}

// client builds an AppClient for the control socket.
func (h *RemoteDomainHost) client() *AppClient { return NewAppClient(h.ControlSocket) }

// Ensure registers the domain (spawning the companion app first if it is not
// already serving) and returns the user-visible domain root — the path the
// consumer symlinks its canonical account dir into. Register is idempotent, so
// an already-registered domain returns its existing root with no churn.
//
// Failure classes (errors.Is): ErrCannotControl (the entitlement is missing /
// the extension disabled — the ONLY retreat condition) and ErrAppLaunchUnsupported
// flow through unwrapped from the spawn; ErrAppUnavailable (app could not be
// brought up, or the op failed mid-flight) and ErrRegisterFailed/ErrBusy are
// transient — the caller retries.
func (h *RemoteDomainHost) Ensure(ctx context.Context, domain string) (string, error) {
	if domain == "" {
		return "", fmt.Errorf("ensure: domain is required")
	}
	if err := h.appSpawn().EnsureRunning(ctx); err != nil {
		return "", fmt.Errorf("ensure domain %s: %w", domain, err)
	}
	path, err := h.client().Register(ctx, domain)
	if err != nil {
		return "", fmt.Errorf("ensure domain %s: %w", domain, err)
	}
	return path, nil
}

// Remove deregisters the domain. The companion app is spawned first if needed
// (a domain survives the app's death, so a removal may have to bring the app
// back to deregister it). A domain the app has no registration for is an OK
// no-op (Remove on the app side absorbs that).
func (h *RemoteDomainHost) Remove(ctx context.Context, domain string) error {
	if domain == "" {
		return fmt.Errorf("remove: domain is required")
	}
	if err := h.appSpawn().EnsureRunning(ctx); err != nil {
		return fmt.Errorf("remove domain %s: %w", domain, err)
	}
	if err := h.client().Remove(ctx, domain); err != nil {
		return fmt.Errorf("remove domain %s: %w", domain, err)
	}
	return nil
}

// Signal tells the app to signal the domain's enumerator so the OS re-enumerates
// after a backing-tree change. It does NOT spawn: a signal is a low-latency
// nudge on the daemon's hot path, and a not-running app has nothing to signal
// (its domain re-enumerates on its own watcher when it next launches), so an
// unreachable app surfaces as ErrAppUnavailable for the caller to ignore rather
// than paying a spawn on every base mutation.
func (h *RemoteDomainHost) Signal(ctx context.Context, domain string) error {
	if domain == "" {
		return fmt.Errorf("signal: domain is required")
	}
	if err := h.client().Signal(ctx, domain); err != nil {
		return fmt.Errorf("signal domain %s: %w", domain, err)
	}
	return nil
}

// State reports the user-visible domain root for an already-registered domain,
// WITHOUT spawning the app or re-registering — the cheap health/adopt probe on
// the daemon's poll hot path. A reachable app with a live registration returns
// the root; ErrNoDomain means the app is up but has no registration (the caller
// re-Ensures); ErrAppUnavailable means the app is not running (its domains
// survive, so this is not a domain failure — the caller debounces or spawns via
// Ensure).
func (h *RemoteDomainHost) State(ctx context.Context, domain string) (string, error) {
	if domain == "" {
		return "", fmt.Errorf("state: domain is required")
	}
	path, err := h.client().Path(ctx, domain)
	if err != nil {
		return "", fmt.Errorf("state domain %s: %w", domain, err)
	}
	return path, nil
}

// Probe asks the app whether File Provider can serve on this machine — the
// capability gate a consumer's Select keys adoption on. It spawns the app first
// (the probe must reach a running app to register a throwaway domain). A false
// with ErrCannotControl is the permanent retreat verdict; a transient error
// (ErrAppUnavailable, ErrRegisterFailed) leaves capability undecided for the
// next probe.
func (h *RemoteDomainHost) Probe(ctx context.Context) (bool, error) {
	if err := h.appSpawn().EnsureRunning(ctx); err != nil {
		return false, fmt.Errorf("probe: %w", err)
	}
	ok, err := h.client().Probe(ctx)
	if err != nil {
		return false, fmt.Errorf("probe: %w", err)
	}
	return ok, nil
}
