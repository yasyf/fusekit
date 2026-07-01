package fileproviderd

import (
	"context"
	"fmt"
	"time"
)

// RemoteDomainHost drives the signed File Provider companion app over its
// control socket to register, locate, signal, and remove an OS-supervised
// domain. Domain truth lives in the app's NSFileProviderManager, so every op
// crosses the socket — no local kernel-state fast path.
type RemoteDomainHost struct {
	// AppPath is the companion app bundle path. Required.
	AppPath string
	// ControlSocket is the companion app's control socket path. Required.
	ControlSocket string
	// SpawnTimeout bounds waiting for a freshly launched app's control socket.
	// Zero means DefaultSpawnTimeout.
	SpawnTimeout time.Duration
}

func (h *RemoteDomainHost) appSpawn() AppSpawn {
	return AppSpawn{
		AppPath:       h.AppPath,
		ControlSocket: h.ControlSocket,
		Timeout:       h.SpawnTimeout,
	}
}

func (h *RemoteDomainHost) client() *AppClient { return NewAppClient(h.ControlSocket) }

// Ensure registers the domain (spawning the companion app if needed) and
// returns the user-visible domain root; registration is idempotent.
// ErrCannotControl and ErrAppLaunchUnsupported flow through unwrapped as
// permanent retreat conditions; other errors are transient and retried.
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

// Remove deregisters the domain, spawning the companion app first if needed
// (domains survive app death). An unregistered domain is a no-op.
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

// Signal tells the app to signal the domain's enumerator so the OS
// re-enumerates after a backing-tree change. It never spawns (hot path; a
// not-running app re-enumerates on next launch): an unreachable app is
// ErrAppUnavailable, which callers ignore.
func (h *RemoteDomainHost) Signal(ctx context.Context, domain string) error {
	if domain == "" {
		return fmt.Errorf("signal: domain is required")
	}
	if err := h.client().Signal(ctx, domain); err != nil {
		return fmt.Errorf("signal domain %s: %w", domain, err)
	}
	return nil
}

// State reports the domain root without spawning or re-registering — the cheap
// poll-path probe. ErrNoDomain: app up but no registration (caller re-Ensures);
// ErrAppUnavailable: app not running (domains survive, so not a domain
// failure).
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
// consumer's adoption gate. It spawns the app (a throwaway domain must be
// registered). ErrCannotControl is the permanent retreat verdict; transient
// errors leave capability undecided.
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
