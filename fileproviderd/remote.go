package fileproviderd

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// defaultDomainLoadSettle is EnsureReport's absence-confirm window when DomainLoadSettle is zero.
const defaultDomainLoadSettle = 3 * time.Second

// defaultDomainLoadPollInterval spaces EnsureReport's absence-confirm polls when DomainLoadPollInterval is zero.
const defaultDomainLoadPollInterval = 100 * time.Millisecond

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
	// DomainLoadSettle is how long EnsureReport's Path pre-check must keep answering
	// ErrNoDomain before the domain is deemed absent. Zero means defaultDomainLoadSettle.
	DomainLoadSettle time.Duration
	// DomainLoadPollInterval spaces EnsureReport's absence-confirm polls. Zero means
	// defaultDomainLoadPollInterval.
	DomainLoadPollInterval time.Duration
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

// EnsureReport registers the domain like Ensure and reports whether THIS call
// freshly created it (fresh), so a caller tears down only a domain it just made.
// fresh=true means the settle-confirmed pre-check proved it absent (see
// confirmAbsent); any answered verdict or pre-check error reports fresh=false.
func (h *RemoteDomainHost) EnsureReport(ctx context.Context, domain string) (string, bool, error) {
	if domain == "" {
		return "", false, fmt.Errorf("ensure: domain is required")
	}
	if err := h.appSpawn().EnsureRunning(ctx); err != nil {
		return "", false, fmt.Errorf("ensure domain %s: %w", domain, err)
	}
	c := h.client()
	fresh := h.confirmAbsent(ctx, c, domain)
	path, err := c.Register(ctx, domain)
	if err != nil {
		return "", false, fmt.Errorf("ensure domain %s: %w", domain, err)
	}
	return path, fresh, nil
}

// confirmAbsent reports whether domain is provably absent: a Path pre-check that
// answers ErrNoDomain across the whole DomainLoadSettle window. Any other verdict —
// including a cold appex revealing a pre-existing domain mid-settle, or a hard
// error — ends the settle early and reports not-absent.
func (h *RemoteDomainHost) confirmAbsent(ctx context.Context, c *AppClient, domain string) bool {
	settle := h.DomainLoadSettle
	if settle <= 0 {
		settle = defaultDomainLoadSettle
	}
	interval := h.DomainLoadPollInterval
	if interval <= 0 {
		interval = defaultDomainLoadPollInterval
	}
	deadline := time.Now().Add(settle)
	for {
		if _, err := c.Path(ctx, domain); !errors.Is(err, ErrNoDomain) {
			return false
		}
		if time.Now().After(deadline) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(interval):
		}
	}
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

// ProbeDomain asks the app whether the domain serves and reports its .claude.json
// byte-count verdict (nil = serving but .claude.json absent; a pointer to 0 =
// present and empty; >0 = bytes read) WITHOUT a materializing filesystem read. Like
// State it does NOT spawn — a zero-spawn probe on the readiness poll path; a
// not-running app is ErrAppUnavailable (domains survive app death, so not a domain
// verdict). ErrDomainNotServing: registered but not yet serving; ErrNoDomain: no
// registration; ErrOpUnsupported: an app too old to know the op.
func (h *RemoteDomainHost) ProbeDomain(ctx context.Context, domain string) (*int64, error) {
	if domain == "" {
		return nil, fmt.Errorf("probe domain: domain is required")
	}
	v, err := h.client().ProbeDomain(ctx, domain)
	if err != nil {
		return nil, fmt.Errorf("probe domain %s: %w", domain, err)
	}
	return v, nil
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
