// Package overlay realizes a per-tenant overlay of one shared base dir: each
// account dir presents the live contents of the base with writes shared straight
// back, so every tenant sees the same entries as the base. It realizes that
// overlay across three backends — symlink, nfs, and fskit — that yield the same
// observable result by different means: symlink links each top-level base entry
// into the account dir in-process, while the two fuse-t backends (nfs, fskit)
// serve a passthrough mirror hosted by a detached mount holder over its socket,
// so the mounts outlive the daemon and CLI processes that ask for them. A small
// set of entries is held back from sharing because it is instance-local runtime
// state that would conflict across concurrent tenants; the consumer declares
// those via Spec (IsPrivate, Excluded). All consumer-specific classification
// flows through Spec — the package names no consumer's domain entries itself — so
// the same machinery serves any consumer mirroring one base into per-tenant dirs.
//
// Selection is the package's job. Select probes this machine — build capability
// via fusekit.Built(), holder reachability, and a holder-side probe mount — and
// returns the realized Provider plus a human-readable reason when it falls back
// to symlink. ProviderFor reconstructs a Provider from a stored backend without
// probing, so a recorded verdict is honored verbatim across processes.
//
// The two constructors are deliberately asymmetric: ProviderFor(BackendSymlink)
// returns a complete in-process provider, but a fuse backend returns a
// RemoteFuseProvider — only the wire/lifecycle half — so the consumer supplies
// the cgofuse filesystem the holder serves via Spec.Holder. The fuse half lives
// out-of-process for a reason: mount capability and the macOS grant are
// per-process, and the holder, not this package, is the process that hosts and
// outlives the mounts.
package overlay

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/fusekit/fileproviderd"
)

// Provider establishes and maintains an overlay of base at accountDir.
type Provider interface {
	// Backend reports which backend this provider realizes.
	Backend() Backend

	// Setup makes accountDir reflect base. Idempotent.
	Setup(base, accountDir string) error

	// Sync re-asserts the overlay, picking up new top-level entries in base
	// and repairing drift. Idempotent.
	Sync(base, accountDir string) error

	// Health returns nil if the overlay is intact, else a descriptive error.
	Health(base, accountDir string) error

	// Teardown removes the overlay from accountDir. It must never touch base.
	Teardown(base, accountDir string) error

	// PrivateRoot returns the directory where account-local (private) files
	// physically live (accountDir for symlink, the backing dir beside the
	// mountpoint for fuse); writing there is correct whether or not a mount is up.
	PrivateRoot(accountDir string) string
}

// FileProviderProvider adapts fileproviderd.RemoteDomainHost to overlay.Provider.
// Unlike RemoteFuseProvider (which embeds mountd.RemoteHost), it implements each
// Provider method explicitly because RemoteDomainHost's ops take a context and a
// domain identifier.
//
// The overlay is a symlink bridge: the OS surfaces the domain under a user-visible
// root, but the canonical account dir string is hashed byte-for-byte into a service
// name and must stay put, so Setup makes accountDir a fail-closed symlink INTO the
// domain root.
type FileProviderProvider struct {
	// host drives the signed companion app; never nil for a constructed provider.
	host *fileproviderd.RemoteDomainHost
	// bridgeSocket is the data socket the daemon's BridgeServer binds; carried for
	// Health reachability and consumer wiring.
	bridgeSocket string
	// readyTimeout is Setup's serve budget (from the app's first answer); zero means defaultFPReadyTimeout.
	readyTimeout time.Duration
	// appReadyTimeout is Setup's contact budget (to first answer at all); zero means defaultFPAppReadyTimeout.
	appReadyTimeout time.Duration
	// upgradeHint is the operator guidance Setup appends when the app is too old to
	// answer probe-domain; never empty for a constructed provider.
	upgradeHint string
}

// defaultFPReadyTimeout is Setup's serve budget when ReadyTimeout is zero, generous
// because an appex can cold-start for minutes under a migrate-storm backlog.
const defaultFPReadyTimeout = 6 * time.Minute

// defaultFPAppReadyTimeout is Setup's contact budget when AppReadyTimeout is zero.
const defaultFPAppReadyTimeout = 2 * time.Minute

// fpReadyPollInterval spaces Setup's ProbeDomain readiness polls; a var so tests
// shrink it.
var fpReadyPollInterval = 100 * time.Millisecond

var _ Provider = (*FileProviderProvider)(nil)

func newFileProvider(fp *FileProviderSpec) *FileProviderProvider {
	hint := fp.UpgradeHint
	if hint == "" {
		hint = "upgrade the companion File Provider app"
	}
	return &FileProviderProvider{
		host: &fileproviderd.RemoteDomainHost{
			AppPath:       fp.AppPath,
			ControlSocket: fp.ControlSocket,
			SpawnTimeout:  fp.SpawnTimeout,
			LaunchTimeout: fp.LaunchTimeout,
		},
		bridgeSocket:    fp.BridgeSocket,
		readyTimeout:    fp.ReadyTimeout,
		appReadyTimeout: fp.AppReadyTimeout,
		upgradeHint:     hint,
	}
}

// domainFor derives the File Provider domain identifier: the account dir's basename
// (e.g. acct-NN), a stable identifier distinct from the hashed account dir string.
func domainFor(accountDir string) string { return filepath.Base(accountDir) }

// Backend reports BackendFileProvider even in a process that can't host the
// extension (only the signed app can), keeping stored-backend fences honest.
func (p *FileProviderProvider) Backend() Backend { return BackendFileProvider }

// PrivateRoot returns the per-account private backing dir, shared with the fuse
// provider (FusePrivateRoot) because FP and FUSE never coexist for one account.
func (p *FileProviderProvider) PrivateRoot(accountDir string) string {
	return FusePrivateRoot(accountDir)
}

// Setup registers the domain, waits for it to serve, then cuts accountDir over (see
// cutOver). Idempotent. On a post-registration failure it removes a domain THIS call
// freshly registered — never a pre-existing one — so a rolled-back add leaves no
// orphan domain. Assumes the consumer serializes Setup per account: two concurrent
// Setups of one domain could each see the other as absent and roll back its cutover.
func (p *FileProviderProvider) Setup(base, accountDir string) error {
	ctx := context.Background()
	domain := domainFor(accountDir)
	registerStart := time.Now()
	root, fresh, err := p.host.EnsureReport(ctx, domain)
	register := time.Since(registerStart)
	if err != nil {
		return fmt.Errorf("file provider setup %s (register %s, serve-wait %s): %w", accountDir, register.Round(time.Second), time.Duration(0), err)
	}
	serveStart := time.Now()
	err = p.cutOver(ctx, accountDir, domain, root)
	serveWait := time.Since(serveStart)
	if err != nil {
		if fresh {
			if rmErr := p.host.RemoveConfirmed(ctx, domain); rmErr != nil {
				err = errors.Join(err, fmt.Errorf("remove just-registered domain: %w", rmErr))
			}
		}
		return fmt.Errorf("file provider setup %s (register %s, serve-wait %s): %w", accountDir, register.Round(time.Second), serveWait.Round(time.Second), err)
	}
	return nil
}

// cutOver waits for the domain to serve, seeds the private store, then symlinks
// accountDir into its root. The symlink is laid last (hardest to retract) so a
// readiness or seed failure never leaves a dangling link to a rolled-back root.
func (p *FileProviderProvider) cutOver(ctx context.Context, accountDir, domain, root string) error {
	if err := p.waitDomainServes(ctx, domain); err != nil {
		return err
	}
	if err := os.MkdirAll(p.PrivateRoot(accountDir), 0o700); err != nil {
		return fmt.Errorf("seed private store: %w", err)
	}
	if err := fileproviderd.AtomicSymlink(accountDir, root); err != nil {
		return err
	}
	return nil
}

// waitDomainServes polls ProbeDomain until the domain serves, across two budgets: a
// contact budget (appReadyTimeout) while the app is not answering at all, and a serve
// budget (readyTimeout) measured from its first answer. Any answered verdict counts
// as serving; either budget expiring fails with ErrDomainNotServing. ErrOpUnsupported
// (an app too old to answer probe-domain) and ErrCannotControl fail immediately with
// no raw-filesystem fallback — a silent read would resurrect the TCC prompt storm.
func (p *FileProviderProvider) waitDomainServes(ctx context.Context, domain string) error {
	contactTimeout := p.appReadyTimeout
	if contactTimeout <= 0 {
		contactTimeout = defaultFPAppReadyTimeout
	}
	serveTimeout := p.readyTimeout
	if serveTimeout <= 0 {
		serveTimeout = defaultFPReadyTimeout
	}
	contactDeadline := time.Now().Add(contactTimeout)
	var serveDeadline time.Time // zero until the app first answers
	for {
		_, err := p.host.ProbeDomain(ctx, domain)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, fileproviderd.ErrOpUnsupported):
			return fmt.Errorf("%s: %w", p.upgradeHint, err)
		case errors.Is(err, fileproviderd.ErrCannotControl):
			return err
		case errors.Is(err, fileproviderd.ErrAppUnavailable):
			// Not answering yet: bounded by the contact budget until first answer.
		case errors.Is(err, fileproviderd.ErrNoDomain),
			errors.Is(err, fileproviderd.ErrDomainNotServing),
			errors.Is(err, fileproviderd.ErrBusy):
			// App answered; domain still materializing. Start the serve budget.
			if serveDeadline.IsZero() {
				serveDeadline = time.Now().Add(serveTimeout)
			}
		default:
			return err
		}
		deadline := contactDeadline
		if !serveDeadline.IsZero() {
			deadline = serveDeadline
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: %s", fileproviderd.ErrDomainNotServing, domain)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %s", fileproviderd.ErrDomainNotServing, domain)
		case <-time.After(fpReadyPollInterval):
		}
	}
}

// ProbeDomain reports the account's File Provider domain verdict without a
// materializing filesystem read: nil = the domain serves but .claude.json is absent;
// a pointer to 0 = present and empty; >0 = bytes actually read. It is a zero-spawn
// probe — errors.Is classes ErrNoDomain, ErrDomainNotServing, ErrBusy,
// ErrAppUnavailable, ErrOpUnsupported flow through for the caller to classify.
func (p *FileProviderProvider) ProbeDomain(ctx context.Context, accountDir string) (*int64, error) {
	v, err := p.host.ProbeDomain(ctx, domainFor(accountDir))
	if err != nil {
		return nil, fmt.Errorf("file provider probe domain %s: %w", accountDir, err)
	}
	return v, nil
}

// RemoveDomain deregisters the account's domain WITHOUT retracting the bridge
// symlink (unlike Teardown), spawning the app if needed since domains survive app
// death. An unregistered domain is a no-op.
func (p *FileProviderProvider) RemoveDomain(accountDir string) error {
	if err := p.host.Remove(context.Background(), domainFor(accountDir)); err != nil {
		return fmt.Errorf("file provider remove domain %s: %w", accountDir, err)
	}
	return nil
}

// DomainRoot reports the user-visible root of the account's registered domain
// WITHOUT spawning the app or reading through the domain — the host's zero-spawn
// registration check (State). An unregistered domain surfaces
// fileproviderd.ErrNoDomain; a not-running app surfaces
// fileproviderd.ErrAppUnavailable (domains survive app death, so registration is
// simply unknown). Consumers use it to detect a domain still registered against a
// row that no longer wants it, so it can be RemoveDomain'd without a spawn storm.
func (p *FileProviderProvider) DomainRoot(ctx context.Context, accountDir string) (string, error) {
	root, err := p.host.State(ctx, domainFor(accountDir))
	if err != nil {
		return "", fmt.Errorf("file provider domain root %s: %w", accountDir, err)
	}
	return root, nil
}

// Sync re-registers the domain, re-asserts the bridge symlink, and nudges the
// enumerator so the OS re-reads after a base change. A Signal against a
// momentarily-unreachable app returns the transient ErrAppUnavailable and is
// ignored (the app re-enumerates on its own watcher when it next launches), so Sync
// fails only on a real registration or symlink failure.
func (p *FileProviderProvider) Sync(base, accountDir string) error {
	domain := domainFor(accountDir)
	root, err := p.host.Ensure(context.Background(), domain)
	if err != nil {
		return fmt.Errorf("file provider sync %s: %w", accountDir, err)
	}
	if err := fileproviderd.AtomicSymlink(accountDir, root); err != nil {
		return fmt.Errorf("file provider sync %s: %w", accountDir, err)
	}
	if err := p.host.Signal(context.Background(), domain); err != nil && !errors.Is(err, fileproviderd.ErrAppUnavailable) {
		return fmt.Errorf("file provider sync %s: signal: %w", accountDir, err)
	}
	return nil
}

// Health reports whether the overlay is intact: the domain is registered (State, a
// zero-spawn probe), the bridge symlink points at the live domain root, and a
// targeted signal is sent. ErrNoDomain and a drifted or missing symlink are
// failures the caller heals with Sync; ErrAppUnavailable (app down) is surfaced so
// the caller debounces rather than retreating — the domain survives the app's death.
func (p *FileProviderProvider) Health(base, accountDir string) error {
	domain := domainFor(accountDir)
	root, err := p.host.State(context.Background(), domain)
	if err != nil {
		return fmt.Errorf("file provider health %s: %w", accountDir, err)
	}
	cur, err := os.Readlink(accountDir)
	if err != nil {
		return fmt.Errorf("file provider health %s: account dir is not the bridge symlink: %w", accountDir, err)
	}
	if cur != root {
		return fmt.Errorf("file provider health %s: bridge symlink points at %q, want the domain root %q", accountDir, cur, root)
	}
	if err := p.host.Signal(context.Background(), domain); err != nil && !errors.Is(err, fileproviderd.ErrAppUnavailable) {
		return fmt.Errorf("file provider health %s: signal: %w", accountDir, err)
	}
	return nil
}

// Teardown retracts the bridge symlink (RemoveSymlink is fail-closed: it refuses to
// delete a real account dir occupying the path) and deregisters the domain, leaving
// the private store in place — Teardown removes the overlay, not the account's
// private state. It never touches base.
func (p *FileProviderProvider) Teardown(base, accountDir string) error {
	if err := fileproviderd.RemoveSymlink(accountDir); err != nil {
		return fmt.Errorf("file provider teardown %s: %w", accountDir, err)
	}
	if err := p.host.Remove(context.Background(), domainFor(accountDir)); err != nil {
		return fmt.Errorf("file provider teardown %s: %w", accountDir, err)
	}
	return nil
}
