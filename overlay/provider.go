// Package overlay realizes a per-tenant overlay of one shared base dir: each
// account dir presents the live contents of the base with writes shared straight
// back, so every tenant sees the same entries as the base. It realizes that
// overlay across four backends — symlink, nfs, fskit, and fileprovider — that
// yield the same observable result by different means: symlink links each
// top-level base entry in-process, the two fuse-t backends serve a passthrough
// mirror from a detached holder, and File Provider bridges to an OS-managed
// domain. The out-of-process backends outlive daemon and CLI callers. A small
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

	// Reconcile makes accountDir reflect base, repairing drift. Idempotent.
	Reconcile(ctx context.Context, base, accountDir string) error

	// Check returns nil if the overlay is intact, else a descriptive error. It
	// must not reconcile, notify content consumers, or spawn a helper process.
	Check(ctx context.Context, base, accountDir string) error

	// Teardown removes the overlay from accountDir. It must never touch base.
	// A non-empty warning means the backend's durable state is stale (a holder
	// journal persist failure after the kernel detach) and a successor could
	// replay the reclaimed overlay — surface it, never treat it as failure.
	Teardown(ctx context.Context, base, accountDir string) (warning string, err error)

	// PrivateRoot returns the directory where account-local (private) files
	// physically live (accountDir for symlink, the backing dir beside the
	// mountpoint for fuse); writing there is correct whether or not a mount is up.
	PrivateRoot(accountDir string) string
}

// ContentNotifier is the optional capability for providers whose visible content
// is cached or enumerated outside the process. NotifyContent tells that external
// consumer to re-read accountDir after the caller has committed a content change.
// It does not reconcile overlay structure. FileProviderProvider implements it;
// the symlink and fuse providers do not need notification.
type ContentNotifier interface {
	NotifyContent(ctx context.Context, accountDir string) error
}

// FileProviderProvider adapts fileproviderd.RemoteDomainHost to overlay.Provider.
// Unlike RemoteFuseProvider (which adapts mountd.RemoteHost), it implements each
// Provider method against contextful domain operations.
//
// The overlay is a symlink bridge: the OS surfaces the domain under a user-visible
// root, but the canonical account dir string is hashed byte-for-byte into a service
// name and must stay put, so Reconcile makes accountDir a fail-closed symlink INTO the
// domain root.
type FileProviderProvider struct {
	// host drives the signed companion app; never nil for a constructed provider.
	host *fileproviderd.RemoteDomainHost
	// bridgeSocket is the data socket the daemon's BridgeServer binds; carried for
	// Check reachability and consumer wiring.
	bridgeSocket string
	// readyTimeout is Reconcile's serve budget (from the app's first answer); zero means defaultFPReadyTimeout.
	readyTimeout time.Duration
	// appReadyTimeout is Reconcile's contact budget (to first answer at all); zero means defaultFPAppReadyTimeout.
	appReadyTimeout time.Duration
	// upgradeHint is the operator guidance Reconcile appends when the app is too old to
	// answer probe-domain; never empty for a constructed provider.
	upgradeHint string
}

// defaultFPReadyTimeout is Reconcile's serve budget when ReadyTimeout is zero, generous
// because an appex can cold-start for minutes under a migrate-storm backlog.
const defaultFPReadyTimeout = 6 * time.Minute

// defaultFPAppReadyTimeout is Reconcile's contact budget when AppReadyTimeout is zero.
const defaultFPAppReadyTimeout = 2 * time.Minute

// fpReadyPollInterval spaces Reconcile's ProbeDomain readiness polls; a var so tests
// shrink it.
var fpReadyPollInterval = 100 * time.Millisecond

var _ Provider = (*FileProviderProvider)(nil)
var _ ContentNotifier = (*FileProviderProvider)(nil)

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

// Reconcile registers the domain, waits for it to serve, then cuts accountDir over
// (see cutOver). Idempotent. On a post-registration failure it removes a domain THIS call
// freshly registered — never a pre-existing one — so a rolled-back add leaves no
// orphan domain. Assumes the consumer serializes Reconcile per account: two concurrent
// calls for one domain could each see the other as absent and roll back its cutover.
// Reconcile never signals the enumerator; content changes use NotifyContent.
func (p *FileProviderProvider) Reconcile(ctx context.Context, base, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	domain := domainFor(accountDir)
	registerStart := time.Now()
	root, fresh, err := p.host.EnsureReport(ctx, domain)
	register := time.Since(registerStart)
	if err != nil {
		return fmt.Errorf("file provider reconcile %s (register %s, serve-wait %s): %w", accountDir, register.Round(time.Second), time.Duration(0), err)
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
		return fmt.Errorf("file provider reconcile %s (register %s, serve-wait %s): %w", accountDir, register.Round(time.Second), serveWait.Round(time.Second), err)
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
			return fmt.Errorf("wait for file provider domain %s: %w", domain, ctx.Err())
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

// ProbeDomainShallow reports whether the account's File Provider domain lists
// .claude.json WITHOUT any materializing read (domain lookup + readdir only) — a
// cheaper readiness check than ProbeDomain. Zero-spawn; errors.Is classes
// ErrNoDomain, ErrDomainNotServing, ErrBusy, ErrAppUnavailable, ErrOpUnsupported
// flow through for the caller to classify.
func (p *FileProviderProvider) ProbeDomainShallow(ctx context.Context, accountDir string) (bool, error) {
	listed, err := p.host.ProbeDomainShallow(ctx, domainFor(accountDir))
	if err != nil {
		return false, fmt.Errorf("file provider probe domain shallow %s: %w", accountDir, err)
	}
	return listed, nil
}

// PrepareDomain force-materializes the account domain's computed settings.json so
// a live session's first read never blocks on a cold File Provider fetch, bounded
// by deadline (0 = the app's default). Zero-spawn. ErrOpUnsupported (an app too
// old to know the op) is prefixed with the provider's upgradeHint — mirroring
// waitDomainServes — so the operator gets actionable guidance; other classes flow
// through for the caller to classify.
func (p *FileProviderProvider) PrepareDomain(ctx context.Context, accountDir string, deadline time.Duration) error {
	err := p.host.PrepareDomain(ctx, domainFor(accountDir), deadline)
	if errors.Is(err, fileproviderd.ErrOpUnsupported) {
		return fmt.Errorf("file provider prepare domain %s: %s: %w", accountDir, p.upgradeHint, err)
	}
	if err != nil {
		return fmt.Errorf("file provider prepare domain %s: %w", accountDir, err)
	}
	return nil
}

// RemoveDomain deregisters the account's domain WITHOUT retracting the bridge
// symlink (unlike Teardown), spawning the app if needed since domains survive app
// death. An unregistered domain is a no-op.
func (p *FileProviderProvider) RemoveDomain(ctx context.Context, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.host.Remove(ctx, domainFor(accountDir)); err != nil {
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

// Check reports whether the domain is registered and the bridge symlink points at
// its live root. State is a zero-spawn control call. Check never registers, launches,
// signals, or otherwise mutates the overlay.
func (p *FileProviderProvider) Check(ctx context.Context, base, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	domain := domainFor(accountDir)
	root, err := p.host.State(ctx, domain)
	if err != nil {
		return fmt.Errorf("file provider check %s: %w", accountDir, err)
	}
	cur, err := os.Readlink(accountDir)
	if err != nil {
		return fmt.Errorf("file provider check %s: account dir is not the bridge symlink: %w", accountDir, err)
	}
	if cur != root {
		return fmt.Errorf("file provider check %s: bridge symlink points at %q, want the domain root %q", accountDir, cur, root)
	}
	return nil
}

// NotifyContent unconditionally signals the account domain's enumerator. Durable
// change gating belongs to the consumer that commits the content mutation; the
// provider keeps no process-local fingerprint cache.
func (p *FileProviderProvider) NotifyContent(ctx context.Context, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.host.Signal(ctx, domainFor(accountDir)); err != nil {
		return fmt.Errorf("file provider notify content %s: %w", accountDir, err)
	}
	return nil
}

// Signal is the explicit unconditional recovery nudge. It is deliberately separate
// from NotifyContent so recovery code never depends on ordinary change gating.
func (p *FileProviderProvider) Signal(ctx context.Context, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.host.Signal(ctx, domainFor(accountDir)); err != nil {
		return fmt.Errorf("file provider signal %s: %w", accountDir, err)
	}
	return nil
}

// Teardown deregisters the domain, then retracts the bridge symlink, leaving the
// private store in place — Teardown removes the overlay, not the account's
// private state. It never touches base. Ask-before-destroy: the app confirms the
// domain removal BEFORE the symlink comes out, so a failed remove leaves the
// canonical path resolving for live sessions; a real (non-symlink) account dir
// refuses the whole teardown up front, domain included. The warning is always
// empty: FP has no deferred durable state.
func (p *FileProviderProvider) Teardown(ctx context.Context, base, accountDir string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if fi, err := os.Lstat(accountDir); err == nil && fi.Mode()&os.ModeSymlink == 0 {
		return "", fmt.Errorf("file provider teardown %s: account dir is not the bridge symlink; refusing", accountDir)
	}
	if err := p.host.Remove(ctx, domainFor(accountDir)); err != nil {
		return "", fmt.Errorf("file provider teardown %s: %w", accountDir, err)
	}
	if err := fileproviderd.RemoveSymlink(accountDir); err != nil {
		return "", fmt.Errorf("file provider teardown %s: %w", accountDir, err)
	}
	return "", nil
}
