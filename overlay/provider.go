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
	// readyTimeout bounds Setup's ProbeDomain poll for the domain to serve; zero
	// means defaultFPReadyTimeout.
	readyTimeout time.Duration
	// upgradeHint is the operator guidance Setup appends when the app is too old to
	// answer probe-domain; never empty for a constructed provider.
	upgradeHint string
}

// defaultFPReadyTimeout bounds Setup's readiness poll when the spec leaves
// ReadyTimeout zero: an appex materialization budget, generous because domain
// add/remove "can take seconds to materialize".
const defaultFPReadyTimeout = 30 * time.Second

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
		},
		bridgeSocket: fp.BridgeSocket,
		readyTimeout: fp.ReadyTimeout,
		upgradeHint:  hint,
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

// Setup registers the domain via the companion app, polls the app (ProbeDomain)
// until the domain actually serves, then makes accountDir a fail-closed symlink into
// the user-visible domain root and seeds the private store dir. It returns nil only
// once the domain served — never cutting an account dir over to a domain that has
// registered but cannot yet answer reads (the pre-readiness cutover that crushed the
// File Provider host under a fleet migrate). Idempotent. AtomicSymlink refuses to
// clobber a real (non-symlink) account dir, so a conversion must drain it
// (MoveSharedOrphans/MovePrivateEntries) before Setup.
func (p *FileProviderProvider) Setup(base, accountDir string) error {
	domain := domainFor(accountDir)
	root, err := p.host.Ensure(context.Background(), domain)
	if err != nil {
		return fmt.Errorf("file provider setup %s: %w", accountDir, err)
	}
	if err := p.waitDomainServes(context.Background(), domain); err != nil {
		return fmt.Errorf("file provider setup %s: %w", accountDir, err)
	}
	if err := fileproviderd.AtomicSymlink(accountDir, root); err != nil {
		return fmt.Errorf("file provider setup %s: %w", accountDir, err)
	}
	if err := os.MkdirAll(p.PrivateRoot(accountDir), 0o700); err != nil {
		return fmt.Errorf("file provider setup %s: seed private store: %w", accountDir, err)
	}
	return nil
}

// waitDomainServes polls the app (ProbeDomain) until the domain serves. ANY
// answered verdict counts as serving, including ".claude.json missing" (a nil
// byte-count) — presence of the file is not the gate, the domain answering is. It
// keeps polling while the app reports the domain unregistered, not-yet-serving, or
// busy (and across a transient app blip), until the deadline, then fails with
// ErrDomainNotServing.
//
// There is NO raw-filesystem read fallback anywhere: an app too old to answer
// probe-domain (ErrOpUnsupported) fails Setup IMMEDIATELY and loudly with the
// operator upgrade hint, because a silent read would resurrect the TCC prompt storm
// this op exists to prevent. The retreat verdict (ErrCannotControl) and any
// unrecognized error also fail immediately rather than being polled away.
func (p *FileProviderProvider) waitDomainServes(ctx context.Context, domain string) error {
	timeout := p.readyTimeout
	if timeout <= 0 {
		timeout = defaultFPReadyTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		_, err := p.host.ProbeDomain(ctx, domain)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, fileproviderd.ErrOpUnsupported):
			return fmt.Errorf("%s: %w", p.upgradeHint, err)
		case errors.Is(err, fileproviderd.ErrCannotControl):
			return err
		case errors.Is(err, fileproviderd.ErrNoDomain),
			errors.Is(err, fileproviderd.ErrDomainNotServing),
			errors.Is(err, fileproviderd.ErrBusy),
			errors.Is(err, fileproviderd.ErrAppUnavailable):
			// Still materializing (or a momentary app blip): keep polling.
		default:
			return err
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
