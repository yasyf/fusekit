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
	// readyTimeout bounds Setup's wait for the domain to serve an enumeration;
	// zero means fileproviderd.DefaultReadyTimeout (WaitDomainServes normalizes it).
	readyTimeout time.Duration
}

var _ Provider = (*FileProviderProvider)(nil)

func newFileProvider(fp *FileProviderSpec) *FileProviderProvider {
	return &FileProviderProvider{
		host: &fileproviderd.RemoteDomainHost{
			AppPath:       fp.AppPath,
			ControlSocket: fp.ControlSocket,
			SpawnTimeout:  fp.SpawnTimeout,
		},
		bridgeSocket: fp.BridgeSocket,
		readyTimeout: fp.ReadyTimeout,
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

// Setup registers the domain via the companion app, waits for it to actually
// serve an enumeration, then makes accountDir a fail-closed symlink into the
// user-visible domain root and seeds the private store dir. It returns nil only
// once the domain served an enumeration — never cutting an account dir over to a
// domain that has registered but cannot yet answer reads (WaitDomainServes).
// Idempotent. AtomicSymlink refuses to clobber a real (non-symlink) account dir, so
// a conversion must drain it (MoveSharedOrphans/MovePrivateEntries) before Setup.
func (p *FileProviderProvider) Setup(base, accountDir string) error {
	domain := domainFor(accountDir)
	root, err := p.host.Ensure(context.Background(), domain)
	if err != nil {
		return fmt.Errorf("file provider setup %s: %w", accountDir, err)
	}
	if err := fileproviderd.WaitDomainServes(root, p.readyTimeout); err != nil {
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
