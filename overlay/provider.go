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

// Provider establishes and maintains an overlay of base at accountDir.
type Provider interface {
	// Backend reports which backend this provider realizes.
	Backend() Backend

	// Setup makes accountDir reflect base. Idempotent.
	Setup(base, accountDir string) error

	// Sync re-asserts the overlay, picking up new top-level entries in base
	// and repairing drift. Idempotent. Safe to call repeatedly.
	Sync(base, accountDir string) error

	// Health returns nil if the overlay is intact, else a descriptive error.
	Health(base, accountDir string) error

	// Teardown removes the overlay from accountDir. It must never touch base.
	Teardown(base, accountDir string) error

	// PrivateRoot returns the directory where account-local (private) files
	// physically live. For the symlink provider that is accountDir itself; for
	// fuse it is the private backing dir beside the mountpoint. Writing there
	// is correct whether or not a mount is currently up.
	PrivateRoot(accountDir string) string
}
