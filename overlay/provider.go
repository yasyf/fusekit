// Package overlay makes a per-account dir present the live contents of a shared
// base dir with writes shared straight back, so each tenant sees the same
// projects/settings/etc. as the base. Two interchangeable providers:
//
//   - symlink (default + always-available fallback): symlink each top-level
//     entry of the base into the account dir.
//   - fuse (preferred when fuse-t is installed): a passthrough mirror hosted by
//     a detached mount holder and driven over its socket, so the mirrors outlive
//     the daemon and CLI processes that ask for them.
//
// Both yield the same observable result. A small set of entries is held back
// from sharing because they are instance-local runtime state that would conflict
// across concurrent tenants; the consumer declares those via Spec (IsPrivate,
// Excluded). All consumer-specific classification flows through Spec — the
// package names no consumer's domain entries itself — so the same machinery
// serves any consumer mirroring one base into per-tenant dirs.
//
// Selection is the package's job: Select probes this machine (build capability,
// holder reachability, a holder-side probe mount) and returns the realized
// provider plus a human-readable reason when it falls back to symlinks.
// ProviderFor reconstructs a provider from a stored backend without re-probing.
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
