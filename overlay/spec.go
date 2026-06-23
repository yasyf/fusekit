package overlay

import "time"

// Spec is the per-consumer classification and wiring the overlay package needs
// to drive a symlink or fuse overlay. It is the ONLY place a consumer's domain
// knowledge enters the package: which top-level entries are per-account private,
// which are excluded empty dirs, which are always-materialized shared dirs, and
// which are skipped as noise. The package itself names no consumer-specific
// entry — all classification flows from these four predicates/sets — so the same
// overlay machinery serves any consumer that mirrors one base dir into per-tenant
// dirs.
type Spec struct {
	// IsPrivate reports whether a top-level entry name is per-account private:
	// never symlinked into an account, kept account-local in the private root.
	// A consumer injects its identity/state/credential names here (e.g.
	// cc-pool's .claude.json* / .credentials.json* / .last-update-result* /
	// remote-settings.json* plus the Excluded dirs — IsPrivate must return true
	// for every Excluded name as well, since the migration primitives treat the
	// excluded dirs as private state). Required.
	IsPrivate func(name string) bool

	// Excluded are top-level entries that must NOT be shared across accounts;
	// each becomes a private, empty per-account directory instead (cc-pool:
	// daemon/ide/backups). Every Excluded name must also satisfy IsPrivate.
	Excluded map[string]bool

	// Shared are top-level entries that must be shared across all accounts even
	// when the base dir does not contain them yet — they are materialized in the
	// base and linked, so a lazily-written entry is born shared rather than
	// scattering per-account (cc-pool: plans). Disjoint from Excluded and the
	// IsPrivate set.
	Shared map[string]bool

	// Skip are top-level entries never linked, mirrored, or moved — OS cruft
	// (cc-pool: .DS_Store).
	Skip map[string]bool

	// PassthroughOnly declares whether the consumer's fuse filesystem serves ONLY
	// real backing files (no synthetic, handler-generated content). It drives
	// FuseBackend's choice between fuse-t's FSKit backend (true, when available)
	// and its NFS backend (false — the safe default; cc-pool's merged mirror sets
	// false so it always lands on NFS, which honors fi->fh read semantics).
	PassthroughOnly bool

	// Holder wires the detached fuse mount holder. A nil Holder disables fuse
	// selection entirely: Select returns the symlink provider, and ProviderFor
	// errors if a fuse backend is requested.
	Holder *HolderSpec
}

// HolderSpec is the consumer's wiring for the detached fuse mount holder — the
// process that hosts fuse-t mounts so they outlive daemon and CLI restarts. It
// maps one-to-one onto mountd.RemoteHost's fields; ProviderFor builds the
// RemoteHost from it.
type HolderSpec struct {
	// Socket is the holder's unix socket path.
	Socket string
	// LogPath receives a spawned holder's stdout and stderr.
	LogPath string
	// StableExecDir, when non-empty, makes the holder binary materialize as a
	// copy under this directory and spawn from there, giving the holder a stable
	// resolved path so the macOS "Network Volumes" TCC grant survives version
	// upgrades. Empty preserves the os.Executable() default.
	StableExecDir string
	// CannotHostHint is the user-facing guidance appended to the pure-build
	// refusal (the consumer's install/enable text).
	CannotHostHint string
	// Version is the consumer's wire version reported through the holder's health
	// op; empty disables Converge.
	Version string
	// Args is the holder argv (e.g. ["mount-holder", "--socket", socket]); the
	// consumer owns the subcommand name and flag spelling.
	Args []string
	// SpawnTimeout bounds waiting for a freshly spawned holder's socket. Zero
	// means mountd.DefaultSpawnTimeout.
	SpawnTimeout time.Duration
}
