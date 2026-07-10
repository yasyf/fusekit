package overlay

import (
	"strings"
	"time"
)

// Spec is the per-consumer classification and wiring that drives a symlink or
// fuse overlay — the only place consumer domain knowledge enters the package.
// All classification flows from its predicates and sets, so the package names no
// consumer-specific entry itself.
type Spec struct {
	// IsPrivate reports whether a top-level entry is per-account private: never
	// symlinked, kept account-local in the private root. Must return true for
	// every Excluded name. Required.
	IsPrivate func(name string) bool

	// Excluded are top-level entries never shared across accounts; each becomes a
	// private, empty per-account directory instead.
	Excluded map[string]bool

	// Shared are top-level entries shared across all accounts even when the base
	// dir lacks them yet: materialized in the base and linked, so a lazily-written
	// entry is born shared instead of scattering per-account. Disjoint from
	// Excluded and the IsPrivate set.
	Shared map[string]bool

	// Skip are top-level entries never linked, mirrored, or moved — OS cruft the
	// consumer wants ignored.
	Skip map[string]bool

	// SkipPrefixes are name prefixes skipped exactly like Skip entries: a
	// top-level entry whose name begins with any of them is never linked,
	// mirrored, or moved (the motivating case: AppleDouble "._" litter).
	// Prefixes must be non-empty — an empty prefix would match every name.
	SkipPrefixes []string

	// PassthroughOnly declares whether the consumer's fuse filesystem serves ONLY
	// real backing files (no synthetic content). It drives FuseBackend's choice:
	// true picks fuse-t's FSKit backend when available, false (the safe default)
	// forces NFS, which honors the fi->fh read semantics synthetic-content
	// handlers need.
	PassthroughOnly bool

	// Holder wires the detached fuse mount holder. Nil disables fuse selection:
	// Select returns the symlink provider, ProviderFor errors if a fuse backend
	// is requested.
	Holder *HolderSpec

	// FileProvider wires the macOS File Provider backend: the signed companion app
	// hosting the NSFileProviderReplicatedExtension and the two sockets the daemon
	// drives it over. Nil disables FP selection (as Holder == nil does for fuse):
	// Select skips the FP arm, ProviderFor errors if BackendFileProvider is
	// requested.
	FileProvider *FileProviderSpec
}

// Skipped reports whether a top-level entry name is ignored by the overlay:
// set in Skip, or beginning with one of SkipPrefixes. Prefixes must be
// non-empty — an empty prefix would match every name.
func (s *Spec) Skipped(name string) bool {
	if s.Skip[name] {
		return true
	}
	for _, p := range s.SkipPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// FileProviderSpec is the consumer's wiring for the macOS File Provider backend,
// the File-Provider analog of HolderSpec: ProviderFor builds the
// fileproviderd.RemoteDomainHost from it.
type FileProviderSpec struct {
	// AppPath is the signed companion app bundle path, passed to `open -g` to
	// bring the control listener up. Required.
	AppPath string
	// ControlSocket is the companion app's control socket — where the app serves
	// Register/Path/Signal/Remove/Probe. Required.
	ControlSocket string
	// BridgeSocket is the data socket the daemon's BridgeServer binds and the
	// sandboxed extension calls for computed content; the provider only carries
	// the path (the daemon binds it).
	BridgeSocket string
	// ExtensionBundleID is the File Provider extension's bundle identifier, handed
	// to `pluginkit -m -i <id>` by FileProviderAvailable to read whether the
	// extension is installed and enabled.
	ExtensionBundleID string
	// AppGroup is the App-Group container the host app and sandboxed extension
	// share (the BridgeSocket typically lives inside it). Carried wiring; the
	// overlay package does not resolve it.
	AppGroup string
	// SpawnTimeout bounds waiting for a freshly launched companion app's control
	// socket. Zero means fileproviderd.DefaultSpawnTimeout.
	SpawnTimeout time.Duration
	// LaunchTimeout bounds the `open -g` launch of the companion app itself, distinct
	// from SpawnTimeout's socket wait. Zero means the fileproviderd default (30s).
	LaunchTimeout time.Duration
	// ReadyTimeout is Setup's serve budget: how long, from the app's first answer, a
	// freshly registered domain may take to serve a read before Setup cuts the account
	// dir over. Zero means a generous default sized for a migrate-storm cold start. A
	// domain that never serves fails Setup with fileproviderd.ErrDomainNotServing.
	ReadyTimeout time.Duration
	// AppReadyTimeout is Setup's contact budget: how long Setup waits for the app to
	// first answer a probe at all (past fileproviderd.ErrAppUnavailable) before the
	// serve budget starts. Zero means a generous default. Kept separate so a slow app
	// spawn never eats the domain's materialization time.
	AppReadyTimeout time.Duration
	// UpgradeHint is the operator-facing guidance Setup appends when the companion
	// app is too old to answer probe-domain (fileproviderd.ErrOpUnsupported), the
	// File-Provider analog of HolderSpec.CannotHostHint — e.g. "upgrade the
	// cc-pool-status cask". Empty falls back to a generic upgrade message.
	UpgradeHint string
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
	// StableExecDir, when non-empty, materializes the holder binary as a copy
	// under this dir and spawns from there, giving a stable resolved path so the
	// macOS "Network Volumes" TCC grant survives version upgrades. Empty uses the
	// os.Executable() default.
	StableExecDir string
	// ExecPath points the holder spawn at the cask binary (mountd.HolderExe).
	ExecPath string
	Owner    string
	// CannotHostHint is the user-facing guidance appended to the pure-build
	// refusal (the consumer's install/enable text).
	CannotHostHint string
	// BridgeSocket, when set, makes the fuse provider register CONTENT mounts:
	// every Setup carries this socket (the consumer's content.BridgeServer data
	// socket) so the holder serves synthetic entries over RPC, forwarded into
	// MountSpec.ContentSocket. The daemon binds it, not the provider. Empty leaves
	// Setup a plain passthrough.
	BridgeSocket string
	// ContentMode selects the holder filesystem for content mounts: "source"
	// mirrors the local base with synth entries served over the bridge. Empty (with
	// no BridgeSocket) is a passthrough mount.
	ContentMode string
	// MuxRoot, when set, serves every account as a logical subtree of ONE native
	// mount at this path (mnt/<basename(accountDir)>) instead of mounting each
	// account dir itself; the account dir becomes a fail-closed bridge symlink into
	// its subtree. All accounts of one holder share the same MuxRoot (and its
	// AttrCache options). Empty keeps today's per-account fuse mount. Forwarded into
	// MountSpec.MuxRoot.
	MuxRoot string
	// ProbePath is the virtual wedge-probe file the holder serves; empty serves
	// none.
	ProbePath string
	// PrivatePrefixes route top-level names equal-to-or-prefixed-by one of them to
	// the per-mount private root rather than base ("source" mode): the consumer's
	// atomic-write temp siblings of its private/synth files.
	PrivatePrefixes []string
	// AttrCache opts every content mount this holder serves into the go-nfsv4
	// server-side attribute cache (default false = noattrcache). Forwarded through
	// MountSpec.AttrCache into MountOptions; sound ONLY when the served filesystem
	// stabilizes its attributes (see fusekit.MountOptions.AttrCache). Content
	// mounts only: without content wiring Setup takes the legacy passthrough path,
	// which drops the opt-in and serves noattrcache — passthrough content is
	// externally mutable, the exact torn-read case the default protects. DEFAULT OFF.
	AttrCache bool
	// AttrCacheTimeout sets the go-nfsv4 attr-cache TTL when AttrCache is true;
	// zero leaves fuse-t's default (whole seconds; see MountOptions.AttrCacheTimeout).
	AttrCacheTimeout time.Duration
	// IdlePolicy tells a self-retiring holder how to prove this consumer's
	// mounts idle before draining them (fusekit.MountSpec.IdlePolicy): "attest"
	// — also the meaning of empty, fail-closed — requires a fresh consumer
	// AttestIdle covering the dir; "probe" attempts a graceful unmount and
	// treats EBUSY as busy. Forwarded into MountSpec.IdlePolicy.
	IdlePolicy string
	// CarcassPolicy tells the holder how to treat a dead-mount carcass at this
	// consumer's kernel roots (fusekit.MountSpec.CarcassPolicy): "force" — also
	// the meaning of empty, every spec's prior behavior — force-clears it;
	// "defer" forbids every autonomous force-unmount, leaving the carcass
	// surfaced for the consumer, who holds the live-session knowledge the
	// holder lacks — a forced unmount of a live mount panics the Apple NFS
	// kext. Rides every subtree spec in mux mode, so one deferring account
	// defers the shared MuxRoot. Forwarded into MountSpec.CarcassPolicy.
	CarcassPolicy string
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
