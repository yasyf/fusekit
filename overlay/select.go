package overlay

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/mountd"
)

// RemoteFuseProvider adapts fusekit's mountd.RemoteHost to the overlay.Provider
// interface. RemoteHost is the wire/lifecycle half (Teardown/Sync/Health,
// inherited via embedding) that drives the detached mount holder over its
// socket, so the mirrors outlive the daemon and CLI processes that ask for them.
// This adapter adds the overlay-specific Backend and PrivateRoot, and — when the
// consumer wires content — a Setup that registers a content mount (the holder
// serves the consumer's synthetic entries over the bridge) rather than a plain
// passthrough. It compiles in every build variant: a running holder is usable by
// any build, and only the spawn path needs the fuse build (or the cask ExecPath).
type RemoteFuseProvider struct {
	*mountd.RemoteHost
	backend Backend
	// content carries the consumer's bridge wiring; when contentSocket or
	// contentMode is set, Setup registers a content mount over RPC.
	contentSocket   string
	contentMode     string
	probePath       string
	privatePrefixes []string
}

var _ Provider = (*RemoteFuseProvider)(nil)

// Backend reports the fuse backend this provider stands in for (nfs or fskit),
// regardless of whether this build could host the mounts itself, so every
// stored-backend fence stays honest.
func (p *RemoteFuseProvider) Backend() Backend { return p.backend }

// PrivateRoot returns the fuse provider's per-account private backing dir.
func (p *RemoteFuseProvider) PrivateRoot(accountDir string) string {
	return FusePrivateRoot(accountDir)
}

// Setup establishes a live mirror of base at accountDir. With content wiring it
// registers a synth-serving mount over RPC (AddMount), carrying the consumer's
// bridge socket, this mount's domain (the account dir) and private root, and the
// content mode/probe/prefixes; the holder reads the consumer's synthetic entries
// off its bridge. Without content wiring it is the embedded passthrough Setup.
func (p *RemoteFuseProvider) Setup(base, accountDir string) error {
	if p.contentSocket == "" && p.contentMode == "" {
		return p.RemoteHost.Setup(base, accountDir)
	}
	return p.RemoteHost.AddMount(fusekit.MountSpec{
		Base:            base,
		Dir:             accountDir,
		Owner:           p.RemoteHost.Owner,
		ContentSocket:   p.contentSocket,
		Domain:          accountDir,
		PrivateRoot:     FusePrivateRoot(accountDir),
		ContentMode:     p.contentMode,
		ProbePath:       p.probePath,
		PrivatePrefixes: p.privatePrefixes,
	})
}

// newRemoteFuse builds the holder-backed fuse provider for backend b from the
// consumer's HolderSpec, carrying the holder argv, install hint, stable exec dir
// or external cask ExecPath, wire version, and the content bridge wiring.
func newRemoteFuse(b Backend, h *HolderSpec) *RemoteFuseProvider {
	return &RemoteFuseProvider{
		RemoteHost: &mountd.RemoteHost{
			Socket:         h.Socket,
			LogPath:        h.LogPath,
			Args:           h.Args,
			CannotHostHint: h.CannotHostHint,
			StableExecDir:  h.StableExecDir,
			ExecPath:       h.ExecPath,
			Owner:          h.Owner,
			Version:        h.Version,
			SpawnTimeout:   h.SpawnTimeout,
		},
		backend:         b,
		contentSocket:   h.BridgeSocket,
		contentMode:     h.ContentMode,
		probePath:       h.ProbePath,
		privatePrefixes: h.PrivatePrefixes,
	}
}

// ProviderFor returns the provider for a stored backend. It never silently
// substitutes backends: BackendSymlink maps to the symlink provider; a fuse
// backend maps to the holder-backed RemoteFuseProvider (which always reports its
// own backend, even in a build that could not host the mounts itself). A fuse
// backend with a nil spec.Holder is a configuration error — fuse selection is
// disabled — and fails loudly.
func ProviderFor(b Backend, spec Spec) (Provider, error) {
	switch b {
	case BackendSymlink:
		return &SymlinkProvider{Spec: spec}, nil
	case BackendNFS, BackendFSKit:
		if spec.Holder == nil {
			return nil, fmt.Errorf("backend %q requires a holder, but spec.Holder is nil", b)
		}
		return newRemoteFuse(b, spec.Holder), nil
	case BackendFileProvider:
		if spec.FileProvider == nil {
			return nil, fmt.Errorf("backend %q requires file provider wiring, but spec.FileProvider is nil", b)
		}
		return newFileProvider(spec.FileProvider), nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownBackend, b)
	}
}

// holderCanSpawn reports whether Select can bring a holder up for this spec. A
// cask ExecPath makes even a pure build host-capable — mountd.Spawn.canHost gates
// the spawn on the cask binary existing, not on fusekit.Built() — so only a spec
// with neither an ExecPath nor a fuse build short-circuits to symlink without
// probing. A nil Holder disables fuse selection entirely.
func holderCanSpawn(h *HolderSpec) bool {
	return h != nil && (h.ExecPath != "" || fusekit.Built())
}

// Select chooses the backend for this machine and returns its provider. The
// preference order is File Provider > fuse > symlink. File Provider is tried
// first when it is wired (spec.FileProvider != nil) and available
// (FileProviderAvailable), gated on a throwaway probe domain that registers and
// enumerates cleanly inside the signed companion app — the capability proof,
// which (unlike a fuse mount) needs no per-process macOS grant in THIS process,
// since the entitlement lives in the app's signature. Unlike fskit, FP is NOT
// gated on PassthroughOnly: it serves synthetic content over the bridge. A probe
// that fails to confirm capability falls through to the fuse→symlink ladder
// below rather than failing — FP is the preferred backend, never the floor.
//
// The fuse arm is unchanged: a fuse backend when this build can host fuse mounts,
// a mount holder is reachable (auto-spawned), and the holder's probe mount
// succeeds; else symlink. A build that cannot host mounts (fusekit.Built()==false),
// or a spec with no Holder wiring, gets the symlink verdict without probing —
// even a reachable leftover holder is deliberately not adopted, because the
// recorded default must survive that holder's death. The probe MUST run in the
// holder, not here: mount capability and the macOS grant are per-process, and the
// holder is the process that will host the mounts.
//
// On a fuse verdict the realized backend is FuseBackend(spec) (fskit when
// passthrough-only and available, else nfs). The returned string is a
// human-readable reason for a symlink fallback (empty on an FP or fuse verdict);
// it names the relevant System Settings pane when a pending grant is the cause
// but carries no consumer-specific CLI commands — the consumer adds those at its
// edge.
func Select(ctx context.Context, spec Spec) (Provider, Backend, string, error) {
	if spec.FileProvider != nil && FileProviderAvailable(spec) {
		fp := newFileProvider(spec.FileProvider)
		if ok, err := fp.host.Probe(ctx); err == nil && ok {
			return fp, BackendFileProvider, "", nil
		}
		// A probe that did not confirm capability (the app unreachable, the
		// entitlement refused, or the throwaway domain failing to enumerate) is not
		// fatal: FP is preferred, not required, so fall through to the fuse→symlink
		// ladder. The final symlink reason, if it lands there, names the fuse cause.
	}
	if !holderCanSpawn(spec.Holder) {
		return &SymlinkProvider{Spec: spec}, BackendSymlink, "this build cannot host fuse mounts", nil
	}
	h := spec.Holder
	if err := (mountd.Spawn{
		Socket:         h.Socket,
		LogPath:        h.LogPath,
		Args:           h.Args,
		Timeout:        h.SpawnTimeout,
		CannotHostHint: h.CannotHostHint,
		StableExecDir:  h.StableExecDir,
		// ExecPath is load-bearing: a cask ExecPath is what makes a pure build
		// host-capable (holderCanSpawn passed on it). Dropping it here sends
		// Spawn.canHost down the fusekit.Built() branch, so a pure build with the
		// cask installed wrongly refuses with ErrCannotHost instead of launching
		// the .app via `open -g`. (Masked whenever the holder is already serving,
		// since EnsureRunning short-circuits on Available before canHost.)
		ExecPath: h.ExecPath,
	}).EnsureRunning(); err != nil {
		return &SymlinkProvider{Spec: spec}, BackendSymlink, fmt.Sprintf("mount holder did not start: %v", err), nil
	}
	fuse := FuseBackend(spec)
	ok, err := mountd.NewClient(h.Socket).Probe()
	switch {
	case errors.Is(err, mountd.ErrMountFailed):
		// A hard mount(2) rejection: fuse-t cannot mount on this machine
		// (missing/unloadable, or the kernel refusing it). NOT the grant — do not
		// send the user chasing a settings toggle that will not help. The real
		// cause is in the mount-holder log.
		return &SymlinkProvider{Spec: spec}, BackendSymlink, fmt.Sprintf("fuse-t cannot mount on this machine (%v); using symlinks", err), nil
	case errors.Is(err, mountd.ErrTCCDenied):
		// The probe is blocked PENDING the backend's one-time macOS grant — a
		// prompt should have appeared. Name the pane so the consumer can route the
		// user there.
		return &SymlinkProvider{Spec: spec}, BackendSymlink, fmt.Sprintf("the macOS grant in %s is not in place (%v); using symlinks — grant it, then re-select", fuse.Enablement().Pane, err), nil
	case err != nil:
		return &SymlinkProvider{Spec: spec}, BackendSymlink, fmt.Sprintf("mount holder probe failed: %v", err), nil
	case !ok:
		return &SymlinkProvider{Spec: spec}, BackendSymlink, "probe mount declined by the holder", nil
	}
	return newRemoteFuse(fuse, h), fuse, "", nil
}
