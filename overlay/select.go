package overlay

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/mountd"
)

// RemoteFuseProvider adapts mountd.RemoteHost — the embedded wire/lifecycle half
// driving the detached holder, so mirrors outlive the daemon and CLI — to the
// overlay.Provider interface, adding Backend, PrivateRoot, and a content-serving
// Setup. It compiles in every build variant; only the spawn path needs the fuse
// build or the cask ExecPath.
type RemoteFuseProvider struct {
	*mountd.RemoteHost
	backend         Backend
	contentSocket   string
	contentMode     string
	probePath       string
	privatePrefixes []string
}

var _ Provider = (*RemoteFuseProvider)(nil)

// Backend reports the fuse backend this provider stands in for (nfs or fskit),
// regardless of whether this build could host the mounts, so stored-backend
// fences stay honest.
func (p *RemoteFuseProvider) Backend() Backend { return p.backend }

// PrivateRoot returns the fuse provider's per-account private backing dir.
func (p *RemoteFuseProvider) PrivateRoot(accountDir string) string {
	return FusePrivateRoot(accountDir)
}

// Setup establishes a live mirror of base at accountDir: with content wiring, a
// synth-serving mount over RPC (the holder reads synthetic entries off the
// bridge); otherwise the embedded passthrough Setup.
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

// ProviderFor returns the provider for a stored backend, never silently
// substituting one for another: a fuse backend with a nil spec.Holder is a
// configuration error and fails loudly.
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

// holderCanSpawn reports whether Select can bring a holder up for this spec: a
// cask ExecPath makes even a pure build host-capable, so only a spec with neither
// an ExecPath nor a fuse build short-circuits to symlink without probing. A nil
// Holder disables fuse selection.
func holderCanSpawn(h *HolderSpec) bool {
	return h != nil && (h.ExecPath != "" || fusekit.Built())
}

// Select chooses the backend for this machine and returns its provider, in
// preference order File Provider > fuse > symlink.
//
// File Provider is tried first when wired and available, gated on a throwaway
// probe domain; unlike a fuse mount it needs no per-process macOS grant in THIS
// process (the entitlement lives in the app's signature), and unlike fskit it is
// not gated on PassthroughOnly (it serves synthetic content over the bridge). A
// probe that does not confirm capability falls through to the fuse→symlink ladder
// rather than failing — FP is preferred, never the floor.
//
// The fuse arm needs a build that can host mounts, a reachable holder
// (auto-spawned), and a succeeding probe mount; else symlink. A build that cannot
// host (fusekit.Built()==false) or a spec with no Holder gets symlink without
// probing — a reachable leftover holder is deliberately not adopted, because the
// recorded default must survive that holder's death. The probe MUST run in the
// holder: mount capability and the macOS grant are per-process.
//
// On a fuse verdict the realized backend is FuseBackend(spec). The returned string
// is a human-readable symlink-fallback reason (empty on an FP or fuse verdict); it
// names the System Settings pane for a pending grant but carries no consumer CLI
// commands — the consumer adds those at its edge.
func Select(ctx context.Context, spec Spec) (Provider, Backend, string, error) {
	if spec.FileProvider != nil && FileProviderAvailable(spec) {
		fp := newFileProvider(spec.FileProvider)
		if ok, err := fp.host.Probe(ctx); err == nil && ok {
			return fp, BackendFileProvider, "", nil
		}
		// Not fatal: fall through, so the symlink reason (if it lands there) names
		// the fuse cause.
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
		// Load-bearing: a cask ExecPath is what makes a pure build host-capable
		// (holderCanSpawn passed on it). Drop it and Spawn.canHost takes the
		// fusekit.Built() branch, so a pure build with the cask wrongly refuses
		// (ErrCannotHost) instead of launching the .app via `open -g`.
		ExecPath: h.ExecPath,
	}).EnsureRunning(); err != nil {
		return &SymlinkProvider{Spec: spec}, BackendSymlink, fmt.Sprintf("mount holder did not start: %v", err), nil
	}
	fuse := FuseBackend(spec)
	ok, err := mountd.NewClient(h.Socket).Probe()
	switch {
	case errors.Is(err, mountd.ErrMountFailed):
		// Hard mount(2) rejection: fuse-t cannot mount here (missing/unloadable, or
		// the kernel refusing). NOT the grant — don't send the user to a settings
		// toggle that won't help; the real cause is in the mount-holder log.
		return &SymlinkProvider{Spec: spec}, BackendSymlink, fmt.Sprintf("fuse-t cannot mount on this machine (%v); using symlinks", err), nil
	case errors.Is(err, mountd.ErrTCCDenied):
		// Blocked PENDING the backend's one-time macOS grant (a prompt should have
		// appeared); name the pane so the consumer can route the user there.
		return &SymlinkProvider{Spec: spec}, BackendSymlink, fmt.Sprintf("the macOS grant in %s is not in place (%v); using symlinks — grant it, then re-select", fuse.Enablement().Pane, err), nil
	case err != nil:
		return &SymlinkProvider{Spec: spec}, BackendSymlink, fmt.Sprintf("mount holder probe failed: %v", err), nil
	case !ok:
		return &SymlinkProvider{Spec: spec}, BackendSymlink, "probe mount declined by the holder", nil
	}
	return newRemoteFuse(fuse, h), fuse, "", nil
}
