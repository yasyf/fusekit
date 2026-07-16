package overlay

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/fileproviderd"
	"github.com/yasyf/fusekit/mountd"
)

// ErrAccountDirOccupied means a mux-mode Reconcile found the account dir occupied by
// real state (a non-empty directory or a non-directory file) where it must lay
// the bridge symlink into the account's subtree. Fail closed — the caller drains
// the dir (MoveSharedOrphans/MovePrivateEntries) before retrying — so a live
// account's files are never clobbered. Mirrors the File Provider provider's
// AtomicSymlink clobber guard.
var ErrAccountDirOccupied = errors.New("account dir is occupied by real state")

// muxHealthProbes bounds the per-account subtree lstat in mux-mode Check: the
// stat traverses the shared native NFS mount, which can wedge, and Check sits
// on the daemon poll hot path — a wedged subtree must cost one detached
// goroutine, never a parked poll.
var muxHealthProbes fusekit.StatProbes[bool]

// muxHealthWait bounds one mux-mode subtree lstat. Var, not const, so tests can shrink it.
var muxHealthWait = 2 * time.Second

// muxShapeProbes bounds mux Teardown's account-dir shape lstat: a wedged legacy
// per-dir mount sitting on a REAL account dir serves the mountpoint's own
// getattr, so an unbounded lstat could hang with it. A genuine mux row is a
// bridge symlink, whose lstat touches no mount and always answers — an
// unanswered probe is itself evidence of the legacy shape.
var muxShapeProbes fusekit.StatProbes[muxShape]

// muxShapeWait bounds one teardown shape lstat. Var, not const, so tests can shrink it.
var muxShapeWait = 2 * time.Second

// muxShape is one bounded account-dir lstat verdict.
type muxShape struct {
	fi  os.FileInfo
	err error
}

// RemoteFuseProvider adapts mountd.RemoteHost — the wire/lifecycle half
// driving the detached holder, so mirrors outlive the daemon and CLI — to the
// overlay.Provider interface, adding Backend, PrivateRoot, and a content-serving
// Reconcile. It compiles in every build variant; only the spawn path needs the fuse
// build or the cask ExecPath.
type RemoteFuseProvider struct {
	RemoteHost       *mountd.RemoteHost
	backend          Backend
	contentSocket    string
	contentMode      string
	probePath        string
	privatePrefixes  []string
	attrCache        bool
	attrCacheTimeout time.Duration
	// muxRoot, when set, serves every account as a subtree of ONE native mount at
	// muxRoot and bridges the account dir to its subtree with a fail-closed
	// symlink (the File Provider provider's pattern). Empty keeps the per-account
	// fuse mount.
	muxRoot string
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

// Reconcile establishes a live mirror of base at accountDir: with content wiring,
// a synth-serving mount over RPC (the holder reads synthetic entries off the
// bridge); otherwise mountd.RemoteHost's passthrough Setup, which deliberately does
// not carry AttrCache — a passthrough mirror's base is externally mutable,
// exactly the torn-read case the noattrcache default protects, so the opt-in
// is dropped (the mount serves noattrcache) rather than forwarded. In mux mode
// the account is a subtree of one shared native mount, bridged by a symlink.
func (p *RemoteFuseProvider) Reconcile(ctx context.Context, base, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.muxRoot != "" {
		return p.reconcileMux(ctx, base, accountDir)
	}
	var err error
	if p.contentSocket == "" && p.contentMode == "" {
		err = p.RemoteHost.Setup(base, accountDir)
	} else {
		err = p.RemoteHost.AddMount(fusekit.MountSpec{
			Base:             base,
			Dir:              accountDir,
			Owner:            p.RemoteHost.Owner,
			ContentSocket:    p.contentSocket,
			Domain:           accountDir,
			PrivateRoot:      FusePrivateRoot(accountDir),
			ContentMode:      p.contentMode,
			ProbePath:        p.probePath,
			PrivatePrefixes:  p.privatePrefixes,
			AttrCache:        p.attrCache,
			AttrCacheTimeout: p.attrCacheTimeout,
		})
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

// subtreeDir is an account's path within the shared native mount:
// muxRoot/<basename(accountDir)>. The holder serves it as a logical subtree; the
// account dir bridges to it with a symlink.
func (p *RemoteFuseProvider) subtreeDir(accountDir string) string {
	return filepath.Join(p.muxRoot, filepath.Base(accountDir))
}

// reconcileMux attaches the account as a subtree of the shared native mount, then
// bridges the canonical account dir to that subtree with a fail-closed symlink —
// the account-dir string (hashed byte-for-byte into a Keychain service name)
// stays put; only its inode becomes a link. An EMPTY real account dir is cleared
// first; a non-empty one is refused (ErrAccountDirOccupied) so a live account's
// files are never clobbered — the caller drains it, then retries.
func (p *RemoteFuseProvider) reconcileMux(ctx context.Context, base, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	subtree := p.subtreeDir(accountDir)
	if err := p.RemoteHost.AddMount(fusekit.MountSpec{
		Base:             base,
		Dir:              subtree,
		MuxRoot:          p.muxRoot,
		Owner:            p.RemoteHost.Owner,
		ContentSocket:    p.contentSocket,
		Domain:           accountDir,
		PrivateRoot:      FusePrivateRoot(accountDir),
		ContentMode:      p.contentMode,
		ProbePath:        p.probePath,
		PrivatePrefixes:  p.privatePrefixes,
		AttrCache:        p.attrCache,
		AttrCacheTimeout: p.attrCacheTimeout,
	}); err != nil {
		return fmt.Errorf("fuse mux reconcile %s: %w", accountDir, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := clearAccountDirForBridge(accountDir); err != nil {
		return fmt.Errorf("fuse mux reconcile %s: %w", accountDir, err)
	}
	if err := fileproviderd.AtomicSymlink(accountDir, subtree); err != nil {
		return fmt.Errorf("fuse mux reconcile %s: bridge symlink: %w", accountDir, err)
	}
	return ctx.Err()
}

// Teardown removes the overlay from accountDir, returning the holder's journal
// persist-warning (Provider contract). In mux mode the account dir's lstat
// shape picks the arm, then the subtree detaches via the holder — the last
// child's native unmount is re-verified holder-side against the ROOT (a wedge
// there surfaces as fusekit.ErrUnmountWedged):
//
//   - Bridge symlink or absent: detach FIRST; the symlink is retracted only
//     after the holder confirms. A bounced detach (ErrBusy — a live session's
//     held lease) or any failure must leave the canonical path resolving:
//     unlinking first would ENOENT the session's config dir while its mount
//     lives on. A confirmed detach means no live lease held the subtree, so
//     the moment the symlink dangles before the unlink is unowned.
//   - Real directory: a pre-mux LEGACY row — a per-dir mount (possibly a dead
//     holder's carcass) still sits on the real dir, or nothing is mounted at
//     all. The embedded pre-mux teardown clears it (an unmounted dir is its
//     no-op) and the dir stays in place: there is no bridge symlink to remove.
//     The detach still runs — a holder that never knew this account answers it
//     as a not-mounted no-op, while a half-established attach (setup failed
//     after AddMount, before the bridge) is released rather than leaked.
//   - Regular file: refused fail-closed before any holder contact, so
//     unexplained real state is never disturbed.
//
// The shape lstat is bounded (muxShapeProbes): a bridge symlink's lstat touches
// no mount and always answers, so an unanswered probe reads as the legacy shape
// — a wedged per-dir mount serving the mountpoint's getattr — and routes to the
// pre-mux teardown, whose own probes are bounded. Plain mode is the embedded
// mountd.RemoteHost teardown.
func (p *RemoteFuseProvider) Teardown(ctx context.Context, base, accountDir string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if p.muxRoot == "" {
		return p.RemoteHost.Teardown(base, accountDir)
	}
	sh, answered := muxShapeProbes.Do(accountDir, muxShapeWait, func() muxShape {
		fi, err := os.Lstat(accountDir)
		return muxShape{fi: fi, err: err}
	})
	switch {
	case answered && sh.err != nil && !os.IsNotExist(sh.err):
		return "", fmt.Errorf("fuse mux teardown %s: lstat: %w", accountDir, sh.err)
	case !answered || (sh.err == nil && sh.fi.IsDir()):
		legacyWarn, terr := p.RemoteHost.Teardown(base, accountDir)
		if terr != nil {
			return legacyWarn, fmt.Errorf("fuse mux teardown %s: legacy per-dir mount: %w", accountDir, terr)
		}
		warning, err := p.RemoteHost.RemoveMount(base, p.subtreeDir(accountDir))
		warning = joinWarn(legacyWarn, warning)
		if err != nil {
			return warning, fmt.Errorf("fuse mux teardown %s: %w", accountDir, err)
		}
		return warning, nil
	case sh.err == nil && sh.fi.Mode()&os.ModeSymlink == 0:
		return "", fmt.Errorf("fuse mux teardown %s: account path is a regular file, not the bridge symlink; refusing", accountDir)
	default:
		warning, err := p.RemoteHost.RemoveMount(base, p.subtreeDir(accountDir))
		if err != nil {
			return warning, fmt.Errorf("fuse mux teardown %s: %w", accountDir, err)
		}
		if serr := fileproviderd.RemoveSymlink(accountDir); serr != nil {
			return warning, fmt.Errorf("fuse mux teardown %s: %w", accountDir, serr)
		}
		return warning, nil
	}
}

// joinWarn joins two optional persist-warnings.
func joinWarn(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	}
	return a + "; " + b
}

// Check reports whether the overlay is intact. In mux mode the checks are: the
// bridge symlink points at the account's subtree, the shared native mount is up,
// and the subtree answers a bounded lstat through it (a wedged mount never
// returns, so the stat is bounded and fails toward ErrLivenessTimeout, which the
// caller debounces rather than remounting the whole pool on one blip). Plain
// mode uses RemoteHost health. It never contacts or spawns the holder.
func (p *RemoteFuseProvider) Check(ctx context.Context, base, accountDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.muxRoot == "" {
		return p.RemoteHost.Health(base, accountDir)
	}
	subtree := p.subtreeDir(accountDir)
	cur, err := os.Readlink(accountDir)
	if err != nil {
		return fmt.Errorf("fuse mux check %s: account dir is not the bridge symlink: %w", accountDir, err)
	}
	if cur != subtree {
		return fmt.Errorf("fuse mux check %s: bridge symlink points at %q, want subtree %q", accountDir, cur, subtree)
	}
	if !fusekit.Mounted(p.muxRoot) {
		return fmt.Errorf("fuse mux check %s: mux root %s is not mounted", accountDir, p.muxRoot)
	}
	alive, ok := muxHealthProbes.Do(subtree, muxHealthWait, func() bool {
		_, err := os.Lstat(subtree)
		return err == nil
	})
	if !ok {
		return fmt.Errorf("%w: mux subtree %s did not answer a liveness stat within %s (holder may be saturated)", fusekit.ErrLivenessTimeout, subtree, muxHealthWait)
	}
	if !alive {
		return fmt.Errorf("fuse mux check %s: subtree %s is not visible through the mount", accountDir, subtree)
	}
	return ctx.Err()
}

// clearAccountDirForBridge removes accountDir when it is an EMPTY real directory
// so AtomicSymlink may replace it with the bridge symlink. A symlink or an absent
// path is left for AtomicSymlink to handle. A non-empty dir or a non-directory
// file holds account state and is refused (ErrAccountDirOccupied) so the caller
// drains it before retrying — a live account's files are never clobbered.
func clearAccountDirForBridge(accountDir string) error {
	fi, err := os.Lstat(accountDir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("lstat %q: %w", accountDir, err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return nil // AtomicSymlink swaps a symlink in place
	}
	if !fi.IsDir() {
		return fmt.Errorf("%w: %s is a file, not a directory", ErrAccountDirOccupied, accountDir)
	}
	entries, err := os.ReadDir(accountDir)
	if err != nil {
		return fmt.Errorf("read account dir %q: %w", accountDir, err)
	}
	if len(entries) > 0 {
		return fmt.Errorf("%w: %s holds %d entries", ErrAccountDirOccupied, accountDir, len(entries))
	}
	if err := os.Remove(accountDir); err != nil {
		return fmt.Errorf("remove empty account dir %q: %w", accountDir, err)
	}
	return nil
}

func newRemoteFuse(b Backend, h *HolderSpec) *RemoteFuseProvider {
	return &RemoteFuseProvider{
		RemoteHost: &mountd.RemoteHost{
			Socket:         h.Socket,
			LogPath:        h.LogPath,
			Args:           h.Args,
			CannotHostHint: h.CannotHostHint,
			ExecPath:       h.ExecPath,
			Owner:          h.Owner,
			SpawnTimeout:   h.SpawnTimeout,
		},
		backend:          b,
		contentSocket:    h.BridgeSocket,
		contentMode:      h.ContentMode,
		probePath:        h.ProbePath,
		privatePrefixes:  h.PrivatePrefixes,
		attrCache:        h.AttrCache,
		attrCacheTimeout: h.AttrCacheTimeout,
		muxRoot:          h.MuxRoot,
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
