package overlay

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/fuset"
)

// Backend identifies which overlay mechanism realizes an account's view of the
// shared base dir.
type Backend string

// The three valid backends. There is no legacy "fuse" value: a fuse overlay is
// always one of the two concrete fuse-t backends (NFS or FSKit), never an
// abstract "fuse".
const (
	// BackendSymlink symlinks each top-level base entry into the account dir.
	// Always available.
	BackendSymlink Backend = "symlink"
	// BackendNFS is fuse-t's default NFS backend — honors libfuse fi->fh read
	// semantics, so it is the safe choice for a filesystem that serves any
	// synthetic content.
	BackendNFS Backend = "nfs"
	// BackendFSKit is fuse-t's FSKit backend (macOS 26+). It does NOT preserve
	// fi->fh, so only a pure-passthrough filesystem may use it.
	BackendFSKit Backend = "fskit"
)

// ErrUnknownBackend is returned by Parse for any string that is not one of the
// three valid backends, including the legacy "fuse" value, which no longer
// names a concrete backend.
var ErrUnknownBackend = errors.New("unknown overlay backend")

// Parse converts a stored backend string to a Backend, accepting only the three
// valid values. Any other string — including the legacy "fuse" — fails loudly
// with ErrUnknownBackend wrapped via %w.
func Parse(s string) (Backend, error) {
	switch Backend(s) {
	case BackendSymlink, BackendNFS, BackendFSKit:
		return Backend(s), nil
	default:
		return "", fmt.Errorf("%w: %q", ErrUnknownBackend, s)
	}
}

// IsFuse reports whether b is one of the fuse-t backends (NFS or FSKit) rather
// than the symlink backend.
func (b Backend) IsFuse() bool {
	return b == BackendNFS || b == BackendFSKit
}

// Available reports whether this machine can realize backend b right now:
// symlink is always available; nfs needs fuse-t installed; fskit needs fuse-t's
// FSKit backend (macOS 26+ with the module bundle present).
func (b Backend) Available() bool {
	switch b {
	case BackendSymlink:
		return true
	case BackendNFS:
		return fuset.Installed()
	case BackendFSKit:
		return fuset.FSKitAvailable()
	default:
		return false
	}
}

// FuseBackend reports the fuse backend this machine plus filesystem would
// realize: BackendFSKit when the consumer's filesystem is pure passthrough AND
// fuse-t's FSKit backend is available here, else BackendNFS. It is a local
// derivation — FSKitAvailable is machine-global, so no wire reporting is needed.
func FuseBackend(spec Spec) Backend {
	if spec.PassthroughOnly && fuset.FSKitAvailable() {
		return BackendFSKit
	}
	return BackendNFS
}

// Enablement describes a one-time macOS grant a fuse backend needs before its
// mounts come live. Needed is false for backends that require no grant (symlink).
type Enablement struct {
	// Needed reports whether the backend requires a one-time grant before mounts
	// come live.
	Needed bool
	// Pane is the human-readable System Settings location of the grant.
	Pane string
	// Guidance is a clear sentence telling the user what to grant and why.
	Guidance string
	// URLs are x-apple.systempreferences deep links tried in order to open the
	// pane (the anchor varies across macOS versions, hence the fallbacks).
	URLs []string
}

// networkVolumesPane names the macOS pane holding the one-time Network Volumes
// TCC grant fuse-t's NFS backend needs.
const networkVolumesPane = "Privacy & Security ▸ Network Volumes"

// networkVolumesSettingsURLs is tried in order to open the Network Volumes
// grant: the dedicated anchor, then Files & Folders (where it lives on older
// macOS), then the bare Privacy & Security root.
var networkVolumesSettingsURLs = []string{
	"x-apple.systempreferences:com.apple.preference.security?Privacy_NetworkVolumes",
	"x-apple.systempreferences:com.apple.preference.security?Privacy_FilesAndFolders",
	"x-apple.systempreferences:com.apple.preference.security",
}

// fskitExtensionsPane names the macOS pane where fuse-t's FSKit module is
// enabled as a login-item extension.
const fskitExtensionsPane = "General ▸ Login Items & Extensions ▸ Extensions ▸ fuse-t ▸ FSKit Modules"

// fskitExtensionsSettingsURLs is tried in order to open the FSKit module's
// enable pane: the Extensions/Login-Items deep link, then the bare General root.
var fskitExtensionsSettingsURLs = []string{
	"x-apple.systempreferences:com.apple.LoginItems-Settings.extension",
	"x-apple.systempreferences:com.apple.systempreferences.GeneralSettings",
}

// Enablement returns the one-time macOS grant backend b needs before its mounts
// come live: the Network Volumes TCC grant for nfs, the FSKit module enable for
// fskit, and {Needed: false} for symlink (no grant).
func (b Backend) Enablement() Enablement {
	switch b {
	case BackendNFS:
		return Enablement{
			Needed:   true,
			Pane:     networkVolumesPane,
			Guidance: "Grant Network Volumes access once in System Settings ▸ Privacy & Security so fuse-t's mounts can come live; the grant persists for every later mount.",
			URLs:     networkVolumesSettingsURLs,
		}
	case BackendFSKit:
		return Enablement{
			Needed:   true,
			Pane:     fskitExtensionsPane,
			Guidance: "Enable the fuse-t FSKit module once in System Settings ▸ General ▸ Login Items & Extensions ▸ Extensions so its mounts can come live.",
			URLs:     fskitExtensionsSettingsURLs,
		}
	default:
		return Enablement{Needed: false}
	}
}

// OpenSettings opens the System Settings pane for backend b's one-time grant,
// trying its Enablement URLs in order and returning nil on the first success.
// A backend with no grant (symlink) errors; off macOS every backend errors. If
// every URL fails it wraps the last failure with %w.
func (b Backend) OpenSettings(ctx context.Context) error {
	en := b.Enablement()
	if !en.Needed {
		return fmt.Errorf("backend %q needs no settings grant", b)
	}
	var lastErr error
	for _, url := range en.URLs {
		if err := openRunner(ctx, url); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return fmt.Errorf("could not open any %s settings pane: %w", en.Pane, lastErr)
}
