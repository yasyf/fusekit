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
	// BackendFileProvider is the macOS File Provider backend: a signed companion
	// app's NSFileProviderReplicatedExtension surfaces the base dir as a
	// system-supervised domain and the account dir becomes a symlink into the
	// domain root. macOS-only, gated by FileProviderAvailable.
	BackendFileProvider Backend = "fileprovider"
)

// ErrUnknownBackend is returned by Parse for a string that names no valid backend.
var ErrUnknownBackend = errors.New("unknown overlay backend")

// Parse converts a stored backend string to a Backend, failing with
// ErrUnknownBackend (wrapped via %w) for any value that is not a valid backend.
func Parse(s string) (Backend, error) {
	switch Backend(s) {
	case BackendSymlink, BackendNFS, BackendFSKit, BackendFileProvider:
		return Backend(s), nil
	default:
		return "", fmt.Errorf("%w: %q", ErrUnknownBackend, s)
	}
}

// IsFuse reports whether b is one of the fuse-t backends (NFS or FSKit).
func (b Backend) IsFuse() bool {
	return b == BackendNFS || b == BackendFSKit
}

// Available reports whether this machine can realize backend b right now.
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
// realize: BackendFSKit when spec.PassthroughOnly and fuse-t's FSKit backend is
// available here, else BackendNFS. FSKitAvailable is machine-global, so the
// holder derives the same backend with no wire report.
func FuseBackend(spec Spec) Backend {
	if spec.PassthroughOnly && fuset.FSKitAvailable() {
		return BackendFSKit
	}
	return BackendNFS
}

// FileProviderAvailable reports whether the consumer's File Provider extension
// (spec.FileProvider.ExtensionBundleID) is installed and enabled. Spec-routed,
// unlike Backend.Available, because the answer depends on that identifier; a nil
// spec.FileProvider or off-macOS is always false.
func FileProviderAvailable(spec Spec) bool {
	if spec.FileProvider == nil || spec.FileProvider.ExtensionBundleID == "" {
		return false
	}
	return fileProviderEnabled(spec.FileProvider.ExtensionBundleID)
}

// fileProviderEnabled is the pluginkit-on-darwin seam FileProviderAvailable
// consults; a var so tests override it without a real extension.
var fileProviderEnabled = fileProviderEnabledPlatform

// Enablement describes a one-time macOS grant a backend needs before its mounts
// come live.
type Enablement struct {
	// Needed reports whether the backend requires a one-time grant.
	Needed bool
	// Pane is the System Settings location of the grant.
	Pane string
	// Guidance is a sentence telling the user what to grant and why.
	Guidance string
	// URLs are x-apple.systempreferences deep links tried in order to open the
	// pane (the anchor varies across macOS versions, hence the fallbacks).
	URLs []string
}

// networkVolumesPane is the pane for fuse-t NFS's one-time Network Volumes TCC grant.
const networkVolumesPane = "Privacy & Security ▸ Network Volumes"

// networkVolumesSettingsURLs are tried in order: the dedicated anchor, then Files
// & Folders (its home on older macOS), then the Privacy & Security root.
var networkVolumesSettingsURLs = []string{
	"x-apple.systempreferences:com.apple.preference.security?Privacy_NetworkVolumes",
	"x-apple.systempreferences:com.apple.preference.security?Privacy_FilesAndFolders",
	"x-apple.systempreferences:com.apple.preference.security",
}

// fskitExtensionsPane is the pane where fuse-t's FSKit module is enabled.
const fskitExtensionsPane = "General ▸ Login Items & Extensions ▸ Extensions ▸ fuse-t ▸ FSKit Modules"

// fskitExtensionsSettingsURLs are tried in order: the Login-Items deep link, then
// the General root.
var fskitExtensionsSettingsURLs = []string{
	"x-apple.systempreferences:com.apple.LoginItems-Settings.extension",
	"x-apple.systempreferences:com.apple.systempreferences.GeneralSettings",
}

// fileProviderExtensionsPane is the pane where a File Provider extension is enabled.
const fileProviderExtensionsPane = "General ▸ Login Items & Extensions ▸ File Providers"

// fileProviderExtensionsSettingsURLs are tried in order: the Login-Items deep
// link, then the General root.
var fileProviderExtensionsSettingsURLs = []string{
	"x-apple.systempreferences:com.apple.LoginItems-Settings.extension",
	"x-apple.systempreferences:com.apple.systempreferences.GeneralSettings",
}

// Enablement returns the one-time macOS grant backend b needs before its mounts
// come live, or {Needed: false} when it needs none.
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
	case BackendFileProvider:
		return Enablement{
			Needed:   true,
			Pane:     fileProviderExtensionsPane,
			Guidance: "Enable the File Provider extension once in System Settings ▸ General ▸ Login Items & Extensions ▸ File Providers so its domains can come live.",
			URLs:     fileProviderExtensionsSettingsURLs,
		}
	default:
		return Enablement{Needed: false}
	}
}

// OpenSettings opens System Settings to backend b's one-time grant, trying its
// Enablement URLs in order. A no-grant backend (and, off macOS, any backend)
// errors; if every URL fails it wraps the last failure with %w.
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
