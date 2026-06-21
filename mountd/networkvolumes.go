package mountd

import (
	"context"
	"os/exec"
)

// NetworkVolumesSettingsURL opens System Settings > Privacy & Security >
// Network Volumes. The exact anchor can vary across macOS versions, hence the
// fallbacks in OpenNetworkVolumesSettings.
const NetworkVolumesSettingsURL = "x-apple.systempreferences:com.apple.preference.security?Privacy_NetworkVolumes"

// NetworkVolumesTCCService is the TCC service name macOS uses for
// network-volume access.
const NetworkVolumesTCCService = "kTCCServiceSystemPolicyNetworkVolumes"

// networkVolumesSettingsURLs is tried in order: the dedicated Network Volumes
// anchor, then Files & Folders (where it lives on older macOS), then the bare
// Privacy & Security root.
var networkVolumesSettingsURLs = []string{
	NetworkVolumesSettingsURL,
	"x-apple.systempreferences:com.apple.preference.security?Privacy_FilesAndFolders",
	"x-apple.systempreferences:com.apple.preference.security",
}

// openRunner runs the macOS "open" command on a settings URL; overridden in tests.
var openRunner = func(ctx context.Context, url string) error {
	return exec.CommandContext(ctx, "open", url).Run()
}
