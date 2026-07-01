//go:build darwin

package overlay

import (
	"context"
	"os/exec"
	"strings"
	"time"
)

// pluginkitTimeout bounds the pluginkit query so a stuck or absent binary cannot
// hang a Select.
const pluginkitTimeout = 5 * time.Second

// fileProviderEnabledPlatform reports whether the File Provider extension with
// the given bundle id is installed AND enabled, via `pluginkit -m -i <bundleID>`.
// pluginkit's first output field is a status flag ('+' enabled, '-' disabled,
// '!' a problem); no output means not registered. Any error (pluginkit missing,
// timeout) reads as unavailable: FP is preferred but never the floor, so an
// undecidable probe falls through to fuse/symlink rather than risk a half-working
// overlay.
func fileProviderEnabledPlatform(bundleID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), pluginkitTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "pluginkit", "-m", "-i", bundleID).Output()
	if err != nil {
		return false
	}
	line := strings.TrimSpace(string(out))
	if line == "" {
		return false
	}
	return strings.HasPrefix(line, "+")
}
