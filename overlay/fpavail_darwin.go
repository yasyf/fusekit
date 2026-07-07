//go:build darwin

package overlay

import (
	"context"
	"fmt"
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

// electFileProviderPlatform runs `pluginkit -e use -i <bundleID>`, asking macOS to
// mark the extension as the elected (enabled) File Provider. It shares detection's
// timeout because the election path can wedge the same way a query can. Any exec
// failure is returned wrapped; the post-election enablement re-check and the
// ineffective-election verdict live in TryEnableFileProvider.
func electFileProviderPlatform(bundleID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), pluginkitTimeout)
	defer cancel()
	if err := exec.CommandContext(ctx, "pluginkit", "-e", "use", "-i", bundleID).Run(); err != nil {
		return fmt.Errorf("pluginkit -e use -i %s: %w", bundleID, err)
	}
	return nil
}
