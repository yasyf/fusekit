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
// Any error (pluginkit missing, timeout) reads as unavailable: FP is preferred
// but never the floor, so an undecidable probe falls through to fuse/symlink
// rather than risk a half-working overlay.
func fileProviderEnabledPlatform(bundleID string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), pluginkitTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "pluginkit", "-m", "-i", bundleID).Output()
	if err != nil {
		return false
	}
	return pluginkitElected(string(out))
}

// pluginkitElected parses `pluginkit -m` output: one registered copy per line,
// each prefixed by a status flag ('+' enabled, '-' disabled, '!' a problem);
// empty output means not registered. pluginkit lists stale duplicates (e.g. an
// old app copy in the Trash) alongside the live one, so an election on ANY line
// wins — a stale disabled duplicate listed first must not mask a live election.
func pluginkitElected(out string) bool {
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "+") {
			return true
		}
	}
	return false
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
