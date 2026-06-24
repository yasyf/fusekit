//go:build darwin

package overlay

import (
	"context"
	"os/exec"
	"strings"
	"time"
)

// pluginkitTimeout bounds the pluginkit query so a stuck or absent binary cannot
// hang a Select. The query is a cheap local lookup, so the bound is tight.
const pluginkitTimeout = 5 * time.Second

// fileProviderEnabledPlatform reports whether the File Provider extension with
// the given bundle identifier is installed AND enabled, via
// `pluginkit -m -i <bundleID>`. pluginkit prints one match line per registered
// plugin whose first field is a status flag: a leading '+' means enabled, '-'
// means disabled, '!' a problem. No output means the extension is not registered
// at all (not installed). Any error — pluginkit missing, the query timing out —
// reads as unavailable: FP is the preferred backend but never the floor, so an
// undecidable probe falls through to fuse/symlink rather than risking a
// half-working overlay.
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
