//go:build darwin

package proc

import (
	"context"
	"fmt"
	"os/exec"
)

// LaunchApp opens a .app bundle in the background via `open -g`, returning
// once LaunchServices accepts — the app comes up asynchronously; callers poll
// its socket. Never direct-exec the bundle's inner Mach-O from a launchd
// daemon: it inherits the daemon's bootstrap context outside the user's Aqua
// session, where fuse-t volume bring-up and the volume-access TCC grant fail.
func LaunchApp(ctx context.Context, appPath string) error {
	out, err := exec.CommandContext(ctx, "open", "-g", appPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("open -g %s: %w: %s", appPath, err, out)
	}
	return nil
}
