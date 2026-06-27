//go:build darwin

package proc

import (
	"context"
	"fmt"
	"os/exec"
)

// LaunchApp opens a signed .app bundle in the background without activating it,
// via macOS `open -g`. `open` returns once LaunchServices has accepted the launch
// request; the app comes up asynchronously, so callers still poll its socket.
//
// This is the ONLY correct way to start a cask holder / companion app from a
// launchd-managed daemon: a daemon-spawned DIRECT-EXEC of the bundle's inner
// Mach-O runs in the daemon's bootstrap context, detached from the user's GUI
// (Aqua) LaunchServices session, where fuse-t's NFS/volume bring-up and the
// macOS volume-access TCC grant do not work — the holder dies before its first
// log line. LaunchServices starts it in-session (ppid 1) with the right context.
// A non-zero `open` exit (a missing/unlaunchable bundle) is wrapped once.
func LaunchApp(ctx context.Context, appPath string) error {
	out, err := exec.CommandContext(ctx, "open", "-g", appPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("open -g %s: %w: %s", appPath, err, out)
	}
	return nil
}
