//go:build darwin

package fileproviderd

import (
	"context"
	"fmt"
	"os/exec"
)

// launchAppPlatform opens the companion app bundle in the background via macOS
// `open -g`. `open` returns once LaunchServices accepts the request; the app then
// comes up asynchronously, which is why the caller still polls the control socket.
// A non-zero exit is wrapped once; the caller folds it into ErrAppUnavailable.
func launchAppPlatform(ctx context.Context, appPath string) error {
	out, err := exec.CommandContext(ctx, "open", "-g", appPath).CombinedOutput()
	if err != nil {
		return fmt.Errorf("open -g %s: %w: %s", appPath, err, out)
	}
	return nil
}
