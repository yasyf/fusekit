//go:build darwin

package overlay

import (
	"context"
	"os/exec"
)

// openSettingsURL opens a System Settings deep link via the macOS "open"
// command. It is the default openRunner on darwin.
func openSettingsURL(ctx context.Context, url string) error {
	return exec.CommandContext(ctx, "open", url).Run()
}
