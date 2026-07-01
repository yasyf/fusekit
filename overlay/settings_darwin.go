//go:build darwin

package overlay

import (
	"context"
	"os/exec"
)

func openSettingsURL(ctx context.Context, url string) error {
	return exec.CommandContext(ctx, "open", url).Run()
}
