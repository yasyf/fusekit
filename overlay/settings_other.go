//go:build !darwin

package overlay

import (
	"context"
	"errors"
)

// openSettingsURL is darwin-only: the System Settings deep links the overlay
// backends point at exist only on macOS.
func openSettingsURL(context.Context, string) error {
	return errors.New("opening System Settings is only supported on macOS")
}
