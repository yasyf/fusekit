//go:build !darwin

package overlay

import (
	"context"
	"errors"
)

func openSettingsURL(context.Context, string) error {
	return errors.New("opening System Settings is only supported on macOS")
}
