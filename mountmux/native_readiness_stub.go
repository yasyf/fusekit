//go:build !darwin

package mountmux

import "context"

// ConfirmNativeMount is unavailable away from the Darwin native presentation.
func ConfirmNativeMount(context.Context, string) error { return ErrNativeMount }
