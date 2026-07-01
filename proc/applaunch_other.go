//go:build !darwin

package proc

import "context"

// LaunchApp always fails with ErrAppLaunchUnsupported: `open` is macOS-only.
func LaunchApp(_ context.Context, _ string) error { return ErrAppLaunchUnsupported }
