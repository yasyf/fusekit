//go:build !darwin

package proc

import "context"

// LaunchApp refuses on non-darwin: `open` and .app bundles are macOS-only. The
// refusal is the permanent ErrAppLaunchUnsupported, never a transient
// did-not-come-up.
func LaunchApp(_ context.Context, _ string) error { return ErrAppLaunchUnsupported }
