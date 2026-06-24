//go:build !darwin

package fileproviderd

import "context"

// launchAppPlatform is darwin-only: the File Provider companion app is launched
// through macOS `open`, and File Provider domains exist only on macOS. Off
// darwin it refuses with the permanent, unwrapped ErrAppLaunchUnsupported.
func launchAppPlatform(context.Context, string) error {
	return ErrAppLaunchUnsupported
}
