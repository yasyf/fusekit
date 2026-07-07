//go:build !darwin

package overlay

import "errors"

// fileProviderEnabledPlatform reports false off darwin: File Provider and pluginkit are macOS-only.
func fileProviderEnabledPlatform(string) bool { return false }

// electFileProviderPlatform errors off darwin: File Provider election needs macOS pluginkit.
func electFileProviderPlatform(string) error {
	return errors.New("electing a File Provider extension is only supported on macOS")
}
