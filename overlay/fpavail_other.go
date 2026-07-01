//go:build !darwin

package overlay

// fileProviderEnabledPlatform reports false off darwin: File Provider and pluginkit are macOS-only.
func fileProviderEnabledPlatform(string) bool { return false }
