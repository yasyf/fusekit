//go:build !darwin

package overlay

// fileProviderEnabledPlatform is darwin-only: the File Provider extension and
// pluginkit exist only on macOS, so off darwin the File Provider backend is
// never available.
func fileProviderEnabledPlatform(string) bool { return false }
