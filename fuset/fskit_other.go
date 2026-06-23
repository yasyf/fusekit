//go:build !darwin

package fuset

// fskitAvailable is false off macOS: fuse-t, and thus its FSKit backend, is
// macOS-only.
func fskitAvailable() bool { return false }
