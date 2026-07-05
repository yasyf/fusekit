//go:build !darwin

package proc

// SuppressStdio is a no-op off darwin: the fuse-t holder is macOS-only.
func SuppressStdio() error { return nil }
