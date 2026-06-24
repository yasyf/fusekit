//go:build !darwin

package proc

// withChildNprocCap runs spawn unmodified on non-darwin platforms. fusekit's
// holder is macOS-only, and on Linux RLIMIT_NPROC counts threads (not just
// processes), so capping it around a fork would risk starving the Go runtime
// rather than bounding a fork bomb. The darwin build carries the real guard.
func withChildNprocCap(spawn func() error) error { return spawn() }
