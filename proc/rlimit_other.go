//go:build !darwin

package proc

// withChildNprocCap runs spawn uncapped off darwin: Linux RLIMIT_NPROC counts
// threads, so capping it around a fork would starve the Go runtime.
func withChildNprocCap(spawn func() error) error { return spawn() }
