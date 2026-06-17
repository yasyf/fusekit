package fusekit

// Built reports whether this binary was compiled with the in-process fuse host
// (the `fuse && cgo` build). It compiles in every build variant — pure binaries
// answer false without a fuse runtime — by reading the build-tagged `built`
// const (built_fuse.go / built_nofuse.go).
//
// The detached mount-holder's spawn path gates on it: only a fuse build can
// host mounts, so a pure binary refuses to spawn a holder with
// mountd.ErrCannotHost instead. It is distinct from a successful HostProbe,
// which additionally proves the fuse runtime loads and the macOS TCC grant is
// in place; Built is the cheap compile-time capability, HostProbe the runtime
// one.
func Built() bool { return built }
