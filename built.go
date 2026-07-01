package fusekit

// Built reports whether this binary was compiled with the in-process fuse host
// (the `fuse && cgo` build). Compile-time capability only — HostProbe
// additionally proves the runtime loads and the macOS TCC grant is in place.
func Built() bool { return built }
