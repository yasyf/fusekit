//go:build !(fuse && cgo)

package fusekit

// built reports whether this binary was compiled with the in-process fuse host
// (the `fuse && cgo` build). In every other build it is false, so a pure-Go
// binary can answer "can I host a mount?" without a fuse runtime. The fuse
// build sets it true in built_fuse.go.
const built = false
