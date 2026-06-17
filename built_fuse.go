//go:build fuse && cgo

package fusekit

// built reports whether this binary was compiled with the in-process fuse host.
// This file is the fuse build's half of the pair: it sets built true so a fuse
// binary answers "can I host a mount?" affirmatively. Every other build
// compiles built_nofuse.go, which sets it false.
const built = true
