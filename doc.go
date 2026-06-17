// Package fusekit provides detached FUSE-T mount-holder and mount-lifecycle
// primitives, extracted from cc-pool. The root package holds the mount-core
// primitives and the in-process fuse host (the latter under the fuse build
// tag); subpackage mountd holds the detached mount-holder and its frozen wire
// protocol, and builds pure (no cgofuse).
package fusekit
