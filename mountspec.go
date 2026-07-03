package fusekit

import "time"

// ContentMode values for MountSpec.ContentMode. The strings are FROZEN wire
// artifacts (mountd carries them verbatim).
const (
	// ContentModeSource mirrors a local Base, serving synthetic entries over
	// the consumer's bridge socket.
	ContentModeSource = "source"
	// ContentModeTree serves EVERY entry from the consumer's content.Tree over
	// the bridge — a fully-remote tenant with no local backing tree.
	ContentModeTree = "tree"
)

// MountSpec describes one mount the holder establishes: the mirror endpoints
// plus the content wiring for serving a consumer's synthetic entries over its
// bridge socket.
type MountSpec struct {
	// Base is the local backing dir the mount mirrors. In ContentModeTree
	// there is no local backing: Base is a NOMINAL identity key (consumers
	// pass their repo root) recorded in the holder registry and never read.
	Base  string
	Dir   string
	Owner string

	// ContentSocket is the consumer's bridge data socket. Empty means a
	// content-less passthrough mount.
	ContentSocket string
	// Domain identifies this mount to the consumer's content source.
	Domain string
	// PrivateRoot is the per-mount backing dir for private/passthrough-write
	// entries ("source" mode only).
	PrivateRoot string
	// ContentMode selects the holder filesystem: "source" mirrors local Base
	// with synth entries served over the bridge; "tree" serves every entry over
	// the bridge. Empty is a plain passthrough.
	ContentMode string
	// ProbePath is the virtual wedge-probe file the holder serves (e.g.
	// "/.ccp-probe"); empty serves none.
	ProbePath string
	// PrivatePrefixes route top-level names equal to or starting with one of
	// them to PrivateRoot rather than Base ("source" mode), so a consumer's
	// tmp→rename commit of a private/synth file stays on one filesystem; exact
	// private names come from the manifest's EntryPrivate classification.
	PrivatePrefixes []string

	// AttrCache opts this mount into the go-nfsv4 server-side attribute cache
	// (default false = noattrcache). Sound ONLY when the served filesystem
	// stabilizes its attributes; see MountOptions.AttrCache for the torn-read
	// hazard and the stability precondition. Forwarded into MountOptions.
	AttrCache bool
	// AttrCacheTimeout sets the go-nfsv4 attr-cache TTL when AttrCache is true;
	// zero leaves fuse-t's default. See MountOptions.AttrCacheTimeout (whole
	// seconds).
	AttrCacheTimeout time.Duration
}

// Flusher is an optional Config.FS capability the holder drains before
// teardown: it blocks up to grace for pending background write-through to
// complete, returning false on timeout.
type Flusher interface {
	FlushWithin(grace time.Duration) bool
}
