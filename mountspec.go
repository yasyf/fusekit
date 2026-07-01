package fusekit

import "time"

// MountSpec describes one mount the holder establishes: the mirror endpoints
// plus the content wiring for serving a consumer's synthetic entries over its
// bridge socket.
type MountSpec struct {
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
}

// Flusher is an optional Config.FS capability the holder drains before
// teardown: it blocks up to grace for pending background write-through to
// complete, returning false on timeout.
type Flusher interface {
	FlushWithin(grace time.Duration) bool
}
