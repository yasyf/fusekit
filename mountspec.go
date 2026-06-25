package fusekit

import "time"

// MountSpec describes one mount the holder establishes: the mirror endpoints
// plus the content wiring that lets the holder serve a consumer's synthetic
// entries over its bridge socket. The content fields are empty for a plain
// passthrough mount (holderfs serves a bare mirror of Base then).
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
	// with synth entries served over the bridge (cc-pool); "tree" serves every
	// entry over the bridge (cc-notes). Empty is a plain passthrough.
	ContentMode string
	// ProbePath is the virtual wedge-probe file the holder serves (e.g.
	// "/.ccp-probe"); empty serves none.
	ProbePath string
	// PrivatePrefixes route any top-level name that equals or starts with one of
	// them to PrivateRoot rather than Base ("source" mode) — the consumer's
	// atomic-write temp siblings of its private/synth files, so a tmp→rename
	// commit stays on one filesystem. Exact private names come from the manifest's
	// EntryPrivate classification.
	PrivatePrefixes []string
}

// Flusher is an optional Config.FS capability the holder drains before
// teardown: it blocks up to grace for pending background write-through to
// complete, returning false on timeout.
type Flusher interface {
	FlushWithin(grace time.Duration) bool
}
