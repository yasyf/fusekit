package fusekit

import (
	"fmt"
	"time"
)

// MountOptions builds the fuse-t / libfuse `-o` option list a mount is served with.
type MountOptions struct {
	// Volname is the mount's display name (fuse-t `volname=`).
	Volname string

	// NoBrowse keeps the mount out of Finder sidebars (fuse-t `nobrowse`).
	NoBrowse bool

	// AttrCache opts this mount into the go-nfsv4 attribute cache. Default false
	// emits `-o noattrcache`, which fuse-t maps to go-nfsv4's `--attrcache=false`
	// (go-nfsv4's own default is attrcache ON): every GETATTR reaching the server
	// is a fresh fuse upcall, never a stale cached attr. This is the safe default
	// for mutable or passthrough content whose backing can change OUTSIDE the fuse
	// handlers — an external edit to the mirrored Base — because a cache lets the
	// client clamp a read to a stale cached size and serve a torn document. Note
	// the macOS NFS *client* attr cache is independent of this knob and stays on
	// regardless (acregmin ≈ 5s; live_test observes a just-cached mtime for ~5s
	// even under noattrcache); this field only governs the server-side cache.
	//
	// Opt in (true) ONLY when EVERY entry the filesystem serves has stable,
	// monotonic attributes, or content is single-writer strictly through the
	// mount, so no external edit can strand a stale cached size. holderfs >=
	// v0.23.0 stabilizes synth attrs (mount-lifetime inode, monotonic
	// mtime/size, open-pinned snapshots), but its source mode still passes
	// unmanifested entries through to live Base stats — there soundness also
	// needs the manifest to carve out every externally-mutable entry. Opting in
	// drops the per-GETATTR upcall. DEFAULT MUST STAY OFF for any consumer that
	// has not proven attr stability.
	AttrCache bool

	// AttrCacheTimeout sets the go-nfsv4 attribute-cache TTL, emitted as
	// `-o attrcache-timeout=<seconds>` — which fuse-t maps to go-nfsv4's
	// `--attrcache-timeout=N` (whole seconds) — only when AttrCache is true and
	// the value is at least one second. Zero (or AttrCache false) leaves fuse-t's
	// default TTL in place. fuse-t exposes attr caching itself as a boolean
	// (`noattrcache`); this TTL is the one tunable it forwards, and its option is
	// integer seconds, so a sub-second value is not representable and emits nothing.
	AttrCacheTimeout time.Duration

	// NamedAttr routes xattr ops to the fs (fuse-t `namedattr`, NFSv4 named
	// attributes). Without it the macOS NFS client fails every xattr op
	// ENOTSUP, tripping xnu's AppleDouble ._ sidecar fallback. CAVEAT: the
	// NFSv4 named-attribute vnode path is implicated in macOS nfs_vinvalbuf2
	// kernel panics (see CHANGELOG); holderfs no longer sets it and blocks
	// the resulting ._ sidecars instead. The field remains for consumers
	// that accept the risk (e.g. on Linux).
	NamedAttr bool

	// Extra carries raw `k=v` (or bare-flag) option strings fusekit does not
	// model, emitted after the structured flags.
	Extra []string
}

// Build returns the flat ["-o", opt, ...] slice, in stable order.
func (o MountOptions) Build() []string {
	var opts []string
	if o.Volname != "" {
		opts = append(opts, "-o", "volname="+o.Volname)
	}
	if o.AttrCache {
		// Cache on: fuse-t leaves go-nfsv4's default-on attrcache alone. Forward
		// only the TTL, and only at whole-second granularity go-nfsv4 accepts.
		if secs := int(o.AttrCacheTimeout / time.Second); secs > 0 {
			opts = append(opts, "-o", fmt.Sprintf("attrcache-timeout=%d", secs))
		}
	} else {
		opts = append(opts, "-o", "noattrcache")
	}
	if o.NoBrowse {
		opts = append(opts, "-o", "nobrowse")
	}
	if o.NamedAttr {
		opts = append(opts, "-o", "namedattr")
	}
	for _, x := range o.Extra {
		opts = append(opts, "-o", x)
	}
	return opts
}
