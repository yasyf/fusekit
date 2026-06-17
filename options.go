package fusekit

import "runtime"

// MountOptions builds the fuse-t / libfuse `-o` option list a mount is served
// with. It is consumer-facing: each app fills the fields its mount needs and
// calls Build to get the flat ["-o", k=v, ...] slice cgofuse's host.Mount
// expects. Build is pure (no syscall) — it keys the noattrcache rule off
// runtime.GOOS — so it is unit-testable without a fuse build.
type MountOptions struct {
	// Volname is the mount's display name (fuse-t `volname=`). Emitted only
	// when non-empty.
	Volname string

	// NoBrowse keeps the mount out of Finder sidebars / browse lists (fuse-t
	// `nobrowse`). Emitted only when true.
	NoBrowse bool

	// AttrCache opts INTO the kernel attribute cache. It is honored only on
	// non-darwin (Linux fuse3, which has no NFS torn-read pathology): when
	// true there, `noattrcache` is omitted. On darwin it is IGNORED —
	// `noattrcache` is forced on regardless — because fuse-t serves the mount
	// over NFS and an enabled attr cache lets the NFS client clamp a read to a
	// stale cached size and serve a torn/truncated document after an external
	// base edit (cc-pool commit d279cff / TestFuseAttrCacheNoTornRead).
	AttrCache bool

	// NamedAttr routes xattr ops to the fs's xattr handlers (fuse-t
	// `namedattr`, NFSv4 named attributes). On macOS this is load-bearing for
	// apps that need xattrs: the NFS client defaults to nonamedattr, under
	// which every xattr op fails ENOTSUP and trips xnu's AppleDouble fallback
	// (._<name> sidecar litter). Emitted only when true.
	NamedAttr bool

	// Extra carries any additional raw `k=v` (or bare flag) option strings,
	// each emitted as a separate "-o", x pair after the structured flags. It
	// is how a consumer passes options fusekit does not model — e.g. cc-pool's
	// rwsize=1048576.
	Extra []string
}

// Build returns the flat ["-o", opt, ...] slice for these options. Ordering is
// stable: volname, noattrcache, nobrowse, namedattr, then each Extra in order
// — so cc-pool's darwin string (volname=…, noattrcache, nobrowse, namedattr,
// rwsize=1048576) is reproduced byte-for-byte.
//
// The noattrcache rule is the one platform conditional: on darwin it is ALWAYS
// emitted (fuse-t-over-NFS torn-read invariant — AttrCache cannot turn it off);
// on non-darwin it is emitted only when AttrCache is false. Equivalently,
// noattrcache is emitted when runtime.GOOS == "darwin" || !AttrCache.
func (o MountOptions) Build() []string {
	var opts []string
	if o.Volname != "" {
		opts = append(opts, "-o", "volname="+o.Volname)
	}
	if runtime.GOOS == "darwin" || !o.AttrCache {
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
