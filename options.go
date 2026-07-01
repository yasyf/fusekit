package fusekit

import "runtime"

// MountOptions builds the fuse-t / libfuse `-o` option list a mount is served with.
type MountOptions struct {
	// Volname is the mount's display name (fuse-t `volname=`).
	Volname string

	// NoBrowse keeps the mount out of Finder sidebars (fuse-t `nobrowse`).
	NoBrowse bool

	// AttrCache opts into the kernel attribute cache; darwin always forces
	// `noattrcache`: fuse-t serves over NFS, and an attr cache lets the NFS
	// client clamp reads to a stale cached size, serving torn documents after
	// an external base edit (TestFuseAttrCacheNoTornRead).
	AttrCache bool

	// NamedAttr routes xattr ops to the fs (fuse-t `namedattr`, NFSv4 named
	// attributes). Without it the macOS NFS client fails every xattr op
	// ENOTSUP, tripping xnu's AppleDouble ._ sidecar fallback.
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
