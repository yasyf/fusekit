package holderfs

import "strings"

// isAppleDouble reports whether the final component of a fuse path (or a bare
// directory-entry name) is an AppleDouble sidecar name ("._*"). The holder
// blocks these outright with macFUSE noappledouble semantics: the mount runs
// without namedattr (NFSv4 named attributes are implicated in macOS
// nfs_vinvalbuf2 kernel panics), so the macOS NFS client fails xattr ops
// ENOTSUP and xnu falls back to writing "._" sidecars through regular vnops.
// Every vnop applies this predicate — creating ops answer EACCES, everything
// else ENOENT, and Readdir never lists a match — so no "._" basename can
// resolve or be created under Base or PrivateRoot through the mount, at any
// depth. (Over NFS the client may surface a blocked create's EACCES as ENOENT
// when a negative-lookup cache entry short-circuits ahead of the create vnop
// under concurrency; either errno means the sidecar was never created —
// verified in the VM, 40/40 EACCES serially, ENOENT only racing in under
// churn.) Names like ".foo", "..data", or "x._y" are not AppleDouble names.
// A "._" component earlier in the path needs no check of its own: it can
// never resolve, so the kernel's per-component lookup never descends past it.
func isAppleDouble(path string) bool {
	name := path
	if i := strings.LastIndexByte(name, '/'); i >= 0 {
		name = name[i+1:]
	}
	return strings.HasPrefix(name, "._")
}
