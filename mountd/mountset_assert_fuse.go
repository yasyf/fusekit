//go:build fuse && cgo

package mountd

import "github.com/yasyf/fusekit"

// Compile-time proof that *fusekit.MountSet satisfies Host: the in-process fuse
// host fusekit consumers build (cc-pool's InProcessFuse, cc-notes' HolderHost)
// must be drivable by the mount-holder Server. MountSet is fuse-tagged — it
// embeds the cgofuse mount registry — so this assertion is too; the pure mountd
// build never references it, keeping the package cgofuse-free.
var _ Host = (*fusekit.MountSet)(nil)
