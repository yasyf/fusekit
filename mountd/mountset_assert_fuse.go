//go:build fuse && cgo

package mountd

import "github.com/yasyf/fusekit"

// Compile-time proof that *fusekit.MountSet satisfies Host. MountSet embeds the
// cgofuse mount registry, so this assertion is fuse-tagged too, keeping the pure
// mountd build cgofuse-free.
var _ Host = (*fusekit.MountSet)(nil)
