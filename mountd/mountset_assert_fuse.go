//go:build fuse && cgo

package mountd

import "github.com/yasyf/fusekit"

// Compile-time proof that *fusekit.MountSet satisfies Host and its optional
// capabilities. MountSet embeds the cgofuse mount registry, so these assertions
// are fuse-tagged too, keeping the pure mountd build cgofuse-free.
var (
	_ Host          = (*fusekit.MountSet)(nil)
	_ MuxRootHolder = (*fusekit.MountSet)(nil)
)
