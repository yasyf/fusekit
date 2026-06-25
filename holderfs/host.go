//go:build fuse && cgo && darwin

package holderfs

import "github.com/yasyf/fusekit"

// Host returns the shared holder's content MountSet: it builds a holderFS per
// mount (a passthrough mirror of Base when the mount carries no content wiring)
// and reports liveness as the kernel mountpoint + base-visible pair.
func Host() *fusekit.MountSet {
	return &fusekit.MountSet{
		Build: Build,
		StateFn: func(base, dir string) (mounted, alive bool) {
			m := fusekit.Mounted(dir)
			return m, m && fusekit.MountAlive(base, dir)
		},
	}
}
