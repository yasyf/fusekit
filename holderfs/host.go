//go:build fuse && cgo && darwin

package holderfs

import (
	"errors"
	"io"
	"os"

	"github.com/yasyf/fusekit"
)

// Host returns the shared holder's content MountSet: it builds a holderFS per mount
// (a passthrough mirror of Base absent content wiring) and gates liveness on the
// kernel mountpoint plus a redirect-agnostic root readdir through the live mount.
func Host() *fusekit.MountSet {
	return &fusekit.MountSet{
		Build: Build,
		StateFn: func(base, dir string) (mounted, alive bool) {
			m := fusekit.Mounted(dir)
			return m, m && servingRoot(dir)
		},
	}
}

// servingRoot reports whether a confirmed-mounted holder mount answers a root
// readdir. The caller gates on Mounted(dir), so the readdir traverses the live NFS
// mount, not the pre-mount dir. Redirect-agnostic on purpose: MountAlive lstats
// Base's first entry, a PrivatePrefixes dotfile (".credentials.json") redirected
// onto an absent PrivateRoot copy — a clean -ENOENT that reads a live mount as dead
// (the defect readyFn fixes for come-up); readdir fills names with nil stats, so an
// absent private backing can't trip it. A wedge is caught separately by the
// consumer's deep probe; this only confirms the server answers a basic op.
func servingRoot(dir string) bool {
	f, err := os.Open(dir)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	_, err = f.Readdirnames(1)
	return err == nil || errors.Is(err, io.EOF)
}
