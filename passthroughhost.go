//go:build fuse && cgo

package fusekit

// PassthroughHost returns a holder host serving a read-only passthrough of base
// at dir — a content-less host that proves the mount and TCC grant.
func PassthroughHost() *MountSet {
	return &MountSet{
		Build: func(base, dir string) Config {
			return Config{
				Base:         base,
				Dir:          dir,
				FS:           &probeFS{root: base},
				Options:      MountOptions{Volname: "fusekit-holder", NoBrowse: true}.Build(),
				Wait:         probeWait,
				FirstWait:    probeFirstWait,
				ClearCarcass: true,
			}
		},
		StateFn: func(base, dir string) (mounted, alive bool) {
			m := Mounted(dir)
			return m, m && MountAlive(base, dir)
		},
	}
}
