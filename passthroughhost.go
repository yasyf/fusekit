//go:build fuse && cgo

package fusekit

// PassthroughHost returns a mount-holder host (a *MountSet) serving a read-only
// passthrough of base at dir. It is the holder's content-less host: enough to
// bring a real fuse-t mount live and prove the TCC grant. A *MountSet satisfies
// mountd.Host structurally, so cmd/holder assigns it to Server.Host.
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
