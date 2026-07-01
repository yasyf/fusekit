package fusekit

// PassthroughOnly is an optional Config.FS marker declaring the FS serves only
// real backing files — no synthetic content keyed on a fuse
// file handle (fi->fh). When it returns true and fuse-t's FSKit backend is
// available, Mount selects backend=fskit over the default NFS backend. Opt-in
// by design: FSKit does not honor fi->fh, so synthetic reads come back torn or
// wrong under it (proven against cc-pool's merged .claude.json); the safe
// default — not implementing it — keeps the NFS backend, which does.
type PassthroughOnly interface {
	FusePassthroughOnly() bool
}

func passthroughEligible(fs any) bool {
	d, ok := fs.(PassthroughOnly)
	return ok && d.FusePassthroughOnly()
}
