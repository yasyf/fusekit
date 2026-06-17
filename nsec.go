package fusekit

import "hash/fnv"

// VersionNsec derives a deterministic mtime nanosecond component from a
// per-version seed, lifted from cc-notes' versionNsec. Entity timestamps have
// second granularity, so a save whose commit lands in the same second would
// otherwise leave the mtime unchanged — and an NFS client would keep serving
// its own written pages over the differing canonical render. Folding the seed
// (typically a chain-tip SHA) into the nanoseconds makes every version a
// visible mtime change, forcing the client to revalidate its data cache. The
// result is in [0, 1e9) so it is always a valid Timespec.Nsec. Pure: usable in
// tests without a fuse build.
func VersionNsec(seed string) int64 {
	return int64(fnv64a("v:"+seed) % 1_000_000_000)
}

// fnv64a is the 64-bit FNV-1a hash of s.
func fnv64a(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
