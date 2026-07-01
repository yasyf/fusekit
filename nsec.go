package fusekit

import "hash/fnv"

// VersionNsec derives a deterministic mtime nanosecond component from a
// per-version seed: timestamps have second granularity, so a same-second
// version change would otherwise leave mtime unchanged and an NFS client would
// keep serving its own written pages instead of revalidating.
func VersionNsec(seed string) int64 {
	return int64(fnv64a("v:"+seed) % 1_000_000_000)
}

func fnv64a(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}
