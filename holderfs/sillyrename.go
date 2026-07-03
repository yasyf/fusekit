package holderfs

import (
	"log"
	"os"
	"path/filepath"
	"strings"
)

// sillyRenamePrefixes are the top-level name prefixes fuse-t / go-nfsv4 mints
// when it silly-renames a file: when the NFS client is asked to unlink or
// rename-over a file it still holds open, it renames the victim to a
// placeholder in the same directory and defers the real unlink until the last
// handle closes — which, if the session or mount dies first, may be never,
// stranding the placeholder as litter. The set mirrors cc-pool's overlay
// SkipPrefixes convention.
var sillyRenamePrefixes = []string{".fuse_hidden", ".nfs."}

// sillyRenamed reports whether a top-level directory-entry name (NOT a path) is
// a silly-rename placeholder. It is a class apart from isPrivate: a private name
// is merged into the root Readdir listing and participates in manifest
// agreement, whereas a silly placeholder does neither — it is diverted into the
// mount's private root (never shared Base, where it would be listed and read
// through every other tenant's mount) and never listed. The class is top-level
// only; callers gate on isTopLevel before consulting it.
func sillyRenamed(name string) bool {
	for _, p := range sillyRenamePrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// sweepSillyLitter deletes stale top-level silly-rename placeholders left in
// privateRoot by a previous holder generation. The reasoning mirrors the
// release-all handle sweep: any survivor's opens died with that generation, so
// its deferred unlink will never arrive and the file is pure litter. Only
// privateRoot is swept — legacy litter in shared Base is cleaned up
// out-of-band. A missing or empty privateRoot is a no-op; a per-file remove
// failure is logged and skipped so one unremovable file never blocks the mount.
func sweepSillyLitter(privateRoot string) {
	if privateRoot == "" {
		return
	}
	entries, err := os.ReadDir(privateRoot)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("holderfs: sweep silly litter %s: %v", privateRoot, err)
		}
		return
	}
	for _, e := range entries {
		if !sillyRenamed(e.Name()) {
			continue
		}
		p := filepath.Join(privateRoot, e.Name())
		if err := os.Remove(p); err != nil {
			log.Printf("holderfs: sweep silly litter %s: %v", p, err)
		}
	}
}
