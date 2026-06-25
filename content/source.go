// Package content is the consumer-injected content seam shared by the File
// Provider backend and the fuse holder: fusekit owns the wire and the dispatch,
// the consumer supplies the bytes and the classification.
package content

// EntryKind classifies a top-level domain entry. The string values are FROZEN
// wire artifacts.
type EntryKind string

const (
	// EntrySymlink is a shared entry served by reading the backing file directly.
	EntrySymlink EntryKind = "symlink"
	// EntrySynth is a computed entry whose bytes come from ReadSynth; writes route
	// back through WriteThrough.
	EntrySynth EntryKind = "synth"
	// EntryPrivate is an account-private entry served from the private store.
	EntryPrivate EntryKind = "private"
)

// Entry is one top-level domain entry in a Manifest. Version is the opaque
// freshness key the consumer derives however it likes.
type Entry struct {
	Name    string    `json:"name"`
	Kind    EntryKind `json:"kind"`
	Version string    `json:"version"`
	Size    int64     `json:"size,omitempty"`
	// Target is the absolute backing path an EntrySymlink resolves to.
	Target string `json:"target,omitempty"`
	// Private redirects an EntrySynth write to the private store rather than base.
	Private bool `json:"private,omitempty"`
	// Freshness lists local files whose mtime/size gate an EntrySynth entry's
	// cached bytes: the holder re-reads over the bridge only when one changes,
	// so a steady-state Getattr costs a local stat, not an RPC.
	Freshness []string `json:"freshness,omitempty"`
}

// Source is the consumer-injected content seam: the consumer supplies the bytes,
// the merge schema, the classification, the version strategy. Every method takes
// the domain so one source serves every registered domain. Implementations must
// be safe for concurrent calls.
type Source interface {
	Manifest(domain string) ([]Entry, error)
	ReadSynth(domain, name string) ([]byte, error)
	WriteThrough(domain, name string, data []byte) error
	Classify(name string) EntryKind
}

// Tree is the holder-side read surface for a fully-remote consumer (one whose
// every entry is synth, served over RPC rather than from a local backing tree).
// It is a superset of Source; a Source-only consumer answers the Tree ops with
// an unknown-op error.
type Tree interface {
	Source
	Stat(domain, name string) (Entry, error)
	List(domain, name string) ([]Entry, error)
	ReadAt(domain, name string, ofst int64, size int) ([]byte, error)
	Readlink(domain, name string) (string, error)
}
