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
	// EntryDir is a directory a Tree consumer synthesizes; only Stat/List emit it,
	// never a Manifest.
	EntryDir EntryKind = "dir"
)

// Entry is one top-level domain entry in a Manifest. Version is an opaque
// freshness key the consumer derives.
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
	// cached bytes; the holder re-reads over the bridge only when one changes
	// (steady-state Getattr = local stat, no RPC).
	Freshness []string `json:"freshness,omitempty"`
	// Mtime is the entry's modification time in Unix nanoseconds, set by Tree
	// consumers so the holder serves the consumer's times (monotonically — see
	// the holderfs attr-stability rules) instead of inventing its own. Zero
	// means the consumer supplies no time.
	Mtime int64 `json:"mtime,omitempty"`
	// Birth is the entry's creation time in Unix nanoseconds; zero falls back
	// to Mtime.
	Birth int64 `json:"birth,omitempty"`
	// Ino is a stable consumer-side identity key for a Tree entry (e.g. an
	// entity id hash), invariant across renames and re-renders. The holder
	// MUST NOT serve it raw — it mints its own mount-lifetime-stable inode
	// keyed by this value, keeping the served fileid space its own.
	Ino uint64 `json:"ino,omitempty"`
}

// Source is the consumer-injected content seam: the consumer supplies the bytes,
// merge schema, classification, and version strategy. Every method takes the
// domain so one source serves all domains. Implementations must be safe for
// concurrent calls.
type Source interface {
	Manifest(domain string) ([]Entry, error)
	ReadSynth(domain, name string) ([]byte, error)
	WriteThrough(domain, name string, data []byte) error
	Classify(name string) EntryKind
}

// Classifier is the optional Source superset the BridgeServer prefers over
// Classify for BridgeOpClassify: it reports a name's serving kind and can signal
// that no verdict is available — the shape a caching relay needs to answer "I
// have nothing cached to classify from, and my upstream is unreachable" without
// guessing, which Classify's bare EntryKind return cannot express. A Source that
// does not implement it keeps answering through Classify unchanged.
type Classifier interface {
	Source
	ClassifyErr(name string) (EntryKind, error)
}

// Tree is the holder-side read surface for a fully-remote consumer (every entry
// synth, served over RPC, not a local backing tree). It is a superset of Source;
// a Source-only consumer answers the Tree ops with an unknown-op error.
type Tree interface {
	Source
	Stat(domain, name string) (Entry, error)
	List(domain, name string) ([]Entry, error)
	ReadAt(domain, name string, ofst int64, size int) ([]byte, error)
	Readlink(domain, name string) (string, error)
}

// WritableTree is the write surface for a fully-remote consumer: the mutations
// the holder forwards path-wise (no open handle) for a writable tenant. A
// no-local-base tenant has nowhere to stage writes, so every mutation crosses
// the bridge and the consumer persists it however its domain requires (e.g.
// parse + diff + CRDT append). Implementations must be safe for concurrent
// calls and fail with a ClassedError so the errno class crosses the wire; a
// write op against a Tree that does not implement WritableTree answers
// ClassUnsupported (see IsUnsupported), never a panic.
type WritableTree interface {
	Tree
	// Create makes name exist as an empty file. The consumer owns mode and
	// presentation, so no mode crosses the wire.
	Create(domain, name string) error
	// WriteAt writes data at ofst, extending the file as needed. It is
	// all-or-error: no partial-write count crosses the wire.
	WriteAt(domain, name string, ofst int64, data []byte) error
	// Truncate resizes name to size, zero-filling growth.
	Truncate(domain, name string, size int64) error
	Unlink(domain, name string) error
	Rename(domain, oldName, newName string) error
	Mkdir(domain, name string) error
}

// HandleTree is the optional per-open surface over a Tree: the holder opens one
// handle per fuse open and the consumer keys its per-open state — snapshot
// cache, edit buffer — by the returned token, so chunked NFS reads never
// re-render per ReadAt and buffered edits commit once at close. A plain Tree
// consumer that skips this surface keeps working exactly as v0.18.0 shipped:
// the holder detects the miss via IsUnsupported and stays stateless.
//
// Token contract: OpenHandle mints an opaque, non-empty, unique token bound to
// (domain, name). Ops on an unknown or released token fail with ClassNotFound;
// a token used against a different name fails with ClassInvalid.
//
// Crash safety is release-all, not TTL: the holder calls ReleaseAllHandles for
// a domain when it starts serving it and when it stops cleanly, so tokens
// leaked by a holder crash are dropped on the next holder generation's first
// call. A TTL was rejected because expiring an idle-but-dirty edit buffer
// under a still-open editor handle silently loses the edit.
type HandleTree interface {
	Tree
	// OpenHandle snapshots name and returns the handle's token plus the
	// snapshot's Entry. The Entry's Size MUST be the exact length of the
	// bytes ReadAtHandle serves for this token: the holder serves it as the
	// open's file size and the kernel caps reads at the size it was served,
	// so a size from anything but the snapshot itself tears reads across a
	// concurrent commit. Mtime, Birth, and Ino carry the same meaning as in
	// Stat.
	OpenHandle(domain, name string) (string, Entry, error)
	// ReadAtHandle reads from the handle's open-time snapshot, immune to
	// concurrent commits.
	ReadAtHandle(domain, name, token string, ofst int64, size int) ([]byte, error)
	// WriteAtHandle writes into the handle's edit buffer; nothing persists
	// until FlushHandle or ReleaseHandle commits it.
	WriteAtHandle(domain, name, token string, ofst int64, data []byte) error
	// TruncateHandle resizes the handle's edit buffer.
	TruncateHandle(domain, name, token string, size int64) error
	// FlushHandle commits the handle's dirty buffer and returns the commit
	// verdict — the op that lets a rejected save (ClassInvalid parse failure,
	// ClassPerm immutable edit) reach the writer at its fsync/close boundary,
	// since a fuse Release status is kernel-discarded.
	FlushHandle(domain, name, token string) error
	// ReleaseHandle drops the token. It backstop-commits a dirty buffer no
	// FlushHandle ever committed; the error is the holder's to log, not the
	// writer's to see.
	ReleaseHandle(domain, name, token string) error
	// ReleaseAllHandles drops every token for domain (the crash-recovery
	// sweep documented above).
	ReleaseAllHandles(domain string) error
}
