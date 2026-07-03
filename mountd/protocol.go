// Package mountd is the mount-holder subsystem: a tiny detached process that
// hosts fuse-t mounts so daemon restarts never disturb live consumer sessions.
// Drivers speak the newline-delimited JSON protocol defined here over the
// holder's 0600 unix socket, one request and one response per connection.
//
// The holder is stateless mechanism with no consumer state or app-specific
// imports: it mounts and unmounts what it is told and reports kernel truth;
// desired state lives with the driver.
//
// The driver side lives here too: RemoteHost drives the holder, and Retire —
// behind RemoteHost.Converge and the RetirePolicy adapter — replaces a
// version-skewed holder and remounts everything it served.
//
// Compatibility policy: proto-1 ops are FROZEN — names, fields, and semantics
// never change. New capability means a new op or a new optional field, never
// a rename, repurpose, or retype. Drivers treat ok=false with an "unknown op"
// error as not-supported, never as holder failure. Bidirectional version skew
// is routine: an old daemon drives a new holder (brew upgrades swap the
// binary under a running daemon) and a new daemon drives an old holder (the
// holder outlives daemon restarts by design).
package mountd

import "time"

// MountProtoVersion stamps every request and response; bumped only on
// incompatible wire changes, which the compatibility policy above rules out.
const MountProtoVersion = 1

// Op is a request operation.
type Op string

const (
	OpHealth   Op = "health"   // liveness + version probe
	OpProbe    Op = "probe"    // throwaway in-process fuse capability mount
	OpMount    Op = "mount"    // ensure a live mirror of base at dir (a held-but-dead mirror is remounted)
	OpUnmount  Op = "unmount"  // unmount the mirror at dir
	OpList     Op = "list"     // snapshot the mounts this holder owns
	OpShutdown Op = "shutdown" // unmount everything, reply, exit
	OpReclaim  Op = "reclaim"  // unmount every mount owned by Request.Owner
)

// Request is one client request. Base and Dir are required by mount AND
// unmount: Teardown refuses base==dir, so even a carcass unmount (a
// mountpoint with no registry row) needs the base named. In tree mode
// (ContentMode "tree") Base is a NOMINAL identity key — the consumer's repo
// root — that the mount never reads; it still keys the registry, teardown,
// and unmount, so dir == base stays refused in every mode.
type Request struct {
	Proto int    `json:"proto"`
	Op    Op     `json:"op"`
	Base  string `json:"base,omitempty"`
	Dir   string `json:"dir,omitempty"`
	// Owner scopes a mount to one consumer; empty is the legacy single-consumer holder.
	Owner string `json:"owner,omitempty"`
	// MuxRoot, when set, serves Dir as a logical subtree of ONE native mount at
	// MuxRoot instead of its own kernel mount (fusekit.MountSpec.MuxRoot). Dir
	// must be a direct child of MuxRoot; every mount sharing a MuxRoot shares that
	// one native mount. Additive optional field: an old holder ignores it and
	// serves a plain per-dir mount, so the client-side MinHolderVersion gate — not
	// this field — is the fail-loud skew guard. Absent decodes to "", exactly
	// today's one-mount-per-dir behavior.
	MuxRoot string `json:"mux_root,omitempty"`
	// Content fields: a mount serving the consumer's synthetic entries over its
	// bridge socket; all empty is a plain passthrough. One-to-one with
	// fusekit.MountSpec.
	ContentSocket   string   `json:"content_socket,omitempty"`
	Domain          string   `json:"domain,omitempty"`
	PrivateRoot     string   `json:"private_root,omitempty"`
	ContentMode     string   `json:"content_mode,omitempty"`
	ProbePath       string   `json:"probe_path,omitempty"`
	PrivatePrefixes []string `json:"private_prefixes,omitempty"`
	// AttrCache and AttrCacheTimeout tune the served mount's go-nfsv4 attr cache
	// (fusekit.MountSpec.AttrCache / .AttrCacheTimeout). Additive optional fields:
	// an old holder ignores them; absent decodes to false / zero, which is exactly
	// today's noattrcache behavior. AttrCacheTimeout is a time.Duration on the wire
	// (JSON nanoseconds); the holder converts to whole seconds at the mount edge.
	AttrCache        bool          `json:"attr_cache,omitempty"`
	AttrCacheTimeout time.Duration `json:"attr_cache_timeout,omitempty"`
}

// MountInfo is one mount in a list or shutdown response. Live is shallow
// kernel truth, not registry membership: dir is currently a mountpoint
// (overlay.Mounted) AND base's contents are visible through it
// (overlay.MountAlive) — either half alone can lie, since a dead mirror
// exposes the underlying dir whose leftover entries can shadow base's. Live
// does NOT reflect a partial wedge (shallow-alive but bulk reads hang): the
// holder ships no deep verdict; the daemon deep-probes through the kernel
// mount and keeps its own per-dir verdict. A lingering old holder may still
// send a "wedged" field; drivers ignore it.
type MountInfo struct {
	Dir  string `json:"dir"`
	Base string `json:"base"`
	Live bool   `json:"live"`
	// Epoch increments on each (re)mount of Dir by this holder, starting at 1;
	// zero means the holder predates the field.
	Epoch uint64 `json:"epoch,omitempty"`
	// MountedAt is the unix-seconds time of the current mount of Dir; zero
	// means the holder predates the field.
	MountedAt int64  `json:"mounted_at,omitempty"`
	Owner     string `json:"owner,omitempty"`
	// MuxRoot is Dir's native mount root when Dir is a logical subtree of a
	// shared mux mount; empty for a plain one-mount-per-dir row (Request.MuxRoot).
	MuxRoot string `json:"mux_root,omitempty"`
}

// Error classes, sent alongside Error so drivers classify failures without
// string matching. Values are frozen; new failure modes get new classes, and
// a driver that predates a class treats it as retryable (ErrUnknownClass),
// never as a mount failure.
const (
	// ClassTCC: mount issued but never came live — almost always the one-time
	// macOS volume-access TCC grant; Error carries the user-facing walkthrough.
	ClassTCC = "tcc"
	// ClassMountTimeout: mount issued but not live within the holder's bounded
	// wait, in a process whose volume-access grant an earlier live mount or
	// probe already proved — NOT TCC. Transient; drivers retry and never
	// surface TCC guidance.
	ClassMountTimeout = "mount-timeout"
	// ClassMountFailed: the mount failed outright.
	ClassMountFailed = "mount-failed"
	// ClassWedged: the unmount did not take; the dir is still a mountpoint
	// and must not be treated as torn down.
	ClassWedged = "wedged"
	// ClassForeignMount: dir is a mountpoint this holder does not own; the
	// caller must unmount it first — the holder never stacks mounts.
	ClassForeignMount = "foreign-mount"
	// ClassBusy: another operation is in flight on the same dir; transient, retry.
	ClassBusy = "busy"
	// ClassBaseMismatch: dir is already mounted by this holder but mirrors a
	// different base; the caller must unmount it first.
	ClassBaseMismatch = "base-mismatch"
	// ClassContentUnavailable: the consumer's content bridge was unreachable
	// (its daemon may be mid-restart). Transient and NEVER a mount verdict — a
	// bare passthrough would serve the wrong bytes, so the holder fails the
	// mount loudly; drivers MUST retry rather than convert/demote the account.
	ClassContentUnavailable = "content-unavailable"
	// ClassMuxMismatch: a mux-mode mount cannot join its MuxRoot's native mount —
	// the root's options disagree, the root path is occupied by a plain mount (or
	// vice versa), or the registered dir names a different topology. Registry
	// state like ClassBaseMismatch, never a mount verdict: drivers unmount the
	// conflicting root/dir and retry, never convert/demote the account.
	ClassMuxMismatch = "mux-mismatch"
)

// Response is one server reply.
type Response struct {
	Proto    int    `json:"proto"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	ErrClass string `json:"err_class,omitempty"`
	Version  string `json:"version,omitempty"` // health
	FuseOK   bool   `json:"fuse_ok,omitempty"` // probe
	// Mounts: list returns every owned mount; shutdown returns only the dirs
	// that FAILED to unmount (empty means a clean sweep).
	Mounts []MountInfo `json:"mounts,omitempty"`
}
