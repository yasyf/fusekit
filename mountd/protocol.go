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
// behind RemoteHost.Converge — replaces a version-skewed holder and remounts
// everything it served.
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

	// Bridge ops host a consumer's File-Provider-facing content bridge inside
	// the shared holder (Track C). Additive: an old holder answers them with the
	// class-less "unknown op" reply, which IsUnknownOp matches; consumers gate on
	// a client-side version pre-flight before issuing them.
	OpAddBridge    Op = "addbridge"    // bind Request.BridgeSocket, relay to Request.ContentSocket
	OpRemoveBridge Op = "removebridge" // stop and drain Request.Owner's bridge
	OpBridges      Op = "bridges"      // snapshot the bridges (scoped by Request.Owner)

	// OpAttestIdle records Request.Owner's TTL-bounded idleness attestation for
	// Request.Dirs: the consumer vouches that no live session is using those
	// mounts, so a self-retiring holder may drain them (MountSpec.IdlePolicy
	// "attest"). Additive, same skew policy as the bridge ops.
	OpAttestIdle Op = "attestidle"

	// OpRevokeIdle synchronously withdraws Request.Owner's own attestations
	// for Request.Dirs — the consumer is about to hand a mount to a new
	// session, so a retire that has not yet swept may no longer drain it.
	// Additive, same skew policy as the bridge ops.
	OpRevokeIdle Op = "revokeidle"

	// OpListDomains asks the holder for the File Provider domains its
	// DomainSource enumerates — the platform's registered-domain truth,
	// orphans included — so a consumer whose FP bridge the holder hosts can
	// reconcile domains without its own fileproviderd path. Additive, same
	// skew policy as the bridge ops.
	OpListDomains Op = "listdomains"
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
	// BridgeSocket is the appex-facing content-bridge socket OpAddBridge asks the
	// holder to BIND (the consumer's group-container path); Request.ContentSocket
	// is then the consumer daemon's own bridge the relay DIALS. Additive optional
	// field, one-to-one with a content bridge; empty in every mount request.
	BridgeSocket string `json:"bridge_socket,omitempty"`
	// AttrCache and AttrCacheTimeout tune the served mount's go-nfsv4 attr cache
	// (fusekit.MountSpec.AttrCache / .AttrCacheTimeout). Additive optional fields:
	// an old holder ignores them; absent decodes to false / zero, which is exactly
	// today's noattrcache behavior. AttrCacheTimeout is a time.Duration on the wire
	// (JSON nanoseconds); the holder converts to whole seconds at the mount edge.
	AttrCache        bool          `json:"attr_cache,omitempty"`
	AttrCacheTimeout time.Duration `json:"attr_cache_timeout,omitempty"`
	// IdlePolicy tells a self-retiring holder how to prove this mount idle
	// before draining it (fusekit.MountSpec.IdlePolicy): "attest" requires a
	// fresh OpAttestIdle covering Dir, "probe" attempts a graceful unmount.
	// Additive optional field; ABSENT decodes to "" and means "attest" —
	// fail-closed, an old consumer's mounts are never drained unattested.
	IdlePolicy string `json:"idle_policy,omitempty"`
	// CarcassPolicy tells the holder how to treat a dead-mount carcass at the
	// mount's kernel root (fusekit.MountSpec.CarcassPolicy): "force" force-
	// clears it, "defer" leaves it in place and surfaces it. On OpUnmount it
	// is the requester's assertion for a carcass the journal does not know
	// (a journaled spec's own declaration wins). Additive optional field;
	// ABSENT decodes to "" and means "force" — the behavior every consumer
	// had before the field existed.
	CarcassPolicy string `json:"carcass_policy,omitempty"`
	// Dirs are the mount dirs an OpAttestIdle or OpRevokeIdle covers; each
	// must be absolute and not owned by another consumer. Additive optional
	// field.
	Dirs []string `json:"dirs,omitempty"`
	// TTL bounds an OpAttestIdle attestation's freshness (JSON nanoseconds,
	// like AttrCacheTimeout). Required by attestidle, capped server-side.
	TTL time.Duration `json:"ttl,omitempty"`
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
	// CarcassPolicy is the mount's declared dead-carcass treatment
	// (Request.CarcassPolicy). Additive optional field; ABSENT decodes to ""
	// and means "force" — a holder too old to send it also predates "defer".
	CarcassPolicy string `json:"carcass_policy,omitempty"`
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
	// ClassForeignBridge: OpAddBridge named a BridgeSocket already bound by a
	// DIFFERENT owner's bridge; the caller must not stack a second binder on it.
	// Registry state like ClassForeignMount, never a content verdict.
	ClassForeignBridge = "foreign-bridge"
	// ClassInvalidOwner: a bridge Owner that is not a safe single path segment
	// (it names the on-disk spool dir). Non-retryable client error — never a
	// content verdict.
	ClassInvalidOwner = "invalid-owner"
	// ClassBridgeSocketChanged: a same-owner OpAddBridge whose BridgeSocket
	// differs from the live bridge's; the consumer must RemoveBridge before
	// rebinding. Non-retryable.
	ClassBridgeSocketChanged = "bridge-socket-changed"
	// ClassOwnerMismatch: an unmount or remove-bridge named a row registered
	// to a DIFFERENT owner. A misfire guard between cooperating consumers over
	// a same-UID socket (Owner is client-asserted), never a security boundary.
	// Ownerless rows — legacy single-consumer mounts and carcasses — stay open
	// to any owner so carcass teardown keeps working. Non-retryable.
	ClassOwnerMismatch = "owner-mismatch"
	// ClassMuxMismatch: a mux-mode mount cannot join its MuxRoot's native mount —
	// the root's options disagree, the root path is occupied by a plain mount (or
	// vice versa), or the registered dir names a different topology. Registry
	// state like ClassBaseMismatch, never a mount verdict: drivers unmount the
	// conflicting root/dir and retry, never convert/demote the account.
	ClassMuxMismatch = "mux-mismatch"
)

// BridgeInfo is one hosted content bridge in a bridges/add/remove response.
// Socket is the appex-facing socket the holder binds; Upstream is the consumer
// daemon's bridge the relay dials.
type BridgeInfo struct {
	Owner  string `json:"owner"`
	Socket string `json:"socket"`
	// State is one of starting|serving|consent-pending|bind-failed.
	State string `json:"state"`
	// PendingWrites is the depth of the relay's durable write spool.
	PendingWrites int `json:"pending_writes,omitempty"`
	// Upstream is the consumer daemon's bridge socket the relay dials.
	Upstream string `json:"upstream,omitempty"`
	// LastErr is the most recent bind failure, set with consent-pending or
	// bind-failed and cleared once serving.
	LastErr string `json:"last_err,omitempty"`
}

// DomainInfo is one File Provider domain in a listdomains response.
type DomainInfo struct {
	// Domain is the stable domain identifier.
	Domain string `json:"domain"`
	// DisplayName is the user-visible name the domain was registered with.
	DisplayName string `json:"display_name,omitempty"`
}

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
	// Bridges: bridges/add/remove return the hosted content bridges, scoped by
	// Request.Owner. Additive; empty in every mount response.
	Bridges []BridgeInfo `json:"bridges,omitempty"`
	// Domains: listdomains returns the holder's DomainSource enumeration.
	// Additive; empty in every other response.
	Domains []DomainInfo `json:"domains,omitempty"`
	// Health status fields, all additive: a holder predating them omits them,
	// which decodes as not-retiring / not-parked / zero counts.
	// Retiring: the holder is draining for a self-retire; new mounts and
	// bridges answer ClassBusy.
	Retiring bool `json:"retiring,omitempty"`
	// ParkedUntil is the unix-seconds deadline of a retire-storm park; zero
	// means not parked.
	ParkedUntil int64 `json:"parked_until,omitempty"`
	// JournalMounts and JournalBridges count the journaled entries.
	JournalMounts  int `json:"journal_mounts,omitempty"`
	JournalBridges int `json:"journal_bridges,omitempty"`
	// RetireStrikes are the recorded retire-attempt times (unix seconds,
	// oldest first) still inside the strike window's history.
	RetireStrikes []int64 `json:"retire_strikes,omitempty"`
	// RetireDeferredDir and RetireDeferredReason surface a skewed holder whose
	// retire the idle gate is deferring (Retiring stays false by design — the
	// holder serves normally): the first non-idle dir, and the skew reason.
	// Empty when not skewed, or once a drain proceeds.
	RetireDeferredDir    string `json:"retire_deferred_dir,omitempty"`
	RetireDeferredReason string `json:"retire_deferred_reason,omitempty"`
}
