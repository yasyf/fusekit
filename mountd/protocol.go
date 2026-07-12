// Package mountd is the mount-holder subsystem: a tiny detached process that
// hosts fuse-t mounts so daemon restarts never disturb live consumer sessions.
// Drivers speak the newline-delimited JSON protocol defined here over the
// holder's 0600 unix socket, one request and one response per connection.
//
// The holder is the ONLY process in the fleet that mounts, unmounts, or
// force-clears. Every safety decision derives from kernel ground truth at
// action time — flock leases (the lease package) and bounded stat verdicts —
// never from journaled or pushed consumer intent. Desired state lives with
// the driver; the holder reports kernel truth.
//
// Compatibility policy: proto 2 is capability-negotiated, not version-
// arithmetic'd. A consumer opens with OpHello and REQUIRES the features it
// needs (HelloInfo.Require); the holder refuses proto-1 requests with a crisp
// error naming the fix. Within proto 2, evolution is additive: new capability
// means a new op or a new optional field plus a new feature string — never a
// rename, repurpose, or retype.
package mountd

import "time"

// MountProtoVersion stamps every request and response. Proto 2 (holder v2):
// hello/features, owner-required ops, the lease ladder, and no wire force —
// proto-1 requests are refused.
const MountProtoVersion = 2

// Op is a request operation.
type Op string

const (
	OpHello   Op = "hello"   // proto + version + feature negotiation
	OpHealth  Op = "health"  // liveness + status snapshot
	OpProbe   Op = "probe"   // throwaway in-process fuse capability mount
	OpMount   Op = "mount"   // ensure a live mirror of base at dir (a held-but-dead mirror is remounted)
	OpUnmount Op = "unmount" // unmount the mirror at dir via the lease ladder
	OpList    Op = "list"    // snapshot the owner's mounts (All: read-only cross-tenant view)
	OpReclaim Op = "reclaim" // unmount every mount owned by Request.Owner
	OpLeases  Op = "leases"  // read-only lease-file diagnostic, owner-scoped (All: cross-tenant view)

	// Bridge ops host a consumer's File-Provider-facing content bridge inside
	// the shared holder.
	OpAddBridge    Op = "addbridge"    // bind Request.BridgeSocket, relay to Request.ContentSocket
	OpRemoveBridge Op = "removebridge" // stop and drain Request.Owner's bridge
	OpBridges      Op = "bridges"      // snapshot the owner's bridges (All: cross-tenant view)
)

// Holder feature strings, returned by OpHello. Consumers gate capabilities on
// these — never on version arithmetic. THE RULE: every capability a consumer
// could depend on across proto-2 skew — a new op, a new request field with
// behavior, a new response surface — ships WITH a feature token here, so
// HelloInfo.Require can prove it exists before use.
const (
	FeatureMux        = "mux"             // MuxRoot subtree mounts
	FeatureBridge     = "bridge"          // hosted content bridges
	FeatureTree       = "tree"            // ContentModeTree mounts
	FeatureLeaseGate  = "lease-gate"      // lease-ladder teardown + lease-gated retire
	FeatureLeases     = "leases"          // OpLeases + the health lease summary (leases_total/leases_held)
	FeatureListAll    = "list-all"        // all:true read-only cross-tenant view on list/bridges/leases
	FeatureWarning    = "persist-warning" // Response.Warning: OK replies can carry a journal persist-warning
	FeatureReplayDone = "replay-done"     // Response.ReplayDone: health reports whether the journal replay finished
	FeatureWedgedDirs = "wedged-dirs"     // Response.WedgedDirs: health lists dirs held wedged with their fences
)

// HolderFeatures is every feature this holder build serves.
var HolderFeatures = []string{FeatureMux, FeatureBridge, FeatureTree, FeatureLeaseGate, FeatureLeases, FeatureListAll, FeatureWarning, FeatureReplayDone, FeatureWedgedDirs}

// WedgeContractViolation suffixes a WedgedDirs entry whose wedge is a host
// contract violation: permanent for the holder's lifetime — no auto-release;
// restart the holder to clear it.
const WedgeContractViolation = " (contract-violation)"

// Request is one client request. Base and Dir are required by mount AND
// unmount: Teardown refuses base==dir, so even a carcass unmount (a
// mountpoint with no registry row) needs the base named. In tree mode
// (ContentMode "tree") Base is a NOMINAL identity key — the consumer's repo
// root — that the mount never reads; it still keys the registry, teardown,
// and unmount, so dir == base stays refused in every mode.
//
// Owner is REQUIRED (validOwner: a safe single path segment) on every op
// except hello, health, and probe.
type Request struct {
	Proto int    `json:"proto"`
	Op    Op     `json:"op"`
	Base  string `json:"base,omitempty"`
	Dir   string `json:"dir,omitempty"`
	Owner string `json:"owner,omitempty"`
	// All widens list/bridges to a read-only cross-tenant view (doctor).
	All bool `json:"all,omitempty"`
	// MuxRoot, when set, serves Dir as a logical subtree of ONE native mount at
	// MuxRoot instead of its own kernel mount (fusekit.MountSpec.MuxRoot). Dir
	// must be a direct child of MuxRoot; every mount sharing a MuxRoot shares
	// that one native mount. Consumers gate on FeatureMux.
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
	// is then the consumer daemon's own bridge the relay DIALS.
	BridgeSocket string `json:"bridge_socket,omitempty"`
	// AttrCache and AttrCacheTimeout tune the served mount's go-nfsv4 attr cache
	// (fusekit.MountSpec.AttrCache / .AttrCacheTimeout). AttrCacheTimeout is a
	// time.Duration on the wire (JSON nanoseconds); the holder converts to whole
	// seconds at the mount edge.
	AttrCache        bool          `json:"attr_cache,omitempty"`
	AttrCacheTimeout time.Duration `json:"attr_cache_timeout,omitempty"`
}

// MountInfo is one mount in a list response. Live is shallow kernel truth,
// not registry membership: dir is currently a mountpoint (overlay.Mounted)
// AND base's contents are visible through it (overlay.MountAlive) — either
// half alone can lie, since a dead mirror exposes the underlying dir whose
// leftover entries can shadow base's. Live does NOT reflect a partial wedge
// (shallow-alive but bulk reads hang): the holder ships no deep verdict; the
// daemon deep-probes through the kernel mount and keeps its own per-dir
// verdict.
type MountInfo struct {
	Dir  string `json:"dir"`
	Base string `json:"base"`
	Live bool   `json:"live"`
	// Epoch increments on each (re)mount of Dir by this holder, starting at 1.
	Epoch uint64 `json:"epoch,omitempty"`
	// MountedAt is the unix-seconds time of the current mount of Dir.
	MountedAt int64  `json:"mounted_at,omitempty"`
	Owner     string `json:"owner,omitempty"`
	// MuxRoot is Dir's native mount root when Dir is a logical subtree of a
	// shared mux mount; empty for a plain one-mount-per-dir row (Request.MuxRoot).
	MuxRoot string `json:"mux_root,omitempty"`
}

// LeaseInfo is one lease file in a leases response — the read-only diagnostic
// view of the lease dir (lease.List).
type LeaseInfo struct {
	File string `json:"file"`
	Held bool   `json:"held,omitempty"`
	// Advisory header fields — the ACQUIRER's provenance; fd inheritors are
	// not enumerable.
	Dir     string `json:"dir,omitempty"`
	Owner   string `json:"owner,omitempty"`
	PID     int    `json:"pid,omitempty"`
	Argv0   string `json:"argv0,omitempty"`
	Started int64  `json:"started,omitempty"` // unix seconds
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
	// ClassWedged: the mount's state could not be proven safe to act on — a
	// graceful unmount did not take, or a carcass verdict came back
	// undetermined (a hanging stat is never proof of death). The dir must not
	// be treated as torn down.
	ClassWedged = "wedged"
	// ClassForeignMount: dir is a mountpoint this holder does not own and that
	// answers stats healthily — a LIVE foreign mount; the caller must unmount
	// it first. The holder never stacks mounts.
	ClassForeignMount = "foreign-mount"
	// ClassBusy: the dir is in use — another operation is in flight, a session
	// lease is held (Error carries the lease provenance), or a graceful
	// unmount answered EBUSY. Retry once the user is gone.
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
	// ClassInvalidOwner: an Owner that is not a safe single path segment (it
	// names the on-disk spool dir and lease provenance). Non-retryable client
	// error.
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
	// ClassProtoMismatch: the request's proto is not the holder's. The Error
	// names the fix per direction: upgrade the consumer, or
	// `brew upgrade --cask fusekit-holder`.
	ClassProtoMismatch = "proto-mismatch"
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

// Response is one server reply.
type Response struct {
	Proto    int    `json:"proto"`
	OK       bool   `json:"ok"`
	Error    string `json:"error,omitempty"`
	ErrClass string `json:"err_class,omitempty"`
	// Warning is a non-fatal persist-warning: the kernel operation succeeded
	// (or, on an error reply, the rows already changed) but the journal write
	// behind it failed (retried; heals on the next write). health joins the
	// UNRESOLVED warnings recorded after their op returned — a parked bridge
	// removal's late flush. Additive within proto 2.
	Warning string `json:"warning,omitempty"`
	Version string `json:"version,omitempty"` // hello, health
	FuseOK  bool   `json:"fuse_ok,omitempty"` // probe
	// Features: hello returns the holder's capability set (HolderFeatures).
	Features []string `json:"features,omitempty"`
	// Mounts: list returns the scoped mounts; unmount/reclaim return the rows
	// that FAILED to come down (empty means a clean sweep).
	Mounts []MountInfo `json:"mounts,omitempty"`
	// Bridges: bridges/add/remove return the hosted content bridges, scoped by
	// Request.Owner (or All).
	Bridges []BridgeInfo `json:"bridges,omitempty"`
	// Leases: the leases op's read-only lease-file diagnostic.
	Leases []LeaseInfo `json:"leases,omitempty"`
	// Health status fields.
	// Retiring: the holder is draining for a self-retire; new mounts and
	// bridges answer ClassBusy.
	Retiring bool `json:"retiring,omitempty"`
	// ParkedUntil is the unix-seconds deadline of a retire-storm park; zero
	// means not parked.
	ParkedUntil int64 `json:"parked_until,omitempty"`
	// JournalMounts and JournalBridges count the journaled entries.
	JournalMounts  int `json:"journal_mounts,omitempty"`
	JournalBridges int `json:"journal_bridges,omitempty"`
	// ReplayDone: Run's journal replay finished (trivially true when
	// journaling is off); false while replay is still running behind the
	// freshly-bound socket. Health only (FeatureReplayDone).
	ReplayDone bool `json:"replay_done,omitempty"`
	// LeasesTotal and LeasesHeld summarize the lease dir (health).
	LeasesTotal int `json:"leases_total,omitempty"`
	LeasesHeld  int `json:"leases_held,omitempty"`
	// WedgedDirs lists dirs whose teardown resolved to a FINAL WEDGE: still
	// fenced and claimed (their lease fences pinned in server state) until
	// the wedge clears or the holder exits. A host-contract-violation wedge
	// — permanent; only a holder restart clears it — carries the
	// WedgeContractViolation suffix. Health only (FeatureWedgedDirs).
	WedgedDirs []string `json:"wedged_dirs,omitempty"`
	// RetireStrikes are the recorded retire-attempt times (unix seconds,
	// oldest first) still inside the strike window's history.
	RetireStrikes []int64 `json:"retire_strikes,omitempty"`
	// RetireDeferredDir and RetireDeferredReason surface a skewed holder whose
	// retire the lease gate is deferring (Retiring stays false by design — the
	// holder serves normally): the first busy dir, and the skew reason.
	// Empty when not skewed, or once a drain proceeds.
	RetireDeferredDir    string `json:"retire_deferred_dir,omitempty"`
	RetireDeferredReason string `json:"retire_deferred_reason,omitempty"`
}
