package mountd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit"
)

// ErrHolderUnavailable means the mount-holder socket could not be reached;
// it aliases proc.ErrChildUnavailable so errors.Is matches either layer.
var ErrHolderUnavailable = proc.ErrChildUnavailable

// Wire error-class sentinels: respErr maps Response.ErrClass onto these for
// errors.Is; the holder's raw human-facing Error stays in the message.
// Per-class semantics: protocol.go.
var (
	// ErrTCCDenied: the mount was issued but never came live — almost always
	// the one-time macOS volume-access grant.
	ErrTCCDenied = errors.New("mount blocked pending TCC grant")
	// ErrMountTimeout: the mount did not come live under an already-proven
	// grant — NOT the TCC condition. Transient: drivers retry; never grounds
	// to convert.
	ErrMountTimeout = errors.New("mount timed out under a proven grant")
	// ErrMountFailed: the mount failed outright.
	ErrMountFailed = errors.New("mount failed")
	// ErrUnmountWedged: the unmount did not take; the dir is still a live
	// mountpoint and must not be treated as torn down.
	ErrUnmountWedged = errors.New("unmount wedged")
	// ErrForeignMount: the dir is a mountpoint the holder does not own; it
	// must be unmounted before the holder will mount there.
	ErrForeignMount = errors.New("foreign mount in the way")
	// ErrBusy: another operation is in flight on the same dir. Transient;
	// safe to retry once it completes.
	ErrBusy = errors.New("dir busy with another operation")
	// ErrBaseMismatch: the dir is already held but mirrors a different base.
	// Registry state, never a mount verdict: drivers unmount-then-retry
	// (handleUnmount tears down by the REGISTERED base); never grounds to
	// convert.
	ErrBaseMismatch = errors.New("dir already mirrors a different base")
	// ErrContentUnavailable: the consumer's content bridge was unreachable.
	// Transient, NOT a mount verdict: drivers retry, never convert.
	ErrContentUnavailable = errors.New("mount blocked: consumer content bridge unavailable")
	// ErrMuxMismatch: a mux-mode mount could not join its MuxRoot's native mount.
	// Registry state, never a mount verdict (like ErrBaseMismatch): drivers
	// unmount the conflicting root/dir and retry, never convert. overlayClass
	// re-wraps it with fusekit.ErrMuxMismatch so mountd-agnostic callers match
	// the same sentinel the in-process host raises.
	ErrMuxMismatch = errors.New("mux mount cannot join its root")
	// ErrForeignBridge: AddBridge named a bridge socket already bound by a
	// different owner. Registry state, never a content verdict.
	ErrForeignBridge = errors.New("bridge socket already bound by another owner")
	// ErrInvalidOwner: a bridge Owner that is not a safe single path segment.
	// Non-retryable — a caller bug, not a transient condition.
	ErrInvalidOwner = errors.New("invalid bridge owner")
	// ErrBridgeSocketChanged: a same-owner AddBridge changed the bind socket.
	// Non-retryable — RemoveBridge, then add the new socket.
	ErrBridgeSocketChanged = errors.New("bridge bind socket changed; remove first")
	// ErrOwnerMismatch: an unmount or remove-bridge named a row registered to
	// a different owner. A misfire guard between cooperating consumers over a
	// same-UID socket — Owner is client-asserted, so this is NOT a security
	// boundary. Non-retryable: the owning consumer tears its own rows down.
	ErrOwnerMismatch = errors.New("row registered to a different owner")
	// ErrUnknownClass: the holder sent an error class this client predates
	// (forward skew — the protocol's additive evolution path). Unclassifiable,
	// so drivers must fail toward retry, never fuse→symlink conversion.
	ErrUnknownClass = errors.New("unrecognized holder error class")
	// ErrProtoMismatch: the holder speaks a different protocol generation.
	// Backward skew (a proto-1 holder answering this proto-2 client) is fixed
	// by `brew upgrade --cask fusekit-holder`; forward skew by upgrading the
	// consumer.
	ErrProtoMismatch = errors.New("holder protocol mismatch")
)

// Client is a short-lived connection to the mount-holder socket.
type Client struct {
	// Socket is the holder's unix socket path.
	Socket string
	Owner  string
}

// NewClient returns a client for the given holder socket path.
func NewClient(socket string) *Client { return &Client{Socket: socket} }

// Available reports whether the holder socket accepts a connection.
func (c *Client) Available() bool {
	conn, err := net.DialTimeout("unix", c.Socket, 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (c *Client) do(req Request, timeout time.Duration) (*Response, error) {
	conn, err := net.DialTimeout("unix", c.Socket, 500*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrHolderUnavailable, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	req.Proto = MountProtoVersion
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, wireErr("send request", err)
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return nil, wireErr("read response", err)
	}
	if resp.Proto != MountProtoVersion {
		// Remediation names the OLDER side: a holder answering a NEWER proto
		// means this consumer must upgrade, not the holder.
		fix := "`brew upgrade --cask fusekit-holder`"
		if resp.Proto > MountProtoVersion {
			fix = "upgrade this consumer"
		}
		return nil, fmt.Errorf("%w: holder answered proto %d, this client requires %d; %s", ErrProtoMismatch, resp.Proto, MountProtoVersion, fix)
	}
	return &resp, nil
}

// wireErr maps I/O failures on a live holder connection (blown deadline, or
// EOF when a fuse-t fault inside Setup kills the holder mid-request) to
// ErrHolderUnavailable: the op's outcome is unknown and must never read as an
// op-level failure class.
func wireErr(stage string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrHolderUnavailable, stage, err)
}

func respErr(resp *Response) error {
	if resp.OK {
		return nil
	}
	var sentinel error
	switch resp.ErrClass {
	case ClassTCC:
		sentinel = ErrTCCDenied
	case ClassMountTimeout:
		sentinel = ErrMountTimeout
	case ClassMountFailed:
		sentinel = ErrMountFailed
	case ClassWedged:
		sentinel = ErrUnmountWedged
	case ClassForeignMount:
		sentinel = ErrForeignMount
	case ClassBusy:
		sentinel = ErrBusy
	case ClassBaseMismatch:
		sentinel = ErrBaseMismatch
	case ClassContentUnavailable, ClassContentDialRefused:
		sentinel = ErrContentUnavailable
	case ClassMuxMismatch:
		sentinel = ErrMuxMismatch
	case ClassForeignBridge:
		sentinel = ErrForeignBridge
	case ClassInvalidOwner:
		sentinel = ErrInvalidOwner
	case ClassBridgeSocketChanged:
		sentinel = ErrBridgeSocketChanged
	case ClassOwnerMismatch:
		sentinel = ErrOwnerMismatch
	case ClassProtoMismatch:
		sentinel = ErrProtoMismatch
	case "":
		return errors.New(resp.Error)
	default:
		return fmt.Errorf("%w (%s): %s", ErrUnknownClass, resp.ErrClass, resp.Error)
	}
	return fmt.Errorf("%w: %s", sentinel, resp.Error)
}

// healthClientTimeout bounds Health and Status: short — health sits on
// liveness hot paths — and above the server's 1s OpHealth deadline
// (opDeadline's coupling rule).
const healthClientTimeout = 2 * time.Second

// Health probes the holder, returning its version.
func (c *Client) Health() (string, error) {
	resp, err := c.do(Request{Op: OpHealth}, healthClientTimeout)
	if err != nil {
		return "", err
	}
	if err := respErr(resp); err != nil {
		return "", err
	}
	return resp.Version, nil
}

// HealthStatus is OpHealth's read-only holder snapshot for doctor/status
// surfaces. A holder predating the status fields reports zero values.
type HealthStatus struct {
	Version string
	// Retiring: the holder is draining for a self-retire; new mounts and
	// bridges bounce retryable ClassBusy.
	Retiring bool
	// ParkedUntil is the retire-storm park deadline; zero means not parked.
	ParkedUntil time.Time
	// JournalMounts and JournalBridges count the journaled entries.
	JournalMounts  int
	JournalBridges int
	// LeasesTotal and LeasesHeld summarize the lease dir (FeatureLeases).
	LeasesTotal int
	LeasesHeld  int
	// ReplayDone: the holder's journal replay finished (FeatureReplayDone) —
	// the deterministic "serving from a settled registry" signal.
	ReplayDone bool
	// RetireStrikes are the recorded retire-attempt times, oldest first.
	RetireStrikes []time.Time
	// RetireDeferredDir and RetireDeferredReason surface a skewed holder whose
	// retire the idle gate is deferring (Retiring stays false): the first
	// non-idle dir, and the skew reason.
	RetireDeferredDir    string
	RetireDeferredReason string
	// WedgedDirs lists dirs held in a FINAL WEDGE — fenced and claimed until
	// the wedge clears or the holder exits; a permanent contract-violation
	// entry carries the WedgeContractViolation suffix (FeatureWedgedDirs).
	WedgedDirs []string
	// ContentDeferred lists journal rows whose replay is deferred on a
	// refusing content socket, annotated with the socket
	// (FeatureContentDeferred).
	ContentDeferred []string
	// Warning joins the holder's unresolved journal persist-warnings
	// (FeatureWarning): durable state is stale until a later save resolves it.
	Warning string
}

// Status probes the holder's health snapshot: version plus the self-retire
// and journal state.
func (c *Client) Status() (*HealthStatus, error) {
	resp, err := c.do(Request{Op: OpHealth}, healthClientTimeout)
	if err != nil {
		return nil, err
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	st := &HealthStatus{
		Version:              resp.Version,
		Retiring:             resp.Retiring,
		JournalMounts:        resp.JournalMounts,
		JournalBridges:       resp.JournalBridges,
		LeasesTotal:          resp.LeasesTotal,
		LeasesHeld:           resp.LeasesHeld,
		ReplayDone:           resp.ReplayDone,
		RetireDeferredDir:    resp.RetireDeferredDir,
		RetireDeferredReason: resp.RetireDeferredReason,
		WedgedDirs:           resp.WedgedDirs,
		ContentDeferred:      resp.ContentDeferred,
		Warning:              resp.Warning,
	}
	if resp.ParkedUntil != 0 {
		st.ParkedUntil = time.Unix(resp.ParkedUntil, 0)
	}
	for _, sec := range resp.RetireStrikes {
		st.RetireStrikes = append(st.RetireStrikes, time.Unix(sec, 0))
	}
	return st, nil
}

// HelloInfo is the holder's OpHello capability handshake.
type HelloInfo struct {
	Version  string
	Features []string
}

// Has reports whether the holder serves feature.
func (h *HelloInfo) Has(feature string) bool {
	return slices.Contains(h.Features, feature)
}

// Require fails when any of features is missing — the consumer-side
// capability gate that replaces version arithmetic.
func (h *HelloInfo) Require(features ...string) error {
	for _, f := range features {
		if !h.Has(f) {
			return fmt.Errorf("holder %s lacks feature %q (has %v); `brew upgrade --cask fusekit-holder`", h.Version, f, h.Features)
		}
	}
	return nil
}

// Hello negotiates capabilities: the holder's version and feature set. A
// proto-1 holder fails with ErrProtoMismatch naming the cask upgrade.
func (c *Client) Hello() (*HelloInfo, error) {
	resp, err := c.do(Request{Op: OpHello}, healthClientTimeout)
	if err != nil {
		return nil, err
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	return &HelloInfo{Version: resp.Version, Features: resp.Features}, nil
}

// Leases returns this owner's lease-file diagnostic: lease files whose
// advisory header names c.Owner, with held/free state (FeatureLeases).
func (c *Client) Leases() ([]LeaseInfo, error) {
	return c.leases(false)
}

// LeasesAll is the read-only cross-tenant lease view (FeatureListAll) for
// doctor surfaces.
func (c *Client) LeasesAll() ([]LeaseInfo, error) {
	return c.leases(true)
}

func (c *Client) leases(all bool) ([]LeaseInfo, error) {
	resp, err := c.do(Request{Op: OpLeases, Owner: c.Owner, All: all}, 12*time.Second)
	if err != nil {
		return nil, err
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	return resp.Leases, nil
}

// Probe asks the holder whether it can host fuse mounts. The holder performs
// a real throwaway mount (which may sit on the one-time TCC prompt), hence
// the long timeout.
func (c *Client) Probe() (bool, error) {
	resp, err := c.do(Request{Op: OpProbe}, 25*time.Second)
	if err != nil {
		return false, err
	}
	// An OK probe whose throwaway mount failed carries that mount's class in
	// ErrClass: surface it so drivers distinguish a hard ErrMountFailed from a
	// pending ErrTCCDenied. respErr keys on non-OK, so reconstruct one.
	if resp.ErrClass != "" {
		return false, respErr(&Response{ErrClass: resp.ErrClass, Error: resp.Error})
	}
	if err := respErr(resp); err != nil {
		return false, err
	}
	return resp.FuseOK, nil
}

// Mount asks the holder to ensure a live mirror of base at dir: a fresh dir
// is mounted, the exact pair held AND live is an idempotent OK, and a
// held-but-dead (or deep-wedged) mirror is torn down and remounted.
// errors.Is classes: ErrTCCDenied, ErrMountTimeout, ErrMountFailed,
// ErrForeignMount, ErrBaseMismatch, ErrBusy, ErrUnmountWedged (dead-mirror
// teardown failed).
func (c *Client) Mount(base, dir string) error {
	// Above the server's 20s OpMount deadline (opDeadline's coupling rule) so
	// the holder's error class, not ErrHolderUnavailable, reaches the driver.
	resp, err := c.do(Request{Op: OpMount, Base: base, Dir: dir, Owner: c.Owner}, 25*time.Second)
	if err != nil {
		return err
	}
	return respErr(resp)
}

// AddMount is the content-aware Mount: spec's bridge wiring lets the holder
// serve the consumer's synthetic entries over RPC; a bare spec is exactly a
// Mount. The wire owner is ALWAYS Client.Owner; a non-empty spec.Owner that
// disagrees is refused here, crisply, rather than silently preferring one.
func (c *Client) AddMount(spec fusekit.MountSpec) error {
	if spec.Owner != "" && spec.Owner != c.Owner {
		return fmt.Errorf("mount %s: MountSpec.Owner %q disagrees with Client.Owner %q — the wire owner is Client.Owner; set them equal or leave spec.Owner empty", spec.Dir, spec.Owner, c.Owner)
	}
	resp, err := c.do(Request{
		Op:               OpMount,
		Base:             spec.Base,
		Dir:              spec.Dir,
		Owner:            c.Owner,
		MuxRoot:          spec.MuxRoot,
		ContentSocket:    spec.ContentSocket,
		Domain:           spec.Domain,
		PrivateRoot:      spec.PrivateRoot,
		ContentMode:      spec.ContentMode,
		ProbePath:        spec.ProbePath,
		PrivatePrefixes:  spec.PrivatePrefixes,
		AttrCache:        spec.AttrCache,
		AttrCacheTimeout: spec.AttrCacheTimeout,
	}, 25*time.Second)
	if err != nil {
		return err
	}
	return respErr(resp)
}

// Unmount asks the holder to unmount the mirror at dir via the lease ladder:
// a held session lease answers ErrBusy with the acquirer's provenance, a free
// lease is seized across a GRACEFUL teardown, and a teardown that does not
// take answers ErrUnmountWedged — the holder never force-unmounts on this
// path. base is required even for a carcass unmount (teardown refuses
// base==dir). A dir not mounted at all is an OK no-op; a dir registered to a
// different owner is refused. warning is a non-empty journal persist-warning
// on a SUCCESSFUL unmount (FeatureWarning): the kernel state changed but the
// holder's durable mirror is stale until its next save — surface it, a
// successor could otherwise replay the reclaimed mount. errors.Is classes:
// ErrUnmountWedged, ErrBusy, ErrOwnerMismatch.
func (c *Client) Unmount(base, dir string) (warning string, err error) {
	// Above the server's 15s OpUnmount deadline (opDeadline's coupling rule):
	// a slow wedge must surface ClassWedged — the dir is still a live
	// mountpoint — not blow the client deadline into ErrHolderUnavailable.
	resp, err := c.do(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: c.Owner}, 17*time.Second)
	if err != nil {
		return "", err
	}
	if err := respErr(resp); err != nil {
		return "", err
	}
	return resp.Warning, nil
}

// List returns this owner's mounts, with per-entry kernel liveness.
func (c *Client) List() ([]MountInfo, error) {
	return c.list(false)
}

// ListAll is the read-only cross-tenant mount view (FeatureListAll) for
// doctor surfaces.
func (c *Client) ListAll() ([]MountInfo, error) {
	return c.list(true)
}

func (c *Client) list(all bool) ([]MountInfo, error) {
	resp, err := c.do(Request{Op: OpList, Owner: c.Owner, All: all}, 3*time.Second)
	if err != nil {
		return nil, err
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	return resp.Mounts, nil
}

// Reclaim unmounts every mount owned by this client's Owner (and its
// bridge), returning those that failed plus any aggregated journal
// persist-warning (FeatureWarning; see Unmount).
func (c *Client) Reclaim() (failed []MountInfo, warning string, err error) {
	resp, err := c.do(Request{Op: OpReclaim, Owner: c.Owner}, 65*time.Second)
	if err != nil {
		return nil, "", err
	}
	if err := respErr(resp); err != nil {
		return nil, "", err
	}
	return resp.Mounts, resp.Warning, nil
}

// AddBridge asks the holder to host this owner's content bridge: bind
// bridgeSocket (the appex-facing socket) and relay it to contentSocket (the
// consumer daemon's own bridge), classifying with privatePrefixes. Idempotent
// for the same owner (adopt); a foreign owner on bridgeSocket answers
// ErrForeignBridge. Returns the owner's bridge listing plus any journal
// persist-warning (FeatureWarning; see Unmount).
func (c *Client) AddBridge(bridgeSocket, contentSocket string, privatePrefixes []string) (bridges []BridgeInfo, warning string, err error) {
	resp, err := c.do(Request{
		Op:              OpAddBridge,
		Owner:           c.Owner,
		BridgeSocket:    bridgeSocket,
		ContentSocket:   contentSocket,
		PrivatePrefixes: privatePrefixes,
	}, 10*time.Second)
	if err != nil {
		return nil, "", err
	}
	if err := respErr(resp); err != nil {
		return nil, "", err
	}
	return resp.Bridges, resp.Warning, nil
}

// RemoveBridge stops and drains this owner's hosted bridge, returning the
// remaining bridge listing for the owner (empty on success) plus any journal
// persist-warning (FeatureWarning; see Unmount).
func (c *Client) RemoveBridge() (bridges []BridgeInfo, warning string, err error) {
	resp, err := c.do(Request{Op: OpRemoveBridge, Owner: c.Owner}, 10*time.Second)
	if err != nil {
		return nil, "", err
	}
	if err := respErr(resp); err != nil {
		return nil, "", err
	}
	return resp.Bridges, resp.Warning, nil
}

// Bridges returns this owner's hosted content bridges, with per-bridge state
// and pending-write depth.
func (c *Client) Bridges() ([]BridgeInfo, error) {
	return c.bridges(false)
}

// BridgesAll is the read-only cross-tenant bridge view (FeatureListAll) for
// doctor surfaces.
func (c *Client) BridgesAll() ([]BridgeInfo, error) {
	return c.bridges(true)
}

func (c *Client) bridges(all bool) ([]BridgeInfo, error) {
	resp, err := c.do(Request{Op: OpBridges, Owner: c.Owner, All: all}, 3*time.Second)
	if err != nil {
		return nil, err
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	return resp.Bridges, nil
}

// WaitGone polls until the socket stops accepting connections or timeout
// elapses, reporting whether it went dead.
func (c *Client) WaitGone(timeout time.Duration) bool {
	return c.WaitGoneContext(context.Background(), timeout)
}

// WaitGoneContext is WaitGone bounded by ctx as well as timeout, so a daemon
// exiting mid-wait need not stall the full timeout (~70s per skew-replace
// leg). Kernel truth wins over cancellation: a socket observed dead reports
// true even under a done ctx.
func (c *Client) WaitGoneContext(ctx context.Context, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("unix", c.Socket, 200*time.Millisecond)
		if err != nil {
			return true
		}
		conn.Close()
		select {
		case <-ctx.Done():
			return false
		case <-time.After(100 * time.Millisecond):
		}
	}
	return false
}
