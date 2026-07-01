package mountd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/proc"
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
	// ErrUnknownClass: the holder sent an error class this client predates
	// (forward skew — the protocol's additive evolution path). Unclassifiable,
	// so drivers must fail toward retry, never fuse→symlink conversion.
	ErrUnknownClass = errors.New("unrecognized holder error class")
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
	case ClassContentUnavailable:
		sentinel = ErrContentUnavailable
	case "":
		return errors.New(resp.Error)
	default:
		return fmt.Errorf("%w (%s): %s", ErrUnknownClass, resp.ErrClass, resp.Error)
	}
	return fmt.Errorf("%w: %s", sentinel, resp.Error)
}

// Health probes the holder, returning its version.
func (c *Client) Health() (string, error) {
	resp, err := c.do(Request{Op: OpHealth}, 2*time.Second)
	if err != nil {
		return "", err
	}
	if err := respErr(resp); err != nil {
		return "", err
	}
	return resp.Version, nil
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
// Mount.
func (c *Client) AddMount(spec fusekit.MountSpec) error {
	resp, err := c.do(Request{
		Op:              OpMount,
		Base:            spec.Base,
		Dir:             spec.Dir,
		Owner:           c.Owner,
		ContentSocket:   spec.ContentSocket,
		Domain:          spec.Domain,
		PrivateRoot:     spec.PrivateRoot,
		ContentMode:     spec.ContentMode,
		ProbePath:       spec.ProbePath,
		PrivatePrefixes: spec.PrivatePrefixes,
	}, 25*time.Second)
	if err != nil {
		return err
	}
	return respErr(resp)
}

// Unmount asks the holder to unmount the mirror at dir. base is required even
// for a carcass unmount (teardown refuses base==dir). A dir not mounted at
// all is an OK no-op. errors.Is classes: ErrUnmountWedged, ErrBusy.
func (c *Client) Unmount(base, dir string) error {
	// Above the server's 15s OpUnmount deadline (opDeadline's coupling rule):
	// a slow wedge must surface ClassWedged — the dir is still a live
	// mountpoint — not blow the client deadline into ErrHolderUnavailable.
	resp, err := c.do(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: c.Owner}, 17*time.Second)
	if err != nil {
		return err
	}
	return respErr(resp)
}

// List returns the mounts the holder owns, with per-entry kernel liveness.
func (c *Client) List() ([]MountInfo, error) {
	resp, err := c.do(Request{Op: OpList, Owner: c.Owner}, 3*time.Second)
	if err != nil {
		return nil, err
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	return resp.Mounts, nil
}

// Reclaim unmounts every mount owned by this client's Owner, returning those that failed.
func (c *Client) Reclaim() ([]MountInfo, error) {
	resp, err := c.do(Request{Op: OpReclaim, Owner: c.Owner}, 65*time.Second)
	if err != nil {
		return nil, err
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	return resp.Mounts, nil
}

// Shutdown asks the holder to unmount everything and exit, returning the dirs
// that failed to come down. Use WaitGone to confirm the socket was released.
func (c *Client) Shutdown() ([]MountInfo, error) {
	resp, err := c.do(Request{Op: OpShutdown}, 65*time.Second)
	if err != nil {
		return nil, err
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	return resp.Mounts, nil
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
