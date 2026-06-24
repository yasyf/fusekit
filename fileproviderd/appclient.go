package fileproviderd

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/yasyf/fusekit/proc"
)

// ErrAppUnavailable is the control-domain alias of proc.ErrChildUnavailable —
// the generic spawn primitive and this client share one identity, so a
// consumer's errors.Is keeps matching whichever layer produced it (the spawn
// failed to bring the app up, or an established control op failed mid-flight).
// It is declared as the package sentinel in control.go and bound here, beside
// the client that produces it.
func init() { ErrAppUnavailable = proc.ErrChildUnavailable }

// Default control-op client timeouts. The companion app's domain ops touch
// NSFileProviderManager, whose add/remove can take a beat to materialize a
// domain root, so register/remove get a generous bound; health/path/signal are
// near-instant.
const (
	controlDialTimeout   = 500 * time.Millisecond
	controlHealthTimeout = 2 * time.Second
	controlProbeTimeout  = 25 * time.Second
	controlPathTimeout   = 3 * time.Second
	controlSignalTimeout = 3 * time.Second
	controlDomainTimeout = 20 * time.Second // register, remove
)

// AppClient is a short-lived connection to the companion app's control socket.
// One op per call: dial, send one request, decode one response, map ErrClass to
// a sentinel. It is the inverse of mountd.Client — there the Go side is the
// server (holder) and the consumer dials it; here the signed app is the server
// and Go dials in.
type AppClient struct {
	// Socket is the companion app's control socket path.
	Socket string
}

// NewAppClient returns a client for the given control socket path.
func NewAppClient(socket string) *AppClient { return &AppClient{Socket: socket} }

// Available reports whether the control socket accepts a connection.
func (c *AppClient) Available() bool {
	conn, err := net.DialTimeout("unix", c.Socket, controlDialTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// do sends one request and reads one response, bounded by ctx and timeout
// (whichever is sooner).
func (c *AppClient) do(ctx context.Context, req Request, timeout time.Duration) (*Response, error) {
	var d net.Dialer
	dialCtx, cancel := context.WithTimeout(ctx, controlDialTimeout)
	defer cancel()
	conn, err := d.DialContext(dialCtx, "unix", c.Socket)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAppUnavailable, err)
	}
	defer conn.Close()

	deadline := time.Now().Add(timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)

	req.Proto = ControlProtoVersion
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, wireErr("send request", err)
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return nil, wireErr("read response", err)
	}
	return &resp, nil
}

// wireErr wraps one I/O failure on an established control connection. Every such
// failure — a blown deadline, an EOF from an app that died mid-request — leaves
// the op's outcome unknown, the same condition as an unreachable app, so it maps
// to ErrAppUnavailable just like a dial failure and must never read as a
// domain-level failure class. The underlying error stays in the chain.
func wireErr(stage string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrAppUnavailable, stage, err)
}

// Health probes the app, returning its reported version.
func (c *AppClient) Health(ctx context.Context) (string, error) {
	resp, err := c.do(ctx, Request{Op: OpHealth}, controlHealthTimeout)
	if err != nil {
		return "", err
	}
	if err := respErr(resp); err != nil {
		return "", err
	}
	return resp.Version, nil
}

// Probe asks the app whether File Provider can serve on this machine: it
// registers a throwaway domain, enumerates it, and tears it down. The long
// timeout covers the OS materializing and removing a real domain.
func (c *AppClient) Probe(ctx context.Context) (bool, error) {
	resp, err := c.do(ctx, Request{Op: OpProbe}, controlProbeTimeout)
	if err != nil {
		return false, err
	}
	// A probe whose RPC succeeded (OK) but whose throwaway domain failed carries
	// that failure's class in ErrClass: surface it so the caller distinguishes a
	// permanent ClassNoEntitlement (retreat) from a transient ClassRegisterFailed
	// (retry). respErr keys on a non-OK response, so reconstruct one.
	if resp.ErrClass != "" {
		return false, respErr(&Response{ErrClass: resp.ErrClass, Error: resp.Error})
	}
	if err := respErr(resp); err != nil {
		return false, err
	}
	return resp.FPOK, nil
}

// Register ensures a domain for the given identifier is registered, returning
// its user-visible domain root. Idempotent: an already-registered domain
// returns its existing root. errors.Is classes: ErrCannotControl (the only
// retreat condition), ErrRegisterFailed, ErrBusy.
func (c *AppClient) Register(ctx context.Context, domain string) (string, error) {
	resp, err := c.do(ctx, Request{Op: OpRegister, Domain: domain}, controlDomainTimeout)
	if err != nil {
		return "", err
	}
	if err := respErr(resp); err != nil {
		return "", err
	}
	return resp.Path, nil
}

// Path returns the user-visible domain root for an already-registered domain
// without re-registering. errors.Is class: ErrNoDomain when the app has no
// registration for it.
func (c *AppClient) Path(ctx context.Context, domain string) (string, error) {
	resp, err := c.do(ctx, Request{Op: OpPath, Domain: domain}, controlPathTimeout)
	if err != nil {
		return "", err
	}
	if err := respErr(resp); err != nil {
		return "", err
	}
	return resp.Path, nil
}

// Signal tells the app to signal the domain's enumerator so the OS
// re-enumerates after a backing-tree change. errors.Is class: ErrNoDomain.
func (c *AppClient) Signal(ctx context.Context, domain string) error {
	resp, err := c.do(ctx, Request{Op: OpSignal, Domain: domain}, controlSignalTimeout)
	if err != nil {
		return err
	}
	return respErr(resp)
}

// Remove deregisters the domain. A domain that is not registered is an OK
// no-op. errors.Is classes: ErrCannotControl, ErrRegisterFailed, ErrBusy.
func (c *AppClient) Remove(ctx context.Context, domain string) error {
	resp, err := c.do(ctx, Request{Op: OpRemove, Domain: domain}, controlDomainTimeout)
	if err != nil {
		return err
	}
	return respErr(resp)
}
