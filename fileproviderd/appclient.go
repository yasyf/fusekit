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

// Alias ErrAppUnavailable so one errors.Is matches both a failed spawn and a
// failed control op.
func init() { ErrAppUnavailable = proc.ErrChildUnavailable }

// NSFileProviderManager domain add/remove can take seconds to materialize a
// domain root, hence the generous register/remove bound.
const (
	controlDialTimeout   = 500 * time.Millisecond
	controlHealthTimeout = 2 * time.Second
	controlProbeTimeout  = 25 * time.Second
	controlPathTimeout   = 3 * time.Second
	controlSignalTimeout = 3 * time.Second
	controlDomainTimeout = 20 * time.Second
	// controlProbeDomainTimeout bounds one probe-domain call: the app enumerates
	// the domain and reads its .claude.json, which can park inside a materializing
	// appex — the readiness poll retries across the overall deadline. Sized ~20%
	// above the app's worst-case reply budget (~13s: lookup 1 + URL 3 + enumerate
	// 5 + read 4) so a slow-but-serving domain yields a verdict, not a timeout.
	controlProbeDomainTimeout = 16 * time.Second
)

// AppClient dials the companion app's control socket — one connection per op —
// and maps ErrClass responses to sentinel errors.
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

// wireErr wraps a mid-op control I/O failure as ErrAppUnavailable: the op's
// outcome is unknown, so it must never surface as a domain-level failure class.
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

// Probe asks the app whether File Provider can serve on this machine by
// registering, enumerating, and removing a throwaway domain.
func (c *AppClient) Probe(ctx context.Context) (bool, error) {
	resp, err := c.do(ctx, Request{Op: OpProbe}, controlProbeTimeout)
	if err != nil {
		return false, err
	}
	// An OK response with ErrClass set means the RPC worked but the throwaway
	// domain failed; respErr keys on non-OK, so reconstruct one to surface the class.
	if resp.ErrClass != "" {
		return false, respErr(&Response{ErrClass: resp.ErrClass, Error: resp.Error})
	}
	if err := respErr(resp); err != nil {
		return false, err
	}
	return resp.FPOK, nil
}

// ProbeDomain asks the app to enumerate the domain and read its .claude.json
// without a materializing filesystem read, returning the byte-count verdict: nil =
// the domain serves but .claude.json is absent; a pointer to 0 = present and empty;
// >0 = bytes actually read. errors.Is classes: ErrNoDomain (unregistered),
// ErrDomainNotServing (registered but not yet serving), ErrBusy.
//
// The skew contract lives HERE, on this op only: an app too old to know
// probe-domain answers its unknown-op default arm — ok:false with an EMPTY
// err_class — which this maps to ErrOpUnsupported so Setup can fail loudly and tell
// the operator to upgrade, never silently falling back to a raw filesystem read.
func (c *AppClient) ProbeDomain(ctx context.Context, domain string) (*int64, error) {
	resp, err := c.do(ctx, Request{Op: OpProbeDomain, Domain: domain}, controlProbeDomainTimeout)
	if err != nil {
		return nil, err
	}
	if !resp.OK && resp.ErrClass == "" {
		return nil, fmt.Errorf("%w: %s", ErrOpUnsupported, resp.Error)
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	return resp.JSONBytes, nil
}

// Register idempotently registers the domain, returning its user-visible root.
// errors.Is classes: ErrCannotControl (the only retreat condition),
// ErrRegisterFailed, ErrBusy.
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

// Path returns the user-visible root of an already-registered domain without
// re-registering. errors.Is class: ErrNoDomain.
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
