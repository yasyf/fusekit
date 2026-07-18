package fileproviderd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

// Alias ErrAppUnavailable so one errors.Is matches both a failed spawn and a
// failed control op.
func init() {
	ErrAppUnavailable = proc.ErrChildUnavailable
	ErrAppDialRefused = fmt.Errorf("%w (dial refused or socket absent)", ErrAppUnavailable)
}

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
	// controlListDomainsTimeout bounds one list-domains call: getDomains is an
	// in-process platform query, no appex materialization on the path.
	controlListDomainsTimeout = 5 * time.Second
	// controlPrepareDomainDefaultDeadline mirrors the app-side default the app
	// applies when PrepareDomain is called with a zero deadline, so the wire I/O
	// budget matches the work the app will do.
	controlPrepareDomainDefaultDeadline = 30 * time.Second
	// controlPrepareDomainSlack pads the wire I/O budget above the app-side
	// materialization deadline so the app's own timeout reply arrives before the
	// client gives up mid-op (which would read as ErrAppUnavailable, not the domain
	// verdict).
	controlPrepareDomainSlack = 5 * time.Second
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
		sentinel := ErrAppUnavailable
		if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOENT) {
			sentinel = ErrAppDialRefused
		}
		return nil, fmt.Errorf("%w: %v", sentinel, err)
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

// ProbeDomainShallow asks the app for a NON-materializing readiness verdict —
// domain lookup + getUserVisibleURL + a readdir of the domain root only, no byte
// read — and reports whether .claude.json appears in the listing. Error classes
// map exactly as ProbeDomain, including the unknown-op default arm (ok:false,
// EMPTY err_class) -> ErrOpUnsupported. An app too old to know the shallow flag
// answers a DEEP probe-domain (Listed absent); the verdict is then derived from
// the deep byte-count shape — JSONBytes nil (absent) = not listed, non-nil = listed.
func (c *AppClient) ProbeDomainShallow(ctx context.Context, domain string) (bool, error) {
	resp, err := c.do(ctx, Request{Op: OpProbeDomain, Domain: domain, Shallow: true}, controlProbeDomainTimeout)
	if err != nil {
		return false, err
	}
	if !resp.OK && resp.ErrClass == "" {
		return false, fmt.Errorf("%w: %s", ErrOpUnsupported, resp.Error)
	}
	if err := respErr(resp); err != nil {
		return false, err
	}
	if resp.Listed != nil {
		return *resp.Listed, nil
	}
	return resp.JSONBytes != nil, nil
}

// ListDomains returns every File Provider domain the platform has registered
// for the app, orphans included. Like ProbeDomain it maps the app's
// unknown-op default arm (ok:false, empty err_class) to ErrOpUnsupported —
// an operator-actionable "upgrade the app", never a silent empty list.
func (c *AppClient) ListDomains(ctx context.Context) ([]DomainInfo, error) {
	resp, err := c.do(ctx, Request{Op: OpListDomains}, controlListDomainsTimeout)
	if err != nil {
		return nil, err
	}
	if !resp.OK && resp.ErrClass == "" {
		return nil, fmt.Errorf("%w: %s", ErrOpUnsupported, resp.Error)
	}
	if err := respErr(resp); err != nil {
		return nil, err
	}
	return resp.Domains, nil
}

// PrepareDomain asks the app to force-materialize the domain's computed
// settings.json (requestDownloadForItem then a wait bounded by deadline; 0 = the
// app's default) so a live session's first read never blocks on a cold fetch. Like
// ProbeDomain and ListDomains it maps the unknown-op default arm (ok:false, empty
// err_class) to ErrOpUnsupported; a timed-out or failed download is
// ErrDomainNotServing, and other classes route through the standard classToErr path.
func (c *AppClient) PrepareDomain(ctx context.Context, domain string, deadline time.Duration) error {
	// Floor a positive deadline to 1ms: a 0 omits the field, so the app applies its
	// larger default while our budget would stay tiny and quit before it answers.
	var ms int64
	if deadline > 0 {
		ms = max(1, deadline.Milliseconds())
	}
	// Budget for the deadline the app will honor (ms, or its default when omitted).
	wire := controlPrepareDomainDefaultDeadline
	if ms > 0 {
		wire = time.Duration(ms) * time.Millisecond
	}
	resp, err := c.do(ctx, Request{Op: OpPrepareDomain, Domain: domain, DeadlineMS: ms}, wire+controlPrepareDomainSlack)
	if err != nil {
		return err
	}
	if !resp.OK && resp.ErrClass == "" {
		return fmt.Errorf("%w: %s", ErrOpUnsupported, resp.Error)
	}
	return respErr(resp)
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
