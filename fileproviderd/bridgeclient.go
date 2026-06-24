package fileproviderd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"
)

// ErrBridgeUnavailable means the bridge data socket could not be reached, or an
// established bridge connection failed mid-op. Like ErrAppUnavailable on the
// control wire it is a transient availability condition — the consumer's daemon
// may be mid-restart — never a content verdict. It is a DISTINCT sentinel: the
// data socket and the control socket fail independently, so a caller can tell a
// dead bridge from a dead app.
var ErrBridgeUnavailable = errors.New("bridge data socket not reachable")

// bridgeDialTimeout and bridgeOpTimeout bound a bridge round-trip. Reads/writes
// of the few computed items are small and local (same disk), so the op bound is
// tight.
const (
	bridgeDialTimeout = 500 * time.Millisecond
	bridgeOpTimeout   = 5 * time.Second
)

// BridgeClient is a short-lived connection to the bridge data socket. It is the
// Go client of the content bridge — the same role the sandboxed extension plays
// from Swift — used by fusekit's tests and a consumer's doctor round-trip to
// exercise the BridgeServer end to end without a real domain.
type BridgeClient struct {
	// Socket is the bridge data socket path.
	Socket string
}

// NewBridgeClient returns a client for the given bridge socket path.
func NewBridgeClient(socket string) *BridgeClient { return &BridgeClient{Socket: socket} }

// Available reports whether the bridge socket accepts a connection.
func (c *BridgeClient) Available() bool {
	conn, err := net.DialTimeout("unix", c.Socket, bridgeDialTimeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// do sends one request and reads one response, bounded by ctx and bridgeOpTimeout.
func (c *BridgeClient) do(ctx context.Context, req BridgeRequest) (*BridgeResponse, error) {
	var d net.Dialer
	dialCtx, cancel := context.WithTimeout(ctx, bridgeDialTimeout)
	defer cancel()
	conn, err := d.DialContext(dialCtx, "unix", c.Socket)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBridgeUnavailable, err)
	}
	defer conn.Close()

	deadline := time.Now().Add(bridgeOpTimeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)

	req.Proto = BridgeProtoVersion
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, fmt.Errorf("%w: send request: %w", ErrBridgeUnavailable, err)
	}
	var resp BridgeResponse
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		return nil, fmt.Errorf("%w: read response: %w", ErrBridgeUnavailable, err)
	}
	return &resp, nil
}

// bridgeRespErr turns a non-OK bridge response into a plain error (the bridge
// wire carries no classes — see BridgeResponse).
func bridgeRespErr(resp *BridgeResponse) error {
	if resp.OK {
		return nil
	}
	return errors.New(resp.Error)
}

// Manifest fetches the domain's top-level Entry list.
func (c *BridgeClient) Manifest(ctx context.Context, domain string) ([]Entry, error) {
	resp, err := c.do(ctx, BridgeRequest{Op: BridgeOpManifest, Domain: domain})
	if err != nil {
		return nil, err
	}
	if err := bridgeRespErr(resp); err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

// Read fetches the computed bytes for a synth entry.
func (c *BridgeClient) Read(ctx context.Context, domain, name string) ([]byte, error) {
	resp, err := c.do(ctx, BridgeRequest{Op: BridgeOpRead, Domain: domain, Name: name})
	if err != nil {
		return nil, err
	}
	if err := bridgeRespErr(resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// Write persists a write to a synth entry.
func (c *BridgeClient) Write(ctx context.Context, domain, name string, data []byte) error {
	resp, err := c.do(ctx, BridgeRequest{Op: BridgeOpWrite, Domain: domain, Name: name, Data: data})
	if err != nil {
		return err
	}
	return bridgeRespErr(resp)
}

// Classify reports how a top-level name should be served.
func (c *BridgeClient) Classify(ctx context.Context, name string) (EntryKind, error) {
	resp, err := c.do(ctx, BridgeRequest{Op: BridgeOpClassify, Name: name})
	if err != nil {
		return "", err
	}
	if err := bridgeRespErr(resp); err != nil {
		return "", err
	}
	return EntryKind(resp.Kind), nil
}
