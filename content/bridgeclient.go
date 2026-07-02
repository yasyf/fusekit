package content

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"time"
)

// ErrBridgeUnavailable means the bridge data socket could not be reached or a
// connection failed mid-op — a transient condition (the consumer's daemon may be
// mid-restart), never a content verdict.
var ErrBridgeUnavailable = errors.New("bridge data socket not reachable")

// bridgeDialTimeout and bridgeOpTimeout bound a bridge round-trip; the few
// computed items are small and local, so the op bound is tight.
const (
	bridgeDialTimeout = 500 * time.Millisecond
	bridgeOpTimeout   = 5 * time.Second
)

// BridgeClient is a short-lived Go client of the bridge data socket — the role
// the sandboxed extension plays from Swift — used by fusekit's tests and a
// consumer's doctor round-trip.
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

// bridgeErr is the client-side error for a not-OK response, carrying the wire
// ErrClass so a caller can errors.As it to a ClassedError.
type bridgeErr struct {
	msg   string
	class string
}

func (e *bridgeErr) Error() string { return e.msg }
func (e *bridgeErr) Class() string { return e.class }

func bridgeRespErr(resp *BridgeResponse) error {
	if resp.OK {
		return nil
	}
	return &bridgeErr{msg: resp.Error, class: resp.ErrClass}
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

// Stat returns the entry metadata for a Tree consumer's name.
func (c *BridgeClient) Stat(ctx context.Context, domain, name string) (Entry, error) {
	resp, err := c.do(ctx, BridgeRequest{Op: BridgeOpStat, Domain: domain, Name: name})
	if err != nil {
		return Entry{}, err
	}
	if err := bridgeRespErr(resp); err != nil {
		return Entry{}, err
	}
	if resp.Item == nil {
		return Entry{}, errors.New("stat: ok response carried no item")
	}
	return *resp.Item, nil
}

// List returns the child entries of a Tree consumer's dir name.
func (c *BridgeClient) List(ctx context.Context, domain, name string) ([]Entry, error) {
	resp, err := c.do(ctx, BridgeRequest{Op: BridgeOpList, Domain: domain, Name: name})
	if err != nil {
		return nil, err
	}
	if err := bridgeRespErr(resp); err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

// ReadAt returns up to size bytes of a Tree consumer's name from ofst.
func (c *BridgeClient) ReadAt(ctx context.Context, domain, name string, ofst int64, size int) ([]byte, error) {
	resp, err := c.do(ctx, BridgeRequest{Op: BridgeOpReadAt, Domain: domain, Name: name, Ofst: ofst, Size: size})
	if err != nil {
		return nil, err
	}
	if err := bridgeRespErr(resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// Readlink returns the target of a Tree consumer's symlink name.
func (c *BridgeClient) Readlink(ctx context.Context, domain, name string) (string, error) {
	resp, err := c.do(ctx, BridgeRequest{Op: BridgeOpReadlink, Domain: domain, Name: name})
	if err != nil {
		return "", err
	}
	if err := bridgeRespErr(resp); err != nil {
		return "", err
	}
	return resp.Target, nil
}

// doOK sends req and reduces the reply to an error — the shape of every
// mutation, whose success carries no payload.
func (c *BridgeClient) doOK(ctx context.Context, req BridgeRequest) error {
	resp, err := c.do(ctx, req)
	if err != nil {
		return err
	}
	return bridgeRespErr(resp)
}

// Create asks a WritableTree consumer to create name as an empty file.
func (c *BridgeClient) Create(ctx context.Context, domain, name string) error {
	return c.doOK(ctx, BridgeRequest{Op: BridgeOpCreate, Domain: domain, Name: name})
}

// WriteAt writes data at ofst into a WritableTree consumer's name, path-wise
// (no handle token). All-or-error: no partial-write count crosses the wire.
func (c *BridgeClient) WriteAt(ctx context.Context, domain, name string, ofst int64, data []byte) error {
	return c.doOK(ctx, BridgeRequest{Op: BridgeOpWriteAt, Domain: domain, Name: name, Ofst: ofst, Data: data})
}

// Truncate resizes a WritableTree consumer's name to size, path-wise.
func (c *BridgeClient) Truncate(ctx context.Context, domain, name string, size int64) error {
	return c.doOK(ctx, BridgeRequest{Op: BridgeOpTruncate, Domain: domain, Name: name, Length: size})
}

// Unlink removes a WritableTree consumer's name.
func (c *BridgeClient) Unlink(ctx context.Context, domain, name string) error {
	return c.doOK(ctx, BridgeRequest{Op: BridgeOpUnlink, Domain: domain, Name: name})
}

// Rename moves a WritableTree consumer's oldName onto newName.
func (c *BridgeClient) Rename(ctx context.Context, domain, oldName, newName string) error {
	return c.doOK(ctx, BridgeRequest{Op: BridgeOpRename, Domain: domain, Name: oldName, To: newName})
}

// Mkdir creates a directory at a WritableTree consumer's name.
func (c *BridgeClient) Mkdir(ctx context.Context, domain, name string) error {
	return c.doOK(ctx, BridgeRequest{Op: BridgeOpMkdir, Domain: domain, Name: name})
}

// ReleaseAllHandles drops every handle token the HandleTree consumer holds for
// domain — sent when the holder starts serving a domain and when it stops, so
// tokens leaked by a holder crash die on the next generation's first call.
func (c *BridgeClient) ReleaseAllHandles(ctx context.Context, domain string) error {
	return c.doOK(ctx, BridgeRequest{Op: BridgeOpReleaseAll, Domain: domain})
}

// Handle is one per-open bridge handle over a HandleTree consumer: the token
// the consumer keys its open-time snapshot and edit buffer by. Minted by
// OpenHandle; a released or crashed-away token answers ClassNotFound.
type Handle struct {
	c *BridgeClient
	// Domain, Name, and Token identify the handle on the wire.
	Domain, Name, Token string
	// Snapshot is the open-time snapshot's entry as the consumer reported it.
	// Snapshot.Size is the exact length of the bytes ReadAt serves for this
	// handle (the OpenHandle contract), so a holder sizes the open by it —
	// never by a stat cache that may straddle a consumer commit.
	Snapshot Entry
}

// OpenHandle opens a per-open handle on a HandleTree consumer's name. A plain
// Tree consumer answers IsUnsupported, telling the caller to stay stateless.
func (c *BridgeClient) OpenHandle(ctx context.Context, domain, name string) (*Handle, error) {
	resp, err := c.do(ctx, BridgeRequest{Op: BridgeOpOpen, Domain: domain, Name: name})
	if err != nil {
		return nil, err
	}
	if err := bridgeRespErr(resp); err != nil {
		return nil, err
	}
	if resp.Token == "" {
		return nil, errors.New("open: ok response carried no token")
	}
	if resp.Item == nil {
		return nil, errors.New("open: ok response carried no snapshot entry")
	}
	return &Handle{c: c, Domain: domain, Name: name, Token: resp.Token, Snapshot: *resp.Item}, nil
}

// ReadAt reads from the handle's open-time snapshot.
func (h *Handle) ReadAt(ctx context.Context, ofst int64, size int) ([]byte, error) {
	resp, err := h.c.do(ctx, BridgeRequest{Op: BridgeOpReadAt, Domain: h.Domain, Name: h.Name, Token: h.Token, Ofst: ofst, Size: size})
	if err != nil {
		return nil, err
	}
	if err := bridgeRespErr(resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

// WriteAt writes into the handle's edit buffer; FlushHandle or ReleaseHandle
// commits it consumer-side.
func (h *Handle) WriteAt(ctx context.Context, ofst int64, data []byte) error {
	return h.c.doOK(ctx, BridgeRequest{Op: BridgeOpWriteAt, Domain: h.Domain, Name: h.Name, Token: h.Token, Ofst: ofst, Data: data})
}

// Truncate resizes the handle's edit buffer.
func (h *Handle) Truncate(ctx context.Context, size int64) error {
	return h.c.doOK(ctx, BridgeRequest{Op: BridgeOpTruncate, Domain: h.Domain, Name: h.Name, Token: h.Token, Length: size})
}

// Flush commits the handle's dirty buffer and returns the commit verdict — the
// call whose error a writer must see at its fsync/close boundary.
func (h *Handle) Flush(ctx context.Context) error {
	return h.c.doOK(ctx, BridgeRequest{Op: BridgeOpFlush, Domain: h.Domain, Name: h.Name, Token: h.Token})
}

// Release drops the handle's token; the consumer backstop-commits a dirty
// buffer no Flush ever committed.
func (h *Handle) Release(ctx context.Context) error {
	return h.c.doOK(ctx, BridgeRequest{Op: BridgeOpRelease, Domain: h.Domain, Name: h.Name, Token: h.Token})
}
