package catalogservice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalogproto"
)

// RemoteError is one stable application error returned in a typed response.
type RemoteError struct {
	Code    catalogproto.ErrorCode
	Message string
}

// Error implements error.
func (e *RemoteError) Error() string { return e.Message }

// TransportError is one daemonkit session or terminal failure.
type TransportError struct {
	Message string
	cause   error
}

// Error implements error.
func (e *TransportError) Error() string { return e.Message }

// Unwrap returns the typed daemonkit rejection when one is available.
func (e *TransportError) Unwrap() error { return e.cause }

// Client owns one persistent daemonkit session for all catalog operations.
type Client struct {
	business *daemonkit.Business
	owns     bool
}

// NewClient opens one persistent daemonkit business lane against d.
func NewClient(d daemonkit.Daemon) (*Client, error) {
	client, err := daemonkit.Open(d)
	if err != nil {
		return nil, err
	}
	return &Client{business: client.Business(), owns: true}, nil
}

// NewClientOn binds catalog operations to an existing business lane — either a
// socket peer's Client.Business or the handoff-confined lane a spawned child
// builds with daemonkit.BusinessOverConn, where the transport itself is the
// trust boundary.
func NewClientOn(business *daemonkit.Business) (*Client, error) {
	if business == nil {
		return nil, errors.New("catalog service: business lane is required")
	}
	return &Client{business: business}, nil
}

// Close closes the persistent daemonkit session.
func (c *Client) Close() error {
	if !c.owns {
		return nil
	}
	return c.business.Close(context.Background())
}

// Root returns the tenant's stable presentation root.
func (c *Client) Root(ctx context.Context, tenant catalogproto.TenantID, generation uint64) (catalogproto.LookupResponse, error) {
	var response catalogproto.LookupResponse
	err := c.unary(ctx, catalogproto.OperationCatalogRoot, tenant, catalogproto.RootRequest{
		Protocol: catalogproto.Version, Generation: generation,
	}, &response)
	return response, err
}

// Head returns the current tenant revision in O(1).
func (c *Client) Head(ctx context.Context, tenant catalogproto.TenantID, generation uint64) (catalogproto.HeadResponse, error) {
	var response catalogproto.HeadResponse
	err := c.unary(ctx, catalogproto.OperationCatalogHead, tenant, catalogproto.HeadRequest{Protocol: catalogproto.Version, Generation: generation}, &response)
	return response, err
}

// PublishDesiredSourceFleet atomically publishes one complete product-owned source fleet.
func (c *Client) PublishDesiredSourceFleet(
	ctx context.Context,
	request catalogproto.PublishDesiredSourceFleetRequest,
) (catalogproto.PublishDesiredSourceFleetResponse, error) {
	var response catalogproto.PublishDesiredSourceFleetResponse
	err := c.unary(ctx, catalogproto.OperationSourceAuthorityPublishDesiredFleet, "", request, &response)
	return response, err
}

// ReadDesiredSourceFleet returns one immutable generation-pinned desired-fleet page.
func (c *Client) ReadDesiredSourceFleet(
	ctx context.Context,
	request catalogproto.ReadDesiredSourceFleetRequest,
) (catalogproto.ReadDesiredSourceFleetResponse, error) {
	var response catalogproto.ReadDesiredSourceFleetResponse
	err := c.unary(ctx, catalogproto.OperationSourceAuthorityReadDesiredFleet, "", request, &response)
	return response, err
}

// Snapshot returns one immutable metadata-only page.
func (c *Client) Snapshot(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.SnapshotRequest) (catalogproto.SnapshotResponse, error) {
	var response catalogproto.SnapshotResponse
	err := c.unary(ctx, catalogproto.OperationCatalogSnapshot, tenant, request, &response)
	return response, err
}

// ChangesSince returns one ordered metadata-only delta page.
func (c *Client) ChangesSince(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.ChangesSinceRequest) (catalogproto.ChangesSinceResponse, error) {
	var response catalogproto.ChangesSinceResponse
	err := c.unary(ctx, catalogproto.OperationCatalogChangesSince, tenant, request, &response)
	return response, err
}

// Lookup returns one object by stable identity.
func (c *Client) Lookup(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.LookupRequest) (catalogproto.LookupResponse, error) {
	var response catalogproto.LookupResponse
	err := c.unary(ctx, catalogproto.OperationCatalogLookup, tenant, request, &response)
	return response, err
}

// LookupPrivate returns one unpublished object for the authenticated File Provider route.
func (c *Client) LookupPrivate(
	ctx context.Context,
	tenant catalogproto.TenantID,
	request catalogproto.LookupPrivateRequest,
) (catalogproto.LookupPrivateResponse, error) {
	var response catalogproto.LookupPrivateResponse
	err := c.unary(ctx, catalogproto.OperationCatalogLookupPrivate, tenant, request, &response)
	return response, err
}

// LookupName returns one child by exact name.
func (c *Client) LookupName(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.LookupNameRequest) (catalogproto.LookupResponse, error) {
	var response catalogproto.LookupResponse
	err := c.unary(ctx, catalogproto.OperationCatalogLookupName, tenant, request, &response)
	return response, err
}

// OpenAt opens one pinned exact-revision content handle and returns the reader
// that pages it; the object metadata is available immediately.
func (c *Client) OpenAt(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.OpenAtRequest) (*OpenReader, error) {
	var response catalogproto.OpenAtResponse
	if err := c.unary(ctx, catalogproto.OperationCatalogOpenAt, tenant, request, &response); err != nil {
		return nil, err
	}
	if response.Handle == nil {
		return nil, &TransportError{Message: "catalog service: open response carries no handle"}
	}
	readCtx, cancel := context.WithCancel(ctx)
	return &OpenReader{client: c, ctx: ctx, readCtx: readCtx, cancel: cancel, handle: *response.Handle, response: response}, nil
}

// OpenPrivate opens one pinned unpublished capability content handle.
func (c *Client) OpenPrivate(
	ctx context.Context,
	tenant catalogproto.TenantID,
	request catalogproto.OpenPrivateRequest,
) (*OpenReader, error) {
	var response catalogproto.OpenPrivateResponse
	if err := c.unary(ctx, catalogproto.OperationCatalogOpenPrivate, tenant, request, &response); err != nil {
		return nil, err
	}
	if response.Handle == nil {
		return nil, &TransportError{Message: "catalog service: open response carries no handle"}
	}
	readCtx, cancel := context.WithCancel(ctx)
	return &OpenReader{client: c, ctx: ctx, readCtx: readCtx, cancel: cancel, handle: *response.Handle, private: true, privateResp: response}, nil
}

// Mutate submits one closed mutation: a contentless request settles at the
// begin, and a content request stages its body in bounded chunks before the
// commit that consumes them.
func (c *Client) Mutate(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.MutationRequest, content io.Reader) (catalogproto.MutationResponse, error) {
	var response catalogproto.MutationResponse
	if err := validateTenant(tenant); err != nil {
		return response, err
	}
	if request.HasContent && content == nil {
		return response, errors.New("catalog service: content mutation has no reader")
	}
	if !request.HasContent && content != nil {
		return response, errors.New("catalog service: contentless mutation has a reader")
	}
	if !request.HasContent {
		err := c.unary(ctx, catalogproto.OperationCatalogMutateBegin, tenant, request, &response)
		return response, err
	}
	var begin catalogproto.BeginMutationResponse
	if err := c.unary(ctx, catalogproto.OperationCatalogMutateBegin, tenant, request, &begin); err != nil {
		return response, err
	}
	hasher := sha256.New()
	buffer := make([]byte, streamBufferSize)
	var total uint64
	var sequence uint32
	for {
		count, readErr := content.Read(buffer)
		if count > 0 {
			sequence++
			total += uint64(count)
			_, _ = hasher.Write(buffer[:count])
			var chunked catalogproto.MutationChunkResponse
			if err := c.unary(ctx, catalogproto.OperationCatalogMutateChunk, "", catalogproto.MutationChunkRequest{
				Protocol: catalogproto.Version, RequestID: request.RequestID, Sequence: sequence, Payload: buffer[:count],
			}, &chunked); err != nil {
				return response, err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return response, readErr
		}
		if count == 0 {
			return response, errors.New("catalog service: mutation reader made no progress")
		}
	}
	if total == 0 {
		return response, errors.New("catalog service: content mutation streamed no bytes")
	}
	err := c.unary(ctx, catalogproto.OperationCatalogMutateCommit, "", catalogproto.CommitMutationRequest{
		Protocol: catalogproto.Version, RequestID: request.RequestID,
		Total: total, Digest: hex.EncodeToString(hasher.Sum(nil)),
	}, &response)
	return response, err
}

// PrepareTenant prepares one exact generation from authoritative source state.
func (c *Client) PrepareTenant(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.PrepareTenantRequest) (catalogproto.PrepareTenantResponse, error) {
	var response catalogproto.PrepareTenantResponse
	err := c.unary(ctx, catalogproto.OperationTenantPrepare, tenant, request, &response)
	return response, err
}

// CommitFileProviderLease promotes an exact provisional readiness receipt to live demand.
func (c *Client) CommitFileProviderLease(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.CommitFileProviderLeaseRequest) (catalogproto.CommitFileProviderLeaseResponse, error) {
	var response catalogproto.CommitFileProviderLeaseResponse
	err := c.unary(ctx, catalogproto.OperationPresentationLeaseCommit, tenant, request, &response)
	return response, err
}

// RenewFileProviderLease extends an exact committed readiness receipt.
func (c *Client) RenewFileProviderLease(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.RenewFileProviderLeaseRequest) (catalogproto.RenewFileProviderLeaseResponse, error) {
	var response catalogproto.RenewFileProviderLeaseResponse
	err := c.unary(ctx, catalogproto.OperationPresentationLeaseRenew, tenant, request, &response)
	return response, err
}

// ReleaseFileProviderLease retires an exact readiness receipt and its content policy.
func (c *Client) ReleaseFileProviderLease(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.ReleaseFileProviderLeaseRequest) (catalogproto.ReleaseFileProviderLeaseResponse, error) {
	var response catalogproto.ReleaseFileProviderLeaseResponse
	err := c.unary(ctx, catalogproto.OperationPresentationLeaseRelease, tenant, request, &response)
	return response, err
}

// AckActivation acknowledges one exact activation after matching enumeration.
func (c *Client) AckActivation(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.AckActivationRequest) (catalogproto.AckActivationResponse, error) {
	var response catalogproto.AckActivationResponse
	err := c.unary(ctx, catalogproto.OperationActivationAck, tenant, request, &response)
	return response, err
}

// PollActivations drains the next ordered page of activation notifications,
// holding the call open up to the wait bound when none are pending.
func (c *Client) PollActivations(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.PollActivationsRequest) (catalogproto.PollActivationsResponse, error) {
	var response catalogproto.PollActivationsResponse
	err := c.unary(ctx, catalogproto.OperationActivationPoll, tenant, request, &response)
	return response, err
}

// clientCallTimeout bounds a call whose caller stated no deadline; it clears
// MaxPollWaitMillis so a full-length long-poll still settles inside it.
const clientCallTimeout = 35 * time.Second

func (c *Client) unary(ctx context.Context, operation catalogproto.Operation, tenant catalogproto.TenantID, request, response any) error {
	if err := validateOperationTenant(operation, tenant); err != nil {
		return err
	}
	if _, stated := ctx.Deadline(); !stated {
		bounded, cancel := context.WithTimeout(ctx, clientCallTimeout)
		defer cancel()
		ctx = bounded
	}
	payload, err := catalogproto.Encode(request)
	if err != nil {
		return err
	}
	body, err := json.Marshal(requestEnvelope{Tenant: string(tenant), Payload: payload})
	if err != nil {
		return err
	}
	reply, err := c.business.Call(ctx, string(operation), body)
	if err != nil {
		return transportError(err)
	}
	if err := decodeReply(reply, response); err != nil {
		return err
	}
	code, message, err := responseHeader(response)
	if err != nil {
		return err
	}
	return responseError(code, message)
}

func validateOperationTenant(operation catalogproto.Operation, tenant catalogproto.TenantID) error {
	switch operation {
	case catalogproto.OperationSourceAuthorityPublishDesiredFleet,
		catalogproto.OperationSourceAuthorityReadDesiredFleet:
		if tenant != "" {
			return errors.New("catalog service: product admin operation carries a tenant route")
		}
		return nil
	case catalogproto.OperationCatalogRead,
		catalogproto.OperationCatalogClose,
		catalogproto.OperationCatalogMutateChunk,
		catalogproto.OperationCatalogMutateCommit,
		catalogproto.OperationBrokerPoll,
		catalogproto.OperationBrokerResult:
		if tenant != "" {
			return errors.New("catalog service: session-scoped operation carries a tenant route")
		}
		return nil
	}
	return validateTenant(tenant)
}

func transportError(err error) error {
	return &TransportError{Message: err.Error(), cause: err}
}

func decodeReply(reply daemonkit.Reply, response any) error {
	if len(reply.Body) == 0 {
		return &TransportError{Message: "catalog service: daemonkit response has no payload"}
	}
	return catalogproto.Decode(reply.Body, response)
}

func responseHeader(response any) (catalogproto.ErrorCode, string, error) {
	switch value := response.(type) {
	case *catalogproto.HeadResponse:
		return value.Code, value.Message, nil
	case *catalogproto.SnapshotResponse:
		return value.Code, value.Message, nil
	case *catalogproto.ChangesSinceResponse:
		return value.Code, value.Message, nil
	case *catalogproto.LookupResponse:
		return value.Code, value.Message, nil
	case *catalogproto.LookupPrivateResponse:
		return value.Code, value.Message, nil
	case *catalogproto.OpenAtResponse:
		return value.Code, value.Message, nil
	case *catalogproto.OpenPrivateResponse:
		return value.Code, value.Message, nil
	case *catalogproto.ReadResponse:
		return value.Code, value.Message, nil
	case *catalogproto.CloseResponse:
		return value.Code, value.Message, nil
	case *catalogproto.MutationResponse:
		return value.Code, value.Message, nil
	case *catalogproto.BeginMutationResponse:
		return value.Code, value.Message, nil
	case *catalogproto.MutationChunkResponse:
		return value.Code, value.Message, nil
	case *catalogproto.PollActivationsResponse:
		return value.Code, value.Message, nil
	case *catalogproto.BrokerPollResponse:
		return value.Code, value.Message, nil
	case *catalogproto.PostBrokerResultResponse:
		return value.Code, value.Message, nil
	case *catalogproto.PrepareTenantResponse:
		return value.Code, value.Message, nil
	case *catalogproto.CommitFileProviderLeaseResponse:
		return value.Code, value.Message, nil
	case *catalogproto.RenewFileProviderLeaseResponse:
		return value.Code, value.Message, nil
	case *catalogproto.ReleaseFileProviderLeaseResponse:
		return value.Code, value.Message, nil
	case *catalogproto.AckActivationResponse:
		return value.Code, value.Message, nil
	case *catalogproto.PublishDesiredSourceFleetResponse:
		return value.Code, value.Message, nil
	case *catalogproto.ReadDesiredSourceFleetResponse:
		return value.Code, value.Message, nil
	default:
		return "", "", fmt.Errorf("catalog service: unsupported response type %T", response)
	}
}

func responseError(code catalogproto.ErrorCode, message string) error {
	if code == catalogproto.ErrorCodeOk {
		return nil
	}
	return &RemoteError{Code: code, Message: message}
}

func validateTenant(tenant catalogproto.TenantID) error {
	value := string(tenant)
	if value == "" || len(value) > 255 || strings.ContainsAny(value, "/\\\x00") {
		return errors.New("catalog service: invalid tenant id")
	}
	return nil
}

// OpenReader pages one pinned server-side content handle: the successor to the
// withdrawn open stream, reading forward one bounded unary page at a time.
type OpenReader struct {
	client      *Client
	ctx         context.Context
	readCtx     context.Context
	cancel      context.CancelFunc
	handle      catalogproto.HandleID
	private     bool
	response    catalogproto.OpenAtResponse
	privateResp catalogproto.OpenPrivateResponse

	mu      sync.Mutex
	current []byte
	offset  uint64
	eof     bool
	closed  bool
	err     error
}

// Read reads immutable pinned content to EOF.
func (r *OpenReader) Read(buffer []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(buffer) == 0 {
		return 0, nil
	}
	for {
		if len(r.current) > 0 {
			count := copy(buffer, r.current)
			r.current = r.current[count:]
			return count, nil
		}
		if r.err != nil {
			return 0, r.err
		}
		if r.eof {
			return 0, io.EOF
		}
		if r.closed {
			return 0, errors.New("catalog service: open stream closed before settlement")
		}
		var response catalogproto.ReadResponse
		if err := r.client.unary(r.readCtx, catalogproto.OperationCatalogRead, "", catalogproto.ReadRequest{
			Protocol: catalogproto.Version, Handle: r.handle, Offset: r.offset, Limit: streamBufferSize,
		}, &response); err != nil {
			r.err = err
			return 0, err
		}
		r.offset += uint64(len(response.Data))
		r.current = response.Data
		r.eof = response.EOF
	}
}

// Close cancels any in-flight read, then releases the pinned server-side
// handle without closing the persistent client session.
func (r *OpenReader) Close() error {
	r.cancel()
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), mutationStageCleanupTimeout)
	defer cancel()
	var response catalogproto.CloseResponse
	return r.client.unary(ctx, catalogproto.OperationCatalogClose, "", catalogproto.CloseRequest{
		Protocol: catalogproto.Version, Handle: r.handle,
	}, &response)
}

// Response returns exact object metadata for a namespace open.
func (r *OpenReader) Response() (catalogproto.OpenAtResponse, error) {
	if r.private {
		return catalogproto.OpenAtResponse{}, errors.New("catalog service: private open has no namespace response")
	}
	return r.response, nil
}

// PrivateResponse returns exact unpublished metadata for a private open.
func (r *OpenReader) PrivateResponse() (catalogproto.OpenPrivateResponse, error) {
	if !r.private {
		return catalogproto.OpenPrivateResponse{}, errors.New("catalog service: namespace open has no private response")
	}
	return r.privateResp, nil
}
