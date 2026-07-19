package catalogservice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/transportproto"
)

// RemoteError is one stable application error returned in a typed response.
type RemoteError struct {
	Code    catalogproto.ErrorCode
	Message string
}

// Error implements error.
func (e *RemoteError) Error() string { return e.Message }

// TransportError is an untyped daemonkit session or terminal failure.
type TransportError struct {
	Outcome wire.Outcome
	Message string
}

// Error implements error.
func (e *TransportError) Error() string { return e.Message }

// Client owns one persistent daemonkit session for all catalog operations.
type Client struct {
	wire *wire.Client
	owns bool
}

// NewClient opens one persistent daemonkit session using the generated schema build identity.
func NewClient(ctx context.Context, config wire.ClientConfig) (*Client, error) {
	if config.Build != "" && config.Build != transportproto.Build {
		return nil, fmt.Errorf("catalog service: daemonkit build %q does not match transport suite %q", config.Build, transportproto.Build)
	}
	config.Build = transportproto.Build
	client, err := wire.NewClient(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Client{wire: client, owns: true}, nil
}

// Close closes the persistent daemonkit session.
func (c *Client) Close() error {
	if !c.owns {
		return nil
	}
	return c.wire.Close()
}

// Root returns the tenant's stable presentation root.
func (c *Client) Root(ctx context.Context, tenant catalogproto.TenantID, generation uint64) (catalogproto.LookupResponse, error) {
	var response catalogproto.LookupResponse
	err := c.unary(ctx, catalogproto.OperationCatalogRoot, tenant, catalogproto.RootRequest{
		Protocol: catalogproto.Version, Generation: generation,
	}, &response)
	return response, err
}

// NewClientOn binds catalog operations to an existing exact-suite session.
func NewClientOn(client *wire.Client) (*Client, error) {
	if client == nil || client.PeerBuild().Build != transportproto.Build {
		return nil, fmt.Errorf("catalog service: exact transport session is required")
	}
	return &Client{wire: client}, nil
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

// LookupName returns one child by exact name.
func (c *Client) LookupName(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.LookupNameRequest) (catalogproto.LookupResponse, error) {
	var response catalogproto.LookupResponse
	err := c.unary(ctx, catalogproto.OperationCatalogLookupName, tenant, request, &response)
	return response, err
}

// OpenAt starts an exact-revision content stream. The response metadata is available after EOF.
func (c *Client) OpenAt(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.OpenAtRequest) (*OpenReader, error) {
	if err := validateTenant(tenant); err != nil {
		return nil, err
	}
	payload, err := catalogproto.Encode(request)
	if err != nil {
		return nil, err
	}
	call, err := c.wire.Open(ctx, wire.Op(catalogproto.OperationCatalogOpenAt), string(tenant), payload, true)
	if err != nil {
		return nil, err
	}
	streamContext, cancel := context.WithCancel(ctx)
	return &OpenReader{ctx: streamContext, cancel: cancel, call: call, chunks: call.Chunks()}, nil
}

// Mutate streams request bytes exactly once and submits one closed mutation.
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
	payload, err := catalogproto.Encode(request)
	if err != nil {
		return response, err
	}
	call, err := c.wire.Open(ctx, wire.Op(catalogproto.OperationCatalogMutate), string(tenant), payload, false)
	if err != nil {
		return response, err
	}
	if request.HasContent {
		buffer := make([]byte, streamBufferSize)
		for {
			count, readErr := content.Read(buffer)
			if count > 0 {
				if err := call.SendChunk(ctx, buffer[:count]); err != nil {
					if errors.Is(err, wire.ErrCallDone) {
						settled, settleErr := mutationResponse(ctx, call)
						if settleErr != nil {
							return settled, settleErr
						}
						return settled, errors.New("catalog service: mutation settled before input ended")
					}
					call.Cancel()
					return response, err
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				call.Cancel()
				return response, readErr
			}
			if count == 0 {
				call.Cancel()
				return response, errors.New("catalog service: mutation reader made no progress")
			}
		}
	}
	if err := call.CloseSend(ctx); err != nil {
		if errors.Is(err, wire.ErrCallDone) {
			return mutationResponse(ctx, call)
		}
		call.Cancel()
		return response, err
	}
	return mutationResponse(ctx, call)
}

func mutationResponse(ctx context.Context, call *wire.ClientCall) (catalogproto.MutationResponse, error) {
	var response catalogproto.MutationResponse
	if err := drainChunks(ctx, call); err != nil {
		return response, err
	}
	result, err := call.Response(ctx)
	if err != nil {
		return response, err
	}
	if err := decodeWireResult(result, &response); err != nil {
		return response, err
	}
	return response, responseError(response.Code, response.Message)
}

// PrepareTenant prepares one exact generation from authoritative source state.
func (c *Client) PrepareTenant(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.PrepareTenantRequest) (catalogproto.PrepareTenantResponse, error) {
	var response catalogproto.PrepareTenantResponse
	err := c.unary(ctx, catalogproto.OperationTenantPrepare, tenant, request, &response)
	return response, err
}

// PrepareDomain prepares one exact File Provider domain from an echoed tenant proof.
func (c *Client) PrepareDomain(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.PrepareDomainRequest) (catalogproto.PrepareDomainResponse, error) {
	var response catalogproto.PrepareDomainResponse
	err := c.unary(ctx, catalogproto.OperationDomainPrepare, tenant, request, &response)
	if err == nil && (response.Observation == nil ||
		!validDomainPreparationObservation(tenant, request, *response.Observation)) {
		err = fmt.Errorf("%w: domain preparation response identity differs", catalog.ErrIntegrity)
	}
	return response, err
}

func validDomainPreparationObservation(
	tenant catalogproto.TenantID,
	request catalogproto.PrepareDomainRequest,
	observation catalogproto.DomainObservation,
) bool {
	return observation.TenantID == tenant && observation.DomainID == request.DomainID &&
		observation.Generation == request.Generation && observation.RequestedRevision > 0 &&
		observation.ObservedRevision >= observation.RequestedRevision &&
		observation.CatalogRevision == request.CatalogRevision &&
		observation.SourceAuthority == request.SourceAuthority &&
		observation.SourceRevision == request.SourceRevision &&
		observation.ChangeID == request.ChangeID && observation.OperationID == request.OperationID
}

// AckConvergence acknowledges one exact notification only after matching enumeration.
func (c *Client) AckConvergence(ctx context.Context, tenant catalogproto.TenantID, request catalogproto.AckConvergenceRequest) (catalogproto.AckConvergenceResponse, error) {
	var response catalogproto.AckConvergenceResponse
	err := c.unary(ctx, catalogproto.OperationConvergenceAck, tenant, request, &response)
	return response, err
}

// DecodeConvergenceEvent strictly decodes the event-only convergence notification topic.
func DecodeConvergenceEvent(event wire.Event) (catalogproto.ConvergenceNotification, bool, error) {
	var notification catalogproto.ConvergenceNotification
	if event.Topic != string(catalogproto.OperationConvergenceNotify) {
		return notification, false, nil
	}
	if err := catalogproto.Decode(event.Payload, &notification); err != nil {
		return notification, true, err
	}
	return notification, true, nil
}

func (c *Client) unary(ctx context.Context, operation catalogproto.Operation, tenant catalogproto.TenantID, request, response any) error {
	if err := validateOperationTenant(operation, tenant); err != nil {
		return err
	}
	payload, err := catalogproto.Encode(request)
	if err != nil {
		return err
	}
	result, err := c.wire.Call(ctx, wire.Op(operation), string(tenant), payload)
	if err != nil {
		return err
	}
	if err := decodeWireResult(result, response); err != nil {
		return err
	}
	code, message, err := responseHeader(response)
	if err != nil {
		return err
	}
	return responseError(code, message)
}

func validateOperationTenant(operation catalogproto.Operation, tenant catalogproto.TenantID) error {
	if operation == catalogproto.OperationSourceAuthorityPublishDesiredFleet ||
		operation == catalogproto.OperationSourceAuthorityReadDesiredFleet {
		if tenant != "" {
			return errors.New("catalog service: product admin operation carries a tenant route")
		}
		return nil
	}
	return validateTenant(tenant)
}

func decodeWireResult(result wire.Result, response any) error {
	if result.Outcome != wire.Delivered || result.Response.Rejected {
		message := result.Response.Reason
		if message == "" {
			message = "catalog service: daemonkit request was not delivered"
		}
		return &TransportError{Outcome: result.Outcome, Message: message}
	}
	if result.Response.Err != "" {
		return &TransportError{Outcome: result.Outcome, Message: result.Response.Err}
	}
	if len(result.Response.Payload) == 0 {
		return &TransportError{Outcome: result.Outcome, Message: "catalog service: daemonkit response has no payload"}
	}
	if err := catalogproto.Decode(result.Response.Payload, response); err != nil {
		return err
	}
	return nil
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
	case *catalogproto.OpenAtResponse:
		return value.Code, value.Message, nil
	case *catalogproto.MutationResponse:
		return value.Code, value.Message, nil
	case *catalogproto.PrepareTenantResponse:
		return value.Code, value.Message, nil
	case *catalogproto.AckConvergenceResponse:
		return value.Code, value.Message, nil
	case *catalogproto.BrokerOpenResponse:
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

func drainChunks(ctx context.Context, call *wire.ClientCall) error {
	for {
		select {
		case <-ctx.Done():
			call.Cancel()
			return ctx.Err()
		case _, ok := <-call.Chunks():
			if !ok {
				return nil
			}
		}
	}
}

// OpenReader is one backpressured exact-revision content stream.
type OpenReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	call   *wire.ClientCall
	chunks <-chan wire.Chunk

	readMu sync.Mutex
	mu     sync.Mutex

	current     []byte
	streamEnded bool
	settled     bool
	response    catalogproto.OpenAtResponse
	err         error
}

// Read reads immutable content and settles the typed response at stream EOF.
func (r *OpenReader) Read(buffer []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if len(buffer) == 0 {
		return 0, nil
	}
	for {
		r.mu.Lock()
		if len(r.current) > 0 {
			count := copy(buffer, r.current)
			r.current = r.current[count:]
			r.mu.Unlock()
			return count, nil
		}
		if r.settled {
			err := r.err
			r.mu.Unlock()
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		streamEnded := r.streamEnded
		r.mu.Unlock()
		if streamEnded {
			r.settle()
			continue
		}
		select {
		case <-r.ctx.Done():
			r.abort(r.ctx.Err())
		case chunk, ok := <-r.chunks:
			if !ok {
				r.mu.Lock()
				r.streamEnded = true
				r.mu.Unlock()
				continue
			}
			r.mu.Lock()
			r.current = append(r.current[:0], chunk.Payload...)
			r.streamEnded = chunk.End
			r.mu.Unlock()
		}
	}
}

// Close cancels an unsettled open without closing the persistent client session.
func (r *OpenReader) Close() error {
	r.abort(errors.New("catalog service: open stream closed before settlement"))
	return nil
}

// Response returns exact object metadata after the stream settles.
func (r *OpenReader) Response() (catalogproto.OpenAtResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.settled {
		return catalogproto.OpenAtResponse{}, errors.New("catalog service: open stream has not settled")
	}
	return r.response, r.err
}

func (r *OpenReader) settle() {
	r.mu.Lock()
	if r.settled {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	var response catalogproto.OpenAtResponse
	result, err := r.call.Response(r.ctx)
	if err == nil {
		err = decodeWireResult(result, &response)
	}
	if err == nil {
		err = responseError(response.Code, response.Message)
	}
	r.mu.Lock()
	if r.settled {
		r.mu.Unlock()
		return
	}
	r.response = response
	r.err = err
	r.settled = true
	r.mu.Unlock()
	r.cancel()
}

func (r *OpenReader) abort(err error) {
	r.mu.Lock()
	if r.settled {
		r.mu.Unlock()
		return
	}
	r.err = err
	r.settled = true
	r.current = nil
	r.mu.Unlock()
	r.cancel()
	r.call.Cancel()
}
