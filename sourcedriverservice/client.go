package sourcedriverservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
)

// Client owns one persistent exact-build SourceDriver session.
type Client struct {
	wire clientTransport
	owns bool
}

type clientTransport interface {
	Call(context.Context, wire.Op, string, []byte) (wire.Result, error)
	Open(context.Context, wire.Op, string, []byte, bool) (*wire.ClientCall, error)
	Close() error
	Abort(error) error
}

// NewClient opens one persistent daemonkit session for the exact v1 schema.
func NewClient(ctx context.Context, config wire.ClientConfig) (*Client, error) {
	if config.WireBuild != "" && config.WireBuild != sourcedriverproto.Build {
		return nil, exactBuild(config.WireBuild)
	}
	config.WireBuild = sourcedriverproto.Build
	client, err := wire.NewClient(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Client{wire: client, owns: true}, nil
}

type spawnedClientTransport struct{ *wire.SpawnedClient }

func (c spawnedClientTransport) Open(
	ctx context.Context,
	op wire.Op,
	tenant string,
	payload []byte,
	endInput bool,
) (*wire.ClientCall, error) {
	return c.OpenStream(ctx, op, tenant, payload, endInput)
}

// NewSpawnedClient consumes one exact daemonkit spawned-session endpoint.
func NewSpawnedClient(ctx context.Context, endpoint proc.SpawnedSessionEndpoint) (*Client, error) {
	config, err := spawnedClientConfig(endpoint)
	if err != nil {
		return nil, err
	}
	client, err := wire.NewSpawnedClient(ctx, config)
	if err != nil {
		return nil, err
	}
	return &Client{wire: spawnedClientTransport{SpawnedClient: client}, owns: true}, nil
}

// NewClientOn binds SourceDriver operations to an existing exact-build session.
func NewClientOn(client *wire.Client) (*Client, error) {
	if client == nil {
		return nil, errors.New("source driver service: daemonkit client is nil")
	}
	if err := exactBuild(client.PeerWireIdentity().WireBuild); err != nil {
		return nil, err
	}
	return &Client{wire: client}, nil
}

// Close closes an owned persistent session.
func (c *Client) Close() error {
	if !c.owns {
		return nil
	}
	return c.wire.Close()
}

// Refresh returns the driver's exact authoritative head.
func (c *Client) Refresh(ctx context.Context, authority causal.SourceAuthorityID) (sourcedriver.Head, error) {
	var response sourcedriverproto.RefreshResponse
	err := c.unary(ctx, sourcedriverproto.OperationRefresh, authority,
		sourcedriverproto.RefreshRequest{Protocol: sourcedriverproto.Version}, &response)
	if err != nil {
		return sourcedriver.Head{}, err
	}
	head := sourcedriver.Head{Revision: sourcedriver.RevisionToken(response.Revision)}
	return head, sourcedriver.ValidateHead(head)
}

// InspectTargetSet returns the durable declaration state for ref.
func (c *Client) InspectTargetSet(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	ref sourcedriver.TargetSetRef,
) (sourcedriver.TargetSetState, error) {
	if err := sourcedriver.ValidateTargetSetRef(authority, ref); err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	var response sourcedriverproto.InspectTargetSetResponse
	if err := c.unary(ctx, sourcedriverproto.OperationInspectTargetSet, authority,
		sourcedriverproto.InspectTargetSetRequest{
			Protocol: sourcedriverproto.Version, Ref: protocolTargetSetRef(ref),
		}, &response); err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	if response.State == nil {
		return sourcedriver.TargetSetState{}, sourcedriver.ErrIntegrity
	}
	state, err := domainTargetSetState(*response.State)
	if err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	if err := sourcedriver.ValidateTargetSetState(authority, state); err != nil || state.Ref != ref {
		return sourcedriver.TargetSetState{}, errors.Join(sourcedriver.ErrIntegrity, err)
	}
	return state, nil
}

// DeclareTargetSet durably advances or exactly replays one declaration page.
func (c *Client) DeclareTargetSet(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	page sourcedriver.TargetSetPage,
) (sourcedriver.TargetSetState, error) {
	if err := sourcedriver.ValidateTargetSetPage(authority, page); err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	var response sourcedriverproto.DeclareTargetSetResponse
	if err := c.unary(ctx, sourcedriverproto.OperationDeclareTargetSet, authority,
		sourcedriverproto.DeclareTargetSetRequest{
			Protocol: sourcedriverproto.Version, Page: protocolTargetSetPage(page),
		}, &response); err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	if response.State == nil {
		return sourcedriver.TargetSetState{}, sourcedriver.ErrIntegrity
	}
	state, err := domainTargetSetState(*response.State)
	if err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	if err := sourcedriver.ValidateTargetSetState(authority, state); err != nil || state.Ref != page.Ref {
		return sourcedriver.TargetSetState{}, errors.Join(sourcedriver.ErrIntegrity, err)
	}
	return state, nil
}

// Snapshot returns one bounded immutable source page.
func (c *Client) Snapshot(ctx context.Context, authority causal.SourceAuthorityID, request sourcedriver.SnapshotRequest) (sourcedriver.SnapshotPage, error) {
	if err := sourcedriver.ValidateTargetSetRef(authority, request.TargetSet); err != nil {
		return sourcedriver.SnapshotPage{}, err
	}
	if err := sourcedriver.ValidateSnapshotRequest(request); err != nil {
		return sourcedriver.SnapshotPage{}, err
	}
	var response sourcedriverproto.SnapshotResponse
	err := c.unary(ctx, sourcedriverproto.OperationSnapshot, authority, sourcedriverproto.SnapshotRequest{
		Protocol: sourcedriverproto.Version, TargetSet: protocolTargetSetRef(request.TargetSet),
		Revision: string(request.Revision), Cursor: protocolCursor(request.Cursor), Limit: uint32(request.Limit),
	}, &response)
	if err != nil {
		var stale *sourcedriver.StaleRevisionError
		if errors.As(err, &stale) {
			stale.Expected = request.Revision
		}
		return sourcedriver.SnapshotPage{}, err
	}
	objects := make([]sourcedriver.Projection, len(response.Objects))
	for index := range response.Objects {
		objects[index], err = domainProjection(response.Objects[index])
		if err != nil {
			return sourcedriver.SnapshotPage{}, err
		}
	}
	next, err := domainCursor(response.Next)
	if err != nil {
		return sourcedriver.SnapshotPage{}, err
	}
	digest, err := digest(response.Digest)
	if err != nil {
		return sourcedriver.SnapshotPage{}, err
	}
	page := sourcedriver.SnapshotPage{Revision: sourcedriver.RevisionToken(response.Revision), Objects: objects, Next: next, Digest: digest}
	return page, sourcedriver.ValidateSnapshotPage(request, page)
}

// ChangesSince returns one bounded immutable delta page.
func (c *Client) ChangesSince(ctx context.Context, authority causal.SourceAuthorityID, request sourcedriver.ChangesRequest) (sourcedriver.ChangePage, error) {
	if err := sourcedriver.ValidateTargetSetRef(authority, request.TargetSet); err != nil {
		return sourcedriver.ChangePage{}, err
	}
	if err := sourcedriver.ValidateChangesRequest(request); err != nil {
		return sourcedriver.ChangePage{}, err
	}
	var response sourcedriverproto.ChangesSinceResponse
	err := c.unary(ctx, sourcedriverproto.OperationChangesSince, authority, sourcedriverproto.ChangesSinceRequest{
		Protocol: sourcedriverproto.Version, TargetSet: protocolTargetSetRef(request.TargetSet),
		From: string(request.From), To: string(request.To), Cursor: protocolCursor(request.Cursor), Limit: uint32(request.Limit),
	}, &response)
	if err != nil {
		var snapshot *sourcedriver.SnapshotRequiredError
		if errors.As(err, &snapshot) {
			snapshot.From = request.From
		}
		return sourcedriver.ChangePage{}, err
	}
	changes := make([]sourcedriver.Change, len(response.Changes))
	for index := range response.Changes {
		changes[index], err = domainChange(response.Changes[index])
		if err != nil {
			return sourcedriver.ChangePage{}, err
		}
	}
	next, err := domainCursor(response.Next)
	if err != nil {
		return sourcedriver.ChangePage{}, err
	}
	digest, err := digest(response.Digest)
	if err != nil {
		return sourcedriver.ChangePage{}, err
	}
	page := sourcedriver.ChangePage{
		From: sourcedriver.RevisionToken(response.From), To: sourcedriver.RevisionToken(response.To), Changes: changes, Next: next, Digest: digest,
	}
	return page, sourcedriver.ValidateChangePage(request, page)
}

// OpenContent opens one backpressured immutable source body.
func (c *Client) OpenContent(ctx context.Context, authority causal.SourceAuthorityID, ref sourcedriver.ContentRef) (contentstream.Source, error) {
	if err := validateAuthority(authority); err != nil {
		return nil, err
	}
	if err := sourcedriver.ValidateContentRef(ref); err != nil {
		return nil, err
	}
	payload, err := sourcedriverproto.Encode(sourcedriverproto.OpenContentRequest{Protocol: sourcedriverproto.Version, Content: protocolContentRef(ref)})
	if err != nil {
		return nil, err
	}
	call, err := c.wire.Open(ctx, wire.Op(sourcedriverproto.OperationOpenContent), string(authority), payload, true)
	if err != nil {
		return nil, err
	}
	streamContext, cancel := context.WithCancel(ctx)
	return &openSource{
		ctx: streamContext, cancel: cancel, call: call, chunks: call.Chunks(), expected: ref,
		hasher: sha256.New(), done: make(chan struct{}),
	}, nil
}

// ApplyMutation streams one body and returns its exact durable receipt.
func (c *Client) ApplyMutation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	request sourcedriver.MutationRequest,
	content contentstream.Source,
) (sourcedriver.MutationReceipt, error) {
	if err := validateAuthority(authority); err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	if err := sourcedriver.ValidateTargetSetRef(authority, request.TargetSet); err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	if err := sourcedriver.ValidateMutationRequest(request); err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	if request.HasContent != (content != nil) {
		return sourcedriver.MutationReceipt{}, errors.New("source driver service: mutation content ownership differs from request")
	}
	input := sourcedriverproto.ApplyMutationRequest{
		Protocol: sourcedriverproto.Version, TargetSet: protocolTargetSetRef(request.TargetSet), Tenant: string(request.Tenant),
		Generation: uint64(request.Generation), OperationID: request.OperationID.String(), Expected: string(request.Expected),
		Context: protocolMutationContext(request.Context), HasContent: request.HasContent,
		ContentSize: request.ContentSize,
	}
	if request.HasContent {
		input.ContentHash = fmt.Sprintf("%x", request.ContentHash)
	}
	payload, err := sourcedriverproto.Encode(input)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	call, err := c.wire.Open(ctx, wire.Op(sourcedriverproto.OperationApplyMutation), string(authority), payload, false)
	if err != nil {
		return sourcedriver.MutationReceipt{}, settleOwned(ctx, content, err)
	}
	if content != nil {
		buffer := make([]byte, streamChunkBytes)
		var total int64
		hasher := sha256.New()
		for {
			count, readErr := content.Read(buffer)
			if count > 0 {
				total += int64(count)
				_, _ = hasher.Write(buffer[:count])
				if total > request.ContentSize || total > sourcedriver.MaxContentBytes {
					err = fmt.Errorf("%w: mutation content exceeds exact size", sourcedriver.ErrIntegrity)
					call.Cancel()
					return sourcedriver.MutationReceipt{}, settleOwned(ctx, content, err)
				}
				if sendErr := call.SendChunk(ctx, buffer[:count]); sendErr != nil {
					if errors.Is(sendErr, wire.ErrCallDone) {
						receipt, responseErr := receiveApplyResponse(ctx, call, request)
						if responseErr == nil {
							responseErr = fmt.Errorf("%w: mutation settled before its input ended", sourcedriver.ErrIntegrity)
						}
						return receipt, settleOwned(ctx, content, responseErr)
					}
					call.Cancel()
					return sourcedriver.MutationReceipt{}, settleOwned(ctx, content, sendErr)
				}
			}
			if errors.Is(readErr, io.EOF) {
				var actual catalog.ContentHash
				copy(actual[:], hasher.Sum(nil))
				if total != request.ContentSize || actual != request.ContentHash {
					err = fmt.Errorf("%w: mutation content size or digest differs", sourcedriver.ErrIntegrity)
					call.Cancel()
					return sourcedriver.MutationReceipt{}, settleOwned(ctx, content, err)
				}
				if err := settleOwned(ctx, content, nil); err != nil {
					call.Cancel()
					return sourcedriver.MutationReceipt{}, err
				}
				break
			}
			if readErr != nil || count == 0 {
				if readErr == nil {
					readErr = errors.New("source driver service: mutation content reader made no progress")
				}
				call.Cancel()
				return sourcedriver.MutationReceipt{}, settleOwned(ctx, content, readErr)
			}
		}
	}
	if err := call.CloseSend(ctx); err != nil {
		if !errors.Is(err, wire.ErrCallDone) {
			call.Cancel()
			return sourcedriver.MutationReceipt{}, err
		}
	}
	return receiveApplyResponse(ctx, call, request)
}

func receiveApplyResponse(
	ctx context.Context,
	call *wire.ClientCall,
	request sourcedriver.MutationRequest,
) (sourcedriver.MutationReceipt, error) {
	var response sourcedriverproto.ApplyMutationResponse
	if err := streamResponse(ctx, call, &response); err != nil {
		var stale *sourcedriver.StaleRevisionError
		if errors.As(err, &stale) {
			stale.Expected = request.Expected
		}
		return sourcedriver.MutationReceipt{}, err
	}
	receipt, err := domainReceipt(*response.Receipt)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	requestDigest, err := sourcedriver.MutationRequestDigest(request)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	if receipt.OperationID != request.OperationID || receipt.RequestDigest != requestDigest || receipt.Expected != request.Expected {
		return sourcedriver.MutationReceipt{}, fmt.Errorf("%w: mutation response identity differs", sourcedriver.ErrIntegrity)
	}
	return receipt, nil
}

// InspectMutation returns the exact durable state of one operation ID.
func (c *Client) InspectMutation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	id catalog.MutationID,
	requestDigest [sha256.Size]byte,
) (sourcedriver.MutationReceipt, error) {
	if id == (catalog.MutationID{}) || requestDigest == ([sha256.Size]byte{}) {
		return sourcedriver.MutationReceipt{}, sourcedriver.ErrInvalidValue
	}
	var response sourcedriverproto.InspectMutationResponse
	err := c.unary(ctx, sourcedriverproto.OperationInspectMutation, authority, sourcedriverproto.InspectMutationRequest{
		Protocol: sourcedriverproto.Version, OperationID: id.String(), RequestDigest: fmt.Sprintf("%x", requestDigest),
	}, &response)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	receipt, err := domainReceipt(*response.Receipt)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	if receipt.OperationID != id || receipt.State != sourcedriver.MutationNotFound && receipt.RequestDigest != requestDigest {
		return sourcedriver.MutationReceipt{}, fmt.Errorf("%w: inspected operation id differs", sourcedriver.ErrIntegrity)
	}
	return receipt, nil
}

// SettleMutation applies one exact source-side mutation receipt transition.
func (c *Client) SettleMutation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	settlement sourcedriver.MutationSettlement,
) error {
	if err := sourcedriver.ValidateTargetSetRef(authority, settlement.TargetSet); err != nil {
		return err
	}
	if err := sourcedriver.ValidateMutationSettlement(settlement); err != nil {
		return err
	}
	var response sourcedriverproto.SettleMutationResponse
	return c.unary(
		ctx,
		sourcedriverproto.OperationSettleMutation,
		authority,
		sourcedriverproto.SettleMutationRequest{
			Protocol: sourcedriverproto.Version, Settlement: protocolSettlement(settlement),
		},
		&response,
	)
}

func (c *Client) unary(ctx context.Context, operation sourcedriverproto.Operation, authority causal.SourceAuthorityID, request, response any) error {
	if err := validateAuthority(authority); err != nil {
		return err
	}
	payload, err := sourcedriverproto.Encode(request)
	if err != nil {
		return err
	}
	result, err := c.wire.Call(ctx, wire.Op(operation), string(authority), payload)
	if err != nil {
		return err
	}
	if err := decodeResult(result, response); err != nil {
		return err
	}
	code, message, actual, err := responseHeader(response)
	if err != nil {
		return err
	}
	return responseError(code, message, actual)
}

func streamResponse(ctx context.Context, call *wire.ClientCall, response any) error {
	for {
		select {
		case <-ctx.Done():
			call.Cancel()
			return ctx.Err()
		case _, ok := <-call.Chunks():
			if !ok {
				result, err := call.Response(ctx)
				if err != nil {
					return err
				}
				if err := decodeResult(result, response); err != nil {
					return err
				}
				code, message, actual, err := responseHeader(response)
				if err != nil {
					return err
				}
				return responseError(code, message, actual)
			}
		}
	}
}

func decodeResult(result wire.Result, response any) error {
	if result.Outcome != wire.Delivered || result.Response.Rejected {
		message := result.Response.Reason
		if message == "" {
			message = "source driver service: daemonkit request was not delivered"
		}
		return &TransportError{Outcome: result.Outcome, Message: message}
	}
	if result.Response.Err != "" {
		return &TransportError{Outcome: result.Outcome, Message: result.Response.Err}
	}
	if len(result.Response.Payload) == 0 {
		return &TransportError{Outcome: result.Outcome, Message: "source driver service: response has no payload"}
	}
	return sourcedriverproto.Decode(result.Response.Payload, response)
}

func responseHeader(response any) (sourcedriverproto.ErrorCode, string, string, error) {
	switch value := response.(type) {
	case *sourcedriverproto.RefreshResponse:
		return value.Code, value.Message, value.Actual, nil
	case *sourcedriverproto.InspectTargetSetResponse:
		return value.Code, value.Message, "", nil
	case *sourcedriverproto.DeclareTargetSetResponse:
		return value.Code, value.Message, "", nil
	case *sourcedriverproto.SnapshotResponse:
		return value.Code, value.Message, value.Actual, nil
	case *sourcedriverproto.ChangesSinceResponse:
		return value.Code, value.Message, value.Actual, nil
	case *sourcedriverproto.OpenContentResponse:
		return value.Code, value.Message, value.Actual, nil
	case *sourcedriverproto.ApplyMutationResponse:
		return value.Code, value.Message, value.Actual, nil
	case *sourcedriverproto.InspectMutationResponse:
		return value.Code, value.Message, "", nil
	case *sourcedriverproto.SettleMutationResponse:
		return value.Code, value.Message, "", nil
	default:
		return "", "", "", fmt.Errorf("source driver service: unsupported response type %T", response)
	}
}

func responseError(code sourcedriverproto.ErrorCode, message, actual string) error {
	if code == sourcedriverproto.ErrorCodeOK {
		return nil
	}
	switch code {
	case sourcedriverproto.ErrorCodeNotFound:
		return errors.Join(sourcedriver.ErrNotFound, &RemoteError{Code: code, Message: message})
	case sourcedriverproto.ErrorCodeConflict:
		return errors.Join(sourcedriver.ErrConflict, &RemoteError{Code: code, Message: message})
	case sourcedriverproto.ErrorCodeIntegrity:
		return errors.Join(sourcedriver.ErrIntegrity, &RemoteError{Code: code, Message: message})
	case sourcedriverproto.ErrorCodeSnapshotRequired:
		return &sourcedriver.SnapshotRequiredError{Head: sourcedriver.RevisionToken(actual)}
	case sourcedriverproto.ErrorCodeStaleRevision:
		return &sourcedriver.StaleRevisionError{Actual: sourcedriver.RevisionToken(actual)}
	default:
		return &RemoteError{Code: code, Message: message, Actual: actual}
	}
}

func validateAuthority(authority causal.SourceAuthorityID) error {
	return causal.ValidateSourceAuthorityID(authority)
}

func settleOwned(ctx context.Context, source contentstream.Source, cause error) error {
	if source == nil {
		return cause
	}
	settleErr := source.Settle(cause)
	waitErr := source.Wait(ctx)
	return errors.Join(cause, settleErr, waitErr)
}

var _ sourcedriver.Driver = (*Client)(nil)

type openSource struct {
	ctx      context.Context
	cancel   context.CancelFunc
	call     *wire.ClientCall
	chunks   <-chan wire.Chunk
	expected sourcedriver.ContentRef
	hasher   hashWriter
	done     chan struct{}

	readMu     sync.Mutex
	mu         sync.Mutex
	finishOnce sync.Once
	current    []byte
	count      int64
	ended      bool
	settled    bool
	err        error
}

func (s *openSource) Read(buffer []byte) (int, error) {
	s.readMu.Lock()
	defer s.readMu.Unlock()
	if len(buffer) == 0 {
		return 0, nil
	}
	for {
		s.mu.Lock()
		if len(s.current) > 0 {
			count := copy(buffer, s.current)
			s.current = s.current[count:]
			s.count += int64(count)
			_, _ = s.hasher.Write(buffer[:count])
			s.mu.Unlock()
			return count, nil
		}
		if s.settled {
			err := s.err
			s.mu.Unlock()
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		ended := s.ended
		s.mu.Unlock()
		if ended {
			s.finish()
			continue
		}
		select {
		case <-s.ctx.Done():
			s.abort(s.ctx.Err())
		case chunk, ok := <-s.chunks:
			if !ok {
				s.mu.Lock()
				s.ended = true
				s.mu.Unlock()
				continue
			}
			s.mu.Lock()
			s.current = append(s.current[:0], chunk.Payload...)
			s.ended = chunk.End
			s.mu.Unlock()
		}
	}
}

func (s *openSource) Settle(cause error) error {
	if cause != nil {
		s.abort(cause)
		return cause
	}
	s.mu.Lock()
	settled, ended, err := s.settled, s.ended, s.err
	s.mu.Unlock()
	if !settled && ended {
		s.finish()
		s.mu.Lock()
		settled, err = s.settled, s.err
		s.mu.Unlock()
	}
	if !settled {
		err = fmt.Errorf("%w: content settled before EOF", sourcedriver.ErrIntegrity)
		s.abort(err)
	}
	return err
}

func (s *openSource) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-ctx.Done():
		s.abort(ctx.Err())
		<-s.done
		return ctx.Err()
	}
}

func (s *openSource) finish() {
	s.finishOnce.Do(s.finishResponse)
}

func (s *openSource) finishResponse() {
	s.mu.Lock()
	if s.settled {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	var response sourcedriverproto.OpenContentResponse
	result, err := s.call.Response(s.ctx)
	if err == nil {
		err = decodeResult(result, &response)
	}
	if err == nil {
		err = responseError(response.Code, response.Message, response.Actual)
	}
	if err == nil {
		ref, convertErr := domainContentRef(*response.Content)
		if convertErr != nil {
			err = convertErr
		} else if ref != s.expected {
			err = fmt.Errorf("%w: content terminal identity differs", sourcedriver.ErrIntegrity)
		}
	}
	if err == nil {
		var actual catalog.ContentHash
		copy(actual[:], s.hasher.Sum(nil))
		if s.count != s.expected.Size || actual != s.expected.Hash {
			err = fmt.Errorf("%w: streamed content size or digest differs", sourcedriver.ErrIntegrity)
		}
	}
	s.complete(err, false)
}

func (s *openSource) abort(err error) {
	s.complete(err, true)
}

func (s *openSource) complete(err error, cancel bool) {
	s.mu.Lock()
	if s.settled {
		s.mu.Unlock()
		return
	}
	s.err = err
	s.settled = true
	s.current = nil
	close(s.done)
	s.mu.Unlock()
	if cancel {
		s.call.Cancel()
	}
	s.cancel()
}

var _ contentstream.Source = (*openSource)(nil)
