package sourcedriverservice

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
)

// SessionClient is the unary business lane one SourceDriver session carries.
// *daemonkit.Business satisfies it; an in-process session substitutes for it.
type SessionClient interface {
	Call(ctx context.Context, op string, body []byte) (daemonkit.Reply, error)
	Close(ctx context.Context) error
}

// Client owns one exact-schema SourceDriver session.
type Client struct {
	session   SessionClient
	deadlines map[string]time.Duration
	owns      bool
}

// NewClientOn binds SourceDriver operations to an existing business lane.
func NewClientOn(session SessionClient) (*Client, error) {
	if session == nil {
		return nil, errors.New("source driver service: session is required")
	}
	return &Client{session: session, deadlines: spawnedDeadlines(spawnedClientDeadline)}, nil
}

// NewSpawnedClient opens the business lane one supervised SourceDriver child serves.
func NewSpawnedClient(ctx context.Context, child *daemonkit.Child) (*Client, error) {
	if child == nil {
		return nil, errors.New("source driver service: child is required")
	}
	session, err := child.Business(ctx, SpawnedContract())
	if err != nil {
		return nil, err
	}
	client, err := NewClientOn(session)
	if err != nil {
		return nil, err
	}
	client.owns = true
	return client, nil
}

// Close closes an owned session.
func (c *Client) Close() error {
	if !c.owns {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), settleTimeout)
	defer cancel()
	return c.session.Close(ctx)
}

// Refresh returns the driver's exact authoritative head.
func (c *Client) Refresh(ctx context.Context, authority causal.SourceAuthorityID) (sourcedriver.Head, error) {
	var response sourcedriverproto.RefreshResponse
	err := c.unary(ctx, sourcedriverproto.OperationRefresh,
		sourcedriverproto.RefreshRequest{Protocol: sourcedriverproto.Version, Authority: string(authority)}, &response)
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
	if err := c.unary(ctx, sourcedriverproto.OperationInspectTargetSet,
		sourcedriverproto.InspectTargetSetRequest{
			Protocol: sourcedriverproto.Version, Authority: string(authority), Ref: protocolTargetSetRef(ref),
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
	if err := c.unary(ctx, sourcedriverproto.OperationDeclareTargetSet,
		sourcedriverproto.DeclareTargetSetRequest{
			Protocol: sourcedriverproto.Version, Authority: string(authority), Page: protocolTargetSetPage(page),
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
	err := c.unary(ctx, sourcedriverproto.OperationSnapshot, sourcedriverproto.SnapshotRequest{
		Protocol: sourcedriverproto.Version, Authority: string(authority), TargetSet: protocolTargetSetRef(request.TargetSet),
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
	err := c.unary(ctx, sourcedriverproto.OperationChangesSince, sourcedriverproto.ChangesSinceRequest{
		Protocol: sourcedriverproto.Version, Authority: string(authority), TargetSet: protocolTargetSetRef(request.TargetSet),
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

// OpenContent opens one immutable source body, read forward by bounded pages
// against the handle the open pinned server-side.
func (c *Client) OpenContent(ctx context.Context, authority causal.SourceAuthorityID, ref sourcedriver.ContentRef) (contentstream.Source, error) {
	if err := sourcedriver.ValidateContentRef(ref); err != nil {
		return nil, err
	}
	var response sourcedriverproto.OpenContentResponse
	if err := c.unary(ctx, sourcedriverproto.OperationOpenContent, sourcedriverproto.OpenContentRequest{
		Protocol: sourcedriverproto.Version, Authority: string(authority), Content: protocolContentRef(ref),
	}, &response); err != nil {
		return nil, err
	}
	opened, err := domainContentRef(*response.Content)
	if err != nil {
		return nil, err
	}
	if opened != ref {
		return nil, fmt.Errorf("%w: content open identity differs", sourcedriver.ErrIntegrity)
	}
	return &openSource{
		client: c, ctx: ctx, authority: authority, handle: *response.Handle, expected: ref,
		hasher: sha256.New(), done: make(chan struct{}),
	}, nil
}

// ApplyMutation stages one body page by page and returns its exact durable receipt.
func (c *Client) ApplyMutation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	request sourcedriver.MutationRequest,
	content contentstream.Source,
) (sourcedriver.MutationReceipt, error) {
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
		Protocol: sourcedriverproto.Version, Authority: string(authority),
		TargetSet: protocolTargetSetRef(request.TargetSet), Tenant: string(request.Tenant),
		Generation: uint64(request.Generation), OperationID: request.OperationID.String(), Expected: string(request.Expected),
		Context: protocolMutationContext(request.Context), HasContent: request.HasContent,
		ContentSize: request.ContentSize,
	}
	if request.HasContent {
		input.ContentHash = fmt.Sprintf("%x", request.ContentHash)
	}
	var begin sourcedriverproto.BeginApplyMutationResponse
	if err := c.unary(ctx, sourcedriverproto.OperationApplyMutationBegin, input, &begin); err != nil {
		return sourcedriver.MutationReceipt{}, settleOwned(ctx, content, err)
	}
	if content != nil {
		if err := c.upload(ctx, authority, request, content); err != nil {
			return sourcedriver.MutationReceipt{}, err
		}
	}
	commit := sourcedriverproto.CommitApplyMutationRequest{
		Protocol: sourcedriverproto.Version, Authority: string(authority),
		OperationID: request.OperationID.String(),
	}
	if request.HasContent {
		commit.Total = uint64(request.ContentSize)
		commit.Digest = fmt.Sprintf("%x", request.ContentHash)
	}
	var response sourcedriverproto.ApplyMutationResponse
	if err := c.unary(ctx, sourcedriverproto.OperationApplyMutationCommit, commit, &response); err != nil {
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

// upload stages the mutation body chunk by chunk ahead of the commit that
// consumes it, settling the caller's source exactly once whichever way the
// staging ends.
func (c *Client) upload(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	request sourcedriver.MutationRequest,
	content contentstream.Source,
) error {
	operation := request.OperationID.String()
	buffer := make([]byte, streamChunkBytes)
	hasher := sha256.New()
	var total int64
	var sequence uint32
	for {
		count, readErr := content.Read(buffer)
		if count > 0 {
			total += int64(count)
			_, _ = hasher.Write(buffer[:count])
			if total > request.ContentSize || total > sourcedriver.MaxContentBytes {
				return settleOwned(ctx, content, fmt.Errorf("%w: mutation content exceeds exact size", sourcedriver.ErrIntegrity))
			}
			sequence++
			if err := c.stage(ctx, authority, operation, sequence, buffer[:count]); err != nil {
				return settleOwned(ctx, content, err)
			}
		}
		if errors.Is(readErr, io.EOF) {
			var actual catalog.ContentHash
			copy(actual[:], hasher.Sum(nil))
			if total != request.ContentSize || actual != request.ContentHash {
				return settleOwned(ctx, content, fmt.Errorf("%w: mutation content size or digest differs", sourcedriver.ErrIntegrity))
			}
			return settleOwned(ctx, content, nil)
		}
		if readErr != nil {
			return settleOwned(ctx, content, readErr)
		}
		if count == 0 {
			return settleOwned(ctx, content, errors.New("source driver service: mutation content reader made no progress"))
		}
	}
}

func (c *Client) stage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	operation string,
	sequence uint32,
	payload []byte,
) error {
	var response sourcedriverproto.ApplyMutationChunkResponse
	return c.unary(ctx, sourcedriverproto.OperationApplyMutationChunk, sourcedriverproto.ApplyMutationChunkRequest{
		Protocol: sourcedriverproto.Version, Authority: string(authority), OperationID: operation,
		Sequence: sequence, Payload: payload,
	}, &response)
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
	err := c.unary(ctx, sourcedriverproto.OperationInspectMutation, sourcedriverproto.InspectMutationRequest{
		Protocol: sourcedriverproto.Version, Authority: string(authority),
		OperationID: id.String(), RequestDigest: fmt.Sprintf("%x", requestDigest),
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
		sourcedriverproto.SettleMutationRequest{
			Protocol: sourcedriverproto.Version, Authority: string(authority),
			Settlement: protocolSettlement(settlement),
		},
		&response,
	)
}

// unary issues one call under its operation's exact deadline and decodes the
// application header the driver answered with.
func (c *Client) unary(ctx context.Context, operation sourcedriverproto.Operation, request, response any) error {
	deadline, declared := c.deadlines[string(operation)]
	if !declared {
		return fmt.Errorf("source driver service: operation %q has no deadline", operation)
	}
	payload, err := sourcedriverproto.Encode(request)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	reply, err := c.session.Call(callCtx, string(operation), payload)
	if err != nil {
		return fmt.Errorf("source driver service: %s: %w", operation, err)
	}
	if err := sourcedriverproto.Decode(reply.Body, response); err != nil {
		return err
	}
	code, message, actual, err := responseHeader(response)
	if err != nil {
		return err
	}
	return responseError(code, message, actual)
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
	case *sourcedriverproto.ReadContentResponse:
		return value.Code, value.Message, "", nil
	case *sourcedriverproto.CloseContentResponse:
		return value.Code, value.Message, "", nil
	case *sourcedriverproto.BeginApplyMutationResponse:
		return value.Code, value.Message, "", nil
	case *sourcedriverproto.ApplyMutationChunkResponse:
		return value.Code, value.Message, "", nil
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

func settleOwned(ctx context.Context, source contentstream.Source, cause error) error {
	if source == nil {
		return cause
	}
	settleErr := source.Settle(cause)
	waitErr := source.Wait(ctx)
	return errors.Join(cause, settleErr, waitErr)
}

var _ sourcedriver.Driver = (*Client)(nil)

// openSource pages one pinned server-side content handle forward, verifying the
// body's exact size and digest before it reports EOF. The opening context
// bounds the stream: its cancellation aborts reads and releases the handle.
type openSource struct {
	client    *Client
	ctx       context.Context
	authority causal.SourceAuthorityID
	handle    sourcedriverproto.HandleID
	expected  sourcedriver.ContentRef
	hasher    hash.Hash
	done      chan struct{}

	readMu  sync.Mutex
	mu      sync.Mutex
	current []byte
	count   int64
	ended   bool
	settled bool
	err     error
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
			s.mu.Unlock()
			return count, nil
		}
		settled, ended, err := s.settled, s.ended, s.err
		s.mu.Unlock()
		if settled {
			if err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		if ended {
			return 0, io.EOF
		}
		if err := s.page(); err != nil {
			return 0, err
		}
	}
}

// page fetches the next bounded read against the handle, folding it into the
// running digest so a body that differs from its ref never reaches the caller.
func (s *openSource) page() error {
	s.mu.Lock()
	offset := s.count
	s.mu.Unlock()
	var response sourcedriverproto.ReadContentResponse
	if err := s.client.unary(s.ctx, sourcedriverproto.OperationReadContent, sourcedriverproto.ReadContentRequest{
		Protocol: sourcedriverproto.Version, Authority: string(s.authority),
		Handle: s.handle, Offset: uint64(offset), Limit: sourcedriverproto.MaxChunkBytes,
	}, &response); err != nil {
		return s.complete(errors.Join(err, s.releaseHandle()))
	}
	s.mu.Lock()
	s.current = append(s.current[:0], response.Data...)
	s.count += int64(len(response.Data))
	_, _ = s.hasher.Write(response.Data)
	s.ended = response.EOF
	count, ended := s.count, s.ended
	s.mu.Unlock()
	if count > s.expected.Size {
		return s.complete(errors.Join(
			fmt.Errorf("%w: streamed content exceeds exact size", sourcedriver.ErrIntegrity), s.releaseHandle(),
		))
	}
	if ended {
		var actual catalog.ContentHash
		copy(actual[:], s.hasher.Sum(nil))
		if count != s.expected.Size || actual != s.expected.Hash {
			return s.complete(errors.Join(
				fmt.Errorf("%w: streamed content size or digest differs", sourcedriver.ErrIntegrity), s.releaseHandle(),
			))
		}
	}
	return nil
}

func (s *openSource) Settle(cause error) error {
	s.mu.Lock()
	settled, ended, settledErr := s.settled, s.ended, s.err
	s.mu.Unlock()
	if settled {
		return settledErr
	}
	if cause == nil && !ended {
		cause = fmt.Errorf("%w: content settled before EOF", sourcedriver.ErrIntegrity)
	}
	return s.complete(errors.Join(cause, s.releaseHandle()))
}

func (s *openSource) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-ctx.Done():
		_ = s.Settle(ctx.Err())
		<-s.done
		return ctx.Err()
	}
}

func (s *openSource) releaseHandle() error {
	var response sourcedriverproto.CloseContentResponse
	return s.client.unary(context.Background(), sourcedriverproto.OperationCloseContent, sourcedriverproto.CloseContentRequest{
		Protocol: sourcedriverproto.Version, Authority: string(s.authority), Handle: s.handle,
	}, &response)
}

func (s *openSource) complete(err error) error {
	s.mu.Lock()
	if s.settled {
		settled := s.err
		s.mu.Unlock()
		return settled
	}
	s.err = err
	s.settled = true
	s.current = nil
	close(s.done)
	s.mu.Unlock()
	return err
}

var _ contentstream.Source = (*openSource)(nil)
