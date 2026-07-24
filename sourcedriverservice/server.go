package sourcedriverservice

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
)

const (
	streamChunkBytes = 64 << 10
	settleTimeout    = 5 * time.Second
)

// Server binds one multi-authority SourceDriver to exact daemonkit wire v1.
type Server struct {
	driver sourcedriver.Driver
}

// Register installs every SourceDriver operation on an exact-build server.
func Register(server *wire.Server, driver sourcedriver.Driver) (*Server, error) {
	if server == nil || driver == nil {
		return nil, errors.New("source driver service: server and driver are required")
	}
	if err := exactBuild(server.WireBuild); err != nil {
		return nil, err
	}
	service := &Server{driver: driver}
	for _, handler := range service.handlerSpecs() {
		server.Register(handler)
	}
	return service, nil
}

func (s *Server) handlerSpecs() []wire.HandlerSpec {
	return []wire.HandlerSpec{
		{Op: wire.Op(sourcedriverproto.OperationRefresh), Handler: s.handleRefresh, Concurrent: true},
		{Op: wire.Op(sourcedriverproto.OperationInspectTargetSet), Handler: s.handleInspectTargetSet, Concurrent: true},
		{Op: wire.Op(sourcedriverproto.OperationDeclareTargetSet), Handler: s.handleDeclareTargetSet, Concurrent: true},
		{Op: wire.Op(sourcedriverproto.OperationSnapshot), Handler: s.handleSnapshot, Concurrent: true},
		{Op: wire.Op(sourcedriverproto.OperationChangesSince), Handler: s.handleChanges, Concurrent: true},
		{Op: wire.Op(sourcedriverproto.OperationOpenContent), Handler: s.handleOpenContent, Concurrent: true},
		{Op: wire.Op(sourcedriverproto.OperationApplyMutation), Handler: s.handleApplyMutation, Concurrent: true},
		{Op: wire.Op(sourcedriverproto.OperationInspectMutation), Handler: s.handleInspectMutation, Concurrent: true},
		{Op: wire.Op(sourcedriverproto.OperationSettleMutation), Handler: s.handleSettleMutation, Concurrent: true},
	}
}

func (s *Server) handleInspectTargetSet(ctx context.Context, request wire.Request) (any, error) {
	authority, err := requestAuthority(request)
	if err != nil {
		return encoded(inspectTargetSetFailure(err))
	}
	var input sourcedriverproto.InspectTargetSetRequest
	if err := sourcedriverproto.Decode(request.Payload, &input); err != nil {
		return encoded(inspectTargetSetFailure(err))
	}
	ref, err := domainTargetSetRef(input.Ref)
	if err != nil {
		return encoded(inspectTargetSetFailure(err))
	}
	if err := sourcedriver.ValidateTargetSetRef(authority, ref); err != nil {
		return encoded(inspectTargetSetFailure(err))
	}
	state, err := s.driver.InspectTargetSet(ctx, authority, ref)
	if err != nil {
		return encoded(inspectTargetSetFailure(err))
	}
	if err := sourcedriver.ValidateTargetSetState(authority, state); err != nil || state.Ref != ref {
		return encoded(inspectTargetSetFailure(errors.Join(sourcedriver.ErrIntegrity, err)))
	}
	protocolState := protocolTargetSetState(state)
	return encoded(sourcedriverproto.InspectTargetSetResponse{
		Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK, State: &protocolState,
	})
}

func (s *Server) handleDeclareTargetSet(ctx context.Context, request wire.Request) (any, error) {
	authority, err := requestAuthority(request)
	if err != nil {
		return encoded(declareTargetSetFailure(err))
	}
	var input sourcedriverproto.DeclareTargetSetRequest
	if err := sourcedriverproto.Decode(request.Payload, &input); err != nil {
		return encoded(declareTargetSetFailure(err))
	}
	page, err := domainTargetSetPage(input.Page)
	if err != nil {
		return encoded(declareTargetSetFailure(err))
	}
	if err := sourcedriver.ValidateTargetSetPage(authority, page); err != nil {
		return encoded(declareTargetSetFailure(err))
	}
	state, err := s.driver.DeclareTargetSet(ctx, authority, page)
	if err != nil {
		return encoded(declareTargetSetFailure(err))
	}
	if err := sourcedriver.ValidateTargetSetState(authority, state); err != nil || state.Ref != page.Ref {
		return encoded(declareTargetSetFailure(errors.Join(sourcedriver.ErrIntegrity, err)))
	}
	protocolState := protocolTargetSetState(state)
	return encoded(sourcedriverproto.DeclareTargetSetResponse{
		Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK, State: &protocolState,
	})
}

func (s *Server) handleRefresh(ctx context.Context, request wire.Request) (any, error) {
	authority, err := requestAuthority(request)
	if err != nil {
		return encoded(sourcedriverproto.RefreshResponse{Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeInvalidRequest, Message: boundedMessage(err.Error())})
	}
	var input sourcedriverproto.RefreshRequest
	if err := sourcedriverproto.Decode(request.Payload, &input); err != nil {
		return encoded(sourcedriverproto.RefreshResponse{Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeInvalidRequest, Message: boundedMessage(err.Error())})
	}
	head, err := s.driver.Refresh(ctx, authority)
	if err != nil {
		code, message, actual := applicationError(err)
		return encoded(sourcedriverproto.RefreshResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message, Actual: actual})
	}
	if err := sourcedriver.ValidateHead(head); err != nil {
		code, message, actual := applicationError(errors.Join(sourcedriver.ErrIntegrity, err))
		return encoded(sourcedriverproto.RefreshResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message, Actual: actual})
	}
	return encoded(sourcedriverproto.RefreshResponse{Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK, Revision: string(head.Revision)})
}

func (s *Server) handleSnapshot(ctx context.Context, request wire.Request) (any, error) {
	authority, err := requestAuthority(request)
	if err != nil {
		return encoded(snapshotFailure(err))
	}
	var input sourcedriverproto.SnapshotRequest
	if err := sourcedriverproto.Decode(request.Payload, &input); err != nil {
		return encoded(snapshotFailure(err))
	}
	targetSet, err := domainTargetSetRef(input.TargetSet)
	if err != nil {
		return encoded(snapshotFailure(err))
	}
	if err := sourcedriver.ValidateTargetSetRef(authority, targetSet); err != nil {
		return encoded(snapshotFailure(err))
	}
	cursor, err := domainCursor(input.Cursor)
	if err != nil {
		return encoded(snapshotFailure(err))
	}
	domainRequest := sourcedriver.SnapshotRequest{
		TargetSet: targetSet,
		Revision:  sourcedriver.RevisionToken(input.Revision), Cursor: cursor, Limit: int(input.Limit),
	}
	page, err := s.driver.Snapshot(ctx, authority, domainRequest)
	if err != nil {
		return encoded(snapshotFailure(err))
	}
	if err := sourcedriver.ValidateSnapshotPage(domainRequest, page); err != nil {
		return encoded(snapshotFailure(errors.Join(sourcedriver.ErrIntegrity, err)))
	}
	objects := make([]sourcedriverproto.Projection, len(page.Objects))
	for index := range page.Objects {
		objects[index] = protocolProjection(page.Objects[index])
	}
	return encoded(sourcedriverproto.SnapshotResponse{
		Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK, Revision: string(page.Revision),
		Objects: objects, Next: protocolCursor(page.Next), Digest: fmt.Sprintf("%x", page.Digest),
	})
}

func (s *Server) handleChanges(ctx context.Context, request wire.Request) (any, error) {
	authority, err := requestAuthority(request)
	if err != nil {
		return encoded(changesFailure(err))
	}
	var input sourcedriverproto.ChangesSinceRequest
	if err := sourcedriverproto.Decode(request.Payload, &input); err != nil {
		return encoded(changesFailure(err))
	}
	targetSet, err := domainTargetSetRef(input.TargetSet)
	if err != nil {
		return encoded(changesFailure(err))
	}
	if err := sourcedriver.ValidateTargetSetRef(authority, targetSet); err != nil {
		return encoded(changesFailure(err))
	}
	cursor, err := domainCursor(input.Cursor)
	if err != nil {
		return encoded(changesFailure(err))
	}
	domainRequest := sourcedriver.ChangesRequest{
		TargetSet: targetSet,
		From:      sourcedriver.RevisionToken(input.From), To: sourcedriver.RevisionToken(input.To), Cursor: cursor, Limit: int(input.Limit),
	}
	page, err := s.driver.ChangesSince(ctx, authority, domainRequest)
	if err != nil {
		return encoded(changesFailure(err))
	}
	if err := sourcedriver.ValidateChangePage(domainRequest, page); err != nil {
		return encoded(changesFailure(errors.Join(sourcedriver.ErrIntegrity, err)))
	}
	changes := make([]sourcedriverproto.Change, len(page.Changes))
	for index := range page.Changes {
		changes[index] = protocolChange(page.Changes[index])
	}
	return encoded(sourcedriverproto.ChangesSinceResponse{
		Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK,
		From: string(page.From), To: string(page.To), Changes: changes,
		Next: protocolCursor(page.Next), Digest: fmt.Sprintf("%x", page.Digest),
	})
}

func (s *Server) handleOpenContent(ctx context.Context, request wire.Request) (any, error) {
	authority, err := requestAuthority(request)
	if err != nil {
		return emptyContentStream(err)
	}
	var input sourcedriverproto.OpenContentRequest
	if err := sourcedriverproto.Decode(request.Payload, &input); err != nil {
		return emptyContentStream(err)
	}
	ref, err := domainContentRef(input.Content)
	if err != nil {
		return emptyContentStream(err)
	}
	source, err := s.driver.OpenContent(ctx, authority, ref)
	if err != nil {
		return emptyContentStream(err)
	}
	if source == nil {
		return emptyContentStream(errors.New("source driver returned a nil content stream"))
	}
	chunks := make(chan []byte)
	terminal := new(json.RawMessage)
	go streamContent(ctx, source, ref, chunks, terminal)
	return wire.StreamResponse{Chunks: chunks, Value: terminal}, nil
}

func (s *Server) handleApplyMutation(ctx context.Context, request wire.Request) (any, error) {
	authority, err := requestAuthority(request)
	if err != nil {
		return encoded(applyFailure(err))
	}
	var input sourcedriverproto.ApplyMutationRequest
	if err := sourcedriverproto.Decode(request.Payload, &input); err != nil {
		return encoded(applyFailure(err))
	}
	id, err := catalog.ParseMutationID(input.OperationID)
	if err != nil {
		return encoded(applyFailure(err))
	}
	hash := catalog.ContentHash{}
	if input.HasContent {
		hash, err = contentHash(input.ContentHash)
		if err != nil {
			return encoded(applyFailure(err))
		}
	}
	targetSet, err := domainTargetSetRef(input.TargetSet)
	if err != nil {
		return encoded(applyFailure(err))
	}
	if err := sourcedriver.ValidateTargetSetRef(authority, targetSet); err != nil {
		return encoded(applyFailure(err))
	}
	domainRequest := sourcedriver.MutationRequest{
		TargetSet: targetSet, Tenant: catalog.TenantID(input.Tenant),
		Generation: causal.Generation(input.Generation), OperationID: id,
		Expected: sourcedriver.RevisionToken(input.Expected), Context: domainMutationContext(input.Context),
		HasContent: input.HasContent, ContentSize: input.ContentSize, ContentHash: hash,
	}
	if err := sourcedriver.ValidateMutationRequest(domainRequest); err != nil {
		return encoded(applyFailure(err))
	}
	var source contentstream.Source
	var incoming *incomingSource
	if input.HasContent {
		incoming = newIncomingSource(ctx, request.Chunks, input.ContentSize, hash)
		source = incoming
	} else if err := consumeEmptyTerminal(ctx, request.Chunks); err != nil {
		return encoded(applyFailure(err))
	}
	receipt, applyErr := s.driver.ApplyMutation(ctx, authority, domainRequest, source)
	if incoming != nil {
		settleErr := incoming.Settle(applyErr)
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
		waitErr := incoming.Wait(waitCtx)
		cancel()
		applyErr = errors.Join(applyErr, settleErr, waitErr)
	}
	if applyErr != nil {
		return encoded(applyFailure(applyErr))
	}
	if receipt.OperationID != domainRequest.OperationID {
		return encoded(applyFailure(fmt.Errorf("%w: mutation receipt operation id differs", sourcedriver.ErrIntegrity)))
	}
	if err := sourcedriver.ValidateMutationReceipt(receipt); err != nil {
		return encoded(applyFailure(errors.Join(sourcedriver.ErrIntegrity, err)))
	}
	requestDigest, err := sourcedriver.MutationRequestDigest(domainRequest)
	if err != nil || receipt.RequestDigest != requestDigest {
		return encoded(applyFailure(errors.Join(sourcedriver.ErrIntegrity, err, errors.New("mutation receipt request digest differs"))))
	}
	converted := protocolReceipt(receipt)
	return encoded(sourcedriverproto.ApplyMutationResponse{Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK, Receipt: &converted})
}

func (s *Server) handleInspectMutation(ctx context.Context, request wire.Request) (any, error) {
	authority, err := requestAuthority(request)
	if err != nil {
		return encoded(inspectFailure(err))
	}
	var input sourcedriverproto.InspectMutationRequest
	if err := sourcedriverproto.Decode(request.Payload, &input); err != nil {
		return encoded(inspectFailure(err))
	}
	id, err := catalog.ParseMutationID(input.OperationID)
	if err != nil {
		return encoded(inspectFailure(err))
	}
	requestDigest, err := digest(input.RequestDigest)
	if err != nil {
		return encoded(inspectFailure(err))
	}
	receipt, err := s.driver.InspectMutation(ctx, authority, id, requestDigest)
	if err != nil {
		return encoded(inspectFailure(err))
	}
	if receipt.OperationID != id {
		return encoded(inspectFailure(fmt.Errorf("%w: inspected operation id differs", sourcedriver.ErrIntegrity)))
	}
	if err := sourcedriver.ValidateMutationReceipt(receipt); err != nil {
		return encoded(inspectFailure(errors.Join(sourcedriver.ErrIntegrity, err)))
	}
	if receipt.State != sourcedriver.MutationNotFound && receipt.RequestDigest != requestDigest {
		return encoded(inspectFailure(fmt.Errorf("%w: inspected mutation request digest differs", sourcedriver.ErrIntegrity)))
	}
	converted := protocolReceipt(receipt)
	return encoded(sourcedriverproto.InspectMutationResponse{Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK, Receipt: &converted})
}

func (s *Server) handleSettleMutation(ctx context.Context, request wire.Request) (any, error) {
	authority, err := requestAuthority(request)
	if err != nil {
		return encoded(settleFailure(err))
	}
	var input sourcedriverproto.SettleMutationRequest
	if err := sourcedriverproto.Decode(request.Payload, &input); err != nil {
		return encoded(settleFailure(err))
	}
	settlement, err := domainSettlement(input.Settlement)
	if err != nil {
		return encoded(settleFailure(err))
	}
	if err := sourcedriver.ValidateTargetSetRef(authority, settlement.TargetSet); err != nil {
		return encoded(settleFailure(err))
	}
	if err := s.driver.SettleMutation(ctx, authority, settlement); err != nil {
		return encoded(settleFailure(err))
	}
	return encoded(sourcedriverproto.SettleMutationResponse{
		Protocol: sourcedriverproto.Version,
		Code:     sourcedriverproto.ErrorCodeOK,
	})
}

func streamContent(ctx context.Context, source contentstream.Source, ref sourcedriver.ContentRef, chunks chan<- []byte, terminal *json.RawMessage) {
	defer close(chunks)
	hasher := sha256.New()
	var total int64
	buffer := make([]byte, streamChunkBytes)
	finish := func(cause error) error {
		settleErr := source.Settle(cause)
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
		waitErr := source.Wait(waitCtx)
		cancel()
		return errors.Join(cause, settleErr, waitErr)
	}
	for {
		count, readErr := source.Read(buffer)
		if count > 0 {
			total += int64(count)
			_, _ = hasher.Write(buffer[:count])
			if total > ref.Size || total > sourcedriver.MaxContentBytes {
				setContentTerminal(terminal, finish(fmt.Errorf("%w: content exceeds exact size", sourcedriver.ErrIntegrity)))
				return
			}
			payload := append([]byte(nil), buffer[:count]...)
			select {
			case chunks <- payload:
			case <-ctx.Done():
				setContentTerminal(terminal, finish(ctx.Err()))
				return
			}
		}
		if errors.Is(readErr, io.EOF) {
			var actual catalog.ContentHash
			copy(actual[:], hasher.Sum(nil))
			if total != ref.Size || actual != ref.Hash {
				setContentTerminal(terminal, finish(fmt.Errorf("%w: content size or digest differs", sourcedriver.ErrIntegrity)))
				return
			}
			if err := finish(nil); err != nil {
				setContentTerminal(terminal, err)
				return
			}
			payload := protocolContentRef(ref)
			*terminal = mustEncode(sourcedriverproto.OpenContentResponse{Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK, Content: &payload})
			return
		}
		if readErr != nil || count == 0 {
			if readErr == nil {
				readErr = errors.New("content reader made no progress")
			}
			setContentTerminal(terminal, finish(readErr))
			return
		}
	}
}

func setContentTerminal(terminal *json.RawMessage, err error) {
	code, message, actual := applicationError(err)
	*terminal = mustEncode(sourcedriverproto.OpenContentResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message, Actual: actual})
}

func emptyContentStream(err error) (any, error) {
	code, message, actual := applicationError(err)
	payload, encodeErr := sourcedriverproto.Encode(sourcedriverproto.OpenContentResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message, Actual: actual})
	if encodeErr != nil {
		return nil, encodeErr
	}
	chunks := make(chan []byte)
	close(chunks)
	raw := json.RawMessage(payload)
	return wire.StreamResponse{Chunks: chunks, Value: &raw}, nil
}

func snapshotFailure(err error) sourcedriverproto.SnapshotResponse {
	code, message, actual := applicationError(err)
	return sourcedriverproto.SnapshotResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message, Actual: actual}
}

func inspectTargetSetFailure(err error) sourcedriverproto.InspectTargetSetResponse {
	code, message, _ := applicationError(err)
	return sourcedriverproto.InspectTargetSetResponse{
		Protocol: sourcedriverproto.Version, Code: code, Message: message,
	}
}

func declareTargetSetFailure(err error) sourcedriverproto.DeclareTargetSetResponse {
	code, message, _ := applicationError(err)
	return sourcedriverproto.DeclareTargetSetResponse{
		Protocol: sourcedriverproto.Version, Code: code, Message: message,
	}
}

func changesFailure(err error) sourcedriverproto.ChangesSinceResponse {
	code, message, actual := applicationError(err)
	return sourcedriverproto.ChangesSinceResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message, Actual: actual}
}

func applyFailure(err error) sourcedriverproto.ApplyMutationResponse {
	code, message, actual := applicationError(err)
	return sourcedriverproto.ApplyMutationResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message, Actual: actual}
}

func inspectFailure(err error) sourcedriverproto.InspectMutationResponse {
	code, message, _ := applicationError(err)
	return sourcedriverproto.InspectMutationResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message}
}

func settleFailure(err error) sourcedriverproto.SettleMutationResponse {
	code, message, _ := applicationError(err)
	return sourcedriverproto.SettleMutationResponse{
		Protocol: sourcedriverproto.Version,
		Code:     code,
		Message:  message,
	}
}

func requestAuthority(request wire.Request) (causal.SourceAuthorityID, error) {
	authority := causal.SourceAuthorityID(request.Tenant)
	if err := causal.ValidateSourceAuthorityID(authority); err != nil {
		return "", err
	}
	return authority, nil
}

func applicationError(err error) (sourcedriverproto.ErrorCode, string, string) {
	message := boundedMessage(err.Error())
	var snapshot *sourcedriver.SnapshotRequiredError
	var stale *sourcedriver.StaleRevisionError
	switch {
	case errors.As(err, &snapshot):
		return sourcedriverproto.ErrorCodeSnapshotRequired, message, string(snapshot.Head)
	case errors.As(err, &stale):
		return sourcedriverproto.ErrorCodeStaleRevision, message, string(stale.Actual)
	case errors.Is(err, sourcedriver.ErrInvalidValue):
		return sourcedriverproto.ErrorCodeInvalidRequest, message, ""
	case errors.Is(err, sourcedriverproto.ErrInvalidMessage), errors.Is(err, sourcedriverproto.ErrProtocol):
		return sourcedriverproto.ErrorCodeInvalidRequest, message, ""
	case errors.Is(err, sourcedriver.ErrNotFound):
		return sourcedriverproto.ErrorCodeNotFound, message, ""
	case errors.Is(err, sourcedriver.ErrConflict):
		return sourcedriverproto.ErrorCodeConflict, message, ""
	case errors.Is(err, sourcedriver.ErrIntegrity):
		return sourcedriverproto.ErrorCodeIntegrity, message, ""
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return sourcedriverproto.ErrorCodeCanceled, message, ""
	default:
		return sourcedriverproto.ErrorCodeUnavailable, message, ""
	}
}

func encoded(value any) (any, error) {
	payload, err := sourcedriverproto.Encode(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(payload), nil
}

func mustEncode(value any) json.RawMessage {
	payload, err := sourcedriverproto.Encode(value)
	if err != nil {
		panic(err)
	}
	return json.RawMessage(payload)
}

func boundedMessage(message string) string {
	message = strings.ToValidUTF8(message, "\uFFFD")
	limit := int(sourcedriverproto.MaxErrorMessageBytes)
	if len(message) <= limit {
		return message
	}
	end := limit - len("...")
	for end > 0 && !utf8.RuneStart(message[end]) {
		end--
	}
	return message[:end] + "..."
}

func consumeEmptyTerminal(ctx context.Context, chunks <-chan wire.Chunk) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case chunk, ok := <-chunks:
		if !ok || !chunk.End || len(chunk.Payload) != 0 {
			return fmt.Errorf("%w: contentless mutation has invalid terminal framing", sourcedriver.ErrIntegrity)
		}
		return nil
	}
}

type incomingSource struct {
	ctx       context.Context
	chunks    <-chan wire.Chunk
	expected  int64
	hash      catalog.ContentHash
	hasher    hashWriter
	current   []byte
	total     int64
	ended     bool
	exhausted atomic.Bool
	settle    sync.Once
	done      chan struct{}
	err       error
}

type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func newIncomingSource(ctx context.Context, chunks <-chan wire.Chunk, size int64, hash catalog.ContentHash) *incomingSource {
	return &incomingSource{ctx: ctx, chunks: chunks, expected: size, hash: hash, hasher: sha256.New(), done: make(chan struct{})}
}

func (s *incomingSource) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	for len(s.current) == 0 {
		if s.ended {
			var actual catalog.ContentHash
			copy(actual[:], s.hasher.Sum(nil))
			if s.total != s.expected || actual != s.hash {
				return 0, fmt.Errorf("%w: mutation content size or digest differs", sourcedriver.ErrIntegrity)
			}
			s.exhausted.Store(true)
			return 0, io.EOF
		}
		select {
		case <-s.ctx.Done():
			return 0, s.ctx.Err()
		case chunk, ok := <-s.chunks:
			if !ok || len(chunk.Payload) > streamChunkBytes || len(chunk.Payload) == 0 && !chunk.End {
				return 0, fmt.Errorf("%w: mutation content framing is invalid", sourcedriver.ErrIntegrity)
			}
			s.current = chunk.Payload
			s.ended = chunk.End
		}
	}
	count := copy(buffer, s.current)
	s.current = s.current[count:]
	s.total += int64(count)
	_, _ = s.hasher.Write(buffer[:count])
	if s.total > s.expected || s.total > sourcedriver.MaxContentBytes {
		return count, fmt.Errorf("%w: mutation content exceeds exact size", sourcedriver.ErrIntegrity)
	}
	return count, nil
}

func (s *incomingSource) Settle(cause error) error {
	s.settle.Do(func() {
		if cause == nil && !s.exhausted.Load() {
			s.err = fmt.Errorf("%w: mutation content settled before EOF", sourcedriver.ErrIntegrity)
		} else {
			s.err = cause
		}
		close(s.done)
	})
	return s.err
}

func (s *incomingSource) Wait(ctx context.Context) error {
	select {
	case <-s.done:
		return s.err
	case <-ctx.Done():
		_ = s.Settle(ctx.Err())
		<-s.done
		return errors.Join(ctx.Err(), s.err)
	}
}

var _ contentstream.Source = (*incomingSource)(nil)
