package sourcedriverservice

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
	"github.com/yasyf/fusekit/transportproto"
)

const (
	streamChunkBytes = int(sourcedriverproto.MaxChunkBytes)
	settleTimeout    = 5 * time.Second
)

// Server binds one multi-authority SourceDriver to the exact v1 schema.
type Server struct {
	driver sourcedriver.Driver

	mu       sync.Mutex
	sessions map[uint64]*sessionState
}

func newServer(driver sourcedriver.Driver) *Server {
	return &Server{driver: driver, sessions: make(map[uint64]*sessionState)}
}

// bind returns the state this session keys, watching its close signal exactly
// once so open content and staged uploads die with the peer that owns them.
func (s *Server) bind(request daemonkit.Request) *sessionState {
	id := request.Session.ID()
	s.mu.Lock()
	state, bound := s.sessions[id]
	if !bound {
		state = newSessionState()
		s.sessions[id] = state
	}
	s.mu.Unlock()
	if bound {
		return state
	}
	if done := request.Session.Done(); done != nil {
		go func() {
			<-done
			s.drop(id)
		}()
	}
	return state
}

func (s *Server) drop(id uint64) {
	s.mu.Lock()
	state, bound := s.sessions[id]
	delete(s.sessions, id)
	s.mu.Unlock()
	if bound {
		_ = state.release()
	}
}

func (s *Server) release() error {
	s.mu.Lock()
	sessions := s.sessions
	s.sessions = make(map[uint64]*sessionState)
	s.mu.Unlock()
	released := make([]error, 0, len(sessions))
	for _, state := range sessions {
		released = append(released, state.release())
	}
	return errors.Join(released...)
}

func (s *Server) handlerSpecs() []transportproto.HandlerSpec {
	return []transportproto.HandlerSpec{
		{Op: string(sourcedriverproto.OperationRefresh), Handler: s.handleRefresh, Concurrent: true},
		{Op: string(sourcedriverproto.OperationInspectTargetSet), Handler: s.handleInspectTargetSet, Concurrent: true},
		{Op: string(sourcedriverproto.OperationDeclareTargetSet), Handler: s.handleDeclareTargetSet, Concurrent: true},
		{Op: string(sourcedriverproto.OperationSnapshot), Handler: s.handleSnapshot, Concurrent: true},
		{Op: string(sourcedriverproto.OperationChangesSince), Handler: s.handleChanges, Concurrent: true},
		{Op: string(sourcedriverproto.OperationOpenContent), Handler: s.handleOpenContent, Concurrent: true},
		{Op: string(sourcedriverproto.OperationReadContent), Handler: s.handleReadContent, Concurrent: true},
		{Op: string(sourcedriverproto.OperationCloseContent), Handler: s.handleCloseContent, Concurrent: true},
		{Op: string(sourcedriverproto.OperationApplyMutationBegin), Handler: s.handleApplyMutationBegin, Concurrent: true},
		{Op: string(sourcedriverproto.OperationApplyMutationChunk), Handler: s.handleApplyMutationChunk, Concurrent: true},
		{Op: string(sourcedriverproto.OperationApplyMutationCommit), Handler: s.handleApplyMutationCommit, Concurrent: true},
		{Op: string(sourcedriverproto.OperationInspectMutation), Handler: s.handleInspectMutation, Concurrent: true},
		{Op: string(sourcedriverproto.OperationSettleMutation), Handler: s.handleSettleMutation, Concurrent: true},
	}
}

func (s *Server) handleInspectTargetSet(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.InspectTargetSetRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(inspectTargetSetFailure(err))
	}
	authority := causal.SourceAuthorityID(input.Authority)
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

func (s *Server) handleDeclareTargetSet(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.DeclareTargetSetRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(declareTargetSetFailure(err))
	}
	authority := causal.SourceAuthorityID(input.Authority)
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

func (s *Server) handleRefresh(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.RefreshRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(refreshFailure(err))
	}
	head, err := s.driver.Refresh(ctx, causal.SourceAuthorityID(input.Authority))
	if err != nil {
		return encoded(refreshFailure(err))
	}
	if err := sourcedriver.ValidateHead(head); err != nil {
		return encoded(refreshFailure(errors.Join(sourcedriver.ErrIntegrity, err)))
	}
	return encoded(sourcedriverproto.RefreshResponse{Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK, Revision: string(head.Revision)})
}

func (s *Server) handleSnapshot(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.SnapshotRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(snapshotFailure(err))
	}
	authority := causal.SourceAuthorityID(input.Authority)
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

func (s *Server) handleChanges(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.ChangesSinceRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(changesFailure(err))
	}
	authority := causal.SourceAuthorityID(input.Authority)
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

func (s *Server) handleOpenContent(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.OpenContentRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(openFailure(err))
	}
	ref, err := domainContentRef(input.Content)
	if err != nil {
		return encoded(openFailure(err))
	}
	source, err := s.driver.OpenContent(ctx, causal.SourceAuthorityID(input.Authority), ref)
	if err != nil {
		return encoded(openFailure(err))
	}
	if source == nil {
		return encoded(openFailure(errors.New("source driver returned a nil content stream")))
	}
	handle, err := s.bind(request).openContent(source, ref)
	if err != nil {
		return encoded(openFailure(errors.Join(err, source.Settle(err))))
	}
	payload := protocolContentRef(ref)
	return encoded(sourcedriverproto.OpenContentResponse{
		Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK,
		Content: &payload, Handle: &handle,
	})
}

func (s *Server) handleReadContent(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.ReadContentRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(readFailure(err))
	}
	handle, err := s.bind(request).content(input.Handle)
	if err != nil {
		return encoded(readFailure(err))
	}
	payload, eof, err := handle.read(ctx, int64(input.Offset), int(input.Limit))
	if err != nil {
		return encoded(readFailure(err))
	}
	return encoded(sourcedriverproto.ReadContentResponse{
		Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK,
		Data: payload, EOF: eof,
	})
}

func (s *Server) handleCloseContent(_ context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.CloseContentRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(closeFailure(err))
	}
	handle, err := s.bind(request).takeContent(input.Handle)
	if err != nil {
		return encoded(closeFailure(err))
	}
	if err := handle.settle(nil); err != nil {
		return encoded(closeFailure(err))
	}
	return encoded(sourcedriverproto.CloseContentResponse{
		Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK,
	})
}

func (s *Server) handleApplyMutationBegin(_ context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.ApplyMutationRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(beginApplyFailure(err))
	}
	domainRequest, err := domainApplyRequest(input)
	if err != nil {
		return encoded(beginApplyFailure(err))
	}
	if err := s.bind(request).begin(input.OperationID, domainRequest); err != nil {
		return encoded(beginApplyFailure(err))
	}
	return encoded(sourcedriverproto.BeginApplyMutationResponse{
		Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK,
	})
}

func (s *Server) handleApplyMutationChunk(_ context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.ApplyMutationChunkRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(chunkApplyFailure(err))
	}
	if err := s.bind(request).chunk(input.OperationID, input.Sequence, input.Payload); err != nil {
		return encoded(chunkApplyFailure(err))
	}
	return encoded(sourcedriverproto.ApplyMutationChunkResponse{
		Protocol: sourcedriverproto.Version, Code: sourcedriverproto.ErrorCodeOK,
	})
}

func domainApplyRequest(input sourcedriverproto.ApplyMutationRequest) (sourcedriver.MutationRequest, error) {
	authority := causal.SourceAuthorityID(input.Authority)
	id, err := catalog.ParseMutationID(input.OperationID)
	if err != nil {
		return sourcedriver.MutationRequest{}, err
	}
	hash := catalog.ContentHash{}
	if input.HasContent {
		hash, err = contentHash(input.ContentHash)
		if err != nil {
			return sourcedriver.MutationRequest{}, err
		}
	}
	targetSet, err := domainTargetSetRef(input.TargetSet)
	if err != nil {
		return sourcedriver.MutationRequest{}, err
	}
	if err := sourcedriver.ValidateTargetSetRef(authority, targetSet); err != nil {
		return sourcedriver.MutationRequest{}, err
	}
	domainRequest := sourcedriver.MutationRequest{
		TargetSet: targetSet, Tenant: catalog.TenantID(input.Tenant),
		Generation: causal.Generation(input.Generation), OperationID: id,
		Expected: sourcedriver.RevisionToken(input.Expected), Context: domainMutationContext(input.Context),
		HasContent: input.HasContent, ContentSize: input.ContentSize, ContentHash: hash,
	}
	if err := sourcedriver.ValidateMutationRequest(domainRequest); err != nil {
		return sourcedriver.MutationRequest{}, err
	}
	return domainRequest, nil
}

func (s *Server) handleApplyMutationCommit(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.CommitApplyMutationRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(applyFailure(err))
	}
	authority := causal.SourceAuthorityID(input.Authority)
	pending, err := s.bind(request).takeApply(input.OperationID)
	if err != nil {
		return encoded(applyFailure(err))
	}
	domainRequest := pending.request
	var staged contentstream.Source
	if domainRequest.HasContent {
		if input.Digest == "" {
			return encoded(applyFailure(errors.Join(
				fmt.Errorf("%w: commit carries no seal for staged content", sourcedriver.ErrIntegrity), pending.close(),
			)))
		}
		hash, hashErr := contentHash(input.Digest)
		if hashErr != nil {
			return encoded(applyFailure(errors.Join(hashErr, pending.close())))
		}
		if input.Total != uint64(domainRequest.ContentSize) || hash != domainRequest.ContentHash {
			return encoded(applyFailure(errors.Join(
				fmt.Errorf("%w: commit seal differs from the begun mutation", sourcedriver.ErrIntegrity), pending.close(),
			)))
		}
		staged, err = pending.upload.source(domainRequest.ContentSize, hash)
		if err != nil {
			return encoded(applyFailure(errors.Join(err, pending.close())))
		}
	} else if input.Total != 0 || input.Digest != "" {
		return encoded(applyFailure(fmt.Errorf("%w: contentless commit carries a seal", sourcedriver.ErrIntegrity)))
	}
	receipt, applyErr := s.driver.ApplyMutation(ctx, authority, domainRequest, staged)
	if staged != nil {
		settleErr := staged.Settle(applyErr)
		waitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), settleTimeout)
		waitErr := staged.Wait(waitCtx)
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

func (s *Server) handleInspectMutation(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.InspectMutationRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
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
	receipt, err := s.driver.InspectMutation(ctx, causal.SourceAuthorityID(input.Authority), id, requestDigest)
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

func (s *Server) handleSettleMutation(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input sourcedriverproto.SettleMutationRequest
	if err := sourcedriverproto.Decode(request.Body, &input); err != nil {
		return encoded(settleFailure(err))
	}
	authority := causal.SourceAuthorityID(input.Authority)
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

func refreshFailure(err error) sourcedriverproto.RefreshResponse {
	code, message, actual := applicationError(err)
	return sourcedriverproto.RefreshResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message, Actual: actual}
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

func openFailure(err error) sourcedriverproto.OpenContentResponse {
	code, message, actual := applicationError(err)
	return sourcedriverproto.OpenContentResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message, Actual: actual}
}

func readFailure(err error) sourcedriverproto.ReadContentResponse {
	code, message, _ := applicationError(err)
	return sourcedriverproto.ReadContentResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message}
}

func closeFailure(err error) sourcedriverproto.CloseContentResponse {
	code, message, _ := applicationError(err)
	return sourcedriverproto.CloseContentResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message}
}

func beginApplyFailure(err error) sourcedriverproto.BeginApplyMutationResponse {
	code, message, _ := applicationError(err)
	return sourcedriverproto.BeginApplyMutationResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message}
}

func chunkApplyFailure(err error) sourcedriverproto.ApplyMutationChunkResponse {
	code, message, _ := applicationError(err)
	return sourcedriverproto.ApplyMutationChunkResponse{Protocol: sourcedriverproto.Version, Code: code, Message: message}
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

func encoded(value any) ([]byte, error) {
	return sourcedriverproto.Encode(value)
}

func boundedMessage(message string) string {
	message = strings.ToValidUTF8(message, "�")
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
