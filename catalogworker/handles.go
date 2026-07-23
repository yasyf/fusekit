package catalogworker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
)

const (
	maxSnapshotRead    = 1 << 20
	maxSnapshotHandles = 4096
	maxOwnerHandles    = 1024
	maxNativeOwners    = 128
)

type snapshotHandleRecord struct {
	owner  string
	handle *catalog.SnapshotHandle

	mu       sync.Mutex
	closed   bool
	closeErr error
}

type closedSnapshotHandle struct {
	owner  string
	handle *catalog.SnapshotHandle
	err    error
}

func validateNativeOwner(owner string) error {
	if owner == "" || len(owner) > 249 || strings.TrimSpace(owner) != owner {
		return fmt.Errorf("%w: invalid native session owner", catalog.ErrInvalidObject)
	}
	return nil
}

func nativeRetentionOwner(owner string) (catalog.RetentionOwner, error) {
	if err := validateNativeOwner(owner); err != nil {
		return "", err
	}
	return catalog.NewRetentionOwner("native:" + owner)
}

func requestRetentionOwner(header requestHeader) (catalog.RetentionOwner, error) {
	if err := header.validate(header.Worker); err != nil {
		return "", err
	}
	return catalog.NewRetentionOwner("stream:" + hex.EncodeToString(header.OperationID[:]))
}

func (s *server) newSnapshotToken() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return s.identity.Generation + "." + hex.EncodeToString(random[:]), nil
}

func (s *server) validSnapshotToken(token string) bool {
	random, found := strings.CutPrefix(token, s.identity.Generation+".")
	if !found || len(random) != 32 || strings.ToLower(random) != random {
		return false
	}
	decoded, err := hex.DecodeString(random)
	return err == nil && len(decoded) == 16
}

func closeAndForgetSnapshotHandle(
	ctx context.Context,
	handle *catalog.SnapshotHandle,
) error {
	if err := handle.Close(); err != nil {
		return err
	}
	return handle.Forget(ctx)
}

func snapshotHandleCapacityReached(total, owner int) bool {
	return total >= maxSnapshotHandles || owner >= maxOwnerHandles
}

func (s *server) handleOpenSnapshotAt(ctx context.Context, request wire.Request) (any, error) {
	var input openSnapshotAtRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(openSnapshotAtResponse{Header: decodeError(err)})
	}
	response := openSnapshotAtResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	if err := validateNativeOwner(input.Owner); err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	retentionOwner, err := nativeRetentionOwner(input.Owner)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	s.handleMu.Lock()
	if snapshotHandleCapacityReached(
		len(s.handles)+len(s.closedHandle), s.ownerHandles[input.Owner],
	) {
		s.handleMu.Unlock()
		response.Header.Error = encodeRemoteError(fmt.Errorf("%w: snapshot handle capacity", catalog.ErrStorageQuota))
		return encodeResponse(response)
	}
	_, ownerKnown := s.ownerHandles[input.Owner]
	if !ownerKnown && len(s.ownerHandles) >= maxNativeOwners {
		s.handleMu.Unlock()
		response.Header.Error = encodeRemoteError(fmt.Errorf("%w: native owner capacity", catalog.ErrStorageQuota))
		return encodeResponse(response)
	}
	s.handleMu.Unlock()

	handle, err := s.store.OpenAt(
		ctx, retentionOwner, input.Tenant, input.Presentation,
		input.Generation, input.ID, input.Revision,
	)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	token, err := s.newSnapshotToken()
	if err != nil {
		_ = closeAndForgetSnapshotHandle(context.WithoutCancel(ctx), handle)
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	record := &snapshotHandleRecord{owner: input.Owner, handle: handle}
	s.handleMu.Lock()
	if snapshotHandleCapacityReached(
		len(s.handles)+len(s.closedHandle), s.ownerHandles[input.Owner],
	) {
		s.handleMu.Unlock()
		response.Header.Error = encodeRemoteError(errors.Join(
			fmt.Errorf("%w: snapshot handle capacity", catalog.ErrStorageQuota),
			closeAndForgetSnapshotHandle(context.WithoutCancel(ctx), handle),
		))
		return encodeResponse(response)
	}
	s.handles[token] = record
	s.ownerHandles[input.Owner]++
	s.handleMu.Unlock()
	response.Token = token
	response.Object = handle.Object
	return encodeResponse(response)
}

func (s *server) handleReadSnapshotAt(ctx context.Context, request wire.Request) (any, error) {
	var input readSnapshotAtRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(readSnapshotAtResponse{Header: decodeError(err)})
	}
	response := readSnapshotAtResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	if err := validateNativeOwner(input.Owner); err != nil || input.Offset < 0 ||
		input.Limit <= 0 || input.Limit > maxSnapshotRead {
		if err == nil {
			err = fmt.Errorf("%w: invalid snapshot read range", catalog.ErrInvalidObject)
		}
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	s.handleMu.Lock()
	record := s.handles[input.Token]
	s.handleMu.Unlock()
	if record == nil {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.closed || record.owner != input.Owner {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	if err := ctx.Err(); err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	buffer := make([]byte, input.Limit)
	count, err := record.handle.ReadAt(buffer, input.Offset)
	if err != nil && !errors.Is(err, io.EOF) {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	response.Data = buffer[:count]
	response.EOF = errors.Is(err, io.EOF) || input.Offset+int64(count) >= record.handle.Object.Size
	return encodeResponse(response)
}

func (s *server) handleCloseSnapshot(ctx context.Context, request wire.Request) (any, error) {
	var input closeSnapshotRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(closeSnapshotResponse{Header: decodeError(err)})
	}
	response := closeSnapshotResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	if err := validateNativeOwner(input.Owner); err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	s.handleMu.Lock()
	record := s.handles[input.Token]
	if record == nil {
		closed, found := s.closedHandle[input.Token]
		s.handleMu.Unlock()
		if !found || closed.owner != input.Owner {
			response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		} else {
			response.Header.Error = encodeRemoteError(closed.err)
		}
		return encodeResponse(response)
	}
	if record.owner != input.Owner {
		s.handleMu.Unlock()
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	s.handleMu.Unlock()

	record.mu.Lock()
	if !record.closed {
		record.closed = true
		if err := record.handle.Close(); err != nil {
			record.closeErr = fmt.Errorf("%w: close snapshot handle: %v", catalog.ErrIntegrity, err)
		}
	}
	response.Header.Error = encodeRemoteError(record.closeErr)
	record.mu.Unlock()
	s.handleMu.Lock()
	if _, found := s.handles[input.Token]; found {
		delete(s.handles, input.Token)
		s.ownerHandles[input.Owner]--
		s.closedHandle[input.Token] = closedSnapshotHandle{
			owner: input.Owner, handle: record.handle, err: record.closeErr,
		}
	}
	s.handleMu.Unlock()
	return encodeResponse(response)
}

func (s *server) handleForgetSnapshot(ctx context.Context, request wire.Request) (any, error) {
	var input forgetSnapshotRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(forgetSnapshotResponse{Header: decodeError(err)})
	}
	response := forgetSnapshotResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	if err := validateNativeOwner(input.Owner); err != nil || !s.validSnapshotToken(input.Token) {
		if err == nil {
			err = fmt.Errorf("%w: invalid snapshot handle", catalog.ErrInvalidObject)
		}
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	s.handleMu.Lock()
	closed, found := s.closedHandle[input.Token]
	_, ownerKnown := s.ownerHandles[input.Owner]
	s.handleMu.Unlock()
	if !found {
		if !ownerKnown {
			response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		}
		return encodeResponse(response)
	}
	if closed.owner != input.Owner || closed.handle == nil {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	if err := closed.handle.Forget(ctx); err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	s.handleMu.Lock()
	if current, exists := s.closedHandle[input.Token]; exists &&
		current.owner == input.Owner && current.handle == closed.handle {
		delete(s.closedHandle, input.Token)
	}
	s.handleMu.Unlock()
	return encodeResponse(response)
}

func (s *server) handleCloseNativeSession(ctx context.Context, request wire.Request) (any, error) {
	var input closeNativeSessionRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(closeNativeSessionResponse{Header: decodeError(err)})
	}
	response := closeNativeSessionResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	if err := validateNativeOwner(input.Owner); err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	s.handleMu.Lock()
	records := make([]*snapshotHandleRecord, 0, s.ownerHandles[input.Owner])
	for token, record := range s.handles {
		if record.owner == input.Owner {
			records = append(records, record)
			delete(s.handles, token)
		}
	}
	for token, closed := range s.closedHandle {
		if closed.owner == input.Owner {
			delete(s.closedHandle, token)
		}
	}
	delete(s.ownerHandles, input.Owner)
	s.handleMu.Unlock()
	var closeErr error
	for _, record := range records {
		record.mu.Lock()
		if !record.closed {
			record.closed = true
			closeErr = errors.Join(closeErr, record.handle.Close())
		}
		record.mu.Unlock()
	}
	response.Header.Error = encodeRemoteError(closeErr)
	s.writeMu.Lock()
	writeErr := s.closeOwnerWrites(ctx, input.Owner)
	s.writeMu.Unlock()
	var retireErr error
	if writeErr == nil {
		retentionOwner, err := nativeRetentionOwner(input.Owner)
		if err != nil {
			retireErr = err
		} else {
			for {
				retirement, err := s.store.RetireRetentionOwner(ctx, retentionOwner)
				if err != nil {
					retireErr = err
					break
				}
				if !retirement.More {
					break
				}
			}
		}
	}
	response.Header.Error = encodeRemoteError(errors.Join(
		decodeRemoteError(response.Header.Error), writeErr, retireErr,
	))
	return encodeResponse(response)
}

func (s *server) closeSnapshotHandles() error {
	s.handleMu.Lock()
	records := make([]*snapshotHandleRecord, 0, len(s.handles))
	for _, record := range s.handles {
		records = append(records, record)
	}
	s.handles = make(map[string]*snapshotHandleRecord)
	s.closedHandle = make(map[string]closedSnapshotHandle)
	s.ownerHandles = make(map[string]int)
	s.handleMu.Unlock()
	var result error
	for _, record := range records {
		record.mu.Lock()
		if !record.closed {
			record.closed = true
			result = errors.Join(result, record.handle.Close())
		}
		record.mu.Unlock()
	}
	return result
}

func (c *Client) OpenSnapshotAt(
	ctx context.Context,
	owner string,
	tenant catalog.TenantID,
	presentation catalog.Presentation,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (string, catalog.Object, error) {
	header, err := c.header()
	if err != nil {
		return "", catalog.Object{}, err
	}
	response, err := call[openSnapshotAtResponse](ctx, c.wire, OperationOpenSnapshotAt, openSnapshotAtRequest{
		Header: header, Owner: owner, Tenant: tenant, Presentation: presentation,
		Generation: generation, ID: id, Revision: revision,
	})
	if err := validateResponse(header, response.Header, err); err != nil {
		return "", catalog.Object{}, err
	}
	return response.Token, response.Object, nil
}

func (c *Client) ReadSnapshotAt(
	ctx context.Context, owner, token string, offset int64, limit int,
) ([]byte, bool, error) {
	header, err := c.header()
	if err != nil {
		return nil, false, err
	}
	response, err := call[readSnapshotAtResponse](ctx, c.wire, OperationReadSnapshotAt, readSnapshotAtRequest{
		Header: header, Owner: owner, Token: token, Offset: offset, Limit: limit,
	})
	if err := validateResponse(header, response.Header, err); err != nil {
		return nil, false, err
	}
	return response.Data, response.EOF, nil
}

func (c *Client) CloseSnapshot(ctx context.Context, owner, token string) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[closeSnapshotResponse](ctx, c.wire, OperationCloseSnapshot, closeSnapshotRequest{
		Header: header, Owner: owner, Token: token,
	})
	if err := validateResponse(header, response.Header, err); err != nil {
		return err
	}
	forgetHeader, err := c.header()
	if err != nil {
		return err
	}
	forgotten, err := call[forgetSnapshotResponse](
		ctx, c.wire, OperationForgetSnapshot,
		forgetSnapshotRequest{Header: forgetHeader, Owner: owner, Token: token},
	)
	return validateResponse(forgetHeader, forgotten.Header, err)
}

func (c *Client) CloseNativeSession(ctx context.Context, owner string) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[closeNativeSessionResponse](ctx, c.wire, OperationCloseNativeSession, closeNativeSessionRequest{
		Header: header, Owner: owner,
	})
	return validateResponse(header, response.Header, err)
}

func (m *Manager) OpenSnapshotAt(
	ctx context.Context,
	owner string,
	tenant catalog.TenantID,
	presentation catalog.Presentation,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (string, catalog.Object, error) {
	type openedSnapshot struct {
		token  string
		object catalog.Object
	}
	result, err := nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (openedSnapshot, error) {
		return managerCall(m, operationCtx, func(client *Client) (openedSnapshot, error) {
			token, object, openErr := client.OpenSnapshotAt(
				operationCtx, owner, tenant, presentation, generation, id, revision,
			)
			return openedSnapshot{token: token, object: object}, openErr
		})
	})
	return result.token, result.object, err
}

func (m *Manager) ReadSnapshotAt(
	ctx context.Context, owner, token string, offset int64, limit int,
) ([]byte, bool, error) {
	type snapshotRead struct {
		data []byte
		eof  bool
	}
	result, err := nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (snapshotRead, error) {
		return managerCall(m, operationCtx, func(client *Client) (snapshotRead, error) {
			data, eof, readErr := client.ReadSnapshotAt(operationCtx, owner, token, offset, limit)
			return snapshotRead{data: data, eof: eof}, readErr
		})
	})
	return result.data, result.eof, err
}

func (m *Manager) CloseSnapshot(ctx context.Context, owner, token string) error {
	_, err := nativeOwnerCall(m, ctx, owner, func(operationCtx context.Context) (struct{}, error) {
		return struct{}{}, m.handleCall(operationCtx, func(client *Client) error {
			return client.CloseSnapshot(operationCtx, owner, token)
		})
	})
	return err
}

func (m *Manager) CloseNativeSession(ctx context.Context, owner string) error {
	return m.closeNativeOwner(ctx, owner, func(cleanupCtx context.Context) error {
		return m.handleCall(cleanupCtx, func(client *Client) error {
			return client.CloseNativeSession(cleanupCtx, owner)
		})
	})
}

func (m *Manager) handleCall(ctx context.Context, call func(*Client) error) error {
	_, err := managerCall(m, ctx, func(client *Client) (struct{}, error) {
		return struct{}{}, call(client)
	})
	return err
}
