package catalogworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
)

const (
	maxContentHandles = 1024
	maxStageUploads   = 64
)

type contentHandle interface {
	readAt(buffer []byte, offset int64) (int, bool, error)
	settle(ctx context.Context, cause error) error
}

type contentHandleRecord struct {
	mu     sync.Mutex
	handle contentHandle
}

type snapshotContentHandle struct {
	service *server
	owner   catalog.RetentionOwner
	handle  *catalog.SnapshotHandle
}

func (h *snapshotContentHandle) readAt(buffer []byte, offset int64) (int, bool, error) {
	count, err := h.handle.ReadAt(buffer, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, false, err
	}
	eof := errors.Is(err, io.EOF) || offset+int64(count) >= h.handle.Object.Size
	if count == 0 && !eof {
		return 0, false, errors.New("catalog worker: snapshot handle made no progress")
	}
	return count, eof, nil
}

func (h *snapshotContentHandle) settle(ctx context.Context, _ error) error {
	return h.service.settleOperationSnapshot(ctx, h.owner, h.handle)
}

type sourceContentHandle struct {
	source   contentstream.Source
	consumed int64
	eof      bool
}

func (h *sourceContentHandle) readAt(buffer []byte, offset int64) (int, bool, error) {
	if offset != h.consumed {
		return 0, false, fmt.Errorf(
			"%w: sequential content read at %d expects offset %d",
			catalog.ErrInvalidObject, offset, h.consumed,
		)
	}
	if h.eof {
		return 0, true, nil
	}
	count, err := h.source.Read(buffer)
	h.consumed += int64(count)
	if errors.Is(err, io.EOF) {
		h.eof = true
		return count, true, nil
	}
	if err != nil {
		return 0, false, err
	}
	if count == 0 {
		return 0, false, errors.New("catalog worker: content reader made no progress")
	}
	return count, false, nil
}

func (h *sourceContentHandle) settle(ctx context.Context, cause error) error {
	if cause == nil && !h.eof {
		cause = errors.New("catalog worker: content stream closed before its end")
	}
	settleErr := h.source.Settle(cause)
	waitCtx, waitCancel := context.WithTimeout(context.WithoutCancel(ctx), workerWALTimeout)
	waitErr := h.source.Wait(waitCtx)
	waitCancel()
	return errors.Join(settleErr, waitErr)
}

func (s *server) retainContent(ctx context.Context, handle contentHandle) (string, error) {
	token, err := s.newSnapshotToken()
	if err != nil {
		return "", errors.Join(err, handle.settle(context.WithoutCancel(ctx), err))
	}
	s.contentMu.Lock()
	if len(s.content) >= maxContentHandles {
		s.contentMu.Unlock()
		capacity := fmt.Errorf("%w: content handle capacity", catalog.ErrStorageQuota)
		return "", errors.Join(capacity, handle.settle(context.WithoutCancel(ctx), capacity))
	}
	s.content[token] = &contentHandleRecord{handle: handle}
	s.contentMu.Unlock()
	return token, nil
}

func (s *server) handleReadContent(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input readContentRequest
	if err := decodePayload(request.Body, &input); err != nil {
		return encodeResponse(readContentResponse{Header: decodeError(err)})
	}
	response := readContentResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	if input.Offset < 0 || input.Limit <= 0 || input.Limit > streamChunkSize {
		response.Header.Error = encodeRemoteError(
			fmt.Errorf("%w: invalid content read range", catalog.ErrInvalidObject),
		)
		return encodeResponse(response)
	}
	s.contentMu.Lock()
	record := s.content[input.Token]
	s.contentMu.Unlock()
	if record == nil {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.handle == nil {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	if err := ctx.Err(); err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	buffer := make([]byte, input.Limit)
	count, eof, err := record.handle.readAt(buffer, input.Offset)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	response.Data, response.EOF = buffer[:count], eof
	return encodeResponse(response)
}

func (s *server) handleCloseContent(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input closeContentRequest
	if err := decodePayload(request.Body, &input); err != nil {
		return encodeResponse(closeContentResponse{Header: decodeError(err)})
	}
	response := closeContentResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	s.contentMu.Lock()
	record := s.content[input.Token]
	delete(s.content, input.Token)
	s.contentMu.Unlock()
	if record == nil {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	var cause error
	if input.Abort {
		cause = errors.New("catalog worker: content stream aborted by its reader")
	}
	record.mu.Lock()
	handle := record.handle
	record.handle = nil
	record.mu.Unlock()
	if handle == nil {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	response.Header.Error = encodeRemoteError(handle.settle(ctx, cause))
	return encodeResponse(response)
}

func (s *server) settleContentHandles(ctx context.Context) error {
	s.contentMu.Lock()
	records := make([]*contentHandleRecord, 0, len(s.content))
	for _, record := range s.content {
		records = append(records, record)
	}
	s.content = make(map[string]*contentHandleRecord)
	s.contentMu.Unlock()
	shutdown := errors.New("catalog worker: session ended with the content stream open")
	var result error
	for _, record := range records {
		record.mu.Lock()
		handle := record.handle
		record.handle = nil
		record.mu.Unlock()
		if handle != nil {
			result = errors.Join(result, handle.settle(ctx, shutdown))
		}
	}
	return result
}

func (s *server) handleOpenAt(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input openAtRequest
	if err := decodePayload(request.Body, &input); err != nil {
		return encodeResponse(openAtResponse{Header: decodeError(err)})
	}
	response := openAtResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	retentionOwner, err := requestRetentionOwner(input.Header)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	handle, err := s.store.OpenAt(
		ctx, retentionOwner, input.Tenant, input.Presentation,
		input.Generation, input.ID, input.Revision,
	)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	pinned := &snapshotContentHandle{service: s, owner: retentionOwner, handle: handle}
	if err := validateWorkerObject(handle.Object); err != nil {
		response.Header.Error = encodeRemoteError(errors.Join(
			err, pinned.settle(context.WithoutCancel(ctx), err),
		))
		return encodeResponse(response)
	}
	response.Object = handle.Object
	response.Token, response.Header.Error = valueResult(s.retainContent(ctx, pinned))
	return encodeResponse(response)
}

func (s *server) settleOperationSnapshot(
	ctx context.Context,
	owner catalog.RetentionOwner,
	handle *catalog.SnapshotHandle,
) error {
	if err := handle.Close(); err != nil {
		return err
	}
	if err := handle.Forget(ctx); err != nil {
		return err
	}
	for {
		retirement, err := s.store.RetireRetentionOwner(ctx, owner)
		if err != nil {
			return err
		}
		if !retirement.More {
			return nil
		}
	}
}

type stageRecord struct {
	writer *io.PipeWriter
	cancel context.CancelFunc
	done   chan struct{}
	ref    catalog.ContentRef
	err    error

	mu   sync.Mutex
	seq  uint64
	live bool
}

func (s *server) handleBeginStageContent(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input beginStageContentRequest
	if err := decodePayload(request.Body, &input); err != nil {
		return encodeResponse(beginStageContentResponse{Header: decodeError(err)})
	}
	response := beginStageContentResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	token, err := s.newSnapshotToken()
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	s.stageMu.Lock()
	if len(s.stages) >= maxStageUploads {
		s.stageMu.Unlock()
		response.Header.Error = encodeRemoteError(
			fmt.Errorf("%w: content upload capacity", catalog.ErrStorageQuota),
		)
		return encodeResponse(response)
	}
	reader, writer := io.Pipe()
	stageCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	record := &stageRecord{writer: writer, cancel: cancel, done: make(chan struct{}), live: true}
	s.stages[token] = record
	s.stageMu.Unlock()
	go func() {
		defer close(record.done)
		record.ref, record.err = s.store.StageContent(stageCtx, reader)
		_ = reader.CloseWithError(record.err)
	}()
	response.Token = token
	return encodeResponse(response)
}

func (s *server) handleStageContentChunk(_ context.Context, request daemonkit.Request) ([]byte, error) {
	var input stageContentChunkRequest
	if err := decodePayload(request.Body, &input); err != nil {
		return encodeResponse(stageContentChunkResponse{Header: decodeError(err)})
	}
	response := stageContentChunkResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	if len(input.Payload) == 0 || len(input.Payload) > streamChunkSize {
		response.Header.Error = encodeRemoteError(
			fmt.Errorf("%w: invalid content upload chunk", catalog.ErrInvalidObject),
		)
		return encodeResponse(response)
	}
	record := s.stage(input.Token)
	if record == nil {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if !record.live || input.Seq != record.seq {
		response.Header.Error = encodeRemoteError(fmt.Errorf(
			"%w: content upload chunk %d is out of sequence", catalog.ErrInvalidTransition, input.Seq,
		))
		return encodeResponse(response)
	}
	if _, err := record.writer.Write(input.Payload); err != nil {
		record.live = false
		response.Header.Error = encodeRemoteError(err)
		return encodeResponse(response)
	}
	record.seq++
	return encodeResponse(response)
}

func (s *server) handleCommitStageContent(ctx context.Context, request daemonkit.Request) ([]byte, error) {
	var input commitStageContentRequest
	if err := decodePayload(request.Body, &input); err != nil {
		return encodeResponse(commitStageContentResponse{Header: decodeError(err)})
	}
	response := commitStageContentResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	record := s.takeStage(input.Token)
	if record == nil {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	record.mu.Lock()
	sequenced := record.live && input.Seq == record.seq
	record.live = false
	record.mu.Unlock()
	if !sequenced {
		response.Header.Error = encodeRemoteError(errors.Join(
			fmt.Errorf("%w: content upload commit is out of sequence", catalog.ErrInvalidTransition),
			s.abandonStage(record),
		))
		return encodeResponse(response)
	}
	closeErr := record.writer.Close()
	<-record.done
	record.cancel()
	staged := errors.Join(closeErr, record.err)
	switch {
	case staged != nil:
		response.Header.Error = encodeRemoteError(staged)
	case record.ref.Hash != input.Digest:
		response.Header.Error = encodeRemoteError(fmt.Errorf(
			"%w: staged content digest %x does not match the uploaded %x",
			catalog.ErrIntegrity, record.ref.Hash, input.Digest,
		))
	default:
		response.Ref = record.ref
	}
	if err := s.enforceWAL(context.WithoutCancel(ctx)); err != nil {
		response.Header.Error = encodeRemoteError(errors.Join(
			decodeRemoteError(response.Header.Error),
			fmt.Errorf("catalog worker: post-content WAL recovery: %w", err),
		))
	}
	return encodeResponse(response)
}

func (s *server) handleAbortStageContent(_ context.Context, request daemonkit.Request) ([]byte, error) {
	var input abortStageContentRequest
	if err := decodePayload(request.Body, &input); err != nil {
		return encodeResponse(abortStageContentResponse{Header: decodeError(err)})
	}
	response := abortStageContentResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return encodeResponse(response)
	}
	record := s.takeStage(input.Token)
	if record == nil {
		response.Header.Error = encodeRemoteError(catalog.ErrHandleClosed)
		return encodeResponse(response)
	}
	response.Header.Error = encodeRemoteError(s.abandonStage(record))
	return encodeResponse(response)
}

func (s *server) stage(token string) *stageRecord {
	s.stageMu.Lock()
	defer s.stageMu.Unlock()
	return s.stages[token]
}

func (s *server) takeStage(token string) *stageRecord {
	s.stageMu.Lock()
	defer s.stageMu.Unlock()
	record := s.stages[token]
	delete(s.stages, token)
	return record
}

var errStageAbandoned = errors.New("catalog worker: content upload abandoned")

func (s *server) abandonStage(record *stageRecord) error {
	record.mu.Lock()
	record.live = false
	record.mu.Unlock()
	_ = record.writer.CloseWithError(errStageAbandoned)
	<-record.done
	record.cancel()
	if errors.Is(record.err, errStageAbandoned) {
		return nil
	}
	return record.err
}

func (s *server) abandonStageUploads() error {
	s.stageMu.Lock()
	records := make([]*stageRecord, 0, len(s.stages))
	for _, record := range s.stages {
		records = append(records, record)
	}
	s.stages = make(map[string]*stageRecord)
	s.stageMu.Unlock()
	var result error
	for _, record := range records {
		result = errors.Join(result, s.abandonStage(record))
	}
	return result
}

// StageContent uploads one immutable blob as sequenced bounded chunks and
// settles it against the digest the client computed over the same bytes.
func (c *Client) StageContent(ctx context.Context, source io.Reader) (catalog.ContentRef, error) {
	if source == nil {
		return catalog.ContentRef{}, errors.New("catalog worker: content source is required")
	}
	token, err := c.beginStageContent(ctx)
	if err != nil {
		return catalog.ContentRef{}, err
	}
	digest := sha256.New()
	buffer := make([]byte, streamChunkSize)
	for seq := uint64(0); ; {
		count, readErr := source.Read(buffer)
		if count > 0 {
			digest.Write(buffer[:count])
			if err := c.stageContentChunk(ctx, token, seq, buffer[:count]); err != nil {
				return catalog.ContentRef{}, errors.Join(err, c.abortStageContent(ctx, token))
			}
			seq++
		}
		if errors.Is(readErr, io.EOF) {
			var expected catalog.ContentHash
			copy(expected[:], digest.Sum(nil))
			return c.commitStageContent(ctx, token, seq, expected)
		}
		if readErr != nil || count == 0 {
			if readErr == nil {
				readErr = errors.New("content source made no progress")
			}
			return catalog.ContentRef{}, errors.Join(
				&TransportError{Message: "read content upload", Cause: readErr},
				c.abortStageContent(ctx, token),
			)
		}
	}
}

func (c *Client) beginStageContent(ctx context.Context) (string, error) {
	header, err := c.header()
	if err != nil {
		return "", err
	}
	response, err := call[beginStageContentResponse](
		ctx, c, OperationBeginStageContent, beginStageContentRequest{Header: header},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return "", err
	}
	return response.Token, nil
}

func (c *Client) stageContentChunk(ctx context.Context, token string, seq uint64, payload []byte) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[stageContentChunkResponse](
		ctx, c, OperationStageContentChunk,
		stageContentChunkRequest{Header: header, Token: token, Seq: seq, Payload: payload},
	)
	return validateResponse(header, response.Header, err)
}

func (c *Client) commitStageContent(
	ctx context.Context, token string, seq uint64, digest catalog.ContentHash,
) (catalog.ContentRef, error) {
	header, err := c.header()
	if err != nil {
		return catalog.ContentRef{}, err
	}
	response, err := call[commitStageContentResponse](
		ctx, c, OperationCommitStageContent,
		commitStageContentRequest{Header: header, Token: token, Seq: seq, Digest: digest},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.ContentRef{}, err
	}
	return response.Ref, nil
}

func (c *Client) abortStageContent(ctx context.Context, token string) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[abortStageContentResponse](
		ctx, c, OperationAbortStageContent, abortStageContentRequest{Header: header, Token: token},
	)
	return validateResponse(header, response.Header, err)
}

func (m *Manager) StageOwnedContent(ctx context.Context, source contentstream.Source) (catalog.ContentRef, error) {
	return managerUploadCall(m, ctx, source, func(client *Client) (catalog.ContentRef, error) {
		return client.StageContent(ctx, source)
	})
}

// OpenAt pins one exact object revision server-side and returns a reader that
// drains it with bounded read-content calls.
func (c *Client) OpenAt(
	ctx context.Context,
	tenant catalog.TenantID,
	presentation catalog.Presentation,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (catalog.Object, io.ReadCloser, error) {
	header, err := c.header()
	if err != nil {
		return catalog.Object{}, nil, err
	}
	response, err := call[openAtResponse](
		ctx, c, OperationOpenAt, openAtRequest{
			Header: header, Tenant: tenant, Presentation: presentation,
			Generation: generation, ID: id, Revision: revision,
		})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.Object{}, nil, err
	}
	object := response.Object
	if object.Tenant != tenant || object.ID != id || object.Revision != revision ||
		object.Kind != catalog.KindFile || object.Tombstone ||
		!object.Visibility.Has(presentation) || object.Size < 0 {
		return catalog.Object{}, nil, errors.Join(
			&TransportError{
				Message: "validate snapshot metadata",
				Cause:   errors.New("catalog worker: snapshot metadata does not match the exact request"),
			},
			c.closeContent(ctx, response.Token, true),
		)
	}
	return object, c.contentReader(ctx, response.Token), nil
}

func (c *Client) contentReader(ctx context.Context, token string) *pagedContentReader {
	return &pagedContentReader{ctx: ctx, client: c, token: token, done: make(chan struct{})}
}

func (c *Client) readContent(ctx context.Context, token string, offset int64, limit int) ([]byte, bool, error) {
	header, err := c.header()
	if err != nil {
		return nil, false, err
	}
	response, err := call[readContentResponse](
		ctx, c, OperationReadContent,
		readContentRequest{Header: header, Token: token, Offset: offset, Limit: limit},
	)
	if err := validateResponse(header, response.Header, err); err != nil {
		return nil, false, err
	}
	return response.Data, response.EOF, nil
}

func (c *Client) closeContent(ctx context.Context, token string, abort bool) error {
	header, err := c.header()
	if err != nil {
		return err
	}
	response, err := call[closeContentResponse](
		ctx, c, OperationCloseContent,
		closeContentRequest{Header: header, Token: token, Abort: abort},
	)
	return validateResponse(header, response.Header, err)
}

// pagedContentReader satisfies io.ReadCloser and contentstream.Source at once:
// both settle through the same close call.
type pagedContentReader struct {
	ctx    context.Context
	client *Client
	token  string

	readMu sync.Mutex
	mu     sync.Mutex

	current  []byte
	offset   int64
	streamed bool
	settled  bool
	err      error
	done     chan struct{}
}

func (r *pagedContentReader) Read(buffer []byte) (int, error) {
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
		if r.streamed {
			r.mu.Unlock()
			r.close(nil)
			continue
		}
		offset := r.offset
		r.mu.Unlock()
		data, eof, err := r.client.readContent(r.ctx, r.token, offset, streamChunkSize)
		if err != nil {
			r.close(err)
			continue
		}
		r.mu.Lock()
		r.current = append(r.current[:0], data...)
		r.offset += int64(len(data))
		r.streamed = eof
		r.mu.Unlock()
	}
}

// Close releases the server-side handle; closing mid-stream aborts it.
func (r *pagedContentReader) Close() error {
	r.mu.Lock()
	settled, streamed := r.settled, r.streamed
	r.mu.Unlock()
	if settled || streamed {
		r.close(nil)
	} else {
		r.close(errors.New("catalog worker: content stream closed before its end"))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *pagedContentReader) Settle(result error) error {
	r.close(result)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *pagedContentReader) Wait(ctx context.Context) error {
	select {
	case <-r.done:
	case <-ctx.Done():
		r.close(ctx.Err())
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *pagedContentReader) close(cause error) {
	r.mu.Lock()
	if r.settled {
		r.mu.Unlock()
		<-r.done
		return
	}
	r.settled = true
	r.current = nil
	r.err = cause
	r.mu.Unlock()
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), defaultStopTimeout)
	closeErr := r.client.closeContent(closeCtx, r.token, cause != nil)
	cancel()
	r.mu.Lock()
	r.err = errors.Join(r.err, closeErr)
	r.mu.Unlock()
	close(r.done)
}

func (m *Manager) OpenContentAt(
	ctx context.Context,
	tenant catalog.TenantID,
	presentation catalog.Presentation,
	generation catalog.Generation,
	id catalog.ObjectID,
	revision catalog.Revision,
) (catalog.Object, io.ReadCloser, error) {
	type openedContent struct {
		object catalog.Object
		reader io.ReadCloser
	}
	opened, worker, err := managerGenerationCall(m, ctx, func(client *Client) (openedContent, error) {
		object, reader, openErr := client.OpenAt(ctx, tenant, presentation, generation, id, revision)
		return openedContent{object: object, reader: reader}, openErr
	})
	if err != nil {
		return catalog.Object{}, nil, err
	}
	return opened.object, newManagedContentReader(
		ctx, opened.reader, m, worker, opened.object.Size, opened.object.Hash,
	), nil
}

func decodeStrictJSON(payload []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("catalog worker: trailing JSON")
		}
		return err
	}
	return nil
}
