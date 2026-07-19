package catalogworker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
)

type requestChunkReader struct {
	ctx     context.Context
	chunks  <-chan wire.Chunk
	current []byte
	ended   bool
}

func (r *requestChunkReader) Read(buffer []byte) (int, error) {
	if len(buffer) == 0 {
		return 0, nil
	}
	for len(r.current) == 0 {
		if r.ended {
			return 0, io.EOF
		}
		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case chunk, ok := <-r.chunks:
			if !ok {
				return 0, errors.New("catalog worker: content request ended without terminal chunk")
			}
			if len(chunk.Payload) > streamChunkSize {
				return 0, errors.New("catalog worker: content request chunk exceeds limit")
			}
			r.current = chunk.Payload
			r.ended = chunk.End
			if len(r.current) == 0 && !r.ended {
				return 0, errors.New("catalog worker: empty non-terminal content chunk")
			}
		}
	}
	count := copy(buffer, r.current)
	r.current = r.current[count:]
	return count, nil
}

func (r *requestChunkReader) Close() error {
	r.current = nil
	r.ended = true
	return nil
}

func (s *server) handleStageContent(ctx context.Context, request wire.Request) (any, error) {
	var input stageContentRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return encodeResponse(stageContentResponse{Header: decodeError(err)})
	}
	response := stageContentResponse{Header: s.response(input.Header)}
	if response.Header.Error == nil {
		reader := &requestChunkReader{ctx: ctx, chunks: request.Chunks}
		response.Ref, response.Header.Error = valueResult(s.store.StageContent(ctx, reader))
		if response.Header.Error == nil {
			var extra [1]byte
			if count, err := reader.Read(extra[:]); count != 0 || !errors.Is(err, io.EOF) {
				if err == nil {
					err = errors.New("catalog worker: content request has trailing bytes")
				}
				response.Header.Error = encodeRemoteError(err)
			}
		}
	}
	if err := s.enforceWAL(context.WithoutCancel(ctx)); err != nil {
		response.Header.Error = encodeRemoteError(errors.Join(
			decodeRemoteError(response.Header.Error),
			fmt.Errorf("catalog worker: post-content WAL recovery: %w", err),
		))
	}
	return encodeResponse(response)
}

func (c *Client) StageContent(ctx context.Context, source io.Reader) (catalog.ContentRef, error) {
	if source == nil {
		return catalog.ContentRef{}, errors.New("catalog worker: content source is required")
	}
	header, err := c.header()
	if err != nil {
		return catalog.ContentRef{}, err
	}
	payload, err := json.Marshal(stageContentRequest{Header: header})
	if err != nil {
		return catalog.ContentRef{}, err
	}
	call, err := c.wire.Open(ctx, wire.Op(OperationStageContent), "", payload, false)
	if err != nil {
		return catalog.ContentRef{}, &TransportError{Message: "open content upload", Cause: err}
	}
	buffer := make([]byte, streamChunkSize)
	for {
		count, readErr := source.Read(buffer)
		if count > 0 {
			if sendErr := call.SendChunk(ctx, buffer[:count]); sendErr != nil {
				call.Cancel()
				return catalog.ContentRef{}, &TransportError{Message: "send content upload", Cause: sendErr}
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil || count == 0 {
			if readErr == nil {
				readErr = errors.New("content source made no progress")
			}
			call.Cancel()
			return catalog.ContentRef{}, &TransportError{Message: "read content upload", Cause: readErr}
		}
	}
	if err := call.CloseSend(ctx); err != nil {
		call.Cancel()
		return catalog.ContentRef{}, &TransportError{Message: "finish content upload", Cause: err}
	}
	result, err := call.Response(ctx)
	if err != nil {
		return catalog.ContentRef{}, &TransportError{Message: "settle content upload", Cause: err}
	}
	var response stageContentResponse
	if err := decodeWireResponse(result, &response); err != nil {
		return catalog.ContentRef{}, err
	}
	if err := validateResponse(header, response.Header, nil); err != nil {
		return catalog.ContentRef{}, err
	}
	return response.Ref, nil
}

func (m *Manager) StageOwnedContent(ctx context.Context, source contentstream.Source) (catalog.ContentRef, error) {
	return managerUploadCall(m, ctx, source, func(client *Client) (catalog.ContentRef, error) {
		return client.StageContent(ctx, source)
	})
}

type openAtPreamble struct {
	Object catalog.Object `json:"object"`
}

func (s *server) handleOpenAt(ctx context.Context, request wire.Request) (any, error) {
	var input openAtRequest
	if err := decodePayload(request.Payload, &input); err != nil {
		return emptyOpenAtStream(openAtResponse{Header: decodeError(err)})
	}
	response := openAtResponse{Header: s.response(input.Header)}
	if response.Header.Error != nil {
		return emptyOpenAtStream(response)
	}
	retentionOwner, err := requestRetentionOwner(input.Header)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return emptyOpenAtStream(response)
	}
	handle, err := s.store.OpenAt(
		ctx, retentionOwner, input.Tenant, input.Presentation,
		input.Generation, input.ID, input.Revision,
	)
	if err != nil {
		response.Header.Error = encodeRemoteError(err)
		return emptyOpenAtStream(response)
	}
	if err := validateWorkerObject(handle.Object); err != nil {
		_ = s.settleOperationSnapshot(context.WithoutCancel(ctx), retentionOwner, handle)
		response.Header.Error = encodeRemoteError(err)
		return emptyOpenAtStream(response)
	}
	response.Object = handle.Object
	preamble, err := json.Marshal(openAtPreamble{Object: handle.Object})
	if err != nil {
		_ = s.settleOperationSnapshot(context.WithoutCancel(ctx), retentionOwner, handle)
		response.Header.Error = encodeRemoteError(err)
		return emptyOpenAtStream(response)
	}
	chunks := make(chan []byte)
	terminal := new(json.RawMessage)
	go func() {
		defer func() {
			response.Header.Error = encodeRemoteError(errors.Join(
				decodeRemoteError(response.Header.Error),
				s.settleOperationSnapshot(context.WithoutCancel(ctx), retentionOwner, handle),
			))
			*terminal = mustResponse(response)
			close(chunks)
		}()
		select {
		case chunks <- preamble:
		case <-ctx.Done():
			response.Header.Error = encodeRemoteError(ctx.Err())
			return
		}
		buffer := make([]byte, streamChunkSize)
		for {
			count, readErr := handle.Read(buffer)
			if count > 0 {
				chunk := append([]byte(nil), buffer[:count]...)
				select {
				case chunks <- chunk:
				case <-ctx.Done():
					response.Header.Error = encodeRemoteError(ctx.Err())
					return
				}
			}
			if errors.Is(readErr, io.EOF) {
				return
			}
			if readErr != nil || count == 0 {
				if readErr == nil {
					readErr = errors.New("catalog worker: snapshot handle made no progress")
				}
				response.Header.Error = encodeRemoteError(readErr)
				return
			}
		}
	}()
	return wire.StreamResponse{Chunks: chunks, Value: terminal}, nil
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

func emptyOpenAtStream(response openAtResponse) (any, error) {
	chunks := make(chan []byte)
	close(chunks)
	raw := mustResponse(response)
	return wire.StreamResponse{Chunks: chunks, Value: &raw}, nil
}

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
	payload, err := json.Marshal(openAtRequest{
		Header: header, Tenant: tenant, Presentation: presentation,
		Generation: generation, ID: id, Revision: revision,
	})
	if err != nil {
		return catalog.Object{}, nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	call, err := c.wire.Open(streamCtx, wire.Op(OperationOpenAt), "", payload, true)
	if err != nil {
		cancel()
		return catalog.Object{}, nil, &TransportError{Message: "open snapshot stream", Cause: err}
	}
	chunks := call.Chunks()
	var first wire.Chunk
	select {
	case <-streamCtx.Done():
		cancel()
		call.Cancel()
		return catalog.Object{}, nil, &TransportError{Message: "read snapshot preamble", Cause: streamCtx.Err()}
	case chunk, ok := <-chunks:
		if !ok {
			result, responseErr := call.Response(streamCtx)
			cancel()
			var response openAtResponse
			if responseErr == nil {
				responseErr = decodeWireResponse(result, &response)
			}
			if responseErr == nil {
				responseErr = validateResponse(header, response.Header, nil)
			}
			if responseErr == nil {
				responseErr = errors.New("catalog worker: snapshot stream omitted metadata preamble")
			}
			return catalog.Object{}, nil, &TransportError{Message: "read snapshot preamble", Cause: responseErr}
		}
		first = chunk
	}
	if first.End || len(first.Payload) == 0 || len(first.Payload) > streamChunkSize {
		cancel()
		call.Cancel()
		return catalog.Object{}, nil, &TransportError{
			Message: "validate snapshot preamble framing",
			Cause:   errors.New("catalog worker: invalid snapshot metadata preamble chunk"),
		}
	}
	var preamble openAtPreamble
	if err := decodeStrictJSON(first.Payload, &preamble); err != nil {
		cancel()
		call.Cancel()
		return catalog.Object{}, nil, &TransportError{Message: "decode snapshot preamble", Cause: err}
	}
	if preamble.Object.Tenant != tenant || preamble.Object.ID != id ||
		preamble.Object.Revision != revision || preamble.Object.Kind != catalog.KindFile ||
		preamble.Object.Tombstone || !preamble.Object.Visibility.Has(presentation) ||
		preamble.Object.Size < 0 {
		cancel()
		call.Cancel()
		return catalog.Object{}, nil, &TransportError{
			Message: "validate snapshot preamble",
			Cause:   errors.New("catalog worker: snapshot metadata does not match the exact request"),
		}
	}
	reader := &openAtReader{
		ctx: streamCtx, cancel: cancel, call: call, chunks: chunks,
		request: header, object: preamble.Object,
	}
	return preamble.Object, reader, nil
}

type openAtReader struct {
	ctx     context.Context
	cancel  context.CancelFunc
	call    *wire.ClientCall
	chunks  <-chan wire.Chunk
	request requestHeader
	object  catalog.Object

	readMu sync.Mutex
	mu     sync.Mutex

	current     []byte
	streamEnded bool
	settled     bool
	err         error
}

func (r *openAtReader) Read(buffer []byte) (int, error) {
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
		ended := r.streamEnded
		r.mu.Unlock()
		if ended {
			r.settle()
			continue
		}
		select {
		case <-r.ctx.Done():
			r.abort(r.ctx.Err())
		case chunk, ok := <-r.chunks:
			if !ok {
				r.abort(errors.New("catalog worker: snapshot stream ended without terminal chunk"))
				continue
			}
			if len(chunk.Payload) > streamChunkSize ||
				(len(chunk.Payload) == 0 && !chunk.End) {
				r.abort(errors.New("catalog worker: invalid snapshot content chunk"))
				continue
			}
			r.mu.Lock()
			r.current = append(r.current[:0], chunk.Payload...)
			r.streamEnded = chunk.End
			r.mu.Unlock()
		}
	}
}

func (r *openAtReader) Close() error {
	r.mu.Lock()
	settled := r.settled
	r.mu.Unlock()
	if !settled {
		r.abort(errors.New("catalog worker: snapshot stream closed before settlement"))
	}
	r.mu.Lock()
	err := r.err
	r.mu.Unlock()
	return err
}

func (r *openAtReader) settle() {
	result, err := r.call.Response(r.ctx)
	var response openAtResponse
	if err == nil {
		err = decodeWireResponse(result, &response)
	}
	if err == nil {
		err = validateResponse(r.request, response.Header, nil)
	}
	if err == nil && response.Object != r.object {
		err = errors.New("catalog worker: snapshot stream metadata changed")
	}
	r.mu.Lock()
	if !r.settled {
		r.err = err
		r.settled = true
	}
	r.mu.Unlock()
	r.cancel()
}

func (r *openAtReader) abort(err error) {
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
	settleCtx, settleCancel := context.WithTimeout(context.Background(), defaultStopTimeout)
	_, settleErr := r.call.Response(settleCtx)
	settleCancel()
	r.mu.Lock()
	r.err = errors.Join(r.err, settleErr)
	r.mu.Unlock()
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

func decodeWireResponse[T any](result wire.Result, response *T) error {
	if result.Outcome != wire.Delivered {
		return &TransportError{Message: "request not delivered", Cause: errors.New(result.Outcome.String())}
	}
	if result.Response.Err != "" {
		return &TransportError{Message: "remote transport", Cause: errors.New(result.Response.Err)}
	}
	if err := decodeStrictJSON(result.Response.Payload, response); err != nil {
		return &TransportError{Message: "decode response", Cause: err}
	}
	return nil
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
