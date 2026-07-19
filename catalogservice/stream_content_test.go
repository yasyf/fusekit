package catalogservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

func TestStreamContentSettlesRemoteCloseBeforeSuccess(t *testing.T) {
	content := &closeErrorReader{
		Reader: bytes.NewReader([]byte("content")),
		err:    catalog.ErrIntegrity,
	}
	chunks := make(chan []byte)
	terminal := new(json.RawMessage)
	go streamContent(t.Context(), content, catalogproto.CatalogObject{}, chunks, terminal)

	var body []byte
	for chunk := range chunks {
		body = append(body, chunk...)
	}
	if string(body) != "content" {
		t.Fatalf("content = %q", body)
	}
	if content.closes != 1 {
		t.Fatalf("close count = %d, want 1", content.closes)
	}
	var response catalogproto.OpenAtResponse
	if err := catalogproto.Decode(*terminal, &response); err != nil {
		t.Fatalf("decode terminal: %v", err)
	}
	if response.Code != catalogproto.ErrorCodeIntegrity {
		t.Fatalf("terminal code = %q, want integrity", response.Code)
	}
}

func TestApplicationErrorEnforcesExactRemoteMessageBound(t *testing.T) {
	exact := strings.Repeat("x", remoteErrorMessageBytes)
	_, message := applicationError(errors.New(exact))
	if message != exact {
		t.Fatalf("exact-bound message changed: got %d bytes", len(message))
	}
	_, message = applicationError(errors.New(exact + "y"))
	if len(message) != remoteErrorMessageBytes || !strings.HasSuffix(message, "...") {
		t.Fatalf("over-bound message = %d bytes, %q suffix", len(message), message[len(message)-3:])
	}
}

type closeErrorReader struct {
	io.Reader
	err    error
	closes int
}

func (r *closeErrorReader) Close() error {
	r.closes++
	return r.err
}

func TestStreamContentClosesOnceAfterReadFailure(t *testing.T) {
	content := &closeErrorReader{
		Reader: errorReader{err: errors.New("read failed")},
	}
	chunks := make(chan []byte)
	terminal := new(json.RawMessage)
	go streamContent(t.Context(), content, catalogproto.CatalogObject{}, chunks, terminal)

	for range chunks {
	}
	if content.closes != 1 {
		t.Fatalf("close count = %d, want 1", content.closes)
	}
	var response catalogproto.OpenAtResponse
	if err := catalogproto.Decode(*terminal, &response); err != nil {
		t.Fatalf("decode terminal: %v", err)
	}
	if response.Code != catalogproto.ErrorCodeUnavailable {
		t.Fatalf("terminal code = %q, want unavailable", response.Code)
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestChunkReaderSettlementInterruptsBlockedRead(t *testing.T) {
	reader := &chunkReader{
		ctx: context.Background(), chunks: make(chan wire.Chunk),
		closed: make(chan struct{}), settled: make(chan struct{}),
	}
	result := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		result <- err
	}()

	if err := reader.Settle(context.Canceled); err != nil {
		t.Fatal(err)
	}
	if err := reader.Settle(context.Canceled); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("blocked Read succeeded after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Settle did not interrupt blocked Read")
	}
	if err := reader.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestChunkReaderRejectsSuccessfulSettlementBeforeEOF(t *testing.T) {
	reader := &chunkReader{
		ctx: context.Background(), chunks: make(chan wire.Chunk),
		closed: make(chan struct{}), settled: make(chan struct{}),
	}
	if err := reader.Settle(nil); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("Settle(nil) = %v, want integrity", err)
	}
	if err := reader.Settle(nil); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("replayed Settle(nil) = %v, want same integrity", err)
	}
	if err := reader.Wait(t.Context()); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("Wait() = %v, want integrity", err)
	}
}

func TestChunkReaderRejectsMalformedStreamFraming(t *testing.T) {
	t.Parallel()
	for name, chunks := range map[string][]wire.Chunk{
		"empty nonterminal": {{Payload: nil}},
		"oversized":         {{Payload: make([]byte, streamBufferSize+1), End: true}},
	} {
		t.Run(name, func(t *testing.T) {
			input := make(chan wire.Chunk, len(chunks))
			for _, chunk := range chunks {
				input <- chunk
			}
			reader := &chunkReader{
				ctx: context.Background(), chunks: input,
				closed: make(chan struct{}), settled: make(chan struct{}),
			}
			if _, err := reader.Read(make([]byte, 1)); !errors.Is(err, catalog.ErrInvalidObject) {
				t.Fatalf("Read() = %v, want invalid object", err)
			}
		})
	}

	closed := make(chan wire.Chunk)
	close(closed)
	reader := &chunkReader{
		ctx: context.Background(), chunks: closed,
		closed: make(chan struct{}), settled: make(chan struct{}),
	}
	if _, err := reader.Read(make([]byte, 1)); err == nil {
		t.Fatal("Read() accepted a stream without terminal framing")
	}
}

func TestValidateEmptyMutationInputRequiresOneEmptyTerminalChunk(t *testing.T) {
	valid := make(chan wire.Chunk, 1)
	valid <- wire.Chunk{End: true}
	if err := validateEmptyMutationInput(t.Context(), valid, time.Second); err != nil {
		t.Fatalf("valid terminal: %v", err)
	}

	for name, chunk := range map[string]wire.Chunk{
		"payload":     {Payload: []byte{1}, End: true},
		"nonterminal": {},
	} {
		t.Run(name, func(t *testing.T) {
			chunks := make(chan wire.Chunk, 1)
			chunks <- chunk
			if err := validateEmptyMutationInput(t.Context(), chunks, time.Second); err == nil {
				t.Fatal("invalid framing accepted")
			}
		})
	}

	closed := make(chan wire.Chunk)
	close(closed)
	if err := validateEmptyMutationInput(t.Context(), closed, time.Second); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("closed stream = %v, want integrity", err)
	}

	if err := validateEmptyMutationInput(
		context.Background(), make(chan wire.Chunk), time.Millisecond,
	); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing terminal = %v, want deadline", err)
	}
}
