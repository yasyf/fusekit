package catalogworker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
)

func TestWorkerContractCarriesTheWholePayload(t *testing.T) {
	contract := childContract()
	if detail := daemonkit.MaxDetail(contract.MaxFrame); detail < maxPayloadSize {
		t.Fatalf("MaxDetail(%d) = %d, want at least the %d payload", contract.MaxFrame, detail, maxPayloadSize)
	}
	if streamChunkSize > maxPayloadSize || maxSnapshotRead > maxPayloadSize {
		t.Fatalf("chunk %d and snapshot read %d must fit the %d payload", streamChunkSize, maxSnapshotRead, maxPayloadSize)
	}
}

func TestOpenContentAtDrainsPinnedHandleAndReleasesItOnClose(t *testing.T) {
	manager, provision, object, revision := newMutableWriteManagerForTest(t)
	opened, reader, err := manager.OpenContentAt(
		t.Context(), provision.Tenant, catalog.PresentationMount,
		provision.Generation, object.ID, revision,
	)
	if err != nil {
		t.Fatal(err)
	}
	if opened.ID != object.ID || opened.Revision != revision || opened.Size != object.Size {
		t.Fatalf("opened object = %+v, want %+v", opened, object)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("drain pinned content: %v", err)
	}
	if string(body) != "initial" {
		t.Fatalf("content = %q, want %q", body, "initial")
	}
	if catalog.ContentHash(sha256.Sum256(body)) != opened.Hash {
		t.Fatalf("content digest does not match the object hash")
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close drained content: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("close replay: %v", err)
	}
}

func TestOpenContentAtRefusesReadsAfterTheHandleIsReleased(t *testing.T) {
	manager, provision, object, revision := newMutableWriteManagerForTest(t)
	_, err := managerCall(manager, t.Context(), func(client *Client) (struct{}, error) {
		opened, token, openErr := client.openContentAtForTest(t, provision, object, revision)
		if openErr != nil {
			return struct{}{}, openErr
		}
		data, eof, readErr := client.readContent(t.Context(), token, 0, streamChunkSize)
		if readErr != nil || !eof || string(data) != "initial" {
			t.Fatalf("first read = %q eof=%t err=%v", data, eof, readErr)
		}
		if _, _, readErr := client.readContent(t.Context(), token, opened.Size+1, streamChunkSize); readErr != nil {
			t.Fatalf("past-end read of a random-access handle = %v", readErr)
		}
		if err := client.closeContent(t.Context(), token, false); err != nil {
			return struct{}{}, err
		}
		if _, _, err := client.readContent(t.Context(), token, 0, streamChunkSize); !errors.Is(err, catalog.ErrHandleClosed) {
			t.Fatalf("read after close = %v, want handle closed", err)
		}
		if err := client.closeContent(t.Context(), token, false); !errors.Is(err, catalog.ErrHandleClosed) {
			t.Fatalf("close replay = %v, want handle closed", err)
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestReadContentRefusesRangesOutsideTheChunkBudget(t *testing.T) {
	manager, provision, object, revision := newMutableWriteManagerForTest(t)
	_, err := managerCall(manager, t.Context(), func(client *Client) (struct{}, error) {
		_, token, openErr := client.openContentAtForTest(t, provision, object, revision)
		if openErr != nil {
			return struct{}{}, openErr
		}
		for _, tt := range []struct {
			name   string
			offset int64
			limit  int
		}{
			{name: "negative offset", offset: -1, limit: streamChunkSize},
			{name: "zero limit", offset: 0, limit: 0},
			{name: "over budget", offset: 0, limit: streamChunkSize + 1},
		} {
			if _, _, err := client.readContent(t.Context(), token, tt.offset, tt.limit); !errors.Is(err, catalog.ErrInvalidObject) {
				t.Errorf("%s read = %v, want invalid object", tt.name, err)
			}
		}
		return struct{}{}, client.closeContent(t.Context(), token, true)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSequentialContentHandleServesOnlyItsOwnCursor(t *testing.T) {
	source := &recordingContentSource{body: []byte("sequential content")}
	handle := &sourceContentHandle{source: source}
	buffer := make([]byte, 8)
	count, eof, err := handle.readAt(buffer, 0)
	if err != nil || eof || count != 8 || string(buffer[:count]) != "sequenti" {
		t.Fatalf("first read = %q eof=%t err=%v", buffer[:count], eof, err)
	}
	if _, _, err := handle.readAt(buffer, 0); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("replayed offset = %v, want invalid object", err)
	}
	if _, _, err := handle.readAt(buffer, 16); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("skipped offset = %v, want invalid object", err)
	}
	var drained []byte
	for offset := int64(8); ; {
		count, eof, err := handle.readAt(buffer, offset)
		if err != nil {
			t.Fatalf("drain at %d: %v", offset, err)
		}
		drained = append(drained, buffer[:count]...)
		offset += int64(count)
		if eof {
			break
		}
	}
	if string(drained) != "al content" {
		t.Fatalf("drained tail = %q, want %q", drained, "al content")
	}
	if err := handle.settle(t.Context(), nil); err != nil {
		t.Fatalf("settle a fully drained source: %v", err)
	}
	if source.settled != 1 || !source.waited {
		t.Fatalf("settle count = %d waited = %t, want exactly one settle and one wait", source.settled, source.waited)
	}
}

func TestSequentialContentHandleSettlesAnUndrainedSourceWithACause(t *testing.T) {
	source := &recordingContentSource{body: []byte("sequential content")}
	handle := &sourceContentHandle{source: source}
	if _, _, err := handle.readAt(make([]byte, 4), 0); err != nil {
		t.Fatal(err)
	}
	if err := handle.settle(t.Context(), nil); err != nil {
		t.Fatalf("settle a partially drained source: %v", err)
	}
	if source.cause == nil {
		t.Fatal("partial drain settled without a cause")
	}
}

type recordingContentSource struct {
	body    []byte
	offset  int
	settled int
	cause   error
	waited  bool
}

func (s *recordingContentSource) Read(buffer []byte) (int, error) {
	if s.offset >= len(s.body) {
		return 0, io.EOF
	}
	count := copy(buffer, s.body[s.offset:])
	s.offset += count
	return count, nil
}

func (s *recordingContentSource) Settle(cause error) error {
	s.settled++
	s.cause = cause
	return nil
}

func (s *recordingContentSource) Wait(context.Context) error {
	s.waited = true
	return nil
}

func TestStageContentCommitsOnlyAnInSequenceDigestMatchedUpload(t *testing.T) {
	manager, _ := newTestManager(t)
	_, err := managerCall(manager, t.Context(), func(client *Client) (struct{}, error) {
		token, err := client.beginStageContent(t.Context())
		if err != nil {
			return struct{}{}, err
		}
		body := []byte("staged worker content")
		if err := client.stageContentChunk(t.Context(), token, 0, body); err != nil {
			return struct{}{}, err
		}
		if err := client.stageContentChunk(t.Context(), token, 0, body); !errors.Is(err, catalog.ErrInvalidTransition) {
			t.Fatalf("replayed chunk = %v, want invalid transition", err)
		}
		if err := client.stageContentChunk(t.Context(), token, 7, body); !errors.Is(err, catalog.ErrInvalidTransition) {
			t.Fatalf("skipped chunk = %v, want invalid transition", err)
		}
		if _, err := client.commitStageContent(
			t.Context(), token, 1, sha256.Sum256([]byte("some other content")),
		); !errors.Is(err, catalog.ErrIntegrity) {
			t.Fatalf("digest mismatch commit = %v, want integrity", err)
		}
		if err := client.abortStageContent(t.Context(), token); !errors.Is(err, catalog.ErrHandleClosed) {
			t.Fatalf("abort after a settled commit = %v, want handle closed", err)
		}
		ref, err := client.StageContent(t.Context(), bytes.NewReader(body))
		if err != nil {
			return struct{}{}, err
		}
		if ref.Size != int64(len(body)) || ref.Hash != sha256.Sum256(body) {
			t.Fatalf("staged ref = %+v, want size %d and the body digest", ref, len(body))
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStageContentSpansSeveralChunksAndAbortsAnAbandonedUpload(t *testing.T) {
	manager, _ := newTestManager(t)
	body := bytes.Repeat([]byte("abcdefgh"), streamChunkSize/4)
	_, err := managerCall(manager, t.Context(), func(client *Client) (struct{}, error) {
		ref, err := client.StageContent(t.Context(), bytes.NewReader(body))
		if err != nil {
			return struct{}{}, err
		}
		if ref.Size != int64(len(body)) || ref.Hash != sha256.Sum256(body) {
			t.Fatalf("multi-chunk ref = %+v, want size %d", ref, len(body))
		}
		token, err := client.beginStageContent(t.Context())
		if err != nil {
			return struct{}{}, err
		}
		if err := client.stageContentChunk(t.Context(), token, 0, []byte("partial")); err != nil {
			return struct{}{}, err
		}
		if err := client.abortStageContent(t.Context(), token); err != nil {
			t.Fatalf("abort abandoned upload: %v", err)
		}
		if _, err := client.commitStageContent(
			t.Context(), token, 1, sha256.Sum256([]byte("partial")),
		); !errors.Is(err, catalog.ErrHandleClosed) {
			t.Fatalf("commit after abort = %v, want handle closed", err)
		}
		return struct{}{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStageContentRefusesAnEmptyChunkAndAMissingSource(t *testing.T) {
	manager, _ := newTestManager(t)
	_, err := managerCall(manager, t.Context(), func(client *Client) (struct{}, error) {
		if _, err := client.StageContent(t.Context(), nil); err == nil ||
			!strings.Contains(err.Error(), "content source is required") {
			t.Fatalf("nil source = %v, want a required-source refusal", err)
		}
		token, err := client.beginStageContent(t.Context())
		if err != nil {
			return struct{}{}, err
		}
		if err := client.stageContentChunk(t.Context(), token, 0, nil); !errors.Is(err, catalog.ErrInvalidObject) {
			t.Fatalf("empty chunk = %v, want invalid object", err)
		}
		return struct{}{}, client.abortStageContent(t.Context(), token)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (c *Client) openContentAtForTest(
	t *testing.T,
	provision catalog.TenantProvision,
	object catalog.Object,
	revision catalog.Revision,
) (catalog.Object, string, error) {
	t.Helper()
	header, err := c.header()
	if err != nil {
		return catalog.Object{}, "", err
	}
	response, err := call[openAtResponse](t.Context(), c, OperationOpenAt, openAtRequest{
		Header: header, Tenant: provision.Tenant, Presentation: catalog.PresentationMount,
		Generation: provision.Generation, ID: object.ID, Revision: revision,
	})
	if err := validateResponse(header, response.Header, err); err != nil {
		return catalog.Object{}, "", err
	}
	return response.Object, response.Token, nil
}
