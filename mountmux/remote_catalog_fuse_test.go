//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/mountproto"
	"github.com/yasyf/fusekit/mountservice"
)

func TestNativeCatalogObjectRejectsSizeOutsideNativeRange(t *testing.T) {
	_, err := nativeCatalogObject("tenant", catalogproto.CatalogObject{Size: uint64(1 << 63)})
	if !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("nativeCatalogObject oversized error = %v, want integrity", err)
	}
}

func TestRemoteWriteStageRecoversExactOperationBeforeRebase(t *testing.T) {
	client := &recordingNativeWriteClient{loseAfterApply: 1}
	stage := &remoteWriteStage{
		client: client, handle: "write", tenant: "tenant", size: 4,
	}
	if count, err := stage.WriteAt([]byte("next"), 0); err != nil || count != 4 {
		t.Fatalf("WriteAt = %d, %v", count, err)
	}
	first, err := stage.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit with lost response recovery: %v", err)
	}
	if client.commitCalls != 2 || len(client.mutations) != 1 {
		t.Fatalf("commit calls = %d, mutations = %v; want one applied mutation replayed once",
			client.commitCalls, client.mutations)
	}
	firstMutation := client.mutations[0]
	if first.ContentRevision != 4 {
		t.Fatalf("first content revision = %d, want 4", first.ContentRevision)
	}

	if count, err := stage.WriteAt([]byte("!"), stage.Size()); err != nil || count != 1 {
		t.Fatalf("WriteAt after rebase = %d, %v", count, err)
	}
	second, err := stage.Commit(context.Background())
	if err != nil {
		t.Fatalf("Commit after rebase: %v", err)
	}
	if got := client.mutations[len(client.mutations)-1]; got == firstMutation {
		t.Fatal("rebased dirty epoch reused its prior operation")
	}
	if second.ContentRevision != 5 {
		t.Fatalf("second content revision = %d, want 5", second.ContentRevision)
	}
}

func TestRemoteWriteStageFencesPayloadUntilAmbiguousCommitRecovers(t *testing.T) {
	client := &recordingNativeWriteClient{failBeforeApply: 2}
	stage := &remoteWriteStage{
		client: client, handle: "write", tenant: "tenant", size: 4,
	}
	if _, err := stage.WriteAt([]byte("next"), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if _, err := stage.Commit(context.Background()); err == nil {
		t.Fatal("Commit with two unavailable responses succeeded")
	}
	if client.commitCalls != 2 || len(client.mutations) != 0 {
		t.Fatalf("ambiguous calls = %d, mutations = %v; want no applied mutation",
			client.commitCalls, client.mutations)
	}
	if _, err := stage.WriteAt([]byte("substitute"), 0); !errors.Is(err, catalog.ErrConflict) {
		t.Fatalf("WriteAt while commit ambiguous = %v, want conflict", err)
	}
	if err := stage.Truncate(1); !errors.Is(err, catalog.ErrConflict) {
		t.Fatalf("Truncate while commit ambiguous = %v, want conflict", err)
	}
	if err := stage.Close(); err != nil {
		t.Fatalf("Close recovering exact operation: %v", err)
	}
	if client.commitCalls != 3 || len(client.mutations) != 1 {
		t.Fatalf("close recovery calls = %d, mutations = %v", client.commitCalls, client.mutations)
	}
	if !client.aborted {
		t.Fatal("stage was not aborted after exact commit recovery and rebase")
	}
}

func TestRemoteWriteStageRejectsCleanCommit(t *testing.T) {
	client := &recordingNativeWriteClient{}
	stage := &remoteWriteStage{
		client: client, handle: "write", tenant: "tenant", size: 4,
	}
	if _, err := stage.Commit(context.Background()); !errors.Is(err, catalog.ErrConflict) {
		t.Fatalf("clean Commit = %v, want conflict", err)
	}
	if client.commitCalls != 0 {
		t.Fatalf("clean commit calls = %d, want zero", client.commitCalls)
	}
}

func TestRemoteWriteStageAbortsAfterAmbiguousPayloadMutation(t *testing.T) {
	client := &recordingNativeWriteClient{}
	stage := &remoteWriteStage{
		client: client, handle: "write", tenant: "tenant", size: 4,
	}
	if _, err := stage.WriteAt([]byte("base"), 0); err != nil {
		t.Fatalf("WriteAt(base): %v", err)
	}
	client.failWrite = true
	if _, err := stage.WriteAt([]byte("unknown"), 0); err == nil {
		t.Fatal("ambiguous WriteAt succeeded")
	}
	if _, err := stage.Commit(context.Background()); !errors.Is(err, catalog.ErrInvalidTransition) {
		t.Fatalf("Commit after ambiguous write = %v, want invalid transition", err)
	}
	if client.commitCalls != 0 {
		t.Fatalf("commit calls after ambiguous write = %d, want zero", client.commitCalls)
	}
	if err := stage.Close(); err != nil {
		t.Fatalf("Close after ambiguous write: %v", err)
	}
	if !client.aborted {
		t.Fatal("ambiguous write stage was not aborted")
	}
}

type recordingNativeWriteClient struct {
	failBeforeApply int
	loseAfterApply  int
	failWrite       bool
	commitCalls     int
	mutations       []catalog.MutationID
	result          *mountproto.NativeWriteCommitResponse
	revision        catalog.Revision
	contentRevision catalog.Revision
	size            int64
	aborted         bool
}

func (c *recordingNativeWriteClient) NativeWriteRead(context.Context, string, int64, int) (mountproto.NativeWriteReadResponse, error) {
	return mountproto.NativeWriteReadResponse{}, errors.New("unexpected read")
}

func (c *recordingNativeWriteClient) NativeWrite(_ context.Context, _ string, offset int64, data []byte) (int, error) {
	c.size = max(c.size, offset+int64(len(data)))
	if c.failWrite {
		return 0, &mountservice.RemoteError{
			Code: mountproto.ErrorCodeUnavailable, Message: "injected write delivery unknown",
		}
	}
	c.result = nil
	return len(data), nil
}

func (c *recordingNativeWriteClient) NativeWriteTruncate(_ context.Context, _ string, size int64) error {
	c.size = size
	c.result = nil
	return nil
}

func (*recordingNativeWriteClient) NativeWriteSync(context.Context, string) error { return nil }

func (c *recordingNativeWriteClient) NativeWriteCommit(
	_ context.Context,
	_ string,
) (mountproto.NativeWriteCommitResponse, error) {
	c.commitCalls++
	if c.result != nil {
		return *c.result, nil
	}
	if c.failBeforeApply > 0 {
		c.failBeforeApply--
		return mountproto.NativeWriteCommitResponse{}, &mountservice.RemoteError{
			Code: mountproto.ErrorCodeUnavailable, Message: "injected delivery unknown",
		}
	}
	if c.revision == 0 {
		c.revision = 7
		c.contentRevision = 3
		c.size = max(c.size, 4)
	}
	c.revision++
	c.contentRevision++
	mutation := mutationForRevision(c.revision)
	c.mutations = append(c.mutations, mutation)
	object := mountproto.NativeObject{
		ID: "01000000000000000000000000000000", ParentID: "02000000000000000000000000000000",
		Name: "settings.json", Kind: mountproto.ObjectKindFile, Mode: 0o600, Size: c.size,
		Hash: strings.Repeat("0", 64), Revision: uint64(c.revision),
		MetadataRevision: uint64(c.revision), ContentRevision: uint64(c.contentRevision),
	}
	response := mountproto.NativeWriteCommitResponse{
		Protocol: mountproto.Version, Code: mountproto.ErrorCodeOk,
		Handle: "write", MutationID: mountproto.MutationID(mutation.String()), Object: &object,
	}
	c.result = &response
	if c.loseAfterApply > 0 {
		c.loseAfterApply--
		return mountproto.NativeWriteCommitResponse{}, &mountservice.RemoteError{
			Code: mountproto.ErrorCodeUnavailable, Message: "injected applied response loss",
		}
	}
	return response, nil
}

func (c *recordingNativeWriteClient) NativeWriteAbort(context.Context, string) error {
	c.aborted = true
	return nil
}

var _ nativeWriteClient = (*recordingNativeWriteClient)(nil)

func mutationForRevision(revision catalog.Revision) catalog.MutationID {
	var mutation catalog.MutationID
	binary.BigEndian.PutUint64(mutation[:8], uint64(revision))
	mutation[len(mutation)-1] = byte(revision)
	return mutation
}
