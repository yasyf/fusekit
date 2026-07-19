package catalogservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/transportproto"
)

const testTenant catalogproto.TenantID = "acct-18"

func TestPersistentCatalogTransportPreservesOperationBoundaries(t *testing.T) {
	reader := newFakeReader(10_000)
	mutations := &fakeMutations{}
	server, path := startCatalogServer(t, reader, mutations)
	client := newCatalogClient(t, path)
	ctx := context.Background()

	head, err := client.Head(ctx, testTenant, 7)
	if err != nil || head.Revision != 2 {
		t.Fatalf("Head() = %#v, %v", head, err)
	}
	var (
		after *catalogproto.ObjectID
		seen  int
	)
	for {
		parent := catalogproto.ObjectID(reader.objects[0].ID.String())
		page, err := client.Snapshot(ctx, testTenant, catalogproto.SnapshotRequest{
			Protocol: catalogproto.Version, Generation: 7, Revision: head.Revision,
			Scope: catalogproto.EnumerationScope{Kind: catalogproto.EnumerationScopeKindContainer, ParentID: &parent},
			After: after, Limit: 257,
		})
		if err != nil {
			t.Fatalf("Snapshot(): %v", err)
		}
		seen += len(page.Objects)
		if page.Next == nil {
			break
		}
		after = page.Next
	}
	if seen != 10_000 {
		t.Fatalf("snapshot objects = %d, want 10000", seen)
	}
	reader.mu.Lock()
	if reader.openCalls != 0 {
		t.Fatalf("snapshot opened %d content bodies", reader.openCalls)
	}
	reader.mu.Unlock()

	changes, err := client.ChangesSince(ctx, testTenant, catalogproto.ChangesSinceRequest{
		Protocol: catalogproto.Version, Generation: 7,
		Cursor: catalogproto.ChangeCursor{Revision: 1, Sequence: catalogproto.ChangeCursorCompleteSequence},
		Scope:  catalogproto.EnumerationScope{Kind: catalogproto.EnumerationScopeKindContainer, ParentID: ptrProtocolObjectID(reader.objects[0].ID)},
		Limit:  10,
	})
	if err != nil || len(changes.Changes) != 1 {
		t.Fatalf("ChangesSince() = %#v, %v", changes, err)
	}
	reader.mu.Lock()
	if reader.snapshotCalls == 0 || reader.openCalls != 0 || reader.changeCalls != 1 {
		t.Fatalf("query calls snapshot=%d changes=%d open=%d", reader.snapshotCalls, reader.changeCalls, reader.openCalls)
	}
	reader.mu.Unlock()

	open, err := client.OpenAt(ctx, testTenant, catalogproto.OpenAtRequest{
		Protocol: catalogproto.Version, Generation: 7,
		ObjectID: catalogproto.ObjectID(reader.objects[42].ID.String()), Revision: 2,
	})
	if err != nil {
		t.Fatalf("OpenAt(): %v", err)
	}
	content, err := io.ReadAll(open)
	if err != nil {
		t.Fatalf("read OpenAt: %v", err)
	}
	if got, want := string(content), "content-42"; got != want {
		t.Fatalf("OpenAt content = %q, want %q", got, want)
	}
	openResponse, err := open.Response()
	if err != nil || openResponse.Object == nil || openResponse.Object.Revision != 2 {
		t.Fatalf("OpenAt response = %#v, %v", openResponse, err)
	}

	mode := uint32(0o644)
	name := "created"
	kind := catalogproto.ObjectKindFile
	contentRevision := uint64(1)
	parent := catalogproto.ObjectID(reader.objects[0].ID.String())
	operation := catalogproto.MutationID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	source := &singlePassReader{data: []byte("one-pass")}
	mutation, err := client.Mutate(ctx, testTenant, catalogproto.MutationRequest{
		Protocol: catalogproto.Version, OperationID: operation, Generation: 7, ExpectedRevision: 2,
		Kind:       catalogproto.MutationKindCreate,
		ObjectKind: &kind, HasContent: true, ParentID: &parent, Name: &name, Mode: &mode,
		ContentRevision: &contentRevision,
	}, source)
	if err != nil || mutation.OperationID == nil || *mutation.OperationID != operation {
		t.Fatalf("Mutate() = %#v, %v", mutation, err)
	}
	mutations.mu.Lock()
	if mutations.stageCalls != 1 || mutations.submitCalls != 1 || string(mutations.staged) != "one-pass" {
		t.Fatalf("mutation calls stage=%d submit=%d bytes=%q", mutations.stageCalls, mutations.submitCalls, mutations.staged)
	}
	mutations.mu.Unlock()
	if source.readAfterEOF != 0 {
		t.Fatalf("mutation source read after EOF %d times", source.readAfterEOF)
	}

	if server == nil {
		t.Fatal("catalog server was not registered")
	}
}

func TestSourceReconcileStreamsExactRecordsUnderAuthenticatedAuthority(t *testing.T) {
	reader := newFakeReader(1)
	sources := &recordingSources{}
	_, path := startCatalogServerWithServices(t, reader, &fakeMutations{}, sources, fakeBroker{})
	client := newCatalogClient(t, path)
	content := []byte("authoritative settings")
	hash := sha256.Sum256(content)
	request := catalogproto.SourceReconcileRequest{
		Protocol: catalogproto.Version, Mode: catalogproto.SourceModeDelta, SourceAuthority: "source-main",
		SourceRevision: 5, PredecessorRevision: 4,
		ChangeID: "11111111111111111111111111111111", OperationID: "22222222222222222222222222222222",
		Cause: catalogproto.ConvergenceCauseDaemonWrite, AffectedKeys: []string{"settings.json"}, TenantCount: 1,
	}
	response, err := client.ReconcileSource(t.Context(), request, []SourceTenantInput{{
		Record: catalogproto.SourceTenantRecord{TenantID: testTenant, Generation: 7, RootKey: "root:acct-18", ObjectCount: 2, DeleteCount: 1},
		Objects: []SourceObjectInput{
			{Record: catalogproto.SourceObjectRecord{
				SourceKey: "config", Name: "config", Kind: catalogproto.ObjectKindDirectory,
				Mode: 0o700, MountVisible: true, FileProviderVisible: true,
			}},
			{Record: catalogproto.SourceObjectRecord{
				SourceKey: "settings", ParentKey: "config", Name: "settings.json", Kind: catalogproto.ObjectKindFile,
				Mode: 0o600, ContentRevision: 5, Size: uint64(len(content)), Hash: hex.EncodeToString(hash[:]),
				MountVisible: true, FileProviderVisible: true,
			}, Content: bytes.NewReader(content)},
		},
		Deletes: []catalogproto.SourceDeleteRecord{{SourceKey: "obsolete"}},
	}})
	if err != nil {
		t.Fatalf("ReconcileSource: %v", err)
	}
	if response.SourceAuthority != request.SourceAuthority || response.SourceRevision != request.SourceRevision ||
		response.ChangeID != request.ChangeID || response.OperationID != request.OperationID || len(response.Commits) != 1 {
		t.Fatalf("source response = %+v", response)
	}
	sources.mu.Lock()
	defer sources.mu.Unlock()
	if sources.applyCalls != 1 || len(sources.staged) != 2 || string(sources.staged[1]) != string(content) || sources.discardCalls != 0 {
		t.Fatalf("source calls apply=%d staged=%q discard=%d", sources.applyCalls, sources.staged, sources.discardCalls)
	}
	if len(sources.submission.Tenants) != 1 || len(sources.submission.Tenants[0].Objects) != 2 || len(sources.submission.Tenants[0].Deletes) != 1 {
		t.Fatalf("source submission = %+v", sources.submission)
	}
}

func TestRoleAwarePeerAuthorizationAllowsSourceAndRejectsProtectedTraffic(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &fakeMutations{}
	sources := &recordingSources{}
	protectedErr := errors.New("designated requirement mismatch")
	_, path := startCatalogServerWithServicesAndProtectedPeer(
		t, reader, mutations, sources, fakeBroker{}, func(wire.Peer) error { return protectedErr },
	)
	client := newCatalogClient(t, path)
	request := catalogproto.SourceReconcileRequest{
		Protocol: catalogproto.Version, Mode: catalogproto.SourceModeDelta, SourceAuthority: "source-main",
		SourceRevision: 5, PredecessorRevision: 4,
		ChangeID: "11111111111111111111111111111111", OperationID: "22222222222222222222222222222222",
		Cause: catalogproto.ConvergenceCauseDaemonWrite, AffectedKeys: []string{"settings.json"}, TenantCount: 1,
	}
	if _, err := client.ReconcileSource(t.Context(), request, []SourceTenantInput{{
		Record: catalogproto.SourceTenantRecord{TenantID: testTenant, Generation: 7, RootKey: "root:acct-18"},
	}}); err != nil {
		t.Fatalf("unsigned source publisher: %v", err)
	}
	if _, err := client.Mutate(t.Context(), testTenant, testMutationRequest(9), bytes.NewReader([]byte("blocked"))); err == nil {
		t.Fatal("protected mutation succeeded with a mismatched signed identity")
	}
	mutations.mu.Lock()
	if mutations.stageCalls != 0 || mutations.submitCalls != 0 {
		t.Fatalf("rejected mutation reached service: stage=%d submit=%d", mutations.stageCalls, mutations.submitCalls)
	}
	mutations.mu.Unlock()
	payload, err := catalogproto.Encode(catalogproto.BrokerOpenRequest{Protocol: catalogproto.Version})
	if err != nil {
		t.Fatalf("Encode(BrokerOpenRequest): %v", err)
	}
	call, err := client.wire.Open(t.Context(), wire.Op(catalogproto.OperationBrokerOpen), "", payload, false)
	if err != nil {
		t.Fatalf("open rejected broker: %v", err)
	}
	if err := drainChunks(t.Context(), call); err != nil {
		t.Fatalf("drain rejected broker: %v", err)
	}
	result, err := call.Response(t.Context())
	if err != nil {
		t.Fatalf("rejected broker response: %v", err)
	}
	var response catalogproto.BrokerOpenResponse
	if err := decodeWireResult(result, &response); err != nil {
		t.Fatalf("decode rejected broker: %v", err)
	}
	if response.Code != catalogproto.ErrorCodeUnavailable {
		t.Fatalf("rejected broker code = %q, want unavailable", response.Code)
	}
}

func TestAuthorizationRolesCannotCrossOperationBoundaries(t *testing.T) {
	route := Route{Tenant: "acct-18", Generation: 7}
	tests := []struct {
		name          string
		authorization Authorization
		operation     catalogproto.Operation
	}{
		{"source mutation", Authorization{Principal: "source", Role: RoleSourcePublisher, SourceAuthority: "main", Route: route}, catalogproto.OperationCatalogMutate},
		{"tenant owner source", Authorization{Principal: "owner", Role: RoleTenantOwner, Route: route}, catalogproto.OperationSourceReconcile},
		{"mount prepare", Authorization{Principal: "mount", Role: RoleMount, Presentation: catalog.PresentationMount, Route: route}, catalogproto.OperationTenantPrepare},
		{"file provider source", Authorization{Principal: "broker", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: route}, catalogproto.OperationSourceReconcile},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateAuthorization(test.authorization, test.operation); err == nil {
				t.Fatal("cross-role authorization succeeded")
			}
		})
	}
}

func TestMutationSettlementHonorsFinalSourceEOF(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &fakeMutations{}
	_, path := startCatalogServer(t, reader, mutations)
	client := newCatalogClient(t, path)
	for index, body := range [][]byte{nil, []byte("one-pass")} {
		request := testMutationRequest(byte(index + 1))
		response, err := client.Mutate(context.Background(), testTenant, request, bytes.NewReader(body))
		if err != nil || response.OperationID == nil || *response.OperationID != request.OperationID {
			t.Fatalf("Mutate(%q) = %#v, %v", body, response, err)
		}
	}
}

func TestMutationTerminalBeforeSourceEOFRemainsAnError(t *testing.T) {
	reader := newFakeReader(1)
	_, path := startCatalogServer(t, reader, rejectingMutations{})
	client := newCatalogClient(t, path)
	response, err := client.Mutate(context.Background(), testTenant, testMutationRequest(3), bytes.NewReader(bytes.Repeat([]byte("x"), 1<<20)))
	if err == nil || response.Code == catalogproto.ErrorCodeOk {
		t.Fatalf("early mutation terminal = %#v, %v", response, err)
	}
}

func TestMutationFinalChunkTerminalRaceAndDecodeFailure(t *testing.T) {
	path := startRawMutationServer(t, func(ctx context.Context, request wire.Request) (any, error) {
		var input catalogproto.MutationRequest
		if err := catalogproto.Decode(request.Payload, &input); err != nil {
			return nil, err
		}
		for {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case _, ok := <-request.Chunks:
				if !ok {
					primary := catalogproto.ObjectID("05050505050505050505050505050505")
					payload, err := catalogproto.Encode(catalogproto.MutationResponse{
						Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk,
						OperationID: &input.OperationID, Revision: 2, PrimaryID: &primary,
					})
					return json.RawMessage(payload), err
				}
			}
		}
	})
	client := newCatalogClient(t, path)
	for iteration := 0; iteration < 200; iteration++ {
		request := testMutationRequest(4)
		response, err := client.Mutate(context.Background(), testTenant, request, &singlePassReader{data: []byte("final")})
		if err != nil || response.OperationID == nil || *response.OperationID != request.OperationID {
			t.Fatalf("iteration %d: Mutate = %#v, %v", iteration, response, err)
		}
	}

	decodePath := startRawMutationServer(t, func(_ context.Context, request wire.Request) (any, error) {
		for range request.Chunks {
		}
		return json.RawMessage("{"), nil
	})
	decodeClient := newCatalogClient(t, decodePath)
	if _, err := decodeClient.Mutate(context.Background(), testTenant, testMutationRequest(6), bytes.NewReader(nil)); err == nil {
		t.Fatal("invalid mutation terminal decoded successfully")
	}
}

func TestBrokerForwardCarriesAuthoritativeBoundRoute(t *testing.T) {
	reader := newFakeReader(1)
	_, path := startCatalogServer(t, reader, &fakeMutations{})
	transport, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), Build: transportproto.Build,
	})
	if err != nil {
		t.Fatalf("wire.NewClient: %v", err)
	}
	defer func() {
		if err := transport.Close(); err != nil {
			t.Errorf("Close transport: %v", err)
		}
	}()
	domain, err := catalogproto.DeriveDomainID("test-owner", "test-account")
	if err != nil {
		t.Fatalf("DeriveDomainID: %v", err)
	}
	call := func(boundGeneration, innerGeneration uint64) catalogproto.HeadResponse {
		t.Helper()
		payload, err := catalogproto.Encode(catalogproto.HeadRequest{Protocol: catalogproto.Version, Generation: innerGeneration})
		if err != nil {
			t.Fatalf("Encode(inner): %v", err)
		}
		envelope, err := catalogproto.Encode(catalogproto.BrokerForwardRequest{
			Protocol: catalogproto.Version,
			Context: catalogproto.BrokerForwardContext{
				DomainID: domain, TenantID: testTenant, Generation: boundGeneration,
			},
			Operation: catalogproto.OperationCatalogHead, Payload: payload,
		})
		if err != nil {
			t.Fatalf("Encode(envelope): %v", err)
		}
		result, err := transport.Call(context.Background(), wire.Op(catalogproto.OperationBrokerForward), "", envelope)
		if err != nil {
			t.Fatalf("broker.forward: %v", err)
		}
		var response catalogproto.HeadResponse
		if err := catalogproto.Decode(result.Response.Payload, &response); err != nil {
			t.Fatalf("Decode(response): %v", err)
		}
		return response
	}
	if response := call(7, 7); response.Code != catalogproto.ErrorCodeOk || response.Revision != 2 {
		t.Fatalf("matched forward response = %#v", response)
	}
	if response := call(7, 8); response.Code == catalogproto.ErrorCodeOk {
		t.Fatalf("inner generation mismatch was accepted: %#v", response)
	}
	if response := call(6, 6); response.Code == catalogproto.ErrorCodeOk {
		t.Fatalf("stale broker binding was accepted: %#v", response)
	}
}

func TestOldApplicationAndLFProtocolsCannotReachMutation(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &fakeMutations{}
	_, path := startCatalogServer(t, reader, mutations)

	transport, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), Build: transportproto.Build,
	})
	if err != nil {
		t.Fatalf("wire.NewClient: %v", err)
	}
	defer func() {
		if err := transport.Close(); err != nil {
			t.Errorf("Close transport: %v", err)
		}
	}()
	result, err := transport.Call(context.Background(), wire.Op(catalogproto.OperationCatalogMutate), string(testTenant), []byte(`{"protocol":0}`))
	if err != nil {
		t.Fatalf("old application call: %v", err)
	}
	var response catalogproto.MutationResponse
	if err := catalogproto.Decode(result.Response.Payload, &response); err != nil {
		t.Fatalf("decode rejection: %v", err)
	}
	if response.Code != catalogproto.ErrorCodeInvalidRequest {
		t.Fatalf("old application response code = %q", response.Code)
	}

	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial LF client: %v", err)
	}
	defer func() {
		if err := connection.Close(); err != nil {
			t.Errorf("Close connection: %v", err)
		}
	}()
	if _, err := connection.Write([]byte("{\"op\":\"catalog.mutate\"}\n")); err != nil {
		t.Fatalf("write LF request: %v", err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set LF deadline: %v", err)
	}
	buffer := make([]byte, 1)
	if _, err := connection.Read(buffer); err == nil {
		t.Fatal("legacy LF connection remained readable")
	}
	mutations.mu.Lock()
	defer mutations.mu.Unlock()
	if mutations.stageCalls != 0 || mutations.submitCalls != 0 {
		t.Fatalf("rejected protocols reached mutation: stage=%d submit=%d", mutations.stageCalls, mutations.submitCalls)
	}
}

func TestMutationStageIdentityMismatchCannotSubmit(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &fakeMutations{wrongGeneration: true}
	_, path := startCatalogServer(t, reader, mutations)
	client := newCatalogClient(t, path)
	mode := uint32(0o644)
	name := "created"
	kind := catalogproto.ObjectKindFile
	contentRevision := uint64(1)
	parent := catalogproto.ObjectID(reader.objects[0].ID.String())
	_, err := client.Mutate(context.Background(), testTenant, catalogproto.MutationRequest{
		Protocol: catalogproto.Version, OperationID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Generation: 7, ExpectedRevision: 2, Kind: catalogproto.MutationKindCreate,
		ObjectKind: &kind, HasContent: true, ParentID: &parent, Name: &name, Mode: &mode,
		ContentRevision: &contentRevision,
	}, bytes.NewBufferString("bytes"))
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != catalogproto.ErrorCodeIntegrity {
		t.Fatalf("Mutate() error = %v, want integrity RemoteError", err)
	}
	mutations.mu.Lock()
	defer mutations.mu.Unlock()
	if mutations.stageCalls != 1 || mutations.submitCalls != 0 {
		t.Fatalf("identity mismatch calls stage=%d submit=%d", mutations.stageCalls, mutations.submitCalls)
	}
}

func TestOpenReaderCloseUnblocksBlockedRead(t *testing.T) {
	reader := newFakeReader(1)
	started := make(chan struct{})
	reader.openOverride = func(ctx context.Context, object catalog.Object, _ int) (OpenResult, error) {
		return OpenResult{Object: object, Content: &contextBlockingContent{ctx: ctx, started: started}}, nil
	}
	_, path := startCatalogServer(t, reader, &fakeMutations{})
	client := newCatalogClient(t, path)
	object := reader.objects[0]
	stream, err := client.OpenAt(context.Background(), testTenant, catalogproto.OpenAtRequest{
		Protocol: catalogproto.Version, Generation: 7,
		ObjectID: catalogproto.ObjectID(object.ID.String()), Revision: uint64(object.Revision),
	})
	if err != nil {
		t.Fatalf("OpenAt: %v", err)
	}
	readDone := make(chan error, 1)
	go func() {
		buffer := make([]byte, 1)
		_, err := stream.Read(buffer)
		readDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("server content read did not block")
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-readDone:
		if err == nil {
			t.Fatal("blocked Read returned nil error after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Read")
	}
	if _, err := stream.Response(); err == nil {
		t.Fatal("Response after canceled open returned nil error")
	}
}

func TestBrokerReplacementSettlesPriorStream(t *testing.T) {
	broker := &recordingBroker{opened: make(chan *recordingBrokerSession, 2)}
	_, path := startCatalogServerWithBroker(t, newFakeReader(1), &fakeMutations{}, broker)
	client := newCatalogClient(t, path)
	payload, err := catalogproto.Encode(catalogproto.BrokerOpenRequest{Protocol: catalogproto.Version})
	if err != nil {
		t.Fatalf("Encode(BrokerOpenRequest): %v", err)
	}
	first, err := client.wire.Open(context.Background(), wire.Op(catalogproto.OperationBrokerOpen), "", payload, false)
	if err != nil {
		t.Fatalf("open first broker: %v", err)
	}
	firstSession := <-broker.opened
	second, err := client.wire.Open(context.Background(), wire.Op(catalogproto.OperationBrokerOpen), "", payload, false)
	if err != nil {
		t.Fatalf("open replacement broker: %v", err)
	}
	select {
	case <-firstSession.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("prior broker did not settle before replacement")
	}
	select {
	case <-broker.opened:
	case <-time.After(2 * time.Second):
		t.Fatal("replacement broker did not open")
	}
	if err := drainChunks(context.Background(), first); err != nil {
		t.Fatalf("drain first broker: %v", err)
	}
	result, err := first.Response(context.Background())
	if err != nil {
		t.Fatalf("first broker response: %v", err)
	}
	var response catalogproto.BrokerOpenResponse
	if err := decodeWireResult(result, &response); err != nil {
		t.Fatalf("decode first broker response: %v", err)
	}
	if response.Code != catalogproto.ErrorCodeUnavailable {
		t.Fatalf("first broker response code = %q, want unavailable", response.Code)
	}
	second.Cancel()
}

type fakeReader struct {
	mu sync.Mutex

	objects       []catalog.Object
	snapshotCalls int
	changeCalls   int
	openCalls     int
	openOverride  func(context.Context, catalog.Object, int) (OpenResult, error)
}

func (r *fakeReader) Root(context.Context, Authorization, catalog.TenantID) (catalog.Object, error) {
	if len(r.objects) == 0 {
		return catalog.Object{}, catalog.ErrNotFound
	}
	return r.objects[0], nil
}

func newFakeReader(count int) *fakeReader {
	objects := make([]catalog.Object, count)
	for index := range count {
		id := objectID(index + 1)
		objects[index] = catalog.Object{
			Tenant: "acct-18", ID: id, Parent: objectID(1), Revision: 2, MetadataRevision: 2,
			ContentRevision: 1, Name: fmt.Sprintf("item-%05d", index), Kind: catalog.KindFile,
			Mode: 0o644, Size: int64(len(fmt.Sprintf("content-%d", index))), Visibility: catalog.Visibility{FileProvider: true},
		}
	}
	return &fakeReader{objects: objects}
}

func (r *fakeReader) Head(context.Context, Authorization, catalog.TenantID) (catalog.Revision, error) {
	return 2, nil
}

func (r *fakeReader) Snapshot(_ context.Context, _ Authorization, _ catalog.TenantID, _ catalog.EnumerationScope, revision catalog.Revision, cursor catalog.SnapshotCursor, limit int) (catalog.SnapshotPage, error) {
	r.mu.Lock()
	r.snapshotCalls++
	r.mu.Unlock()
	start := 0
	if cursor.After != nil {
		for index := range r.objects {
			if r.objects[index].ID == *cursor.After {
				start = index + 1
				break
			}
		}
	}
	end := min(start+limit, len(r.objects))
	page := catalog.SnapshotPage{Revision: revision, Objects: append([]catalog.Object(nil), r.objects[start:end]...)}
	if end < len(r.objects) {
		after := r.objects[end-1].ID
		page.Next = &catalog.SnapshotCursor{After: &after}
	}
	return page, nil
}

func (r *fakeReader) ChangesSince(context.Context, Authorization, catalog.TenantID, catalog.EnumerationScope, catalog.ChangeCursor, int) (catalog.ChangePage, error) {
	r.mu.Lock()
	r.changeCalls++
	r.mu.Unlock()
	return catalog.ChangePage{
		Floor: 0, Head: 2, Next: catalog.CompleteChangeCursor(2), Complete: true,
		Changes: []catalog.Change{{Revision: 2, Sequence: 0, Kind: catalog.ChangeUpsert, Object: r.objects[0]}},
	}, nil
}

func (r *fakeReader) Lookup(_ context.Context, _ Authorization, _ catalog.TenantID, id catalog.ObjectID) (catalog.Object, error) {
	for _, object := range r.objects {
		if object.ID == id {
			return object, nil
		}
	}
	return catalog.Object{}, catalog.ErrNotFound
}

func (r *fakeReader) LookupName(_ context.Context, _ Authorization, _ catalog.TenantID, parent catalog.ObjectID, name string) (catalog.Object, error) {
	for _, object := range r.objects {
		if object.Parent == parent && object.Name == name {
			return object, nil
		}
	}
	return catalog.Object{}, catalog.ErrNotFound
}

func (r *fakeReader) OpenAt(ctx context.Context, _ Authorization, _ catalog.TenantID, _ catalog.Generation, id catalog.ObjectID, revision catalog.Revision) (OpenResult, error) {
	r.mu.Lock()
	r.openCalls++
	r.mu.Unlock()
	for index, object := range r.objects {
		if object.ID == id && object.Revision == revision {
			if r.openOverride != nil {
				return r.openOverride(ctx, object, index)
			}
			return OpenResult{Object: object, Content: io.NopCloser(bytes.NewBufferString(fmt.Sprintf("content-%d", index)))}, nil
		}
	}
	return OpenResult{}, catalog.ErrNotFound
}

type fakeMutations struct {
	mu sync.Mutex

	stageCalls      int
	submitCalls     int
	staged          []byte
	wrongGeneration bool
}

type rejectingMutations struct{}

func (rejectingMutations) StageMutation(context.Context, Identity, Authorization, catalog.TenantID, catalog.MutationID, catalog.Generation, bool, io.Reader) (MutationStage, error) {
	return MutationStage{}, catalog.ErrConflict
}

func (rejectingMutations) SubmitMutation(context.Context, Identity, Authorization, MutationSubmission) (MutationResult, error) {
	return MutationResult{}, errors.New("unexpected mutation submission")
}

func (m *fakeMutations) StageMutation(_ context.Context, _ Identity, _ Authorization, tenant catalog.TenantID, operation catalog.MutationID, generation catalog.Generation, _ bool, source io.Reader) (MutationStage, error) {
	content, err := io.ReadAll(source)
	if err != nil {
		return MutationStage{}, err
	}
	m.mu.Lock()
	m.stageCalls++
	m.staged = append([]byte(nil), content...)
	m.mu.Unlock()
	if m.wrongGeneration {
		generation++
	}
	return MutationStage{Token: "stage", OperationID: operation, Tenant: tenant, Generation: generation, Size: int64(len(content))}, nil
}

func (m *fakeMutations) SubmitMutation(_ context.Context, _ Identity, _ Authorization, submission MutationSubmission) (MutationResult, error) {
	m.mu.Lock()
	m.submitCalls++
	m.mu.Unlock()
	operation, err := catalog.ParseMutationID(string(submission.Request.OperationID))
	if err != nil {
		return MutationResult{}, err
	}
	primary := objectID(10_001)
	return MutationResult{OperationID: operation, Revision: 3, PrimaryID: &primary}, nil
}

type fakePreparation struct{}

func (fakePreparation) PrepareTenant(_ context.Context, _ Identity, tenant catalog.TenantID, request catalogproto.PrepareTenantRequest) (catalogproto.PreparationProof, error) {
	return preparationProof(tenant, request), nil
}

type fakeConvergence struct{}

func (fakeConvergence) AckConvergence(_ context.Context, _ Identity, tenant catalog.TenantID, request catalogproto.AckConvergenceRequest) (catalogproto.DomainObservation, error) {
	return catalogproto.DomainObservation{
		TenantID: catalogproto.TenantID(tenant), DomainID: request.DomainID, Generation: request.Generation,
		RequestedRevision: request.Revision, ObservedRevision: request.Revision,
		CatalogRevision: request.CatalogRevision, SourceAuthority: request.SourceAuthority, SourceRevision: request.SourceRevision,
		ChangeID: request.ChangeID, OperationID: request.OperationID,
	}, nil
}

type fakeSources struct{}

func (fakeSources) StageSourceObject(context.Context, Identity, Authorization, catalogproto.SourceReconcileRequest, catalogproto.SourceTenantRecord, catalogproto.SourceObjectRecord, io.Reader) (catalog.SourceObject, error) {
	return catalog.SourceObject{}, errors.New("unexpected source staging")
}

func (fakeSources) DiscardSource(context.Context, Identity, Authorization, []catalog.SourceTenant) error {
	return nil
}

func (fakeSources) ApplySource(context.Context, Identity, Authorization, SourceSubmission) (catalog.SourceResult, error) {
	return catalog.SourceResult{}, errors.New("unexpected source publication")
}

type recordingSources struct {
	mu           sync.Mutex
	staged       [][]byte
	discardCalls int
	applyCalls   int
	submission   SourceSubmission
}

func (s *recordingSources) StageSourceObject(_ context.Context, _ Identity, _ Authorization, _ catalogproto.SourceReconcileRequest, _ catalogproto.SourceTenantRecord, record catalogproto.SourceObjectRecord, reader io.Reader) (catalog.SourceObject, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return catalog.SourceObject{}, err
	}
	s.mu.Lock()
	s.staged = append(s.staged, content)
	s.mu.Unlock()
	object := catalog.SourceObject{
		Key: catalog.SourceObjectKey(record.SourceKey), Parent: catalog.SourceObjectKey(record.ParentKey), Name: record.Name,
		Mode: record.Mode, ContentRevision: catalog.Revision(record.ContentRevision),
		Visibility: catalog.Visibility{Mount: record.MountVisible, FileProvider: record.FileProviderVisible},
	}
	kind := catalog.KindDirectory
	if record.Kind == catalogproto.ObjectKindFile {
		kind = catalog.KindFile
		decoded, err := hex.DecodeString(record.Hash)
		if err != nil {
			return catalog.SourceObject{}, err
		}
		copy(object.Content.Hash[:], decoded)
		object.Content.Size = int64(record.Size)
		object.Content.Stage[0] = 1
	}
	object.Kind = kind
	return object, nil
}

func (s *recordingSources) DiscardSource(context.Context, Identity, Authorization, []catalog.SourceTenant) error {
	s.mu.Lock()
	s.discardCalls++
	s.mu.Unlock()
	return nil
}

func (s *recordingSources) ApplySource(_ context.Context, _ Identity, authorization Authorization, submission SourceSubmission) (catalog.SourceResult, error) {
	s.mu.Lock()
	s.applyCalls++
	s.submission = submission
	s.mu.Unlock()
	changeID, err := convergenceChangeID(submission.Request.ChangeID)
	if err != nil {
		return catalog.SourceResult{}, err
	}
	operationID, err := convergenceOperationID(submission.Request.OperationID)
	if err != nil {
		return catalog.SourceResult{}, err
	}
	return catalog.SourceResult{
		Authority: authorization.SourceAuthority, Revision: causal.Revision(submission.Request.SourceRevision),
		ChangeID: causal.ChangeID(changeID), Operation: causal.OperationID(operationID),
		Commits: []causal.CatalogCommit{{Tenant: causal.TenantID(testTenant), CatalogRevision: 12}},
	}, nil
}

type fakeAuthorizer struct{}

func (fakeAuthorizer) Authorize(_ context.Context, identity Identity, operation catalogproto.Operation, route Route) (Authorization, error) {
	if identity.Session == nil || identity.Build != transportproto.Build || identity.Peer.PID == 0 {
		return Authorization{}, errors.New("bad identity")
	}
	if operation == catalogproto.OperationBrokerOpen {
		return Authorization{Principal: "test-app", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: route}, nil
	}
	if operation == catalogproto.OperationSourceReconcile && route == (Route{}) {
		return Authorization{Principal: "test-source", Role: RoleSourcePublisher, SourceAuthority: "source-main"}, nil
	}
	if route.Generation != 7 {
		return Authorization{}, catalog.ErrGenerationMismatch
	}
	if route.Forwarded {
		return Authorization{Principal: "test-app", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: route}, nil
	}
	if operation == catalogproto.OperationTenantPrepare {
		return Authorization{Principal: "test-owner", Role: RoleTenantOwner, Route: route}, nil
	}
	return Authorization{Principal: "test-app", Role: RoleMount, Presentation: catalog.PresentationMount, Route: route}, nil
}

type fakeBroker struct{}

func (fakeBroker) OpenBroker(context.Context, Identity, string) (BrokerSession, error) {
	commands := make(chan catalogproto.BrokerCommand)
	close(commands)
	return &fakeBrokerSession{commands: commands}, nil
}

type fakeBrokerSession struct {
	commands <-chan catalogproto.BrokerCommand
}

func (s *fakeBrokerSession) Commands() <-chan catalogproto.BrokerCommand { return s.commands }
func (s *fakeBrokerSession) AcceptResult(context.Context, catalogproto.BrokerResult) error {
	return nil
}
func (s *fakeBrokerSession) Close(error) {}

type recordingBroker struct {
	opened chan *recordingBrokerSession
}

func (b *recordingBroker) OpenBroker(_ context.Context, _ Identity, _ string) (BrokerSession, error) {
	session := &recordingBrokerSession{commands: make(chan catalogproto.BrokerCommand), closed: make(chan struct{})}
	b.opened <- session
	return session, nil
}

type recordingBrokerSession struct {
	commands chan catalogproto.BrokerCommand
	closed   chan struct{}
}

func (s *recordingBrokerSession) Commands() <-chan catalogproto.BrokerCommand { return s.commands }
func (s *recordingBrokerSession) AcceptResult(context.Context, catalogproto.BrokerResult) error {
	return nil
}
func (s *recordingBrokerSession) Close(error) { close(s.closed) }

type singlePassReader struct {
	data         []byte
	done         bool
	readAfterEOF int
}

type contextBlockingContent struct {
	ctx     context.Context
	started chan struct{}
	once    sync.Once
}

func (r *contextBlockingContent) Read([]byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	<-r.ctx.Done()
	return 0, r.ctx.Err()
}

func (*contextBlockingContent) Close() error { return nil }

func (r *singlePassReader) Read(buffer []byte) (int, error) {
	if r.done {
		r.readAfterEOF++
		return 0, io.EOF
	}
	r.done = true
	return copy(buffer, r.data), io.EOF
}

func startCatalogServer(t *testing.T, reader Reader, mutations MutationService) (*Server, string) {
	return startCatalogServerWithBroker(t, reader, mutations, fakeBroker{})
}

func startCatalogServerWithBroker(t *testing.T, reader Reader, mutations MutationService, broker BrokerService) (*Server, string) {
	return startCatalogServerWithServices(t, reader, mutations, fakeSources{}, broker)
}

func startCatalogServerWithServices(t *testing.T, reader Reader, mutations MutationService, sources SourcePublicationService, broker BrokerService) (*Server, string) {
	return startCatalogServerWithServicesAndProtectedPeer(t, reader, mutations, sources, broker, func(wire.Peer) error { return nil })
}

func startCatalogServerWithServicesAndProtectedPeer(
	t *testing.T,
	reader Reader,
	mutations MutationService,
	sources SourcePublicationService,
	broker BrokerService,
	protectedPeer func(wire.Peer) error,
) (*Server, string) {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "fusekit-catalog-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "socket")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	wireServer := &wire.Server{Build: transportproto.Build, MaxFrame: 4 << 20}
	service, err := Register(wireServer, Config{
		Reader: reader, Mutations: mutations, Sources: sources, Preparation: fakePreparation{}, Convergence: fakeConvergence{},
		Broker: broker, Authorizer: fakeAuthorizer{}, ProtectedPeer: protectedPeer,
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		admit := func() (func(), error) { return func() {}, nil }
		done <- wireServer.Serve(ctx, listener, admit, admit)
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	return service, path
}

func startRawMutationServer(t *testing.T, handler wire.Handler) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "fusekit-catalog-raw-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "socket")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	server := &wire.Server{Build: transportproto.Build, MaxFrame: 4 << 20}
	server.RegisterConcurrent(wire.Op(catalogproto.OperationCatalogMutate), handler)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		admit := func() (func(), error) { return func() {}, nil }
		done <- server.Serve(ctx, listener, admit, admit)
	}()
	t.Cleanup(func() {
		cancel()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})
	return path
}

func testMutationRequest(marker byte) catalogproto.MutationRequest {
	mode := uint32(0o600)
	name := fmt.Sprintf("file-%d", marker)
	kind := catalogproto.ObjectKindFile
	contentRevision := uint64(1)
	parent := catalogproto.ObjectID("01010101010101010101010101010101")
	operation := catalogproto.MutationID(fmt.Sprintf("%032x", marker))
	return catalogproto.MutationRequest{
		Protocol: catalogproto.Version, OperationID: operation, Generation: 7, ExpectedRevision: 1,
		Kind: catalogproto.MutationKindCreate, ObjectKind: &kind, HasContent: true,
		ParentID: &parent, Name: &name, Mode: &mode, ContentRevision: &contentRevision,
	}
}

func newCatalogClient(t *testing.T, path string) *Client {
	t.Helper()
	client, err := NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), StreamQueue: 32, MaxFrame: 4 << 20,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func objectID(value int) catalog.ObjectID {
	id, err := catalog.ParseObjectID(fmt.Sprintf("%032x", value))
	if err != nil {
		panic(err)
	}
	return id
}

func preparationProof(tenant catalog.TenantID, request catalogproto.PrepareTenantRequest) catalogproto.PreparationProof {
	return catalogproto.PreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(tenant), Generation: request.Generation, Requested: request.CatalogRevision,
			Desired: request.CatalogRevision, Observed: request.CatalogRevision,
			Verified: request.CatalogRevision, Applied: request.CatalogRevision,
		},
		Domain: catalogproto.DomainObservation{
			TenantID: catalogproto.TenantID(tenant), DomainID: request.DomainID, Generation: request.Generation,
			RequestedRevision: 1, ObservedRevision: 1,
			CatalogRevision: request.CatalogRevision, SourceAuthority: request.SourceAuthority, SourceRevision: request.SourceRevision,
			ChangeID: request.ChangeID, OperationID: request.OperationID,
		},
	}
}

func ptrProtocolObjectID(id catalog.ObjectID) *catalogproto.ObjectID {
	value := catalogproto.ObjectID(id.String())
	return &value
}
