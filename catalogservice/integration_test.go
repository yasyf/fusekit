package catalogservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/daemon"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
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
	requestID := catalogproto.MutationRequestID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	source := &singlePassReader{data: []byte("one-pass")}
	mutation, err := client.Mutate(ctx, testTenant, catalogproto.MutationRequest{
		Protocol: catalogproto.Version, RequestID: requestID, Generation: 7, ExpectedRevision: 2,
		Kind:       catalogproto.MutationKindCreate,
		ObjectKind: &kind, HasContent: true, ParentID: &parent, Name: &name, Mode: &mode,
		ContentRevision: &contentRevision,
	}, source)
	if err != nil || mutation.RequestID == nil || *mutation.RequestID != requestID ||
		mutation.MutationID == nil {
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

func TestMutationServerSettlesSourceWhenServiceRejectsWithoutOwnershipCleanup(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &unsettledRejectingMutations{}
	_, path := startCatalogServer(t, reader, mutations)
	client := newCatalogClient(t, path)
	if _, err := client.Mutate(
		t.Context(), testTenant, testMutationRequest(7), bytes.NewReader([]byte("rejected")),
	); err == nil {
		t.Fatal("Mutate() unexpectedly succeeded")
	}
	mutations.mu.Lock()
	source := mutations.source
	mutations.mu.Unlock()
	if source == nil {
		t.Fatal("mutation source was not transferred to the service")
	}
	waitCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := source.Wait(waitCtx); err != nil {
		t.Fatalf("rejected source did not settle: %v", err)
	}
}

func TestPrepareTenantWireCarriesPresentationActivationAndReturnsSourceProof(t *testing.T) {
	reader := newFakeReader(1)
	_, path := startCatalogServer(t, reader, &fakeMutations{})
	client := newCatalogClient(t, path)
	response, err := client.PrepareTenant(t.Context(), testTenant, catalogproto.PrepareTenantRequest{
		Protocol: catalogproto.Version, Generation: 7,
		Presentation: catalogproto.PresentationKindFileProvider, ActivationGeneration: "activation-7",
	})
	if err != nil {
		t.Fatalf("PrepareTenant: %v", err)
	}
	if response.Proof == nil || response.Proof.Catalog.Requested != 12 ||
		response.Proof.CatalogRevision != 12 || response.Proof.SourceRevision != 8 {
		t.Fatalf("preparation proof = %+v", response.Proof)
	}
}

func TestRoleAwarePeerAuthorizationRejectsProtectedTraffic(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &fakeMutations{}
	broker := &countingBroker{}
	protectedErr := errors.New("designated requirement mismatch")
	_, path := startCatalogServerWithProtectedPeer(
		t, reader, mutations, broker, func(context.Context, wire.Peer) error { return protectedErr },
	)
	client := newCatalogClient(t, path)
	domain, err := catalogproto.DeriveDomainID("test-owner", "test-account")
	if err != nil {
		t.Fatalf("DeriveDomainID: %v", err)
	}
	head, err := catalogproto.Encode(catalogproto.HeadRequest{Protocol: catalogproto.Version, Generation: 7})
	if err != nil {
		t.Fatalf("Encode(head): %v", err)
	}
	forward, err := catalogproto.Encode(catalogproto.BrokerForwardRequest{
		Protocol: catalogproto.Version,
		Context: catalogproto.BrokerForwardContext{
			DomainID: domain, TenantID: testTenant, Generation: 7,
		},
		Operation: catalogproto.OperationCatalogHead, Payload: head,
	})
	if err != nil {
		t.Fatalf("Encode(forward): %v", err)
	}
	forwardResult, err := client.wire.Call(t.Context(), wire.Op(catalogproto.OperationBrokerForward), "", forward)
	if err != nil {
		t.Fatalf("broker.forward: %v", err)
	}
	var forwarded catalogproto.HeadResponse
	if err := catalogproto.Decode(forwardResult.Response.Payload, &forwarded); err != nil {
		t.Fatalf("Decode(forwarded response): %v", err)
	}
	if forwarded.Code == catalogproto.ErrorCodeOk {
		t.Fatal("protected File Provider read succeeded with a mismatched signed identity")
	}
	reader.mu.Lock()
	if reader.headCalls != 0 {
		t.Fatalf("rejected File Provider read reached service %d times", reader.headCalls)
	}
	reader.mu.Unlock()
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
	broker.mu.Lock()
	opens := broker.opens
	broker.mu.Unlock()
	if opens != 0 {
		t.Fatalf("rejected broker reached protected service %d times", opens)
	}
}

func TestAuthorizationRolesCannotCrossOperationBoundaries(t *testing.T) {
	route := Route{Tenant: "acct-18", Generation: 7}
	tests := []struct {
		name          string
		authorization Authorization
		operation     catalogproto.Operation
	}{
		{"tenant owner mutation", Authorization{Principal: "owner", Role: RoleTenantOwner, Route: route}, catalogproto.OperationCatalogMutate},
		{"mount prepare", Authorization{Principal: "mount", Role: RoleMount, Presentation: catalog.PresentationMount, Route: route}, catalogproto.OperationTenantPrepare},
		{"file provider prepare", Authorization{Principal: "broker", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: route}, catalogproto.OperationTenantPrepare},
		{"tenant owner activation ack", Authorization{Principal: "owner", Role: RoleTenantOwner, Route: route}, catalogproto.OperationActivationAck},
		{"tenant owner source fleet publish", Authorization{Principal: "owner", Role: RoleTenantOwner}, catalogproto.OperationSourceAuthorityPublishDesiredFleet},
		{"product admin mutation", Authorization{Principal: "owner", Role: RoleProductAdmin}, catalogproto.OperationCatalogMutate},
		{"routed product admin", Authorization{Principal: "owner", Role: RoleProductAdmin, Route: route}, catalogproto.OperationSourceAuthorityPublishDesiredFleet},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateAuthorization(test.authorization, test.operation); err == nil {
				t.Fatal("cross-role authorization succeeded")
			}
		})
	}
}

func TestDesiredSourceFleetPublishRetriesAfterLostResponse(t *testing.T) {
	publisher := &lostResponseSourceFleetService{}
	_, path := startCatalogServerWithSourceFleets(t, newFakeReader(1), &fakeMutations{}, publisher)
	client := newCatalogClient(t, path)
	request := catalogproto.PublishDesiredSourceFleetRequest{
		Protocol: catalogproto.Version, Owner: "owner", Generation: 1,
		Declarations: []catalogproto.SourceAuthorityDeclaration{{
			Authority: "authority-a", DriverID: "driver.v1", DriverConfig: []byte("exact-config"),
			DeclarationDigest: strings.Repeat("a", 64),
		}, {
			Authority: "authority-b", DriverID: "driver.v1", DriverConfig: []byte("second-config"),
			DeclarationDigest: strings.Repeat("b", 64),
		}},
	}
	if _, err := client.PublishDesiredSourceFleet(t.Context(), request); err == nil {
		t.Fatal("first publication returned a response after simulated response loss")
	}
	response, err := client.PublishDesiredSourceFleet(t.Context(), request)
	if err != nil {
		t.Fatalf("retry publication: %v", err)
	}
	if response.State == nil || response.State.Generation != 1 || response.State.AuthorityCount != 2 {
		t.Fatalf("retry state = %+v", response.State)
	}
	publisher.mu.Lock()
	calls := publisher.calls
	publisher.mu.Unlock()
	if calls != 2 {
		t.Fatalf("publisher calls = %d, want exact retry", calls)
	}
	first, err := client.ReadDesiredSourceFleet(t.Context(), catalogproto.ReadDesiredSourceFleetRequest{
		Protocol: catalogproto.Version, Owner: "owner", Limit: 1,
	})
	if err != nil || first.State == nil || len(first.Declarations) != 1 || first.Next == nil {
		t.Fatalf("first desired fleet page = %+v, %v", first, err)
	}
	snapshot := first.State.DeclarationsDigest
	second, err := client.ReadDesiredSourceFleet(t.Context(), catalogproto.ReadDesiredSourceFleetRequest{
		Protocol: catalogproto.Version, Owner: "owner", Generation: first.State.Generation,
		SnapshotDigest: &snapshot, After: first.Next, Limit: 1,
	})
	if err != nil || len(second.Declarations) != 1 || second.Next != nil ||
		string(second.Declarations[0].DriverConfig) != "second-config" {
		t.Fatalf("second desired fleet page = %+v, %v", second, err)
	}
	drift := strings.Repeat("e", 64)
	if _, err := client.ReadDesiredSourceFleet(t.Context(), catalogproto.ReadDesiredSourceFleetRequest{
		Protocol: catalogproto.Version, Owner: "owner", Generation: first.State.Generation,
		SnapshotDigest: &drift, After: first.Next, Limit: 1,
	}); err == nil {
		t.Fatal("snapshot-drifted desired fleet continuation succeeded")
	}
	conflict := request
	conflict.Declarations = append([]catalogproto.SourceAuthorityDeclaration(nil), request.Declarations...)
	conflict.Declarations[0].DriverConfig = []byte("different-config")
	conflict.Declarations[0].DeclarationDigest = strings.Repeat("b", 64)
	if _, err := client.PublishDesiredSourceFleet(t.Context(), conflict); err == nil {
		t.Fatal("conflicting same-generation publication succeeded")
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
		if err != nil || response.RequestID == nil || *response.RequestID != request.RequestID ||
			response.MutationID == nil {
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

func TestBrokerForwardCarriesAuthoritativeBoundRoute(t *testing.T) {
	reader := newFakeReader(1)
	_, path := startCatalogServer(t, reader, &fakeMutations{})
	transport, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), WireBuild: transportproto.WireBuild, Role: trust.UnprotectedRole,
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
	reader.mu.Lock()
	headCalls := reader.headCalls
	reader.mu.Unlock()
	if headCalls != 1 {
		t.Fatalf("generation-mismatched requests reached catalog %d times, want only matched request", headCalls)
	}
}

func TestBrokerForwardAcknowledgesOnlyTheExactBoundDomain(t *testing.T) {
	_, path := startCatalogServer(t, newFakeReader(1), &fakeMutations{})
	transport, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), WireBuild: transportproto.WireBuild, Role: trust.UnprotectedRole,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := transport.Close(); err != nil {
			t.Errorf("Close transport: %v", err)
		}
	}()
	domain, err := catalogproto.DeriveDomainID("test-owner", "test-account")
	if err != nil {
		t.Fatal(err)
	}
	otherDomain, err := catalogproto.DeriveDomainID("test-owner", "other-account")
	if err != nil {
		t.Fatal(err)
	}
	call := func(requestDomain catalogproto.DomainID) catalogproto.AckActivationResponse {
		t.Helper()
		payload, err := catalogproto.Encode(catalogproto.AckActivationRequest{
			Protocol: catalogproto.Version, DomainID: requestDomain, Generation: 7,
			ActivationChangeID: "11111111111111111111111111111111",
			ActivationRevision: 8, CatalogHead: 12,
			HeadDigest: strings.Repeat("2", 64),
		})
		if err != nil {
			t.Fatal(err)
		}
		envelope, err := catalogproto.Encode(catalogproto.BrokerForwardRequest{
			Protocol: catalogproto.Version,
			Context: catalogproto.BrokerForwardContext{
				DomainID: domain, TenantID: testTenant, Generation: 7,
			},
			Operation: catalogproto.OperationActivationAck, Payload: payload,
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := transport.Call(
			context.Background(), wire.Op(catalogproto.OperationBrokerForward), "", envelope,
		)
		if err != nil {
			t.Fatalf("broker.forward activation ack: %v", err)
		}
		var response catalogproto.AckActivationResponse
		if err := catalogproto.Decode(result.Response.Payload, &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	matched := call(domain)
	if matched.Code != catalogproto.ErrorCodeOk {
		t.Fatalf("matched activation acknowledgement = %+v", matched)
	}
	if mismatched := call(otherDomain); mismatched.Code == catalogproto.ErrorCodeOk {
		t.Fatalf("mismatched activation acknowledgement succeeded: %+v", mismatched)
	}
	tenantPayload, err := catalogproto.Encode(catalogproto.PrepareTenantRequest{
		Protocol: catalogproto.Version, Generation: 7,
		Presentation: catalogproto.PresentationKindFileProvider, ActivationGeneration: "activation-7",
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = catalogproto.Encode(catalogproto.BrokerForwardRequest{
		Protocol: catalogproto.Version,
		Context: catalogproto.BrokerForwardContext{
			DomainID: domain, TenantID: testTenant, Generation: 7,
		},
		Operation: catalogproto.OperationTenantPrepare, Payload: tenantPayload,
	})
	if err == nil {
		t.Fatal("tenant preparation crossed the File Provider broker")
	}
}

func TestOldApplicationAndLFProtocolsCannotReachMutation(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &fakeMutations{}
	_, path := startCatalogServer(t, reader, mutations)

	transport, err := wire.NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), WireBuild: transportproto.WireBuild, Role: trust.UnprotectedRole,
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
		Protocol: catalogproto.Version, RequestID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
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
	headCalls     int
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
	r.mu.Lock()
	r.headCalls++
	r.mu.Unlock()
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

type unsettledRejectingMutations struct {
	mu     sync.Mutex
	source contentstream.Source
}

func (m *unsettledRejectingMutations) StageMutation(
	_ context.Context,
	_ Identity,
	_ Authorization,
	_ catalog.TenantID,
	_ catalogproto.MutationRequestID,
	_ catalog.Generation,
	_ bool,
	source contentstream.Source,
) (MutationStage, error) {
	m.mu.Lock()
	m.source = source
	m.mu.Unlock()
	return MutationStage{}, catalog.ErrConflict
}

func (*unsettledRejectingMutations) SubmitMutation(
	context.Context, Identity, Authorization, MutationSubmission,
) (MutationResult, error) {
	return MutationResult{}, errors.New("unexpected mutation submission")
}

func (rejectingMutations) StageMutation(ctx context.Context, _ Identity, _ Authorization, _ catalog.TenantID, _ catalogproto.MutationRequestID, _ catalog.Generation, _ bool, source contentstream.Source) (MutationStage, error) {
	err := catalog.ErrConflict
	if source == nil {
		return MutationStage{}, err
	}
	return MutationStage{}, errors.Join(err, source.Settle(err), source.Wait(ctx))
}

func (rejectingMutations) SubmitMutation(context.Context, Identity, Authorization, MutationSubmission) (MutationResult, error) {
	return MutationResult{}, errors.New("unexpected mutation submission")
}

func (m *fakeMutations) StageMutation(ctx context.Context, _ Identity, _ Authorization, tenant catalog.TenantID, requestID catalogproto.MutationRequestID, generation catalog.Generation, _ bool, source contentstream.Source) (stage MutationStage, err error) {
	var content []byte
	if source != nil {
		defer func() { err = errors.Join(err, source.Settle(err), source.Wait(ctx)) }()
		content, err = io.ReadAll(source)
		if err != nil {
			return MutationStage{}, err
		}
	}
	m.mu.Lock()
	m.stageCalls++
	m.staged = append([]byte(nil), content...)
	m.mu.Unlock()
	if m.wrongGeneration {
		generation++
	}
	return MutationStage{
		Token: "stage", RequestID: requestID, Tenant: tenant,
		Generation: generation, Size: int64(len(content)),
	}, nil
}

func (m *fakeMutations) SubmitMutation(_ context.Context, _ Identity, _ Authorization, submission MutationSubmission) (MutationResult, error) {
	m.mu.Lock()
	m.submitCalls++
	m.mu.Unlock()
	const revision catalog.Revision = 3
	operation, err := catalog.ParseMutationID(
		fmt.Sprintf(
			"%016x%s",
			revision,
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		),
	)
	if err != nil {
		return MutationResult{}, err
	}
	primary := objectID(10_001)
	return MutationResult{
		RequestID: submission.Request.RequestID, OperationID: operation,
		Revision: revision, PrimaryID: &primary,
	}, nil
}

type fakePreparation struct{}

func (fakePreparation) PrepareTenant(_ context.Context, _ Identity, tenant catalog.TenantID, request catalogproto.PrepareTenantRequest) (catalogproto.TenantPreparationProof, error) {
	return preparationProof(tenant, request), nil
}

type fakeActivations struct{}

func (fakeActivations) AckActivation(context.Context, Identity, catalog.TenantID, catalogproto.AckActivationRequest) error {
	return nil
}

type fakeSourceFleetService struct{}

func (fakeSourceFleetService) PublishDesiredSourceFleet(
	context.Context,
	catalog.PublishDesiredSourceFleetRequest,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	return catalog.DesiredSourceAuthorityFleetState{}, errors.New("unexpected source fleet publication")
}

func (fakeSourceFleetService) DesiredSourceFleetPage(
	context.Context,
	catalog.DesiredSourceFleetPageRequest,
) (catalog.DesiredSourceFleetPage, error) {
	return catalog.DesiredSourceFleetPage{}, errors.New("unexpected source fleet read")
}

type lostResponseSourceFleetService struct {
	mu       sync.Mutex
	calls    int
	state    *catalog.DesiredSourceAuthorityFleetState
	request  *catalog.PublishDesiredSourceFleetRequest
	lostOnce bool
}

func (p *lostResponseSourceFleetService) PublishDesiredSourceFleet(
	_ context.Context,
	request catalog.PublishDesiredSourceFleetRequest,
) (catalog.DesiredSourceAuthorityFleetState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	authorities := make([]causal.SourceAuthorityID, len(request.Declarations))
	for index, declaration := range request.Declarations {
		authorities[index] = declaration.Authority
	}
	authoritiesDigest, err := catalog.SourceAuthorityFleetDigest(authorities)
	if err != nil {
		return catalog.DesiredSourceAuthorityFleetState{}, err
	}
	declarationsDigest, err := catalog.SourceAuthorityFleetDeclarationsDigest(request.Declarations)
	if err != nil {
		return catalog.DesiredSourceAuthorityFleetState{}, err
	}
	if p.state != nil {
		if p.state.Generation == request.Generation && p.request.ExpectedGeneration == request.ExpectedGeneration &&
			p.state.AuthoritiesDigest == authoritiesDigest && p.state.DeclarationsDigest == declarationsDigest {
			return *p.state, nil
		}
		return catalog.DesiredSourceAuthorityFleetState{}, catalog.ErrGenerationMismatch
	}
	state := catalog.DesiredSourceAuthorityFleetState{
		Owner: request.Owner, Generation: request.Generation, AuthorityCount: uint64(len(request.Declarations)),
		AuthoritiesDigest: authoritiesDigest, DeclarationsDigest: declarationsDigest,
	}
	p.state = &state
	copyRequest := request
	p.request = &copyRequest
	if !p.lostOnce {
		p.lostOnce = true
		return state, errors.New("simulated lost publication response")
	}
	return state, nil
}

func (p *lostResponseSourceFleetService) DesiredSourceFleetPage(
	_ context.Context,
	request catalog.DesiredSourceFleetPageRequest,
) (catalog.DesiredSourceFleetPage, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state == nil || p.request == nil || request.Owner != p.state.Owner {
		return catalog.DesiredSourceFleetPage{}, catalog.ErrNotFound
	}
	if request.Generation != 0 &&
		(request.Generation != p.state.Generation || request.DeclarationsDigest != p.state.DeclarationsDigest) {
		return catalog.DesiredSourceFleetPage{}, catalog.ErrGenerationMismatch
	}
	start := 0
	if request.After != "" {
		for start < len(p.request.Declarations) && p.request.Declarations[start].Authority <= request.After {
			start++
		}
	}
	end := min(start+request.Limit, len(p.request.Declarations))
	declarations := append([]catalog.SourceAuthorityDeclaration(nil), p.request.Declarations[start:end]...)
	next := causal.SourceAuthorityID("")
	if end < len(p.request.Declarations) && len(declarations) != 0 {
		next = declarations[len(declarations)-1].Authority
	}
	return catalog.DesiredSourceFleetPage{State: *p.state, Declarations: declarations, Next: next}, nil
}

type fakeAuthorizer struct{}

func (fakeAuthorizer) Authorize(_ context.Context, identity Identity, operation catalogproto.Operation, route Route) (Authorization, error) {
	if identity.Session == nil || identity.WireBuild != transportproto.WireBuild || identity.Peer.PID == 0 {
		return Authorization{}, errors.New("bad identity")
	}
	if operation == catalogproto.OperationBrokerOpen {
		return Authorization{Principal: "test-app", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: route}, nil
	}
	if operation == catalogproto.OperationSourceAuthorityPublishDesiredFleet ||
		operation == catalogproto.OperationSourceAuthorityReadDesiredFleet {
		return Authorization{Principal: "test-owner", Role: RoleProductAdmin, Route: route}, nil
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

type countingBroker struct {
	mu    sync.Mutex
	opens int
}

func (b *countingBroker) OpenBroker(context.Context, Identity, string) (BrokerSession, error) {
	b.mu.Lock()
	b.opens++
	b.mu.Unlock()
	return fakeBroker{}.OpenBroker(context.Background(), Identity{}, "")
}

func (fakeBroker) OpenBroker(context.Context, Identity, string) (BrokerSession, error) {
	commands := make(chan catalogproto.BrokerCommand)
	close(commands)
	return &fakeBrokerSession{commands: commands}, nil
}

type fakeBrokerSession struct {
	commands <-chan catalogproto.BrokerCommand
}

func (s *fakeBrokerSession) Commands() <-chan catalogproto.BrokerCommand { return s.commands }
func (*fakeBrokerSession) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}
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
func (s *recordingBrokerSession) Done() <-chan struct{}                       { return s.closed }
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
	return startCatalogServerWithProtectedPeer(t, reader, mutations, broker, func(context.Context, wire.Peer) error { return nil })
}

func startCatalogServerWithProtectedPeer(
	t *testing.T,
	reader Reader,
	mutations MutationService,
	broker BrokerService,
	protectedPeer func(context.Context, wire.Peer) error,
) (*Server, string) {
	return startCatalogServerWithSourceFleetsAndProtectedPeer(
		t, reader, mutations, broker, fakeSourceFleetService{}, protectedPeer,
	)
}

func startCatalogServerWithSourceFleets(
	t *testing.T,
	reader Reader,
	mutations MutationService,
	sourceFleets SourceFleetService,
) (*Server, string) {
	return startCatalogServerWithSourceFleetsAndProtectedPeer(
		t, reader, mutations, fakeBroker{}, sourceFleets, func(context.Context, wire.Peer) error { return nil },
	)
}

func startCatalogServerWithSourceFleetsAndProtectedPeer(
	t *testing.T,
	reader Reader,
	mutations MutationService,
	broker BrokerService,
	sourceFleets SourceFleetService,
	protectedPeer func(context.Context, wire.Peer) error,
) (*Server, string) {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "fusekit-catalog-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "socket")
	wireServer := &wire.Server{WireBuild: transportproto.WireBuild, MaxFrame: 4 << 20}
	fileProvider := FileProviderConfig{
		Activations: fakeActivations{},
		Broker:      broker, ProtectedPeer: protectedPeer,
	}
	service, err := New(CoreConfig{
		Reader: reader, Mutations: mutations, Preparation: fakePreparation{},
		SourceFleets: sourceFleets, Authorizer: fakeAuthorizer{},
	}, &fileProvider)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	startCatalogTestRuntime(t, path, wireServer, service, Routes{FileProvider: true})
	return service, path
}

func startCatalogTestRuntime(
	t *testing.T,
	socket string,
	server *wire.Server,
	service *Server,
	routes Routes,
) *daemon.Runtime {
	t.Helper()
	runtime := newCatalogTestRuntime(t, socket, server)
	slot := daemon.NewPublicationSlot[*Server](runtime)
	if err := Register(server, routes, func(request wire.Request) (*Server, error) {
		resolved, ok := slot.LoadPinned(request.Publication)
		if !ok {
			return nil, daemon.ErrPublicationStale
		}
		return resolved, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	beginCatalogTestRuntime(t, runtime, slot, service)
	return runtime
}

func newCatalogTestRuntime(t *testing.T, socket string, server *wire.Server) *daemon.Runtime {
	t.Helper()
	directory := filepath.Dir(socket)
	workers, err := worker.NewPool(worker.Config{
		Capacity: 4, QueueCapacity: 4, MaxTotalRun: 5 * time.Second,
		MaxStdinBytes: 64 << 10, MaxStdoutBytes: 64 << 10, MaxStderrBytes: 64 << 10,
	}, &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(directory, "workers.db")},
		Generation: "catalogservice-test-workers", Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	children, err := proc.NewManager(4, &proc.Reaper{
		Store:      &proc.FileStore{Path: filepath.Join(directory, "children.db")},
		Generation: "catalogservice-test-children", Grace: 10 * time.Millisecond, Settlement: time.Second,
	})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	policy, err := trust.NewTrustPolicy(trust.TrustPolicyConfig{
		ExpectedUID: os.Geteuid(),
		Roles: map[trust.PeerRole]trust.Requirement{
			"fusekit.stop-controller.v1":      {TeamID: "DAEMONKITTEST", SigningIdentifier: "com.yasyf.fusekit.catalogservice.stop"},
			"fusekit.receipt-controller.v1":   {TeamID: "DAEMONKITTEST", SigningIdentifier: "com.yasyf.fusekit.catalogservice.receipt"},
			"fusekit.readiness-controller.v1": {TeamID: "DAEMONKITTEST", SigningIdentifier: "com.yasyf.fusekit.catalogservice.readiness"},
		},
		StopRoles:      []trust.PeerRole{"fusekit.stop-controller.v1"},
		ReceiptRoles:   []trust.PeerRole{"fusekit.receipt-controller.v1"},
		ReadinessRoles: []trust.PeerRole{"fusekit.readiness-controller.v1"},
	})
	if err != nil {
		t.Fatalf("NewTrustPolicy: %v", err)
	}
	runtime, err := wire.NewRuntime(wire.RuntimeConfig{
		Socket: socket, RuntimeBuild: "catalogservice-test-v1", RuntimeProtocol: 1,
		Wire: server, TrustPolicy: policy,
		StopControlStore: &proc.FileStore{Path: filepath.Join(directory, "stop.db")},
		Workers:          workers, Children: children, ShutdownTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	return runtime
}

func beginCatalogTestRuntime[T any](
	t *testing.T,
	runtime *daemon.Runtime,
	slot *daemon.PublicationSlot[T],
	value T,
) {
	t.Helper()
	activation, err := runtime.Begin(t.Context())
	if err != nil {
		t.Fatalf("Begin runtime: %v", err)
	}
	publication, err := slot.Stage(activation, value)
	if err != nil {
		t.Fatalf("Stage runtime: %v", err)
	}
	if err := activation.CommitReady(publication); err != nil {
		t.Fatalf("CommitReady: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Errorf("Close runtime: %v", err)
		}
	})
}

func testMutationRequest(marker byte) catalogproto.MutationRequest {
	mode := uint32(0o600)
	name := fmt.Sprintf("file-%d", marker)
	kind := catalogproto.ObjectKindFile
	contentRevision := uint64(1)
	parent := catalogproto.ObjectID("01010101010101010101010101010101")
	requestID := catalogproto.MutationRequestID(fmt.Sprintf("%032x", marker))
	return catalogproto.MutationRequest{
		Protocol: catalogproto.Version, RequestID: requestID, Generation: 7, ExpectedRevision: 1,
		Kind: catalogproto.MutationKindCreate, ObjectKind: &kind, HasContent: true,
		ParentID: &parent, Name: &name, Mode: &mode, ContentRevision: &contentRevision,
	}
}

func newCatalogClient(t *testing.T, path string) *Client {
	t.Helper()
	client, err := NewClient(context.Background(), wire.ClientConfig{
		Dial: wire.UnixDialer(path), StreamQueue: 32, MaxFrame: 4 << 20, Role: trust.UnprotectedRole,
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

func preparationProof(tenant catalog.TenantID, request catalogproto.PrepareTenantRequest) catalogproto.TenantPreparationProof {
	const catalogRevision = 12
	domain, err := catalogproto.DeriveDomainID("test-owner", "test-presentation")
	if err != nil {
		panic(err)
	}
	fileProvider := catalogproto.FileProviderPresentationProof{
		TenantID: catalogproto.TenantID(tenant), DomainID: domain, Generation: request.Generation,
		PublicPath: "/File Provider/Test", ActivationGeneration: request.ActivationGeneration,
	}
	return catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(tenant), Generation: request.Generation, Requested: catalogRevision,
			Desired: catalogRevision, Observed: catalogRevision, Verified: catalogRevision, Applied: catalogRevision,
		},
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindFileProvider, FileProvider: &fileProvider,
		},
		SourceAuthority: "source-main", SourceRevision: 8, CatalogRevision: catalogRevision,
		ChangeID: "11111111111111111111111111111111", OperationID: "22222222222222222222222222222222",
	}
}

func ptrProtocolObjectID(id catalog.ObjectID) *catalogproto.ObjectID {
	value := catalogproto.ObjectID(id.String())
	return &value
}
