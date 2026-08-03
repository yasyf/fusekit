package catalogservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
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
	server, d := startCatalogServer(t, reader, mutations)
	client := newCatalogClient(t, d)
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
	if err := open.Close(); err != nil {
		t.Fatalf("close OpenAt: %v", err)
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
		Kind: catalogproto.MutationKindCreate, Disposition: catalogproto.MutationDispositionNamespace,
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
	_, d := startCatalogServer(t, reader, mutations)
	client := newCatalogClient(t, d)
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
	_, d := startCatalogServer(t, reader, &fakeMutations{})
	client := newCatalogClient(t, d)
	response, err := client.PrepareTenant(t.Context(), testTenant, catalogproto.PrepareTenantRequest{
		Protocol: catalogproto.Version, Generation: 7,
		Presentation: catalogproto.PresentationKindFileProvider, ActivationGeneration: "activation-7",
		CriticalPolicyDigest: strings.Repeat("a", 64),
		CriticalObjects:      []catalogproto.CriticalObjectRequirement{{LogicalID: "settings", Role: "settings"}},
		LeaseID:              "lease-7", LeaseExpiresUnixNano: 7,
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
	_, d := startConfiguredCatalogServer(t, reader, mutations, catalogServerConfig{
		broker:     broker,
		authorizer: fakeAuthorizer{fileProvider: true},
		protected:  func(context.Context, daemonkit.Caller) error { return protectedErr },
	})
	client := newCatalogClient(t, d)
	if _, err := client.LookupPrivate(t.Context(), testTenant, catalogproto.LookupPrivateRequest{
		Protocol: catalogproto.Version, Generation: 7, ObjectID: catalogproto.ObjectID(strings.Repeat("02", 16)),
	}); err == nil {
		t.Fatal("protected File Provider read succeeded with a mismatched signed identity")
	}
	mutations.mu.Lock()
	if mutations.lookupPrivateCalls != 0 {
		t.Fatalf("rejected File Provider read reached service %d times", mutations.lookupPrivateCalls)
	}
	mutations.mu.Unlock()
	var poll catalogproto.BrokerPollResponse
	err := client.unary(t.Context(), catalogproto.OperationBrokerPoll, "", catalogproto.BrokerPollRequest{
		Protocol: catalogproto.Version,
	}, &poll)
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code == catalogproto.ErrorCodeOk {
		t.Fatalf("rejected broker poll = %v", err)
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
		{"tenant owner mutation", Authorization{Principal: "owner", Role: RoleTenantOwner, Route: route}, catalogproto.OperationCatalogMutateBegin},
		{"mount prepare", Authorization{Principal: "mount", Role: RoleMount, Presentation: catalog.PresentationMount, Route: route}, catalogproto.OperationTenantPrepare},
		{"file provider prepare", Authorization{Principal: "broker", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: route}, catalogproto.OperationTenantPrepare},
		{"tenant owner activation ack", Authorization{Principal: "owner", Role: RoleTenantOwner, Route: route}, catalogproto.OperationActivationAck},
		{"tenant owner source fleet publish", Authorization{Principal: "owner", Role: RoleTenantOwner}, catalogproto.OperationSourceAuthorityPublishDesiredFleet},
		{"product admin mutation", Authorization{Principal: "owner", Role: RoleProductAdmin}, catalogproto.OperationCatalogMutateBegin},
		{"routed product admin", Authorization{Principal: "owner", Role: RoleProductAdmin, Route: route}, catalogproto.OperationSourceAuthorityPublishDesiredFleet},
		{"routed broker poll", Authorization{Principal: "broker", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: route}, catalogproto.OperationBrokerPoll},
		{"mount activation poll", Authorization{Principal: "mount", Role: RoleMount, Presentation: catalog.PresentationMount, Route: route}, catalogproto.OperationActivationPoll},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateAuthorization(test.authorization, test.operation); err == nil {
				t.Fatal("cross-role authorization succeeded")
			}
		})
	}
}

func TestMountCannotReachPrivateMutationOrCapabilityServices(t *testing.T) {
	mutations := &fakeMutations{}
	_, d := startCatalogServer(t, newFakeReader(1), mutations)
	client := newCatalogClient(t, d)
	parent := catalogproto.ObjectID("01010101010101010101010101010101")
	object := catalogproto.ObjectID("02020202020202020202020202020202")
	creator := catalogproto.MutationID("0000000000000002100000000000000000000000000000000000000000000001")
	kind := catalogproto.ObjectKindDirectory
	name := ".private-stage"
	mode := uint32(0o700)
	target := catalogproto.ObjectID("03030303030303030303030303030303")
	privateRequests := []catalogproto.MutationRequest{
		{
			Protocol: catalogproto.Version, RequestID: "11111111111111111111111111111111",
			Generation: 7, ExpectedRevision: 2, Kind: catalogproto.MutationKindCreate,
			Disposition: catalogproto.MutationDispositionPrivateStaging,
			ObjectKind:  &kind, ParentID: &parent, Name: &name, Mode: &mode,
		},
		{
			Protocol: catalogproto.Version, RequestID: "22222222222222222222222222222222",
			Generation: 7, ExpectedRevision: 2, Kind: catalogproto.MutationKindPromote,
			Disposition: catalogproto.MutationDispositionNamespace,
			ObjectID:    &object, PrivateCreator: &creator, ParentID: &parent, Name: &name,
		},
		{
			Protocol: catalogproto.Version, RequestID: "33333333333333333333333333333333",
			Generation: 7, ExpectedRevision: 2, Kind: catalogproto.MutationKindReplace,
			Disposition: catalogproto.MutationDispositionNamespace,
			ObjectID:    &object, PrivateCreator: &creator, TargetID: &target,
		},
	}
	for _, request := range privateRequests {
		if _, err := client.Mutate(t.Context(), testTenant, request, nil); err == nil {
			t.Fatalf("mount role submitted private request kind=%s", request.Kind)
		}
	}
	_, err := client.LookupPrivate(t.Context(), testTenant, catalogproto.LookupPrivateRequest{
		Protocol: catalogproto.Version, Generation: 7, ObjectID: object,
	})
	if err == nil {
		t.Fatal("mount role looked up a private capability")
	}
	if _, err := client.OpenPrivate(t.Context(), testTenant, catalogproto.OpenPrivateRequest{
		Protocol: catalogproto.Version, Generation: 7, ObjectID: object, Creator: creator,
	}); err == nil {
		t.Fatal("mount role opened private content")
	}
	mutations.mu.Lock()
	defer mutations.mu.Unlock()
	if mutations.stageCalls != 0 || mutations.submitCalls != 0 ||
		mutations.lookupPrivateCalls != 0 || mutations.openPrivateCalls != 0 {
		t.Fatalf("rejected mount private traffic reached services: stage=%d submit=%d lookup=%d open=%d",
			mutations.stageCalls, mutations.submitCalls, mutations.lookupPrivateCalls, mutations.openPrivateCalls)
	}
}

func TestFileProviderSessionAdmitsPrivateMutation(t *testing.T) {
	mutations := &fakeMutations{}
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), mutations, catalogServerConfig{
		authorizer: fakeAuthorizer{fileProvider: true},
	})
	client := newCatalogClient(t, d)
	parent := catalogproto.ObjectID("01010101010101010101010101010101")
	kind := catalogproto.ObjectKindDirectory
	name := ".private-stage"
	mode := uint32(0o700)
	response, err := client.Mutate(t.Context(), testTenant, catalogproto.MutationRequest{
		Protocol: catalogproto.Version, RequestID: "22222222222222222222222222222222",
		Generation: 7, ExpectedRevision: 2, Kind: catalogproto.MutationKindCreate,
		Disposition: catalogproto.MutationDispositionPrivateStaging,
		ObjectKind:  &kind, ParentID: &parent, Name: &name, Mode: &mode,
	}, nil)
	if err != nil || response.Code != catalogproto.ErrorCodeOk {
		t.Fatalf("private mutation = %+v, %v", response, err)
	}
	mutations.mu.Lock()
	defer mutations.mu.Unlock()
	if mutations.stageCalls != 1 || mutations.submitCalls != 1 {
		t.Fatalf("exact File Provider calls: stage=%d submit=%d", mutations.stageCalls, mutations.submitCalls)
	}
}

func TestAckActivationRefusesDomainOutsideBrokerBinding(t *testing.T) {
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{
		authorizer: fakeAuthorizer{fileProvider: true},
	})
	client := newCatalogClient(t, d)
	request := catalogproto.AckActivationRequest{
		Protocol: catalogproto.Version, ActivationChangeID: "40000000000000000000000000000001",
		DomainID: testBoundDomain(), Generation: 7, ActivationRevision: 8,
		CatalogHead: 12, HeadDigest: strings.Repeat("a", 64),
	}
	var response catalogproto.AckActivationResponse
	if err := client.unary(t.Context(), catalogproto.OperationActivationAck, testTenant, request, &response); err != nil {
		t.Fatalf("broker-bound ack: %v", err)
	}
	other, err := catalogproto.DeriveDomainID("test-owner", "other-account")
	if err != nil {
		t.Fatalf("DeriveDomainID: %v", err)
	}
	request.DomainID = other
	err = client.unary(t.Context(), catalogproto.OperationActivationAck, testTenant, request, &response)
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != catalogproto.ErrorCodeInvalidRequest ||
		remote.Message != "acknowledged domain does not match broker binding" {
		t.Fatalf("mismatched-domain ack = %v, want the broker-binding refusal", err)
	}
}

func TestMutationResultDispositionRejectsWrongResponseArm(t *testing.T) {
	primary := objectID(1)
	private := catalog.PrivateMutationResult{ObjectID: objectID(2)}
	tests := []struct {
		name        string
		disposition catalogproto.MutationDisposition
		result      MutationResult
		wantErr     bool
	}{
		{name: "private exact", disposition: catalogproto.MutationDispositionPrivateStaging, result: MutationResult{Private: &private}},
		{name: "private namespace arm", disposition: catalogproto.MutationDispositionPrivateStaging, result: MutationResult{PrimaryID: &primary}, wantErr: true},
		{name: "namespace exact", disposition: catalogproto.MutationDispositionNamespace, result: MutationResult{PrimaryID: &primary}},
		{name: "namespace private arm", disposition: catalogproto.MutationDispositionNamespace, result: MutationResult{Private: &private}, wantErr: true},
		{name: "namespace missing arm", disposition: catalogproto.MutationDispositionNamespace, result: MutationResult{}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMutationResultDisposition(catalogproto.MutationRequest{Disposition: test.disposition}, test.result)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateMutationResultDisposition() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestDesiredSourceFleetPublishRetriesAfterLostResponse(t *testing.T) {
	publisher := &lostResponseSourceFleetService{}
	_, d := startConfiguredCatalogServer(t, newFakeReader(1), &fakeMutations{}, catalogServerConfig{sourceFleets: publisher})
	client := newCatalogClient(t, d)
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
	_, d := startCatalogServer(t, reader, mutations)
	client := newCatalogClient(t, d)
	directory := catalogproto.ObjectKindDirectory
	directoryMode := uint32(0o700)
	contentless := testMutationRequest(1)
	contentless.HasContent = false
	contentless.ContentRevision = nil
	contentless.ObjectKind = &directory
	contentless.Mode = &directoryMode
	response, err := client.Mutate(context.Background(), testTenant, contentless, nil)
	if err != nil || response.RequestID == nil || *response.RequestID != contentless.RequestID ||
		response.MutationID == nil {
		t.Fatalf("contentless Mutate() = %#v, %v", response, err)
	}
	request := testMutationRequest(2)
	response, err = client.Mutate(context.Background(), testTenant, request, bytes.NewReader([]byte("one-pass")))
	if err != nil || response.RequestID == nil || *response.RequestID != request.RequestID ||
		response.MutationID == nil {
		t.Fatalf("content Mutate() = %#v, %v", response, err)
	}
}

func TestMutationCommitRefusesDigestMismatch(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &fakeMutations{}
	_, d := startCatalogServer(t, reader, mutations)
	client := newCatalogClient(t, d)
	request := testMutationRequest(3)
	var begin catalogproto.BeginMutationResponse
	if err := client.unary(t.Context(), catalogproto.OperationCatalogMutateBegin, testTenant, request, &begin); err != nil {
		t.Fatalf("begin: %v", err)
	}
	var chunked catalogproto.MutationChunkResponse
	if err := client.unary(t.Context(), catalogproto.OperationCatalogMutateChunk, "", catalogproto.MutationChunkRequest{
		Protocol: catalogproto.Version, RequestID: request.RequestID, Sequence: 1, Payload: []byte("bytes"),
	}, &chunked); err != nil {
		t.Fatalf("chunk: %v", err)
	}
	var response catalogproto.MutationResponse
	err := client.unary(t.Context(), catalogproto.OperationCatalogMutateCommit, "", catalogproto.CommitMutationRequest{
		Protocol: catalogproto.Version, RequestID: request.RequestID,
		Total: 5, Digest: strings.Repeat("d", 64),
	}, &response)
	var remote *RemoteError
	if !errors.As(err, &remote) || remote.Code != catalogproto.ErrorCodeIntegrity {
		t.Fatalf("digest mismatch commit = %v, want integrity RemoteError", err)
	}
	mutations.mu.Lock()
	defer mutations.mu.Unlock()
	if mutations.stageCalls != 0 || mutations.submitCalls != 0 {
		t.Fatalf("mismatched commit reached services: stage=%d submit=%d", mutations.stageCalls, mutations.submitCalls)
	}
}

func TestOldApplicationProtocolCannotReachMutation(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &fakeMutations{}
	_, d := startCatalogServer(t, reader, mutations)
	business := openBusiness(t, d)
	body, err := json.Marshal(requestEnvelope{Tenant: string(testTenant), Payload: json.RawMessage(`{"protocol":0}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	reply, err := business.Call(ctx, string(catalogproto.OperationCatalogMutateBegin), body)
	if err != nil {
		t.Fatalf("old application call: %v", err)
	}
	var response catalogproto.MutationResponse
	if err := catalogproto.Decode(reply.Body, &response); err != nil {
		t.Fatalf("decode rejection: %v", err)
	}
	if response.Code != catalogproto.ErrorCodeInvalidRequest {
		t.Fatalf("old application response code = %q", response.Code)
	}
	mutations.mu.Lock()
	defer mutations.mu.Unlock()
	if mutations.stageCalls != 0 || mutations.submitCalls != 0 {
		t.Fatalf("rejected protocol reached mutation: stage=%d submit=%d", mutations.stageCalls, mutations.submitCalls)
	}
}

func TestMutationStageIdentityMismatchCannotSubmit(t *testing.T) {
	reader := newFakeReader(1)
	mutations := &fakeMutations{wrongGeneration: true}
	_, d := startCatalogServer(t, reader, mutations)
	client := newCatalogClient(t, d)
	mode := uint32(0o644)
	name := "created"
	kind := catalogproto.ObjectKindFile
	contentRevision := uint64(1)
	parent := catalogproto.ObjectID(reader.objects[0].ID.String())
	_, err := client.Mutate(context.Background(), testTenant, catalogproto.MutationRequest{
		Protocol: catalogproto.Version, RequestID: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Generation: 7, ExpectedRevision: 2, Kind: catalogproto.MutationKindCreate,
		Disposition: catalogproto.MutationDispositionNamespace,
		ObjectKind:  &kind, HasContent: true, ParentID: &parent, Name: &name, Mode: &mode,
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
	content := &blockingContent{started: make(chan struct{}), release: make(chan struct{})}
	reader.openOverride = func(_ context.Context, object catalog.Object, _ int) (OpenResult, error) {
		return OpenResult{Object: object, Content: content}, nil
	}
	server, d := startCatalogServer(t, reader, &fakeMutations{})
	client := newCatalogClient(t, d)
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
	case <-content.started:
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
	select {
	case <-content.release:
	case <-time.After(time.Second):
		t.Fatal("Close did not release the pinned server content")
	}
	server.sessionMu.Lock()
	for _, state := range server.sessions {
		state.mu.Lock()
		if len(state.handles) != 0 {
			t.Errorf("closed handle still pinned: %d", len(state.handles))
		}
		state.mu.Unlock()
	}
	server.sessionMu.Unlock()
}

const daemonkitHomeEnv = "DAEMONKIT_HOME"

type catalogServerConfig struct {
	broker       BrokerService
	sourceFleets SourceFleetService
	authorizer   Authorizer
	protected    func(context.Context, daemonkit.Caller) error
}

func startCatalogServer(t *testing.T, reader Reader, mutations MutationService) (*Server, daemonkit.Daemon) {
	t.Helper()
	return startConfiguredCatalogServer(t, reader, mutations, catalogServerConfig{})
}

// startConfiguredCatalogServer serves one generation-local catalog service on
// a real daemonkit daemon under a private DAEMONKIT_HOME, so every session,
// disconnect, and drain signal in these tests is the production one.
func startConfiguredCatalogServer(t *testing.T, reader Reader, mutations MutationService, config catalogServerConfig) (*Server, daemonkit.Daemon) {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "fkc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv(daemonkitHomeEnv, home)
	if config.broker == nil {
		config.broker = fakeBroker{}
	}
	if config.sourceFleets == nil {
		config.sourceFleets = fakeSourceFleetService{}
	}
	if config.authorizer == nil {
		config.authorizer = fakeAuthorizer{}
	}
	if config.protected == nil {
		config.protected = func(context.Context, daemonkit.Caller) error { return nil }
	}
	service, err := New(CoreConfig{
		Reader: reader, Mutations: mutations, Preparation: fakePreparation{},
		Leases: fakeFileProviderLeaseStore{}, SourceFleets: config.sourceFleets, Authorizer: config.authorizer,
	}, &FileProviderConfig{
		Activations: fakeActivations{}, Materialization: &fakeMaterialization{},
		Broker: config.broker, CriticalFetches: fakeCriticalFetches{}, ProtectedPeer: config.protected,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	specs, err := Register(Routes{FileProvider: true}, func(daemonkit.Request) (*Server, error) {
		return service, nil
	})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	deadlines := make(map[string]time.Duration, len(specs))
	for _, spec := range specs {
		deadlines[spec.Op] = 5 * time.Second
	}
	mux, err := transportproto.NewMux(deadlines, specs...)
	if err != nil {
		t.Fatalf("NewMux: %v", err)
	}
	d := daemonkit.Daemon{
		Label:     "fkcatalog",
		Schemas:   []daemonkit.Schema{daemonkit.Schema(transportproto.WireBuild)},
		Trust:     daemonkit.Trust{Serving: daemonkit.ServingSameUser()},
		Shutdown:  daemonkit.Grace(10 * time.Second),
		Handshake: daemonkit.Grace(10 * time.Second),
	}
	serveCtx, stopServing := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		_, err := daemonkit.Serve(serveCtx, d, func(daemonkit.Ctx) (daemonkit.Product, error) {
			return &catalogTestProduct{mux: mux}, nil
		})
		served <- err
	}()
	t.Cleanup(func() {
		stopServing()
		select {
		case err := <-served:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Error("daemon did not return after drain")
		}
	})
	awaitCatalogDaemonReady(t, d)
	return service, d
}

// catalogTestProduct is the daemon half: the mux answers business, and the
// drain stages have nothing of their own to settle.
type catalogTestProduct struct {
	mux *transportproto.Mux
}

func (p *catalogTestProduct) Handle(ctx context.Context, request daemonkit.Request) (daemonkit.Reply, error) {
	return p.mux.Handle(ctx, request)
}

func (p *catalogTestProduct) Drain(daemonkit.Budget) error { return nil }

func (p *catalogTestProduct) Close(daemonkit.Budget) error { return nil }

// awaitCatalogDaemonReady probes the business lane with an operation the mux
// does not route: a ProductError proves the daemon is bound, admitting, and
// dispatching, and mutates nothing on the way.
func awaitCatalogDaemonReady(t *testing.T, d daemonkit.Daemon) {
	t.Helper()
	client, err := daemonkit.Open(d)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	business := client.Business()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = business.Close(ctx)
	}()
	deadline := time.Now().Add(20 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		_, err := business.Call(ctx, "catalogservice.readiness.probe", []byte(`{}`))
		cancel()
		var product *daemonkit.ProductError
		if errors.As(err, &product) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon did not begin dispatching: %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func openBusiness(t *testing.T, d daemonkit.Daemon) *daemonkit.Business {
	t.Helper()
	client, err := daemonkit.Open(d)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	business := client.Business()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = business.Close(ctx)
	})
	return business
}

func newCatalogClient(t *testing.T, d daemonkit.Daemon) *Client {
	t.Helper()
	client, err := NewClientOn(openBusiness(t, d))
	if err != nil {
		t.Fatalf("NewClientOn: %v", err)
	}
	return client
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
		Kind: catalogproto.MutationKindCreate, Disposition: catalogproto.MutationDispositionNamespace,
		ObjectKind: &kind, HasContent: true,
		ParentID: &parent, Name: &name, Mode: &mode, ContentRevision: &contentRevision,
	}
}

func objectID(value int) catalog.ObjectID {
	id, err := catalog.ParseObjectID(fmt.Sprintf("%032x", value))
	if err != nil {
		panic(err)
	}
	return id
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

	stageCalls         int
	submitCalls        int
	lookupPrivateCalls int
	openPrivateCalls   int
	staged             []byte
	wrongGeneration    bool
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

func (*unsettledRejectingMutations) LookupPrivate(
	context.Context, Identity, Authorization, catalog.TenantID, catalog.ObjectID,
) (catalog.PrivateMutationResult, error) {
	return catalog.PrivateMutationResult{}, catalog.ErrNotFound
}

func (*unsettledRejectingMutations) OpenPrivate(
	context.Context, Identity, Authorization, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.MutationID,
) (PrivateOpenResult, error) {
	return PrivateOpenResult{}, catalog.ErrNotFound
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

func (rejectingMutations) LookupPrivate(
	context.Context, Identity, Authorization, catalog.TenantID, catalog.ObjectID,
) (catalog.PrivateMutationResult, error) {
	return catalog.PrivateMutationResult{}, catalog.ErrNotFound
}

func (rejectingMutations) OpenPrivate(
	context.Context, Identity, Authorization, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.MutationID,
) (PrivateOpenResult, error) {
	return PrivateOpenResult{}, catalog.ErrNotFound
}

func (m *fakeMutations) StageMutation(ctx context.Context, _ Identity, _ Authorization, tenant catalog.TenantID, requestID catalogproto.MutationRequestID, generation catalog.Generation, hasContent bool, source contentstream.Source) (stage MutationStage, err error) {
	if hasContent != (source != nil) {
		return MutationStage{}, errors.New("mutation content source presence mismatch")
	}
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
	if submission.Request.Disposition == catalogproto.MutationDispositionPrivateStaging {
		private := catalog.PrivateMutationResult{
			Mutation: operation, Tenant: submission.Stage.Tenant, Generation: submission.Stage.Generation,
			ObjectID: primary, Parent: objectID(1), Name: ".private-stage",
			Kind: catalog.KindDirectory, Mode: 0o700, CreatedAgainstHead: catalog.Revision(submission.Request.ExpectedRevision),
		}
		return MutationResult{
			RequestID: submission.Request.RequestID, OperationID: operation,
			Revision: revision, Private: &private,
		}, nil
	}
	return MutationResult{
		RequestID: submission.Request.RequestID, OperationID: operation,
		Revision: revision, PrimaryID: &primary,
	}, nil
}

func (m *fakeMutations) LookupPrivate(
	context.Context, Identity, Authorization, catalog.TenantID, catalog.ObjectID,
) (catalog.PrivateMutationResult, error) {
	m.mu.Lock()
	m.lookupPrivateCalls++
	m.mu.Unlock()
	return catalog.PrivateMutationResult{}, catalog.ErrNotFound
}

func (m *fakeMutations) OpenPrivate(
	context.Context, Identity, Authorization, catalog.TenantID, catalog.Generation, catalog.ObjectID, catalog.MutationID,
) (PrivateOpenResult, error) {
	m.mu.Lock()
	m.openPrivateCalls++
	m.mu.Unlock()
	return PrivateOpenResult{}, catalog.ErrNotFound
}

type fakePreparation struct{}

func (fakePreparation) PrepareTenant(_ context.Context, _ Identity, tenant catalog.TenantID, request catalogproto.PrepareTenantRequest) (catalogproto.TenantPreparationProof, error) {
	return preparationProof(tenant, request), nil
}

type fakeFileProviderLeaseStore struct{}

func (fakeFileProviderLeaseStore) PrepareFileProviderLease(_ context.Context, lease catalog.FileProviderLease) (catalog.FileProviderLease, error) {
	return lease, nil
}

func (fakeFileProviderLeaseStore) CommitFileProviderLease(_ context.Context, lease catalog.FileProviderLease) (catalog.FileProviderLease, error) {
	return lease, nil
}

func (fakeFileProviderLeaseStore) RenewFileProviderLease(_ context.Context, lease catalog.FileProviderLease) (catalog.FileProviderLease, error) {
	return lease, nil
}

func (fakeFileProviderLeaseStore) ReleaseFileProviderLease(_ context.Context, lease catalog.FileProviderLease) (catalog.FileProviderLease, error) {
	lease.State = catalog.FileProviderLeaseReleased
	return lease, nil
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

type fakeAuthorizer struct{ fileProvider bool }

func testBoundDomain() catalogproto.DomainID {
	domain, err := catalogproto.DeriveDomainID("test-owner", "test-account")
	if err != nil {
		panic(err)
	}
	return domain
}

func (a fakeAuthorizer) Authorize(_ context.Context, identity Identity, operation catalogproto.Operation, route Route) (Authorization, error) {
	if identity.Caller.PID == 0 {
		return Authorization{}, errors.New("bad identity")
	}
	switch operation {
	case catalogproto.OperationBrokerPoll, catalogproto.OperationBrokerResult:
		return Authorization{Principal: "test-app", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: route}, nil
	case catalogproto.OperationSourceAuthorityPublishDesiredFleet, catalogproto.OperationSourceAuthorityReadDesiredFleet:
		return Authorization{Principal: "test-owner", Role: RoleProductAdmin, Route: route}, nil
	}
	if route.Generation != 7 {
		return Authorization{}, catalog.ErrGenerationMismatch
	}
	switch operation {
	case catalogproto.OperationTenantPrepare,
		catalogproto.OperationPresentationLeaseCommit,
		catalogproto.OperationPresentationLeaseRenew,
		catalogproto.OperationPresentationLeaseRelease:
		return Authorization{Principal: "test-owner", Role: RoleTenantOwner, Route: route}, nil
	}
	if a.fileProvider && fileProviderOperation(operation) {
		enriched := route
		enriched.Forwarded = true
		enriched.Domain = testBoundDomain()
		return Authorization{Principal: "test-app", Role: RoleFileProvider, Presentation: catalog.PresentationFileProvider, Route: enriched}, nil
	}
	return Authorization{Principal: "test-app", Role: RoleMount, Presentation: catalog.PresentationMount, Route: route}, nil
}

type fakeBroker struct{}

type countingBroker struct {
	mu    sync.Mutex
	opens int
}

func (b *countingBroker) Draining() <-chan struct{} { return nil }

func (b *countingBroker) OpenBroker(context.Context, Identity, string) (BrokerSession, error) {
	b.mu.Lock()
	b.opens++
	b.mu.Unlock()
	return fakeBroker{}.OpenBroker(context.Background(), Identity{}, "")
}

func (fakeBroker) Draining() <-chan struct{} { return nil }

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

type blockingContent struct {
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
	closeOnce sync.Once
}

func (c *blockingContent) Read([]byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return 0, errors.New("catalog test content released")
}

func (c *blockingContent) Close() error {
	c.closeOnce.Do(func() { close(c.release) })
	return nil
}

func (r *singlePassReader) Read(buffer []byte) (int, error) {
	if r.done {
		r.readAfterEOF++
		return 0, io.EOF
	}
	r.done = true
	return copy(buffer, r.data), io.EOF
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
		PresentationInstanceID: "test-presentation", RootID: catalogproto.ObjectID(strings.Repeat("1", 32)),
	}
	proof := catalogproto.TenantPreparationProof{
		Catalog: catalogproto.CatalogLaneProof{
			Tenant: catalogproto.TenantID(tenant), Generation: request.Generation, Requested: catalogRevision,
			Desired: catalogRevision, Observed: catalogRevision, Verified: catalogRevision, Applied: catalogRevision,
		},
		Presentation: catalogproto.PresentationProof{
			Kind: catalogproto.PresentationKindFileProvider, FileProvider: &fileProvider,
		},
		SourceAuthority: "source-main", SourcePublication: "33333333333333333333333333333333",
		SourceRevision: 8, CatalogRevision: catalogRevision,
		ChangeID: "11111111111111111111111111111111", OperationID: "22222222222222222222222222222222",
	}
	proof.CriticalReadiness = &catalogproto.CriticalReadinessProof{
		PolicyDigest: request.CriticalPolicyDigest, ResolutionDigest: strings.Repeat("b", 64),
		ReadChallenge: strings.Repeat("d", 64),
		CatalogHead:   catalogRevision, SourceRevision: 8, TenantGeneration: request.Generation,
		DomainID: domain, PresentationInstanceID: fileProvider.PresentationInstanceID,
		RootID: fileProvider.RootID, ActivationGeneration: request.ActivationGeneration,
		Objects: []catalogproto.ResolvedCriticalObjectProof{{
			LogicalID: "settings", Role: "settings", ObjectID: catalogproto.ObjectID(strings.Repeat("2", 32)),
			ObjectRevision: catalogRevision, ContentRevision: catalogRevision, Size: 8, Hash: strings.Repeat("c", 64),
		}},
	}
	readProof := strings.Repeat("e", 64)
	proof.CriticalReadiness.ReadProofDigest = &readProof
	proof.CriticalReadiness.Lease = catalogproto.FileProviderLeaseReceipt{
		LeaseID: request.LeaseID, TenantID: catalogproto.TenantID(tenant), DomainID: domain,
		Generation: request.Generation, RootID: fileProvider.RootID,
		PresentationInstanceID: fileProvider.PresentationInstanceID,
		State:                  catalogproto.FileProviderLeaseStateProvisional,
		PolicyDigest:           request.CriticalPolicyDigest, ResolutionDigest: proof.CriticalReadiness.ResolutionDigest,
		CatalogHead: catalogRevision, SourceAuthority: proof.SourceAuthority,
		SourcePublication: proof.SourcePublication, SourceRevision: proof.SourceRevision,
		ActivationGeneration: request.ActivationGeneration, ExpiresUnixNano: request.LeaseExpiresUnixNano,
	}
	return proof
}

func ptrProtocolObjectID(id catalog.ObjectID) *catalogproto.ObjectID {
	value := catalogproto.ObjectID(id.String())
	return &value
}
