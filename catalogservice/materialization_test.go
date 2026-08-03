package catalogservice

import (
	"context"
	"sync"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
)

type fakeMaterialization struct {
	mu       sync.Mutex
	identity catalog.FileProviderMaterializationIdentity
	page     catalog.FileProviderMaterializationPage
	commit   catalog.FileProviderMaterializationCommit
	begins   int
}

func (f *fakeMaterialization) BeginFileProviderMaterializationSnapshot(
	_ context.Context,
	identity catalog.FileProviderMaterializationIdentity,
) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.identity = identity
	f.begins++
	return 1, nil
}

func TestMaterializationPayloadMustMatchAuthenticatedBrokerRoute(t *testing.T) {
	materialization := &fakeMaterialization{}
	core := testCoreConfig()
	core.Authorizer = fakeAuthorizer{fileProvider: true}
	server, err := New(core, &FileProviderConfig{
		Activations: fakeActivations{}, Broker: fakeBroker{}, Materialization: materialization, CriticalFetches: fakeCriticalFetches{},
		ProtectedPeer: func(context.Context, daemonkit.Caller) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	domain := testBoundDomain()
	otherDomain, err := catalogproto.DeriveDomainID("test-owner", "other-account")
	if err != nil {
		t.Fatal(err)
	}
	call := func(payloadTenant catalogproto.TenantID, payloadDomain catalogproto.DomainID, payloadGeneration uint64) catalogproto.BeginMaterializationSnapshotResponse {
		t.Helper()
		inner, err := catalogproto.Encode(catalogproto.BeginMaterializationSnapshotRequest{
			Protocol: catalogproto.Version, TenantID: payloadTenant, DomainID: payloadDomain,
			Generation: payloadGeneration, SnapshotID: "11111111111111111111111111111111",
			BackingStoreIdentity: []byte("backing"),
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.WithValue(t.Context(), routingTenantKey{}, string(testTenant))
		payload, err := server.handleBeginMaterializationSnapshot(ctx, daemonkit.Request{
			Op: string(catalogproto.OperationMaterializationSnapshotBegin), Body: inner,
			Caller: daemonkit.Caller{UID: 501, PID: 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		var response catalogproto.BeginMaterializationSnapshotResponse
		if err := catalogproto.Decode(payload, &response); err != nil {
			t.Fatal(err)
		}
		return response
	}
	if response := call(testTenant, domain, 7); response.Code != catalogproto.ErrorCodeOk || response.Epoch != 1 {
		t.Fatalf("matched route = %+v", response)
	}
	for _, mismatch := range []struct {
		tenant     catalogproto.TenantID
		domain     catalogproto.DomainID
		generation uint64
	}{
		{tenant: "other", domain: domain, generation: 7},
		{tenant: testTenant, domain: otherDomain, generation: 7},
		{tenant: testTenant, domain: domain, generation: 8},
	} {
		if response := call(mismatch.tenant, mismatch.domain, mismatch.generation); response.Code == catalogproto.ErrorCodeOk {
			t.Fatalf("mismatched route succeeded: %+v", mismatch)
		}
	}
	materialization.mu.Lock()
	defer materialization.mu.Unlock()
	if materialization.begins != 1 {
		t.Fatalf("mismatched payload reached catalog %d times", materialization.begins)
	}
}

func (f *fakeMaterialization) SuspendFileProviderMaterialization(
	context.Context,
	catalog.TenantID,
	causal.DomainID,
	catalog.Generation,
) error {
	return nil
}

func (f *fakeMaterialization) StageFileProviderMaterializationPage(
	_ context.Context,
	page catalog.FileProviderMaterializationPage,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.page = page
	return nil
}

func (f *fakeMaterialization) CommitFileProviderMaterializationSnapshot(
	_ context.Context,
	commit catalog.FileProviderMaterializationCommit,
) (catalog.FileProviderMaterializationResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commit = commit
	return catalog.FileProviderMaterializationResult{Revision: 1}, nil
}
