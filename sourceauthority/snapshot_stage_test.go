package sourceauthority

import (
	"context"
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/tenant"
)

type snapshotRootCountingStore struct {
	Store
	roots    map[catalog.TenantID]catalog.SourceSnapshotRoot
	requests []catalog.SourceSnapshotRootLookupRequest
}

func (s *snapshotRootCountingStore) SourceSnapshotRootLookup(
	_ context.Context,
	request catalog.SourceSnapshotRootLookupRequest,
) (catalog.SourceSnapshotRootLookupPage, error) {
	s.requests = append(s.requests, request)
	entries := make([]catalog.SourceSnapshotRootLookupEntry, len(request.Tenants))
	for index, id := range request.Tenants {
		entries[index].Tenant = id
		if root, found := s.roots[id]; found {
			copy := root
			entries[index].Root = &copy
		}
	}
	return catalog.NewSourceSnapshotRootLookupPage(request, entries)
}

type snapshotBindingLookupStore struct {
	Store
	requests []catalog.SourceAuthorityBindingLookupRequest
}

func (s *snapshotBindingLookupStore) SourceAuthorityBindingLookup(
	_ context.Context,
	request catalog.SourceAuthorityBindingLookupRequest,
) (catalog.SourceAuthorityBindingLookupPage, error) {
	s.requests = append(s.requests, request)
	entries := make([]catalog.SourceAuthorityBindingLookupEntry, len(request.Logicals))
	for index, logical := range request.Logicals {
		entries[index].Logical = logical
		if index%2 == 0 {
			entries[index].Record = &catalog.SourceAuthorityBindingRecord{
				Authority: request.Authority, LogicalID: logical,
				SourceKey:   catalog.SourceObjectKey("existing-" + logical),
				Fingerprint: [32]byte{1},
			}
		}
	}
	return catalog.NewSourceAuthorityBindingLookupPage(request, entries)
}

func TestSnapshotStageBindingsUseBoundedKeyedPages(t *testing.T) {
	const count = 10_000
	logicals := make([]LogicalID, count)
	for index := range count {
		logicals[index] = LogicalID(fmt.Sprintf("logical-%05d", index))
	}
	store := &snapshotBindingLookupStore{}
	runtime := &Runtime{catalog: store, authority: "authority"}
	state := newSnapshotStageState()
	if err := state.ensureBindings(t.Context(), runtime, logicals); err != nil {
		t.Fatal(err)
	}
	if len(state.bindings) != count {
		t.Fatalf("loaded bindings = %d, want %d", len(state.bindings), count)
	}
	wantCalls := (count + catalog.SourceKeyedLookupLimit - 1) / catalog.SourceKeyedLookupLimit
	if len(store.requests) != wantCalls {
		t.Fatalf("binding lookup calls = %d, want %d", len(store.requests), wantCalls)
	}
	for index, request := range store.requests {
		if len(request.Logicals) < 1 || len(request.Logicals) > catalog.SourceKeyedLookupLimit {
			t.Fatalf("binding lookup %d count = %d", index, len(request.Logicals))
		}
	}
	before := len(store.requests)
	if err := state.ensureBindings(t.Context(), runtime, logicals); err != nil {
		t.Fatal(err)
	}
	if len(store.requests) != before {
		t.Fatalf("cached bindings issued %d additional lookups", len(store.requests)-before)
	}
}

func TestSnapshotStageRootsUseBoundedKeyedPagesAcrossTenThousandObjects(t *testing.T) {
	const count = 10_000
	specs := make([]tenant.TenantSpec, count)
	ids := make([]catalog.TenantID, count)
	roots := make(map[catalog.TenantID]catalog.SourceSnapshotRoot, count)
	for index := range count {
		id := catalog.TenantID(fmt.Sprintf("tenant-%05d", index))
		ids[index] = id
		specs[index] = tenant.TenantSpec{ID: id, Generation: 1}
		roots[id] = catalog.SourceSnapshotRoot{
			Tenant: id, Generation: 1, LogicalID: fmt.Sprintf("root-%05d", index),
			RootKey: catalog.SourceObjectKey(fmt.Sprintf("root-key-%05d", index)),
		}
	}
	store := &snapshotRootCountingStore{roots: roots}
	state := newSnapshotStageState()
	state.initializeFleet(specs)
	if err := state.ensureRoots(t.Context(), store, "authority", "snapshot", ids); err != nil {
		t.Fatal(err)
	}
	for pass := range 2 {
		for index := range count {
			id := specs[index].ID
			root, found, err := state.rootForTenant(id)
			if err != nil || !found || root.Tenant != id {
				t.Fatalf("pass %d tenant %s root = %+v, %v", pass, id, root, err)
			}
		}
	}
	wantCalls := (count + catalog.SourceKeyedLookupLimit - 1) / catalog.SourceKeyedLookupLimit
	if len(store.requests) != wantCalls {
		t.Fatalf("10,000 roots used %d keyed pages, want %d", len(store.requests), wantCalls)
	}
	if err := state.ensureRoots(t.Context(), store, "authority", "snapshot", ids); err != nil {
		t.Fatal(err)
	}
	if len(store.requests) != wantCalls {
		t.Fatalf("cached roots issued %d additional keyed pages", len(store.requests)-wantCalls)
	}
}

func TestSnapshotMaterializationTaskPagesBoundTenThousandReads(t *testing.T) {
	const count = 10_000
	requests := make([]MaterializationRequest, count)
	for index := range count {
		requests[index] = MaterializationRequest{
			Logical: LogicalID(fmt.Sprintf("logical-%05d", index)),
			Inputs: []PathRef{{
				Root: "root", Relative: fmt.Sprintf("input-%05d", index),
			}},
			Payload: []byte("policy"),
		}
	}
	pages := 0
	for start := 0; start < len(requests); {
		end, err := snapshotMaterializationBatchEnd(requests, start)
		if err != nil {
			t.Fatal(err)
		}
		if end-start < 1 || end-start > snapshotMaterializationBatchLimit {
			t.Fatalf("task page %d count = %d", pages, end-start)
		}
		start = end
		pages++
	}
	want := (count + snapshotMaterializationBatchLimit - 1) / snapshotMaterializationBatchLimit
	if pages != want {
		t.Fatalf("10,000 reads used %d bounded task pages, want %d", pages, want)
	}
}
