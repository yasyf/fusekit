package sourceauthority

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/catalog"
)

var _ Store = (*catalog.Catalog)(nil)

func TestProductionSourceAuthorityDoesNotOwnConcreteCatalog(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".go") && !strings.HasSuffix(entry.Name(), "_test.go") {
			paths = append(paths, entry.Name())
		}
	}
	paths = append(paths, filepath.Join("..", "holder", "source_authority.go"))
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(content, []byte("catalog.Catalog")) {
			t.Errorf("%s leaks concrete catalog ownership into the source-authority runtime", path)
		}
	}
}

type physicalLookupStore struct {
	Store
	requests []catalog.SourcePhysicalIndexLookupRequest
	found    map[catalog.SourceIndexLocator]catalog.SourcePhysicalIndexRecord
}

func (s *physicalLookupStore) SourcePhysicalIndexLookup(
	_ context.Context,
	request catalog.SourcePhysicalIndexLookupRequest,
) (catalog.SourcePhysicalIndexLookupPage, error) {
	s.requests = append(s.requests, request)
	entries := make([]catalog.SourcePhysicalIndexLookupEntry, len(request.Locators))
	for index, locator := range request.Locators {
		entries[index].Locator = locator
		if record, found := s.found[locator]; found {
			copy := record
			entries[index].Record = &copy
		}
	}
	return catalog.NewSourcePhysicalIndexLookupPage(request, entries)
}

func TestSourcePhysicalIndexLookupHasCallerSideBound(t *testing.T) {
	store := &physicalLookupStore{}
	runtime := &Runtime{catalog: store, authority: "authority"}
	if _, err := runtime.sourcePhysicalIndexRecords(
		t.Context(), make([]catalog.SourceIndexLocator, maxSourcePhysicalIndexLocators+1),
	); !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("oversized physical lookup = %v, want snapshot required", err)
	}
	if len(store.requests) != 0 {
		t.Fatal("oversized physical lookup reached the remote store")
	}
	locators := make([]catalog.SourceIndexLocator, maxSourcePhysicalIndexLocators)
	for index := range locators {
		locators[index] = catalog.SourceIndexLocator{
			RootID: "root", Relative: fmt.Sprintf("entry-%04d", index),
		}
	}
	if _, err := runtime.sourcePhysicalIndexRecords(
		t.Context(), locators,
	); err != nil {
		t.Fatal(err)
	}
	if len(store.requests) != 32 {
		t.Fatalf("8192-locator lookup calls = %d, want 32", len(store.requests))
	}
	for index, request := range store.requests {
		if len(request.Locators) != catalog.SourceKeyedLookupLimit ||
			request.Cursor != uint32(index*catalog.SourceKeyedLookupLimit) {
			t.Fatalf("lookup request %d = cursor %d, %d locators", index, request.Cursor, len(request.Locators))
		}
	}
}

func TestSourcePhysicalIndexLookupPreservesOrderedMissingSemantics(t *testing.T) {
	locators := []catalog.SourceIndexLocator{
		{RootID: "root", Relative: "a"},
		{RootID: "root", Relative: "missing"},
		{RootID: "root", Relative: "c"},
	}
	record := func(locator catalog.SourceIndexLocator, identity byte) catalog.SourcePhysicalIndexRecord {
		return catalog.SourcePhysicalIndexRecord{
			Authority: "authority", RootID: locator.RootID, Relative: locator.Relative,
			FileIdentity: []byte{identity}, Kind: 1,
			MetadataFingerprint: [32]byte{identity}, ContentFingerprint: [32]byte{identity},
			Payload: []byte{identity},
		}
	}
	store := &physicalLookupStore{found: map[catalog.SourceIndexLocator]catalog.SourcePhysicalIndexRecord{
		locators[0]: record(locators[0], 1),
		locators[2]: record(locators[2], 3),
	}}
	runtime := &Runtime{catalog: store, authority: "authority"}
	records, err := runtime.sourcePhysicalIndexRecords(t.Context(), locators)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || records[0].Relative != "a" || records[1].Relative != "c" {
		t.Fatalf("ordered existing records = %+v", records)
	}
}

type bindingLookupStore struct {
	Store
	requests []catalog.SourceAuthorityBindingLookupRequest
}

func (s *bindingLookupStore) SourceAuthorityBindingLookup(
	_ context.Context,
	request catalog.SourceAuthorityBindingLookupRequest,
) (catalog.SourceAuthorityBindingLookupPage, error) {
	s.requests = append(s.requests, request)
	entries := make([]catalog.SourceAuthorityBindingLookupEntry, len(request.Logicals))
	for index, logical := range request.Logicals {
		entries[index].Logical = logical
	}
	return catalog.NewSourceAuthorityBindingLookupPage(request, entries)
}

func TestSourceAuthorityBindingLookupHasExactCallerSideBatchBound(t *testing.T) {
	store := &bindingLookupStore{}
	runtime := &Runtime{catalog: store, authority: "authority"}
	if _, err := runtime.sourceAuthorityBindings(
		t.Context(), make([]LogicalID, maxSourceAuthorityLogicals+1),
	); !errors.Is(err, ErrSnapshotRequired) {
		t.Fatalf("oversized binding lookup = %v, want snapshot required", err)
	}
	if len(store.requests) != 0 {
		t.Fatal("oversized binding lookup reached the remote store")
	}
	logicals := make([]LogicalID, maxSourceAuthorityLogicals)
	for index := range logicals {
		logicals[index] = LogicalID(fmt.Sprintf("logical-%04d", index))
	}
	bindings, err := runtime.sourceAuthorityBindings(t.Context(), logicals)
	if err != nil {
		t.Fatal(err)
	}
	if len(bindings) != 0 {
		t.Fatalf("all-missing bindings = %+v", bindings)
	}
	if len(store.requests) != 32 {
		t.Fatalf("8192-logical lookup calls = %d, want 32", len(store.requests))
	}
	for index, request := range store.requests {
		if request.Authority != "authority" ||
			len(request.Logicals) != catalog.SourceKeyedLookupLimit ||
			request.Cursor != uint32(index*catalog.SourceKeyedLookupLimit) {
			t.Fatalf("binding request %d = authority %q cursor %d, %d logicals",
				index, request.Authority, request.Cursor, len(request.Logicals))
		}
	}
}
