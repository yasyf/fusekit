package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestSourceKeyedLookupsPreserveOrderAbsenceAndReplay(t *testing.T) {
	c := newTestCatalog(t)
	authority := stageSourceKeyedLookupFixture(t, c)
	locators := []SourceIndexLocator{
		{RootID: "root", Relative: "a"},
		{RootID: "root", Relative: "missing"},
		{RootID: "root", Relative: "c"},
		{RootID: "root", Relative: "a"},
	}
	request, err := NewSourcePhysicalIndexLookupRequest(authority, 17, locators)
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.SourcePhysicalIndexLookup(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.SourcePhysicalIndexLookup(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("physical replay differs:\nfirst=%+v\nsecond=%+v", first, second)
	}
	if first.Next != 21 || len(first.Entries) != len(locators) ||
		first.Entries[0].Record == nil || first.Entries[1].Record != nil ||
		first.Entries[2].Record == nil || first.Entries[3].Record == nil ||
		first.Entries[0].Record.Relative != "a" || first.Entries[2].Record.Relative != "c" ||
		first.Entries[3].Record.Relative != "a" {
		t.Fatalf("physical keyed page = %+v", first)
	}

	bindingRequest, err := NewSourceAuthorityBindingLookupRequest(
		authority, 9, []string{"logical-a", "missing", "logical-c", "logical-a"},
	)
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := c.SourceAuthorityBindingLookup(t.Context(), bindingRequest)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := c.SourceAuthorityBindingLookup(t.Context(), bindingRequest)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(bindings, replayed) {
		t.Fatalf("binding replay differs:\nfirst=%+v\nsecond=%+v", bindings, replayed)
	}
	if bindings.Next != 13 || bindings.Entries[0].Record == nil ||
		bindings.Entries[1].Record != nil || bindings.Entries[2].Record == nil ||
		bindings.Entries[3].Record == nil ||
		bindings.Entries[0].Record.SourceKey != "source-key-a" ||
		bindings.Entries[2].Record.SourceKey != "source-key-c" {
		t.Fatalf("binding keyed page = %+v", bindings)
	}
	foreignBindings, err := NewSourceAuthorityBindingLookupRequest(
		"foreign-authority", bindingRequest.Cursor, bindingRequest.Logicals,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := bindings.Validate(foreignBindings); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("cross-authority binding replay = %v, want integrity", err)
	}
}

func TestSourceKeyedLookupRejectsCursorOrderDigestAndBounds(t *testing.T) {
	c := newTestCatalog(t)
	authority := stageSourceKeyedLookupFixture(t, c)
	request, err := NewSourcePhysicalIndexLookupRequest(authority, 4, []SourceIndexLocator{
		{RootID: "root", Relative: "a"},
		{RootID: "root", Relative: "c"},
	})
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.SourcePhysicalIndexLookup(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	reordered := page
	reordered.Entries = append([]SourcePhysicalIndexLookupEntry(nil), page.Entries...)
	reordered.Entries[0], reordered.Entries[1] = reordered.Entries[1], reordered.Entries[0]
	if err := reordered.Validate(request); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("reordered page = %v, want integrity", err)
	}
	badCursor := page
	badCursor.Cursor++
	if err := badCursor.Validate(request); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("bad cursor = %v, want integrity", err)
	}
	badDigest := page
	badDigest.Digest[0] ^= 0xff
	if err := badDigest.Validate(request); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("bad digest = %v, want integrity", err)
	}
	foreignRequest, err := NewSourcePhysicalIndexLookupRequest(
		"foreign-authority", request.Cursor, request.Locators,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := page.Validate(foreignRequest); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("cross-authority physical replay = %v, want integrity", err)
	}

	maximum := make([]SourceIndexLocator, SourceKeyedLookupLimit)
	for index := range maximum {
		maximum[index] = SourceIndexLocator{RootID: "root", Relative: fmt.Sprintf("missing-%03d", index)}
	}
	maxRequest, err := NewSourcePhysicalIndexLookupRequest(authority, 0, maximum)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.SourcePhysicalIndexLookup(t.Context(), maxRequest); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSourcePhysicalIndexLookupRequest(authority, 0, append(maximum, SourceIndexLocator{
		RootID: "root", Relative: "overflow",
	})); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("257-key request = %v, want invalid object", err)
	}

	oversized := append([]SourceIndexLocator(nil), maximum...)
	for index := range oversized {
		oversized[index].Relative = strings.Repeat("x", 4096)
	}
	encoded, err := json.Marshal(SourcePhysicalIndexLookupRequest{
		Locators: oversized, Digest: [32]byte{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) <= SourceKeyedLookupByteLimit {
		t.Fatalf("oversize fixture encoded to only %d bytes", len(encoded))
	}
	if _, err := NewSourcePhysicalIndexLookupRequest(authority, 0, oversized); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("oversize encoded request = %v, want invalid object", err)
	}
}

func TestSourceSnapshotKeyedProofsRejectCrossScopeReplay(t *testing.T) {
	c := newTestCatalog(t)
	authority := causal.SourceAuthorityID("snapshot-keyed-scope")
	configureSourceObserverForIndexTest(t, c, authority)
	if err := c.BeginSourceSnapshotStage(t.Context(), authority, "snapshot-a"); err != nil {
		t.Fatal(err)
	}
	locator := SourceIndexLocator{RootID: "root", Relative: "settings.json"}
	if err := c.AppendSourceSnapshotStagePage(t.Context(), authority, "snapshot-a", SourceSnapshotPage{
		Records: []SourcePhysicalIndexRecord{{
			Authority: authority, RootID: locator.RootID, Relative: locator.Relative,
			FileIdentity: []byte("identity"), Kind: 1,
			Payload: []byte(`{"path":"settings.json"}`),
		}},
		Next: locator,
	}); err != nil {
		t.Fatal(err)
	}
	requestA, err := NewSourceSnapshotPhysicalLookupRequest(
		authority, "snapshot-a", 0, []SourceIndexLocator{locator},
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.SourceSnapshotStageLookup(t.Context(), requestA)
	if err != nil {
		t.Fatal(err)
	}
	requestB, err := NewSourceSnapshotPhysicalLookupRequest(
		authority, "snapshot-b", 0, []SourceIndexLocator{locator},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := page.Validate(requestB); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("cross-snapshot physical replay = %v, want integrity", err)
	}
	foreign, err := NewSourceSnapshotPhysicalLookupRequest(
		"foreign-authority", "snapshot-a", 0, []SourceIndexLocator{locator},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := page.Validate(foreign); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("cross-authority snapshot physical replay = %v, want integrity", err)
	}

	rootRequestA, err := NewSourceSnapshotRootLookupRequest(
		authority, "snapshot-a", 0, []TenantID{"tenant"},
	)
	if err != nil {
		t.Fatal(err)
	}
	rootPage, err := NewSourceSnapshotRootLookupPage(rootRequestA, []SourceSnapshotRootLookupEntry{
		{Tenant: "tenant"},
	})
	if err != nil {
		t.Fatal(err)
	}
	rootRequestB, err := NewSourceSnapshotRootLookupRequest(
		authority, "snapshot-b", 0, []TenantID{"tenant"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := rootPage.Validate(rootRequestB); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("cross-snapshot root replay = %v, want integrity", err)
	}
	foreignRoot, err := NewSourceSnapshotRootLookupRequest(
		"foreign-authority", "snapshot-a", 0, []TenantID{"tenant"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := rootPage.Validate(foreignRoot); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("cross-authority root replay = %v, want integrity", err)
	}
}

func stageSourceKeyedLookupFixture(t *testing.T, c *Catalog) causal.SourceAuthorityID {
	t.Helper()
	authority := causal.SourceAuthorityID("source-keyed-lookup")
	configureSourceObserverForIndexTest(t, c, authority)
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	fingerprintC := [32]byte{3}
	for _, record := range []struct {
		relative, logical, key string
		fingerprint            [32]byte
	}{
		{relative: "a", logical: "logical-a", key: "source-key-a"},
		{relative: "c", logical: "logical-c", key: "source-key-c", fingerprint: fingerprintC},
	} {
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_physical_index(
    source_authority, root_id, relative_path, file_identity, physical_kind,
    metadata_fingerprint, content_fingerprint, payload
) VALUES (?, 'root', ?, ?, 1, ?, ?, ?)`,
			string(authority), record.relative, []byte("identity-"+record.relative),
			record.fingerprint[:], record.fingerprint[:], []byte(`{"path":"`+record.relative+`"}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_physical_logical(source_authority, logical_id, root_id, relative_path)
VALUES (?, ?, 'root', ?)`, string(authority), record.logical, record.relative); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_authority_bindings(source_authority, logical_id, source_key, effective_fingerprint)
VALUES (?, ?, ?, ?)`, string(authority), record.logical, record.key, record.fingerprint[:]); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return authority
}
