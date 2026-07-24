package catalog

import (
	"errors"
	"fmt"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func TestCriticalObjectPolicyDigestRejectsDuplicateAndUnorderedLogicals(t *testing.T) {
	duplicate := []CriticalObjectRequirement{
		{LogicalID: "settings", Role: "settings"},
		{LogicalID: "settings", Role: "credentials"},
	}
	if _, err := CriticalObjectPolicyDigest(duplicate); err == nil {
		t.Fatal("duplicate logical policy unexpectedly accepted")
	} else {
		var binding *CriticalObjectBindingError
		if !errors.As(err, &binding) || binding.Failure != CriticalObjectBindingDuplicate {
			t.Fatalf("duplicate error = %v", err)
		}
	}
	unordered := []CriticalObjectRequirement{
		{LogicalID: "z", Role: "settings"},
		{LogicalID: "a", Role: "credentials"},
	}
	if _, err := CriticalObjectPolicyDigest(unordered); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("unordered error = %v, want ErrInvalidObject", err)
	}
}

func TestCriticalObjectDigestsBindPolicyAndOpaqueResolution(t *testing.T) {
	policy := []CriticalObjectRequirement{
		{LogicalID: "credentials", Role: "credentials"},
		{LogicalID: "settings", Role: "settings"},
	}
	policyDigest, err := CriticalObjectPolicyDigest(policy)
	if err != nil {
		t.Fatalf("policy digest: %v", err)
	}
	changedRole := append([]CriticalObjectRequirement(nil), policy...)
	changedRole[1].Role = "configuration"
	changedPolicyDigest, err := CriticalObjectPolicyDigest(changedRole)
	if err != nil {
		t.Fatalf("changed policy digest: %v", err)
	}
	if policyDigest == changedPolicyDigest {
		t.Fatal("role change did not change policy digest")
	}

	resolution := criticalObjectResolutionForTest(t, policy)
	resolutionDigest, err := CriticalObjectResolutionDigest(resolution)
	if err != nil {
		t.Fatalf("resolution digest: %v", err)
	}
	replaced := resolution
	replaced.Objects = append([]ResolvedCriticalObject(nil), resolution.Objects...)
	replaced.Objects[1].ObjectID = criticalObjectIDForTest(t, 99)
	replaced.Objects[1].ContentRevision++
	replacedDigest, err := CriticalObjectResolutionDigest(replaced)
	if err != nil {
		t.Fatalf("replaced resolution digest: %v", err)
	}
	if resolutionDigest == replacedDigest {
		t.Fatal("atomic replacement identity did not change resolution digest")
	}
}

func TestResolveCriticalObjectsBindsLiveOpaqueReplacement(t *testing.T) {
	store := newTestCatalog(t)
	authority := causal.SourceAuthorityID("driver-authority")
	configureSourceObserverForIndexTest(t, store, authority)
	provision := testTenantProvision(t, "critical-resolution", 1)
	provision.ContentSourceID = string(authority)
	if _, err := provisionTenantForTest(t, store, t.Context(), provision); err != nil {
		t.Fatalf("provision tenant: %v", err)
	}
	viewTx, err := store.readDB.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := readCatalogView(t.Context(), viewTx, provision.Tenant)
	_ = viewTx.Rollback()
	if err != nil {
		t.Fatal(err)
	}
	var publication causal.OperationID
	copy(publication[:], view.publication)
	root, err := store.Root(t.Context(), provision.Tenant)
	if err != nil {
		t.Fatal(err)
	}
	object := Object{
		Tenant: provision.Tenant, ID: criticalObjectIDForTest(t, 41), Parent: root.ID,
		Revision: view.head, MetadataRevision: view.head, ContentRevision: view.head,
		Name: "settings.json", Kind: KindFile, Mode: 0o600, Size: 8,
		Convergence: Convergence{Desired: view.head, Observed: view.head, Verified: view.head, Applied: view.head},
		Visibility:  Visibility{FileProvider: true},
	}
	object.Hash[0] = 1
	tx, err := store.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	insertPublicationObject(t, tx, publication[:], "critical-key", object, true)
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_authority_bindings(source_authority, logical_id, source_key, effective_fingerprint)
VALUES (?, 'settings', 'critical-key', ?)`, string(authority), make([]byte, 32)); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	request := CriticalObjectResolutionRequest{
		Authority: authority, Publication: publication, Tenant: provision.Tenant,
		Generation: provision.Generation, Head: view.head,
		Objects: []CriticalObjectRequirement{{LogicalID: "settings", Role: "settings"}},
	}
	resolved, err := store.ResolveCriticalObjects(t.Context(), request)
	if err != nil {
		t.Fatalf("resolve critical objects: %v", err)
	}
	if len(resolved.Objects) != 1 || resolved.Objects[0].ObjectID != object.ID || resolved.Objects[0].Hash != object.Hash {
		t.Fatalf("resolution = %+v", resolved)
	}

	replacementID := criticalObjectIDForTest(t, 42)
	replacementHash := object.Hash
	replacementHash[0] = 2
	if _, err := store.db.ExecContext(t.Context(), `
UPDATE source_driver_publication_objects
SET object_id = ?, content_revision = content_revision + 1, hash = ?
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND source_key = 'critical-key'`,
		replacementID[:], replacementHash[:], string(authority), publication[:], string(provision.Tenant)); err != nil {
		t.Fatal(err)
	}
	replaced, err := store.ResolveCriticalObjects(t.Context(), request)
	if err != nil {
		t.Fatalf("resolve replacement: %v", err)
	}
	if replaced.Objects[0].ObjectID != replacementID || replaced.Objects[0].ContentRevision == resolved.Objects[0].ContentRevision {
		t.Fatalf("replacement resolution = %+v, prior = %+v", replaced, resolved)
	}
	request.Objects[0].LogicalID = "missing"
	_, err = store.ResolveCriticalObjects(t.Context(), request)
	var binding *CriticalObjectBindingError
	if !errors.As(err, &binding) || binding.LogicalID != "missing" || binding.Failure != CriticalObjectBindingMissing {
		t.Fatalf("missing binding error = %v", err)
	}
}

func criticalObjectResolutionForTest(t *testing.T, policy []CriticalObjectRequirement) CriticalObjectResolution {
	t.Helper()
	var publication causal.OperationID
	publication[0] = 1
	objects := make([]ResolvedCriticalObject, len(policy))
	for index, requirement := range policy {
		objects[index] = ResolvedCriticalObject{
			LogicalID: requirement.LogicalID, Role: requirement.Role,
			ObjectID: criticalObjectIDForTest(t, index+1), ObjectRevision: Revision(index + 2),
			ContentRevision: Revision(index + 3), Size: int64(index + 4), Hash: ContentHash{},
		}
		objects[index].Hash[0] = byte(index + 1)
	}
	return CriticalObjectResolution{
		Authority: "source-main", Publication: publication, Tenant: "acct-01",
		Generation: 7, Head: 11, Objects: objects,
	}
}

func criticalObjectIDForTest(t *testing.T, value int) ObjectID {
	t.Helper()
	id, err := ParseObjectID(fmt.Sprintf("%032x", value))
	if err != nil {
		t.Fatalf("parse object ID: %v", err)
	}
	return id
}
