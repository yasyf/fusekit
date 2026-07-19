package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

func TestSourceCommitsPagesExactProofWithinHardLimit(t *testing.T) {
	c := newTestCatalog(t)
	result := SourceResult{
		Authority: "paged-commits", Revision: 1,
		ChangeID: numberedChange(1), Operation: numberedOperation(1),
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_operations(
    operation_id, change_id, source_authority, source_revision,
    predecessor_revision, mode, request_hash, result_json
) VALUES (?, ?, ?, ?, 0, ?, ?, ?)`,
		result.Operation[:], result.ChangeID[:], string(result.Authority), uint64(result.Revision),
		uint8(SourceSnapshot), make([]byte, 32), encoded); err != nil {
		t.Fatal(err)
	}
	const total = SourceCommitPageLimit + 44
	for index := range total {
		tenant := fmt.Sprintf("tenant-%04d", index)
		operation := sourceCatalogOperation(result.Operation, TenantID(tenant))
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_commits(
    catalog_operation_id, source_operation_id, tenant, generation, catalog_revision,
    catalog_fingerprint, file_provider_fingerprint
)
VALUES (?, ?, ?, 1, ?, zeroblob(32), zeroblob(32))`,
			operation[:], result.Operation[:], tenant, index+1); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.SourceCommits(t.Context(), result, SourceCommitCursor{}, SourceCommitPageLimit+1); !errors.Is(err, ErrInvalidObject) {
		t.Fatalf("oversized SourceCommits = %v, want invalid object", err)
	}
	first, err := c.SourceCommits(t.Context(), result, SourceCommitCursor{}, SourceCommitPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Commits) != SourceCommitPageLimit || first.Next == nil {
		t.Fatalf("first source commit page = %d, next=%+v", len(first.Commits), first.Next)
	}
	second, err := c.SourceCommits(t.Context(), result, *first.Next, SourceCommitPageLimit)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Commits) != total-SourceCommitPageLimit || second.Next != nil {
		t.Fatalf("second source commit page = %d, next=%+v", len(second.Commits), second.Next)
	}
}
