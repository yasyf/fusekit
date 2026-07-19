package catalog

import (
	"context"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

// SourceCommitPageLimit is the hard maximum for one source-commit proof page.
const SourceCommitPageLimit = 256

// SourceCommitCursor continues an exact source-commit proof after one tenant.
type SourceCommitCursor struct {
	After causal.TenantID
}

// SourceCommitPage is one bounded page of catalog commits for a source result.
type SourceCommitPage struct {
	Commits []causal.CatalogCommit
	Next    *SourceCommitCursor
}

// SourceCommits returns one bounded page of the durable catalog commits proved by result.
func (c *Catalog) SourceCommits(
	ctx context.Context,
	result SourceResult,
	cursor SourceCommitCursor,
	limit int,
) (SourceCommitPage, error) {
	if result.Authority == "" || result.Revision == 0 || result.ChangeID == (causal.ChangeID{}) ||
		result.Operation == (causal.OperationID{}) || limit < 1 || limit > SourceCommitPageLimit {
		return SourceCommitPage{}, fmt.Errorf("%w: invalid source commit page request", ErrInvalidObject)
	}
	stored, found, err := readSourceOperation(ctx, c.readDB, result.Operation)
	if err != nil {
		return SourceCommitPage{}, err
	}
	if !found {
		return SourceCommitPage{}, ErrNotFound
	}
	if stored.result != result {
		return SourceCommitPage{}, ErrMutationConflict
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT tenant, catalog_revision, file_provider_fingerprint
FROM source_commits
WHERE source_operation_id = ? AND tenant > ?
ORDER BY tenant LIMIT ?`, result.Operation[:], string(cursor.After), limit+1)
	if err != nil {
		return SourceCommitPage{}, fmt.Errorf("catalog: query source commits: %w", err)
	}
	defer func() { _ = rows.Close() }()
	commits := make([]causal.CatalogCommit, 0, limit+1)
	for rows.Next() {
		var tenant string
		var revision uint64
		var rawFingerprint []byte
		if err := rows.Scan(&tenant, &revision, &rawFingerprint); err != nil {
			return SourceCommitPage{}, fmt.Errorf("catalog: scan source commit: %w", err)
		}
		if tenant == "" || revision == 0 || len(rawFingerprint) != len(causal.CatalogCommit{}.FileProviderFingerprint) {
			return SourceCommitPage{}, ErrIntegrity
		}
		commit := causal.CatalogCommit{
			Tenant: causal.TenantID(tenant), CatalogRevision: causal.CatalogRevision(revision),
		}
		copy(commit.FileProviderFingerprint[:], rawFingerprint)
		commits = append(commits, commit)
	}
	if err := rows.Err(); err != nil {
		return SourceCommitPage{}, fmt.Errorf("catalog: read source commits: %w", err)
	}
	page := SourceCommitPage{Commits: commits}
	if len(page.Commits) > limit {
		page.Commits = page.Commits[:limit]
		page.Next = &SourceCommitCursor{After: page.Commits[len(page.Commits)-1].Tenant}
	}
	return page, nil
}
