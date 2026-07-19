package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/yasyf/fusekit/causal"
)

func claimConvergenceOutboxForTest(ctx context.Context, c *Catalog) (*causal.OutboxPage, error) {
	claim, err := c.ClaimConvergenceOutbox(ctx)
	if err != nil || claim == nil {
		return nil, err
	}
	result := &causal.OutboxPage{}
	seen := map[causal.OutboxCursor]struct{}{}
	for {
		if _, duplicate := seen[claim.Cursor]; duplicate {
			return nil, fmt.Errorf("catalog test: convergence outbox cursor cycle at %+v", claim.Cursor)
		}
		seen[claim.Cursor] = struct{}{}
		page, err := c.PageConvergenceOutbox(ctx, *claim)
		if err != nil {
			return nil, err
		}
		if result.Change.ChangeID == (causal.ChangeID{}) {
			result.Change = page.Change
			result.Change.AffectedKeys = nil
		} else if !sameOutboxHeader(result.Change, page.Change) {
			return nil, fmt.Errorf("catalog test: convergence outbox header changed between pages")
		}
		result.Change.AffectedKeys = append(result.Change.AffectedKeys, page.Change.AffectedKeys...)
		result.Commits = append(result.Commits, page.Commits...)
		if page.Next == nil {
			if page.Settlement == nil {
				return nil, fmt.Errorf("catalog test: terminal convergence outbox page has no settlement")
			}
			result.Settlement = page.Settlement
			return result, nil
		}
		if page.Settlement != nil {
			return nil, fmt.Errorf("catalog test: nonterminal convergence outbox page has a settlement")
		}
		claim.Cursor = *page.Next
	}
}

func sameOutboxHeader(left, right causal.ChangeSet) bool {
	return left.SourceAuthority == right.SourceAuthority &&
		left.SourceRevision == right.SourceRevision &&
		left.ChangeID == right.ChangeID &&
		left.OperationID == right.OperationID &&
		left.Cause == right.Cause &&
		left.Origin == right.Origin &&
		left.OriginGeneration == right.OriginGeneration
}

func TestConvergenceOutboxDrainsHeaderWithoutFileProviderTargets(t *testing.T) {
	c := newTestCatalog(t)
	change := seedPagedConvergenceOutbox(t, c, 3, 0)
	page, err := claimConvergenceOutboxForTest(t.Context(), c)
	if err != nil {
		t.Fatal(err)
	}
	if page == nil || page.Change.ChangeID != change || len(page.Change.AffectedKeys) != 3 ||
		len(page.Commits) != 0 || page.Settlement == nil {
		t.Fatalf("zero-target page = %+v", page)
	}
	if err := c.SettleConvergenceOutbox(t.Context(), *page.Settlement); err != nil {
		t.Fatal(err)
	}
	if pending, err := c.ClaimConvergenceOutbox(t.Context()); err != nil || pending != nil {
		t.Fatalf("post-settlement claim = %+v, %v", pending, err)
	}
	var state uint8
	if err := c.db.QueryRowContext(t.Context(), `
SELECT outbox_state FROM convergence_changes WHERE change_id = ?`, change[:]).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if convergenceOutboxState(state) != outboxSettled {
		t.Fatalf("header state = %d, want settled", state)
	}
}

func TestConvergenceOutboxPagesAreDurablyChainedAndSettlementIsTerminal(t *testing.T) {
	c := newTestCatalog(t)
	change := seedPagedConvergenceOutbox(t, c, 517, 19)
	claim, err := c.ClaimConvergenceOutbox(t.Context())
	if err != nil || claim == nil || claim.ChangeID != change {
		t.Fatalf("ClaimConvergenceOutbox = %+v, %v", claim, err)
	}
	reclaimed, err := c.ClaimConvergenceOutbox(t.Context())
	if err != nil || !reflect.DeepEqual(reclaimed, claim) {
		t.Fatalf("reclaimed convergence outbox = %+v, %v; want %+v", reclaimed, err, claim)
	}
	first, err := c.PageConvergenceOutbox(t.Context(), *claim)
	if err != nil {
		t.Fatal(err)
	}
	if first.Next == nil || first.Settlement != nil ||
		len(first.Change.AffectedKeys) != ConvergenceOutboxPageLimit || len(first.Commits) != 19 {
		t.Fatalf("first convergence outbox page = %+v", first)
	}
	replayed, err := c.PageConvergenceOutbox(t.Context(), *claim)
	if err != nil || !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed first page = %+v, %v; want %+v", replayed, err, first)
	}
	forged := causal.OutboxSettlement{ChangeID: change}
	forged.Digest[0] = 1
	if err := c.SettleConvergenceOutbox(t.Context(), forged); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("early forged settlement = %v, want mutation conflict", err)
	}

	keys, commits := append([]causal.LogicalKey(nil), first.Change.AffectedKeys...), append([]causal.CatalogCommit(nil), first.Commits...)
	claim.Cursor = *first.Next
	var settlement *causal.OutboxSettlement
	for pageNumber := 1; ; pageNumber++ {
		page, err := c.PageConvergenceOutbox(t.Context(), *claim)
		if err != nil {
			t.Fatalf("page %d: %v", pageNumber, err)
		}
		if len(page.Change.AffectedKeys) > ConvergenceOutboxPageLimit ||
			len(page.Commits) > ConvergenceOutboxPageLimit {
			t.Fatalf("page %d exceeded hard request bound: %+v", pageNumber, page)
		}
		keys = append(keys, page.Change.AffectedKeys...)
		commits = append(commits, page.Commits...)
		if page.Next == nil {
			if page.Settlement == nil {
				t.Fatalf("terminal page %d has no settlement", pageNumber)
			}
			settlement = page.Settlement
			break
		}
		if page.Settlement != nil {
			t.Fatalf("nonterminal page %d has settlement %+v", pageNumber, page.Settlement)
		}
		claim.Cursor = *page.Next
	}
	if len(keys) != 517 || len(commits) != 19 {
		t.Fatalf("paged convergence outbox counts = %d keys / %d commits", len(keys), len(commits))
	}
	forged = *settlement
	forged.Digest[0] ^= 0xff
	if err := c.SettleConvergenceOutbox(t.Context(), forged); !errors.Is(err, ErrMutationConflict) {
		t.Fatalf("forged terminal settlement = %v, want mutation conflict", err)
	}
	if err := c.SettleConvergenceOutbox(t.Context(), *settlement); err != nil {
		t.Fatalf("SettleConvergenceOutbox: %v", err)
	}
	if err := c.SettleConvergenceOutbox(t.Context(), *settlement); err != nil {
		t.Fatalf("replayed SettleConvergenceOutbox: %v", err)
	}
	if pending, err := c.ClaimConvergenceOutbox(t.Context()); err != nil || pending != nil {
		t.Fatalf("settled convergence outbox = %+v, %v", pending, err)
	}
}

func TestConvergenceOutboxPartialPageCrashReplaysBeforeAdvancing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.sqlite")
	c, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	change := seedPagedConvergenceOutbox(t, c, ConvergenceOutboxPageLimit+9, 3)
	claim, err := c.ClaimConvergenceOutbox(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.PageConvergenceOutbox(t.Context(), *claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = recovered.Close() })
	resumed, err := recovered.ClaimConvergenceOutbox(t.Context())
	if err != nil || resumed == nil || resumed.ChangeID != change || resumed.Cursor != claim.Cursor {
		t.Fatalf("resumed claim = %+v, %v; want replay cursor %+v", resumed, err, claim.Cursor)
	}
	replayed, err := recovered.PageConvergenceOutbox(t.Context(), *resumed)
	if err != nil || !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed crash page = %+v, %v; want %+v", replayed, err, first)
	}
	resumed.Cursor = *replayed.Next
	for {
		page, err := recovered.PageConvergenceOutbox(t.Context(), *resumed)
		if err != nil {
			t.Fatal(err)
		}
		if page.Settlement != nil {
			if err := recovered.SettleConvergenceOutbox(t.Context(), *page.Settlement); err != nil {
				t.Fatal(err)
			}
			break
		}
		resumed.Cursor = *page.Next
	}
}

func TestConvergenceOutboxRejectsIncompleteOldestChange(t *testing.T) {
	c := newTestCatalog(t)
	change := seedPagedConvergenceOutbox(t, c, 3, 3)
	if _, err := c.db.ExecContext(t.Context(), `
DELETE FROM convergence_outbox
WHERE change_id = ? AND tenant = 'tenant-0002'`, change[:]); err != nil {
		t.Fatal(err)
	}
	if claim, err := c.ClaimConvergenceOutbox(t.Context()); !errors.Is(err, ErrIntegrity) || claim != nil {
		t.Fatalf("incomplete oldest convergence outbox = %+v, %v; want integrity error", claim, err)
	}
}

func TestZeroTargetConvergenceHeaderPagesAndSettles(t *testing.T) {
	c := newTestCatalog(t)
	change := seedPagedConvergenceOutbox(t, c, 3, 0)
	claim, err := c.ClaimConvergenceOutbox(t.Context())
	if err != nil || claim == nil || claim.ChangeID != change {
		t.Fatalf("ClaimConvergenceOutbox = %+v, %v", claim, err)
	}
	page, err := c.PageConvergenceOutbox(t.Context(), *claim)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Commits) != 0 || len(page.Change.AffectedKeys) != 3 || page.Settlement == nil || page.Next != nil {
		t.Fatalf("zero-target terminal page = %+v", page)
	}
	if err := c.SettleConvergenceOutbox(t.Context(), *page.Settlement); err != nil {
		t.Fatal(err)
	}
	if err := c.SettleConvergenceOutbox(t.Context(), *page.Settlement); err != nil {
		t.Fatalf("replayed settlement: %v", err)
	}
	var state uint8
	if err := c.readDB.QueryRowContext(t.Context(),
		`SELECT outbox_state FROM convergence_changes WHERE change_id = ?`, change[:],
	).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if convergenceOutboxState(state) != outboxSettled {
		t.Fatalf("header state = %d, want settled", state)
	}
	if next, err := c.ClaimConvergenceOutbox(t.Context()); err != nil || next != nil {
		t.Fatalf("post-settlement claim = %+v, %v", next, err)
	}
}

func seedPagedConvergenceOutbox(t *testing.T, c *Catalog, keyCount, commitCount int) causal.ChangeID {
	t.Helper()
	result := SourceResult{
		Authority: "paged-outbox", Revision: 1,
		ChangeID: numberedChange(700), Operation: numberedOperation(700),
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := c.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_operations(
    operation_id, change_id, source_authority, source_revision,
    predecessor_revision, mode, request_hash, result_json
) VALUES (?, ?, ?, 1, 0, ?, ?, ?)`,
		result.Operation[:], result.ChangeID[:], string(result.Authority),
		uint8(SourceSnapshot), make([]byte, 32), encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `
INSERT INTO convergence_changes(
    change_id, source_operation_id, source_authority, source_revision,
    cause, origin_domain, origin_generation
) VALUES (?, ?, ?, 1, ?, '', 0)`,
		result.ChangeID[:], result.Operation[:], string(result.Authority), string(causal.CauseMigration)); err != nil {
		t.Fatal(err)
	}
	for index := range keyCount {
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO convergence_change_affected(change_id, affected_key) VALUES (?, ?)`,
			result.ChangeID[:], fmt.Sprintf("key-%04d", index)); err != nil {
			t.Fatal(err)
		}
	}
	for index := range commitCount {
		tenant := fmt.Sprintf("tenant-%04d", index)
		operation := sourceCatalogOperation(result.Operation, TenantID(tenant))
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO convergence_change_targets(change_id, tenant) VALUES (?, ?)`,
			result.ChangeID[:], tenant); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO source_commits(
    catalog_operation_id, source_operation_id, tenant, generation, catalog_revision,
    catalog_fingerprint, file_provider_fingerprint
) VALUES (?, ?, ?, 1, 1, zeroblob(32), zeroblob(32))`,
			operation[:], result.Operation[:], tenant); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(t.Context(), `
INSERT INTO convergence_outbox(
    catalog_operation_id, change_id, tenant, catalog_revision, file_provider_fingerprint, state
) VALUES (?, ?, ?, 1, zeroblob(32), ?)`,
			operation[:], result.ChangeID[:], tenant, uint8(outboxPending)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	return result.ChangeID
}
