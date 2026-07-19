package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/yasyf/fusekit/causal"
)

// ConvergenceTarget is one tenant-local catalog commit of a causal source change.
type ConvergenceTarget struct {
	Change          causal.ChangeSet
	Tenant          TenantID
	CatalogRevision Revision
}

// ConvergenceTargets returns the exact durable tenant commits for one source change.
func (c *Catalog) ConvergenceTargets(ctx context.Context, change causal.ChangeSet) ([]ConvergenceTarget, error) {
	stored, targets, err := readConvergenceChange(ctx, c.readDB, change.ChangeID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: read convergence targets: %w", err)
	}
	if !equalCausalChange(stored, change) {
		return nil, fmt.Errorf("%w: convergence change identity mismatch", ErrInvalidTransition)
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT tenant, catalog_revision
FROM convergence_outbox WHERE change_id = ? ORDER BY tenant`, change.ChangeID[:])
	if err != nil {
		return nil, fmt.Errorf("catalog: query convergence targets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := make([]ConvergenceTarget, 0, len(targets))
	for rows.Next() {
		var tenant string
		var revision uint64
		if err := rows.Scan(&tenant, &revision); err != nil {
			return nil, fmt.Errorf("catalog: scan convergence target: %w", err)
		}
		result = append(result, ConvergenceTarget{Change: stored, Tenant: TenantID(tenant), CatalogRevision: Revision(revision)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog: read convergence targets: %w", err)
	}
	if len(result) != len(targets) {
		return nil, fmt.Errorf("%w: convergence target count does not match durable change", ErrIntegrity)
	}
	for index := range result {
		if causal.TenantID(result[index].Tenant) != targets[index] {
			return nil, fmt.Errorf("%w: convergence target order does not match durable change", ErrIntegrity)
		}
	}
	return result, nil
}

// CurrentConvergenceTarget returns the newest causal catalog commit for one tenant.
func (c *Catalog) CurrentConvergenceTarget(ctx context.Context, tenant TenantID) (ConvergenceTarget, error) {
	var rawChange []byte
	if err := c.readDB.QueryRowContext(ctx, `
SELECT change_id FROM convergence_outbox
WHERE tenant = ? ORDER BY catalog_revision DESC LIMIT 1`, string(tenant)).Scan(&rawChange); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ConvergenceTarget{}, ErrNotFound
		}
		return ConvergenceTarget{}, fmt.Errorf("catalog: read current convergence target: %w", err)
	}
	if len(rawChange) != len(causal.ChangeID{}) {
		return ConvergenceTarget{}, fmt.Errorf("%w: corrupt current convergence change id", ErrIntegrity)
	}
	var changeID causal.ChangeID
	copy(changeID[:], rawChange)
	change, _, err := readConvergenceChange(ctx, c.readDB, changeID)
	if err != nil {
		return ConvergenceTarget{}, fmt.Errorf("catalog: read current convergence change: %w", err)
	}
	targets, err := c.ConvergenceTargets(ctx, change)
	if err != nil {
		return ConvergenceTarget{}, err
	}
	for _, target := range targets {
		if target.Tenant == tenant {
			return target, nil
		}
	}
	return ConvergenceTarget{}, fmt.Errorf("%w: current convergence target is missing", ErrIntegrity)
}

// SourceObjectBinding resolves one logical source key for a tenant without allocating identity.
func (c *Catalog) SourceObjectBinding(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	tenant TenantID,
	key causal.LogicalKey,
) (ObjectID, bool, error) {
	if authority == "" || tenant == "" || key == "" || strings.ContainsRune(string(key), 0) {
		return ObjectID{}, false, fmt.Errorf("%w: source binding identity is incomplete", ErrInvalidObject)
	}
	var raw []byte
	err := c.readDB.QueryRowContext(ctx, `
SELECT i.object_id
FROM source_object_bindings b
JOIN source_object_ids i
  ON i.source_authority = b.source_authority AND i.source_key = b.source_key
WHERE b.source_authority = ? AND b.tenant = ? AND b.source_key = ?`,
		string(authority), string(tenant), string(key)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return ObjectID{}, false, nil
	}
	if err != nil {
		return ObjectID{}, false, fmt.Errorf("catalog: read source object binding: %w", err)
	}
	id, err := objectID(raw)
	return id, err == nil, err
}
