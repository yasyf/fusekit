package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// RetentionOwner identifies one authenticated session within the exact
// catalog worker generation.
type RetentionOwner string

// NewRetentionOwner validates one authenticated session owner.
func NewRetentionOwner(value string) (RetentionOwner, error) {
	if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
		return "", fmt.Errorf("%w: invalid retention owner", ErrInvalidObject)
	}
	return RetentionOwner(value), nil
}

// RetentionRetirement reports one bounded owner-retirement page.
type RetentionRetirement struct {
	Closed int
	More   bool
}

// CatalogGenerationRetirement reports one bounded proven-dead generation page.
type CatalogGenerationRetirement struct {
	Retired int
	More    bool
}

// CatalogGenerationCollection reports one bounded retired-generation page.
type CatalogGenerationCollection struct {
	RetentionOwners int
	Generations     int
	More            bool
}

const retentionGenerationAfterRetire = "retention.generation_after_retire"

func (c *Catalog) registerCatalogGeneration(ctx context.Context) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin generation registration: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO catalog_generations(owner_id, retired)
VALUES (?, 0)
ON CONFLICT(owner_id) DO NOTHING`, c.owner[:]); err != nil {
		return fmt.Errorf("catalog: register generation: %w", err)
	}
	var retired bool
	if err := tx.QueryRowContext(ctx,
		"SELECT retired FROM catalog_generations WHERE owner_id = ?",
		c.owner[:]).Scan(&retired); err != nil {
		return fmt.Errorf("catalog: read registered generation: %w", err)
	}
	if retired {
		return fmt.Errorf("%w: catalog generation identity was already retired", ErrIntegrity)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit generation registration: %w", err)
	}
	return nil
}

// RetirePriorCatalogGenerations records one bounded page of exact prior
// process generations as unreachable. Callers invoke it only after the parent
// has reaped the prior worker and durable manifest recovery has completed.
func (c *Catalog) RetirePriorCatalogGenerations(
	ctx context.Context,
) (CatalogGenerationRetirement, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return CatalogGenerationRetirement{}, fmt.Errorf(
			"catalog: begin prior generation retirement: %w", err,
		)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT owner_id
FROM catalog_generations
WHERE owner_id <> ? AND retired = 0
ORDER BY owner_id
LIMIT ?`, c.owner[:], RetainedIdentityPageLimit+1)
	if err != nil {
		return CatalogGenerationRetirement{}, fmt.Errorf(
			"catalog: select prior generations: %w", err,
		)
	}
	var generations []HandleOwnerID
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return CatalogGenerationRetirement{}, fmt.Errorf(
				"catalog: scan prior generation: %w", err,
			)
		}
		if len(raw) != len(HandleOwnerID{}) {
			_ = rows.Close()
			return CatalogGenerationRetirement{}, fmt.Errorf(
				"%w: prior catalog generation length", ErrIntegrity,
			)
		}
		var generation HandleOwnerID
		copy(generation[:], raw)
		generations = append(generations, generation)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return CatalogGenerationRetirement{}, fmt.Errorf(
			"catalog: read prior generations: %w", err,
		)
	}
	if err := rows.Close(); err != nil {
		return CatalogGenerationRetirement{}, fmt.Errorf(
			"catalog: close prior generations: %w", err,
		)
	}
	more := len(generations) > RetainedIdentityPageLimit
	if more {
		generations = generations[:RetainedIdentityPageLimit]
	}
	for _, generation := range generations {
		result, err := tx.ExecContext(ctx, `
UPDATE catalog_generations SET retired = 1
WHERE owner_id = ? AND retired = 0`, generation[:])
		if err != nil {
			return CatalogGenerationRetirement{}, fmt.Errorf(
				"catalog: retire prior generation: %w", err,
			)
		}
		if err := requireOneRow(result, "prior catalog generation"); err != nil {
			return CatalogGenerationRetirement{}, err
		}
	}
	if err := c.trip(retentionGenerationAfterRetire); err != nil {
		return CatalogGenerationRetirement{}, err
	}
	if err := tx.Commit(); err != nil {
		return CatalogGenerationRetirement{}, fmt.Errorf(
			"catalog: commit prior generation retirement: %w", err,
		)
	}
	return CatalogGenerationRetirement{Retired: len(generations), More: more}, nil
}

// CollectRetiredCatalogGenerations deletes one bounded page of retired process
// generations after every session receipt and content stage has drained.
func (c *Catalog) CollectRetiredCatalogGenerations(
	ctx context.Context,
) (CatalogGenerationCollection, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return CatalogGenerationCollection{}, fmt.Errorf(
			"catalog: begin retired generation collection: %w", err,
		)
	}
	defer func() { _ = tx.Rollback() }()
	result := CatalogGenerationCollection{}
	result.RetentionOwners, result.More, err = collectRetiredOwners(
		ctx, tx, RetainedIdentityPageLimit,
	)
	if err != nil {
		return CatalogGenerationCollection{}, err
	}
	remaining := RetainedIdentityPageLimit - result.RetentionOwners
	if remaining == 0 {
		result.More = true
		if err := tx.Commit(); err != nil {
			return CatalogGenerationCollection{}, fmt.Errorf(
				"catalog: commit retired generation collection: %w", err,
			)
		}
		return result, nil
	}
	rows, err := tx.QueryContext(ctx, `
SELECT generation.owner_id
FROM catalog_generations generation
WHERE generation.retired = 1
  AND NOT EXISTS (
      SELECT 1 FROM retention_owners owner
      WHERE owner.owner_id = generation.owner_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM content_stages stage
      WHERE stage.owner_id = generation.owner_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM storage_entries entry INDEXED BY storage_entries_generation
      WHERE entry.owner_id = generation.owner_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM storage_transitions transition INDEXED BY storage_transitions_owner
      WHERE transition.owner_id = generation.owner_id
  )
ORDER BY generation.owner_id
LIMIT ?`, remaining+1)
	if err != nil {
		return CatalogGenerationCollection{}, fmt.Errorf(
			"catalog: select retired generations: %w", err,
		)
	}
	var generations []HandleOwnerID
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return CatalogGenerationCollection{}, fmt.Errorf(
				"catalog: scan retired generation: %w", err,
			)
		}
		if len(raw) != len(HandleOwnerID{}) {
			_ = rows.Close()
			return CatalogGenerationCollection{}, fmt.Errorf(
				"%w: retired catalog generation length", ErrIntegrity,
			)
		}
		var generation HandleOwnerID
		copy(generation[:], raw)
		generations = append(generations, generation)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return CatalogGenerationCollection{}, fmt.Errorf(
			"catalog: read retired generations: %w", err,
		)
	}
	if err := rows.Close(); err != nil {
		return CatalogGenerationCollection{}, fmt.Errorf(
			"catalog: close retired generations: %w", err,
		)
	}
	more := len(generations) > remaining
	if more {
		generations = generations[:remaining]
	}
	for _, generation := range generations {
		result, err := tx.ExecContext(ctx, `
DELETE FROM catalog_generations
WHERE owner_id = ? AND retired = 1
  AND NOT EXISTS (
      SELECT 1 FROM retention_owners owner
      WHERE owner.owner_id = catalog_generations.owner_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM content_stages stage
      WHERE stage.owner_id = catalog_generations.owner_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM storage_entries entry INDEXED BY storage_entries_generation
      WHERE entry.owner_id = catalog_generations.owner_id
  )
  AND NOT EXISTS (
      SELECT 1 FROM storage_transitions transition INDEXED BY storage_transitions_owner
      WHERE transition.owner_id = catalog_generations.owner_id
  )`, generation[:])
		if err != nil {
			return CatalogGenerationCollection{}, fmt.Errorf(
				"catalog: delete retired generation: %w", err,
			)
		}
		if err := requireOneRow(result, "retired catalog generation"); err != nil {
			return CatalogGenerationCollection{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return CatalogGenerationCollection{}, fmt.Errorf(
			"catalog: commit retired generation collection: %w", err,
		)
	}
	result.Generations = len(generations)
	result.More = result.More || more
	if result.RetentionOwners+result.Generations == RetainedIdentityPageLimit {
		result.More = true
	}
	return result, nil
}

func ensureRetentionOwner(
	ctx context.Context,
	tx *sql.Tx,
	generation HandleOwnerID,
	owner RetentionOwner,
) error {
	if _, err := NewRetentionOwner(string(owner)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO retention_owners(owner_id, session_owner, retired)
VALUES (?, ?, 0)
ON CONFLICT(owner_id, session_owner) DO NOTHING`,
		generation[:], string(owner)); err != nil {
		return fmt.Errorf("catalog: register retention owner: %w", err)
	}
	var retired bool
	if err := tx.QueryRowContext(ctx, `
SELECT retired FROM retention_owners
WHERE owner_id = ? AND session_owner = ?`,
		generation[:], string(owner)).Scan(&retired); err != nil {
		return fmt.Errorf("catalog: read retention owner: %w", err)
	}
	if retired {
		return ErrHandleClosed
	}
	return nil
}

func requireLiveRetentionOwner(
	ctx context.Context,
	tx *sql.Tx,
	generation HandleOwnerID,
	owner RetentionOwner,
) error {
	if _, err := NewRetentionOwner(string(owner)); err != nil {
		return err
	}
	var retired bool
	err := tx.QueryRowContext(ctx, `
SELECT retired FROM retention_owners
WHERE owner_id = ? AND session_owner = ?`,
		generation[:], string(owner)).Scan(&retired)
	if errors.Is(err, sql.ErrNoRows) || retired {
		return ErrHandleClosed
	}
	if err != nil {
		return fmt.Errorf("catalog: read live retention owner: %w", err)
	}
	return nil
}

// RetireRetentionOwner fences one exact live session and closes at most one
// bounded page of its retained identities.
func (c *Catalog) RetireRetentionOwner(
	ctx context.Context,
	owner RetentionOwner,
) (RetentionRetirement, error) {
	if _, err := NewRetentionOwner(string(owner)); err != nil {
		return RetentionRetirement{}, err
	}
	return c.retireRetentionOwner(ctx, c.owner, owner)
}

// RetirePriorRetentionOwners closes one bounded page from one prior catalog
// generation. Callers invoke it only after durable manifest recovery has
// handed every still-live mutation to the current generation.
func (c *Catalog) RetirePriorRetentionOwners(
	ctx context.Context,
) (RetentionRetirement, error) {
	var raw []byte
	var session string
	err := c.readDB.QueryRowContext(ctx, `
SELECT owner.owner_id, owner.session_owner
FROM retention_owners owner
JOIN catalog_generations generation
  ON generation.owner_id = owner.owner_id AND generation.retired = 1
WHERE owner.owner_id <> ?
  AND (
      owner.retired = 0
      OR EXISTS (
          SELECT 1 FROM handles handle
          WHERE handle.owner_id = owner.owner_id
            AND handle.session_owner = owner.session_owner
            AND handle.closed = 0
      )
      OR EXISTS (
          SELECT 1 FROM mutation_pins pin
          WHERE pin.owner_id = owner.owner_id
            AND pin.session_owner = owner.session_owner
            AND pin.closed = 0
      )
  )
ORDER BY owner.owner_id, owner.session_owner
LIMIT 1`, c.owner[:]).Scan(&raw, &session)
	if errors.Is(err, sql.ErrNoRows) {
		return RetentionRetirement{}, nil
	}
	if err != nil {
		return RetentionRetirement{}, fmt.Errorf("catalog: select prior retention owner: %w", err)
	}
	if len(raw) != len(HandleOwnerID{}) {
		return RetentionRetirement{}, fmt.Errorf("%w: prior retention owner generation length", ErrIntegrity)
	}
	var generation HandleOwnerID
	copy(generation[:], raw)
	owner, err := NewRetentionOwner(session)
	if err != nil {
		return RetentionRetirement{}, fmt.Errorf("%w: corrupt prior retention owner", ErrIntegrity)
	}
	result, err := c.retireRetentionOwner(ctx, generation, owner)
	if err != nil || result.More {
		return result, err
	}
	var more bool
	if err := c.readDB.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM retention_owners owner
    JOIN catalog_generations generation
      ON generation.owner_id = owner.owner_id AND generation.retired = 1
    WHERE owner.owner_id <> ?
      AND (
          owner.retired = 0
          OR EXISTS (
              SELECT 1 FROM handles handle
              WHERE handle.owner_id = owner.owner_id
                AND handle.session_owner = owner.session_owner
                AND handle.closed = 0
          )
          OR EXISTS (
              SELECT 1 FROM mutation_pins pin
              WHERE pin.owner_id = owner.owner_id
                AND pin.session_owner = owner.session_owner
                AND pin.closed = 0
          )
      )
)`, c.owner[:]).Scan(&more); err != nil {
		return RetentionRetirement{}, fmt.Errorf("catalog: inspect prior retention owners: %w", err)
	}
	result.More = more
	return result, nil
}

type retainedIdentity struct {
	kind   uint8
	id     [16]byte
	tenant TenantID
}

const (
	retainedHandle uint8 = iota + 1
	retainedMutationPin
)

func (c *Catalog) retireRetentionOwner(
	ctx context.Context,
	generation HandleOwnerID,
	owner RetentionOwner,
) (RetentionRetirement, error) {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return RetentionRetirement{}, fmt.Errorf("catalog: begin retention owner retirement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE retention_owners SET retired = 1
WHERE owner_id = ? AND session_owner = ?`,
		generation[:], string(owner))
	if err != nil {
		return RetentionRetirement{}, fmt.Errorf("catalog: fence retention owner: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return RetentionRetirement{}, fmt.Errorf("catalog: inspect retention owner fence: %w", err)
	}
	if changed == 0 {
		if generation != c.owner {
			return RetentionRetirement{}, ErrHandleClosed
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO retention_owners(owner_id, session_owner, retired)
VALUES (?, ?, 1)`, generation[:], string(owner)); err != nil {
			return RetentionRetirement{}, fmt.Errorf("catalog: record empty retired owner: %w", err)
		}
	}
	rows, err := tx.QueryContext(ctx, `
SELECT kind, identity, tenant
FROM (
    SELECT ? AS kind, handle_id AS identity, tenant
    FROM handles
    WHERE owner_id = ? AND session_owner = ? AND closed = 0
    UNION ALL
    SELECT ? AS kind, pin_id AS identity, tenant
    FROM mutation_pins
    WHERE owner_id = ? AND session_owner = ? AND closed = 0
)
ORDER BY kind, identity
LIMIT ?`,
		retainedHandle, generation[:], string(owner),
		retainedMutationPin, generation[:], string(owner),
		RetainedIdentityPageLimit+1)
	if err != nil {
		return RetentionRetirement{}, fmt.Errorf("catalog: select retained identities for retirement: %w", err)
	}
	var identities []retainedIdentity
	for rows.Next() {
		var kind uint8
		var rawID []byte
		var tenant string
		if err := rows.Scan(&kind, &rawID, &tenant); err != nil {
			_ = rows.Close()
			return RetentionRetirement{}, fmt.Errorf("catalog: scan retained identity retirement: %w", err)
		}
		if len(rawID) != len([16]byte{}) {
			_ = rows.Close()
			return RetentionRetirement{}, fmt.Errorf("%w: retained identity length", ErrIntegrity)
		}
		var id [16]byte
		copy(id[:], rawID)
		identities = append(identities, retainedIdentity{kind: kind, id: id, tenant: TenantID(tenant)})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return RetentionRetirement{}, fmt.Errorf("catalog: read retained identity retirement: %w", err)
	}
	if err := rows.Close(); err != nil {
		return RetentionRetirement{}, fmt.Errorf("catalog: close retained identity retirement: %w", err)
	}
	more := len(identities) > RetainedIdentityPageLimit
	if more {
		identities = identities[:RetainedIdentityPageLimit]
	}
	enqueued := make(map[TenantID]struct{})
	for _, identity := range identities {
		var statement string
		switch identity.kind {
		case retainedHandle:
			statement = `
UPDATE handles SET closed = 1
WHERE handle_id = ? AND owner_id = ? AND session_owner = ? AND closed = 0`
		case retainedMutationPin:
			statement = `
UPDATE mutation_pins SET closed = 1
WHERE pin_id = ? AND owner_id = ? AND session_owner = ? AND closed = 0`
		default:
			return RetentionRetirement{}, fmt.Errorf("%w: unknown retained identity kind", ErrIntegrity)
		}
		update, err := tx.ExecContext(ctx, statement, identity.id[:], generation[:], string(owner))
		if err != nil {
			return RetentionRetirement{}, fmt.Errorf("catalog: close retained identity: %w", err)
		}
		rows, err := update.RowsAffected()
		if err != nil {
			return RetentionRetirement{}, fmt.Errorf("catalog: inspect retained identity close: %w", err)
		}
		if rows != 1 {
			return RetentionRetirement{}, fmt.Errorf("%w: retained identity changed during retirement", ErrIntegrity)
		}
		enqueued[identity.tenant] = struct{}{}
	}
	for tenant := range enqueued {
		if err := enqueueCatalogMaintenance(ctx, tx, tenant); err != nil {
			return RetentionRetirement{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return RetentionRetirement{}, fmt.Errorf("catalog: commit retention owner retirement: %w", err)
	}
	return RetentionRetirement{Closed: len(identities), More: more}, nil
}
