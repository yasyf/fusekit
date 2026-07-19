package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

const convergenceStateSchema = 3

// LoadConvergenceState returns the opaque convergence payload owned by this catalog database.
func (c *Catalog) LoadConvergenceState(ctx context.Context) ([]byte, error) {
	var schema int
	var payload []byte
	err := c.readDB.QueryRowContext(ctx, `
SELECT schema_version, payload FROM convergence_state WHERE singleton = 1`).Scan(&schema, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: load convergence state: %w", err)
	}
	if schema != convergenceStateSchema {
		return nil, fmt.Errorf("%w: convergence state schema %d", ErrSchemaMismatch, schema)
	}
	return append([]byte(nil), payload...), nil
}

// SaveConvergenceState atomically replaces the opaque convergence payload on the catalog writer.
func (c *Catalog) SaveConvergenceState(ctx context.Context, payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("%w: convergence state payload is empty", ErrIntegrity)
	}
	if _, err := c.db.ExecContext(ctx, `
INSERT INTO convergence_state(singleton, schema_version, payload) VALUES (1, ?, ?)
ON CONFLICT(singleton) DO UPDATE SET schema_version = excluded.schema_version, payload = excluded.payload`,
		convergenceStateSchema, payload); err != nil {
		return fmt.Errorf("catalog: save convergence state: %w", err)
	}
	return nil
}
