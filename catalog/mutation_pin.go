package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// MutationPin durably retains exact committed replay state for one live
// native mutable handle.
type MutationPin struct {
	ID       MutationPinID
	Owner    RetentionOwner
	Tenant   TenantID
	Mutation MutationID
	Target   Revision
}

// PinMutation retains one exact mutation until the returned pin is closed.
func (c *Catalog) PinMutation(
	ctx context.Context,
	owner RetentionOwner,
	tenant TenantID,
	mutation MutationID,
) (MutationPin, error) {
	if _, err := NewRetentionOwner(string(owner)); err != nil {
		return MutationPin{}, err
	}
	target := mutation.TargetRevision()
	if target == 0 {
		return MutationPin{}, fmt.Errorf("%w: mutation pin target is zero", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return MutationPin{}, fmt.Errorf("catalog: begin mutation pin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := ensureRetentionOwner(ctx, tx, c.owner, owner); err != nil {
		return MutationPin{}, err
	}
	record, found, err := mutationRecord(ctx, tx, mutation)
	if err != nil {
		return MutationPin{}, err
	}
	if found {
		if record.Tenant != tenant || record.Revision != target {
			return MutationPin{}, ErrMutationConflict
		}
	} else {
		prepared, preparedFound, preparedErr := readPreparedMutation(ctx, tx, mutation)
		if preparedErr != nil {
			return MutationPin{}, preparedErr
		}
		if !preparedFound {
			if err := rejectExpiredMutation(ctx, tx, tenant, mutation); err != nil {
				return MutationPin{}, err
			}
			return MutationPin{}, ErrNotFound
		}
		if prepared.Tenant != tenant || prepared.ExpectedHead+1 != target {
			return MutationPin{}, ErrMutationConflict
		}
	}
	var raw []byte
	err = tx.QueryRowContext(ctx, `
SELECT pin_id FROM mutation_pins
WHERE owner_id = ? AND session_owner = ? AND mutation_id = ? AND closed = 0`,
		c.owner[:], string(owner), mutation[:]).Scan(&raw)
	if err == nil {
		if len(raw) != len(MutationPinID{}) {
			return MutationPin{}, fmt.Errorf("%w: mutation pin id length", ErrIntegrity)
		}
		var id MutationPinID
		copy(id[:], raw)
		if err := tx.Commit(); err != nil {
			return MutationPin{}, fmt.Errorf("catalog: finish mutation pin replay: %w", err)
		}
		return MutationPin{
			ID: id, Owner: owner, Tenant: tenant, Mutation: mutation, Target: target,
		}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return MutationPin{}, fmt.Errorf("catalog: inspect mutation pin replay: %w", err)
	}
	id, err := NewMutationPinID()
	if err != nil {
		return MutationPin{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO mutation_pins(
    pin_id, owner_id, session_owner, tenant, mutation_id, target_revision, closed
) VALUES (?, ?, ?, ?, ?, ?, 0)`,
		id[:], c.owner[:], string(owner), string(tenant), mutation[:], uint64(target)); err != nil {
		return MutationPin{}, mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return MutationPin{}, fmt.Errorf("catalog: commit mutation pin: %w", err)
	}
	return MutationPin{
		ID: id, Owner: owner, Tenant: tenant, Mutation: mutation, Target: target,
	}, nil
}

// CloseMutationPin idempotently releases one exact replay pin.
func (c *Catalog) CloseMutationPin(ctx context.Context, pin MutationPin) error {
	if pin.ID == (MutationPinID{}) || pin.Owner == "" || pin.Tenant == "" ||
		pin.Mutation == (MutationID{}) || pin.Target != pin.Mutation.TargetRevision() {
		return fmt.Errorf("%w: incomplete mutation pin", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin mutation pin close: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
UPDATE mutation_pins SET closed = 1
WHERE pin_id = ? AND owner_id = ? AND session_owner = ?
  AND tenant = ? AND mutation_id = ?
  AND target_revision = ? AND closed = 0`,
		pin.ID[:], c.owner[:], string(pin.Owner),
		string(pin.Tenant), pin.Mutation[:], uint64(pin.Target))
	if err != nil {
		return fmt.Errorf("catalog: close mutation pin: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect mutation pin close: %w", err)
	}
	if changed == 1 {
		if err := enqueueCatalogMaintenance(ctx, tx, pin.Tenant); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("catalog: commit mutation pin close: %w", err)
		}
		return nil
	}
	var closed int
	if err := tx.QueryRowContext(ctx, `
SELECT closed FROM mutation_pins
WHERE pin_id = ? AND owner_id = ? AND session_owner = ?
  AND tenant = ? AND mutation_id = ? AND target_revision = ?`,
		pin.ID[:], c.owner[:], string(pin.Owner),
		string(pin.Tenant), pin.Mutation[:], uint64(pin.Target)).Scan(&closed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("catalog: read mutation pin close replay: %w", err)
	}
	if closed != 1 {
		return ErrConflict
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit mutation pin close replay: %w", err)
	}
	return nil
}

// ForgetMutationPin removes one acknowledged closed-pin receipt. Missing
// receipts are an idempotent success for the exact still-live owner.
func (c *Catalog) ForgetMutationPin(ctx context.Context, pin MutationPin) error {
	if pin.ID == (MutationPinID{}) || pin.Owner == "" || pin.Tenant == "" ||
		pin.Mutation == (MutationID{}) || pin.Target != pin.Mutation.TargetRevision() {
		return fmt.Errorf("%w: incomplete mutation pin", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin mutation pin forget: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := requireLiveRetentionOwner(ctx, tx, c.owner, pin.Owner); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM mutation_pins
WHERE pin_id = ? AND owner_id = ? AND session_owner = ?
  AND tenant = ? AND mutation_id = ? AND target_revision = ? AND closed = 1`,
		pin.ID[:], c.owner[:], string(pin.Owner),
		string(pin.Tenant), pin.Mutation[:], uint64(pin.Target))
	if err != nil {
		return fmt.Errorf("catalog: forget mutation pin receipt: %w", err)
	}
	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("catalog: inspect mutation pin forget: %w", err)
	}
	if err := enqueueCatalogMaintenance(ctx, tx, pin.Tenant); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit mutation pin forget: %w", err)
	}
	return nil
}
