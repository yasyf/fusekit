package catalog

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

func transitionSourceDriverTargetEpoch(
	ctx context.Context,
	tx *sql.Tx,
	previous string,
	next string,
) error {
	if err := causal.ValidateSourceAuthorityID(causal.SourceAuthorityID(next)); err != nil {
		return fmt.Errorf("%w: invalid source driver target authority", ErrInvalidObject)
	}
	if previous == "" {
		return addSourceDriverTargetEpoch(ctx, tx, next)
	}
	if err := causal.ValidateSourceAuthorityID(causal.SourceAuthorityID(previous)); err != nil {
		return ErrIntegrity
	}
	if previous == next {
		return retireSourceDriverTargetEpoch(ctx, tx, previous)
	}
	if err := retireSourceDriverTargetEpoch(ctx, tx, previous); err != nil {
		return err
	}
	return addSourceDriverTargetEpoch(ctx, tx, next)
}

func addSourceDriverTargetEpoch(ctx context.Context, tx *sql.Tx, authority string) error {
	_, err := tx.ExecContext(ctx, `
INSERT INTO source_driver_target_epochs(source_authority, target_epoch)
VALUES (?, 1)
ON CONFLICT(source_authority) DO UPDATE SET target_epoch = target_epoch + 1`, authority)
	if err != nil {
		return fmt.Errorf("catalog: advance source driver target epoch: %w", mapConstraint(err))
	}
	return nil
}

func retireSourceDriverTargetEpoch(ctx context.Context, tx *sql.Tx, authority string) error {
	result, err := tx.ExecContext(ctx, `
UPDATE source_driver_target_epochs SET target_epoch = target_epoch + 1
WHERE source_authority = ?`, authority)
	if err != nil {
		return fmt.Errorf("catalog: retire source driver target epoch: %w", err)
	}
	if changed, err := result.RowsAffected(); err != nil {
		return err
	} else if changed != 1 {
		return ErrIntegrity
	}
	return nil
}
