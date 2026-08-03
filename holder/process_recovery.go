package holder

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

var receiptRecoveryIDs = [...]recoveryid.ID{
	recoveryid.SourceOwner,
	recoveryid.SourceDriver,
	recoveryid.Broker,
	recoveryid.NativeMount,
	recoveryid.CatalogWorker,
	recoveryid.SourceObserver,
	recoveryid.SourceTask,
	recoveryid.Holder,
}

func recoverSourceOwnerReceipts(
	ctx context.Context,
	ledger *processLedger,
	store sourceOwnerRecoveryStore,
) error {
	floor, err := ledger.Recover(
		ctx,
		recoveryid.SourceOwner,
		func(ctx context.Context, receipt catalog.ReapReceipt) error {
			if err := verifyReceiptID(receipt, recoveryid.SourceOwner, false); err != nil {
				return err
			}
			result, err := store.RecoverReapedSourceAuthorityRuntimes(ctx, receipt)
			if err != nil {
				return err
			}
			if err := result.Validate(receipt); err != nil {
				return err
			}
			for _, fence := range result.Closed {
				state, err := store.SourceAuthorityRuntimeStatus(ctx, catalog.SourceAuthorityRuntimeRef{
					Owner: fence.Owner, Generation: fence.Generation, Authority: fence.Authority,
				})
				if err != nil {
					return err
				}
				if !state.Closed || state.Epoch != fence.Epoch ||
					state.Process == nil || *state.Process != receipt.Record {
					return fmt.Errorf("%w: recovered source owner fence is not exact", catalog.ErrIntegrity)
				}
			}
			return requireNoReceiptLiabilities(
				ctx, ledger, recoveryid.SourceOwner, recoveryid.SourceDriver, recoveryid.Holder,
			)
		},
	)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: recover source-owner receipts: %w", err)
	}
	if floor.Sequence == 0 {
		return nil
	}
	if err := store.AcknowledgeSourceAuthorityRuntimeRecovery(ctx, floor); err != nil {
		return fmt.Errorf("FuseKit runtime: compact acknowledged source-owner receipts: %w", err)
	}
	return nil
}

type sourceOwnerRecoveryStore interface {
	RecoverReapedSourceAuthorityRuntimes(context.Context, catalog.ReapReceipt) (catalog.SourceAuthorityRuntimeRecoveryResult, error)
	SourceAuthorityRuntimeStatus(context.Context, catalog.SourceAuthorityRuntimeRef) (catalog.SourceAuthorityRuntimeState, error)
	AcknowledgeSourceAuthorityRuntimeRecovery(context.Context, catalog.ReapReceiptFloor) error
}

func recoverBrokerReceipts(
	ctx context.Context,
	ledger *processLedger,
	store interface {
		RecoverReapedBrokerCommandAttempts(context.Context, catalog.BrokerProcessIdentity) error
	},
) error {
	_, err := ledger.Recover(ctx, recoveryid.Broker, func(ctx context.Context, receipt catalog.ReapReceipt) error {
		if err := verifyReceiptID(receipt, recoveryid.Broker, true); err != nil {
			return err
		}
		return store.RecoverReapedBrokerCommandAttempts(ctx, catalog.BrokerProcessIdentity{
			PID: receipt.Record.PID, StartTime: receipt.Record.StartTime,
			Boot: receipt.Record.Boot, Generation: receipt.Record.Generation.String(),
		})
	})
	if err != nil {
		return fmt.Errorf("FuseKit runtime: recover broker receipts: %w", err)
	}
	return nil
}

func recoverProcessGroupReceipts(
	ctx context.Context,
	ledger *processLedger,
	id recoveryid.ID,
) error {
	_, err := ledger.Recover(ctx, id, func(_ context.Context, receipt catalog.ReapReceipt) error {
		return verifyReceiptID(receipt, id, true)
	})
	if err != nil {
		return fmt.Errorf("FuseKit runtime: recover %q receipts: %w", id, err)
	}
	return nil
}

func recoverSourceDriverReceipts(
	ctx context.Context,
	ledger *processLedger,
	store interface {
		PendingSourceDriverReceiptAuthorities(context.Context, causal.SourceAuthorityID, int) (catalog.SourceDriverReceiptAuthorityPage, error)
	},
) error {
	_, err := ledger.Recover(
		ctx,
		recoveryid.SourceDriver,
		func(ctx context.Context, receipt catalog.ReapReceipt) error {
			if err := verifyReceiptID(receipt, recoveryid.SourceDriver, true); err != nil {
				return err
			}
			return requireNoSourceDriverCatalogLiabilities(ctx, store)
		},
	)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: recover source-driver receipts: %w", err)
	}
	return nil
}

func requireNoSourceDriverCatalogLiabilities(
	ctx context.Context,
	store interface {
		PendingSourceDriverReceiptAuthorities(context.Context, causal.SourceAuthorityID, int) (catalog.SourceDriverReceiptAuthorityPage, error)
	},
) error {
	if store == nil {
		return errors.New("FuseKit runtime: source-driver receipt has no catalog receipt barrier")
	}
	page, err := store.PendingSourceDriverReceiptAuthorities(ctx, "", 1)
	if err != nil {
		return err
	}
	if len(page.Authorities) != 0 {
		return fmt.Errorf(
			"%w: source-driver catalog receipt remains unsettled for %q",
			catalog.ErrIntegrity, page.Authorities[0],
		)
	}
	return nil
}

func recoverHolderReceipts(ctx context.Context, ledger *processLedger) error {
	_, err := ledger.Recover(ctx, recoveryid.Holder, func(ctx context.Context, receipt catalog.ReapReceipt) error {
		if err := verifyReceiptID(receipt, recoveryid.Holder, false); err != nil {
			return err
		}
		return requireNoReceiptLiabilities(ctx, ledger, recoveryid.Holder)
	})
	if err != nil {
		return fmt.Errorf("FuseKit runtime: recover runtime-owner receipts: %w", err)
	}
	return nil
}

func requireNoReceiptLiabilities(
	_ context.Context,
	ledger *processLedger,
	excluded ...recoveryid.ID,
) error {
	for _, id := range receiptRecoveryIDs {
		if slices.Contains(excluded, id) {
			continue
		}
		if receipts := ledger.Receipts(id, 1); len(receipts) != 0 {
			return fmt.Errorf("FuseKit runtime: recovery ID %q remains before admission", id)
		}
	}
	return nil
}

func verifyReceiptID(
	receipt catalog.ReapReceipt,
	id recoveryid.ID,
	processGroup bool,
) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	if receipt.Record.RecoveryID != id || receipt.Record.ProcessGroup != processGroup {
		return fmt.Errorf("FuseKit runtime: receipt recovery ID or ownership shape changed")
	}
	if processGroup && receipt.Record.SessionID != receipt.Record.PID {
		return errors.New("FuseKit runtime: receipt process group is not a dedicated session")
	}
	return nil
}
