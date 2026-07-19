package holder

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

var receiptRecoveryClasses = [...]proc.RecoveryClass{
	proc.RecoverySourceOwner,
	proc.RecoverySourceDriver,
	proc.RecoveryBroker,
	proc.RecoveryNativeMount,
	proc.RecoveryCatalogWorker,
	proc.RecoveryObserver,
	proc.RecoveryTask,
	proc.RecoveryService,
	proc.RecoveryTrust,
	proc.RecoveryHolder,
}

func recoverSourceOwnerReceipts(
	ctx context.Context,
	registry *durableProcessRegistry,
	store sourceOwnerRecoveryStore,
) error {
	floor, err := registry.RecoverReapReceipts(
		ctx,
		proc.RecoverySourceOwner,
		func(ctx context.Context, receipt proc.ReapReceipt) error {
			if err := verifyReceiptClass(receipt, proc.RecoverySourceOwner, false); err != nil {
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
				if !state.Closed || state.Epoch != fence.Epoch || state.Process != receipt.Record {
					return fmt.Errorf("%w: recovered source owner fence is not exact", catalog.ErrIntegrity)
				}
			}
			return requireNoReceiptLiabilities(
				ctx, registry, proc.RecoverySourceOwner, proc.RecoverySourceDriver, proc.RecoveryHolder,
			)
		},
	)
	if err != nil {
		return fmt.Errorf("holder: recover source-owner receipts: %w", err)
	}
	if floor.Sequence == 0 {
		return nil
	}
	if err := store.AcknowledgeSourceAuthorityRuntimeRecovery(ctx, floor); err != nil {
		return fmt.Errorf("holder: compact acknowledged source-owner receipts: %w", err)
	}
	return nil
}

type sourceOwnerRecoveryStore interface {
	RecoverReapedSourceAuthorityRuntimes(context.Context, proc.ReapReceipt) (catalog.SourceAuthorityRuntimeRecoveryResult, error)
	SourceAuthorityRuntimeStatus(context.Context, catalog.SourceAuthorityRuntimeRef) (catalog.SourceAuthorityRuntimeState, error)
	AcknowledgeSourceAuthorityRuntimeRecovery(context.Context, proc.ReapReceiptFloor) error
}

func recoverBrokerReceipts(
	ctx context.Context,
	registry *durableProcessRegistry,
	store interface {
		RecoverReapedBrokerCommandAttempts(context.Context, catalog.BrokerProcessIdentity) error
	},
) error {
	_, err := registry.RecoverReapReceipts(ctx, proc.RecoveryBroker, func(ctx context.Context, receipt proc.ReapReceipt) error {
		if err := verifyReceiptClass(receipt, proc.RecoveryBroker, true); err != nil {
			return err
		}
		return store.RecoverReapedBrokerCommandAttempts(ctx, catalog.BrokerProcessIdentity{
			PID: receipt.Record.PID, StartTime: receipt.Record.StartTime,
			Boot: receipt.Record.Boot, Generation: receipt.Record.Generation,
		})
	})
	if err != nil {
		return fmt.Errorf("holder: recover broker receipts: %w", err)
	}
	return nil
}

func recoverProcessGroupReceipts(
	ctx context.Context,
	registry *durableProcessRegistry,
	class proc.RecoveryClass,
) error {
	_, err := registry.RecoverReapReceipts(ctx, class, func(_ context.Context, receipt proc.ReapReceipt) error {
		return verifyReceiptClass(receipt, class, true)
	})
	if err != nil {
		return fmt.Errorf("holder: recover %d receipts: %w", class, err)
	}
	return nil
}

func recoverSourceDriverReceipts(
	ctx context.Context,
	registry *durableProcessRegistry,
	store interface {
		PendingSourceDriverReceiptAuthorities(context.Context, causal.SourceAuthorityID, int) (catalog.SourceDriverReceiptAuthorityPage, error)
	},
) error {
	_, err := registry.RecoverReapReceipts(
		ctx,
		proc.RecoverySourceDriver,
		func(ctx context.Context, receipt proc.ReapReceipt) error {
			if err := verifyReceiptClass(receipt, proc.RecoverySourceDriver, true); err != nil {
				return err
			}
			return requireNoSourceDriverCatalogLiabilities(ctx, store)
		},
	)
	if err != nil {
		return fmt.Errorf("holder: recover source-driver receipts: %w", err)
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
		return errors.New("holder: source-driver receipt has no catalog receipt barrier")
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

func recoverHolderReceipts(ctx context.Context, registry *durableProcessRegistry) error {
	_, err := registry.RecoverReapReceipts(ctx, proc.RecoveryHolder, func(ctx context.Context, receipt proc.ReapReceipt) error {
		if err := verifyReceiptClass(receipt, proc.RecoveryHolder, false); err != nil {
			return err
		}
		return requireNoReceiptLiabilities(ctx, registry, proc.RecoveryHolder)
	})
	if err != nil {
		return fmt.Errorf("holder: recover holder receipts: %w", err)
	}
	return nil
}

func requireNoReceiptLiabilities(
	ctx context.Context,
	registry *durableProcessRegistry,
	excluded ...proc.RecoveryClass,
) error {
	for _, class := range receiptRecoveryClasses {
		if slices.Contains(excluded, class) {
			continue
		}
		page, err := registry.ReapReceipts(ctx, class, proc.ReapReceiptCursor{}, 1)
		if err != nil {
			return err
		}
		if len(page.Receipts) != 0 {
			return fmt.Errorf("holder: recovery class %d remains before admission", class)
		}
	}
	return nil
}

func verifyReceiptClass(
	receipt proc.ReapReceipt,
	class proc.RecoveryClass,
	processGroup bool,
) error {
	if err := receipt.Validate(); err != nil {
		return err
	}
	if receipt.Record.RecoveryClass != class || receipt.Record.ProcessGroup != processGroup {
		return fmt.Errorf("holder: receipt recovery class or ownership shape changed")
	}
	if processGroup && receipt.Record.SessionID != receipt.Record.PID {
		return errors.New("holder: receipt process group is not a dedicated session")
	}
	return nil
}
