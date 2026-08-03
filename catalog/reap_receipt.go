package catalog

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/internal/recoveryid"
)

var (
	// ErrReapReceiptOrder means an acknowledgement skipped a receipt sequence.
	ErrReapReceiptOrder = errors.New("catalog: reap receipt acknowledgement is out of order")
	// ErrReapReceiptStale means a receipt predates the retained acknowledged floor.
	ErrReapReceiptStale = errors.New("catalog: stale reap receipt")
)

// ReceiptLedgerID identifies one durable receipt ledger across process restarts.
type ReceiptLedgerID [16]byte

// ReapReceiptFloor is the highest contiguously acknowledged recovery-ID sequence.
type ReapReceiptFloor struct {
	LedgerID   ReceiptLedgerID
	RecoveryID recoveryid.ID
	Sequence   uint64
}

// ReapOutcome records how the exact prior process identity became retired.
type ReapOutcome uint8

const (
	// ReapCrossBoot proves the recorded boot is no longer current.
	ReapCrossBoot ReapOutcome = iota + 1
	// ReapAbsent proves the exact recorded process was already absent.
	ReapAbsent
	// ReapIdentityReused proves the PID now names a different process instance.
	ReapIdentityReused
	// ReapTerminated proves the identity-gated TERM/KILL ladder settled.
	ReapTerminated
)

// ReapReceipt is the durable exact proof for one retired process generation.
// Digest covers the complete ProcessRecord and Outcome; no wall-clock field can
// make replay produce different bytes.
type ReapReceipt struct {
	LedgerID         ReceiptLedgerID   `json:"ledger_id"`
	Sequence         uint64            `json:"sequence"`
	Record           ProcessRecord     `json:"record"`
	ReaperGeneration ProcessGeneration `json:"reaper_generation"`
	Outcome          ReapOutcome       `json:"outcome"`
	Digest           [32]byte          `json:"digest"`
}

// NewReapReceipt seals one retirement proof at its exact ledger sequence.
func NewReapReceipt(
	ledgerID ReceiptLedgerID,
	sequence uint64,
	record ProcessRecord,
	reaperGeneration ProcessGeneration,
	outcome ReapOutcome,
) (ReapReceipt, error) {
	if err := record.Validate(); err != nil {
		return ReapReceipt{}, err
	}
	digest, err := reapReceiptDigest(ledgerID, sequence, record, reaperGeneration, outcome)
	if err != nil {
		return ReapReceipt{}, err
	}
	receipt := ReapReceipt{
		LedgerID: ledgerID, Sequence: sequence,
		Record: record, ReaperGeneration: reaperGeneration,
		Outcome: outcome, Digest: digest,
	}
	if err := receipt.Validate(); err != nil {
		return ReapReceipt{}, err
	}
	return receipt, nil
}

// Validate requires the exact canonical digest of a valid process record and
// typed retirement outcome.
func (r ReapReceipt) Validate() error {
	if r.LedgerID == (ReceiptLedgerID{}) || r.Sequence == 0 {
		return fmt.Errorf("%w: reap receipt ledger identity and sequence are required", ErrInvalidObject)
	}
	if err := r.Record.Validate(); err != nil {
		return err
	}
	if r.ReaperGeneration == (ProcessGeneration{}) || r.ReaperGeneration == r.Record.Generation {
		return fmt.Errorf("%w: reap receipt successor generation is invalid", ErrInvalidObject)
	}
	switch r.Outcome {
	case ReapCrossBoot, ReapAbsent, ReapIdentityReused, ReapTerminated:
	default:
		return fmt.Errorf("%w: unknown reap outcome %d", ErrInvalidObject, r.Outcome)
	}
	digest, err := reapReceiptDigest(
		r.LedgerID, r.Sequence, r.Record, r.ReaperGeneration, r.Outcome,
	)
	if err != nil {
		return err
	}
	if r.Digest != digest {
		return fmt.Errorf("%w: reap receipt digest mismatch", ErrInvalidObject)
	}
	return nil
}

func reapReceiptDigest(
	ledgerID ReceiptLedgerID,
	sequence uint64,
	record ProcessRecord,
	reaperGeneration ProcessGeneration,
	outcome ReapOutcome,
) ([32]byte, error) {
	payload, err := json.Marshal(struct {
		LedgerID         ReceiptLedgerID   `json:"ledger_id"`
		Sequence         uint64            `json:"sequence"`
		Record           ProcessRecord     `json:"record"`
		ReaperGeneration ProcessGeneration `json:"reaper_generation"`
		Outcome          ReapOutcome       `json:"outcome"`
	}{
		LedgerID: ledgerID, Sequence: sequence, Record: record,
		ReaperGeneration: reaperGeneration, Outcome: outcome,
	})
	if err != nil {
		return [32]byte{}, fmt.Errorf("catalog: encode reap receipt: %w", err)
	}
	return sha256.Sum256(payload), nil
}
