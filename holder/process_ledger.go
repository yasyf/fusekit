package holder

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"sync"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/durable"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

type processLedgerState struct {
	LedgerID  catalog.ReceiptLedgerID  `json:"ledger_id"`
	Sequence  uint64                   `json:"sequence"`
	Sequences map[recoveryid.ID]uint64 `json:"sequences,omitempty"`
	Records   []catalog.ProcessRecord  `json:"records"`
	Receipts  []catalog.ReapReceipt    `json:"receipts"`
}

func (s processLedgerState) Validate() error {
	if s.LedgerID == (catalog.ReceiptLedgerID{}) {
		return errors.New("FuseKit runtime: process ledger id is zero")
	}
	for _, record := range s.Records {
		if err := record.Validate(); err != nil {
			return err
		}
	}
	for _, receipt := range s.Receipts {
		if err := receipt.Validate(); err != nil {
			return err
		}
		if receipt.LedgerID != s.LedgerID {
			return errors.New("FuseKit runtime: process ledger receipt names a foreign ledger")
		}
		if receipt.Sequence > s.issued(receipt.Record.RecoveryID) {
			return errors.New("FuseKit runtime: process ledger receipt outruns the sequence")
		}
	}
	return nil
}

func (s processLedgerState) issued(id recoveryid.ID) uint64 {
	if s.Sequences == nil {
		return s.Sequence
	}
	return s.Sequences[id]
}

// processLedger is the holder-owned durable spawn ledger: the record identity
// half of the withdrawn v0.20 receipt model. Spawn tracks a
// catalog.ProcessRecord per child, reclaim joins prior-generation records
// against daemonkit's settled proof to synthesize catalog.ReapReceipts, and
// the recovery flows consume those receipts exactly as before.
type processLedger struct {
	path       string
	generation catalog.ProcessGeneration

	mu    sync.Mutex
	state processLedgerState
}

func openProcessLedger(path string) (*processLedger, error) {
	var generation catalog.ProcessGeneration
	if _, err := rand.Read(generation[:]); err != nil {
		return nil, fmt.Errorf("FuseKit runtime: mint process generation: %w", err)
	}
	if generation == (catalog.ProcessGeneration{}) {
		return nil, errors.New("FuseKit runtime: process generation is zero")
	}
	ledger := &processLedger{path: path, generation: generation}
	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := ledger.mint(); err != nil {
			return nil, err
		}
	case err != nil:
		return nil, fmt.Errorf("FuseKit runtime: read process ledger: %w", err)
	default:
		state, decodeErr := durable.Unmarshal[processLedgerState](data)
		if decodeErr != nil {
			// A v0.20 proc.FileStore file has no v0.21 successor schema: archive
			// it aside on first open, the same doctrine as the deploy tree.
			if err := durable.Rename(path, path+".v20"); err != nil {
				return nil, fmt.Errorf("FuseKit runtime: archive pre-v0.21 process store: %w", err)
			}
			if err := ledger.mint(); err != nil {
				return nil, err
			}
			return ledger, nil
		}
		ledger.state = state
		if ledger.seedLegacySequences() {
			if err := ledger.persistLocked(); err != nil {
				return nil, err
			}
		}
	}
	return ledger, nil
}

// seedLegacySequences carries a pre-per-recovery-ID ledger onto the current
// numbering. Every recovery ID starts at the retired global high-water, which
// outruns every sequence the shared counter ever issued, so a receipt minted
// here can never collide with one a catalog floor already settled.
func (l *processLedger) seedLegacySequences() bool {
	if l.state.Sequences != nil {
		return false
	}
	l.state.Sequences = make(map[recoveryid.ID]uint64, len(receiptRecoveryIDs))
	for _, id := range receiptRecoveryIDs {
		l.state.Sequences[id] = l.state.Sequence
	}
	return true
}

func (l *processLedger) mint() error {
	var id catalog.ReceiptLedgerID
	if _, err := rand.Read(id[:]); err != nil {
		return fmt.Errorf("FuseKit runtime: mint process ledger id: %w", err)
	}
	if id == (catalog.ReceiptLedgerID{}) {
		return errors.New("FuseKit runtime: process ledger id is zero")
	}
	l.state = processLedgerState{LedgerID: id}
	return l.persistLocked()
}

func (l *processLedger) persistLocked() error {
	data, err := durable.Marshal(l.state)
	if err != nil {
		return fmt.Errorf("FuseKit runtime: encode process ledger: %w", err)
	}
	if err := durable.WriteFile(l.path, data, 0o600); err != nil {
		return fmt.Errorf("FuseKit runtime: publish process ledger: %w", err)
	}
	return nil
}

func (l *processLedger) Generation() catalog.ProcessGeneration { return l.generation }

// Reclaim joins every prior-generation record against daemonkit's settled
// children and the live process table, synthesizing one durable reap receipt
// per retired record. A prior record still running is an integrity failure.
func (l *processLedger) Reclaim(reclaimed []daemonkit.Reclaimed) error {
	settled := make(map[int]struct{}, len(reclaimed))
	for _, child := range reclaimed {
		settled[child.PID] = struct{}{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var live []catalog.ProcessRecord
	var receipts []catalog.ReapReceipt
	sequence := l.state.Sequence
	sequences := maps.Clone(l.state.Sequences)
	if sequences == nil {
		sequences = make(map[recoveryid.ID]uint64, len(receiptRecoveryIDs))
	}
	for _, record := range l.state.Records {
		if record.Generation == l.generation {
			live = append(live, record)
			continue
		}
		outcome := catalog.ReapTerminated
		if _, ok := settled[record.PID]; !ok {
			classified, err := classifyRecordedProcess(record)
			if err != nil {
				return fmt.Errorf("FuseKit runtime: classify recorded process %d: %w", record.PID, err)
			}
			outcome = classified
		}
		receipt, err := catalog.NewReapReceipt(
			l.state.LedgerID, sequences[record.RecoveryID]+1, record, l.generation, outcome,
		)
		if err != nil {
			return fmt.Errorf("FuseKit runtime: seal reap receipt for pid %d: %w", record.PID, err)
		}
		sequences[record.RecoveryID]++
		sequence = max(sequence, sequences[record.RecoveryID])
		receipts = append(receipts, receipt)
	}
	if len(receipts) == 0 {
		return nil
	}
	l.state.Records = live
	l.state.Sequence = sequence
	l.state.Sequences = sequences
	l.state.Receipts = append(l.state.Receipts, receipts...)
	return l.persistLocked()
}

func (l *processLedger) Track(record catalog.ProcessRecord) error {
	if err := record.Validate(); err != nil {
		return fmt.Errorf("FuseKit runtime: validate tracked process record: %w", err)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state.Records = append(l.state.Records, record)
	if err := l.persistLocked(); err != nil {
		l.state.Records = l.state.Records[:len(l.state.Records)-1]
		return err
	}
	return nil
}

func (l *processLedger) Untrack(record catalog.ProcessRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	index := slices.Index(l.state.Records, record)
	if index < 0 {
		return errors.New("FuseKit runtime: untracked process record is not in the ledger")
	}
	l.state.Records = slices.Delete(l.state.Records, index, index+1)
	if err := l.persistLocked(); err != nil {
		l.state.Records = slices.Insert(l.state.Records, index, record)
		return err
	}
	return nil
}

func (l *processLedger) RegisterOwner(id recoveryid.ID) (catalog.ProcessRecord, error) {
	record, err := captureCurrentProcessRecord(id, l.generation)
	if err != nil {
		return catalog.ProcessRecord{}, err
	}
	if err := l.Track(record); err != nil {
		return catalog.ProcessRecord{}, err
	}
	return record, nil
}

// Receipts returns up to limit unsettled receipts for one recovery barrier in
// sequence order.
func (l *processLedger) Receipts(id recoveryid.ID, limit int) []catalog.ReapReceipt {
	l.mu.Lock()
	defer l.mu.Unlock()
	var result []catalog.ReapReceipt
	for _, receipt := range l.state.Receipts {
		if receipt.Record.RecoveryID != id {
			continue
		}
		result = append(result, receipt)
		if limit > 0 && len(result) == limit {
			break
		}
	}
	return result
}

// Recover feeds every unsettled receipt for one recovery barrier through fn in
// sequence order, then durably drops the settled receipts and returns the
// acknowledged floor. A floor with Sequence zero means nothing was pending.
func (l *processLedger) Recover(
	ctx context.Context,
	id recoveryid.ID,
	fn func(context.Context, catalog.ReapReceipt) error,
) (catalog.ReapReceiptFloor, error) {
	l.mu.Lock()
	pending := make([]catalog.ReapReceipt, 0)
	for _, receipt := range l.state.Receipts {
		if receipt.Record.RecoveryID == id {
			pending = append(pending, receipt)
		}
	}
	floor := catalog.ReapReceiptFloor{LedgerID: l.state.LedgerID, RecoveryID: id}
	l.mu.Unlock()
	for _, receipt := range pending {
		if err := fn(ctx, receipt); err != nil {
			return catalog.ReapReceiptFloor{}, err
		}
		floor.Sequence = receipt.Sequence
	}
	if len(pending) == 0 {
		return floor, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state.Receipts = slices.DeleteFunc(l.state.Receipts, func(receipt catalog.ReapReceipt) bool {
		return receipt.Record.RecoveryID == id && receipt.Sequence <= floor.Sequence
	})
	if err := l.persistLocked(); err != nil {
		return catalog.ReapReceiptFloor{}, err
	}
	return floor, nil
}
