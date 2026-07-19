package catalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// BrokerProcessIdentity is the exact daemonkit-owned signed broker process.
type BrokerProcessIdentity struct {
	PID        int
	StartTime  string
	Boot       string
	Generation string
}

// BrokerCommandAttemptState is the durable one-way delivery state.
type BrokerCommandAttemptState uint8

const (
	// BrokerCommandPlanned is durable before a command enters the broker stream.
	BrokerCommandPlanned BrokerCommandAttemptState = iota + 1
	// BrokerCommandSent means the command may have reached FileProviderManager.
	BrokerCommandSent
	// BrokerCommandAccepted means the signed broker returned an exact success.
	BrokerCommandAccepted
	// BrokerCommandDeliveryUnknown means the process generation was poisoned.
	BrokerCommandDeliveryUnknown
)

// BrokerCommandAttempt identifies one command and the process generation that
// may execute it.
type BrokerCommandAttempt struct {
	AttemptID     BrokerCommandAttemptID
	CommandID     uint64
	Process       BrokerProcessIdentity
	Kind          string
	PayloadDigest [32]byte
	DomainID      string
	Revision      uint64
	State         BrokerCommandAttemptState
	CreatedAt     time.Time
	SettledAt     time.Time
}

// BeginBrokerCommandAttempt durably plans one command. A signal revision that
// was already attempted is returned instead of being sent again.
func (c *Catalog) BeginBrokerCommandAttempt(
	ctx context.Context,
	attempt BrokerCommandAttempt,
) (BrokerCommandAttempt, bool, error) {
	if err := validateBrokerCommandAttempt(attempt, BrokerCommandPlanned); err != nil {
		return BrokerCommandAttempt{}, false, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return BrokerCommandAttempt{}, false, fmt.Errorf("catalog: begin broker command attempt: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if attempt.Kind == "signal_domain" {
		existing, found, err := readBrokerSignalWatermark(ctx, tx, attempt.DomainID)
		if err != nil {
			return BrokerCommandAttempt{}, false, err
		}
		if found {
			if attempt.Revision < existing.Revision {
				return BrokerCommandAttempt{}, false, fmt.Errorf(
					"%w: broker signal revision %d is below watermark %d",
					ErrInvalidTransition, attempt.Revision, existing.Revision,
				)
			}
			if attempt.Revision == existing.Revision && !sameBrokerSignalRequest(existing, attempt) {
				return BrokerCommandAttempt{}, false, fmt.Errorf("%w: broker signal attempt changed", ErrBrokerAttemptConflict)
			}
			if attempt.Revision == existing.Revision {
				return existing, false, nil
			}
			if existing.State == BrokerCommandPlanned || existing.State == BrokerCommandSent {
				return BrokerCommandAttempt{}, false, fmt.Errorf(
					"%w: broker signal revision %d remains unsettled",
					ErrConflict, existing.Revision,
				)
			}
		}
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
INSERT INTO broker_command_attempts(
    attempt_id, command_id, process_pid, process_start_time, process_boot, process_generation,
    command_kind, payload_digest, domain_id, revision, state, created_unix_nano, settled_unix_nano
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)`,
		attempt.AttemptID[:], attempt.CommandID, attempt.Process.PID, attempt.Process.StartTime, attempt.Process.Boot,
		attempt.Process.Generation, attempt.Kind, attempt.PayloadDigest[:], attempt.DomainID,
		attempt.Revision, uint8(BrokerCommandPlanned), now.UnixNano()); err != nil {
		return BrokerCommandAttempt{}, false, fmt.Errorf("catalog: insert broker command attempt: %w", err)
	}
	if attempt.Kind == "signal_domain" {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO broker_signal_watermarks(domain_id, revision, attempt_id) VALUES (?, ?, ?)
ON CONFLICT(domain_id) DO UPDATE SET revision = excluded.revision, attempt_id = excluded.attempt_id
WHERE excluded.revision > broker_signal_watermarks.revision`,
			attempt.DomainID, attempt.Revision, attempt.AttemptID[:]); err != nil {
			return BrokerCommandAttempt{}, false, fmt.Errorf("catalog: advance broker signal watermark: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_command_attempts
WHERE command_kind = 'signal_domain' AND domain_id = ? AND revision < ?
  AND state IN (?, ?)`,
			attempt.DomainID, attempt.Revision,
			uint8(BrokerCommandAccepted), uint8(BrokerCommandDeliveryUnknown)); err != nil {
			return BrokerCommandAttempt{}, false, fmt.Errorf("catalog: compact broker signal attempts: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return BrokerCommandAttempt{}, false, fmt.Errorf("catalog: commit broker command attempt: %w", err)
	}
	attempt.State = BrokerCommandPlanned
	attempt.CreatedAt = now
	return attempt, true, nil
}

// TransitionBrokerCommandAttempt advances one exact attempt without permitting
// a process generation or payload substitution.
func (c *Catalog) TransitionBrokerCommandAttempt(
	ctx context.Context,
	attempt BrokerCommandAttempt,
	next BrokerCommandAttemptState,
) (BrokerCommandAttempt, error) {
	if err := validateBrokerCommandAttempt(attempt, attempt.State); err != nil {
		return BrokerCommandAttempt{}, err
	}
	if next < BrokerCommandSent || next > BrokerCommandDeliveryUnknown {
		return BrokerCommandAttempt{}, fmt.Errorf("%w: invalid broker command transition", ErrInvalidTransition)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return BrokerCommandAttempt{}, fmt.Errorf("catalog: begin broker command transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := readBrokerCommandAttempt(ctx, tx, attempt.CommandID)
	if err != nil {
		return BrokerCommandAttempt{}, err
	}
	if !sameBrokerCommandAttempt(current, attempt) {
		return BrokerCommandAttempt{}, fmt.Errorf("%w: broker command attempt changed", ErrBrokerAttemptConflict)
	}
	if current.State == next {
		return current, nil
	}
	valid := current.State == BrokerCommandPlanned && next == BrokerCommandSent ||
		current.State == BrokerCommandSent &&
			(next == BrokerCommandAccepted || next == BrokerCommandDeliveryUnknown)
	if !valid {
		return BrokerCommandAttempt{}, fmt.Errorf(
			"%w: broker command state %d to %d", ErrInvalidTransition, current.State, next,
		)
	}
	settled := int64(0)
	if next == BrokerCommandAccepted || next == BrokerCommandDeliveryUnknown {
		settled = time.Now().UTC().UnixNano()
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE broker_command_attempts SET state = ?, settled_unix_nano = ?
WHERE attempt_id = ?`, uint8(next), settled, attempt.AttemptID[:]); err != nil {
		return BrokerCommandAttempt{}, fmt.Errorf("catalog: update broker command attempt: %w", err)
	}
	if settled != 0 && current.Kind == "signal_domain" {
		if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_command_attempts
WHERE attempt_id = ?
  AND NOT EXISTS (
      SELECT 1 FROM broker_signal_watermarks WHERE attempt_id = ?
  )`, attempt.AttemptID[:], attempt.AttemptID[:]); err != nil {
			return BrokerCommandAttempt{}, fmt.Errorf("catalog: compact superseded broker signal attempt: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return BrokerCommandAttempt{}, fmt.Errorf("catalog: commit broker command transition: %w", err)
	}
	current.State = next
	if settled != 0 {
		current.SettledAt = time.Unix(0, settled).UTC()
	}
	return current, nil
}

// RecoverBrokerCommandAttempts runs only after daemonkit has reaped every
// prior-generation broker. Planned commands were never eligible for emission;
// sent commands become permanent unknown deliveries.
func (c *Catalog) RecoverBrokerCommandAttempts(ctx context.Context) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin broker attempt recovery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_signal_watermarks
WHERE attempt_id IN (
    SELECT attempt_id FROM broker_command_attempts WHERE state = ?
)`, uint8(BrokerCommandPlanned)); err != nil {
		return fmt.Errorf("catalog: release planned broker signal watermarks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_command_attempts WHERE state = ?`, uint8(BrokerCommandPlanned)); err != nil {
		return fmt.Errorf("catalog: abandon planned broker commands: %w", err)
	}
	now := time.Now().UTC().UnixNano()
	if _, err := tx.ExecContext(ctx, `
UPDATE broker_command_attempts SET state = ?, settled_unix_nano = ?
WHERE state = ?`, uint8(BrokerCommandDeliveryUnknown), now, uint8(BrokerCommandSent)); err != nil {
		return fmt.Errorf("catalog: poison prior-generation broker commands: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_command_attempts
WHERE command_kind <> 'signal_domain' AND state IN (?, ?)`,
		uint8(BrokerCommandAccepted), uint8(BrokerCommandDeliveryUnknown)); err != nil {
		return fmt.Errorf("catalog: clear recovered non-signal broker commands: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_command_attempts
WHERE command_kind = 'signal_domain' AND state IN (?, ?)
  AND NOT EXISTS (
      SELECT 1 FROM broker_signal_watermarks
      WHERE broker_signal_watermarks.attempt_id = broker_command_attempts.attempt_id
  )`,
		uint8(BrokerCommandAccepted), uint8(BrokerCommandDeliveryUnknown)); err != nil {
		return fmt.Errorf("catalog: clear recovered superseded broker signals: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit broker attempt recovery: %w", err)
	}
	return nil
}

// AbandonBrokerCommandAttempt removes a command proven not to have entered the
// broker stream.
func (c *Catalog) AbandonBrokerCommandAttempt(ctx context.Context, attempt BrokerCommandAttempt) error {
	if err := validateBrokerCommandAttempt(attempt, BrokerCommandPlanned); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin broker command abandonment: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	current, err := readBrokerCommandAttempt(ctx, tx, attempt.CommandID)
	if err != nil {
		return err
	}
	if !sameBrokerCommandAttempt(current, attempt) || current.State != BrokerCommandPlanned {
		return fmt.Errorf("%w: broker command cannot be abandoned", ErrBrokerAttemptConflict)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM broker_command_attempts WHERE attempt_id = ?`, attempt.AttemptID[:]); err != nil {
		return fmt.Errorf("catalog: delete broker command attempt: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit broker command abandonment: %w", err)
	}
	return nil
}

// RecoverReapedBrokerCommandAttempts settles every command owned by one exact
// process only after daemonkit proves that process is gone.
func (c *Catalog) RecoverReapedBrokerCommandAttempts(
	ctx context.Context,
	process BrokerProcessIdentity,
) error {
	if err := validateBrokerProcessIdentity(process); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin reaped broker command recovery: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	processArgs := []any{process.PID, process.StartTime, process.Boot, process.Generation}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_signal_watermarks
WHERE attempt_id IN (
    SELECT attempt_id FROM broker_command_attempts
    WHERE process_pid = ? AND process_start_time = ? AND process_boot = ?
      AND process_generation = ? AND state = ?
)`, append(processArgs, uint8(BrokerCommandPlanned))...); err != nil {
		return fmt.Errorf("catalog: release reaped planned broker signal watermarks: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_command_attempts
WHERE process_pid = ? AND process_start_time = ? AND process_boot = ?
  AND process_generation = ? AND state = ?`,
		append(processArgs, uint8(BrokerCommandPlanned))...); err != nil {
		return fmt.Errorf("catalog: abandon reaped planned broker commands: %w", err)
	}
	now := time.Now().UTC().UnixNano()
	if _, err := tx.ExecContext(ctx, `
UPDATE broker_command_attempts SET state = ?, settled_unix_nano = ?
WHERE process_pid = ? AND process_start_time = ? AND process_boot = ?
  AND process_generation = ? AND state = ?`,
		uint8(BrokerCommandDeliveryUnknown), now,
		process.PID, process.StartTime, process.Boot, process.Generation,
		uint8(BrokerCommandSent)); err != nil {
		return fmt.Errorf("catalog: settle reaped sent broker commands: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_command_attempts
WHERE process_pid = ? AND process_start_time = ? AND process_boot = ?
  AND process_generation = ? AND command_kind <> 'signal_domain'
  AND state IN (?, ?)`,
		process.PID, process.StartTime, process.Boot, process.Generation,
		uint8(BrokerCommandAccepted), uint8(BrokerCommandDeliveryUnknown)); err != nil {
		return fmt.Errorf("catalog: clear reaped broker command attempts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
DELETE FROM broker_command_attempts
WHERE process_pid = ? AND process_start_time = ? AND process_boot = ?
  AND process_generation = ? AND command_kind = 'signal_domain'
  AND state IN (?, ?)
  AND NOT EXISTS (
      SELECT 1 FROM broker_signal_watermarks
      WHERE broker_signal_watermarks.attempt_id = broker_command_attempts.attempt_id
  )`,
		process.PID, process.StartTime, process.Boot, process.Generation,
		uint8(BrokerCommandAccepted), uint8(BrokerCommandDeliveryUnknown)); err != nil {
		return fmt.Errorf("catalog: clear superseded reaped broker signals: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit reaped broker command recovery: %w", err)
	}
	return nil
}

func readBrokerSignalWatermark(
	ctx context.Context,
	tx *sql.Tx,
	domain string,
) (BrokerCommandAttempt, bool, error) {
	attempt, err := scanBrokerCommandAttempt(tx.QueryRowContext(ctx, `
SELECT a.attempt_id, a.command_id, a.process_pid, a.process_start_time, a.process_boot, a.process_generation,
       a.command_kind, a.payload_digest, a.domain_id, a.revision, a.state, a.created_unix_nano, a.settled_unix_nano
FROM broker_signal_watermarks w
JOIN broker_command_attempts a ON a.attempt_id = w.attempt_id
WHERE w.domain_id = ?`, domain))
	if errors.Is(err, sql.ErrNoRows) {
		return BrokerCommandAttempt{}, false, nil
	}
	if err != nil {
		return BrokerCommandAttempt{}, false, err
	}
	return attempt, true, nil
}

func readBrokerCommandAttempt(
	ctx context.Context,
	tx *sql.Tx,
	commandID uint64,
) (BrokerCommandAttempt, error) {
	attempt, err := scanBrokerCommandAttempt(tx.QueryRowContext(ctx, `
SELECT attempt_id, command_id, process_pid, process_start_time, process_boot, process_generation,
       command_kind, payload_digest, domain_id, revision, state, created_unix_nano, settled_unix_nano
FROM broker_command_attempts WHERE command_id = ?`, commandID))
	if errors.Is(err, sql.ErrNoRows) {
		return BrokerCommandAttempt{}, ErrNotFound
	}
	return attempt, err
}

type brokerAttemptScanner interface {
	Scan(...any) error
}

func scanBrokerCommandAttempt(row brokerAttemptScanner) (BrokerCommandAttempt, error) {
	var attempt BrokerCommandAttempt
	var attemptID []byte
	var digest []byte
	var state uint8
	var created, settled int64
	if err := row.Scan(
		&attemptID, &attempt.CommandID, &attempt.Process.PID, &attempt.Process.StartTime, &attempt.Process.Boot,
		&attempt.Process.Generation, &attempt.Kind, &digest, &attempt.DomainID, &attempt.Revision,
		&state, &created, &settled,
	); err != nil {
		return BrokerCommandAttempt{}, err
	}
	if len(attemptID) != len(attempt.AttemptID) {
		return BrokerCommandAttempt{}, fmt.Errorf("%w: broker attempt id length", ErrIntegrity)
	}
	copy(attempt.AttemptID[:], attemptID)
	if len(digest) != len(attempt.PayloadDigest) {
		return BrokerCommandAttempt{}, fmt.Errorf("%w: broker command digest length", ErrIntegrity)
	}
	copy(attempt.PayloadDigest[:], digest)
	attempt.State = BrokerCommandAttemptState(state)
	attempt.CreatedAt = time.Unix(0, created).UTC()
	if settled != 0 {
		attempt.SettledAt = time.Unix(0, settled).UTC()
	}
	if err := validateBrokerCommandAttempt(attempt, attempt.State); err != nil {
		return BrokerCommandAttempt{}, err
	}
	return attempt, nil
}

func validateBrokerCommandAttempt(attempt BrokerCommandAttempt, state BrokerCommandAttemptState) error {
	if attempt.AttemptID == (BrokerCommandAttemptID{}) || attempt.CommandID == 0 ||
		attempt.Kind == "" || attempt.PayloadDigest == ([32]byte{}) {
		return fmt.Errorf("%w: incomplete broker command attempt", ErrInvalidObject)
	}
	if err := validateBrokerProcessIdentity(attempt.Process); err != nil {
		return err
	}
	if state < BrokerCommandPlanned || state > BrokerCommandDeliveryUnknown {
		return fmt.Errorf("%w: invalid broker command state", ErrInvalidObject)
	}
	if attempt.Kind == "signal_domain" {
		if attempt.DomainID == "" || attempt.Revision == 0 {
			return fmt.Errorf("%w: incomplete broker signal identity", ErrInvalidObject)
		}
	} else if attempt.DomainID != "" || attempt.Revision != 0 {
		return fmt.Errorf("%w: non-signal broker command has a signal identity", ErrInvalidObject)
	}
	return nil
}

func validateBrokerProcessIdentity(process BrokerProcessIdentity) error {
	if process.PID <= 1 || process.StartTime == "" || process.Boot == "" || process.Generation == "" {
		return fmt.Errorf("%w: incomplete broker process identity", ErrInvalidObject)
	}
	return nil
}

func sameBrokerCommandAttempt(left, right BrokerCommandAttempt) bool {
	return left.AttemptID == right.AttemptID && left.CommandID == right.CommandID && left.Process == right.Process &&
		left.Kind == right.Kind && bytes.Equal(left.PayloadDigest[:], right.PayloadDigest[:]) &&
		left.DomainID == right.DomainID && left.Revision == right.Revision
}

func sameBrokerSignalRequest(left, right BrokerCommandAttempt) bool {
	return left.Kind == right.Kind && bytes.Equal(left.PayloadDigest[:], right.PayloadDigest[:]) &&
		left.DomainID == right.DomainID && left.Revision == right.Revision
}
