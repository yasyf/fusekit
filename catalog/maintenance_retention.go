package catalog

import (
	"context"
	"fmt"
	"time"
)

// BrokerAttemptCompactionPageLimit is the hard maximum accepted broker
// attempts retired by one compaction call.
const BrokerAttemptCompactionPageLimit = 256

// FileProviderLeaseCompactionPageLimit is the hard maximum expired leases
// retired by one compaction call.
const FileProviderLeaseCompactionPageLimit = 256

// BrokerAttemptCompactionResult reports one bounded broker-attempt page.
type BrokerAttemptCompactionResult struct {
	Attempts int
	More     bool
}

// FileProviderLeaseCompactionResult reports one bounded expired-lease page.
type FileProviderLeaseCompactionResult struct {
	Leases int
	More   bool
}

// CompactBrokerCommandAttempts retires accepted non-signal attempts while
// retaining the newest exact attempt for every live process generation.
func (c *Catalog) CompactBrokerCommandAttempts(
	ctx context.Context,
	limit int,
) (BrokerAttemptCompactionResult, error) {
	if limit < 1 || limit > BrokerAttemptCompactionPageLimit {
		return BrokerAttemptCompactionResult{}, fmt.Errorf(
			"%w: broker attempt compaction limit is outside [1,%d]",
			ErrInvalidObject, BrokerAttemptCompactionPageLimit,
		)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return BrokerAttemptCompactionResult{}, fmt.Errorf("catalog: begin broker attempt compaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT old.attempt_id
FROM broker_command_attempts old
WHERE old.command_kind <> 'signal_domain' AND old.state = ?
  AND EXISTS (
      SELECT 1
      FROM broker_command_attempts fence
      WHERE fence.process_pid = old.process_pid
        AND fence.process_start_time = old.process_start_time
        AND fence.process_boot = old.process_boot
        AND fence.process_generation = old.process_generation
        AND fence.command_kind <> 'signal_domain'
        AND fence.state = ?
        AND fence.command_id > old.command_id
  )
ORDER BY old.process_pid, old.process_start_time, old.process_boot,
         old.process_generation, old.command_id, old.attempt_id
LIMIT ?`,
		uint8(BrokerCommandAccepted), uint8(BrokerCommandAccepted), limit+1)
	if err != nil {
		return BrokerAttemptCompactionResult{}, fmt.Errorf("catalog: select compactable broker attempts: %w", err)
	}
	var attempts [][]byte
	for rows.Next() {
		var attempt []byte
		if err := rows.Scan(&attempt); err != nil {
			_ = rows.Close()
			return BrokerAttemptCompactionResult{}, fmt.Errorf("catalog: scan compactable broker attempt: %w", err)
		}
		attempts = append(attempts, append([]byte(nil), attempt...))
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return BrokerAttemptCompactionResult{}, fmt.Errorf("catalog: read compactable broker attempts: %w", err)
	}
	if err := rows.Close(); err != nil {
		return BrokerAttemptCompactionResult{}, fmt.Errorf("catalog: close compactable broker attempts: %w", err)
	}
	more := len(attempts) > limit
	if more {
		attempts = attempts[:limit]
	}
	for _, attempt := range attempts {
		result, err := tx.ExecContext(ctx, `
DELETE FROM broker_command_attempts
WHERE attempt_id = ? AND command_kind <> 'signal_domain' AND state = ?
  AND EXISTS (
      SELECT 1
      FROM broker_command_attempts fence
      WHERE fence.process_pid = broker_command_attempts.process_pid
        AND fence.process_start_time = broker_command_attempts.process_start_time
        AND fence.process_boot = broker_command_attempts.process_boot
        AND fence.process_generation = broker_command_attempts.process_generation
        AND fence.command_kind <> 'signal_domain'
        AND fence.state = ?
        AND fence.command_id > broker_command_attempts.command_id
  )`, attempt, uint8(BrokerCommandAccepted), uint8(BrokerCommandAccepted))
		if err != nil {
			return BrokerAttemptCompactionResult{}, fmt.Errorf("catalog: compact broker attempt: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return BrokerAttemptCompactionResult{}, fmt.Errorf("catalog: inspect compacted broker attempt: %w", err)
		}
		if changed != 1 {
			return BrokerAttemptCompactionResult{}, fmt.Errorf("%w: compactable broker attempt changed", ErrIntegrity)
		}
	}
	if err := tx.Commit(); err != nil {
		return BrokerAttemptCompactionResult{}, fmt.Errorf("catalog: commit broker attempt compaction: %w", err)
	}
	return BrokerAttemptCompactionResult{Attempts: len(attempts), More: more}, nil
}

// CompactExpiredFileProviderLeases retires one exact bounded expiry page.
func (c *Catalog) CompactExpiredFileProviderLeases(
	ctx context.Context,
	now time.Time,
	limit int,
) (FileProviderLeaseCompactionResult, error) {
	if now.IsZero() || now.UnixNano() <= 0 || limit < 1 || limit > FileProviderLeaseCompactionPageLimit {
		return FileProviderLeaseCompactionResult{}, fmt.Errorf(
			"%w: File Provider lease compaction bounds are invalid", ErrInvalidObject,
		)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return FileProviderLeaseCompactionResult{}, fmt.Errorf("catalog: begin File Provider lease compaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT lease_id, tenant, domain_id, generation, expires_unix_nano
FROM file_provider_leases
WHERE expires_unix_nano <= ?
ORDER BY expires_unix_nano, lease_id
LIMIT ?`, now.UnixNano(), limit+1)
	if err != nil {
		return FileProviderLeaseCompactionResult{}, fmt.Errorf("catalog: select expired File Provider leases: %w", err)
	}
	type expiredLease struct {
		id         string
		tenant     string
		domain     string
		generation uint64
		expires    int64
	}
	var leases []expiredLease
	for rows.Next() {
		var lease expiredLease
		if err := rows.Scan(&lease.id, &lease.tenant, &lease.domain, &lease.generation, &lease.expires); err != nil {
			_ = rows.Close()
			return FileProviderLeaseCompactionResult{}, fmt.Errorf("catalog: scan expired File Provider lease: %w", err)
		}
		leases = append(leases, lease)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return FileProviderLeaseCompactionResult{}, fmt.Errorf("catalog: read expired File Provider leases: %w", err)
	}
	if err := rows.Close(); err != nil {
		return FileProviderLeaseCompactionResult{}, fmt.Errorf("catalog: close expired File Provider leases: %w", err)
	}
	more := len(leases) > limit
	if more {
		leases = leases[:limit]
	}
	for _, lease := range leases {
		result, err := tx.ExecContext(ctx, `
DELETE FROM file_provider_leases
WHERE lease_id = ? AND tenant = ? AND domain_id = ?
  AND generation = ? AND expires_unix_nano = ? AND expires_unix_nano <= ?`,
			lease.id, lease.tenant, lease.domain, lease.generation, lease.expires, now.UnixNano())
		if err != nil {
			return FileProviderLeaseCompactionResult{}, fmt.Errorf("catalog: compact expired File Provider lease: %w", err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return FileProviderLeaseCompactionResult{}, fmt.Errorf("catalog: inspect compacted File Provider lease: %w", err)
		}
		if changed != 1 {
			return FileProviderLeaseCompactionResult{}, fmt.Errorf("%w: expired File Provider lease changed", ErrIntegrity)
		}
	}
	if err := tx.Commit(); err != nil {
		return FileProviderLeaseCompactionResult{}, fmt.Errorf("catalog: commit File Provider lease compaction: %w", err)
	}
	return FileProviderLeaseCompactionResult{Leases: len(leases), More: more}, nil
}
