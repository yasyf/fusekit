package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/convergence"
)

type activationOutboxState uint8

const (
	activationOutboxPending activationOutboxState = iota + 1
	activationOutboxDelivering
	activationOutboxAwaitingAck
	activationOutboxAcked
	activationOutboxSuperseded
	activationOutboxQuarantined
)

const (
	activationOutboxErrorCodeLimit   = 128
	activationOutboxErrorDetailLimit = 4096
)

// RecoverDeliveries fences Delivering rows owned by dead holder runtime generations.
func (c *Catalog) RecoverDeliveries(ctx context.Context, runtimeGeneration string, now time.Time) error {
	if runtimeGeneration == "" || now.IsZero() {
		return fmt.Errorf("%w: incomplete delivery recovery identity", ErrInvalidObject)
	}
	deadline := now.Add(convergence.AckTimeout).UnixNano()
	if _, err := c.db.ExecContext(ctx, `
UPDATE convergence_outbox SET
    state = ?, outcome = ?, ack_deadline_unix_nano = ?,
    last_error_code = 'delivery_owner_lost',
    last_error_detail = 'delivery owner exited before recording a definite outcome',
    version = version + 1
WHERE state = ? AND holder_runtime_generation <> ?`,
		uint8(activationOutboxAwaitingAck), uint8(convergence.DeliveryUnknown), deadline,
		uint8(activationOutboxDelivering), runtimeGeneration); err != nil {
		return fmt.Errorf("catalog: recover activation deliveries: %w", mapConstraint(err))
	}
	return nil
}

// ClaimDelivery atomically claims the newest eligible activation for one presentation.
func (c *Catalog) ClaimDelivery(
	ctx context.Context,
	request convergence.ClaimRequest,
) (*convergence.DeliveryClaim, error) {
	if request.RuntimeGeneration == "" || request.HolderOperation == (causal.OperationID{}) ||
		request.ClaimToken == (causal.OperationID{}) || request.ClaimedAt.IsZero() {
		return nil, fmt.Errorf("%w: incomplete activation delivery claim", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("catalog: begin activation delivery claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var inFlight int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM convergence_outbox WHERE state IN (?, ?)`,
		uint8(activationOutboxDelivering), uint8(activationOutboxAwaitingAck)).Scan(&inFlight); err != nil {
		return nil, fmt.Errorf("catalog: count activation deliveries: %w", err)
	}
	if inFlight >= convergence.MaxAwaiting {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("catalog: finish full activation delivery claim: %w", err)
		}
		return nil, nil
	}
	var rawChange []byte
	var presentation string
	if err := tx.QueryRowContext(ctx, `
SELECT candidate.activation_change_id, candidate.presentation_id
FROM convergence_outbox candidate
WHERE candidate.state = ?
  AND NOT EXISTS (
      SELECT 1 FROM convergence_outbox active
      WHERE active.presentation_id = candidate.presentation_id AND active.state IN (?, ?)
  )
  AND NOT EXISTS (
      SELECT 1 FROM convergence_outbox newer
      WHERE newer.presentation_id = candidate.presentation_id AND newer.state = ?
        AND newer.expected_activation_revision > candidate.expected_activation_revision
  )
ORDER BY candidate.expected_activation_revision, candidate.presentation_id
LIMIT 1`, uint8(activationOutboxPending), uint8(activationOutboxDelivering),
		uint8(activationOutboxAwaitingAck), uint8(activationOutboxPending)).Scan(&rawChange, &presentation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if err := tx.Commit(); err != nil {
				return nil, fmt.Errorf("catalog: finish empty activation delivery claim: %w", err)
			}
			return nil, nil
		}
		return nil, fmt.Errorf("catalog: select activation delivery: %w", err)
	}
	changeID, err := activationChangeID(rawChange)
	if err != nil {
		return nil, err
	}
	if err := requireSupersededActivationsIncluded(ctx, tx, changeID, causal.PresentationID(presentation)); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE convergence_outbox SET
    state = ?, outcome = ?, satisfied_by_activation_change_id = ?, version = version + 1
WHERE presentation_id = ? AND state = ?
  AND expected_activation_revision < (
      SELECT expected_activation_revision FROM convergence_outbox
      WHERE activation_change_id = ? AND presentation_id = ?
  )`, uint8(activationOutboxSuperseded), uint8(convergence.DeliveryNotSent), changeID[:], presentation,
		uint8(activationOutboxPending), changeID[:], presentation); err != nil {
		return nil, fmt.Errorf("catalog: supersede older pending activations: %w", mapConstraint(err))
	}
	result, err := tx.ExecContext(ctx, `
UPDATE convergence_outbox SET
    state = ?, outcome = 0, holder_runtime_generation = ?, holder_operation_id = ?,
    claim_token = ?, attempt_count = attempt_count + 1, claimed_unix_nano = ?,
    ack_deadline_unix_nano = 0, last_error_code = NULL, last_error_detail = NULL,
    version = version + 1
WHERE activation_change_id = ? AND presentation_id = ? AND state = ?`,
		uint8(activationOutboxDelivering), request.RuntimeGeneration, request.HolderOperation[:],
		request.ClaimToken[:], request.ClaimedAt.UnixNano(), changeID[:], presentation,
		uint8(activationOutboxPending))
	if err != nil {
		return nil, fmt.Errorf("catalog: claim activation delivery: %w", mapConstraint(err))
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return nil, ErrInvalidTransition
	}
	event, attempt, err := readActivationEvent(ctx, tx, causal.ActivationKey{
		ActivationChangeID: changeID,
		PresentationID:     causal.PresentationID(presentation),
	})
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("catalog: commit activation delivery claim: %w", err)
	}
	return &convergence.DeliveryClaim{Event: event, ClaimToken: request.ClaimToken, Attempt: attempt}, nil
}

// RecordDelivery settles the synchronous notifier outcome for one Delivering row.
func (c *Catalog) RecordDelivery(ctx context.Context, delivery convergence.DeliveryResult) error {
	if delivery.Key.ActivationChangeID == (causal.ActivationChangeID{}) || delivery.Key.PresentationID == "" ||
		delivery.ClaimToken == (causal.OperationID{}) || !validDeliveryFailure(delivery.Failure) {
		return fmt.Errorf("%w: incomplete activation delivery result", ErrInvalidObject)
	}
	var state activationOutboxState
	var deadline int64
	switch delivery.Outcome {
	case convergence.DeliveryNotSent:
		state = activationOutboxPending
		if !delivery.AckDeadline.IsZero() {
			return fmt.Errorf("%w: NotSent delivery has an acknowledgement deadline", ErrInvalidObject)
		}
	case convergence.DeliverySent:
		state = activationOutboxAwaitingAck
		if !deliveryFailureEmpty(delivery.Failure) || delivery.AckDeadline.IsZero() {
			return fmt.Errorf("%w: Sent delivery result is inconsistent", ErrInvalidObject)
		}
		deadline = delivery.AckDeadline.UnixNano()
	case convergence.DeliveryUnknown:
		state = activationOutboxAwaitingAck
		if delivery.AckDeadline.IsZero() {
			return fmt.Errorf("%w: Unknown delivery has no acknowledgement deadline", ErrInvalidObject)
		}
		deadline = delivery.AckDeadline.UnixNano()
	default:
		return fmt.Errorf("%w: unknown activation delivery outcome", ErrInvalidObject)
	}
	errorCode, errorDetail := nullableDeliveryFailure(delivery.Failure)
	var result sql.Result
	var err error
	if state == activationOutboxPending {
		result, err = c.db.ExecContext(ctx, `
UPDATE convergence_outbox SET
    state = ?, outcome = ?, holder_runtime_generation = NULL, holder_operation_id = NULL,
    claim_token = NULL, claimed_unix_nano = 0, ack_deadline_unix_nano = 0,
    last_error_code = ?, last_error_detail = ?, version = version + 1
WHERE activation_change_id = ? AND presentation_id = ? AND state = ? AND claim_token = ?`,
			uint8(state), uint8(delivery.Outcome), errorCode, errorDetail,
			delivery.Key.ActivationChangeID[:], string(delivery.Key.PresentationID),
			uint8(activationOutboxDelivering), delivery.ClaimToken[:])
	} else {
		result, err = c.db.ExecContext(ctx, `
UPDATE convergence_outbox SET
    state = ?, outcome = ?, ack_deadline_unix_nano = ?,
    last_error_code = ?, last_error_detail = ?, version = version + 1
WHERE activation_change_id = ? AND presentation_id = ? AND state = ? AND claim_token = ?
  AND claimed_unix_nano < ?`, uint8(state), uint8(delivery.Outcome), deadline, errorCode, errorDetail,
			delivery.Key.ActivationChangeID[:], string(delivery.Key.PresentationID),
			uint8(activationOutboxDelivering), delivery.ClaimToken[:], deadline)
	}
	if err != nil {
		return fmt.Errorf("catalog: record activation delivery: %w", mapConstraint(err))
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrInvalidTransition
	}
	return nil
}

// AcknowledgeDelivery settles an exact presentation observation and satisfies older activations.
func (c *Catalog) AcknowledgeDelivery(ctx context.Context, ack causal.ActivationAck) error {
	if err := causal.ValidateActivationAck(ack); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidObject, err)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin activation acknowledgement: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var state uint8
	var tenant string
	var generation, backend, expectedRevision, expectedHead uint64
	var expectedDigest, observedDigest []byte
	var observedRevision, observedHead sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
SELECT state, tenant_id, tenant_generation, backend,
       expected_activation_revision, expected_catalog_head, expected_head_digest,
       observed_activation_revision, observed_catalog_head, observed_head_digest
FROM convergence_outbox
WHERE activation_change_id = ? AND presentation_id = ?`, ack.ActivationChangeID[:],
		string(ack.PresentationID)).Scan(&state, &tenant, &generation, &backend,
		&expectedRevision, &expectedHead, &expectedDigest,
		&observedRevision, &observedHead, &observedDigest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("catalog: read activation acknowledgement target: %w", err)
	}
	if activationOutboxState(state) == activationOutboxAcked {
		if tenant == string(ack.TenantID) && generation == uint64(ack.TenantGeneration) &&
			backend == uint64(ack.Backend) && observedRevision.Valid && observedHead.Valid &&
			uint64(observedRevision.Int64) == uint64(ack.ObservedActivationRevision) &&
			uint64(observedHead.Int64) == uint64(ack.ObservedCatalogHead) &&
			bytes.Equal(observedDigest, ack.ObservedHeadDigest[:]) {
			if err := tx.Commit(); err != nil {
				return err
			}
			return nil
		}
		return ErrMutationConflict
	}
	if activationOutboxState(state) != activationOutboxAwaitingAck || tenant != string(ack.TenantID) ||
		generation != uint64(ack.TenantGeneration) || backend != uint64(ack.Backend) ||
		expectedRevision != uint64(ack.ObservedActivationRevision) ||
		expectedHead != uint64(ack.ObservedCatalogHead) ||
		!bytes.Equal(expectedDigest, ack.ObservedHeadDigest[:]) {
		return ErrInvalidTransition
	}
	result, err := tx.ExecContext(ctx, `
UPDATE convergence_outbox SET
    state = ?, holder_runtime_generation = NULL, holder_operation_id = NULL, claim_token = NULL,
    claimed_unix_nano = 0, ack_deadline_unix_nano = 0,
    last_error_code = NULL, last_error_detail = NULL,
    observed_activation_revision = ?, observed_catalog_head = ?, observed_head_digest = ?,
    version = version + 1
WHERE activation_change_id = ? AND presentation_id = ? AND state = ?`,
		uint8(activationOutboxAcked), uint64(ack.ObservedActivationRevision),
		uint64(ack.ObservedCatalogHead), ack.ObservedHeadDigest[:], ack.ActivationChangeID[:],
		string(ack.PresentationID), uint8(activationOutboxAwaitingAck))
	if err != nil {
		return fmt.Errorf("catalog: acknowledge activation delivery: %w", mapConstraint(err))
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrInvalidTransition
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE convergence_outbox SET
    state = ?, outcome = CASE WHEN outcome = 0 THEN ? ELSE outcome END,
    holder_runtime_generation = NULL, holder_operation_id = NULL, claim_token = NULL,
    claimed_unix_nano = 0, ack_deadline_unix_nano = 0,
    last_error_code = NULL, last_error_detail = NULL,
    observed_activation_revision = NULL, observed_catalog_head = NULL, observed_head_digest = NULL,
    satisfied_by_activation_change_id = ?, version = version + 1
WHERE presentation_id = ? AND activation_change_id <> ?
  AND tenant_id = ? AND expected_activation_revision < ? AND expected_catalog_head <= ?
  AND state IN (?, ?, ?, ?)`, uint8(activationOutboxSuperseded), uint8(convergence.DeliveryNotSent),
		ack.ActivationChangeID[:], string(ack.PresentationID), ack.ActivationChangeID[:], string(ack.TenantID),
		uint64(ack.ObservedActivationRevision), uint64(ack.ObservedCatalogHead),
		uint8(activationOutboxPending), uint8(activationOutboxDelivering),
		uint8(activationOutboxAwaitingAck), uint8(activationOutboxQuarantined)); err != nil {
		return fmt.Errorf("catalog: satisfy older activation deliveries: %w", mapConstraint(err))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit activation acknowledgement: %w", err)
	}
	return nil
}

// QuarantineExpired permanently quarantines AwaitingAck rows whose exact deadline elapsed.
func (c *Catalog) QuarantineExpired(ctx context.Context, now time.Time) error {
	if now.IsZero() {
		return fmt.Errorf("%w: quarantine timestamp is empty", ErrInvalidObject)
	}
	if _, err := c.db.ExecContext(ctx, `
UPDATE convergence_outbox SET
    state = ?, holder_runtime_generation = NULL, holder_operation_id = NULL, claim_token = NULL,
    claimed_unix_nano = 0, ack_deadline_unix_nano = 0,
    last_error_code = 'ack_timeout', last_error_detail = 'presentation did not acknowledge activation before its deadline',
    retry_eligible = 0, version = version + 1
WHERE state = ? AND ack_deadline_unix_nano <= ?`, uint8(activationOutboxQuarantined),
		uint8(activationOutboxAwaitingAck), now.UnixNano()); err != nil {
		return fmt.Errorf("catalog: quarantine expired activation deliveries: %w", mapConstraint(err))
	}
	return nil
}

// ActivationPresentationTarget returns the immutable signal plan for one exact delivery key.
func (c *Catalog) ActivationPresentationTarget(
	ctx context.Context,
	key causal.ActivationKey,
) (TenantPresentationTarget, error) {
	if key.ActivationChangeID == (causal.ActivationChangeID{}) || key.PresentationID == "" {
		return TenantPresentationTarget{}, ErrInvalidObject
	}
	var target TenantPresentationTarget
	var presentation string
	var backend, coalesced uint8
	var fingerprint, digest []byte
	if err := c.readDB.QueryRowContext(ctx, `
SELECT presentation_id, backend, provider_fingerprint, signal_target_count,
       signal_target_digest, signal_coalesced
FROM convergence_outbox
WHERE activation_change_id = ? AND presentation_id = ?`, key.ActivationChangeID[:],
		string(key.PresentationID)).Scan(&presentation, &backend, &fingerprint,
		&target.SignalTargetCount, &digest, &coalesced); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return TenantPresentationTarget{}, ErrNotFound
		}
		return TenantPresentationTarget{}, err
	}
	target.PresentationID = causal.PresentationID(presentation)
	target.Backend = causal.Backend(backend)
	target.SignalsCoalesced = coalesced != 0
	if copyExactID(target.ProviderFingerprint[:], fingerprint) != nil ||
		copyExactID(target.SignalTargetDigest[:], digest) != nil ||
		target.PresentationID != key.PresentationID || target.Backend != causal.BackendFileProvider ||
		target.SignalTargetCount == 0 || target.SignalTargetDigest == ([sha256.Size]byte{}) {
		return TenantPresentationTarget{}, ErrIntegrity
	}
	rows, err := c.readDB.QueryContext(ctx, `
SELECT kind, parent_id FROM convergence_outbox_signal_targets
WHERE activation_change_id = ? AND presentation_id = ? ORDER BY sequence`,
		key.ActivationChangeID[:], string(key.PresentationID))
	if err != nil {
		return TenantPresentationTarget{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var kind uint8
		var parent []byte
		if err := rows.Scan(&kind, &parent); err != nil {
			return TenantPresentationTarget{}, err
		}
		var signal FileProviderSignalTarget
		switch kind {
		case 1:
			if len(parent) != 0 {
				return TenantPresentationTarget{}, ErrIntegrity
			}
			signal.WorkingSet = true
		case 2:
			if copyExactID(signal.Parent[:], parent) != nil {
				return TenantPresentationTarget{}, ErrIntegrity
			}
		default:
			return TenantPresentationTarget{}, ErrIntegrity
		}
		target.SignalTargets = append(target.SignalTargets, signal)
	}
	if err := rows.Err(); err != nil {
		return TenantPresentationTarget{}, err
	}
	expectedTargets := target.SignalTargetCount
	if target.SignalsCoalesced {
		expectedTargets = 1
	}
	if uint64(len(target.SignalTargets)) != expectedTargets {
		return TenantPresentationTarget{}, ErrIntegrity
	}
	return target, nil
}

func readActivationEvent(
	ctx context.Context,
	query interface {
		QueryRowContext(context.Context, string, ...any) *sql.Row
		QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	},
	key causal.ActivationKey,
) (causal.ActivationEvent, uint64, error) {
	var event causal.ActivationEvent
	var rawChange, headDigest []byte
	var tenant, presentation string
	var generation, backend, revision, head, attempt, causeCount uint64
	if err := query.QueryRowContext(ctx, `
SELECT outbox.activation_change_id, outbox.presentation_id, outbox.tenant_id,
       outbox.tenant_generation, outbox.backend, outbox.expected_activation_revision,
       outbox.expected_catalog_head, outbox.expected_head_digest, outbox.attempt_count,
       activation.cause_count
FROM convergence_outbox outbox
JOIN tenant_activation_changes activation
  ON activation.activation_change_id = outbox.activation_change_id
WHERE outbox.activation_change_id = ? AND outbox.presentation_id = ?`,
		key.ActivationChangeID[:], string(key.PresentationID)).Scan(
		&rawChange, &presentation, &tenant, &generation, &backend, &revision,
		&head, &headDigest, &attempt, &causeCount,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return causal.ActivationEvent{}, 0, ErrNotFound
		}
		return causal.ActivationEvent{}, 0, fmt.Errorf("catalog: read activation event: %w", err)
	}
	changeID, err := activationChangeID(rawChange)
	if err != nil {
		return causal.ActivationEvent{}, 0, err
	}
	if len(headDigest) != sha256.Size {
		return causal.ActivationEvent{}, 0, ErrIntegrity
	}
	event = causal.ActivationEvent{
		ActivationChangeID: changeID, TenantID: causal.TenantID(tenant),
		TenantGeneration: causal.Generation(generation), ActivationRevision: causal.Revision(revision),
		PresentationID: causal.PresentationID(presentation), Backend: causal.Backend(backend),
		CatalogHead: causal.CatalogRevision(head),
	}
	copy(event.HeadDigest[:], headDigest)
	rows, err := query.QueryContext(ctx, `
SELECT cause.publication_id, cause.change_id, cause.source_revision,
       cause.source_operation_id, publication.cause, publication.affected_keys_digest
FROM tenant_activation_causes cause
JOIN source_driver_publications publication
  ON publication.source_authority = cause.source_authority
 AND publication.publication_id = cause.publication_id
WHERE cause.activation_change_id = ?
ORDER BY cause.position`, changeID[:])
	if err != nil {
		return causal.ActivationEvent{}, 0, fmt.Errorf("catalog: read activation causes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cause causal.SourceCause
		var publication, change, operation, affected []byte
		var sourceRevision uint64
		var causeName string
		if err := rows.Scan(&publication, &change, &sourceRevision, &operation, &causeName, &affected); err != nil {
			return causal.ActivationEvent{}, 0, fmt.Errorf("catalog: scan activation cause: %w", err)
		}
		cause.SourceRevision, cause.Cause = causal.Revision(sourceRevision), causal.Cause(causeName)
		if err := copyExactID(cause.PublicationID[:], publication); err != nil {
			return causal.ActivationEvent{}, 0, err
		}
		if err := copyExactID(cause.ChangeID[:], change); err != nil {
			return causal.ActivationEvent{}, 0, err
		}
		if err := copyExactID(cause.OperationID[:], operation); err != nil {
			return causal.ActivationEvent{}, 0, err
		}
		if err := copyExactID(cause.AffectedKeysDigest[:], affected); err != nil {
			return causal.ActivationEvent{}, 0, err
		}
		event.Causes = append(event.Causes, cause)
	}
	if err := rows.Err(); err != nil {
		return causal.ActivationEvent{}, 0, fmt.Errorf("catalog: read activation causes: %w", err)
	}
	if uint64(len(event.Causes)) != causeCount {
		return causal.ActivationEvent{}, 0, ErrIntegrity
	}
	if err := causal.ValidateActivationEvent(event); err != nil {
		return causal.ActivationEvent{}, 0, fmt.Errorf("%w: %v", ErrIntegrity, err)
	}
	return event, attempt, nil
}

func requireSupersededActivationsIncluded(
	ctx context.Context,
	tx *sql.Tx,
	change causal.ActivationChangeID,
	presentation causal.PresentationID,
) error {
	var missing int
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM convergence_outbox older
    JOIN convergence_outbox newer
      ON newer.activation_change_id = ? AND newer.presentation_id = older.presentation_id
    JOIN tenant_activation_causes old_cause
      ON old_cause.activation_change_id = older.activation_change_id
    LEFT JOIN tenant_activation_causes new_cause
      ON new_cause.activation_change_id = newer.activation_change_id
     AND new_cause.source_authority = old_cause.source_authority
     AND new_cause.publication_id = old_cause.publication_id
    WHERE older.presentation_id = ? AND older.state = ?
      AND older.expected_activation_revision < newer.expected_activation_revision
      AND new_cause.activation_change_id IS NULL
)`, change[:], string(presentation), uint8(activationOutboxPending)).Scan(&missing); err != nil {
		return fmt.Errorf("catalog: validate superseded activation causes: %w", err)
	}
	if missing != 0 {
		return fmt.Errorf("%w: newer pending activation does not include older causes", ErrIntegrity)
	}
	return nil
}

func activationChangeID(raw []byte) (causal.ActivationChangeID, error) {
	var result causal.ActivationChangeID
	if err := copyExactID(result[:], raw); err != nil {
		return causal.ActivationChangeID{}, err
	}
	return result, nil
}

func validDeliveryFailure(failure convergence.DeliveryFailure) bool {
	return deliveryFailureEmpty(failure) || (failure.Code != "" && failure.Detail != "" &&
		len(failure.Code) <= activationOutboxErrorCodeLimit && len(failure.Detail) <= activationOutboxErrorDetailLimit)
}

func nullableDeliveryFailure(failure convergence.DeliveryFailure) (any, any) {
	if deliveryFailureEmpty(failure) {
		return nil, nil
	}
	return failure.Code, failure.Detail
}

func deliveryFailureEmpty(failure convergence.DeliveryFailure) bool {
	return failure.Code == "" && failure.Detail == ""
}
