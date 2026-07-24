package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/yasyf/fusekit/contentstream"
)

const preparedAfterIntentCommit = "prepared.after_intent_commit"

// BeginMutation durably prepares one tenant-exclusive namespace mutation and
// derives its revision-fenced identity from the final semantic intent.
func (c *Catalog) BeginMutation(ctx context.Context, tenant TenantID, expectedHead Revision, intent MutationIntent) (PreparedMutation, error) {
	if expectedHead == 0 || expectedHead == Revision(^uint64(0)) {
		return PreparedMutation{}, fmt.Errorf("%w: prepared mutation expected head is zero", ErrInvalidTransition)
	}
	kind, err := validateMutationIntent(intent)
	if err != nil {
		return PreparedMutation{}, err
	}
	payload, digest, err := encodeMutationIntent(tenant, expectedHead, kind, intent)
	if err != nil {
		return PreparedMutation{}, err
	}
	binding := MutationBinding{
		Tenant: tenant, Target: expectedHead + 1, Issuer: intent.SourceID,
		RequestDigest: MutationRequestDigest(digest),
	}
	id, err := deriveMutationID(binding)
	if err != nil {
		return PreparedMutation{}, err
	}
	if existing, found, err := readPreparedMutation(ctx, c.readDB, id); err != nil {
		return PreparedMutation{}, err
	} else if found {
		if existing.Tenant != tenant || existing.Kind != kind || existing.digest != digest ||
			existing.ExpectedHead != expectedHead || validateMutationID(id, binding) != nil {
			return PreparedMutation{}, ErrMutationConflict
		}
		return existing.PreparedMutation, nil
	}
	if err := rejectExpiredMutation(ctx, c.readDB, tenant, id); err != nil {
		return PreparedMutation{}, err
	}
	var active int
	if err := c.readDB.QueryRowContext(ctx, `
SELECT COUNT(*) FROM prepared_mutations
WHERE tenant = ? AND state IN (?, ?)`,
		string(tenant), uint8(MutationPrepared), uint8(MutationApplying)).Scan(&active); err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: inspect active prepared mutation: %w", err)
	}
	if active != 0 {
		return PreparedMutation{}, ErrMutationActive
	}
	if err := c.verifyIntentContent(ctx, id, tenant, intent); err != nil {
		return PreparedMutation{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: begin prepared mutation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if existing, found, err := readPreparedMutation(ctx, tx, id); err != nil {
		return PreparedMutation{}, err
	} else if found {
		if existing.Tenant != tenant || existing.Kind != kind || existing.digest != digest ||
			existing.ExpectedHead != expectedHead || validateMutationID(id, binding) != nil {
			return PreparedMutation{}, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return PreparedMutation{}, fmt.Errorf("catalog: finish prepared mutation lookup: %w", err)
		}
		return existing.PreparedMutation, nil
	}
	if _, found, err := mutationRecord(ctx, tx, id); err != nil {
		return PreparedMutation{}, err
	} else if found {
		return PreparedMutation{}, ErrMutationConflict
	}
	if err := rejectExpiredMutation(ctx, tx, tenant, id); err != nil {
		return PreparedMutation{}, err
	}
	active = 0
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM prepared_mutations
WHERE tenant = ? AND state IN (?, ?)`,
		string(tenant), uint8(MutationPrepared), uint8(MutationApplying)).Scan(&active); err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: inspect active prepared mutation: %w", err)
	}
	if active != 0 {
		return PreparedMutation{}, ErrMutationActive
	}
	pending, err := pendingSourceDriverTarget(ctx, tx, tenant)
	if err != nil {
		return PreparedMutation{}, err
	}
	if pending {
		return PreparedMutation{}, ErrMutationActive
	}
	head, _, err := effectiveRevisionState(ctx, tx, tenant)
	if err != nil {
		return PreparedMutation{}, err
	}
	if head != expectedHead {
		return PreparedMutation{}, errMutationHeadChanged
	}
	if err := c.validateIntentCatalog(ctx, tx, tenant, intent); err != nil {
		return PreparedMutation{}, err
	}
	if err := c.claimIntentContent(ctx, tx, id, tenant, intent); err != nil {
		return PreparedMutation{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO prepared_mutations(
    mutation_id, tenant, kind, request_hash, intent_json, source_id, expected_head, state
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		id[:], string(tenant), uint8(kind), digest[:], payload, intent.SourceID, uint64(expectedHead), uint8(MutationPrepared)); err != nil {
		return PreparedMutation{}, mapConstraint(err)
	}
	if err := tx.Commit(); err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: commit prepared mutation: %w", err)
	}
	if err := c.trip(preparedAfterIntentCommit); err != nil {
		return PreparedMutation{}, err
	}
	return PreparedMutation{
		OperationID: id, Tenant: tenant, Kind: kind, State: MutationPrepared,
		ExpectedHead: expectedHead, Intent: intent,
	}, nil
}

// ClaimMutation durably fences one external source attempt.
func (c *Catalog) ClaimMutation(ctx context.Context, id MutationID, owner MutationOwnerID) (PreparedMutation, error) {
	if owner == (MutationOwnerID{}) {
		return PreparedMutation{}, fmt.Errorf("%w: mutation owner id is zero", ErrInvalidObject)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: begin mutation claim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, found, err := readPreparedMutation(ctx, tx, id)
	if err != nil {
		return PreparedMutation{}, err
	}
	if !found {
		return PreparedMutation{}, ErrNotFound
	}
	switch record.State {
	case MutationPrepared:
	case MutationApplying:
		if record.Claim != nil && record.Claim.Owner == owner && record.Claim.Epoch == 1 {
			if err := tx.Commit(); err != nil {
				return PreparedMutation{}, fmt.Errorf("catalog: finish mutation claim replay: %w", err)
			}
			return record.PreparedMutation, nil
		}
		return PreparedMutation{}, ErrMutationClaimed
	case MutationCommitted:
		return PreparedMutation{}, ErrInvalidTransition
	default:
		return PreparedMutation{}, fmt.Errorf("catalog: invalid prepared mutation state %d", record.State)
	}
	claim := MutationClaim{Owner: owner, Epoch: 1}
	if _, err := tx.ExecContext(ctx, `
UPDATE prepared_mutations SET state = ?, claim_owner = ?, claim_epoch = ?
WHERE mutation_id = ? AND state = ?`,
		uint8(MutationApplying), owner[:], claim.Epoch, id[:], uint8(MutationPrepared)); err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: claim prepared mutation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: commit mutation claim: %w", err)
	}
	record.State = MutationApplying
	record.Claim = &claim
	return record.PreparedMutation, nil
}

// ReclaimMutation fences a new attempt after the prior worker owner is proven settled.
func (c *Catalog) ReclaimMutation(ctx context.Context, id MutationID, stale MutationClaim, owner MutationOwnerID) (PreparedMutation, error) {
	if err := validateMutationClaim(stale); err != nil {
		return PreparedMutation{}, err
	}
	if owner == (MutationOwnerID{}) {
		return PreparedMutation{}, fmt.Errorf("%w: mutation owner id is zero", ErrInvalidObject)
	}
	if stale.Epoch == ^uint64(0) {
		return PreparedMutation{}, fmt.Errorf("%w: mutation claim epoch exhausted", ErrInvalidTransition)
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: begin mutation reclaim: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, found, err := readPreparedMutation(ctx, tx, id)
	if err != nil {
		return PreparedMutation{}, err
	}
	if !found {
		return PreparedMutation{}, ErrNotFound
	}
	if record.State == MutationApplying && record.Claim != nil &&
		record.Claim.Owner == owner && record.Claim.Epoch == stale.Epoch+1 {
		if err := tx.Commit(); err != nil {
			return PreparedMutation{}, fmt.Errorf("catalog: finish mutation reclaim replay: %w", err)
		}
		return record.PreparedMutation, nil
	}
	if record.State != MutationApplying || record.Claim == nil || *record.Claim != stale {
		return PreparedMutation{}, ErrMutationClaimed
	}
	claim := MutationClaim{Owner: owner, Epoch: stale.Epoch + 1}
	result, err := tx.ExecContext(ctx, `
UPDATE prepared_mutations SET claim_owner = ?, claim_epoch = ?
WHERE mutation_id = ? AND state = ? AND claim_owner = ? AND claim_epoch = ?`,
		owner[:], claim.Epoch, id[:], uint8(MutationApplying), stale.Owner[:], stale.Epoch)
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: reclaim prepared mutation: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: inspect mutation reclaim: %w", err)
	}
	if changed != 1 {
		return PreparedMutation{}, ErrMutationClaimed
	}
	if err := tx.Commit(); err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: commit mutation reclaim: %w", err)
	}
	record.Claim = &claim
	return record.PreparedMutation, nil
}

// PreparedMutation returns one durable namespace mutation intent.
func (c *Catalog) PreparedMutation(ctx context.Context, tenant TenantID, id MutationID) (PreparedMutation, error) {
	record, found, err := readPreparedMutation(ctx, c.readDB, id)
	if err != nil {
		return PreparedMutation{}, err
	}
	if !found {
		if err := rejectExpiredMutation(ctx, c.readDB, tenant, id); err != nil {
			return PreparedMutation{}, err
		}
		return PreparedMutation{}, ErrNotFound
	}
	if record.Tenant != tenant {
		return PreparedMutation{}, ErrMutationConflict
	}
	return record.PreparedMutation, nil
}

// PendingMutation returns the tenant-exclusive intent that must be resolved before admission.
func (c *Catalog) PendingMutation(ctx context.Context, tenant TenantID) (*PreparedMutation, error) {
	record, err := scanPreparedMutation(c.readDB.QueryRowContext(ctx, `
SELECT mutation_id, tenant, kind, request_hash, intent_json, source_context_json, source_result_json, expected_head, state, claim_owner, claim_epoch
FROM prepared_mutations
WHERE tenant = ? AND state IN (?, ?)
ORDER BY mutation_id`,
		string(tenant), uint8(MutationPrepared), uint8(MutationApplying)))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("catalog: read pending mutation: %w", err)
	}
	return &record.PreparedMutation, nil
}

// OpenMutationContent opens the verified staged bytes owned by a prepared mutation.
func (c *Catalog) OpenMutationContent(ctx context.Context, tenant TenantID, id MutationID) (contentstream.Source, error) {
	prepared, err := c.PreparedMutation(ctx, tenant, id)
	if err != nil {
		return nil, err
	}
	var kind Kind
	var ref ContentRef
	switch {
	case prepared.Intent.Create != nil:
		kind = prepared.Intent.Create.Spec.Kind
		ref = prepared.Intent.Create.Spec.Content
	case prepared.Intent.Revise != nil && prepared.Intent.Revise.Spec.Content != nil:
		current, err := c.lookupAnyObject(ctx, prepared.Tenant, prepared.Intent.Revise.Object)
		if err != nil {
			return nil, err
		}
		kind = current.Kind
		ref = prepared.Intent.Revise.Spec.Content.Ref
	case prepared.Intent.Replace != nil && prepared.Intent.Replace.Content != nil:
		current, err := c.replaceSourceObject(ctx, c.readDB, prepared.Tenant, prepared.Intent)
		if err != nil {
			return nil, err
		}
		kind = current.Kind
		ref = prepared.Intent.Replace.Content.Ref
	default:
		return nil, fmt.Errorf("%w: prepared mutation has no staged content", ErrInvalidObject)
	}
	if kind != KindFile {
		return nil, fmt.Errorf("%w: prepared mutation has no file content", ErrInvalidObject)
	}
	if err := c.validateMutationContentRef(ctx, c.readDB, id, kind, ref); err != nil {
		return nil, err
	}
	if err := c.trip(contentBeforeVerify); err != nil {
		return nil, err
	}
	file, err := c.openBlob(ctx, ref)
	if err != nil {
		return nil, err
	}
	if err := c.trip(contentAfterOpen); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := verifyOpenFile(file, ref); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &ownedMutationFile{ReadCloser: file}, nil
}

type ownedMutationFile struct {
	io.ReadCloser
	once sync.Once
	err  error
}

func (s *ownedMutationFile) Settle(_ error) error {
	s.once.Do(func() { s.err = s.Close() })
	return s.err
}

func (*ownedMutationFile) Wait(context.Context) error { return nil }

type preparedRecord struct {
	PreparedMutation
	digest [32]byte
}

func readPreparedMutation(ctx context.Context, query rowQuerier, id MutationID) (preparedRecord, bool, error) {
	record, err := scanPreparedMutation(query.QueryRowContext(ctx, `
SELECT mutation_id, tenant, kind, request_hash, intent_json, source_context_json, source_result_json, expected_head, state, claim_owner, claim_epoch
FROM prepared_mutations WHERE mutation_id = ?`, id[:]))
	if errors.Is(err, sql.ErrNoRows) {
		return preparedRecord{}, false, nil
	}
	if err != nil {
		return preparedRecord{}, false, fmt.Errorf("catalog: read prepared mutation: %w", err)
	}
	return record, true, nil
}

func scanPreparedMutation(scanner rowScanner) (preparedRecord, error) {
	var rawID, rawDigest, payload, rawSourceContext, rawSourceResult, rawClaimOwner []byte
	var tenant string
	var kind, state uint8
	var expected uint64
	var claimEpoch sql.NullInt64
	if err := scanner.Scan(&rawID, &tenant, &kind, &rawDigest, &payload, &rawSourceContext, &rawSourceResult, &expected, &state, &rawClaimOwner, &claimEpoch); err != nil {
		return preparedRecord{}, err
	}
	id, err := mutationID(rawID)
	if err != nil {
		return preparedRecord{}, err
	}
	if len(rawDigest) != sha256.Size {
		return preparedRecord{}, fmt.Errorf("catalog: corrupt prepared mutation digest length %d", len(rawDigest))
	}
	var digest [sha256.Size]byte
	copy(digest[:], rawDigest)
	var intent MutationIntent
	if err := json.Unmarshal(payload, &intent); err != nil {
		return preparedRecord{}, fmt.Errorf("catalog: decode prepared mutation intent: %w", err)
	}
	if intent.SourceID == "" {
		return preparedRecord{}, fmt.Errorf("catalog: corrupt prepared mutation source id")
	}
	parsedKind := MutationKind(kind)
	intentKind, err := validateMutationIntent(intent)
	if err != nil {
		return preparedRecord{}, fmt.Errorf("%w: corrupt prepared mutation intent: %v", ErrIntegrity, err)
	}
	if parsedKind < MutationCreate || parsedKind > MutationReplace || parsedKind != intentKind {
		return preparedRecord{}, fmt.Errorf(
			"%w: prepared mutation kind %d does not match intent kind %d",
			ErrIntegrity, parsedKind, intentKind,
		)
	}
	var source *SourceMutationContext
	if rawSourceContext != nil {
		var value SourceMutationContext
		if err := json.Unmarshal(rawSourceContext, &value); err != nil {
			return preparedRecord{}, fmt.Errorf("catalog: decode prepared source context: %w", err)
		}
		if err := validateSourceMutationContext(value); err != nil {
			return preparedRecord{}, fmt.Errorf("catalog: corrupt prepared source context: %w", err)
		}
		source = &value
	}
	var sourceResult *SourceLocator
	if rawSourceResult != nil {
		var value SourceLocator
		if err := json.Unmarshal(rawSourceResult, &value); err != nil {
			return preparedRecord{}, fmt.Errorf("catalog: decode prepared source result: %w", err)
		}
		if err := validateSourceLocator(value); err != nil {
			return preparedRecord{}, fmt.Errorf("catalog: corrupt prepared source result: %w", err)
		}
		sourceResult = &value
	}
	if sourceResult != nil && source == nil {
		return preparedRecord{}, fmt.Errorf("catalog: corrupt prepared source result without context")
	}
	var claim *MutationClaim
	if rawClaimOwner == nil != !claimEpoch.Valid {
		return preparedRecord{}, fmt.Errorf("catalog: corrupt prepared mutation claim")
	}
	if rawClaimOwner != nil {
		if len(rawClaimOwner) != len(MutationOwnerID{}) || claimEpoch.Int64 <= 0 {
			return preparedRecord{}, fmt.Errorf("catalog: corrupt prepared mutation claim")
		}
		owner := MutationOwnerID(rawClaimOwner)
		claim = &MutationClaim{Owner: owner, Epoch: uint64(claimEpoch.Int64)}
	}
	parsedState := PreparedMutationState(state)
	if parsedState < MutationPrepared || parsedState > MutationCommitted {
		return preparedRecord{}, fmt.Errorf("%w: corrupt prepared mutation state %d", ErrIntegrity, state)
	}
	if (parsedState == MutationPrepared) != (claim == nil) {
		return preparedRecord{}, fmt.Errorf(
			"%w: corrupt prepared mutation state %d claim", ErrIntegrity, parsedState,
		)
	}
	return preparedRecord{
		PreparedMutation: PreparedMutation{
			OperationID: id, Tenant: TenantID(tenant), Kind: parsedKind,
			State: parsedState, ExpectedHead: Revision(expected), Intent: intent,
			Source: source, SourceResult: sourceResult, Claim: claim,
		},
		digest: digest,
	}, nil
}

func validateMutationClaim(claim MutationClaim) error {
	if claim.Owner == (MutationOwnerID{}) || claim.Epoch == 0 {
		return fmt.Errorf("%w: invalid mutation claim", ErrInvalidObject)
	}
	return nil
}

func validateMutationIntent(intent MutationIntent) (MutationKind, error) {
	if intent.SourceID == "" || strings.IndexByte(intent.SourceID, 0) >= 0 {
		return 0, fmt.Errorf("%w: source id is empty or contains NUL", ErrInvalidObject)
	}
	if strings.IndexByte(intent.SourceMetadata, 0) >= 0 {
		return 0, fmt.Errorf("%w: source metadata contains NUL", ErrInvalidObject)
	}
	if err := validateCausalOrigin(intent.Origin); err != nil {
		return 0, err
	}
	count := 0
	var kind MutationKind
	if intent.Create != nil {
		count++
		kind = MutationCreate
		if err := validateCreateSpec(intent.Create.Spec); err != nil {
			return 0, err
		}
	}
	if intent.Revise != nil {
		count++
		kind = MutationRevise
		if zeroObjectID(intent.Revise.Object) {
			return 0, fmt.Errorf("%w: revise object id is zero", ErrInvalidObject)
		}
		if err := validateRevisionSpec(intent.Revise.Spec); err != nil {
			return 0, err
		}
	}
	if intent.Delete != nil {
		count++
		kind = MutationDelete
		if zeroObjectID(intent.Delete.Object) {
			return 0, fmt.Errorf("%w: delete object id is zero", ErrInvalidObject)
		}
	}
	if intent.Replace != nil {
		count++
		kind = MutationReplace
		if zeroObjectID(intent.Replace.Source) || zeroObjectID(intent.Replace.Target) || intent.Replace.Source == intent.Replace.Target {
			return 0, fmt.Errorf("%w: invalid replace object ids", ErrInvalidObject)
		}
		if intent.Replace.Parent != nil && zeroObjectID(*intent.Replace.Parent) {
			return 0, fmt.Errorf("%w: replace parent id is zero", ErrInvalidObject)
		}
		if intent.Replace.Name != nil {
			if err := validateName(*intent.Replace.Name); err != nil {
				return 0, err
			}
		}
		if intent.Replace.Content != nil && intent.Replace.Content.Revision == 0 {
			return 0, fmt.Errorf("%w: replace content revision is zero", ErrInvalidObject)
		}
	}
	if count != 1 {
		return 0, fmt.Errorf("%w: mutation intent must contain exactly one operation", ErrInvalidObject)
	}
	return kind, nil
}

func encodeMutationIntent(tenant TenantID, expectedHead Revision, kind MutationKind, intent MutationIntent) ([]byte, [32]byte, error) {
	payload, err := json.Marshal(intent)
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("catalog: encode prepared mutation intent: %w", err)
	}
	var semantic MutationIntent
	if err := json.Unmarshal(payload, &semantic); err != nil {
		return nil, [32]byte{}, fmt.Errorf("catalog: canonicalize prepared mutation intent: %w", err)
	}
	clearMutationIntentStages(&semantic)
	digestPayload, err := json.Marshal(struct {
		Tenant       TenantID
		ExpectedHead Revision
		Kind         MutationKind
		Intent       MutationIntent
	}{tenant, expectedHead, kind, semantic})
	if err != nil {
		return nil, [32]byte{}, fmt.Errorf("catalog: encode prepared mutation digest: %w", err)
	}
	return payload, sha256.Sum256(digestPayload), nil
}

func clearMutationIntentStages(intent *MutationIntent) {
	switch {
	case intent.Create != nil:
		intent.Create.Spec.Content.Stage = StageID{}
	case intent.Revise != nil && intent.Revise.Spec.Content != nil:
		intent.Revise.Spec.Content.Ref.Stage = StageID{}
	case intent.Replace != nil && intent.Replace.Content != nil:
		intent.Replace.Content.Ref.Stage = StageID{}
	}
}

func (c *Catalog) verifyIntentContent(
	ctx context.Context, operation MutationID, tenant TenantID, intent MutationIntent,
) error {
	switch {
	case intent.Create != nil:
		return c.verifyPreparedContentRef(ctx, intent.Create.Spec.Kind, intent.Create.Spec.Content)
	case intent.Revise != nil && intent.Revise.Spec.Content != nil:
		current, err := c.lookupAnyObject(ctx, tenant, intent.Revise.Object)
		if err != nil {
			return err
		}
		return c.verifyPreparedContentRef(ctx, current.Kind, intent.Revise.Spec.Content.Ref)
	case intent.Replace != nil && intent.Replace.Content != nil:
		current, err := c.replaceSourceObject(ctx, c.readDB, tenant, intent)
		if err != nil {
			return fmt.Errorf("catalog: verify replace source: %w", err)
		}
		return c.verifyPreparedContentRef(ctx, current.Kind, intent.Replace.Content.Ref)
	default:
		return nil
	}
}

func (c *Catalog) verifyPreparedContentRef(
	ctx context.Context, kind Kind, ref ContentRef,
) error {
	return c.verifyContentRef(ctx, c.readDB, kind, ref)
}

func (c *Catalog) validateIntentCatalog(ctx context.Context, tx *sql.Tx, tenant TenantID, intent MutationIntent) error {
	switch {
	case intent.Create != nil:
		spec := intent.Create.Spec
		parent, err := currentObject(ctx, tx, tenant, spec.Parent, false)
		if err != nil {
			return err
		}
		if err := validateParentVisibility(parent, spec.Visibility); err != nil {
			return err
		}
		return validateBindingAvailable(ctx, tx, tenant, spec.Parent, spec.Name, ObjectID{}, spec.Visibility)
	case intent.Revise != nil:
		current, err := currentObject(ctx, tx, tenant, intent.Revise.Object, false)
		if err != nil {
			return err
		}
		parent, err := currentObject(ctx, tx, tenant, intent.Revise.Spec.Parent, false)
		if err != nil {
			return err
		}
		if err := validateParentVisibility(parent, intent.Revise.Spec.Visibility); err != nil {
			return err
		}
		if err := validateNext(current, intent.Revise.Spec); err != nil {
			return err
		}
		return validateBindingAvailable(ctx, tx, tenant, intent.Revise.Spec.Parent, intent.Revise.Spec.Name, current.ID, intent.Revise.Spec.Visibility)
	case intent.Delete != nil:
		current, err := currentObject(ctx, tx, tenant, intent.Delete.Object, false)
		if err != nil {
			return err
		}
		var root []byte
		if err := tx.QueryRowContext(ctx,
			"SELECT root_id FROM tenants WHERE tenant = ?", string(tenant)).Scan(&root); err != nil {
			return fmt.Errorf("catalog: read tenant root for prepared delete: %w", err)
		}
		rootID, err := objectID(root)
		if err != nil {
			return err
		}
		if current.ID == rootID {
			return fmt.Errorf("%w: tenant root cannot be deleted", ErrInvalidObject)
		}
		children, err := currentChildCount(ctx, tx, tenant, current.ID)
		if err != nil {
			return fmt.Errorf("catalog: count prepared delete children: %w", err)
		}
		if children != 0 {
			return fmt.Errorf("%w: directory is not empty", ErrConflict)
		}
		return nil
	case intent.Replace != nil:
		source, err := c.replaceSourceObject(ctx, tx, tenant, intent)
		if err != nil {
			return fmt.Errorf("catalog: validate replace source: %w", err)
		}
		target, err := currentObject(ctx, tx, tenant, intent.Replace.Target, false)
		if err != nil {
			return fmt.Errorf("catalog: validate replace target: %w", err)
		}
		if source.Kind != target.Kind {
			return fmt.Errorf("%w: replace kinds differ", ErrInvalidObject)
		}
		if target.Visibility == (Visibility{}) {
			return fmt.Errorf("%w: replace target is not visible", ErrInvalidObject)
		}
		parentID := target.Parent
		if intent.Replace.Parent != nil {
			parentID = *intent.Replace.Parent
		}
		if parentID == source.ID || parentID == target.ID {
			return fmt.Errorf("%w: replace parent is replaced object", ErrInvalidObject)
		}
		parent, err := currentObject(ctx, tx, tenant, parentID, false)
		if err != nil {
			return fmt.Errorf("catalog: validate replace parent: %w", err)
		}
		visibility := Visibility{
			Mount:        source.Visibility.Mount || target.Visibility.Mount,
			FileProvider: source.Visibility.FileProvider || target.Visibility.FileProvider,
		}
		if intent.Replace.Visibility != nil {
			visibility = *intent.Replace.Visibility
		}
		if err := validateParentVisibility(parent, visibility); err != nil {
			return err
		}
		name := target.Name
		if intent.Replace.Name != nil {
			name = *intent.Replace.Name
		}
		if intent.Replace.Content != nil {
			if source.Kind != KindFile {
				return fmt.Errorf("%w: only regular files accept body revisions", ErrInvalidObject)
			}
			if err := validateKindContent(source.Kind, intent.Replace.Content.Revision, intent.Replace.Content.Ref, ""); err != nil {
				return err
			}
			if intent.Replace.Content.Revision <= source.ContentRevision {
				return fmt.Errorf("%w: replace content revision did not advance", ErrInvalidTransition)
			}
		}
		return validateReplaceBindingAvailable(ctx, tx, tenant, parentID, name, source.ID, target.ID, visibility)
	default:
		return fmt.Errorf("%w: missing prepared mutation operation", ErrInvalidObject)
	}
}

func validateBindingAvailable(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	parent ObjectID,
	name string,
	exclude ObjectID,
	visibility Visibility,
) error {
	policy, err := tenantCasePolicy(ctx, tx, tenant)
	if err != nil {
		return err
	}
	for _, presentation := range catalogPresentations() {
		if !visibility.Has(presentation) {
			continue
		}
		column, err := visibilityColumn(presentation)
		if err != nil {
			return err
		}
		view, err := readCatalogView(ctx, tx, tenant)
		if err != nil {
			return err
		}
		var query string
		var args []any
		if len(view.publication) != 0 {
			query = `SELECT COUNT(*) FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND parent_id = ? AND name_key = ? AND tombstone = 0 AND ` + column + ` = 1`
			args = []any{view.authority, view.publication, string(tenant), parent[:], normalizeName(policy, name)}
		} else {
			query = `SELECT COUNT(*) FROM object_versions candidate
WHERE candidate.tenant = ? AND candidate.parent_id = ? AND candidate.name_key = ?
  AND candidate.revision = (SELECT MAX(version.revision) FROM object_versions version
      WHERE version.tenant = candidate.tenant AND version.object_id = candidate.object_id
        AND version.revision <= ?)
  AND candidate.tombstone = 0 AND candidate.` + column + ` = 1`
			args = []any{string(tenant), parent[:], normalizeName(policy, name), uint64(view.head)}
		}
		if !zeroObjectID(exclude) {
			query += " AND object_id <> ?"
			args = append(args, exclude[:])
		}
		var conflicts int
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&conflicts); err != nil {
			return fmt.Errorf("catalog: check prepared namespace binding: %w", err)
		}
		if conflicts != 0 {
			return ErrConflict
		}
	}
	return nil
}

func validateReplaceBindingAvailable(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	parent ObjectID,
	name string,
	source ObjectID,
	target ObjectID,
	visibility Visibility,
) error {
	policy, err := tenantCasePolicy(ctx, tx, tenant)
	if err != nil {
		return err
	}
	for _, presentation := range catalogPresentations() {
		if !visibility.Has(presentation) {
			continue
		}
		column, err := visibilityColumn(presentation)
		if err != nil {
			return err
		}
		view, err := readCatalogView(ctx, tx, tenant)
		if err != nil {
			return err
		}
		var conflicts int
		var query string
		var args []any
		if len(view.publication) != 0 {
			query = `SELECT COUNT(*) FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND parent_id = ? AND name_key = ? AND tombstone = 0 AND ` + column + ` = 1
  AND object_id <> ? AND object_id <> ?`
			args = []any{view.authority, view.publication, string(tenant), parent[:], normalizeName(policy, name), source[:], target[:]}
		} else {
			query = `SELECT COUNT(*) FROM object_versions candidate
WHERE candidate.tenant = ? AND candidate.parent_id = ? AND candidate.name_key = ?
  AND candidate.revision = (SELECT MAX(version.revision) FROM object_versions version
      WHERE version.tenant = candidate.tenant AND version.object_id = candidate.object_id
        AND version.revision <= ?)
  AND candidate.tombstone = 0 AND candidate.` + column + ` = 1
  AND candidate.object_id <> ? AND candidate.object_id <> ?`
			args = []any{string(tenant), parent[:], normalizeName(policy, name), uint64(view.head), source[:], target[:]}
		}
		if err := tx.QueryRowContext(ctx, query, args...).Scan(&conflicts); err != nil {
			return fmt.Errorf("catalog: check prepared replace binding: %w", err)
		}
		if conflicts != 0 {
			return ErrConflict
		}
	}
	return nil
}

func currentChildCount(ctx context.Context, tx *sql.Tx, tenant TenantID, parent ObjectID) (int, error) {
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return 0, err
	}
	var count int
	if len(view.publication) != 0 {
		err = tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND parent_id = ? AND object_id <> ? AND tombstone = 0`,
			view.authority, view.publication, string(tenant), parent[:], parent[:]).Scan(&count)
	} else {
		err = tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM object_versions candidate
WHERE candidate.tenant = ? AND candidate.parent_id = ? AND candidate.object_id <> ?
  AND candidate.revision = (SELECT MAX(version.revision) FROM object_versions version
      WHERE version.tenant = candidate.tenant AND version.object_id = candidate.object_id
        AND version.revision <= ?)
  AND candidate.tombstone = 0`, string(tenant), parent[:], parent[:], uint64(view.head)).Scan(&count)
	}
	return count, err
}

func validateParentVisibility(parent Object, child Visibility) error {
	if parent.Kind != KindDirectory {
		return fmt.Errorf("%w: parent is not a directory", ErrInvalidObject)
	}
	for _, presentation := range catalogPresentations() {
		if child.Has(presentation) && !parent.Visibility.Has(presentation) {
			return fmt.Errorf("%w: parent is absent from presentation %d", ErrInvalidObject, presentation)
		}
	}
	return nil
}

func (c *Catalog) claimIntentContent(ctx context.Context, tx *sql.Tx, id MutationID, tenant TenantID, intent MutationIntent) error {
	var kind Kind
	var ref ContentRef
	switch {
	case intent.Create != nil:
		kind = intent.Create.Spec.Kind
		ref = intent.Create.Spec.Content
	case intent.Revise != nil && intent.Revise.Spec.Content != nil:
		current, err := currentObject(ctx, tx, tenant, intent.Revise.Object, false)
		if err != nil {
			return err
		}
		kind = current.Kind
		ref = intent.Revise.Spec.Content.Ref
	case intent.Replace != nil && intent.Replace.Content != nil:
		current, err := c.replaceSourceObject(ctx, tx, tenant, intent)
		if err != nil {
			return fmt.Errorf("catalog: claim replace source: %w", err)
		}
		kind = current.Kind
		ref = intent.Replace.Content.Ref
	default:
		return nil
	}
	if kind != KindFile {
		return c.validateContentRef(ctx, tx, kind, ref)
	}
	if err := c.validateContentRef(ctx, tx, kind, ref); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
UPDATE content_stages SET mutation_id = ?
WHERE stage_id = ? AND owner_id = ? AND mutation_id IS NULL AND published = 1`,
		id[:], ref.Stage[:], c.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: transfer content stage to prepared mutation: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect prepared content transfer: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("%w: content stage ownership changed during prepare", ErrInvalidTransition)
	}
	return nil
}

func (c *Catalog) replaceSourceObject(
	ctx context.Context,
	query rowQuerier,
	tenant TenantID,
	intent MutationIntent,
) (Object, error) {
	if intent.Replace == nil {
		return Object{}, ErrInvalidTransition
	}
	current, err := currentObjectFromQuery(ctx, query, tenant, intent.Replace.Source)
	if err == nil || !errors.Is(err, ErrNotFound) {
		return current, err
	}
	private, found, err := readPrivatePromotionSource(
		ctx, query, tenant, intent.Replace.Source, intent.SourceID,
	)
	if err != nil {
		return Object{}, err
	}
	if !found {
		return Object{}, ErrNotFound
	}
	return private.object(), nil
}

func currentObjectFromQuery(ctx context.Context, query rowQuerier, tenant TenantID, id ObjectID) (Object, error) {
	view, err := readCatalogView(ctx, query, tenant)
	if err != nil {
		return Object{}, err
	}
	return currentObjectFromView(ctx, query, view, tenant, id, false, "")
}
