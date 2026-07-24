package catalog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

// PrepareMutationSource durably attaches the catalog's exact authority-owned
// locators to one claimed external mutation.
func (c *Catalog) PrepareMutationSource(ctx context.Context, id MutationID, claim MutationClaim) (PreparedMutation, error) {
	if err := validateMutationClaim(claim); err != nil {
		return PreparedMutation{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: begin source mutation context: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, found, err := readPreparedMutation(ctx, tx, id)
	if err != nil {
		return PreparedMutation{}, err
	}
	if !found {
		return PreparedMutation{}, ErrNotFound
	}
	if record.State != MutationApplying || record.Claim == nil || *record.Claim != claim {
		return PreparedMutation{}, ErrMutationClaimed
	}
	derived, err := deriveSourceMutationContext(ctx, tx, record.PreparedMutation)
	if err != nil {
		return PreparedMutation{}, err
	}
	if record.Source != nil {
		if !sourceMutationContextsEqual(*record.Source, derived) {
			return PreparedMutation{}, ErrSourceLocatorStale
		}
		if err := tx.Commit(); err != nil {
			return PreparedMutation{}, fmt.Errorf("catalog: finish source mutation context lookup: %w", err)
		}
		return record.PreparedMutation, nil
	}
	payload, err := json.Marshal(derived)
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: encode source mutation context: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
UPDATE prepared_mutations SET source_context_json = ?
WHERE mutation_id = ? AND state = ? AND claim_owner = ? AND claim_epoch = ? AND source_context_json IS NULL`,
		payload, id[:], uint8(MutationApplying), claim.Owner[:], claim.Epoch)
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: persist source mutation context: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: inspect source mutation context update: %w", err)
	}
	if changed != 1 {
		return PreparedMutation{}, ErrMutationClaimed
	}
	if err := tx.Commit(); err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: commit source mutation context: %w", err)
	}
	record.Source = &derived
	return record.PreparedMutation, nil
}

// SetMutationSourceResult reserves the authority key selected by product
// policy for a create before its disposable worker starts.
func (c *Catalog) SetMutationSourceResult(ctx context.Context, id MutationID, claim MutationClaim, locator SourceLocator) (PreparedMutation, error) {
	if err := validateMutationClaim(claim); err != nil {
		return PreparedMutation{}, err
	}
	if err := validateSourceLocator(locator); err != nil {
		return PreparedMutation{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: begin source result reservation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	record, found, err := readPreparedMutation(ctx, tx, id)
	if err != nil {
		return PreparedMutation{}, err
	}
	if !found {
		return PreparedMutation{}, ErrNotFound
	}
	if record.State != MutationApplying || record.Claim == nil || *record.Claim != claim {
		return PreparedMutation{}, ErrMutationClaimed
	}
	if record.Kind != MutationCreate || record.Source == nil || record.Source.Parent == nil {
		return PreparedMutation{}, fmt.Errorf("%w: source result is only valid for a resolved create", ErrInvalidTransition)
	}
	if locator.SourceAuthority != record.Source.Parent.SourceAuthority || locator.SourceRevision != record.Source.Parent.SourceRevision {
		return PreparedMutation{}, ErrSourceLocatorStale
	}
	if record.SourceResult != nil {
		if *record.SourceResult != locator {
			return PreparedMutation{}, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return PreparedMutation{}, fmt.Errorf("catalog: finish source result lookup: %w", err)
		}
		return record.PreparedMutation, nil
	}
	var existingID []byte
	err = tx.QueryRowContext(ctx, `
SELECT object_id FROM source_object_ids WHERE source_authority = ? AND source_key = ?`,
		string(locator.SourceAuthority), string(locator.SourceKey)).Scan(&existingID)
	if err == nil {
		return PreparedMutation{}, ErrMutationConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return PreparedMutation{}, fmt.Errorf("catalog: inspect source result identity: %w", err)
	}
	var reserved []byte
	err = tx.QueryRowContext(ctx, `
SELECT mutation_id FROM source_key_reservations WHERE source_authority = ? AND source_key = ?`,
		string(locator.SourceAuthority), string(locator.SourceKey)).Scan(&reserved)
	if err == nil {
		reservedID, parseErr := mutationID(reserved)
		if parseErr != nil {
			return PreparedMutation{}, parseErr
		}
		if reservedID != id {
			return PreparedMutation{}, ErrMutationConflict
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_key_reservations(source_authority, source_key, mutation_id) VALUES (?, ?, ?)`,
			string(locator.SourceAuthority), string(locator.SourceKey), id[:]); err != nil {
			return PreparedMutation{}, mapConstraint(err)
		}
	} else {
		return PreparedMutation{}, fmt.Errorf("catalog: inspect source result reservation: %w", err)
	}
	payload, err := json.Marshal(locator)
	if err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: encode source result: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE prepared_mutations SET source_result_json = ? WHERE mutation_id = ? AND source_result_json IS NULL`, payload, id[:]); err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: persist source result: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return PreparedMutation{}, fmt.Errorf("catalog: commit source result reservation: %w", err)
	}
	record.SourceResult = &locator
	return record.PreparedMutation, nil
}

func deriveSourceMutationContext(ctx context.Context, tx *sql.Tx, prepared PreparedMutation) (SourceMutationContext, error) {
	var authority string
	var sourceRevision uint64
	var rootKey string
	var rawRoot []byte
	err := tx.QueryRowContext(ctx, `
SELECT generation.content_source_id,
       COALESCE(NULLIF(visibility.source_revision, 0), checkpoint.source_revision, watermark.source_revision),
       COALESCE(root.root_key, target.root_key), t.root_id
FROM tenant_activations activation
JOIN tenant_generations generation
  ON generation.tenant_id = activation.tenant_id
 AND generation.generation = activation.active_generation
JOIN tenants t ON t.tenant = activation.tenant_id
LEFT JOIN source_driver_publication_heads visibility
  ON visibility.source_authority = generation.content_source_id
LEFT JOIN source_driver_publication_targets target
  ON target.source_authority = visibility.source_authority
 AND target.publication_id = visibility.publication_id
 AND target.tenant = activation.tenant_id
LEFT JOIN source_driver_checkpoints checkpoint
  ON checkpoint.source_authority = generation.content_source_id
LEFT JOIN source_watermarks watermark
  ON watermark.source_authority = generation.content_source_id
LEFT JOIN source_tenant_roots root
  ON root.source_authority = generation.content_source_id AND root.tenant = activation.tenant_id
WHERE activation.tenant_id = ? AND activation.active_generation IS NOT NULL
  AND COALESCE(NULLIF(visibility.source_revision, 0), checkpoint.source_revision, watermark.source_revision) IS NOT NULL
  AND COALESCE(root.root_key, target.root_key) IS NOT NULL`, string(prepared.Tenant)).Scan(&authority, &sourceRevision, &rootKey, &rawRoot)
	if errors.Is(err, sql.ErrNoRows) {
		return SourceMutationContext{}, ErrSourceLocatorMissing
	}
	if err != nil {
		return SourceMutationContext{}, fmt.Errorf("catalog: read source mutation authority: %w", err)
	}
	root, err := objectID(rawRoot)
	if err != nil {
		return SourceMutationContext{}, err
	}
	head, _, err := effectiveRevisionState(ctx, tx, prepared.Tenant)
	if err != nil {
		return SourceMutationContext{}, err
	}
	if head != prepared.ExpectedHead {
		return SourceMutationContext{}, ErrSourceLocatorStale
	}
	authorityID := causal.SourceAuthorityID(authority)
	revision := causal.Revision(sourceRevision)
	locator := func(id ObjectID) (*SourceLocator, error) {
		key, err := sourceKeyForObject(ctx, tx, authorityID, prepared.Tenant, root, SourceObjectKey(rootKey), id)
		if err != nil {
			return nil, err
		}
		return &SourceLocator{SourceAuthority: authorityID, SourceKey: key, SourceRevision: revision}, nil
	}
	context := SourceMutationContext{}
	switch prepared.Kind {
	case MutationCreate:
		context.Operation = sourceMutationOperation(prepared.Kind, prepared.Intent.Create.Spec.Name, prepared.Intent.Create.Spec.Kind,
			prepared.Intent.Create.Spec.Mode, prepared.Intent.Create.Spec.LinkTarget, prepared.Intent.Create.Spec.Kind == KindFile)
		context.Parent, err = locator(prepared.Intent.Create.Spec.Parent)
	case MutationRevise:
		current, currentErr := currentObject(ctx, tx, prepared.Tenant, prepared.Intent.Revise.Object, false)
		if currentErr != nil {
			return SourceMutationContext{}, currentErr
		}
		context.Operation = sourceMutationOperation(prepared.Kind, prepared.Intent.Revise.Spec.Name, current.Kind,
			prepared.Intent.Revise.Spec.Mode, current.LinkTarget, prepared.Intent.Revise.Spec.Content != nil)
		context.Object, err = locator(current.ID)
		if err == nil {
			context.Parent, err = locator(prepared.Intent.Revise.Spec.Parent)
		}
	case MutationDelete:
		current, currentErr := currentObject(ctx, tx, prepared.Tenant, prepared.Intent.Delete.Object, false)
		if currentErr != nil {
			return SourceMutationContext{}, currentErr
		}
		context.Operation = sourceMutationOperation(prepared.Kind, current.Name, current.Kind, current.Mode, current.LinkTarget, false)
		context.Object, err = locator(current.ID)
		if err == nil {
			context.Parent, err = locator(current.Parent)
		}
	case MutationReplace:
		source, sourceErr := currentObject(ctx, tx, prepared.Tenant, prepared.Intent.Replace.Source, false)
		if sourceErr != nil {
			return SourceMutationContext{}, sourceErr
		}
		target, targetErr := currentObject(ctx, tx, prepared.Tenant, prepared.Intent.Replace.Target, false)
		if targetErr != nil {
			return SourceMutationContext{}, targetErr
		}
		parent := target.Parent
		if prepared.Intent.Replace.Parent != nil {
			parent = *prepared.Intent.Replace.Parent
		}
		name := target.Name
		if prepared.Intent.Replace.Name != nil {
			name = *prepared.Intent.Replace.Name
		}
		mode := source.Mode
		if prepared.Intent.Replace.Mode != nil {
			mode = *prepared.Intent.Replace.Mode
		}
		context.Operation = sourceMutationOperation(prepared.Kind, name, source.Kind, mode, source.LinkTarget, prepared.Intent.Replace.Content != nil)
		context.Object, err = locator(source.ID)
		if err == nil {
			context.Target, err = locator(target.ID)
		}
		if err == nil {
			context.Parent, err = locator(parent)
		}
	default:
		return SourceMutationContext{}, fmt.Errorf("%w: invalid source mutation kind", ErrInvalidTransition)
	}
	if err != nil {
		return SourceMutationContext{}, err
	}
	if err := validateSourceMutationContext(context); err != nil {
		return SourceMutationContext{}, err
	}
	return context, nil
}

func sourceKeyForObject(
	ctx context.Context,
	tx *sql.Tx,
	authority causal.SourceAuthorityID,
	tenant TenantID,
	root ObjectID,
	rootKey SourceObjectKey,
	id ObjectID,
) (SourceObjectKey, error) {
	if id == root {
		return rootKey, nil
	}
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return "", err
	}
	var key string
	if len(view.publication) != 0 {
		if view.authority != string(authority) {
			return "", ErrSourceLocatorStale
		}
		err := tx.QueryRowContext(ctx, `
SELECT source_key FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND object_id = ?`,
			view.authority, view.publication, string(tenant), id[:]).Scan(&key)
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrSourceLocatorMissing
		}
		if err != nil {
			return "", fmt.Errorf("catalog: read active source object locator: %w", err)
		}
		return SourceObjectKey(key), nil
	}
	err = tx.QueryRowContext(ctx, `
SELECT b.source_key
FROM source_object_bindings b
JOIN source_object_ids i
  ON i.source_authority = b.source_authority AND i.source_key = b.source_key
WHERE b.source_authority = ? AND b.tenant = ? AND i.object_id = ?`,
		string(authority), string(tenant), id[:]).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrSourceLocatorMissing
	}
	if err != nil {
		return "", fmt.Errorf("catalog: read source object locator: %w", err)
	}
	return SourceObjectKey(key), nil
}

func sourceMutationOperation(kind MutationKind, name string, objectKind Kind, mode uint32, linkTarget string, hasContent bool) SourceMutationOperation {
	return SourceMutationOperation{
		Kind: kind, Name: name, ObjectKind: objectKind, Mode: mode,
		LinkTarget: linkTarget, HasContent: hasContent,
	}
}

func validateSourceLocator(locator SourceLocator) error {
	if locator.SourceAuthority == "" || !validSourceKey(locator.SourceKey) || locator.SourceRevision == 0 {
		return fmt.Errorf("%w: source locator is incomplete", ErrInvalidObject)
	}
	return nil
}

func validateSourceMutationContext(value SourceMutationContext) error {
	if value.Operation.Kind < MutationCreate || value.Operation.Kind > MutationReplace || value.Operation.ObjectKind < KindDirectory || value.Operation.ObjectKind > KindSymlink {
		return fmt.Errorf("%w: source mutation operation is incomplete", ErrInvalidObject)
	}
	if value.Operation.Name == "" {
		return fmt.Errorf("%w: source mutation name is empty", ErrInvalidObject)
	}
	locators := []*SourceLocator{value.Object, value.Parent, value.Target}
	var authority causal.SourceAuthorityID
	var revision causal.Revision
	for _, locator := range locators {
		if locator == nil {
			continue
		}
		if err := validateSourceLocator(*locator); err != nil {
			return err
		}
		if authority == "" {
			authority, revision = locator.SourceAuthority, locator.SourceRevision
		} else if locator.SourceAuthority != authority || locator.SourceRevision != revision {
			return ErrSourceLocatorStale
		}
	}
	switch value.Operation.Kind {
	case MutationCreate:
		if value.Object != nil || value.Parent == nil || value.Target != nil {
			return fmt.Errorf("%w: create source locators are inconsistent", ErrInvalidObject)
		}
	case MutationRevise, MutationDelete:
		if value.Object == nil || value.Parent == nil || value.Target != nil {
			return fmt.Errorf("%w: object source locators are inconsistent", ErrInvalidObject)
		}
	case MutationReplace:
		if value.Object == nil || value.Parent == nil || value.Target == nil {
			return fmt.Errorf("%w: replace source locators are inconsistent", ErrInvalidObject)
		}
	}
	return nil
}

// ValidateSourceMutationContext verifies one exact catalog-derived source
// mutation context before it crosses a source-driver boundary.
func ValidateSourceMutationContext(value SourceMutationContext) error {
	return validateSourceMutationContext(value)
}

func sourceMutationContextsEqual(left, right SourceMutationContext) bool {
	return left.Operation == right.Operation && sourceLocatorsEqual(left.Object, right.Object) &&
		sourceLocatorsEqual(left.Parent, right.Parent) && sourceLocatorsEqual(left.Target, right.Target)
}

func sourceLocatorsEqual(left, right *SourceLocator) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (c *Catalog) bindPreparedSourceResult(ctx context.Context, prepared PreparedMutation) error {
	if prepared.SourceResult == nil {
		return nil
	}
	if prepared.Kind != MutationCreate || prepared.Source == nil {
		return fmt.Errorf("%w: prepared source result has the wrong mutation shape", ErrIntegrity)
	}
	record, err := c.Mutation(ctx, prepared.Tenant, prepared.OperationID)
	if err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("catalog: begin source result binding: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	locator := *prepared.SourceResult
	var raw []byte
	err = tx.QueryRowContext(ctx, `
SELECT object_id FROM source_object_ids WHERE source_authority = ? AND source_key = ?`,
		string(locator.SourceAuthority), string(locator.SourceKey)).Scan(&raw)
	if err == nil {
		bound, parseErr := objectID(raw)
		if parseErr != nil {
			return parseErr
		}
		if bound != record.Primary {
			return ErrMutationConflict
		}
	} else if errors.Is(err, sql.ErrNoRows) {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_object_ids(source_authority, source_key, object_id) VALUES (?, ?, ?)`,
			string(locator.SourceAuthority), string(locator.SourceKey), record.Primary[:]); err != nil {
			return mapConstraint(err)
		}
	} else {
		return fmt.Errorf("catalog: read source result binding: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_object_bindings(source_authority, tenant, source_key)
VALUES (?, ?, ?) ON CONFLICT(source_authority, tenant, source_key) DO NOTHING`,
		string(locator.SourceAuthority), string(prepared.Tenant), string(locator.SourceKey)); err != nil {
		return mapConstraint(err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM source_key_reservations WHERE mutation_id = ?`, prepared.OperationID[:]); err != nil {
		return fmt.Errorf("catalog: release source result reservation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("catalog: commit source result binding: %w", err)
	}
	return nil
}
