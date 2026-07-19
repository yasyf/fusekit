package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	sqlite3 "modernc.org/sqlite"
	sqlite3lib "modernc.org/sqlite/lib"
)

const (
	mutationAfterBegin    = "mutation.after_begin"
	mutationAfterRevision = "mutation.after_revision"
	mutationAfterApply    = "mutation.after_apply"
	mutationAfterJournal  = "mutation.after_journal"
	mutationAfterOutbox   = "mutation.after_outbox"
	mutationBeforeCommit  = "mutation.before_commit"
	mutationAfterCommit   = "mutation.after_commit"
)

type mutationApply func(context.Context, *sql.Tx, Revision) (ObjectID, ObjectID, error)

var errMutationHeadChanged = errors.New("catalog: prepared mutation expected head changed")

// CreateTenant creates a tenant and its stable root directory.
func (c *Catalog) CreateTenant(ctx context.Context, mutation MutationID, tenant TenantID, policy CasePolicy, presentations PresentationSet) (Object, error) {
	if policy != CaseSensitive && policy != CaseInsensitive {
		return Object{}, fmt.Errorf("%w: unknown case policy %d", ErrInvalidObject, policy)
	}
	if !presentations.valid() {
		return Object{}, fmt.Errorf("%w: invalid tenant presentation set %d", ErrInvalidObject, presentations)
	}
	root := objectFromMutation(mutation)
	request := struct {
		Policy        CasePolicy
		Presentations PresentationSet
	}{policy, presentations}
	record, err := c.applyMutation(ctx, mutation, tenant, MutationCreateTenant, request,
		func(ctx context.Context, tx *sql.Tx, revision Revision) (ObjectID, ObjectID, error) {
			if _, err := tx.ExecContext(ctx, `
INSERT INTO tenants(tenant, root_id, case_policy, presentation_set, head, floor) VALUES (?, ?, ?, ?, ?, 0)`,
				string(tenant), root[:], uint8(policy), uint8(presentations), uint64(revision)); err != nil {
				return ObjectID{}, ObjectID{}, mapConstraint(err)
			}
			obj := Object{
				Tenant: tenant, ID: root, Parent: root, Revision: revision,
				MetadataRevision: revision, Name: "", Kind: KindDirectory,
				Visibility: Visibility{
					Mount: presentations.Has(PresentationMount), FileProvider: presentations.Has(PresentationFileProvider),
				},
			}
			if err := writeNewObject(ctx, tx, obj); err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			return root, ObjectID{}, nil
		})
	if err != nil {
		return Object{}, err
	}
	return c.objectAt(ctx, tenant, ObjectID(record.Primary), record.Revision)
}

func (c *Catalog) create(ctx context.Context, mutation MutationID, tenant TenantID, expectedHead Revision, spec CreateSpec, origin CausalOrigin) (Object, error) {
	id := objectFromMutation(mutation)
	if err := validateCreateSpec(spec); err != nil {
		return Object{}, err
	}
	record, err := c.applyPreparedMutation(ctx, mutation, tenant, MutationCreate, spec, expectedHead, origin,
		func(ctx context.Context) error {
			return c.verifyMutationContentRef(ctx, c.readDB, mutation, spec.Kind, spec.Content)
		},
		func(ctx context.Context, tx *sql.Tx, revision Revision) (ObjectID, ObjectID, error) {
			parent, err := currentObject(ctx, tx, tenant, spec.Parent, false)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			if parent.Kind != KindDirectory {
				return ObjectID{}, ObjectID{}, fmt.Errorf("%w: parent is not a directory", ErrInvalidObject)
			}
			obj := Object{
				Tenant: tenant, ID: id, Parent: spec.Parent, Revision: revision,
				MetadataRevision: revision, ContentRevision: spec.ContentRevision,
				Name: spec.Name, Kind: spec.Kind, Mode: spec.Mode, Size: spec.Content.Size,
				Hash: spec.Content.Hash, Convergence: spec.Convergence, Visibility: spec.Visibility,
			}
			if err := c.consumeContentStage(ctx, tx, mutation, spec.Kind, spec.Content); err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			if err := writeNewObject(ctx, tx, obj); err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			for _, presentation := range catalogPresentations() {
				if !obj.Visibility.Has(presentation) {
					continue
				}
				if err := writeChange(ctx, tx, tenant, revision, EnumerationScope{Kind: EnumerationContainer, Presentation: presentation, Parent: obj.Parent}, 0, ChangeUpsert, id, obj.Revision); err != nil {
					return ObjectID{}, ObjectID{}, err
				}
			}
			return id, ObjectID{}, nil
		})
	if err != nil {
		return Object{}, err
	}
	return c.objectAt(ctx, tenant, ObjectID(record.Primary), record.Revision)
}

func (c *Catalog) revise(ctx context.Context, mutation MutationID, tenant TenantID, expectedHead Revision, id ObjectID, spec RevisionSpec, origin CausalOrigin) (Object, error) {
	if err := validateRevisionSpec(spec); err != nil {
		return Object{}, err
	}
	request := struct {
		ID   ObjectID
		Spec RevisionSpec
	}{id, spec}
	var prepare func(context.Context) error
	if spec.Content != nil {
		prepare = func(ctx context.Context) error {
			current, err := c.lookupAnyObject(ctx, tenant, id)
			if err != nil {
				return err
			}
			return c.verifyMutationContentRef(ctx, c.readDB, mutation, current.Kind, spec.Content.Ref)
		}
	}
	record, err := c.applyPreparedMutation(ctx, mutation, tenant, MutationRevise, request, expectedHead, origin, prepare,
		func(ctx context.Context, tx *sql.Tx, revision Revision) (ObjectID, ObjectID, error) {
			current, err := currentObject(ctx, tx, tenant, id, false)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			parent, err := currentObject(ctx, tx, tenant, spec.Parent, false)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			if parent.Kind != KindDirectory {
				return ObjectID{}, ObjectID{}, fmt.Errorf("%w: parent is not a directory", ErrInvalidObject)
			}
			if err := validateNext(current, spec); err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			next := current
			next.Revision = revision
			if current.Parent != spec.Parent || current.Name != spec.Name || current.Mode != spec.Mode {
				next.MetadataRevision = revision
			}
			next.Parent = spec.Parent
			next.Name = spec.Name
			next.Mode = spec.Mode
			next.Visibility = spec.Visibility
			if spec.Content != nil {
				next.ContentRevision = spec.Content.Revision
				next.Size = spec.Content.Ref.Size
				next.Hash = spec.Content.Ref.Hash
				if err := c.consumeContentStage(ctx, tx, mutation, next.Kind, spec.Content.Ref); err != nil {
					return ObjectID{}, ObjectID{}, err
				}
			}
			next.Convergence = spec.Convergence
			if err := writeObjectRevision(ctx, tx, next); err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			metadataChanged := current.Parent != next.Parent || current.Name != next.Name || current.Mode != next.Mode || current.Visibility != next.Visibility
			for _, presentation := range catalogPresentations() {
				wasVisible := current.Visibility.Has(presentation)
				isVisible := next.Visibility.Has(presentation)
				if wasVisible && (!isVisible || current.Parent != next.Parent) {
					if err := writeChange(ctx, tx, tenant, revision, EnumerationScope{Kind: EnumerationContainer, Presentation: presentation, Parent: current.Parent}, 0, ChangeDelete, id, current.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
				if isVisible && (!wasVisible || metadataChanged) {
					if err := writeChange(ctx, tx, tenant, revision, EnumerationScope{Kind: EnumerationContainer, Presentation: presentation, Parent: next.Parent}, 0, ChangeUpsert, id, next.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
			}
			if current.Visibility.FileProvider || next.Visibility.FileProvider {
				owners, err := liveInterestOwners(ctx, tx, tenant, id)
				if err != nil {
					return ObjectID{}, ObjectID{}, err
				}
				for _, owner := range owners {
					if owner.Presentation != PresentationFileProvider {
						continue
					}
					if current.Visibility.FileProvider && !next.Visibility.FileProvider {
						if err := writeChange(ctx, tx, tenant, revision, workingSetScope(owner), 0, ChangeDelete, id, current.Revision); err != nil {
							return ObjectID{}, ObjectID{}, err
						}
					} else if next.Visibility.FileProvider && (!current.Visibility.FileProvider || spec.Content != nil || current.Convergence != next.Convergence) {
						if err := writeChange(ctx, tx, tenant, revision, workingSetScope(owner), 0, ChangeUpsert, id, next.Revision); err != nil {
							return ObjectID{}, ObjectID{}, err
						}
					}
				}
			}
			return id, ObjectID{}, nil
		})
	if err != nil {
		return Object{}, err
	}
	return c.objectAt(ctx, tenant, ObjectID(record.Primary), record.Revision)
}

func (c *Catalog) delete(ctx context.Context, mutation MutationID, tenant TenantID, expectedHead Revision, id ObjectID, origin CausalOrigin) (Object, error) {
	record, err := c.applyPreparedMutation(ctx, mutation, tenant, MutationDelete, id, expectedHead, origin, nil,
		func(ctx context.Context, tx *sql.Tx, revision Revision) (ObjectID, ObjectID, error) {
			current, err := currentObject(ctx, tx, tenant, id, false)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			var root []byte
			if err := tx.QueryRowContext(ctx,
				"SELECT root_id FROM tenants WHERE tenant = ?", string(tenant)).Scan(&root); err != nil {
				return ObjectID{}, ObjectID{}, fmt.Errorf("catalog: read tenant root: %w", err)
			}
			rootID, err := objectID(root)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			if current.ID == rootID {
				return ObjectID{}, ObjectID{}, fmt.Errorf("%w: tenant root cannot be deleted", ErrInvalidObject)
			}
			var children int
			if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM objects WHERE tenant = ? AND parent_id = ? AND tombstone = 0`,
				string(tenant), id[:]).Scan(&children); err != nil {
				return ObjectID{}, ObjectID{}, fmt.Errorf("catalog: count children: %w", err)
			}
			if children != 0 {
				return ObjectID{}, ObjectID{}, fmt.Errorf("%w: directory is not empty", ErrConflict)
			}
			current.Revision = revision
			current.MetadataRevision = revision
			current.Tombstone = true
			wasVisible := current.Visibility
			current.Visibility = Visibility{}
			if err := writeObjectRevision(ctx, tx, current); err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			for _, presentation := range catalogPresentations() {
				if !wasVisible.Has(presentation) {
					continue
				}
				if err := writeChange(ctx, tx, tenant, revision, EnumerationScope{Kind: EnumerationContainer, Presentation: presentation, Parent: current.Parent}, 0, ChangeDelete, id, current.Revision); err != nil {
					return ObjectID{}, ObjectID{}, err
				}
			}
			if wasVisible.FileProvider {
				owners, err := liveInterestOwners(ctx, tx, tenant, id)
				if err != nil {
					return ObjectID{}, ObjectID{}, err
				}
				for _, owner := range owners {
					if owner.Presentation != PresentationFileProvider {
						continue
					}
					if err := writeChange(ctx, tx, tenant, revision, workingSetScope(owner), 0, ChangeDelete, id, current.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
			}
			return id, ObjectID{}, nil
		})
	if err != nil {
		return Object{}, err
	}
	return c.objectAt(ctx, tenant, ObjectID(record.Primary), record.Revision)
}

func (c *Catalog) replace(ctx context.Context, mutation MutationID, tenant TenantID, expectedHead Revision, spec ReplaceMutation, origin CausalOrigin) (ReplaceResult, error) {
	if spec.Source == spec.Target {
		return ReplaceResult{}, fmt.Errorf("%w: replace source equals target", ErrInvalidObject)
	}
	var prepare func(context.Context) error
	if spec.Content != nil {
		prepare = func(ctx context.Context) error {
			current, err := c.lookupAnyObject(ctx, tenant, spec.Source)
			if err != nil {
				return err
			}
			return c.verifyMutationContentRef(ctx, c.readDB, mutation, current.Kind, spec.Content.Ref)
		}
	}
	record, err := c.applyPreparedMutation(ctx, mutation, tenant, MutationReplace, spec, expectedHead, origin, prepare,
		func(ctx context.Context, tx *sql.Tx, revision Revision) (ObjectID, ObjectID, error) {
			source, err := currentObject(ctx, tx, tenant, spec.Source, false)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			target, err := currentObject(ctx, tx, tenant, spec.Target, false)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			if source.Kind != target.Kind {
				return ObjectID{}, ObjectID{}, fmt.Errorf("%w: replace kinds differ", ErrInvalidObject)
			}
			if target.Visibility == (Visibility{}) {
				return ObjectID{}, ObjectID{}, fmt.Errorf("%w: replace target is not visible", ErrInvalidObject)
			}
			sourceBefore := source
			targetBefore := target
			finalParent := target.Parent
			if spec.Parent != nil {
				finalParent = *spec.Parent
			}
			finalName := target.Name
			if spec.Name != nil {
				finalName = *spec.Name
			}
			finalMode := source.Mode
			if spec.Mode != nil {
				finalMode = *spec.Mode
			}
			finalVisibility := Visibility{
				Mount:        source.Visibility.Mount || target.Visibility.Mount,
				FileProvider: source.Visibility.FileProvider || target.Visibility.FileProvider,
			}
			if spec.Visibility != nil {
				finalVisibility = *spec.Visibility
			}

			target.Revision = revision
			target.MetadataRevision = revision
			target.Tombstone = true
			target.Visibility = Visibility{}
			if err := writeObjectRevision(ctx, tx, target); err != nil {
				return ObjectID{}, ObjectID{}, err
			}

			source.Revision = revision
			source.MetadataRevision = revision
			source.Parent = finalParent
			source.Name = finalName
			source.Mode = finalMode
			source.Visibility = finalVisibility
			if spec.Content != nil {
				source.ContentRevision = spec.Content.Revision
				source.Size = spec.Content.Ref.Size
				source.Hash = spec.Content.Ref.Hash
				if err := c.consumeContentStage(ctx, tx, mutation, source.Kind, spec.Content.Ref); err != nil {
					return ObjectID{}, ObjectID{}, err
				}
			}
			if err := writeObjectRevision(ctx, tx, source); err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			sequences := make(map[EnumerationScope]uint32)
			writeScoped := func(scope EnumerationScope, kind ChangeKind, id ObjectID, objectRevision Revision) error {
				sequence := sequences[scope]
				if err := writeChange(ctx, tx, tenant, revision, scope, sequence, kind, id, objectRevision); err != nil {
					return err
				}
				sequences[scope] = sequence + 1
				return nil
			}
			for _, presentation := range catalogPresentations() {
				if sourceBefore.Visibility.Has(presentation) && (!source.Visibility.Has(presentation) || sourceBefore.Parent != source.Parent) {
					if err := writeScoped(EnumerationScope{Kind: EnumerationContainer, Presentation: presentation, Parent: sourceBefore.Parent}, ChangeDelete, spec.Source, sourceBefore.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
				if targetBefore.Visibility.Has(presentation) {
					if err := writeScoped(EnumerationScope{Kind: EnumerationContainer, Presentation: presentation, Parent: targetBefore.Parent}, ChangeDelete, spec.Target, target.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
				if source.Visibility.Has(presentation) {
					if err := writeScoped(EnumerationScope{Kind: EnumerationContainer, Presentation: presentation, Parent: source.Parent}, ChangeUpsert, spec.Source, source.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
			}
			targetWasFileProviderVisible := targetBefore.Visibility.FileProvider
			sourceWasFileProviderVisible := sourceBefore.Visibility.FileProvider
			sourceIsFileProviderVisible := source.Visibility.FileProvider
			sourceOwnersBefore, err := liveInterestOwners(ctx, tx, tenant, spec.Source)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			targetOwners, err := liveInterestOwners(ctx, tx, tenant, spec.Target)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			if err := transferInterests(ctx, tx, mutation, tenant, spec.Source, spec.Target, revision); err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			sourceOwnersAfter, err := liveInterestOwners(ctx, tx, tenant, spec.Source)
			if err != nil {
				return ObjectID{}, ObjectID{}, err
			}
			if targetWasFileProviderVisible {
				for _, owner := range targetOwners {
					if owner.Presentation != PresentationFileProvider {
						continue
					}
					if err := writeScoped(workingSetScope(owner), ChangeDelete, spec.Target, target.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
			}
			if sourceIsFileProviderVisible {
				for _, owner := range sourceOwnersAfter {
					if owner.Presentation != PresentationFileProvider {
						continue
					}
					if err := writeScoped(workingSetScope(owner), ChangeUpsert, spec.Source, source.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
			} else if sourceWasFileProviderVisible {
				for _, owner := range sourceOwnersBefore {
					if owner.Presentation != PresentationFileProvider {
						continue
					}
					if err := writeScoped(workingSetScope(owner), ChangeDelete, spec.Source, sourceBefore.Revision); err != nil {
						return ObjectID{}, ObjectID{}, err
					}
				}
			}
			return spec.Source, spec.Target, nil
		})
	if err != nil {
		return ReplaceResult{}, err
	}
	source, err := c.objectAt(ctx, tenant, ObjectID(record.Primary), record.Revision)
	if err != nil {
		return ReplaceResult{}, err
	}
	target, err := c.objectAt(ctx, tenant, ObjectID(record.Secondary), record.Revision)
	if err != nil {
		return ReplaceResult{}, err
	}
	return ReplaceResult{Revision: record.Revision, Source: source, Target: target}, nil
}

func (c *Catalog) applyMutation(
	ctx context.Context,
	id MutationID,
	tenant TenantID,
	kind MutationKind,
	request any,
	apply mutationApply,
) (record MutationRecord, err error) {
	return c.applyPreparedMutation(ctx, id, tenant, kind, request, 0, defaultCausalOrigin(kind), nil, apply)
}

func (c *Catalog) applyPreparedMutation(
	ctx context.Context,
	id MutationID,
	tenant TenantID,
	kind MutationKind,
	request any,
	expectedHead Revision,
	origin CausalOrigin,
	prepare func(context.Context) error,
	apply mutationApply,
) (record MutationRecord, err error) {
	if err := validateMutationIdentity(id, tenant); err != nil {
		return MutationRecord{}, err
	}
	digest, err := requestDigest(tenant, kind, request)
	if err != nil {
		return MutationRecord{}, err
	}
	if prepare != nil {
		existing, found, err := mutationRecord(ctx, c.readDB, id)
		if err != nil {
			return MutationRecord{}, err
		}
		if found {
			if existing.Tenant != tenant || existing.Kind != kind || existing.digest != digest {
				return MutationRecord{}, ErrMutationConflict
			}
			return existing.MutationRecord, nil
		}
		if err := prepare(ctx); err != nil {
			existing, found, loadErr := mutationRecord(ctx, c.readDB, id)
			if loadErr != nil {
				return MutationRecord{}, errors.Join(err, loadErr)
			}
			if found {
				if existing.Tenant != tenant || existing.Kind != kind || existing.digest != digest {
					return MutationRecord{}, ErrMutationConflict
				}
				return existing.MutationRecord, nil
			}
			return MutationRecord{}, err
		}
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return MutationRecord{}, fmt.Errorf("catalog: begin mutation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := c.trip(mutationAfterBegin); err != nil {
		return MutationRecord{}, err
	}

	existing, found, err := mutationRecord(ctx, tx, id)
	if err != nil {
		return MutationRecord{}, err
	}
	if found {
		if existing.Tenant != tenant || existing.Kind != kind || existing.digest != digest {
			return MutationRecord{}, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return MutationRecord{}, fmt.Errorf("catalog: finish mutation lookup: %w", err)
		}
		return existing.MutationRecord, nil
	}

	revision := Revision(1)
	if kind != MutationCreateTenant {
		if expectedHead == 0 {
			var active int
			if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM prepared_mutations WHERE tenant = ? AND state <> ?`, string(tenant), uint8(MutationCommitted)).Scan(&active); err != nil {
				return MutationRecord{}, fmt.Errorf("catalog: inspect active mutation before head change: %w", err)
			}
			if active != 0 {
				return MutationRecord{}, ErrMutationActive
			}
		}
		var rawRevision uint64
		statement := "UPDATE tenants SET head = head + 1 WHERE tenant = ?"
		args := []any{string(tenant)}
		if expectedHead != 0 {
			statement += " AND head = ?"
			args = append(args, uint64(expectedHead))
		}
		statement += " RETURNING head"
		if err := tx.QueryRowContext(ctx, statement, args...).Scan(&rawRevision); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				if expectedHead != 0 {
					return MutationRecord{}, errMutationHeadChanged
				}
				return MutationRecord{}, ErrNotFound
			}
			return MutationRecord{}, fmt.Errorf("catalog: allocate revision: %w", err)
		}
		revision = Revision(rawRevision)
	}
	if err := c.trip(mutationAfterRevision); err != nil {
		return MutationRecord{}, err
	}
	primary, secondary, err := apply(ctx, tx, revision)
	if err != nil {
		return MutationRecord{}, err
	}
	if err := c.trip(mutationAfterApply); err != nil {
		return MutationRecord{}, err
	}
	if err := insertMutation(ctx, tx, id, tenant, kind, digest, revision, primary, secondary); err != nil {
		return MutationRecord{}, err
	}
	if err := c.trip(mutationAfterJournal); err != nil {
		return MutationRecord{}, err
	}
	if err := insertConvergenceOutbox(ctx, tx, id, tenant, revision, origin); err != nil {
		return MutationRecord{}, err
	}
	if err := c.trip(mutationAfterOutbox); err != nil {
		return MutationRecord{}, err
	}
	if err := c.trip(mutationBeforeCommit); err != nil {
		return MutationRecord{}, err
	}
	if err := tx.Commit(); err != nil {
		return MutationRecord{}, fmt.Errorf("catalog: commit mutation: %w", err)
	}
	record = MutationRecord{
		ID: id, Tenant: tenant, Kind: kind, Revision: revision,
		Primary: primary, Secondary: secondary,
	}
	if err := c.trip(mutationAfterCommit); err != nil {
		return MutationRecord{}, err
	}
	return record, nil
}

type journalRecord struct {
	MutationRecord
	digest [32]byte
}

func mutationRecord(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, id MutationID) (journalRecord, bool, error) {
	var rawID, digest, primary, secondary []byte
	var tenant string
	var kind uint8
	var revision uint64
	err := q.QueryRowContext(ctx, `
SELECT mutation_id, tenant, kind, request_hash, revision, primary_object, secondary_object
FROM mutation_journal WHERE mutation_id = ?`, id[:]).Scan(
		&rawID, &tenant, &kind, &digest, &revision, &primary, &secondary,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return journalRecord{}, false, nil
	}
	if err != nil {
		return journalRecord{}, false, fmt.Errorf("catalog: read mutation journal: %w", err)
	}
	parsedID, err := mutationID(rawID)
	if err != nil {
		return journalRecord{}, false, err
	}
	primaryID, err := objectID(primary)
	if err != nil {
		return journalRecord{}, false, err
	}
	var secondaryID ObjectID
	if secondary != nil {
		secondaryID, err = objectID(secondary)
		if err != nil {
			return journalRecord{}, false, err
		}
	}
	if len(digest) != sha256.Size {
		return journalRecord{}, false, fmt.Errorf("catalog: corrupt mutation digest length %d", len(digest))
	}
	var parsedDigest [32]byte
	copy(parsedDigest[:], digest)
	return journalRecord{
		MutationRecord: MutationRecord{
			ID: parsedID, Tenant: TenantID(tenant), Kind: MutationKind(kind),
			Revision: Revision(revision), Primary: primaryID, Secondary: secondaryID,
		},
		digest: parsedDigest,
	}, true, nil
}

// Mutation returns a durable mutation outcome after commit or restart.
func (c *Catalog) Mutation(ctx context.Context, id MutationID) (MutationRecord, error) {
	record, found, err := mutationRecord(ctx, c.readDB, id)
	if err != nil {
		return MutationRecord{}, err
	}
	if !found {
		return MutationRecord{}, ErrNotFound
	}
	return record.MutationRecord, nil
}

func requestDigest(tenant TenantID, kind MutationKind, request any) ([32]byte, error) {
	payload, err := json.Marshal(struct {
		Tenant  TenantID
		Kind    MutationKind
		Request any
	}{tenant, kind, request})
	if err != nil {
		return [32]byte{}, fmt.Errorf("catalog: encode mutation request: %w", err)
	}
	return sha256.Sum256(payload), nil
}

func insertMutation(
	ctx context.Context,
	tx *sql.Tx,
	id MutationID,
	tenant TenantID,
	kind MutationKind,
	digest [32]byte,
	revision Revision,
	primary, secondary ObjectID,
) error {
	var secondaryValue any
	if !zeroObjectID(secondary) {
		secondaryValue = secondary[:]
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO mutation_journal(
    mutation_id, tenant, kind, request_hash, revision, primary_object, secondary_object
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		id[:], string(tenant), uint8(kind), digest[:], uint64(revision), primary[:], secondaryValue); err != nil {
		return fmt.Errorf("catalog: write mutation journal: %w", err)
	}
	return nil
}

func writeNewObject(ctx context.Context, tx *sql.Tx, obj Object) error {
	policy, err := tenantCasePolicy(ctx, tx, obj.Tenant)
	if err != nil {
		return err
	}
	key := normalizeName(policy, obj.Name)
	if err := insertObjectVersion(ctx, tx, obj, key); err != nil {
		return err
	}
	args := objectArgs(obj, key)
	if _, err := tx.ExecContext(ctx, `
INSERT INTO objects(
    tenant, object_id, parent_id, revision, metadata_revision, content_revision,
    name, name_key, kind, mode, size, hash, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, args...); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func writeObjectRevision(ctx context.Context, tx *sql.Tx, obj Object) error {
	policy, err := tenantCasePolicy(ctx, tx, obj.Tenant)
	if err != nil {
		return err
	}
	key := normalizeName(policy, obj.Name)
	if err := insertObjectVersion(ctx, tx, obj, key); err != nil {
		return err
	}
	args := append(objectArgs(obj, key), string(obj.Tenant), obj.ID[:])
	if _, err := tx.ExecContext(ctx, `
UPDATE objects SET
    tenant = ?, object_id = ?, parent_id = ?, revision = ?, metadata_revision = ?,
    content_revision = ?, name = ?, name_key = ?, kind = ?, mode = ?, size = ?, hash = ?,
    desired_revision = ?, observed_revision = ?, verified_revision = ?,
    applied_revision = ?, mount_visible = ?, file_provider_visible = ?, tombstone = ?
WHERE tenant = ? AND object_id = ?`, args...); err != nil {
		return mapConstraint(err)
	}
	return nil
}

func insertObjectVersion(ctx context.Context, tx *sql.Tx, obj Object, key string) error {
	if _, err := tx.ExecContext(ctx, `
INSERT INTO object_versions(
    tenant, object_id, parent_id, revision, metadata_revision, content_revision,
    name, name_key, kind, mode, size, hash, desired_revision, observed_revision,
    verified_revision, applied_revision, mount_visible, file_provider_visible, tombstone
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, objectArgs(obj, key)...); err != nil {
		return fmt.Errorf("catalog: append object revision: %w", err)
	}
	return nil
}

func objectArgs(obj Object, key string) []any {
	return []any{
		string(obj.Tenant), obj.ID[:], obj.Parent[:], uint64(obj.Revision),
		uint64(obj.MetadataRevision), uint64(obj.ContentRevision), obj.Name, key,
		uint8(obj.Kind), obj.Mode, obj.Size, obj.Hash[:],
		uint64(obj.Convergence.Desired), uint64(obj.Convergence.Observed),
		uint64(obj.Convergence.Verified), uint64(obj.Convergence.Applied),
		obj.Visibility.Mount, obj.Visibility.FileProvider, obj.Tombstone,
	}
}

func writeChange(ctx context.Context, tx *sql.Tx, tenant TenantID, revision Revision, scope EnumerationScope, sequence uint32, kind ChangeKind, id ObjectID, objectRevision Revision) error {
	if sequence == CompleteChangeSequence {
		return fmt.Errorf("%w: change sequence exhausted", ErrIntegrity)
	}
	scopeKind, presentation, scopeParent, scopeDomain, scopeGeneration, err := enumerationScopeKey(scope)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO changes(tenant, revision, scope_kind, presentation, scope_parent, scope_domain, scope_generation, sequence, kind, object_id, object_revision)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		string(tenant), uint64(revision), scopeKind, presentation, scopeParent, scopeDomain, scopeGeneration, sequence, uint8(kind), id[:], uint64(objectRevision)); err != nil {
		return fmt.Errorf("catalog: append change: %w", err)
	}
	return nil
}

func currentObject(ctx context.Context, tx *sql.Tx, tenant TenantID, id ObjectID, tombstone bool) (Object, error) {
	query := "SELECT " + objectColumns +
		" FROM objects WHERE tenant = ? AND object_id = ? AND tombstone = ?"
	obj, err := scanObject(tx.QueryRowContext(ctx, query, string(tenant), id[:], tombstone))
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read current object: %w", err)
	}
	return obj, nil
}

func (c *Catalog) objectAt(ctx context.Context, tenant TenantID, id ObjectID, revision Revision) (Object, error) {
	query := "SELECT " + objectColumns + `
 FROM object_versions WHERE tenant = ? AND object_id = ? AND revision = ?`
	obj, err := scanObject(c.readDB.QueryRowContext(ctx, query, string(tenant), id[:], uint64(revision)))
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read object revision: %w", err)
	}
	return obj, nil
}

func validateMutationIdentity(id MutationID, tenant TenantID) error {
	if zeroMutationID(id) {
		return fmt.Errorf("%w: mutation id is zero", ErrInvalidObject)
	}
	if _, err := NewTenantID(string(tenant)); err != nil {
		return err
	}
	return nil
}

func validateCreateSpec(spec CreateSpec) error {
	if zeroObjectID(spec.Parent) {
		return fmt.Errorf("%w: parent id is zero", ErrInvalidObject)
	}
	if err := validateName(spec.Name); err != nil {
		return err
	}
	if err := validateKindContent(spec.Kind, spec.ContentRevision, spec.Content); err != nil {
		return err
	}
	return validateConvergence(Convergence{}, spec.Convergence)
}

func validateRevisionSpec(spec RevisionSpec) error {
	if zeroObjectID(spec.Parent) {
		return fmt.Errorf("%w: parent id is zero", ErrInvalidObject)
	}
	if err := validateName(spec.Name); err != nil {
		return err
	}
	return nil
}

func validateNext(current Object, spec RevisionSpec) error {
	if spec.Content != nil {
		if err := validateKindContent(current.Kind, spec.Content.Revision, spec.Content.Ref); err != nil {
			return err
		}
		if spec.Content.Revision <= current.ContentRevision {
			return fmt.Errorf("%w: content update revision did not advance", ErrInvalidTransition)
		}
	}
	return validateConvergence(current.Convergence, spec.Convergence)
}

func validateKindContent(kind Kind, revision Revision, content ContentRef) error {
	if content.Size < 0 {
		return fmt.Errorf("%w: negative content size", ErrInvalidObject)
	}
	switch kind {
	case KindDirectory:
		if revision != 0 || content != (ContentRef{}) {
			return fmt.Errorf("%w: directory carries file content", ErrInvalidObject)
		}
	case KindFile:
		if revision == 0 {
			return fmt.Errorf("%w: file content revision is zero", ErrInvalidObject)
		}
	default:
		return fmt.Errorf("%w: unknown kind %d", ErrInvalidObject, kind)
	}
	return nil
}

func validateConvergence(previous, next Convergence) error {
	if next.Applied > next.Verified || next.Verified > next.Observed || next.Observed > next.Desired {
		return fmt.Errorf("%w: convergence proof order", ErrInvalidTransition)
	}
	if next.Desired < previous.Desired || next.Observed < previous.Observed ||
		next.Verified < previous.Verified || next.Applied < previous.Applied {
		return fmt.Errorf("%w: convergence revision regressed", ErrInvalidTransition)
	}
	return nil
}

func validateName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsRune(name, '/') || strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: invalid name %q", ErrInvalidObject, name)
	}
	return nil
}

func mapConstraint(err error) error {
	var sqliteErr *sqlite3.Error
	if errors.As(err, &sqliteErr) {
		switch sqliteErr.Code() {
		case sqlite3lib.SQLITE_CONSTRAINT_UNIQUE, sqlite3lib.SQLITE_CONSTRAINT_PRIMARYKEY:
			return fmt.Errorf("%w: %v", ErrConflict, err)
		}
	}
	return fmt.Errorf("catalog: write object: %w", err)
}

func zeroObjectID(id ObjectID) bool { return id == ObjectID{} }

func zeroMutationID(id MutationID) bool { return id == MutationID{} }

func objectFromMutation(id MutationID) ObjectID {
	digest := sha256.Sum256(append([]byte("fusekit.catalog.object\x00"), id[:]...))
	var object ObjectID
	copy(object[:], digest[:len(object)])
	return object
}
