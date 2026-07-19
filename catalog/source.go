package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/yasyf/fusekit/causal"
)

const (
	sourceAfterBegin     = "source.after_begin"
	sourceAfterRevisions = "source.after_revisions"
	sourceAfterApply     = "source.after_apply"
	sourceAfterJournal   = "source.after_journal"
	sourceAfterWatermark = "source.after_watermark"
	sourceAfterOutbox    = "source.after_outbox"
	sourceBeforeCommit   = "source.before_commit"
	sourceAfterCommit    = "source.after_commit"
)

// SourceMode selects a complete authority snapshot or one predecessor-fenced delta.
type SourceMode uint8

const (
	// SourceSnapshot replaces every source-owned object for the authority fleet.
	SourceSnapshot SourceMode = iota + 1
	// SourceDelta applies one exact successor to the current authority watermark.
	SourceDelta
)

// SourceObjectKey is an opaque path-independent identity assigned by the authority.
type SourceObjectKey string

// SourceObject is one complete authoritative object value.
type SourceObject struct {
	Key             SourceObjectKey
	Parent          SourceObjectKey
	Name            string
	Kind            Kind
	Mode            uint32
	ContentRevision Revision
	Content         ContentRef
	LinkTarget      string
	Visibility      Visibility
}

// SourceTenant is one generation-fenced tenant projection in a publication.
type SourceTenant struct {
	Tenant     TenantID
	Generation Generation
	Objects    []SourceObject
	Deletes    []SourceObjectKey
}

// SourcePublication is one immutable authority revision and its complete causal identity.
type SourcePublication struct {
	Mode        SourceMode
	Predecessor causal.Revision
	Change      causal.ChangeSet
	Tenants     []SourceTenant
}

// SourceResult proves the exact catalog commits covered by an authority revision.
type SourceResult struct {
	Authority causal.SourceAuthorityID
	Revision  causal.Revision
	ChangeID  causal.ChangeID
	Operation causal.OperationID
	Commits   []causal.CatalogCommit
}

// ErrSourcePredecessor means a delta did not name the exact durable predecessor.
var ErrSourcePredecessor = errors.New("catalog: source predecessor mismatch")

// ErrSourceRequiresSnapshot means a missing or skipped revision requires a full snapshot.
var ErrSourceRequiresSnapshot = errors.New("catalog: source snapshot required")

// ApplySource atomically publishes one authority revision across every target tenant.
func (c *Catalog) ApplySource(ctx context.Context, publication SourcePublication) (result SourceResult, err error) {
	defer func() {
		err = errors.Join(err, c.releaseSourceStages(context.WithoutCancel(ctx), publication))
	}()
	if err := validateSourcePublication(publication); err != nil {
		return SourceResult{}, err
	}
	for _, target := range publication.Tenants {
		for _, object := range target.Objects {
			if err := c.verifyContentRef(ctx, c.readDB, object.Kind, object.Content); err != nil {
				return SourceResult{}, err
			}
		}
	}
	digest, err := sourcePublicationDigest(publication)
	if err != nil {
		return SourceResult{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return SourceResult{}, fmt.Errorf("catalog: begin source publication: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := c.trip(sourceAfterBegin); err != nil {
		return SourceResult{}, err
	}
	existing, found, err := readSourceOperation(ctx, tx, publication.Change.OperationID)
	if err != nil {
		return SourceResult{}, err
	}
	if found {
		if existing.digest != digest || !sameSourceIdentity(existing.result, publication.Change) {
			return SourceResult{}, ErrMutationConflict
		}
		if err := tx.Commit(); err != nil {
			return SourceResult{}, fmt.Errorf("catalog: finish source replay: %w", err)
		}
		return existing.result, nil
	}
	if err := validateSourceWatermark(ctx, tx, publication); err != nil {
		return SourceResult{}, err
	}
	if err := validateSourceTargets(ctx, tx, publication); err != nil {
		return SourceResult{}, err
	}

	commits := make([]causal.CatalogCommit, 0, len(publication.Tenants))
	catalogOperations := make([]MutationID, 0, len(publication.Tenants))
	for _, target := range publication.Tenants {
		var revision uint64
		if err := tx.QueryRowContext(ctx,
			"UPDATE tenants SET head = head + 1 WHERE tenant = ? RETURNING head", string(target.Tenant)).Scan(&revision); err != nil {
			return SourceResult{}, fmt.Errorf("catalog: advance source tenant %q: %w", target.Tenant, err)
		}
		commit := causal.CatalogCommit{Tenant: causal.TenantID(target.Tenant), CatalogRevision: causal.CatalogRevision(revision)}
		commits = append(commits, commit)
		catalogOperations = append(catalogOperations, sourceCatalogOperation(publication.Change.OperationID, target.Tenant))
	}
	if err := c.trip(sourceAfterRevisions); err != nil {
		return SourceResult{}, err
	}
	for index, target := range publication.Tenants {
		if err := c.applySourceTenant(ctx, tx, publication, target, Revision(commits[index].CatalogRevision)); err != nil {
			return SourceResult{}, err
		}
	}
	if err := c.trip(sourceAfterApply); err != nil {
		return SourceResult{}, err
	}
	result = SourceResult{
		Authority: publication.Change.SourceAuthority, Revision: publication.Change.SourceRevision,
		ChangeID: publication.Change.ChangeID, Operation: publication.Change.OperationID,
		Commits: commits,
	}
	encodedResult, err := json.Marshal(result)
	if err != nil {
		return SourceResult{}, fmt.Errorf("catalog: encode source result: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_operations(
    operation_id, change_id, source_authority, source_revision,
    predecessor_revision, mode, request_hash, result_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, publication.Change.OperationID[:], publication.Change.ChangeID[:],
		string(publication.Change.SourceAuthority), uint64(publication.Change.SourceRevision), uint64(publication.Predecessor),
		uint8(publication.Mode), digest[:], encodedResult); err != nil {
		return SourceResult{}, mapConstraint(err)
	}
	for index, commit := range commits {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_commits(catalog_operation_id, source_operation_id, tenant, generation, catalog_revision)
VALUES (?, ?, ?, ?, ?)`, catalogOperations[index][:], publication.Change.OperationID[:], string(commit.Tenant),
			uint64(publication.Tenants[index].Generation), uint64(commit.CatalogRevision)); err != nil {
			return SourceResult{}, mapConstraint(err)
		}
	}
	if err := c.trip(sourceAfterJournal); err != nil {
		return SourceResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_watermarks(source_authority, source_revision, change_id, operation_id)
VALUES (?, ?, ?, ?)
ON CONFLICT(source_authority) DO UPDATE SET
    source_revision = excluded.source_revision,
    change_id = excluded.change_id,
    operation_id = excluded.operation_id`, string(publication.Change.SourceAuthority), uint64(publication.Change.SourceRevision),
		publication.Change.ChangeID[:], publication.Change.OperationID[:]); err != nil {
		return SourceResult{}, mapConstraint(err)
	}
	if err := c.trip(sourceAfterWatermark); err != nil {
		return SourceResult{}, err
	}
	targets := make([]causal.TenantID, len(publication.Tenants))
	for index, target := range publication.Tenants {
		targets[index] = causal.TenantID(target.Tenant)
	}
	if err := insertConvergenceChange(ctx, tx, publication.Change, targets, false); err != nil {
		return SourceResult{}, err
	}
	for index, commit := range commits {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO convergence_outbox(catalog_operation_id, change_id, tenant, catalog_revision, state)
VALUES (?, ?, ?, ?, ?)`, catalogOperations[index][:], publication.Change.ChangeID[:], string(commit.Tenant),
			uint64(commit.CatalogRevision), uint8(outboxPending)); err != nil {
			return SourceResult{}, mapConstraint(err)
		}
	}
	if err := c.trip(sourceAfterOutbox); err != nil {
		return SourceResult{}, err
	}
	if err := c.trip(sourceBeforeCommit); err != nil {
		return SourceResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return SourceResult{}, fmt.Errorf("catalog: commit source publication: %w", err)
	}
	if err := c.trip(sourceAfterCommit); err != nil {
		return SourceResult{}, err
	}
	return result, nil
}

func validateSourcePublication(publication SourcePublication) error {
	if publication.Mode != SourceSnapshot && publication.Mode != SourceDelta {
		return fmt.Errorf("%w: invalid source mode %d", ErrInvalidObject, publication.Mode)
	}
	if err := validateSourceChange(publication.Change); err != nil {
		return err
	}
	if publication.Change.Cause == causal.CauseOnDemand {
		return fmt.Errorf("%w: source publication cannot be on-demand", ErrInvalidObject)
	}
	if publication.Mode == SourceSnapshot && publication.Predecessor != 0 {
		return fmt.Errorf("%w: snapshots must reset the predecessor", ErrInvalidObject)
	}
	if len(publication.Tenants) == 0 {
		return fmt.Errorf("%w: source publication has no tenants", ErrInvalidObject)
	}
	for index, target := range publication.Tenants {
		if target.Tenant == "" || target.Generation == 0 || (index > 0 && publication.Tenants[index-1].Tenant >= target.Tenant) {
			return fmt.Errorf("%w: source tenants are not sorted and unique", ErrInvalidObject)
		}
		if publication.Mode == SourceSnapshot && len(target.Deletes) != 0 {
			return fmt.Errorf("%w: source snapshot carries explicit deletes", ErrInvalidObject)
		}
		seen := make(map[SourceObjectKey]struct{}, len(target.Objects)+len(target.Deletes))
		for _, object := range target.Objects {
			if err := validateSourceObject(object); err != nil {
				return err
			}
			if _, duplicate := seen[object.Key]; duplicate {
				return fmt.Errorf("%w: duplicate source key %q", ErrInvalidObject, object.Key)
			}
			seen[object.Key] = struct{}{}
		}
		for keyIndex, key := range target.Deletes {
			if !validSourceKey(key) || (keyIndex > 0 && target.Deletes[keyIndex-1] >= key) {
				return fmt.Errorf("%w: source deletes are not sorted and unique", ErrInvalidObject)
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("%w: source key %q is both upserted and deleted", ErrInvalidObject, key)
			}
			seen[key] = struct{}{}
		}
	}
	return nil
}

func validateSourceObject(object SourceObject) error {
	if !validSourceKey(object.Key) || (object.Parent != "" && !validSourceKey(object.Parent)) || object.Parent == object.Key {
		return fmt.Errorf("%w: invalid source object key", ErrInvalidObject)
	}
	if err := validateName(object.Name); err != nil {
		return err
	}
	if !object.Visibility.Mount && !object.Visibility.FileProvider {
		return fmt.Errorf("%w: source object is invisible", ErrInvalidObject)
	}
	if object.Kind == KindDirectory {
		if object.ContentRevision != 0 || object.Content != (ContentRef{}) || object.LinkTarget != "" {
			return fmt.Errorf("%w: source directory carries content", ErrInvalidObject)
		}
		return nil
	}
	if object.Kind == KindSymlink {
		if object.ContentRevision == 0 || object.Content != (ContentRef{}) {
			return fmt.Errorf("%w: source symlink carries staged body content", ErrInvalidObject)
		}
		return validateLinkTarget(object.LinkTarget)
	}
	if object.Kind != KindFile || object.ContentRevision == 0 || object.Content.Stage == (StageID{}) || object.Content.Size < 0 || object.LinkTarget != "" {
		return fmt.Errorf("%w: source file content is incomplete", ErrInvalidObject)
	}
	return nil
}

func validSourceKey(key SourceObjectKey) bool {
	return key != "" && len(key) <= 4096 && !strings.ContainsRune(string(key), 0)
}

func validateSourceWatermark(ctx context.Context, tx *sql.Tx, publication SourcePublication) error {
	var current uint64
	err := tx.QueryRowContext(ctx, "SELECT source_revision FROM source_watermarks WHERE source_authority = ?",
		string(publication.Change.SourceAuthority)).Scan(&current)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("catalog: read source watermark: %w", err)
	}
	if publication.Mode == SourceSnapshot {
		if publication.Change.SourceRevision <= causal.Revision(current) {
			return ErrSourcePredecessor
		}
		return nil
	}
	if current == 0 {
		return ErrSourceRequiresSnapshot
	}
	if publication.Predecessor != causal.Revision(current) {
		return ErrSourcePredecessor
	}
	if publication.Change.SourceRevision != publication.Predecessor+1 {
		return ErrSourceRequiresSnapshot
	}
	return nil
}

func validateSourceTargets(ctx context.Context, tx *sql.Tx, publication SourcePublication) error {
	for _, target := range publication.Tenants {
		provision, found, err := tenantProvision(ctx, tx, target.Tenant)
		if err != nil {
			return err
		}
		if !found {
			return ErrNotFound
		}
		if provision.Generation != target.Generation || provision.ContentSourceID != string(publication.Change.SourceAuthority) {
			return ErrGenerationMismatch
		}
	}
	if publication.Mode != SourceSnapshot {
		return nil
	}
	rows, err := tx.QueryContext(ctx, "SELECT tenant FROM desired_tenants WHERE content_source_id = ? ORDER BY tenant",
		string(publication.Change.SourceAuthority))
	if err != nil {
		return fmt.Errorf("catalog: list source authority tenants: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var expected []TenantID
	for rows.Next() {
		var tenant string
		if err := rows.Scan(&tenant); err != nil {
			return fmt.Errorf("catalog: scan source authority tenant: %w", err)
		}
		expected = append(expected, TenantID(tenant))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("catalog: read source authority tenants: %w", err)
	}
	actual := make([]TenantID, len(publication.Tenants))
	for index, target := range publication.Tenants {
		actual[index] = target.Tenant
	}
	if !slices.Equal(expected, actual) {
		return fmt.Errorf("%w: source snapshot does not cover the complete authority fleet", ErrInvalidObject)
	}
	return nil
}

func (c *Catalog) applySourceTenant(ctx context.Context, tx *sql.Tx, publication SourcePublication, target SourceTenant, revision Revision) error {
	bindings, err := sourceBindings(ctx, tx, publication.Change.SourceAuthority, target.Tenant)
	if err != nil {
		return err
	}
	for _, source := range target.Objects {
		if _, exists := bindings[source.Key]; exists {
			continue
		}
		id, err := sourceObjectIdentity(ctx, tx, publication.Change.SourceAuthority, source.Key)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO source_object_bindings(source_authority, tenant, source_key)
VALUES (?, ?, ?)`, string(publication.Change.SourceAuthority), string(target.Tenant), string(source.Key)); err != nil {
			return mapConstraint(err)
		}
		bindings[source.Key] = id
	}
	deletes := append([]SourceObjectKey(nil), target.Deletes...)
	if publication.Mode == SourceSnapshot {
		present := make(map[SourceObjectKey]struct{}, len(target.Objects))
		for _, object := range target.Objects {
			present[object.Key] = struct{}{}
		}
		for key := range bindings {
			if _, retained := present[key]; !retained {
				deletes = append(deletes, key)
			}
		}
	}
	plan, err := c.planSourceTenant(ctx, tx, target.Tenant, bindings, target.Objects, deletes)
	if err != nil {
		return err
	}
	for _, id := range plan.detach {
		if _, err := tx.ExecContext(ctx, `UPDATE objects SET tombstone = 1 WHERE tenant = ? AND object_id = ? AND tombstone = 0`, string(target.Tenant), id[:]); err != nil {
			return fmt.Errorf("catalog: detach source namespace binding: %w", err)
		}
	}
	if err := c.deleteSourceObjects(ctx, tx, target.Tenant, bindings, deletes, revision); err != nil {
		return err
	}
	for _, upsert := range plan.upserts {
		if err := c.upsertSourceObject(ctx, tx, publication.Change.OperationID, target.Tenant, upsert.id, upsert.parent, upsert.source, revision); err != nil {
			return err
		}
	}
	return nil
}

type plannedSourceUpsert struct {
	source SourceObject
	id     ObjectID
	parent ObjectID
}

type sourceTenantPlan struct {
	upserts []plannedSourceUpsert
	detach  []ObjectID
}

func (c *Catalog) planSourceTenant(ctx context.Context, tx *sql.Tx, tenant TenantID, bindings map[SourceObjectKey]ObjectID, sources []SourceObject, deletes []SourceObjectKey) (sourceTenantPlan, error) {
	var rawRoot []byte
	if err := tx.QueryRowContext(ctx, "SELECT root_id FROM tenants WHERE tenant = ?", string(tenant)).Scan(&rawRoot); err != nil {
		return sourceTenantPlan{}, fmt.Errorf("catalog: read source tenant root: %w", err)
	}
	root, err := objectID(rawRoot)
	if err != nil {
		return sourceTenantPlan{}, err
	}
	deleted := make(map[SourceObjectKey]struct{}, len(deletes))
	for _, key := range deletes {
		deleted[key] = struct{}{}
	}
	current := make(map[ObjectID]Object, len(bindings))
	for _, id := range bindings {
		object, err := currentObject(ctx, tx, tenant, id, false)
		if err == nil {
			current[id] = object
		} else if !errors.Is(err, ErrNotFound) {
			return sourceTenantPlan{}, err
		}
	}
	byKey := make(map[SourceObjectKey]SourceObject, len(sources))
	for _, source := range sources {
		byKey[source.Key] = source
	}
	ordered, err := orderSourceUpserts(sources, byKey)
	if err != nil {
		return sourceTenantPlan{}, err
	}
	plan := sourceTenantPlan{upserts: make([]plannedSourceUpsert, 0, len(ordered))}
	final := make(map[ObjectID]Object, len(current)+len(sources))
	for key, id := range bindings {
		if _, removed := deleted[key]; removed {
			continue
		}
		if object, live := current[id]; live {
			final[id] = object
		}
	}
	for _, source := range ordered {
		id := bindings[source.Key]
		parent := root
		if source.Parent != "" {
			var ok bool
			parent, ok = bindings[source.Parent]
			if !ok {
				return sourceTenantPlan{}, fmt.Errorf("%w: source parent %q is missing", ErrInvalidObject, source.Parent)
			}
		}
		size, hash := catalogContent(source.Kind, source.Content, source.LinkTarget)
		next := Object{
			Tenant: tenant, ID: id, Parent: parent, Name: source.Name, Kind: source.Kind, Mode: source.Mode,
			ContentRevision: source.ContentRevision, Size: size, Hash: hash, LinkTarget: source.LinkTarget,
			Visibility: source.Visibility,
		}
		if before, live := current[id]; live {
			if before.Kind != next.Kind {
				return sourceTenantPlan{}, fmt.Errorf("%w: source object kind changed", ErrInvalidObject)
			}
			if before.Parent != next.Parent || before.Name != next.Name || before.Visibility != next.Visibility {
				plan.detach = append(plan.detach, id)
			}
		}
		final[id] = next
		plan.upserts = append(plan.upserts, plannedSourceUpsert{source: source, id: id, parent: parent})
	}
	if err := validateSourceFinalNamespace(ctx, tx, tenant, root, bindings, final); err != nil {
		return sourceTenantPlan{}, err
	}
	return plan, nil
}

func orderSourceUpserts(sources []SourceObject, byKey map[SourceObjectKey]SourceObject) ([]SourceObject, error) {
	ordered := make([]SourceObject, 0, len(sources))
	done := make(map[SourceObjectKey]struct{}, len(sources))
	for len(ordered) < len(sources) {
		advanced := false
		for _, source := range sources {
			if _, exists := done[source.Key]; exists {
				continue
			}
			if _, parentPending := byKey[source.Parent]; source.Parent != "" && parentPending {
				if _, ready := done[source.Parent]; !ready {
					continue
				}
			}
			ordered = append(ordered, source)
			done[source.Key] = struct{}{}
			advanced = true
		}
		if !advanced {
			return nil, fmt.Errorf("%w: source object parents are cyclic", ErrInvalidObject)
		}
	}
	return ordered, nil
}

func validateSourceFinalNamespace(ctx context.Context, tx *sql.Tx, tenant TenantID, root ObjectID, bindings map[SourceObjectKey]ObjectID, final map[ObjectID]Object) error {
	bound := make(map[ObjectID]struct{}, len(bindings))
	for _, id := range bindings {
		bound[id] = struct{}{}
	}
	policy, err := tenantCasePolicy(ctx, tx, tenant)
	if err != nil {
		return err
	}
	seen := make(map[string]ObjectID)
	for id, object := range final {
		if object.Parent != root {
			parent, ok := final[object.Parent]
			if !ok || parent.Kind != KindDirectory {
				return fmt.Errorf("%w: source object parent is absent or not a directory", ErrInvalidObject)
			}
		}
		for ancestor := object.Parent; ancestor != root; {
			if ancestor == id {
				return fmt.Errorf("%w: source namespace contains a cycle", ErrInvalidObject)
			}
			parent, ok := final[ancestor]
			if !ok {
				break
			}
			ancestor = parent.Parent
		}
		for _, presentation := range catalogPresentations() {
			if !object.Visibility.Has(presentation) {
				continue
			}
			key := fmt.Sprintf("%d:%x:%s", presentation, object.Parent[:], normalizeName(policy, object.Name))
			if other, duplicate := seen[key]; duplicate && other != id {
				return fmt.Errorf("%w: duplicate final source namespace binding", ErrConflict)
			}
			seen[key] = id
			column, err := visibilityColumn(presentation)
			if err != nil {
				return err
			}
			var raw []byte
			query := "SELECT object_id FROM objects WHERE tenant = ? AND parent_id = ? AND name_key = ? AND tombstone = 0 AND " + column + " = 1"
			err = tx.QueryRowContext(ctx, query, string(tenant), object.Parent[:], normalizeName(policy, object.Name)).Scan(&raw)
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			if err != nil {
				return fmt.Errorf("catalog: validate final source namespace: %w", err)
			}
			existing, err := objectID(raw)
			if err != nil {
				return err
			}
			if _, sourceOwned := bound[existing]; !sourceOwned && existing != id {
				return ErrConflict
			}
		}
	}
	return nil
}

func sourceBindings(ctx context.Context, tx *sql.Tx, authority causal.SourceAuthorityID, tenant TenantID) (map[SourceObjectKey]ObjectID, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT b.source_key, i.object_id
FROM source_object_bindings b
JOIN source_object_ids i
  ON i.source_authority = b.source_authority AND i.source_key = b.source_key
WHERE b.source_authority = ? AND b.tenant = ?`, string(authority), string(tenant))
	if err != nil {
		return nil, fmt.Errorf("catalog: list source object bindings: %w", err)
	}
	defer func() { _ = rows.Close() }()
	bindings := make(map[SourceObjectKey]ObjectID)
	for rows.Next() {
		var key string
		var raw []byte
		if err := rows.Scan(&key, &raw); err != nil {
			return nil, fmt.Errorf("catalog: scan source object binding: %w", err)
		}
		id, err := objectID(raw)
		if err != nil {
			return nil, err
		}
		bindings[SourceObjectKey(key)] = id
	}
	return bindings, rows.Err()
}

func sourceObjectIdentity(ctx context.Context, tx *sql.Tx, authority causal.SourceAuthorityID, key SourceObjectKey) (ObjectID, error) {
	var raw []byte
	err := tx.QueryRowContext(ctx, `
SELECT object_id FROM source_object_ids WHERE source_authority = ? AND source_key = ?`, string(authority), string(key)).Scan(&raw)
	if err == nil {
		return objectID(raw)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ObjectID{}, fmt.Errorf("catalog: read source object identity: %w", err)
	}
	id, err := NewObjectID()
	if err != nil {
		return ObjectID{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO source_object_ids(source_authority, source_key, object_id) VALUES (?, ?, ?)`, string(authority), string(key), id[:]); err != nil {
		return ObjectID{}, mapConstraint(err)
	}
	return id, nil
}

func (c *Catalog) upsertSourceObject(ctx context.Context, tx *sql.Tx, operation causal.OperationID, tenant TenantID, id, parent ObjectID, source SourceObject, revision Revision) error {
	if err := c.claimSourceContent(ctx, tx, operation, source); err != nil {
		return err
	}
	current, err := currentObject(ctx, tx, tenant, id, false)
	live := err == nil
	retained := live
	if errors.Is(err, ErrNotFound) {
		current, err = currentObject(ctx, tx, tenant, id, true)
		retained = err == nil
		if retained {
			committed, committedErr := objectVersionAt(ctx, tx, tenant, id, current.Revision)
			if committedErr != nil {
				return committedErr
			}
			if !committed.Tombstone {
				current = committed
				live = true
			}
		}
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if !retained {
		current = Object{Tenant: tenant, ID: id}
	}
	size, hash := catalogContent(source.Kind, source.Content, source.LinkTarget)
	next := Object{
		Tenant: tenant, ID: id, Parent: parent, Revision: revision, MetadataRevision: revision,
		ContentRevision: source.ContentRevision, Name: source.Name, Kind: source.Kind, Mode: source.Mode,
		Size: size, Hash: hash, LinkTarget: source.LinkTarget, Visibility: source.Visibility,
	}
	if live {
		if current.Parent == next.Parent && current.Name == next.Name && current.Kind == next.Kind && current.Mode == next.Mode &&
			current.ContentRevision == next.ContentRevision && current.Size == next.Size && current.Hash == next.Hash && current.LinkTarget == next.LinkTarget && current.Visibility == next.Visibility {
			return c.consumeSourceContent(ctx, tx, operation, source)
		}
		if current.Kind != next.Kind {
			return fmt.Errorf("%w: source object kind changed", ErrInvalidObject)
		}
		if current.Parent == next.Parent && current.Name == next.Name && current.Mode == next.Mode && current.LinkTarget == next.LinkTarget && current.Visibility == next.Visibility {
			next.MetadataRevision = current.MetadataRevision
		}
		if err := writeObjectRevision(ctx, tx, next); err != nil {
			return err
		}
		if err := appendSourceChanges(ctx, tx, current, next); err != nil {
			return err
		}
		return c.consumeSourceContent(ctx, tx, operation, source)
	}
	if retained {
		if current.Kind != next.Kind {
			return fmt.Errorf("%w: source object kind changed", ErrInvalidObject)
		}
		if err := writeObjectRevision(ctx, tx, next); err != nil {
			return err
		}
		if err := appendSourceChanges(ctx, tx, Object{}, next); err != nil {
			return err
		}
		return c.consumeSourceContent(ctx, tx, operation, source)
	}
	if err := writeNewObject(ctx, tx, next); err != nil {
		return err
	}
	if err := appendSourceChanges(ctx, tx, Object{}, next); err != nil {
		return err
	}
	return c.consumeSourceContent(ctx, tx, operation, source)
}

func objectVersionAt(ctx context.Context, tx *sql.Tx, tenant TenantID, id ObjectID, revision Revision) (Object, error) {
	query := "SELECT " + objectColumns + `
FROM object_versions WHERE tenant = ? AND object_id = ? AND revision = ?`
	object, err := scanObject(tx.QueryRowContext(ctx, query, string(tenant), id[:], uint64(revision)))
	if errors.Is(err, sql.ErrNoRows) {
		return Object{}, ErrNotFound
	}
	if err != nil {
		return Object{}, fmt.Errorf("catalog: read source object revision: %w", err)
	}
	return object, nil
}

func (c *Catalog) claimSourceContent(ctx context.Context, tx *sql.Tx, operation causal.OperationID, source SourceObject) error {
	if source.Kind != KindFile {
		return nil
	}
	result, err := tx.ExecContext(ctx, `
UPDATE content_stages SET mutation_id = ?
WHERE stage_id = ? AND owner_id = ? AND mutation_id IS NULL AND published = 1`, operation[:], source.Content.Stage[:], c.owner[:])
	if err != nil {
		return fmt.Errorf("catalog: claim source content: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect source content claim: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("%w: source content stage ownership changed", ErrInvalidTransition)
	}
	return nil
}

func (c *Catalog) consumeSourceContent(ctx context.Context, tx *sql.Tx, operation causal.OperationID, source SourceObject) error {
	if source.Kind != KindFile {
		return nil
	}
	return c.consumeContentStage(ctx, tx, MutationID(operation), source.Kind, source.Content)
}

func appendSourceChanges(ctx context.Context, tx *sql.Tx, before, after Object) error {
	tenant := after.Tenant
	if tenant == "" {
		tenant = before.Tenant
	}
	revision := after.Revision
	if revision == 0 {
		revision = before.Revision
	}
	for _, presentation := range catalogPresentations() {
		was := before.ID != (ObjectID{}) && before.Visibility.Has(presentation) && !before.Tombstone
		is := after.ID != (ObjectID{}) && after.Visibility.Has(presentation) && !after.Tombstone
		if was && (!is || before.Parent != after.Parent) {
			if err := appendSourceChange(ctx, tx, tenant, revision, EnumerationScope{Kind: EnumerationContainer, Presentation: presentation, Parent: before.Parent}, ChangeDelete, before.ID, before.Revision); err != nil {
				return err
			}
		}
		if is {
			if err := appendSourceChange(ctx, tx, tenant, revision, EnumerationScope{Kind: EnumerationContainer, Presentation: presentation, Parent: after.Parent}, ChangeUpsert, after.ID, after.Revision); err != nil {
				return err
			}
		}
	}
	if before.Visibility.FileProvider || after.Visibility.FileProvider {
		id := after.ID
		if id == (ObjectID{}) {
			id = before.ID
		}
		owners, err := liveInterestOwners(ctx, tx, tenant, id)
		if err != nil {
			return err
		}
		for _, owner := range owners {
			kind := ChangeUpsert
			objectRevision := after.Revision
			if after.ID == (ObjectID{}) || after.Tombstone || !after.Visibility.FileProvider {
				kind = ChangeDelete
				objectRevision = before.Revision
			}
			if err := appendSourceChange(ctx, tx, tenant, revision, workingSetScope(owner), kind, id, objectRevision); err != nil {
				return err
			}
		}
	}
	return nil
}

func appendSourceChange(ctx context.Context, tx *sql.Tx, tenant TenantID, revision Revision, scope EnumerationScope, kind ChangeKind, id ObjectID, objectRevision Revision) error {
	scopeKind, presentation, parent, domain, generation, err := enumerationScopeKey(scope)
	if err != nil {
		return err
	}
	var sequence uint32
	if err := tx.QueryRowContext(ctx, `
SELECT COALESCE(MAX(sequence) + 1, 0) FROM changes
WHERE tenant = ? AND revision = ? AND scope_kind = ? AND presentation = ?
  AND scope_parent = ? AND scope_domain = ? AND scope_generation = ?`, string(tenant), uint64(revision), scopeKind,
		presentation, parent, domain, generation).Scan(&sequence); err != nil {
		return fmt.Errorf("catalog: allocate source change sequence: %w", err)
	}
	return writeChange(ctx, tx, tenant, revision, scope, sequence, kind, id, objectRevision)
}

func (c *Catalog) deleteSourceObjects(ctx context.Context, tx *sql.Tx, tenant TenantID, bindings map[SourceObjectKey]ObjectID, keys []SourceObjectKey, revision Revision) error {
	pending := make(map[ObjectID]struct{}, len(keys))
	for _, key := range keys {
		if id, found := bindings[key]; found {
			pending[id] = struct{}{}
		}
	}
	for len(pending) > 0 {
		advanced := false
		for id := range pending {
			var children int
			if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM objects WHERE tenant = ? AND parent_id = ? AND tombstone = 0`, string(tenant), id[:]).Scan(&children); err != nil {
				return fmt.Errorf("catalog: count source delete children: %w", err)
			}
			if children != 0 {
				continue
			}
			current, err := currentObject(ctx, tx, tenant, id, false)
			if errors.Is(err, ErrNotFound) {
				delete(pending, id)
				advanced = true
				continue
			}
			if err != nil {
				return err
			}
			tombstone := current
			tombstone.Revision = revision
			tombstone.MetadataRevision = revision
			tombstone.Tombstone = true
			tombstone.Visibility = Visibility{}
			if err := writeObjectRevision(ctx, tx, tombstone); err != nil {
				return err
			}
			if err := appendSourceChanges(ctx, tx, current, tombstone); err != nil {
				return err
			}
			delete(pending, id)
			advanced = true
		}
		if !advanced {
			return fmt.Errorf("%w: source delete would orphan non-source children", ErrConflict)
		}
	}
	return nil
}

func (c *Catalog) releaseSourceStages(ctx context.Context, publication SourcePublication) error {
	return c.ReleaseSourceStages(ctx, publication.Tenants)
}

// ReleaseSourceStages discards unclaimed staged bytes after an incomplete publication stream.
func (c *Catalog) ReleaseSourceStages(ctx context.Context, tenants []SourceTenant) error {
	for _, target := range tenants {
		for _, object := range target.Objects {
			if object.Content.Stage == (StageID{}) {
				continue
			}
			if _, err := c.db.ExecContext(ctx, `
DELETE FROM content_stages WHERE stage_id = ? AND owner_id = ? AND mutation_id IS NULL`, object.Content.Stage[:], c.owner[:]); err != nil {
				return fmt.Errorf("catalog: release source content stage: %w", err)
			}
		}
	}
	return nil
}

func sourcePublicationDigest(publication SourcePublication) ([32]byte, error) {
	copy := publication
	copy.Change.AffectedKeys = append([]causal.LogicalKey(nil), publication.Change.AffectedKeys...)
	copy.Tenants = append([]SourceTenant(nil), publication.Tenants...)
	for targetIndex := range copy.Tenants {
		copy.Tenants[targetIndex].Objects = append([]SourceObject(nil), publication.Tenants[targetIndex].Objects...)
		copy.Tenants[targetIndex].Deletes = append([]SourceObjectKey(nil), publication.Tenants[targetIndex].Deletes...)
		for objectIndex := range copy.Tenants[targetIndex].Objects {
			copy.Tenants[targetIndex].Objects[objectIndex].Content.Stage = StageID{}
		}
	}
	payload, err := json.Marshal(copy)
	if err != nil {
		return [32]byte{}, fmt.Errorf("catalog: encode source publication: %w", err)
	}
	return sha256.Sum256(payload), nil
}

type sourceOperationRecord struct {
	result SourceResult
	digest [32]byte
}

func readSourceOperation(ctx context.Context, query rowQuerier, operation causal.OperationID) (sourceOperationRecord, bool, error) {
	var digest, payload []byte
	err := query.QueryRowContext(ctx, `
SELECT request_hash, result_json FROM source_operations WHERE operation_id = ?`, operation[:]).Scan(&digest, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return sourceOperationRecord{}, false, nil
	}
	if err != nil {
		return sourceOperationRecord{}, false, fmt.Errorf("catalog: read source operation: %w", err)
	}
	if len(digest) != sha256.Size {
		return sourceOperationRecord{}, false, fmt.Errorf("%w: corrupt source request digest", ErrIntegrity)
	}
	var result SourceResult
	if err := json.Unmarshal(payload, &result); err != nil {
		return sourceOperationRecord{}, false, fmt.Errorf("%w: corrupt source result", ErrIntegrity)
	}
	var requestDigest [32]byte
	copy(requestDigest[:], digest)
	return sourceOperationRecord{result: result, digest: requestDigest}, true, nil
}

func sameSourceIdentity(result SourceResult, change causal.ChangeSet) bool {
	return result.Authority == change.SourceAuthority && result.Revision == change.SourceRevision &&
		result.ChangeID == change.ChangeID && result.Operation == change.OperationID
}

func sourceCatalogOperation(operation causal.OperationID, tenant TenantID) MutationID {
	digest := sha256.Sum256(append(append([]byte("fusekit.catalog.source-commit\x00"), operation[:]...), []byte(tenant)...))
	var result MutationID
	copy(result[:], digest[:len(result)])
	return result
}
