package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/yasyf/fusekit/causal"
)

type privateMutationObjectRecord struct {
	PrivateMutationResult
	SourceID string
	Origin   CausalOrigin
}

const privateMutationObjectSelect = `
SELECT private.tenant, private.object_id, private.mutation_id, private.generation,
       private.source_authority, private.source_key, private.source_operation_id,
       private.source_revision, private.created_against_head, private.source_id,
       private.cause, private.origin_domain, private.origin_generation,
       private.parent_id, private.name, private.kind, private.mode,
       private.content_revision, private.size, private.hash, private.link_target
FROM private_mutation_objects private
JOIN source_object_ids identity
  ON identity.source_authority = private.source_authority
 AND identity.source_key = private.source_key
 AND identity.object_id = private.object_id
JOIN tenant_activations activation
  ON activation.tenant_id = private.tenant
 AND activation.active_generation = private.generation
JOIN tenant_generations generation
  ON generation.tenant_id = activation.tenant_id
 AND generation.generation = activation.active_generation
 AND generation.content_source_id = private.source_authority
WHERE private.tenant = ? AND private.object_id = ?`

func readPrivatePromotionSource(
	ctx context.Context,
	query rowQuerier,
	tenant TenantID,
	id ObjectID,
	sourceID string,
) (privateMutationObjectRecord, bool, error) {
	row := query.QueryRowContext(ctx, privateMutationObjectSelect+` AND private.source_id = ?`,
		string(tenant), id[:], sourceID)
	return scanPrivateMutationObject(row)
}

func readPrivateMutationObjectByOrigin(
	ctx context.Context,
	query rowQuerier,
	tenant TenantID,
	id ObjectID,
	origin CausalOrigin,
) (privateMutationObjectRecord, bool, error) {
	row := query.QueryRowContext(ctx, privateMutationObjectSelect+`
  AND private.cause = ? AND private.origin_domain = ? AND private.origin_generation = ?`,
		string(tenant), id[:], string(origin.Cause), string(origin.Domain), uint64(origin.Generation))
	return scanPrivateMutationObject(row)
}

func scanPrivateMutationObject(row rowScanner) (privateMutationObjectRecord, bool, error) {
	var record privateMutationObjectRecord
	var rawID, rawMutation, rawParent, rawHash, rawSourceOperation []byte
	var tenantValue, authority, sourceKey, cause, origin string
	var generation, contentRevision, sourceRevision, createdAgainst, originGeneration uint64
	var kind uint8
	err := row.Scan(
		&tenantValue, &rawID, &rawMutation, &generation, &authority, &sourceKey,
		&rawSourceOperation, &sourceRevision, &createdAgainst, &record.SourceID,
		&cause, &origin, &originGeneration, &rawParent, &record.Name, &kind,
		&record.Mode, &contentRevision, &record.Size, &rawHash, &record.LinkTarget,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return privateMutationObjectRecord{}, false, nil
	}
	if err != nil {
		return privateMutationObjectRecord{}, false, fmt.Errorf("catalog: read private promotion source: %w", err)
	}
	idValue, err := objectID(rawID)
	if err != nil {
		return privateMutationObjectRecord{}, false, err
	}
	mutation, err := mutationID(rawMutation)
	if err != nil {
		return privateMutationObjectRecord{}, false, err
	}
	parent, err := objectID(rawParent)
	if err != nil {
		return privateMutationObjectRecord{}, false, err
	}
	if len(rawHash) != len(ContentHash{}) || len(rawSourceOperation) != len(causal.OperationID{}) {
		return privateMutationObjectRecord{}, false, ErrIntegrity
	}
	record.Mutation = mutation
	record.Tenant = TenantID(tenantValue)
	record.Generation = Generation(generation)
	record.ObjectID = idValue
	record.Parent = parent
	record.Kind = Kind(kind)
	record.ContentRevision = Revision(contentRevision)
	copy(record.Hash[:], rawHash)
	record.SourceAuthority = causal.SourceAuthorityID(authority)
	record.SourceKey = SourceObjectKey(sourceKey)
	copy(record.SourceOperation[:], rawSourceOperation)
	record.SourceRevision = causal.Revision(sourceRevision)
	record.CreatedAgainstHead = Revision(createdAgainst)
	record.Origin = CausalOrigin{
		Cause: causal.Cause(cause), Domain: causal.DomainID(origin), Generation: causal.Generation(originGeneration),
	}
	return record, true, nil
}

// PrivateMutationObject returns one live private object to its exact provider origin.
func (c *Catalog) PrivateMutationObject(
	ctx context.Context,
	tenant TenantID,
	id ObjectID,
	origin CausalOrigin,
) (PrivateMutationResult, error) {
	if origin.Cause != causal.CauseProviderMutation || origin.Domain == "" || origin.Generation == 0 {
		return PrivateMutationResult{}, fmt.Errorf("%w: private lookup origin is not a provider", ErrInvalidObject)
	}
	record, found, err := readPrivateMutationObjectByOrigin(ctx, c.readDB, tenant, id, origin)
	if err != nil {
		return PrivateMutationResult{}, err
	}
	if !found {
		return PrivateMutationResult{}, ErrNotFound
	}
	return record.PrivateMutationResult, nil
}

// PrivateContentHandle streams one authenticated live private file.
type PrivateContentHandle struct {
	Result PrivateMutationResult
	file   *os.File
}

// OpenPrivateContent opens one live private file for its exact creator and provider origin.
func (c *Catalog) OpenPrivateContent(
	ctx context.Context,
	tenant TenantID,
	generation Generation,
	id ObjectID,
	creator MutationID,
	origin CausalOrigin,
) (*PrivateContentHandle, error) {
	if generation == 0 || creator == (MutationID{}) {
		return nil, fmt.Errorf("%w: private content capability is incomplete", ErrInvalidObject)
	}
	result, err := c.PrivateMutationObject(ctx, tenant, id, origin)
	if err != nil {
		return nil, err
	}
	if result.Generation != generation || result.Mutation != creator || result.Kind != KindFile {
		return nil, ErrMutationConflict
	}
	file, err := c.openBlob(ctx, ContentRef{Hash: result.Hash, Size: result.Size})
	if err != nil {
		return nil, fmt.Errorf("catalog: open private content: %w", err)
	}
	if err := verifyOpenFile(file, ContentRef{Hash: result.Hash, Size: result.Size}); err != nil {
		_ = file.Close()
		return nil, err
	}
	confirmed, err := c.PrivateMutationObject(ctx, tenant, id, origin)
	if err != nil || confirmed.Mutation != creator || confirmed.Generation != generation ||
		confirmed.Hash != result.Hash || confirmed.Size != result.Size {
		_ = file.Close()
		if err != nil {
			return nil, err
		}
		return nil, ErrMutationConflict
	}
	return &PrivateContentHandle{Result: result, file: file}, nil
}

// Read streams private content.
func (h *PrivateContentHandle) Read(buffer []byte) (int, error) { return h.file.Read(buffer) }

// ReadAt streams private content from an exact offset.
func (h *PrivateContentHandle) ReadAt(buffer []byte, offset int64) (int, error) {
	return h.file.ReadAt(buffer, offset)
}

// Seek changes the private content offset.
func (h *PrivateContentHandle) Seek(offset int64, whence int) (int64, error) {
	return h.file.Seek(offset, whence)
}

// Close releases the private content descriptor.
func (h *PrivateContentHandle) Close() error { return h.file.Close() }

var _ interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
} = (*PrivateContentHandle)(nil)

func (record privateMutationObjectRecord) object() Object {
	return Object{
		Tenant: record.Tenant, ID: record.ObjectID, Parent: record.Parent,
		Name: record.Name, Kind: record.Kind, Mode: record.Mode,
		ContentRevision: record.ContentRevision, Size: record.Size, Hash: record.Hash,
		LinkTarget: record.LinkTarget,
	}
}

func retirePrivateMutationObjects(
	ctx context.Context,
	tx *sql.Tx,
	where string,
	args ...any,
) error {
	predicate := " WHERE " + where
	if _, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO blob_gc_candidates(hash)
SELECT hash FROM private_mutation_objects`+predicate, args...); err != nil {
		return fmt.Errorf("catalog: enqueue retired private content: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE private_mutation_receipts
SET state = 3, terminal_mutation_id = mutation_id
WHERE state = 1 AND mutation_id IN (
    SELECT mutation_id FROM private_mutation_objects`+predicate+`
)`, args...); err != nil {
		return fmt.Errorf("catalog: retire private mutation receipts: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM private_mutation_objects`+predicate, args...); err != nil {
		return fmt.Errorf("catalog: retire private mutation objects: %w", err)
	}
	return nil
}
