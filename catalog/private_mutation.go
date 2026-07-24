package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/causal"
)

type privateMutationObjectRecord struct {
	PrivateMutationResult
	SourceID string
	Origin   CausalOrigin
}

func readPrivatePromotionSource(
	ctx context.Context,
	query rowQuerier,
	tenant TenantID,
	id ObjectID,
	sourceID string,
) (privateMutationObjectRecord, bool, error) {
	var record privateMutationObjectRecord
	var rawID, rawMutation, rawParent, rawHash, rawSourceOperation []byte
	var tenantValue, authority, sourceKey, cause, origin string
	var generation, contentRevision, sourceRevision, createdAgainst, originGeneration uint64
	var kind uint8
	err := query.QueryRowContext(ctx, `
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
WHERE private.tenant = ? AND private.object_id = ? AND private.source_id = ?`,
		string(tenant), id[:], sourceID).Scan(
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

func (record privateMutationObjectRecord) object() Object {
	return Object{
		Tenant: record.Tenant, ID: record.ObjectID, Parent: record.Parent,
		Name: record.Name, Kind: record.Kind, Mode: record.Mode,
		ContentRevision: record.ContentRevision, Size: record.Size, Hash: record.Hash,
		LinkTarget: record.LinkTarget,
	}
}
