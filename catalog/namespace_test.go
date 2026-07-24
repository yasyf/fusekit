package catalog

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"

	"github.com/yasyf/fusekit/causal"
)

func (c *Catalog) Create(ctx context.Context, tenant TenantID, spec CreateSpec) (Object, error) {
	result, err := c.testNamespaceMutation(ctx, tenant, MutationIntent{
		SourceID: "test", Create: &CreateMutation{Spec: spec},
	})
	return result.Primary, err
}

func (c *Catalog) Revise(ctx context.Context, tenant TenantID, id ObjectID, spec RevisionSpec) (Object, error) {
	result, err := c.testNamespaceMutation(ctx, tenant, MutationIntent{
		SourceID: "test", Revise: &ReviseMutation{Object: id, Spec: spec},
	})
	return result.Primary, err
}

func (c *Catalog) Delete(ctx context.Context, tenant TenantID, id ObjectID) (Object, error) {
	result, err := c.testNamespaceMutation(ctx, tenant, MutationIntent{
		SourceID: "test", Delete: &DeleteMutation{Object: id},
	})
	return result.Primary, err
}

func (c *Catalog) Replace(ctx context.Context, tenant TenantID, source, target ObjectID) (ReplaceResult, error) {
	result, err := c.testNamespaceMutation(ctx, tenant, MutationIntent{
		SourceID: "test", Replace: &ReplaceMutation{Source: source, Target: target},
	})
	if err != nil {
		return ReplaceResult{}, err
	}
	return ReplaceResult{
		Revision: result.Mutation.Revision, Source: result.Primary, Target: *result.Secondary,
	}, nil
}

func (c *Catalog) testNamespaceMutation(
	ctx context.Context,
	tenant TenantID,
	intent MutationIntent,
) (NamespaceMutationResult, error) {
	if intent.Origin.Cause == "" {
		intent.Origin = testCausalOrigin()
	}
	if intent.Disposition == 0 {
		intent.Disposition = MutationDispositionNamespace
	}
	return c.commitTestAuthoritativeMutation(ctx, tenant, intent)
}

func (c *Catalog) commitTestAuthoritativeMutation(
	ctx context.Context,
	tenant TenantID,
	intent MutationIntent,
) (NamespaceMutationResult, error) {
	provision, found, err := appliedTenantProvision(ctx, c.readDB, tenant)
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source provision: %w", err)
	}
	if !found {
		return NamespaceMutationResult{}, ErrNotFound
	}
	checkpoint, err := c.SourceDriverCheckpoint(ctx, causal.SourceAuthorityID(provision.ContentSourceID))
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source checkpoint: %w", err)
	}
	entries, resultKey, err := c.testSourceEntries(ctx, provision, intent)
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source entries: %w", err)
	}
	head, err := c.Head(ctx, tenant)
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source head: %w", err)
	}
	prepared, err := c.BeginMutation(ctx, tenant, head, intent)
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source begin mutation: %w", err)
	}
	owner, err := NewMutationOwnerID()
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	prepared, err = c.ClaimMutation(ctx, prepared.OperationID, owner)
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source claim mutation: %w", err)
	}
	prepared, err = c.PrepareMutationSource(ctx, prepared.OperationID, *prepared.Claim)
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source prepare mutation: %w", err)
	}
	mutationResult := SourceObjectKey("")
	if intent.Create != nil {
		mutationResult = resultKey
		prepared, err = c.SetMutationSourceResult(ctx, prepared.OperationID, *prepared.Claim, SourceLocator{
			SourceAuthority: causal.SourceAuthorityID(provision.ContentSourceID),
			SourceRevision:  prepared.Source.Parent.SourceRevision,
			SourceKey:       mutationResult,
		})
		if err != nil {
			return NamespaceMutationResult{}, fmt.Errorf("test source set mutation result: %w", err)
		}
	}
	targets := []SourceDriverTarget{{Tenant: tenant, Generation: provision.Generation}}
	targetsDigest, err := SourceDriverTargetsDigest(targets)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	declaration := sha256.Sum256([]byte("declaration:" + provision.ContentSourceID))
	identity := testSourceDriverMutationIdentity(provision, checkpoint, targetsDigest, prepared, mutationResult, intent)
	reservation, err := c.ReserveSourceDriverMutation(ctx, sourceDriverMutationReservationRequestForIdentity(identity))
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source reserve mutation: %w", err)
	}
	for !reservation.TargetsPrepared {
		reservation, err = c.PrepareSourceDriverMutationReservationBatch(ctx, identity.Mutation, identity.Claim)
		if err != nil {
			return NamespaceMutationResult{}, fmt.Errorf("test source prepare reservation: %w", err)
		}
	}
	reservation, err = c.BindSourceDriverMutationRequest(
		ctx, identity.Mutation, identity.Claim, identity.MutationRequestDigest,
	)
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source bind request: %w", err)
	}
	reservation, err = c.RecordSourceDriverMutationReceipt(ctx, identity.Mutation, identity.Claim,
		SourceDriverMutationReceiptProof{
			ToToken: identity.ToToken, Result: identity.MutationResult, Digest: identity.MutationReceiptDigest,
		})
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source record receipt: %w", err)
	}
	identity.TargetCount = 1
	identity.TargetsDigest = targetsDigest
	identity.DeclarationDigest = declaration
	identity.AuthorityGeneration = 1
	identity.Claim = reservation.Claim
	if err := c.BeginSourceDriverStage(ctx, identity); err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source begin stage: %w", err)
	}
	pageDigest := sha256.Sum256(append([]byte("test-source-page:"), identity.Operation[:]...))
	stage, err := c.AppendSourceDriverStage(ctx, identity, sourceDriverPageForTest(
		SourceDriverStageState{}, SourceDriverStagePage{Digest: pageDigest, Complete: true, Entries: entries},
	))
	if err != nil {
		return NamespaceMutationResult{}, fmt.Errorf("test source append stage: %w", err)
	}
	for step := 0; step < SourceDriverTargetLimit*16+128; step++ {
		state, prepareErr := c.PrepareSourceDriverPublicationBatch(ctx, identity)
		if prepareErr != nil {
			durable, _ := readSourceDriverPreparationState(ctx, c.readDB, identity)
			var target string
			var targetPhase uint8
			_ = c.readDB.QueryRowContext(ctx, `
SELECT tenant, phase FROM source_driver_publication_targets
WHERE source_authority = ? AND publication_id = ? LIMIT 1`,
				string(identity.Authority), identity.Operation[:]).Scan(&target, &targetPhase)
			return NamespaceMutationResult{}, fmt.Errorf("test source prepare publication step %d phase %d target %q/%d rows %d: %w",
				step, durable.Phase, target, targetPhase, durable.Rows, prepareErr)
		}
		if state.Prepared {
			result, commitErr := c.CommitSourceDriverMutation(ctx, stage)
			if commitErr != nil {
				return NamespaceMutationResult{}, fmt.Errorf("test source commit mutation: %w", commitErr)
			}
			if result.MutationResult == nil {
				return NamespaceMutationResult{}, ErrIntegrity
			}
			if err := c.activateTestSourcePublication(ctx, provision, result); err != nil {
				return NamespaceMutationResult{}, fmt.Errorf("test source activate publication: %w", err)
			}
			if result.MutationResult.Namespace != nil {
				return *result.MutationResult.Namespace, nil
			}
			if result.MutationResult.Private != nil {
				return NamespaceMutationResult{Primary: privateMutationResultObject(*result.MutationResult.Private)}, nil
			}
			return NamespaceMutationResult{}, ErrIntegrity
		}
	}
	return NamespaceMutationResult{}, fmt.Errorf("catalog test: source publication did not converge")
}

func privateMutationResultObject(result PrivateMutationResult) Object {
	return Object{
		Tenant: result.Tenant, ID: result.ObjectID, Parent: result.Parent,
		Name: result.Name, Kind: result.Kind, Mode: result.Mode,
		ContentRevision: result.ContentRevision, Size: result.Size, Hash: result.Hash,
		LinkTarget: result.LinkTarget,
	}
}

func testSourceDriverMutationIdentity(
	provision TenantProvision,
	checkpoint SourceDriverCheckpoint,
	targetsDigest [sha256.Size]byte,
	prepared PreparedMutation,
	resultKey SourceObjectKey,
	intent MutationIntent,
) SourceDriverStageIdentity {
	seed := sha256.Sum256(append(append([]byte(provision.Tenant), byte(checkpoint.SourceRevision+1)), prepared.OperationID[:]...))
	var operation, source causal.OperationID
	var change causal.ChangeID
	copy(operation[:], seed[:16])
	seed[0] ^= 0x55
	copy(source[:], seed[:16])
	seed[0] ^= 0xaa
	copy(change[:], seed[:16])
	return SourceDriverStageIdentity{
		Authority:  causal.SourceAuthorityID(provision.ContentSourceID),
		FleetOwner: SourceAuthorityFleetOwnerID(provision.OwnerID), AuthorityGeneration: 1,
		DeclarationDigest: checkpoint.DeclarationDigest, TargetCount: 1, TargetsDigest: targetsDigest,
		Operation: operation, SourceOperation: source, ChangeID: change,
		Cause: intent.Origin.Cause, Origin: intent.Origin.Domain,
		OriginGeneration: causal.Generation(intent.Origin.Generation),
		Mode:             SourceDriverMutation, FromToken: checkpoint.Token,
		ToToken: fmt.Sprintf("test-%d", checkpoint.SourceRevision+1), Predecessor: checkpoint.SourceRevision,
		Mutation: prepared.OperationID, MutationTenant: provision.Tenant,
		MutationGeneration: provision.Generation, MutationResult: resultKey,
		MutationRequestDigest: sha256.Sum256(append([]byte("request:"), prepared.OperationID[:]...)),
		MutationReceiptDigest: sha256.Sum256(append([]byte("receipt:"), prepared.OperationID[:]...)),
		Claim:                 *prepared.Claim,
	}
}

func (c *Catalog) testSourceEntries(
	ctx context.Context,
	provision TenantProvision,
	intent MutationIntent,
) ([]SourceDriverStageEntry, SourceObjectKey, error) {
	sequence := uint64(1)
	entry := func(key SourceObjectKey, object *SourceObject) SourceDriverStageEntry {
		value := SourceDriverStageEntry{Tenant: provision.Tenant, Generation: provision.Generation, ChangeSequence: sequence, Key: key, Object: object}
		sequence++
		return value
	}
	switch {
	case intent.Create != nil:
		_, parentKey, err := c.testSourceObject(ctx, provision.Tenant, intent.Create.Spec.Parent)
		if err != nil {
			return nil, "", err
		}
		key := SourceObjectKey(fmt.Sprintf("test-%d-%x", mustRawCatalogHead(ctx, c, provision.Tenant)+1, intent.Create.Spec.Name))
		object := sourceObjectFromCreate(key, parentKey, intent.Create.Spec)
		if intent.Create.Spec.Visibility == (Visibility{}) {
			private := PrivateSourceObject{
				Key: object.Key, Parent: object.Parent, Name: object.Name, Kind: object.Kind,
				Mode: object.Mode, ContentRevision: object.ContentRevision,
				Content: object.Content, LinkTarget: object.LinkTarget,
			}
			return []SourceDriverStageEntry{{
				Tenant: provision.Tenant, Generation: provision.Generation,
				ChangeSequence: sequence, Key: key, Private: &private,
			}}, key, nil
		}
		return []SourceDriverStageEntry{entry(key, &object)}, key, nil
	case intent.Revise != nil:
		current, key, err := c.testSourceObject(ctx, provision.Tenant, intent.Revise.Object)
		if err != nil {
			return nil, "", err
		}
		_, parentKey, err := c.testSourceObject(ctx, provision.Tenant, intent.Revise.Spec.Parent)
		if err != nil {
			return nil, "", err
		}
		object, err := c.sourceObjectFromRevision(ctx, current, key, parentKey, intent.Revise.Spec)
		if err != nil {
			return nil, "", err
		}
		return []SourceDriverStageEntry{entry(key, &object)}, key, nil
	case intent.Delete != nil:
		_, key, err := c.testSourceObject(ctx, provision.Tenant, intent.Delete.Object)
		if err != nil {
			return nil, "", err
		}
		return []SourceDriverStageEntry{entry(key, nil)}, key, nil
	case intent.Replace != nil:
		source, sourceKey, err := c.testSourceObject(ctx, provision.Tenant, intent.Replace.Source)
		if err != nil {
			return nil, "", err
		}
		target, targetKey, err := c.testSourceObject(ctx, provision.Tenant, intent.Replace.Target)
		if err != nil {
			return nil, "", err
		}
		parentID := target.Parent
		if intent.Replace.Parent != nil {
			parentID = *intent.Replace.Parent
		}
		_, parentKey, err := c.testSourceObject(ctx, provision.Tenant, parentID)
		if err != nil {
			return nil, "", err
		}
		object, err := c.sourceObjectFromCurrent(ctx, source, sourceKey, parentKey)
		if err != nil {
			return nil, "", err
		}
		object.Name = target.Name
		object.Visibility = Visibility{
			Mount:        source.Visibility.Mount || target.Visibility.Mount,
			FileProvider: source.Visibility.FileProvider || target.Visibility.FileProvider,
		}
		if intent.Replace.Name != nil {
			object.Name = *intent.Replace.Name
		}
		if intent.Replace.Mode != nil {
			object.Mode = *intent.Replace.Mode
		}
		if intent.Replace.Visibility != nil {
			object.Visibility = *intent.Replace.Visibility
		}
		if intent.Replace.Content != nil {
			object.ContentRevision = intent.Replace.Content.Revision
			object.Content = intent.Replace.Content.Ref
		}
		return []SourceDriverStageEntry{entry(sourceKey, &object), entry(targetKey, nil)}, sourceKey, nil
	case intent.DiscardPrivate != nil:
		_, sourceKey, err := c.testSourceObject(ctx, provision.Tenant, intent.DiscardPrivate.Object)
		if err != nil {
			return nil, "", err
		}
		return []SourceDriverStageEntry{entry(sourceKey, nil)}, sourceKey, nil
	case intent.PromotePrivate != nil:
		source, sourceKey, err := c.testSourceObject(ctx, provision.Tenant, intent.PromotePrivate.Object)
		if err != nil {
			return nil, "", err
		}
		_, parentKey, err := c.testSourceObject(ctx, provision.Tenant, intent.PromotePrivate.Parent)
		if err != nil {
			return nil, "", err
		}
		object, err := c.sourceObjectFromCurrent(ctx, source, sourceKey, parentKey)
		if err != nil {
			return nil, "", err
		}
		object.Name = intent.PromotePrivate.Name
		object.Visibility = intent.PromotePrivate.Visibility
		if intent.PromotePrivate.Mode != nil {
			object.Mode = *intent.PromotePrivate.Mode
		}
		if intent.PromotePrivate.Content != nil {
			object.ContentRevision = intent.PromotePrivate.Content.Revision
			object.Content = intent.PromotePrivate.Content.Ref
		}
		return []SourceDriverStageEntry{entry(sourceKey, &object)}, sourceKey, nil
	default:
		return nil, "", ErrInvalidTransition
	}
}

func mustRawCatalogHead(ctx context.Context, c *Catalog, tenant TenantID) Revision {
	var head uint64
	if err := c.readDB.QueryRowContext(ctx, `SELECT head FROM tenants WHERE tenant = ?`, string(tenant)).Scan(&head); err != nil {
		panic(err)
	}
	return Revision(head)
}

func (c *Catalog) testSourceObject(ctx context.Context, tenant TenantID, id ObjectID) (Object, SourceObjectKey, error) {
	var key string
	object, err := scanObjectWithPrefix(c.readDB.QueryRowContext(ctx, `
SELECT object.source_key, `+objectColumns+`
FROM tenant_activations activation
JOIN tenant_applications application
  ON application.tenant_id = activation.tenant_id AND application.generation = activation.active_generation
 AND application.staged_view_id = activation.active_view_id
JOIN source_driver_publication_objects object
  ON object.source_authority = application.source_authority
 AND object.publication_id = application.source_publication_id AND object.tenant = activation.tenant_id
WHERE activation.tenant_id = ? AND object.object_id = ? AND object.tombstone = 0`, string(tenant), id[:]), &key)
	if err == nil {
		return object, SourceObjectKey(key), nil
	}
	private, found, privateErr := readPrivatePromotionSource(ctx, c.readDB, tenant, id, "test")
	if privateErr != nil {
		return Object{}, "", privateErr
	}
	if found {
		return private.object(), private.SourceKey, nil
	}
	return Object{}, "", err
}

func sourceObjectFromCreate(key, parent SourceObjectKey, spec CreateSpec) SourceObject {
	return SourceObject{Key: key, Parent: parent, Name: spec.Name, Kind: spec.Kind, Mode: spec.Mode,
		ContentRevision: spec.ContentRevision, Content: spec.Content, LinkTarget: spec.LinkTarget, Visibility: spec.Visibility}
}

func (c *Catalog) sourceObjectFromRevision(ctx context.Context, current Object, key, parent SourceObjectKey, spec RevisionSpec) (SourceObject, error) {
	object, err := c.sourceObjectFromCurrent(ctx, current, key, parent)
	if err != nil {
		return SourceObject{}, err
	}
	object.Name, object.Mode, object.Visibility = spec.Name, spec.Mode, spec.Visibility
	if spec.Content != nil {
		object.ContentRevision, object.Content = spec.Content.Revision, spec.Content.Ref
	}
	return object, nil
}

func (c *Catalog) sourceObjectFromCurrent(ctx context.Context, current Object, key, parent SourceObjectKey) (SourceObject, error) {
	object := SourceObject{Key: key, Parent: parent, Name: current.Name, Kind: current.Kind, Mode: current.Mode,
		ContentRevision: current.ContentRevision, LinkTarget: current.LinkTarget, Visibility: current.Visibility}
	if current.Kind != KindFile {
		return object, nil
	}
	file, err := os.Open(c.blobPath(current.Hash))
	if err != nil {
		return SourceObject{}, err
	}
	defer func() { _ = file.Close() }()
	object.Content, err = c.StageContent(ctx, file)
	return object, err
}

func (c *Catalog) activateTestSourcePublication(ctx context.Context, provision TenantProvision, result SourceDriverStageResult) error {
	var head, sourceRevision uint64
	var headDigest, publicationDigest []byte
	if err := c.readDB.QueryRowContext(ctx, `
SELECT target.catalog_head, publication.source_revision, target.catalog_fingerprint, publication.stage_digest
FROM source_driver_publications publication
JOIN source_driver_publication_targets target
  ON target.source_authority = publication.source_authority AND target.publication_id = publication.publication_id
WHERE publication.source_authority = ? AND publication.publication_id = ?
  AND target.tenant = ? AND target.generation = ?`, provision.ContentSourceID, result.Identity.Operation[:],
		string(provision.Tenant), uint64(provision.Generation)).Scan(&head, &sourceRevision, &headDigest, &publicationDigest); err != nil {
		return err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `UPDATE tenants SET head = ? WHERE tenant = ?`, head, string(provision.Tenant)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE tenant_applications SET source_publication_id = ?, staged_catalog_head = ?,
    staged_head_digest = ?, staged_source_revision = ?, publication_digest = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND phase = ?`, result.Identity.Operation[:], head,
		headDigest, sourceRevision, publicationDigest, string(provision.Tenant), uint64(provision.Generation),
		uint8(TenantApplicationStaged)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE tenant_activations SET active_catalog_head = ?, source_revision = ?, version = version + 1
WHERE tenant_id = ? AND active_generation = ?`, head, sourceRevision, string(provision.Tenant),
		uint64(provision.Generation)); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE presentation_materializations SET observed_revision = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND phase = ?`, head, string(provision.Tenant),
		uint64(provision.Generation), uint8(PresentationMaterializationActive)); err != nil {
		return err
	}
	return tx.Commit()
}

func testCausalOrigin() CausalOrigin {
	return CausalOrigin{Cause: causal.CauseDaemonWrite}
}

func (c *Catalog) finishTestNamespaceMutation(ctx context.Context, prepared PreparedMutation) (NamespaceMutationResult, error) {
	if prepared.State == MutationCommitted {
		mutation, err := c.Mutation(ctx, prepared.Tenant, prepared.OperationID)
		if err != nil {
			return NamespaceMutationResult{}, err
		}
		primary, err := c.objectAt(ctx, prepared.Tenant, ObjectID(mutation.Primary), mutation.Revision)
		if err != nil {
			return NamespaceMutationResult{}, err
		}
		result := NamespaceMutationResult{Mutation: mutation, Primary: primary}
		if mutation.Secondary != (ObjectID{}) {
			secondary, err := c.objectAt(ctx, prepared.Tenant, ObjectID(mutation.Secondary), mutation.Revision)
			if err != nil {
				return NamespaceMutationResult{}, err
			}
			result.Secondary = &secondary
		}
		return result, nil
	}
	if prepared.State == MutationPrepared {
		owner, err := NewMutationOwnerID()
		if err != nil {
			return NamespaceMutationResult{}, err
		}
		prepared, err = c.ClaimMutation(ctx, prepared.OperationID, owner)
		if err != nil {
			return NamespaceMutationResult{}, err
		}
	}
	if prepared.State != MutationApplying {
		return NamespaceMutationResult{}, ErrInvalidTransition
	}
	_, digest, err := encodeMutationIntent(prepared.Tenant, prepared.ExpectedHead, prepared.Kind, prepared.Intent)
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	binding := MutationBinding{
		Tenant: prepared.Tenant, Target: prepared.ExpectedHead + 1, Issuer: prepared.Intent.SourceID,
		RequestDigest: MutationRequestDigest(digest),
	}
	var primary Object
	var secondary *Object
	switch prepared.Kind {
	case MutationCreate:
		primary, err = c.create(ctx, prepared.OperationID, binding, prepared.Tenant, prepared.ExpectedHead, prepared.Intent.Create.Spec, prepared.Intent.Origin)
	case MutationRevise:
		primary, err = c.revise(ctx, prepared.OperationID, binding, prepared.Tenant, prepared.ExpectedHead, prepared.Intent.Revise.Object, prepared.Intent.Revise.Spec, prepared.Intent.Origin)
	case MutationDelete:
		primary, err = c.delete(ctx, prepared.OperationID, binding, prepared.Tenant, prepared.ExpectedHead, prepared.Intent.Delete.Object, prepared.Intent.Origin)
	case MutationReplace:
		var replaced ReplaceResult
		replaced, err = c.replace(ctx, prepared.OperationID, binding, prepared.Tenant, prepared.ExpectedHead, *prepared.Intent.Replace, prepared.Intent.Origin)
		primary, secondary = replaced.Source, &replaced.Target
	default:
		return NamespaceMutationResult{}, ErrInvalidTransition
	}
	if err != nil {
		return NamespaceMutationResult{}, err
	}
	if _, err := c.db.ExecContext(ctx, `UPDATE prepared_mutations SET state = ? WHERE mutation_id = ? AND state = ?`,
		uint8(MutationCommitted), prepared.OperationID[:], uint8(MutationApplying)); err != nil {
		return NamespaceMutationResult{}, err
	}
	mutation, err := c.Mutation(ctx, prepared.Tenant, prepared.OperationID)
	return NamespaceMutationResult{Mutation: mutation, Primary: primary, Secondary: secondary}, err
}
