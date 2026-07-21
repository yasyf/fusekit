package convergence

import (
	"cmp"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

const stagedSourceDriverID = "convergence-fixture"

func stagedSourceDeclarationDigest(authority causal.SourceAuthorityID) [sha256.Size]byte {
	return sha256.Sum256([]byte("convergence-fixture-declaration:" + authority))
}

type stagedSourceRevision struct {
	mode        catalog.SourceMode
	predecessor causal.Revision
	change      causal.ChangeSet
	tenants     []catalog.SourceTenant
}

func applyStagedSource(
	t *testing.T,
	store *catalog.Catalog,
	revision stagedSourceRevision,
) error {
	t.Helper()
	stream, err := store.SourceObserverStream(t.Context(), revision.change.SourceAuthority)
	if err != nil {
		if !errors.Is(err, catalog.ErrNotFound) {
			return fmt.Errorf("load observer stream: %w", err)
		}
		fleetOwner := catalog.SourceAuthorityFleetOwnerID(
			"convergence-fixture:" + string(revision.change.SourceAuthority),
		)
		const fleetGeneration causal.Generation = 1
		fleetDigest, digestErr := catalog.SourceAuthorityFleetDigest(
			[]causal.SourceAuthorityID{revision.change.SourceAuthority},
		)
		if digestErr != nil {
			return digestErr
		}
		declarations := []catalog.SourceAuthorityDeclaration{{
			Authority: revision.change.SourceAuthority,
			DriverID:  stagedSourceDriverID,
			DeclarationDigest: stagedSourceDeclarationDigest(
				revision.change.SourceAuthority,
			),
		}}
		declarationsDigest, digestErr := catalog.SourceAuthorityFleetDeclarationsDigest(
			declarations,
		)
		if digestErr != nil {
			return digestErr
		}
		fleetStage, stageErr := store.ReconcileSourceAuthorityFleet(
			t.Context(),
			catalog.SourceAuthorityFleetReconcileRequest{
				Owner: fleetOwner, Generation: fleetGeneration,
				Declarations: declarations, Complete: true, AuthorityCount: 1,
				AuthoritiesDigest: fleetDigest, DeclarationsDigest: declarationsDigest,
			},
		)
		if stageErr != nil {
			return fmt.Errorf("reconcile source authority fleet: %w", stageErr)
		}
		if _, stageErr = store.AcknowledgeSourceAuthorityFleet(
			t.Context(),
			catalog.SourceAuthorityFleetAcknowledgement{
				Owner: fleetOwner, Generation: fleetGeneration,
				AuthorityCount: 1, AuthoritiesDigest: fleetDigest,
				DeclarationsDigest: declarationsDigest, StageDigest: fleetStage.StageDigest,
			},
		); stageErr != nil {
			return fmt.Errorf("acknowledge source authority fleet: %w", stageErr)
		}
		configurationOperation := revision.change.OperationID
		configurationOperation[len(configurationOperation)-1] ^= 0xff
		roots := []catalog.SourceObserverRootRecord{{
			ID: "fixture-root", Generation: 1, Path: "/fixture", VolumeUUID: "fixture-volume",
			Inode: 1, Kind: 1,
		}}
		checkpoints := []catalog.SourceObserverCheckpointRecord{{
			Stream: "fixture-stream", RootEpoch: "fixture-epoch",
		}}
		rootsDigest, digestErr := catalog.SourceObserverRootsDigest(roots)
		if digestErr != nil {
			return digestErr
		}
		checkpointsDigest, digestErr := catalog.SourceObserverCheckpointsDigest(checkpoints)
		if digestErr != nil {
			return digestErr
		}
		configuration := catalog.SourceObserverConfigurationIdentity{
			Authority:  revision.change.SourceAuthority,
			FleetOwner: fleetOwner, FleetGeneration: fleetGeneration,
			Operation: configurationOperation,
			Stream:    "fixture-stream", RootEpoch: "fixture-epoch",
			RootDigest: [32]byte{1}, FleetDigest: fleetDigest,
			RootCount: 1, CheckpointCount: 1,
			RootsDigest: rootsDigest, CheckpointsDigest: checkpointsDigest,
		}
		if err := store.BeginSourceObserverConfiguration(t.Context(), configuration); err != nil {
			return fmt.Errorf("begin observer configuration: %w", err)
		}
		var ref catalog.SourceObserverConfigurationRef
		ref, err = store.AppendSourceObserverConfigurationRoots(
			t.Context(), configuration.Authority, configuration.Operation,
			catalog.SourceObserverRootAppendPage{Records: roots},
		)
		if err != nil {
			return fmt.Errorf("append observer roots: %w", err)
		}
		ref, err = store.AppendSourceObserverConfigurationCheckpoints(
			t.Context(), configuration.Authority, configuration.Operation,
			catalog.SourceObserverCheckpointAppendPage{Sequence: ref.Sequence, Records: checkpoints},
		)
		if err != nil {
			return fmt.Errorf("append observer checkpoints: %w", err)
		}
		stream, err = store.CommitSourceObserverConfiguration(t.Context(), ref)
		if err != nil {
			return fmt.Errorf("commit observer configuration: %w", err)
		}
	}
	if err != nil {
		return fmt.Errorf("load observer stream: %w", err)
	}
	if revision.mode == catalog.SourceSnapshot {
		return applyStagedSourceSnapshot(t, store, stream, revision)
	}
	if err := applyStagedSourceDriver(t, store, stream, revision); err != nil {
		return err
	}
	stageOperation := revision.change.OperationID
	stageOperation[len(stageOperation)-1] ^= 0xaa
	identity := catalog.SourcePublicationStageIdentity{
		Authority:  revision.change.SourceAuthority,
		FleetOwner: stream.FleetOwner, FleetGeneration: stream.FleetGeneration,
		DriverID: stagedSourceDriverID, DeclarationDigest: stagedSourceDeclarationDigest(revision.change.SourceAuthority),
		Operation: stageOperation,
		Stream:    stream.Stream, RootEpoch: stream.RootEpoch, Through: stream.LastApplied,
		Predecessor: revision.predecessor,
	}
	if err := store.BeginSourcePublicationStage(t.Context(), identity); err != nil {
		return fmt.Errorf("begin publication stage: %w", err)
	}
	headerChange := revision.change
	headerChange.AffectedKeys = nil
	pages := []catalog.SourcePublicationStagePage{{
		Header: &catalog.SourcePublicationStageHeader{
			Mode: revision.mode, Predecessor: revision.predecessor, Change: headerChange,
		},
	}}
	for _, key := range revision.change.AffectedKeys {
		pages[0].Affected = append(pages[0].Affected, catalog.SourcePublicationAffected{
			Revision: revision.change.SourceRevision, Key: key,
		})
	}
	pages[len(pages)-1].Complete = true
	var ref catalog.SourcePublicationStageRef
	for index := range pages {
		pages[index].Sequence = uint64(index)
		ref, err = store.AppendSourcePublicationStage(t.Context(), identity, pages[index])
		if err != nil {
			_ = store.AbortSourcePublicationStage(t.Context(), identity.Authority, identity.Operation)
			return fmt.Errorf("append publication stage page %d: %w", index, err)
		}
	}
	result, err := store.CommitSourcePublicationStage(t.Context(), ref)
	if err != nil {
		_ = store.AbortSourcePublicationStage(t.Context(), identity.Authority, identity.Operation)
		return fmt.Errorf("commit publication stage: %w", err)
	}
	if result.Count == 0 {
		if err := store.AcknowledgeSourceObserverSettlement(t.Context(), ref); err != nil {
			return fmt.Errorf("acknowledge observer settlement: %w", err)
		}
	}
	return nil
}

func applyStagedSourceDriver(
	t *testing.T,
	store *catalog.Catalog,
	stream catalog.SourceObserverStreamRecord,
	revision stagedSourceRevision,
) error {
	t.Helper()
	const topologyOwner catalog.SourceAuthorityFleetOwnerID = "owner"
	head, err := store.TopologyHead(t.Context(), topologyOwner)
	if err != nil {
		return fmt.Errorf("load fixture topology head: %w", err)
	}
	request := catalog.TopologySnapshotRequest{
		Owner: topologyOwner, Revision: head.Revision, Limit: catalog.TopologyPageLimit,
	}
	var targets []catalog.SourceDriverTarget
	for {
		page, pageErr := store.TopologySnapshot(t.Context(), request)
		if pageErr != nil {
			return fmt.Errorf("page fixture topology: %w", pageErr)
		}
		for _, provision := range page.Tenants {
			if provision.ContentSourceID == string(revision.change.SourceAuthority) {
				targets = append(targets, catalog.SourceDriverTarget{
					Tenant: provision.Tenant, Generation: provision.Generation,
				})
			}
		}
		if page.Next == (catalog.TopologyCursor{}) {
			break
		}
		request.Cursor = page.Next
	}
	slices.SortFunc(targets, func(left, right catalog.SourceDriverTarget) int {
		return cmp.Compare(left.Tenant, right.Tenant)
	})
	targetsDigest, err := catalog.SourceDriverTargetsDigest(targets)
	if err != nil {
		return fmt.Errorf("digest fixture source targets: %w", err)
	}
	operation := revision.change.OperationID
	operation[len(operation)-1] ^= 0x55
	identity := catalog.SourceDriverStageIdentity{
		Authority:  revision.change.SourceAuthority,
		FleetOwner: stream.FleetOwner, AuthorityGeneration: stream.FleetGeneration,
		DeclarationDigest: stagedSourceDeclarationDigest(revision.change.SourceAuthority),
		TargetCount:       uint64(len(targets)), TargetsDigest: targetsDigest,
		Operation: operation, SourceOperation: revision.change.OperationID,
		ChangeID: revision.change.ChangeID, Cause: revision.change.Cause,
		Origin: revision.change.Origin, OriginGeneration: revision.change.OriginGeneration,
		Mode:        catalog.SourceDriverDelta,
		FromToken:   strconv.FormatUint(uint64(revision.predecessor), 10),
		ToToken:     strconv.FormatUint(uint64(revision.change.SourceRevision), 10),
		Predecessor: revision.predecessor,
	}
	if err := store.BeginSourceDriverStage(t.Context(), identity); err != nil {
		return fmt.Errorf("begin source driver stage: %w", err)
	}
	pending, err := store.PendingSourceDriverStage(t.Context(), identity.Authority)
	if err != nil || pending == nil || pending.Identity != identity {
		return fmt.Errorf("load source driver stage: %w", errors.Join(catalog.ErrIntegrity, err))
	}
	entries := make([]catalog.SourceDriverStageEntry, 0)
	for _, tenant := range revision.tenants {
		for _, object := range tenant.Objects {
			object := object
			entries = append(entries, catalog.SourceDriverStageEntry{
				Tenant: tenant.Tenant, Generation: tenant.Generation,
				ChangeSequence: 1, Key: object.Key, Object: &object,
			})
		}
		for _, key := range tenant.Deletes {
			entries = append(entries, catalog.SourceDriverStageEntry{
				Tenant: tenant.Tenant, Generation: tenant.Generation,
				ChangeSequence: 1, Key: key,
			})
		}
	}
	slices.SortFunc(entries, func(left, right catalog.SourceDriverStageEntry) int {
		if order := cmp.Compare(left.Tenant, right.Tenant); order != 0 {
			return order
		}
		return cmp.Compare(left.Key, right.Key)
	})
	page := catalog.SourceDriverStagePage{
		Sequence: pending.Stage.Sequence, Entries: entries, Complete: true,
		PredecessorDigest: catalog.SourceDriverPagePredecessorDigest(pending.Cursor, pending.PageDigest),
	}
	encoded, err := json.Marshal(struct {
		Cursor   []byte
		Entries  []catalog.SourceDriverStageEntry
		Complete bool
	}{page.Cursor, page.Entries, page.Complete})
	if err != nil {
		return err
	}
	page.Digest = sha256.Sum256(encoded)
	state, err := store.AppendSourceDriverStage(t.Context(), identity, page)
	if err != nil {
		_ = store.AbortSourceDriverStage(t.Context(), identity)
		return fmt.Errorf("append source driver stage: %w", err)
	}
	for {
		preparation, prepareErr := store.PrepareSourceDriverPublicationBatch(t.Context(), identity)
		if prepareErr != nil {
			return fmt.Errorf("prepare source driver publication: %w", prepareErr)
		}
		if preparation.Prepared {
			break
		}
	}
	result, err := store.CommitSourceDriverStage(t.Context(), state)
	if err != nil {
		return fmt.Errorf("commit source driver stage: %w", err)
	}
	if err := store.AcknowledgeSourceDriverCommittedReceipt(t.Context(), result); err != nil {
		return fmt.Errorf("acknowledge source driver receipt: %w", err)
	}
	if err := store.ForgetSourceDriverCommittedReceipt(t.Context(), result); err != nil {
		return fmt.Errorf("forget source driver receipt: %w", err)
	}
	return nil
}

func applyStagedSourceSnapshot(
	t *testing.T,
	store *catalog.Catalog,
	stream catalog.SourceObserverStreamRecord,
	revision stagedSourceRevision,
) error {
	t.Helper()
	snapshot := fmt.Sprintf("fixture-%x", revision.change.OperationID)
	if err := store.BeginSourceSnapshotStage(t.Context(), revision.change.SourceAuthority, snapshot); err != nil {
		return fmt.Errorf("begin source snapshot scan: %w", err)
	}
	var physical []catalog.SourcePhysicalIndexRecord
	page := catalog.SourceSnapshotPublicationPage{
		AffectedKeys: append([]causal.LogicalKey(nil), revision.change.AffectedKeys...),
	}
	bindings := make(map[catalog.SourceObjectKey]catalog.SourceAuthorityBindingRecord)
	for _, tenant := range revision.tenants {
		rootLogical := "root:" + string(tenant.Tenant)
		root, err := store.ReserveSourceAuthorityBinding(
			t.Context(), revision.change.SourceAuthority, rootLogical, tenant.RootKey,
		)
		if err != nil {
			return fmt.Errorf("reserve snapshot root: %w", err)
		}
		page.Roots = append(page.Roots, catalog.SourceSnapshotRoot{
			Tenant: tenant.Tenant, Generation: tenant.Generation,
			LogicalID: root.LogicalID, RootKey: root.SourceKey,
		})
		for _, object := range tenant.Objects {
			binding, ok := bindings[object.Key]
			if !ok {
				logical := string(object.Key)
				binding, err = store.ReserveSourceAuthorityBinding(
					t.Context(), revision.change.SourceAuthority, logical, object.Key,
				)
				if err != nil {
					return fmt.Errorf("reserve snapshot object: %w", err)
				}
				bindings[object.Key] = binding
				locator := catalog.SourceIndexLocator{
					RootID: "fixture-root", Relative: string(object.Key),
				}
				physical = append(physical, catalog.SourcePhysicalIndexRecord{
					Authority: revision.change.SourceAuthority,
					RootID:    locator.RootID, Relative: locator.Relative,
					FileIdentity: []byte("fixture:" + logical), Kind: uint8(object.Kind),
					Payload: []byte("{}"),
				})
				page.Bindings = append(page.Bindings, catalog.SourceSnapshotBinding{
					LogicalID: binding.LogicalID, SourceKey: binding.SourceKey,
					Fingerprint: [32]byte{1}, Inputs: []catalog.SourceIndexLocator{locator},
				})
			}
			page.Objects = append(page.Objects, catalog.SourceSnapshotProjection{
				Tenant: tenant.Tenant, Generation: tenant.Generation,
				LogicalID: binding.LogicalID, Object: object,
			})
		}
	}
	slices.Sort(page.AffectedKeys)
	page.AffectedKeys = slices.Compact(page.AffectedKeys)
	slices.SortFunc(page.Roots, func(left, right catalog.SourceSnapshotRoot) int {
		return cmp.Compare(left.Tenant, right.Tenant)
	})
	slices.SortFunc(page.Bindings, func(left, right catalog.SourceSnapshotBinding) int {
		return cmp.Compare(left.LogicalID, right.LogicalID)
	})
	slices.SortFunc(physical, func(left, right catalog.SourcePhysicalIndexRecord) int {
		if order := cmp.Compare(left.RootID, right.RootID); order != 0 {
			return order
		}
		return cmp.Compare(left.Relative, right.Relative)
	})
	if len(physical) != 0 {
		next := catalog.SourceIndexLocator{
			RootID:   physical[len(physical)-1].RootID,
			Relative: physical[len(physical)-1].Relative,
		}
		if err := store.AppendSourceSnapshotStagePage(
			t.Context(), revision.change.SourceAuthority, snapshot,
			catalog.SourceSnapshotPage{Records: physical, Next: next},
		); err != nil {
			return fmt.Errorf("append source snapshot scan: %w", err)
		}
	}
	type fenceCheckpoint struct {
		Identity  string
		Cursor    uint64
		RootEpoch string
	}
	fence := struct {
		Authority           causal.SourceAuthorityID
		AuthorityGeneration causal.Generation
		Streams             []fenceCheckpoint
		Inbox               uint64
		RootDigest          [32]byte
		FleetDigest         [32]byte
	}{
		Authority: revision.change.SourceAuthority, AuthorityGeneration: 1,
		Streams: []fenceCheckpoint{{
			Identity: stream.Stream, Cursor: stream.LastReceived, RootEpoch: stream.RootEpoch,
		}},
		Inbox: stream.LastReceived, RootDigest: stream.RootDigest, FleetDigest: stream.FleetDigest,
	}
	encodedFence, err := json.Marshal(fence)
	if err != nil {
		return err
	}
	change := revision.change
	change.Cause = causal.CauseBootstrap
	change.AffectedKeys = nil
	identity := catalog.SourceSnapshotIdentity{
		Authority: revision.change.SourceAuthority, AuthorityGeneration: 1,
		Snapshot:    snapshot,
		FenceDigest: sha256.Sum256(encodedFence), Change: change,
	}
	if err := store.BeginSourceSnapshotPublication(t.Context(), identity); err != nil {
		return fmt.Errorf("begin source snapshot publication: %w", err)
	}
	ref, err := store.AppendSourceSnapshotPublication(t.Context(), identity, page)
	if err != nil {
		return fmt.Errorf("append source snapshot publication: %w", err)
	}
	_, err = store.PromoteSourceSnapshot(t.Context(), ref, catalog.SourceSnapshotSettlement{
		Fence: catalog.SourceObserverSettlement{
			Authority: ref.Authority, Stream: stream.Stream, RootEpoch: stream.RootEpoch,
			Through: stream.LastReceived, Operation: ref.Operation,
		},
		Snapshot: ref,
	})
	if err != nil {
		return fmt.Errorf("promote source snapshot: %w", err)
	}
	return nil
}
