package sourcedriverservice

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/sourcedriver"
	"github.com/yasyf/fusekit/sourcedriverproto"
)

func protocolCursor(cursor *sourcedriver.PageCursor) *sourcedriverproto.PageCursor {
	if cursor == nil {
		return nil
	}
	return &sourcedriverproto.PageCursor{
		TargetSet: protocolTargetSetRef(cursor.TargetSet), Kind: protocolPageKind(cursor.Kind),
		From: string(cursor.From), To: string(cursor.To), Page: cursor.Page, Limit: cursor.Limit,
		AfterTenant: string(cursor.AfterTenant), AfterGeneration: uint64(cursor.AfterGeneration),
		AfterSequence: cursor.AfterSequence, After: string(cursor.After),
		Continuation:   base64.StdEncoding.EncodeToString(cursor.Continuation),
		PreviousDigest: hex.EncodeToString(cursor.PreviousDigest[:]), Digest: hex.EncodeToString(cursor.Digest[:]),
	}
}

func domainCursor(cursor *sourcedriverproto.PageCursor) (*sourcedriver.PageCursor, error) {
	if cursor == nil {
		return nil, nil
	}
	previous, err := digest(cursor.PreviousDigest)
	if err != nil {
		return nil, err
	}
	targetSet, err := domainTargetSetRef(cursor.TargetSet)
	if err != nil {
		return nil, err
	}
	value, err := sourcedriver.NewPageCursor(
		targetSet, domainPageKind(cursor.Kind),
		sourcedriver.RevisionToken(cursor.From), sourcedriver.RevisionToken(cursor.To), cursor.Page, int(cursor.Limit),
		sourcedriver.PagePosition{
			Tenant: catalog.TenantID(cursor.AfterTenant), Generation: causal.Generation(cursor.AfterGeneration),
			Sequence: cursor.AfterSequence, ID: sourcedriver.LogicalID(cursor.After),
		},
		mustContinuation(cursor.Continuation), previous,
	)
	if err != nil || hex.EncodeToString(value.Digest[:]) != cursor.Digest {
		return nil, fmt.Errorf("source driver service: invalid page cursor")
	}
	return &value, nil
}

func protocolTargetSetRef(ref sourcedriver.TargetSetRef) sourcedriverproto.TargetSetRef {
	return sourcedriverproto.TargetSetRef{
		ID: hex.EncodeToString(ref.ID[:]), AuthorityGeneration: uint64(ref.AuthorityGeneration),
		TargetEpoch: ref.TargetEpoch, DeclarationDigest: hex.EncodeToString(ref.DeclarationDigest[:]),
		TargetCount: ref.TargetCount, TargetsDigest: hex.EncodeToString(ref.TargetsDigest[:]),
	}
}

func domainTargetSetRef(ref sourcedriverproto.TargetSetRef) (sourcedriver.TargetSetRef, error) {
	id, err := hex.DecodeString(ref.ID)
	if err != nil || len(id) != len(sourcedriver.TargetSetID{}) {
		return sourcedriver.TargetSetRef{}, fmt.Errorf("source driver service: invalid target set id")
	}
	declaration, err := digest(ref.DeclarationDigest)
	if err != nil {
		return sourcedriver.TargetSetRef{}, err
	}
	targets, err := digest(ref.TargetsDigest)
	if err != nil {
		return sourcedriver.TargetSetRef{}, err
	}
	value := sourcedriver.TargetSetRef{
		AuthorityGeneration: causal.Generation(ref.AuthorityGeneration), TargetEpoch: ref.TargetEpoch,
		DeclarationDigest: declaration, TargetCount: ref.TargetCount, TargetsDigest: targets,
	}
	copy(value.ID[:], id)
	return value, nil
}

func protocolTargetSetState(state sourcedriver.TargetSetState) sourcedriverproto.TargetSetState {
	value := sourcedriverproto.TargetSetState{
		Ref: protocolTargetSetRef(state.Ref), NextPage: state.NextPage, DeclaredCount: state.DeclaredCount,
		RollingDigest:  hex.EncodeToString(state.RollingDigest[:]),
		LastPageDigest: hex.EncodeToString(state.LastPageDigest[:]), Complete: state.Complete,
	}
	if state.After.Tenant != "" {
		after := protocolTargets([]sourcedriver.TargetDeclaration{state.After})[0]
		value.After = &after
	}
	return value
}

func domainTargetSetState(state sourcedriverproto.TargetSetState) (sourcedriver.TargetSetState, error) {
	ref, err := domainTargetSetRef(state.Ref)
	if err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	rolling, err := digest(state.RollingDigest)
	if err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	last, err := digest(state.LastPageDigest)
	if err != nil {
		return sourcedriver.TargetSetState{}, err
	}
	value := sourcedriver.TargetSetState{
		Ref: ref, NextPage: state.NextPage, DeclaredCount: state.DeclaredCount,
		RollingDigest: rolling, LastPageDigest: last, Complete: state.Complete,
	}
	if state.After != nil {
		value.After = domainTargets([]sourcedriverproto.TargetDeclaration{*state.After})[0]
	}
	return value, nil
}

func protocolTargetSetPage(page sourcedriver.TargetSetPage) sourcedriverproto.TargetSetPage {
	return sourcedriverproto.TargetSetPage{
		Ref: protocolTargetSetRef(page.Ref), Sequence: page.Sequence, Targets: protocolTargets(page.Targets),
		Complete: page.Complete, PreviousDigest: hex.EncodeToString(page.PreviousDigest[:]),
		Digest: hex.EncodeToString(page.Digest[:]), PageDigest: hex.EncodeToString(page.PageDigest[:]),
	}
}

func domainTargetSetPage(page sourcedriverproto.TargetSetPage) (sourcedriver.TargetSetPage, error) {
	ref, err := domainTargetSetRef(page.Ref)
	if err != nil {
		return sourcedriver.TargetSetPage{}, err
	}
	previous, err := digest(page.PreviousDigest)
	if err != nil {
		return sourcedriver.TargetSetPage{}, err
	}
	rolling, err := digest(page.Digest)
	if err != nil {
		return sourcedriver.TargetSetPage{}, err
	}
	pageDigest, err := digest(page.PageDigest)
	if err != nil {
		return sourcedriver.TargetSetPage{}, err
	}
	return sourcedriver.TargetSetPage{
		Ref: ref, Sequence: page.Sequence, Targets: domainTargets(page.Targets), Complete: page.Complete,
		PreviousDigest: previous, Digest: rolling, PageDigest: pageDigest,
	}, nil
}

func protocolTargets(targets []sourcedriver.TargetDeclaration) []sourcedriverproto.TargetDeclaration {
	values := make([]sourcedriverproto.TargetDeclaration, len(targets))
	for index, target := range targets {
		values[index] = sourcedriverproto.TargetDeclaration{
			Tenant: string(target.Tenant), Generation: uint64(target.Generation),
		}
	}
	return values
}

func domainTargets(targets []sourcedriverproto.TargetDeclaration) []sourcedriver.TargetDeclaration {
	values := make([]sourcedriver.TargetDeclaration, len(targets))
	for index, target := range targets {
		values[index] = sourcedriver.TargetDeclaration{
			Tenant: catalog.TenantID(target.Tenant), Generation: causal.Generation(target.Generation),
		}
	}
	return values
}

func protocolContentRef(ref sourcedriver.ContentRef) sourcedriverproto.ContentRef {
	return sourcedriverproto.ContentRef{
		Revision: string(ref.Revision), Tenant: string(ref.Tenant), Generation: uint64(ref.Generation),
		Object: string(ref.Object), Size: ref.Size, Hash: hex.EncodeToString(ref.Hash[:]),
	}
}

func domainContentRef(ref sourcedriverproto.ContentRef) (sourcedriver.ContentRef, error) {
	hash, err := contentHash(ref.Hash)
	if err != nil {
		return sourcedriver.ContentRef{}, err
	}
	value := sourcedriver.ContentRef{
		Revision: sourcedriver.RevisionToken(ref.Revision), Tenant: catalog.TenantID(ref.Tenant),
		Generation: causal.Generation(ref.Generation), Object: sourcedriver.LogicalID(ref.Object), Size: ref.Size, Hash: hash,
	}
	return value, sourcedriver.ValidateContentRef(value)
}

func protocolProjection(object sourcedriver.Projection) sourcedriverproto.Projection {
	value := sourcedriverproto.Projection{
		Tenant: string(object.Tenant), Generation: uint64(object.Generation), ID: string(object.ID), Parent: string(object.Parent),
		Name: object.Name, Kind: protocolObjectKind(object.Kind), Mode: object.Mode, LinkTarget: object.LinkTarget,
		MountVisible: object.Visibility.Mount, FileProviderVisible: object.Visibility.FileProvider,
		Size: object.Size,
	}
	if object.Hash != (catalog.ContentHash{}) {
		value.Hash = hex.EncodeToString(object.Hash[:])
	}
	if object.Content != nil {
		content := protocolContentRef(*object.Content)
		value.Content = &content
	}
	return value
}

func domainProjection(object sourcedriverproto.Projection) (sourcedriver.Projection, error) {
	var hash catalog.ContentHash
	var err error
	if object.Hash != "" {
		hash, err = contentHash(object.Hash)
		if err != nil {
			return sourcedriver.Projection{}, err
		}
	}
	value := sourcedriver.Projection{
		Tenant: catalog.TenantID(object.Tenant), Generation: causal.Generation(object.Generation),
		ID: sourcedriver.LogicalID(object.ID), Parent: sourcedriver.LogicalID(object.Parent), Name: object.Name,
		Kind: domainObjectKind(object.Kind), Mode: object.Mode, LinkTarget: object.LinkTarget,
		Visibility: catalog.Visibility{Mount: object.MountVisible, FileProvider: object.FileProviderVisible},
		Size:       object.Size, Hash: hash,
	}
	if object.Content != nil {
		content, err := domainContentRef(*object.Content)
		if err != nil {
			return sourcedriver.Projection{}, err
		}
		value.Content = &content
	}
	return value, sourcedriver.ValidateProjection(value)
}

func protocolChange(change sourcedriver.Change) sourcedriverproto.Change {
	value := sourcedriverproto.Change{
		Kind: protocolChangeKind(change.Kind), Tenant: string(change.Tenant),
		Generation: uint64(change.Generation), Sequence: change.Sequence, ID: string(change.ID),
	}
	if change.Object != nil {
		object := protocolProjection(*change.Object)
		value.Object = &object
	}
	return value
}

func domainChange(change sourcedriverproto.Change) (sourcedriver.Change, error) {
	value := sourcedriver.Change{
		Kind: domainChangeKind(change.Kind), Tenant: catalog.TenantID(change.Tenant),
		Generation: causal.Generation(change.Generation), Sequence: change.Sequence, ID: sourcedriver.LogicalID(change.ID),
	}
	if change.Object != nil {
		object, err := domainProjection(*change.Object)
		if err != nil {
			return sourcedriver.Change{}, err
		}
		value.Object = &object
	}
	return value, sourcedriver.ValidateChange(value)
}

func protocolPageKind(kind sourcedriver.PageKind) sourcedriverproto.PageKind {
	if kind == sourcedriver.PageSnapshot {
		return sourcedriverproto.PageKindSnapshot
	}
	if kind == sourcedriver.PageChanges {
		return sourcedriverproto.PageKindChanges
	}
	return ""
}

func domainPageKind(kind sourcedriverproto.PageKind) sourcedriver.PageKind {
	if kind == sourcedriverproto.PageKindSnapshot {
		return sourcedriver.PageSnapshot
	}
	if kind == sourcedriverproto.PageKindChanges {
		return sourcedriver.PageChanges
	}
	return 0
}

func mustContinuation(value string) []byte {
	decoded, _ := base64.StdEncoding.DecodeString(value)
	return decoded
}

func protocolMutationContext(value catalog.SourceMutationContext) sourcedriverproto.MutationContext {
	return sourcedriverproto.MutationContext{
		Operation: sourcedriverproto.MutationOperation{
			Kind: uint8(value.Operation.Kind), Name: value.Operation.Name, ObjectKind: uint8(value.Operation.ObjectKind),
			Mode: value.Operation.Mode, LinkTarget: value.Operation.LinkTarget, HasContent: value.Operation.HasContent,
		},
		Object: protocolLocator(value.Object), Parent: protocolLocator(value.Parent), Target: protocolLocator(value.Target),
	}
}

func domainMutationContext(value sourcedriverproto.MutationContext) catalog.SourceMutationContext {
	return catalog.SourceMutationContext{
		Operation: catalog.SourceMutationOperation{
			Kind: catalog.MutationKind(value.Operation.Kind), Name: value.Operation.Name,
			ObjectKind: catalog.Kind(value.Operation.ObjectKind), Mode: value.Operation.Mode,
			LinkTarget: value.Operation.LinkTarget, HasContent: value.Operation.HasContent,
		},
		Object: domainLocator(value.Object), Parent: domainLocator(value.Parent), Target: domainLocator(value.Target),
	}
}

func protocolLocator(locator *catalog.SourceLocator) *sourcedriverproto.SourceLocator {
	if locator == nil {
		return nil
	}
	return &sourcedriverproto.SourceLocator{
		Authority: string(locator.SourceAuthority), Key: string(locator.SourceKey), Revision: uint64(locator.SourceRevision),
	}
}

func domainLocator(locator *sourcedriverproto.SourceLocator) *catalog.SourceLocator {
	if locator == nil {
		return nil
	}
	return &catalog.SourceLocator{
		SourceAuthority: causal.SourceAuthorityID(locator.Authority), SourceKey: catalog.SourceObjectKey(locator.Key),
		SourceRevision: causal.Revision(locator.Revision),
	}
}

func protocolReceipt(receipt sourcedriver.MutationReceipt) sourcedriverproto.MutationReceipt {
	value := sourcedriverproto.MutationReceipt{
		OperationID: receipt.OperationID.String(), State: protocolMutationState(receipt.State), Expected: string(receipt.Expected),
		Committed: string(receipt.Committed), Result: string(receipt.Result),
	}
	if receipt.RequestDigest != ([sha256.Size]byte{}) {
		value.RequestDigest = hex.EncodeToString(receipt.RequestDigest[:])
	}
	if receipt.Digest != ([sha256.Size]byte{}) {
		value.Digest = hex.EncodeToString(receipt.Digest[:])
	}
	return value
}

func domainReceipt(receipt sourcedriverproto.MutationReceipt) (sourcedriver.MutationReceipt, error) {
	id, err := catalog.ParseMutationID(receipt.OperationID)
	if err != nil {
		return sourcedriver.MutationReceipt{}, err
	}
	value := sourcedriver.MutationReceipt{
		OperationID: id, State: domainMutationState(receipt.State), Expected: sourcedriver.RevisionToken(receipt.Expected),
		Committed: sourcedriver.RevisionToken(receipt.Committed), Result: sourcedriver.LogicalID(receipt.Result),
	}
	if receipt.RequestDigest != "" {
		value.RequestDigest, err = digest(receipt.RequestDigest)
		if err != nil {
			return sourcedriver.MutationReceipt{}, err
		}
	}
	if receipt.Digest != "" {
		value.Digest, err = digest(receipt.Digest)
		if err != nil {
			return sourcedriver.MutationReceipt{}, err
		}
	}
	return value, sourcedriver.ValidateMutationReceipt(value)
}

func protocolSettlement(settlement sourcedriver.MutationSettlement) sourcedriverproto.MutationSettlement {
	return sourcedriverproto.MutationSettlement{
		TargetSet:     protocolTargetSetRef(settlement.TargetSet),
		OperationID:   settlement.OperationID.String(),
		RequestDigest: hex.EncodeToString(settlement.RequestDigest[:]),
		ReceiptDigest: hex.EncodeToString(settlement.ReceiptDigest[:]),
		Kind:          protocolSettlementKind(settlement.Kind),
	}
}

func domainSettlement(settlement sourcedriverproto.MutationSettlement) (sourcedriver.MutationSettlement, error) {
	id, err := catalog.ParseMutationID(settlement.OperationID)
	if err != nil {
		return sourcedriver.MutationSettlement{}, err
	}
	value := sourcedriver.MutationSettlement{
		OperationID: id,
		Kind:        domainSettlementKind(settlement.Kind),
	}
	value.TargetSet, err = domainTargetSetRef(settlement.TargetSet)
	if err != nil {
		return sourcedriver.MutationSettlement{}, err
	}
	value.RequestDigest, err = digest(settlement.RequestDigest)
	if err != nil {
		return sourcedriver.MutationSettlement{}, err
	}
	value.ReceiptDigest, err = digest(settlement.ReceiptDigest)
	if err != nil {
		return sourcedriver.MutationSettlement{}, err
	}
	return value, sourcedriver.ValidateMutationSettlement(value)
}

func protocolObjectKind(kind catalog.Kind) sourcedriverproto.ObjectKind {
	switch kind {
	case catalog.KindDirectory:
		return sourcedriverproto.ObjectKindDirectory
	case catalog.KindFile:
		return sourcedriverproto.ObjectKindFile
	case catalog.KindSymlink:
		return sourcedriverproto.ObjectKindSymlink
	default:
		return ""
	}
}

func domainObjectKind(kind sourcedriverproto.ObjectKind) catalog.Kind {
	switch kind {
	case sourcedriverproto.ObjectKindDirectory:
		return catalog.KindDirectory
	case sourcedriverproto.ObjectKindFile:
		return catalog.KindFile
	case sourcedriverproto.ObjectKindSymlink:
		return catalog.KindSymlink
	default:
		return 0
	}
}

func protocolChangeKind(kind sourcedriver.ChangeKind) sourcedriverproto.ChangeKind {
	if kind == sourcedriver.ChangeDelete {
		return sourcedriverproto.ChangeKindDelete
	}
	if kind == sourcedriver.ChangeUpsert {
		return sourcedriverproto.ChangeKindUpsert
	}
	return ""
}

func domainChangeKind(kind sourcedriverproto.ChangeKind) sourcedriver.ChangeKind {
	if kind == sourcedriverproto.ChangeKindDelete {
		return sourcedriver.ChangeDelete
	}
	if kind == sourcedriverproto.ChangeKindUpsert {
		return sourcedriver.ChangeUpsert
	}
	return 0
}

func protocolMutationState(state sourcedriver.MutationState) sourcedriverproto.MutationState {
	switch state {
	case sourcedriver.MutationNotFound:
		return sourcedriverproto.MutationStateNotFound
	case sourcedriver.MutationPrepared:
		return sourcedriverproto.MutationStatePrepared
	case sourcedriver.MutationApplied:
		return sourcedriverproto.MutationStateApplied
	default:
		return ""
	}
}

func domainMutationState(state sourcedriverproto.MutationState) sourcedriver.MutationState {
	switch state {
	case sourcedriverproto.MutationStateNotFound:
		return sourcedriver.MutationNotFound
	case sourcedriverproto.MutationStatePrepared:
		return sourcedriver.MutationPrepared
	case sourcedriverproto.MutationStateApplied:
		return sourcedriver.MutationApplied
	default:
		return 0
	}
}

func protocolSettlementKind(kind sourcedriver.MutationSettlementKind) sourcedriverproto.MutationSettlementKind {
	switch kind {
	case sourcedriver.MutationSettlementAcknowledge:
		return sourcedriverproto.MutationSettlementAcknowledge
	case sourcedriver.MutationSettlementAbandon:
		return sourcedriverproto.MutationSettlementAbandon
	case sourcedriver.MutationSettlementForget:
		return sourcedriverproto.MutationSettlementForget
	default:
		return ""
	}
}

func domainSettlementKind(kind sourcedriverproto.MutationSettlementKind) sourcedriver.MutationSettlementKind {
	switch kind {
	case sourcedriverproto.MutationSettlementAcknowledge:
		return sourcedriver.MutationSettlementAcknowledge
	case sourcedriverproto.MutationSettlementAbandon:
		return sourcedriver.MutationSettlementAbandon
	case sourcedriverproto.MutationSettlementForget:
		return sourcedriver.MutationSettlementForget
	default:
		return 0
	}
}

func contentHash(value string) (catalog.ContentHash, error) {
	var hash catalog.ContentHash
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(hash) {
		return hash, fmt.Errorf("source driver service: invalid content hash")
	}
	copy(hash[:], decoded)
	return hash, nil
}

func digest(value string) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != len(result) {
		return result, fmt.Errorf("source driver service: invalid digest")
	}
	copy(result[:], decoded)
	return result, nil
}
