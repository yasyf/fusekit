package catalogservice

import (
	"bytes"
	"encoding/hex"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
	"github.com/yasyf/fusekit/causal"
)

func catalogPublishDesiredSourceFleetRequest(
	request catalogproto.PublishDesiredSourceFleetRequest,
) (catalog.PublishDesiredSourceFleetRequest, error) {
	declarations := make([]catalog.SourceAuthorityDeclaration, len(request.Declarations))
	for index, declaration := range request.Declarations {
		digest, err := decodeDigest(declaration.DeclarationDigest)
		if err != nil {
			return catalog.PublishDesiredSourceFleetRequest{}, err
		}
		declarations[index] = catalog.SourceAuthorityDeclaration{
			Authority: causal.SourceAuthorityID(declaration.Authority), DriverID: declaration.DriverID,
			DriverConfig: bytes.Clone(declaration.DriverConfig), DeclarationDigest: digest,
		}
	}
	result := catalog.PublishDesiredSourceFleetRequest{
		Owner:              catalog.SourceAuthorityFleetOwnerID(request.Owner),
		ExpectedGeneration: causal.Generation(request.ExpectedGeneration),
		Generation:         causal.Generation(request.Generation),
		Declarations:       declarations,
	}
	if err := result.Validate(); err != nil {
		return catalog.PublishDesiredSourceFleetRequest{}, err
	}
	return result, nil
}

func protocolDesiredSourceFleetState(
	state catalog.DesiredSourceAuthorityFleetState,
) (catalogproto.DesiredSourceFleetState, error) {
	result := catalogproto.DesiredSourceFleetState{
		Owner: string(state.Owner), Generation: uint64(state.Generation), AuthorityCount: state.AuthorityCount,
		AuthoritiesDigest:  hex.EncodeToString(state.AuthoritiesDigest[:]),
		DeclarationsDigest: hex.EncodeToString(state.DeclarationsDigest[:]),
	}
	if err := catalogproto.Validate(result); err != nil {
		return catalogproto.DesiredSourceFleetState{}, err
	}
	return result, nil
}

func catalogDesiredSourceFleetPageRequest(
	request catalogproto.ReadDesiredSourceFleetRequest,
) (catalog.DesiredSourceFleetPageRequest, error) {
	var digest [32]byte
	var err error
	if request.SnapshotDigest != nil {
		digest, err = decodeDigest(*request.SnapshotDigest)
		if err != nil {
			return catalog.DesiredSourceFleetPageRequest{}, err
		}
	}
	after := causal.SourceAuthorityID("")
	if request.After != nil {
		after = causal.SourceAuthorityID(*request.After)
	}
	return catalog.DesiredSourceFleetPageRequest{
		Owner: catalog.SourceAuthorityFleetOwnerID(request.Owner), Generation: causal.Generation(request.Generation),
		DeclarationsDigest: digest, After: after, Limit: int(request.Limit),
	}, nil
}

func protocolDesiredSourceFleetPage(
	page catalog.DesiredSourceFleetPage,
) (catalogproto.ReadDesiredSourceFleetResponse, error) {
	state, err := protocolDesiredSourceFleetState(page.State)
	if err != nil {
		return catalogproto.ReadDesiredSourceFleetResponse{}, err
	}
	declarations := make([]catalogproto.SourceAuthorityDeclaration, len(page.Declarations))
	for index, declaration := range page.Declarations {
		declarations[index] = catalogproto.SourceAuthorityDeclaration{
			Authority: catalogproto.SourceAuthorityID(declaration.Authority), DriverID: declaration.DriverID,
			DriverConfig:      bytes.Clone(declaration.DriverConfig),
			DeclarationDigest: hex.EncodeToString(declaration.DeclarationDigest[:]),
		}
	}
	var next *catalogproto.SourceAuthorityID
	if page.Next != "" {
		value := catalogproto.SourceAuthorityID(page.Next)
		next = &value
	}
	response := catalogproto.ReadDesiredSourceFleetResponse{
		Protocol: catalogproto.Version, Code: catalogproto.ErrorCodeOk, State: &state,
		Declarations: declarations, Next: next,
	}
	if err := catalogproto.Validate(response); err != nil {
		return catalogproto.ReadDesiredSourceFleetResponse{}, err
	}
	return response, nil
}

func decodeDigest(value string) ([32]byte, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return [32]byte{}, fmt.Errorf("catalog service: invalid digest")
	}
	var digest [32]byte
	copy(digest[:], decoded)
	return digest, nil
}

func protocolObject(object catalog.Object) (catalogproto.CatalogObject, error) {
	if _, err := remoteObjectBudget(object); err != nil {
		return catalogproto.CatalogObject{}, err
	}
	if object.Size < 0 {
		return catalogproto.CatalogObject{}, fmt.Errorf("catalog service: negative object size")
	}
	kind := catalogproto.ObjectKindDirectory
	hash := ""
	switch object.Kind {
	case catalog.KindDirectory:
	case catalog.KindFile:
		kind = catalogproto.ObjectKindFile
		hash = hex.EncodeToString(object.Hash[:])
	case catalog.KindSymlink:
		kind = catalogproto.ObjectKindSymlink
		hash = hex.EncodeToString(object.Hash[:])
	default:
		return catalogproto.CatalogObject{}, fmt.Errorf("catalog service: unknown object kind %d", object.Kind)
	}
	result := catalogproto.CatalogObject{
		ID:               catalogproto.ObjectID(object.ID.String()),
		ParentID:         catalogproto.ObjectID(object.Parent.String()),
		Revision:         uint64(object.Revision),
		MetadataRevision: uint64(object.MetadataRevision),
		ContentRevision:  uint64(object.ContentRevision),
		Name:             object.Name,
		Kind:             kind,
		Mode:             object.Mode,
		Size:             uint64(object.Size),
		Hash:             hash,
		LinkTarget:       object.LinkTarget,
		Desired:          uint64(object.Convergence.Desired),
		Observed:         uint64(object.Convergence.Observed),
		Verified:         uint64(object.Convergence.Verified),
		Applied:          uint64(object.Convergence.Applied),
		Tombstone:        object.Tombstone,
	}
	if err := catalogproto.Validate(result); err != nil {
		return catalogproto.CatalogObject{}, err
	}
	return result, nil
}

func protocolObjects(objects []catalog.Object) ([]catalogproto.CatalogObject, error) {
	if len(objects) > int(catalogproto.MaxPageSize) {
		return nil, fmt.Errorf("%w: snapshot exceeds the remote object count", catalog.ErrIntegrity)
	}
	pageBudget := 0
	for _, object := range objects {
		objectBudget, err := remoteObjectBudget(object)
		if err != nil || objectBudget > remotePageWireBudget-pageBudget {
			return nil, fmt.Errorf("%w: snapshot exceeds the semantic wire budget", catalog.ErrIntegrity)
		}
		pageBudget += objectBudget
	}
	result := make([]catalogproto.CatalogObject, 0, len(objects))
	for _, object := range objects {
		converted, err := protocolObject(object)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, nil
}

func protocolChanges(changes []catalog.Change) ([]catalogproto.Change, error) {
	if len(changes) > int(catalogproto.MaxPageSize) {
		return nil, fmt.Errorf("%w: changes exceed the remote object count", catalog.ErrIntegrity)
	}
	pageBudget := 0
	for _, change := range changes {
		objectBudget, err := remoteObjectBudget(change.Object)
		if err != nil || objectBudget > remotePageWireBudget-pageBudget-remoteChangeFixedWireBudget {
			return nil, fmt.Errorf("%w: changes exceed the semantic wire budget", catalog.ErrIntegrity)
		}
		pageBudget += objectBudget + remoteChangeFixedWireBudget
	}
	result := make([]catalogproto.Change, 0, len(changes))
	for _, change := range changes {
		object, err := protocolObject(change.Object)
		if err != nil {
			return nil, err
		}
		kind := catalogproto.ChangeKindUpsert
		switch change.Kind {
		case catalog.ChangeUpsert:
		case catalog.ChangeDelete:
			kind = catalogproto.ChangeKindDelete
		default:
			return nil, fmt.Errorf("catalog service: unknown change kind %d", change.Kind)
		}
		result = append(result, catalogproto.Change{
			Revision: uint64(change.Revision), Sequence: change.Sequence, Kind: kind, Object: object,
		})
	}
	return result, nil
}

func catalogObjectID(id catalogproto.ObjectID) (catalog.ObjectID, error) {
	return catalog.ParseObjectID(string(id))
}

func catalogEnumerationScope(scope catalogproto.EnumerationScope) (catalog.EnumerationScope, error) {
	switch scope.Kind {
	case catalogproto.EnumerationScopeKindWorkingSet:
		return catalog.EnumerationScope{Kind: catalog.EnumerationWorkingSet}, nil
	case catalogproto.EnumerationScopeKindContainer:
		if scope.ParentID == nil {
			return catalog.EnumerationScope{}, fmt.Errorf("catalog service: container scope has no parent")
		}
		parent, err := catalogObjectID(*scope.ParentID)
		if err != nil {
			return catalog.EnumerationScope{}, err
		}
		return catalog.EnumerationScope{Kind: catalog.EnumerationContainer, Parent: parent}, nil
	default:
		return catalog.EnumerationScope{}, fmt.Errorf("catalog service: unknown enumeration scope %q", scope.Kind)
	}
}

func protocolObjectID(id catalog.ObjectID) *catalogproto.ObjectID {
	value := catalogproto.ObjectID(id.String())
	return &value
}
