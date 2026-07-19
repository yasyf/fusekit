package catalogservice

import (
	"encoding/hex"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogproto"
)

func protocolObject(object catalog.Object) (catalogproto.CatalogObject, error) {
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

func protocolChanges(changes []catalog.Change) ([]catalogproto.Change, error) {
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

func catalogMutationID(id catalogproto.MutationID) (catalog.MutationID, error) {
	return catalog.ParseMutationID(string(id))
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
