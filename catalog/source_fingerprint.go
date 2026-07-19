package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"slices"
)

const (
	sourceTenantCatalogFingerprintDomain = "fusekit.catalog.source-tenant.catalog-v1\x00"
	catalogFileProviderFingerprintDomain = "fusekit.catalog.file-provider-v1\x00"
)

type sourceFingerprintObject struct {
	key        SourceObjectKey
	parent     SourceObjectKey
	name       string
	kind       Kind
	mode       uint32
	size       int64
	hash       ContentHash
	linkTarget string
	visibility Visibility
}

type sourceTenantFingerprints struct {
	catalog      [32]byte
	fileProvider [32]byte
}

type fileProviderFingerprintObject struct {
	id         ObjectID
	parent     ObjectID
	name       string
	kind       Kind
	mode       uint32
	size       int64
	hash       ContentHash
	linkTarget string
}

func sourceCatalogProjectionFingerprint(
	generation Generation,
	root SourceObjectKey,
	objects []sourceFingerprintObject,
) ([32]byte, error) {
	ordered := append([]sourceFingerprintObject(nil), objects...)
	slices.SortFunc(ordered, func(left, right sourceFingerprintObject) int {
		switch {
		case left.key < right.key:
			return -1
		case left.key > right.key:
			return 1
		default:
			return 0
		}
	})
	builder, err := newSourceCatalogFingerprintBuilder(generation, root)
	if err != nil {
		return [32]byte{}, err
	}
	for _, object := range ordered {
		if err := builder.add(object); err != nil {
			return [32]byte{}, err
		}
	}
	return builder.sum(), nil
}

func newSourceCatalogFingerprintBuilder(
	generation Generation,
	root SourceObjectKey,
) (*sourceTenantFingerprintBuilder, error) {
	if generation == 0 || !validSourceKey(root) {
		return nil, fmt.Errorf("%w: incomplete source tenant fingerprint identity", ErrInvalidObject)
	}
	builder := &sourceTenantFingerprintBuilder{
		encoder: sourceFingerprintBuilder{digest: sha256.New()},
	}
	builder.encoder.bytes([]byte(sourceTenantCatalogFingerprintDomain))
	builder.encoder.uint64(uint64(generation))
	builder.encoder.string(string(root))
	return builder, nil
}

type sourceTenantFingerprintBuilder struct {
	encoder sourceFingerprintBuilder
	last    SourceObjectKey
}

func (b *sourceTenantFingerprintBuilder) add(object sourceFingerprintObject) error {
	if !validSourceKey(object.key) || (object.parent != "" && !validSourceKey(object.parent)) ||
		object.size < 0 || (b.last != "" && b.last >= object.key) {
		return fmt.Errorf("%w: invalid source tenant fingerprint object", ErrInvalidObject)
	}
	b.encoder.string(string(object.key))
	b.encoder.string(string(object.parent))
	b.encoder.string(object.name)
	b.encoder.uint64(uint64(object.kind))
	b.encoder.uint64(uint64(object.mode))
	b.encoder.uint64(uint64(object.size))
	b.encoder.bytes(object.hash[:])
	b.encoder.string(object.linkTarget)
	b.encoder.boolean(object.visibility.Mount)
	b.encoder.boolean(object.visibility.FileProvider)
	b.last = object.key
	return nil
}

func (b *sourceTenantFingerprintBuilder) sum() [32]byte {
	var result [32]byte
	copy(result[:], b.encoder.digest.Sum(nil))
	return result
}

type sourceFingerprintBuilder struct {
	digest hash.Hash
}

func (b sourceFingerprintBuilder) uint64(value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = b.digest.Write(encoded[:])
}

func (b sourceFingerprintBuilder) bytes(value []byte) {
	b.uint64(uint64(len(value)))
	_, _ = b.digest.Write(value)
}

func (b sourceFingerprintBuilder) string(value string) {
	b.bytes([]byte(value))
}

func (b sourceFingerprintBuilder) boolean(value bool) {
	if value {
		_, _ = b.digest.Write([]byte{1})
		return
	}
	_, _ = b.digest.Write([]byte{0})
}

type fileProviderFingerprintBuilder struct {
	encoder sourceFingerprintBuilder
	last    ObjectID
}

func newFileProviderFingerprintBuilder(root ObjectID) (*fileProviderFingerprintBuilder, error) {
	if root == (ObjectID{}) {
		return nil, fmt.Errorf("%w: incomplete File Provider fingerprint root", ErrInvalidObject)
	}
	builder := &fileProviderFingerprintBuilder{
		encoder: sourceFingerprintBuilder{digest: sha256.New()},
	}
	builder.encoder.bytes([]byte(catalogFileProviderFingerprintDomain))
	builder.encoder.bytes(root[:])
	return builder, nil
}

func (b *fileProviderFingerprintBuilder) add(object fileProviderFingerprintObject) error {
	if object.id == (ObjectID{}) || object.parent == (ObjectID{}) || object.size < 0 ||
		(b.last != (ObjectID{}) && slices.Compare(b.last[:], object.id[:]) >= 0) {
		return fmt.Errorf("%w: invalid File Provider fingerprint object", ErrInvalidObject)
	}
	b.encoder.bytes(object.id[:])
	b.encoder.bytes(object.parent[:])
	b.encoder.string(object.name)
	b.encoder.uint64(uint64(object.kind))
	b.encoder.uint64(uint64(object.mode))
	b.encoder.uint64(uint64(object.size))
	b.encoder.bytes(object.hash[:])
	b.encoder.string(object.linkTarget)
	b.last = object.id
	return nil
}

func (b *fileProviderFingerprintBuilder) sum() [32]byte {
	var result [32]byte
	copy(result[:], b.encoder.digest.Sum(nil))
	return result
}

func fileProviderProjectionFingerprint(
	root ObjectID,
	objects []fileProviderFingerprintObject,
) ([32]byte, error) {
	ordered := append([]fileProviderFingerprintObject(nil), objects...)
	slices.SortFunc(ordered, func(left, right fileProviderFingerprintObject) int {
		return slices.Compare(left.id[:], right.id[:])
	})
	builder, err := newFileProviderFingerprintBuilder(root)
	if err != nil {
		return [32]byte{}, err
	}
	for _, object := range ordered {
		if err := builder.add(object); err != nil {
			return [32]byte{}, err
		}
	}
	return builder.sum(), nil
}

func catalogFileProviderFingerprint(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
) ([32]byte, error) {
	var root []byte
	if err := tx.QueryRowContext(ctx, `
SELECT root_id FROM tenants WHERE tenant = ?`, string(tenant)).Scan(&root); err != nil {
		return [32]byte{}, fmt.Errorf("catalog: read File Provider fingerprint identity: %w", err)
	}
	if len(root) != len(ObjectID{}) {
		return [32]byte{}, fmt.Errorf("%w: corrupt File Provider fingerprint identity", ErrIntegrity)
	}
	rootID, err := objectID(root)
	if err != nil {
		return [32]byte{}, err
	}
	builder, err := newFileProviderFingerprintBuilder(rootID)
	if err != nil {
		return [32]byte{}, err
	}
	view, err := readCatalogView(ctx, tx, tenant)
	if err != nil {
		return [32]byte{}, err
	}
	var query string
	var args []any
	if len(view.publication) != 0 {
		query = `
SELECT object_id, parent_id, name, kind, mode, size, hash, link_target
FROM source_driver_publication_objects
WHERE source_authority = ? AND publication_id = ? AND tenant = ?
  AND object_id <> ? AND revision <= ? AND tombstone = 0 AND file_provider_visible = 1
ORDER BY object_id`
		args = []any{view.authority, view.publication, string(tenant), root, uint64(view.head)}
	} else {
		query = `
SELECT object_id, parent_id, name, kind, mode, size, hash, link_target
FROM object_versions object
WHERE object.tenant = ? AND object.object_id <> ?
  AND object.revision = (SELECT MAX(version.revision) FROM object_versions version
      WHERE version.tenant = object.tenant AND version.object_id = object.object_id
        AND version.revision <= ?)
  AND object.tombstone = 0 AND object.file_provider_visible = 1
ORDER BY object.object_id`
		args = []any{string(tenant), root, uint64(view.head)}
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return [32]byte{}, fmt.Errorf("catalog: query File Provider fingerprint objects: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var object, parent, contentHash []byte
		var name, link string
		var kind uint8
		var mode uint32
		var size int64
		if err := rows.Scan(&object, &parent, &name, &kind, &mode, &size, &contentHash, &link); err != nil {
			return [32]byte{}, fmt.Errorf("catalog: scan File Provider fingerprint object: %w", err)
		}
		if len(object) != len(ObjectID{}) || len(parent) != len(ObjectID{}) ||
			len(contentHash) != len(ContentHash{}) || size < 0 {
			return [32]byte{}, fmt.Errorf("%w: corrupt File Provider fingerprint object", ErrIntegrity)
		}
		objectIdentity, err := objectID(object)
		if err != nil {
			return [32]byte{}, err
		}
		parentID, err := objectID(parent)
		if err != nil {
			return [32]byte{}, err
		}
		var hash ContentHash
		copy(hash[:], contentHash)
		if err := builder.add(fileProviderFingerprintObject{
			id: objectIdentity, parent: parentID, name: name, kind: Kind(kind), mode: mode,
			size: size, hash: hash, linkTarget: link,
		}); err != nil {
			return [32]byte{}, err
		}
	}
	if err := rows.Err(); err != nil {
		return [32]byte{}, fmt.Errorf("catalog: read File Provider fingerprint objects: %w", err)
	}
	return builder.sum(), nil
}
