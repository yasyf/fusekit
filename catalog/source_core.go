package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/yasyf/fusekit/causal"
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
	RootKey    SourceObjectKey
	Objects    []SourceObject
	Deletes    []SourceObjectKey
}

// SourceResult proves one exact durable authority revision.
type SourceResult struct {
	Authority causal.SourceAuthorityID
	Revision  causal.Revision
	ChangeID  causal.ChangeID
	Operation causal.OperationID
}

// ErrSourcePredecessor means a delta did not name the exact durable predecessor.
var ErrSourcePredecessor = errors.New("catalog: source predecessor mismatch")

// ErrSourceRequiresSnapshot means a missing or skipped revision requires a full snapshot.
var ErrSourceRequiresSnapshot = errors.New("catalog: source snapshot required")

// ErrSourceLocatorMissing means an authority has not published an opaque key
// for a catalog object required by an external mutation.
var ErrSourceLocatorMissing = errors.New("catalog: source locator missing")

// ErrSourceLocatorStale means a persisted mutation locator no longer matches
// the authority watermark or binding that produced it.
var ErrSourceLocatorStale = errors.New("catalog: source locator stale")

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
	value := string(key)
	return value != "" && len(value) <= 255 && !strings.ContainsAny(value, "/\\") &&
		strings.IndexFunc(value, unicode.IsControl) < 0
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
	var reserved int
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM source_key_reservations WHERE source_authority = ? AND source_key = ?`,
		string(authority), string(key)).Scan(&reserved); err != nil {
		return ObjectID{}, fmt.Errorf("catalog: inspect source key reservation: %w", err)
	}
	if reserved != 0 {
		return ObjectID{}, ErrMutationActive
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

func (c *Catalog) claimSourceContent(ctx context.Context, tx *sql.Tx, operation causal.OperationID, ref ContentRef) error {
	result, err := tx.ExecContext(ctx, `
UPDATE content_stages SET source_operation_id = ?
WHERE stage_id = ? AND owner_id = ? AND mutation_id IS NULL
  AND source_operation_id IS NULL AND published = 1`, operation[:], ref.Stage[:], c.owner[:])
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

func (c *Catalog) consumeSourceContent(ctx context.Context, tx *sql.Tx, operation causal.OperationID, ref ContentRef) error {
	if err := c.validateSourceContentRef(ctx, tx, operation, KindFile, ref); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
DELETE FROM content_stages
WHERE stage_id = ? AND source_operation_id = ? AND published = 1`, ref.Stage[:], operation[:])
	if err != nil {
		return fmt.Errorf("catalog: consume source content stage: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("catalog: inspect consumed source content stage: %w", err)
	}
	if changed != 1 {
		return fmt.Errorf("%w: source content stage was already consumed", ErrInvalidTransition)
	}
	return nil
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

func sourceCatalogOperation(operation causal.OperationID, tenant TenantID) MutationID {
	digest := sha256.Sum256(append(append([]byte("fusekit.catalog.source-commit\x00"), operation[:]...), []byte(tenant)...))
	var result MutationID
	copy(result[:], digest[:len(result)])
	return result
}
