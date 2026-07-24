package catalog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"sort"
	"strings"

	"github.com/yasyf/fusekit/causal"
)

const (
	// CriticalObjectLimit bounds one launch-readiness proof.
	CriticalObjectLimit = 32
	// CriticalObjectRoleByteLimit bounds the product-defined semantic role.
	CriticalObjectRoleByteLimit      = 128
	criticalObjectLogicalIDByteLimit = 255
)

// CriticalObjectBindingFailure identifies a failed logical source binding.
type CriticalObjectBindingFailure uint8

const (
	// CriticalObjectBindingMissing means no live File Provider object has the logical identity.
	CriticalObjectBindingMissing CriticalObjectBindingFailure = iota + 1
	// CriticalObjectBindingDuplicate means the logical identity did not resolve uniquely.
	CriticalObjectBindingDuplicate
)

// CriticalObjectBindingError is a typed fail-closed logical binding error.
type CriticalObjectBindingError struct {
	LogicalID string
	Failure   CriticalObjectBindingFailure
}

func (e *CriticalObjectBindingError) Error() string {
	return fmt.Sprintf("catalog: critical logical object %q binding failure %d", e.LogicalID, e.Failure)
}

// CriticalObjectRequirement is one stable product logical identity and semantic role.
type CriticalObjectRequirement struct {
	LogicalID string
	Role      string
}

// CriticalObjectResolutionRequest fences logical resolution to one published catalog head.
type CriticalObjectResolutionRequest struct {
	Authority   causal.SourceAuthorityID
	Publication causal.OperationID
	Tenant      TenantID
	Generation  Generation
	Head        Revision
	Objects     []CriticalObjectRequirement
}

// ResolvedCriticalObject is the immutable object proof required for launch readiness.
type ResolvedCriticalObject struct {
	LogicalID       string
	Role            string
	ObjectID        ObjectID
	ObjectRevision  Revision
	ContentRevision Revision
	Size            int64
	Hash            ContentHash
}

// CriticalObjectResolution proves a bounded logical set at one exact catalog head.
type CriticalObjectResolution struct {
	Authority   causal.SourceAuthorityID
	Publication causal.OperationID
	Tenant      TenantID
	Generation  Generation
	Head        Revision
	Objects     []ResolvedCriticalObject
}

// CriticalObjectPolicyDigest canonically identifies one ordered logical-role set.
func CriticalObjectPolicyDigest(objects []CriticalObjectRequirement) ([sha256.Size]byte, error) {
	request := CriticalObjectResolutionRequest{
		Authority: "digest", Publication: causal.OperationID{1}, Tenant: "digest", Generation: 1, Head: 1,
		Objects: objects,
	}
	if err := validateCriticalObjectResolutionRequest(request); err != nil {
		return [sha256.Size]byte{}, err
	}
	digest := sha256.New()
	writeCriticalDigestString(digest, "fusekit.critical-object-policy.v1")
	writeCriticalDigestUint64(digest, uint64(len(objects)))
	for _, object := range objects {
		writeCriticalDigestString(digest, object.LogicalID)
		writeCriticalDigestString(digest, object.Role)
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

// CriticalObjectResolutionDigest canonically identifies resolved opaque objects and contents.
func CriticalObjectResolutionDigest(resolution CriticalObjectResolution) ([sha256.Size]byte, error) {
	request := CriticalObjectResolutionRequest{
		Authority: resolution.Authority, Publication: resolution.Publication, Tenant: resolution.Tenant,
		Generation: resolution.Generation, Head: resolution.Head,
		Objects: make([]CriticalObjectRequirement, len(resolution.Objects)),
	}
	for index, object := range resolution.Objects {
		request.Objects[index] = CriticalObjectRequirement{LogicalID: object.LogicalID, Role: object.Role}
	}
	if err := resolution.Validate(request); err != nil {
		return [sha256.Size]byte{}, err
	}
	digest := sha256.New()
	writeCriticalDigestString(digest, "fusekit.critical-object-resolution.v1")
	writeCriticalDigestUint64(digest, uint64(len(resolution.Objects)))
	for _, object := range resolution.Objects {
		writeCriticalDigestString(digest, object.LogicalID)
		writeCriticalDigestString(digest, object.Role)
		_, _ = digest.Write(object.ObjectID[:])
		writeCriticalDigestUint64(digest, uint64(object.ObjectRevision))
		writeCriticalDigestUint64(digest, uint64(object.ContentRevision))
		writeCriticalDigestUint64(digest, uint64(object.Size))
		_, _ = digest.Write(object.Hash[:])
	}
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

// Validate verifies a critical-object request without database access.
func (r CriticalObjectResolutionRequest) Validate() error {
	return validateCriticalObjectResolutionRequest(r)
}

// Validate verifies an exact ordered resolution against its request.
func (r CriticalObjectResolution) Validate(request CriticalObjectResolutionRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if r.Authority != request.Authority || r.Publication != request.Publication || r.Tenant != request.Tenant ||
		r.Generation != request.Generation || r.Head != request.Head || len(r.Objects) != len(request.Objects) {
		return fmt.Errorf("%w: critical object resolution fence changed", ErrIntegrity)
	}
	for index, object := range r.Objects {
		expected := request.Objects[index]
		if object.LogicalID != expected.LogicalID || object.Role != expected.Role || object.ObjectID == (ObjectID{}) ||
			object.ObjectRevision == 0 || object.ContentRevision == 0 || object.Size < 0 || object.Hash == (ContentHash{}) {
			return fmt.Errorf("%w: critical object resolution is incomplete", ErrIntegrity)
		}
	}
	return nil
}

// ResolveCriticalObjects resolves stable source logical identities to the live opaque
// File Provider objects in one exact publication snapshot.
func (c *Catalog) ResolveCriticalObjects(
	ctx context.Context,
	request CriticalObjectResolutionRequest,
) (CriticalObjectResolution, error) {
	if err := validateCriticalObjectResolutionRequest(request); err != nil {
		return CriticalObjectResolution{}, err
	}
	tx, err := c.readDB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return CriticalObjectResolution{}, fmt.Errorf("catalog: begin critical object resolution: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	view, err := readCatalogView(ctx, tx, request.Tenant)
	if err != nil {
		return CriticalObjectResolution{}, err
	}
	if view.head != request.Head || view.authority != string(request.Authority) ||
		len(view.publication) != len(request.Publication) ||
		!equalOperationID(view.publication, request.Publication) {
		return CriticalObjectResolution{}, fmt.Errorf("%w: critical object catalog view changed", ErrInvalidTransition)
	}
	provision, found, err := appliedTenantProvision(ctx, tx, request.Tenant)
	if err != nil {
		return CriticalObjectResolution{}, fmt.Errorf("catalog: read critical object tenant generation: %w", err)
	}
	if !found {
		return CriticalObjectResolution{}, ErrNotFound
	}
	if provision.Generation != request.Generation {
		return CriticalObjectResolution{}, ErrGenerationMismatch
	}

	result := CriticalObjectResolution{
		Authority: request.Authority, Publication: request.Publication,
		Tenant: request.Tenant, Generation: request.Generation, Head: request.Head,
		Objects: make([]ResolvedCriticalObject, 0, len(request.Objects)),
	}
	for _, requirement := range request.Objects {
		rows, err := tx.QueryContext(ctx, `
SELECT `+versionColumns+`
FROM source_authority_bindings binding
JOIN source_driver_publication_objects v
  ON v.source_authority = binding.source_authority
 AND v.source_key = binding.source_key
WHERE binding.source_authority = ? AND binding.logical_id = ?
  AND v.publication_id = ? AND v.tenant = ?
  AND v.revision <= ? AND v.kind = ? AND v.tombstone = 0 AND v.file_provider_visible = 1
ORDER BY v.object_id`, string(request.Authority), requirement.LogicalID, request.Publication[:],
			string(request.Tenant), uint64(request.Head), uint8(KindFile))
		if err != nil {
			return CriticalObjectResolution{}, fmt.Errorf("catalog: query critical object %q: %w", requirement.LogicalID, err)
		}
		var matches []Object
		for rows.Next() {
			object, scanErr := scanObject(rows)
			if scanErr != nil {
				_ = rows.Close()
				return CriticalObjectResolution{}, fmt.Errorf("catalog: scan critical object %q: %w", requirement.LogicalID, scanErr)
			}
			matches = append(matches, object)
		}
		rowErr := rows.Err()
		closeErr := rows.Close()
		if rowErr != nil || closeErr != nil {
			return CriticalObjectResolution{}, errors.Join(rowErr, closeErr)
		}
		switch len(matches) {
		case 0:
			return CriticalObjectResolution{}, &CriticalObjectBindingError{LogicalID: requirement.LogicalID, Failure: CriticalObjectBindingMissing}
		case 1:
			object := matches[0]
			result.Objects = append(result.Objects, ResolvedCriticalObject{
				LogicalID: requirement.LogicalID, Role: requirement.Role, ObjectID: object.ID,
				ObjectRevision: object.Revision, ContentRevision: object.ContentRevision,
				Size: object.Size, Hash: object.Hash,
			})
		default:
			return CriticalObjectResolution{}, &CriticalObjectBindingError{LogicalID: requirement.LogicalID, Failure: CriticalObjectBindingDuplicate}
		}
	}
	if err := result.Validate(request); err != nil {
		return CriticalObjectResolution{}, err
	}
	return result, nil
}

func validateCriticalObjectResolutionRequest(request CriticalObjectResolutionRequest) error {
	if request.Authority == "" || request.Publication == (causal.OperationID{}) || request.Tenant == "" ||
		request.Generation == 0 || request.Head == 0 || len(request.Objects) == 0 || len(request.Objects) > CriticalObjectLimit {
		return fmt.Errorf("%w: critical object resolution identity is incomplete", ErrInvalidObject)
	}
	seen := make(map[string]struct{}, len(request.Objects))
	for _, object := range request.Objects {
		if object.LogicalID == "" || len(object.LogicalID) > criticalObjectLogicalIDByteLimit || strings.ContainsRune(object.LogicalID, 0) ||
			object.Role == "" || len(object.Role) > CriticalObjectRoleByteLimit || strings.ContainsRune(object.Role, 0) {
			return fmt.Errorf("%w: critical object identity is invalid", ErrInvalidObject)
		}
		if _, ok := seen[object.LogicalID]; ok {
			return &CriticalObjectBindingError{LogicalID: object.LogicalID, Failure: CriticalObjectBindingDuplicate}
		}
		seen[object.LogicalID] = struct{}{}
	}
	if !sort.SliceIsSorted(request.Objects, func(i, j int) bool {
		return request.Objects[i].LogicalID < request.Objects[j].LogicalID
	}) {
		return fmt.Errorf("%w: critical logical identities are not ordered", ErrInvalidObject)
	}
	return nil
}

func equalOperationID(raw []byte, expected causal.OperationID) bool {
	if len(raw) != len(expected) {
		return false
	}
	for index := range raw {
		if raw[index] != expected[index] {
			return false
		}
	}
	return true
}

func writeCriticalDigestString(digest hash.Hash, value string) {
	writeCriticalDigestUint64(digest, uint64(len(value)))
	_, _ = digest.Write([]byte(value))
}

func writeCriticalDigestUint64(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}
