package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yasyf/fusekit/causal"
)

// PrepareFileProviderLease durably binds one provisional readiness policy to a reservation operation.
func (c *Catalog) PrepareFileProviderLease(ctx context.Context, lease FileProviderLease) (FileProviderLease, error) {
	if err := validatePreparedFileProviderLease(lease); err != nil {
		return FileProviderLease{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: begin provisional File Provider lease: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := validateFileProviderLeaseDomain(ctx, tx, lease); err != nil {
		return FileProviderLease{}, err
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_leases(
    lease_id, tenant, domain_id, generation, root_id, presentation_instance_id,
    state, session_id, process_identity, policy_digest, resolution_digest,
    catalog_head, source_authority, source_publication, source_revision,
    activation_generation, expires_unix_nano
) VALUES (?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(lease_id) DO UPDATE SET expires_unix_nano = excluded.expires_unix_nano
WHERE file_provider_leases.tenant = excluded.tenant
  AND file_provider_leases.domain_id = excluded.domain_id
  AND file_provider_leases.generation = excluded.generation
  AND file_provider_leases.root_id = excluded.root_id
  AND file_provider_leases.presentation_instance_id = excluded.presentation_instance_id
  AND file_provider_leases.state = ?
  AND file_provider_leases.session_id = '' AND file_provider_leases.process_identity = ''
  AND file_provider_leases.policy_digest = excluded.policy_digest
  AND file_provider_leases.resolution_digest = excluded.resolution_digest
  AND file_provider_leases.catalog_head = excluded.catalog_head
  AND file_provider_leases.source_authority = excluded.source_authority
  AND file_provider_leases.source_publication = excluded.source_publication
  AND file_provider_leases.source_revision = excluded.source_revision
  AND file_provider_leases.activation_generation = excluded.activation_generation
  AND excluded.expires_unix_nano >= file_provider_leases.expires_unix_nano`,
		lease.ID, string(lease.Tenant), string(lease.DomainID), uint64(lease.Generation), lease.Root[:], lease.PresentationInstance,
		uint8(FileProviderLeaseProvisional), lease.PolicyDigest[:], lease.ResolutionDigest[:], uint64(lease.CatalogHead),
		string(lease.SourceAuthority), lease.SourcePublication[:], uint64(lease.SourceRevision), lease.ActivationGeneration,
		lease.ExpiresAt.UnixNano(), uint8(FileProviderLeaseProvisional))
	if err != nil {
		return FileProviderLease{}, mapConstraint(err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: inspect provisional File Provider lease: %w", err)
	}
	if changed != 1 {
		return FileProviderLease{}, fmt.Errorf("%w: provisional File Provider lease identity changed", ErrInvalidTransition)
	}
	for index, object := range lease.CriticalObjects {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO file_provider_critical_objects(
    lease_id, position, logical_id, role, object_id, object_revision,
    content_revision, size, hash
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(lease_id, position) DO NOTHING`, lease.ID, index, object.LogicalID, object.Role,
			object.ObjectID[:], uint64(object.ObjectRevision), uint64(object.ContentRevision), object.Size, object.Hash[:]); err != nil {
			return FileProviderLease{}, mapConstraint(err)
		}
	}
	stored, err := readFileProviderLease(ctx, tx, lease.ID)
	if err != nil {
		return FileProviderLease{}, err
	}
	if !sameFileProviderLease(stored, lease, true) {
		return FileProviderLease{}, fmt.Errorf("%w: provisional File Provider lease replay changed", ErrInvalidTransition)
	}
	if err := tx.Commit(); err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: commit provisional File Provider lease: %w", err)
	}
	return stored, nil
}

// CommitFileProviderLease promotes one exact provisional receipt to live demand.
func (c *Catalog) CommitFileProviderLease(ctx context.Context, lease FileProviderLease) (FileProviderLease, error) {
	return c.updateCommittedFileProviderLease(ctx, lease, true)
}

// RenewFileProviderLease extends one exact committed live demand receipt.
func (c *Catalog) RenewFileProviderLease(ctx context.Context, lease FileProviderLease) (FileProviderLease, error) {
	return c.updateCommittedFileProviderLease(ctx, lease, false)
}

func (c *Catalog) updateCommittedFileProviderLease(
	ctx context.Context,
	lease FileProviderLease,
	allowProvisional bool,
) (FileProviderLease, error) {
	if err := validateCommittedFileProviderLease(lease); err != nil {
		return FileProviderLease{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: begin committed File Provider lease update: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stored, err := readFileProviderLease(ctx, tx, lease.ID)
	if err != nil {
		return FileProviderLease{}, err
	}
	validState := stored.State == FileProviderLeaseCommitted || allowProvisional && stored.State == FileProviderLeaseProvisional
	if !validState || !sameFileProviderLease(stored, lease, false) || lease.ExpiresAt.Before(stored.ExpiresAt) {
		return FileProviderLease{}, fmt.Errorf("%w: committed File Provider lease identity changed", ErrInvalidTransition)
	}
	if stored.State == FileProviderLeaseCommitted &&
		(stored.SessionID != lease.SessionID || stored.ProcessIdentity != lease.ProcessIdentity) {
		return FileProviderLease{}, fmt.Errorf("%w: committed File Provider session identity changed", ErrInvalidTransition)
	}
	if stored.State == FileProviderLeaseProvisional && (stored.SessionID != "" || stored.ProcessIdentity != "") {
		return FileProviderLease{}, fmt.Errorf("%w: provisional File Provider receipt is corrupt", ErrIntegrity)
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE file_provider_leases
SET state = ?, session_id = ?, process_identity = ?, expires_unix_nano = ?
WHERE lease_id = ? AND state = ?`, uint8(FileProviderLeaseCommitted), lease.SessionID,
		lease.ProcessIdentity, lease.ExpiresAt.UnixNano(), lease.ID, uint8(stored.State)); err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: update committed File Provider lease: %w", err)
	}
	committed, err := readFileProviderLease(ctx, tx, lease.ID)
	if err != nil {
		return FileProviderLease{}, err
	}
	if committed.State != FileProviderLeaseCommitted || !sameFileProviderLease(committed, lease, true) {
		return FileProviderLease{}, fmt.Errorf("%w: committed File Provider receipt changed", ErrIntegrity)
	}
	if err := tx.Commit(); err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: commit File Provider lease update: %w", err)
	}
	return committed, nil
}

// ReleaseFileProviderLease retires one exact receipt and its eager-content policy.
func (c *Catalog) ReleaseFileProviderLease(ctx context.Context, lease FileProviderLease) (FileProviderLease, error) {
	if err := validateFileProviderLeaseFence(lease); err != nil {
		return FileProviderLease{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: begin File Provider lease release: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	stored, err := readFileProviderLease(ctx, tx, lease.ID)
	if err != nil {
		return FileProviderLease{}, err
	}
	if !sameFileProviderLease(stored, lease, false) ||
		((stored.State == FileProviderLeaseCommitted || stored.State == FileProviderLeaseReleased) &&
			(stored.SessionID != lease.SessionID || stored.ProcessIdentity != lease.ProcessIdentity)) {
		return FileProviderLease{}, fmt.Errorf("%w: released File Provider lease identity changed", ErrInvalidTransition)
	}
	if stored.State != FileProviderLeaseReleased {
		if stored.State != FileProviderLeaseProvisional && stored.State != FileProviderLeaseCommitted {
			return FileProviderLease{}, fmt.Errorf("%w: File Provider lease is not releasable", ErrInvalidTransition)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM file_provider_critical_objects WHERE lease_id = ?`, lease.ID); err != nil {
			return FileProviderLease{}, fmt.Errorf("catalog: demote File Provider content policy: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE file_provider_leases SET state = ? WHERE lease_id = ? AND state = ?`,
			uint8(FileProviderLeaseReleased), lease.ID, uint8(stored.State)); err != nil {
			return FileProviderLease{}, fmt.Errorf("catalog: release File Provider lease: %w", err)
		}
	}
	released, err := readFileProviderLease(ctx, tx, lease.ID)
	if err != nil {
		return FileProviderLease{}, err
	}
	if released.State != FileProviderLeaseReleased {
		return FileProviderLease{}, fmt.Errorf("%w: File Provider lease release did not settle", ErrIntegrity)
	}
	if err := tx.Commit(); err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: commit File Provider lease release: %w", err)
	}
	return released, nil
}

// FileProviderContentPolicy reports lease-scoped eager retention for one exact object.
func (c *Catalog) FileProviderContentPolicy(
	ctx context.Context,
	tenant TenantID,
	domain causal.DomainID,
	generation Generation,
	object ObjectID,
	now time.Time,
) (bool, error) {
	if tenant == "" || domain == "" || generation == 0 || object == (ObjectID{}) || now.IsZero() {
		return false, fmt.Errorf("%w: File Provider content policy identity is incomplete", ErrInvalidObject)
	}
	var count uint64
	if err := c.readDB.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM file_provider_critical_objects critical
JOIN file_provider_leases lease ON lease.lease_id = critical.lease_id
WHERE lease.tenant = ? AND lease.domain_id = ? AND lease.generation = ?
  AND lease.state IN (?, ?) AND lease.expires_unix_nano > ? AND critical.object_id = ?`,
		string(tenant), string(domain), uint64(generation), uint8(FileProviderLeaseProvisional),
		uint8(FileProviderLeaseCommitted), now.UnixNano(), object[:]).Scan(&count); err != nil {
		return false, fmt.Errorf("catalog: read File Provider content policy: %w", err)
	}
	return count != 0, nil
}

func validatePreparedFileProviderLease(lease FileProviderLease) error {
	if err := validateFileProviderLeaseFence(lease); err != nil {
		return err
	}
	if lease.State != FileProviderLeaseProvisional || lease.SessionID != "" || lease.ProcessIdentity != "" ||
		len(lease.CriticalObjects) == 0 {
		return fmt.Errorf("%w: provisional File Provider lease is invalid", ErrInvalidObject)
	}
	policy, err := CriticalObjectPolicyDigest(requirementsFromResolution(lease.CriticalObjects))
	if err != nil {
		return err
	}
	resolution, err := CriticalObjectResolutionDigest(CriticalObjectResolution{
		Authority: lease.SourceAuthority, Publication: lease.SourcePublication,
		Tenant: lease.Tenant, Generation: lease.Generation, Head: lease.CatalogHead,
		Objects: lease.CriticalObjects,
	})
	if err != nil {
		return err
	}
	if policy != lease.PolicyDigest || resolution != lease.ResolutionDigest {
		return fmt.Errorf("%w: File Provider lease policy digest changed", ErrIntegrity)
	}
	return nil
}

func validateCommittedFileProviderLease(lease FileProviderLease) error {
	if err := validateFileProviderLeaseFence(lease); err != nil {
		return err
	}
	if lease.State != FileProviderLeaseCommitted || lease.SessionID == "" || lease.ProcessIdentity == "" ||
		strings.ContainsRune(lease.SessionID, 0) || strings.ContainsRune(lease.ProcessIdentity, 0) {
		return fmt.Errorf("%w: committed File Provider lease identity is invalid", ErrInvalidObject)
	}
	return nil
}

func validateFileProviderLeaseFence(lease FileProviderLease) error {
	if lease.ID == "" || strings.ContainsRune(lease.ID, 0) || lease.Tenant == "" || lease.DomainID == "" ||
		lease.Generation == 0 || lease.Root == (ObjectID{}) || lease.PresentationInstance == "" ||
		lease.PolicyDigest == ([32]byte{}) || lease.ResolutionDigest == ([32]byte{}) || lease.CatalogHead == 0 ||
		lease.SourceAuthority == "" || lease.SourcePublication == ([16]byte{}) || lease.SourceRevision == 0 ||
		lease.ActivationGeneration == "" || lease.ExpiresAt.IsZero() || lease.ExpiresAt.UnixNano() <= 0 {
		return fmt.Errorf("%w: File Provider lease fence is incomplete", ErrInvalidObject)
	}
	return nil
}

func validateFileProviderLeaseDomain(ctx context.Context, tx *sql.Tx, lease FileProviderLease) error {
	domain, found, err := fileProviderDomainForTenant(ctx, tx, lease.Tenant)
	if err != nil {
		return err
	}
	if !found {
		return ErrNotFound
	}
	if !domain.Registered || domain.DomainID != lease.DomainID || domain.Generation != lease.Generation ||
		domain.Root != lease.Root || domain.PresentationInstance != lease.PresentationInstance ||
		domain.ActivationGeneration != lease.ActivationGeneration {
		return fmt.Errorf("%w: File Provider lease does not match registered presentation", ErrInvalidTransition)
	}
	return nil
}

func readFileProviderLease(ctx context.Context, tx *sql.Tx, id string) (FileProviderLease, error) {
	var lease FileProviderLease
	var tenant, domain, authority string
	var root, publication, policy, resolution []byte
	var generation, state, head, sourceRevision uint64
	var expires int64
	if err := tx.QueryRowContext(ctx, `
SELECT lease_id, tenant, domain_id, generation, root_id, presentation_instance_id,
       state, session_id, process_identity, policy_digest, resolution_digest,
       catalog_head, source_authority, source_publication, source_revision,
       activation_generation, expires_unix_nano
FROM file_provider_leases WHERE lease_id = ?`, id).Scan(
		&lease.ID, &tenant, &domain, &generation, &root, &lease.PresentationInstance,
		&state, &lease.SessionID, &lease.ProcessIdentity, &policy, &resolution,
		&head, &authority, &publication, &sourceRevision, &lease.ActivationGeneration, &expires,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return FileProviderLease{}, ErrNotFound
		}
		return FileProviderLease{}, fmt.Errorf("catalog: read File Provider lease: %w", err)
	}
	lease.Tenant, lease.DomainID, lease.Generation = TenantID(tenant), causal.DomainID(domain), Generation(generation)
	lease.State, lease.CatalogHead = FileProviderLeaseState(state), Revision(head)
	lease.SourceAuthority, lease.SourceRevision = causal.SourceAuthorityID(authority), causal.Revision(sourceRevision)
	lease.ExpiresAt = time.Unix(0, expires).UTC()
	if copyExactID(lease.Root[:], root) != nil || copyExactID(lease.SourcePublication[:], publication) != nil ||
		copyExactID(lease.PolicyDigest[:], policy) != nil || copyExactID(lease.ResolutionDigest[:], resolution) != nil {
		return FileProviderLease{}, ErrIntegrity
	}
	rows, err := tx.QueryContext(ctx, `
SELECT logical_id, role, object_id, object_revision, content_revision, size, hash
FROM file_provider_critical_objects WHERE lease_id = ? ORDER BY position`, id)
	if err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: read File Provider critical objects: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var object ResolvedCriticalObject
		var rawID, rawHash []byte
		var objectRevision, contentRevision uint64
		if err := rows.Scan(&object.LogicalID, &object.Role, &rawID, &objectRevision, &contentRevision, &object.Size, &rawHash); err != nil {
			return FileProviderLease{}, fmt.Errorf("catalog: scan File Provider critical object: %w", err)
		}
		if copyExactID(object.ObjectID[:], rawID) != nil || copyExactID(object.Hash[:], rawHash) != nil {
			return FileProviderLease{}, ErrIntegrity
		}
		object.ObjectRevision, object.ContentRevision = Revision(objectRevision), Revision(contentRevision)
		lease.CriticalObjects = append(lease.CriticalObjects, object)
	}
	if err := rows.Err(); err != nil {
		return FileProviderLease{}, fmt.Errorf("catalog: finish File Provider critical objects: %w", err)
	}
	return lease, nil
}

func sameFileProviderLease(stored, expected FileProviderLease, includeState bool) bool {
	if stored.ID != expected.ID || stored.Tenant != expected.Tenant || stored.DomainID != expected.DomainID ||
		stored.Generation != expected.Generation || stored.Root != expected.Root ||
		stored.PresentationInstance != expected.PresentationInstance || stored.PolicyDigest != expected.PolicyDigest ||
		stored.ResolutionDigest != expected.ResolutionDigest || stored.CatalogHead != expected.CatalogHead ||
		stored.SourceAuthority != expected.SourceAuthority || stored.SourcePublication != expected.SourcePublication ||
		stored.SourceRevision != expected.SourceRevision || stored.ActivationGeneration != expected.ActivationGeneration ||
		includeState && stored.State != expected.State || len(expected.CriticalObjects) != 0 && !sameResolvedCriticalObjects(stored.CriticalObjects, expected.CriticalObjects) {
		return false
	}
	return true
}

func sameResolvedCriticalObjects(left, right []ResolvedCriticalObject) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func requirementsFromResolution(objects []ResolvedCriticalObject) []CriticalObjectRequirement {
	requirements := make([]CriticalObjectRequirement, len(objects))
	for index, object := range objects {
		requirements[index] = CriticalObjectRequirement{LogicalID: object.LogicalID, Role: object.Role}
	}
	return requirements
}
