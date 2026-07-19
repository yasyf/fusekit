// Package causal defines the storage-neutral identities shared by catalog commits and convergence.
package causal

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
)

// TenantID identifies one logical tenant.
type TenantID string

// DomainID identifies one external domain.
type DomainID string

const domainIdentityPrefix = "fusekit.domain.v1\x00"

// DeriveDomainID returns the stable path-free identity for one owner account instance.
func DeriveDomainID(owner, accountInstance string) (DomainID, error) {
	if owner == "" || accountInstance == "" || strings.ContainsRune(owner, 0) || strings.ContainsRune(accountInstance, 0) {
		return "", errors.New("causal: domain identity is empty or contains NUL")
	}
	digest := sha256.Sum256([]byte(domainIdentityPrefix + owner + "\x00" + accountInstance))
	return DomainID("fk-" + hex.EncodeToString(digest[:])), nil
}

// Generation identifies one exact tenant/domain incarnation.
type Generation uint64

// SourceAuthorityID identifies one independently ordered authoritative source.
type SourceAuthorityID string

// LogicalKey identifies one source key whose change can affect effective content.
type LogicalKey string

// Revision identifies one monotonic source or engine revision.
type Revision uint64

// CatalogRevision identifies a tenant-local immutable catalog revision.
type CatalogRevision uint64

// ChangeID identifies one published source change.
type ChangeID [16]byte

// OperationID identifies the operation that produced a source change.
type OperationID [16]byte

// Cause classifies the known source of convergence work.
type Cause string

const (
	// CauseProviderMutation is a write already acknowledged by its originating domain.
	CauseProviderMutation Cause = "provider_mutation"
	// CauseDaemonWrite is a source mutation initiated by the owning daemon or mount.
	CauseDaemonWrite Cause = "daemon_write"
	// CauseExternalUnattributed is an observed external change with no guessed writer identity.
	CauseExternalUnattributed Cause = "external_unattributed"
	// CauseMigration is a source mutation produced by catalog or state migration.
	CauseMigration Cause = "migration"
	// CauseOnDemand is an engine-generated recovery or preparation attempt.
	CauseOnDemand Cause = "on_demand"
)

// ChangeSet is the immutable causal contract for one published source revision.
type ChangeSet struct {
	SourceAuthority  SourceAuthorityID
	SourceRevision   Revision
	ChangeID         ChangeID
	OperationID      OperationID
	Cause            Cause
	Origin           DomainID
	OriginGeneration Generation
	AffectedKeys     []LogicalKey
}

// CatalogCommit identifies one tenant catalog commit covered by a source change.
type CatalogCommit struct {
	Tenant          TenantID
	CatalogRevision CatalogRevision
}

// OutboxBatch is one complete source change and every catalog commit it covers.
type OutboxBatch struct {
	Change  ChangeSet
	Commits []CatalogCommit
}
