// Package causal defines the storage-neutral identities shared by catalog commits and convergence.
package causal

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"unicode/utf8"
)

// TenantID identifies one logical tenant.
type TenantID string

// DomainID identifies one external domain.
type DomainID string

const domainIdentityPrefix = "fusekit.domain.v1\x00"

// DeriveDomainID returns the stable path-free identity for one owner presentation instance.
func DeriveDomainID(owner, presentationInstance string) (DomainID, error) {
	if owner == "" || presentationInstance == "" || strings.ContainsRune(owner, 0) || strings.ContainsRune(presentationInstance, 0) {
		return "", errors.New("causal: domain identity is empty or contains NUL")
	}
	digest := sha256.Sum256([]byte(domainIdentityPrefix + owner + "\x00" + presentationInstance))
	return DomainID("fk-" + hex.EncodeToString(digest[:])), nil
}

// Generation identifies one exact tenant/domain incarnation.
type Generation uint64

// SourceAuthorityID identifies one independently ordered authoritative source.
type SourceAuthorityID string

// SourceAuthorityIDMaxBytes is the exact transport and storage identity limit.
const SourceAuthorityIDMaxBytes = 255

// ValidateSourceAuthorityID verifies one source authority at the shared protocol boundary.
func ValidateSourceAuthorityID(authority SourceAuthorityID) error {
	value := string(authority)
	if value == "" || len(value) > SourceAuthorityIDMaxBytes ||
		!utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return errors.New("causal: invalid source authority id")
	}
	return nil
}

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
	// CauseBootstrap is an authoritative fresh-state or full-authority bootstrap publication.
	CauseBootstrap Cause = "bootstrap"
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
	Tenant                  TenantID
	CatalogRevision         CatalogRevision
	FileProviderFingerprint [32]byte
}

// OutboxClaim is the durable identity of one claimed source change.
type OutboxClaim struct {
	ChangeID ChangeID
	Cursor   OutboxCursor
}

// OutboxCursor independently continues affected keys and catalog commits.
type OutboxCursor struct {
	Sequence    uint64
	AfterKey    LogicalKey
	AfterTenant TenantID
}

// OutboxSettlement is the exact terminal proof for one fully paged claim.
type OutboxSettlement struct {
	ChangeID ChangeID
	Digest   [32]byte
}

// OutboxPage is one bounded page of a claimed source change.
type OutboxPage struct {
	Change     ChangeSet
	Commits    []CatalogCommit
	Next       *OutboxCursor
	Settlement *OutboxSettlement
}
