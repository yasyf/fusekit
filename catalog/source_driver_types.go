package catalog

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/yasyf/fusekit/causal"
)

const (
	SourceDriverTokenMaxBytes             = 255
	SourceDriverCursorMaxBytes            = 4096
	SourceDriverTargetLimit               = 10_000
	SourceDriverTargetCheckpointPageLimit = 256
	SourceDriverReceiptAuthorityPageLimit = 256

	sourceDriverPagePredecessorDomain  = "fusekit.source-driver-page-predecessor.v1\x00"
	sourceDriverTargetsDigestDomain    = "fusekit.source-driver-targets.v1\x00"
	sourceDriverTargetsDigestStateSize = sha256.Size
	sourceDriverTargetBatchSize        = 128
)

type SourceDriverMode uint8

const (
	SourceDriverSnapshot SourceDriverMode = iota + 1
	SourceDriverDelta
	SourceDriverMutation
)

type SourceDriverSnapshotReason uint8

const (
	SourceDriverSnapshotInitial SourceDriverSnapshotReason = iota + 1
	SourceDriverSnapshotReset
	SourceDriverSnapshotExpiredFloor
)

// SourceDriverTarget is one declared tenant projection of an authority.
type SourceDriverTarget struct {
	Tenant     TenantID   `json:"tenant"`
	Generation Generation `json:"generation"`
}

// SourceDriverTargetCheckpoint is one tenant's applied authority watermark.
type SourceDriverTargetCheckpoint struct {
	SourceDriverTarget
	RootKey         SourceObjectKey
	TargetEpoch     uint64
	SourceRevision  causal.Revision
	CatalogRevision Revision
}

// SourceDriverCheckpoint is one authority-wide opaque source fence.
type SourceDriverCheckpoint struct {
	Authority           causal.SourceAuthorityID
	FleetOwner          SourceAuthorityFleetOwnerID
	AuthorityGeneration causal.Generation
	DeclarationDigest   [sha256.Size]byte
	TargetEpoch         uint64
	TargetCount         uint64
	TargetsDigest       [sha256.Size]byte
	Token               string
	TokenDigest         [sha256.Size]byte
	PublicationID       causal.OperationID
	PublicationDigest   [sha256.Size]byte
	SourceRevision      causal.Revision
	SourceOperation     causal.OperationID
	ChangeID            causal.ChangeID
	Cause               causal.Cause
	Origin              causal.DomainID
	OriginGeneration    causal.Generation
	SnapshotRequired    SourceDriverSnapshotReason
}

// SourceDriverStageIdentity binds one immutable authority-wide publication.
type SourceDriverStageIdentity struct {
	Authority             causal.SourceAuthorityID
	FleetOwner            SourceAuthorityFleetOwnerID
	AuthorityGeneration   causal.Generation
	DeclarationDigest     [sha256.Size]byte
	TargetCount           uint64
	TargetsDigest         [sha256.Size]byte
	Operation             causal.OperationID
	SourceOperation       causal.OperationID
	ChangeID              causal.ChangeID
	Cause                 causal.Cause
	Origin                causal.DomainID
	OriginGeneration      causal.Generation
	Mode                  SourceDriverMode
	SnapshotReason        SourceDriverSnapshotReason
	FromToken             string
	ToToken               string
	Predecessor           causal.Revision
	Mutation              MutationID
	MutationTenant        TenantID
	MutationGeneration    Generation
	MutationResult        SourceObjectKey
	MutationRequestDigest [sha256.Size]byte
	MutationReceiptDigest [sha256.Size]byte
	Claim                 MutationClaim
}

// SourceDriverStageEntry is one tenant-fenced logical upsert or delete.
// Object nil denotes delete; otherwise Object.Key must equal Key.
type SourceDriverStageEntry struct {
	Tenant         TenantID
	Generation     Generation
	ChangeSequence uint64
	Key            SourceObjectKey
	Object         *SourceObject
}

type SourceDriverStagePage struct {
	Sequence          uint64
	Cursor            []byte
	Digest            [sha256.Size]byte
	PredecessorDigest [sha256.Size]byte
	Entries           []SourceDriverStageEntry
	Complete          bool
}

type SourceDriverStageState struct {
	Identity    SourceDriverStageIdentity
	Stage       SourcePublicationStageRef
	TargetEpoch uint64
	Cursor      []byte
	PageDigest  [sha256.Size]byte
}

type SourceDriverStageResult struct {
	Identity       SourceDriverStageIdentity
	Proof          SourcePublicationStageRef
	Stage          SourcePublicationStageResult
	Checkpoint     SourceDriverCheckpoint
	MutationResult *NamespaceMutationResult
	ReceiptDigest  [sha256.Size]byte
}

// SourceDriverTargetCheckpointPage is one bounded immutable publication target page.
type SourceDriverTargetCheckpointPage struct {
	Targets []SourceDriverTargetCheckpoint
	Next    TenantID
}

// SourceDriverCommittedReceipt is one committed catalog result awaiting exact runtime acknowledgement.
type SourceDriverCommittedReceipt struct {
	Result       SourceDriverStageResult
	Acknowledged bool
	Forgotten    bool
}

// SourceDriverReceiptAuthorityPage is one bounded keyset page of authorities with unsettled receipts.
type SourceDriverReceiptAuthorityPage struct {
	Authorities []causal.SourceAuthorityID
	Next        causal.SourceAuthorityID
}

// SourceDriverMutationReservationRequest binds one pre-I/O external mutation attempt.
type SourceDriverMutationReservationRequest struct {
	Mutation            MutationID
	Claim               MutationClaim
	Authority           causal.SourceAuthorityID
	FleetOwner          SourceAuthorityFleetOwnerID
	AuthorityGeneration causal.Generation
	DeclarationDigest   [sha256.Size]byte
	TargetCount         uint64
	TargetsDigest       [sha256.Size]byte
	Target              SourceDriverTarget
	FromToken           string
	Predecessor         causal.Revision
	Operation           causal.OperationID
	SourceOperation     causal.OperationID
	ChangeID            causal.ChangeID
}

// SourceDriverMutationReceiptProof is one exact external source result.
type SourceDriverMutationReceiptProof struct {
	ToToken string
	Result  SourceObjectKey
	Digest  [sha256.Size]byte
}

// SourceDriverMutationReservation is the durable pre-I/O fence and its optional receipt.
type SourceDriverMutationReservation struct {
	SourceDriverMutationReservationRequest
	TargetEpoch         uint64
	DeclaredTargetCount uint64
	TargetCursor        TenantID
	TargetsPrepared     bool
	RequestDigest       [sha256.Size]byte
	RequestBound        bool
	Committed           bool
	Receipt             *SourceDriverMutationReceiptProof
}

// SourceDriverTargetPage is one bounded page of an exact reserved target set.
type SourceDriverTargetPage struct {
	Targets []SourceDriverTarget
	Next    TenantID
}

type SourceDriverCheckpointRebind struct {
	Expected            SourceDriverCheckpoint
	AuthorityGeneration causal.Generation
	DeclarationDigest   [sha256.Size]byte
}

// DeriveSourceDriverRootKey returns one stable path-free target root key.
func DeriveSourceDriverRootKey(authority causal.SourceAuthorityID, tenant TenantID) (SourceObjectKey, error) {
	if err := causal.ValidateSourceAuthorityID(authority); err != nil || tenant == "" {
		return "", fmt.Errorf("%w: invalid source driver target identity", ErrInvalidObject)
	}
	digest := sha256.Sum256([]byte("fusekit.source-driver-root.v1\x00" + string(authority) + "\x00" + string(tenant)))
	return SourceObjectKey("driver-root:" + hex.EncodeToString(digest[:])), nil
}

// SourceDriverTargetsDigest verifies and digests one exact sorted target set.
func SourceDriverTargetsDigest(targets []SourceDriverTarget) ([sha256.Size]byte, error) {
	if len(targets) == 0 || len(targets) > SourceDriverTargetLimit {
		return [sha256.Size]byte{}, fmt.Errorf("%w: invalid source driver target count", ErrInvalidObject)
	}
	state, err := newSourceDriverTargetsDigestState(uint64(len(targets)))
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	var prior TenantID
	for index, target := range targets {
		if target.Tenant == "" || target.Generation == 0 ||
			(index > 0 && targets[index-1].Tenant >= target.Tenant) {
			return [sha256.Size]byte{}, fmt.Errorf("%w: source driver targets are not sorted and unique", ErrInvalidObject)
		}
		state, err = appendSourceDriverTargetsDigestState(state, prior, target)
		if err != nil {
			return [sha256.Size]byte{}, err
		}
		prior = target.Tenant
	}
	return finishSourceDriverTargetsDigestState(state)
}

func newSourceDriverTargetsDigestState(count uint64) ([]byte, error) {
	if count == 0 || count > SourceDriverTargetLimit {
		return nil, fmt.Errorf("%w: invalid source driver target count", ErrInvalidObject)
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(sourceDriverTargetsDigestDomain))
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], count)
	_, _ = hash.Write(encoded[:])
	state := make([]byte, sha256.Size)
	copy(state, hash.Sum(nil))
	return state, nil
}

func appendSourceDriverTargetsDigestState(
	state []byte,
	prior TenantID,
	target SourceDriverTarget,
) ([]byte, error) {
	if target.Tenant == "" || target.Generation == 0 || (prior != "" && prior >= target.Tenant) {
		return nil, fmt.Errorf("%w: source driver targets are not sorted and unique", ErrInvalidObject)
	}
	if len(state) != sourceDriverTargetsDigestStateSize {
		return nil, ErrIntegrity
	}
	hash := sha256.New()
	_, _ = hash.Write(state)
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(len(target.Tenant)))
	_, _ = hash.Write(encoded[:])
	_, _ = hash.Write([]byte(target.Tenant))
	binary.BigEndian.PutUint64(encoded[:], uint64(target.Generation))
	_, _ = hash.Write(encoded[:])
	next := make([]byte, sha256.Size)
	copy(next, hash.Sum(nil))
	return next, nil
}

func finishSourceDriverTargetsDigestState(state []byte) ([sha256.Size]byte, error) {
	if len(state) != sourceDriverTargetsDigestStateSize {
		return [sha256.Size]byte{}, ErrIntegrity
	}
	var digest [sha256.Size]byte
	copy(digest[:], state)
	return digest, nil
}

// SourceDriverPagePredecessorDigest binds a page to the exact prior durable cursor and page digest.
func SourceDriverPagePredecessorDigest(cursor []byte, pageDigest [sha256.Size]byte) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(sourceDriverPagePredecessorDomain))
	_, _ = hash.Write(cursor)
	_, _ = hash.Write(pageDigest[:])
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

func validateSourceDriverStageIdentity(identity SourceDriverStageIdentity) ([sha256.Size]byte, error) {
	if err := causal.ValidateSourceAuthorityID(identity.Authority); err != nil || identity.FleetOwner == "" ||
		identity.AuthorityGeneration == 0 || identity.DeclarationDigest == ([sha256.Size]byte{}) ||
		identity.TargetCount == 0 || identity.TargetCount > SourceDriverTargetLimit ||
		identity.TargetsDigest == ([sha256.Size]byte{}) || identity.Operation == (causal.OperationID{}) ||
		identity.SourceOperation == (causal.OperationID{}) || identity.SourceOperation == identity.Operation ||
		identity.ChangeID == (causal.ChangeID{}) || !validSourceDriverToken(identity.ToToken) {
		return [sha256.Size]byte{}, fmt.Errorf("%w: invalid source driver stage identity", ErrInvalidObject)
	}
	change := causal.ChangeSet{
		SourceAuthority: identity.Authority, SourceRevision: identity.Predecessor + 1,
		ChangeID: identity.ChangeID, OperationID: identity.SourceOperation,
		Cause: identity.Cause, Origin: identity.Origin, OriginGeneration: identity.OriginGeneration,
		AffectedKeys: []causal.LogicalKey{"driver"},
	}
	if err := validateSourceChange(change); err != nil {
		return [sha256.Size]byte{}, err
	}
	switch identity.Mode {
	case SourceDriverSnapshot:
		if identity.SnapshotReason < SourceDriverSnapshotInitial || identity.SnapshotReason > SourceDriverSnapshotExpiredFloor ||
			identity.FromToken != "" || !zeroSourceDriverMutation(identity) {
			return [sha256.Size]byte{}, fmt.Errorf("%w: invalid source driver snapshot identity", ErrInvalidObject)
		}
	case SourceDriverDelta:
		if identity.SnapshotReason != 0 || !validSourceDriverToken(identity.FromToken) ||
			identity.FromToken == identity.ToToken || !zeroSourceDriverMutation(identity) {
			return [sha256.Size]byte{}, fmt.Errorf("%w: invalid source driver delta identity", ErrInvalidObject)
		}
	case SourceDriverMutation:
		if (identity.SnapshotReason != 0 && identity.SnapshotReason != SourceDriverSnapshotReset) ||
			!validSourceDriverToken(identity.FromToken) ||
			identity.FromToken == identity.ToToken || identity.Mutation == (MutationID{}) ||
			identity.MutationTenant == "" || identity.MutationGeneration == 0 ||
			identity.MutationRequestDigest == ([sha256.Size]byte{}) ||
			identity.MutationReceiptDigest == ([sha256.Size]byte{}) ||
			(identity.MutationResult != "" && !validSourceKey(identity.MutationResult)) ||
			validateMutationClaim(identity.Claim) != nil {
			return [sha256.Size]byte{}, fmt.Errorf("%w: invalid source driver mutation identity", ErrInvalidObject)
		}
	default:
		return [sha256.Size]byte{}, fmt.Errorf("%w: invalid source driver mode", ErrInvalidObject)
	}
	encoded, err := json.Marshal(identity)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func validateSourceDriverStagePage(mode SourceDriverMode, page SourceDriverStagePage) (int, error) {
	if page.Digest == ([sha256.Size]byte{}) || page.PredecessorDigest == ([sha256.Size]byte{}) ||
		len(page.Cursor) > SourceDriverCursorMaxBytes || len(page.Entries) > SourcePublicationStagePageItemLimit ||
		page.Complete != (len(page.Cursor) == 0) {
		return 0, fmt.Errorf("%w: invalid source driver stage page", ErrInvalidObject)
	}
	for index, entry := range page.Entries {
		if entry.Tenant == "" || entry.Generation == 0 || !validSourceKey(entry.Key) ||
			(mode == SourceDriverSnapshot && entry.ChangeSequence != 0) ||
			(mode != SourceDriverSnapshot && entry.ChangeSequence == 0) {
			return 0, fmt.Errorf("%w: invalid source driver stage entry", ErrInvalidObject)
		}
		if entry.Object != nil {
			if entry.Object.Key != entry.Key {
				return 0, fmt.Errorf("%w: source driver entry key differs", ErrInvalidObject)
			}
			if err := validateSourceObject(*entry.Object); err != nil {
				return 0, err
			}
		}
		if index > 0 && compareSourceDriverEntry(mode, page.Entries[index-1], entry) >= 0 {
			return 0, fmt.Errorf("%w: source driver entries are not tuple ordered", ErrInvalidObject)
		}
	}
	encoded, err := json.Marshal(page)
	if err != nil {
		return 0, err
	}
	if len(encoded) > SourcePublicationStagePageByteLimit {
		return 0, fmt.Errorf("%w: source driver stage page exceeds byte limit", ErrInvalidObject)
	}
	return len(encoded), nil
}

func compareSourceDriverEntry(mode SourceDriverMode, left, right SourceDriverStageEntry) int {
	if order := cmp.Compare(left.Tenant, right.Tenant); order != 0 {
		return order
	}
	if order := cmp.Compare(left.Generation, right.Generation); order != 0 {
		return order
	}
	if mode != SourceDriverSnapshot {
		if order := cmp.Compare(left.ChangeSequence, right.ChangeSequence); order != 0 {
			return order
		}
	}
	return cmp.Compare(left.Key, right.Key)
}

func zeroSourceDriverMutation(identity SourceDriverStageIdentity) bool {
	return identity.Mutation == (MutationID{}) && identity.MutationTenant == "" &&
		identity.MutationGeneration == 0 && identity.MutationResult == "" &&
		identity.MutationRequestDigest == ([sha256.Size]byte{}) &&
		identity.MutationReceiptDigest == ([sha256.Size]byte{}) && identity.Claim == (MutationClaim{})
}

func validSourceDriverToken(token string) bool {
	return token != "" && len(token) <= SourceDriverTokenMaxBytes && utf8.ValidString(token) && !strings.ContainsRune(token, 0)
}

func sourceDriverTokenDigest(token string) [sha256.Size]byte {
	return sha256.Sum256([]byte("fusekit.source-driver-token.v1\x00" + token))
}
