package catalog

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ObjectID is an opaque, catalog-issued filesystem object identity.
type ObjectID [16]byte

// MutationID identifies one revision-fenced idempotent catalog mutation.
type MutationID [32]byte

// MutationRequestDigest is the exact canonical request digest bound into a
// MutationID.
type MutationRequestDigest [sha256.Size]byte

// MutationBinding is the complete authenticated identity of one catalog
// mutation.
type MutationBinding struct {
	Tenant        TenantID
	Target        Revision
	Issuer        string
	RequestDigest MutationRequestDigest
}

// MutationOwnerID identifies one tenant-runtime process incarnation.
type MutationOwnerID [16]byte

// BrokerCommandAttemptID identifies one durable signed-broker delivery
// attempt.
type BrokerCommandAttemptID [16]byte

// HandleID identifies one durable open-handle pin.
type HandleID [16]byte

// HandleOwnerID identifies one live catalog process generation.
type HandleOwnerID [16]byte

// MutationPinID identifies one durable replay pin held by a native mutable
// handle.
type MutationPinID [16]byte

// InterestID identifies one durable materialization interest.
type InterestID [16]byte

// StageID identifies one durable staged-content ownership record.
type StageID [16]byte

// ContentHash is the exact SHA-256 digest of one content revision.
type ContentHash [32]byte

// Revision is a tenant-local monotone revision.
type Revision uint64

// TenantID identifies one isolated filesystem namespace.
type TenantID string

// NewObjectID returns a cryptographically random object identity.
func NewObjectID() (ObjectID, error) {
	var id ObjectID
	if _, err := rand.Read(id[:]); err != nil {
		return ObjectID{}, fmt.Errorf("catalog: generate object id: %w", err)
	}
	return id, nil
}

// ParseObjectID parses the canonical hexadecimal ObjectID encoding.
func ParseObjectID(value string) (ObjectID, error) {
	var id ObjectID
	if err := decodeID(value, id[:]); err != nil {
		return ObjectID{}, fmt.Errorf("catalog: parse object id: %w", err)
	}
	return id, nil
}

// String returns the canonical hexadecimal ObjectID encoding.
func (id ObjectID) String() string { return hex.EncodeToString(id[:]) }

// MarshalText implements encoding.TextMarshaler.
func (id ObjectID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *ObjectID) UnmarshalText(text []byte) error {
	parsed, err := ParseObjectID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// NewMutationOwnerID returns a cryptographically random mutation-owner identity.
func NewMutationOwnerID() (MutationOwnerID, error) {
	var id MutationOwnerID
	if _, err := rand.Read(id[:]); err != nil {
		return MutationOwnerID{}, fmt.Errorf("catalog: generate mutation owner id: %w", err)
	}
	return id, nil
}

// NewBrokerCommandAttemptID returns a cryptographically random broker-attempt
// identity.
func NewBrokerCommandAttemptID() (BrokerCommandAttemptID, error) {
	var id BrokerCommandAttemptID
	if _, err := rand.Read(id[:]); err != nil {
		return BrokerCommandAttemptID{}, fmt.Errorf("catalog: generate broker command attempt id: %w", err)
	}
	return id, nil
}

// NewHandleID returns a cryptographically random handle identity.
func NewHandleID() (HandleID, error) {
	var id HandleID
	if _, err := rand.Read(id[:]); err != nil {
		return HandleID{}, fmt.Errorf("catalog: generate handle id: %w", err)
	}
	return id, nil
}

// NewMutationPinID returns a cryptographically random mutation replay-pin
// identity.
func NewMutationPinID() (MutationPinID, error) {
	var id MutationPinID
	if _, err := rand.Read(id[:]); err != nil {
		return MutationPinID{}, fmt.Errorf("catalog: generate mutation pin id: %w", err)
	}
	return id, nil
}

func newHandleOwnerID() (HandleOwnerID, error) {
	var id HandleOwnerID
	if _, err := rand.Read(id[:]); err != nil {
		return HandleOwnerID{}, fmt.Errorf("catalog: generate handle owner id: %w", err)
	}
	return id, nil
}

// String returns the canonical hexadecimal HandleID encoding.
func (id HandleID) String() string { return hex.EncodeToString(id[:]) }

// NewInterestID returns a cryptographically random materialization-interest identity.
func NewInterestID() (InterestID, error) {
	var id InterestID
	if _, err := rand.Read(id[:]); err != nil {
		return InterestID{}, fmt.Errorf("catalog: generate interest id: %w", err)
	}
	return id, nil
}

// NewStageID returns a cryptographically random staged-content identity.
func NewStageID() (StageID, error) {
	var id StageID
	if _, err := rand.Read(id[:]); err != nil {
		return StageID{}, fmt.Errorf("catalog: generate stage id: %w", err)
	}
	return id, nil
}

// String returns the canonical hexadecimal InterestID encoding.
func (id InterestID) String() string { return hex.EncodeToString(id[:]) }

// ParseMutationID parses the canonical hexadecimal MutationID encoding.
func ParseMutationID(value string) (MutationID, error) {
	var id MutationID
	if err := decodeID(value, id[:]); err != nil {
		return MutationID{}, fmt.Errorf("catalog: parse mutation id: %w", err)
	}
	return id, nil
}

// String returns the canonical hexadecimal MutationID encoding.
func (id MutationID) String() string { return hex.EncodeToString(id[:]) }

// MarshalText implements encoding.TextMarshaler.
func (id MutationID) MarshalText() ([]byte, error) { return []byte(id.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *MutationID) UnmarshalText(text []byte) error {
	parsed, err := ParseMutationID(string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

const mutationIdentityDomain = "fusekit.catalog.mutation.v1\x00"

// TargetRevision returns the immutable tenant revision fenced by the
// mutation.
func (id MutationID) TargetRevision() Revision {
	return Revision(binary.BigEndian.Uint64(id[:8]))
}

func deriveMutationID(binding MutationBinding) (MutationID, error) {
	if err := validateMutationBinding(binding); err != nil {
		return MutationID{}, err
	}
	material := make([]byte, 0,
		len(mutationIdentityDomain)+4+len(binding.Tenant)+8+4+len(binding.Issuer)+len(binding.RequestDigest))
	material = append(material, mutationIdentityDomain...)
	material = binary.BigEndian.AppendUint32(material, uint32(len(binding.Tenant)))
	material = append(material, binding.Tenant...)
	material = binary.BigEndian.AppendUint64(material, uint64(binding.Target))
	material = binary.BigEndian.AppendUint32(material, uint32(len(binding.Issuer)))
	material = append(material, binding.Issuer...)
	material = append(material, binding.RequestDigest[:]...)
	digest := sha256.Sum256(material)
	var id MutationID
	binary.BigEndian.PutUint64(id[:8], uint64(binding.Target))
	copy(id[8:], digest[:len(id)-8])
	return id, nil
}

func validateMutationID(id MutationID, binding MutationBinding) error {
	expected, err := deriveMutationID(binding)
	if err != nil {
		return err
	}
	if id != expected {
		return ErrMutationConflict
	}
	return nil
}

func validateMutationBinding(binding MutationBinding) error {
	if _, err := NewTenantID(string(binding.Tenant)); err != nil {
		return err
	}
	if binding.Target == 0 {
		return fmt.Errorf("%w: mutation target revision is zero", ErrInvalidObject)
	}
	if binding.Issuer == "" || strings.IndexByte(binding.Issuer, 0) >= 0 {
		return fmt.Errorf("%w: mutation issuer is empty or contains NUL", ErrInvalidObject)
	}
	if uint64(len(binding.Tenant)) > uint64(^uint32(0)) ||
		uint64(len(binding.Issuer)) > uint64(^uint32(0)) {
		return fmt.Errorf("%w: mutation identity field exceeds canonical encoding", ErrInvalidObject)
	}
	return nil
}

// NewTenantID validates and returns a tenant identity.
func NewTenantID(value string) (TenantID, error) {
	if value == "" {
		return "", errors.New("catalog: tenant id is empty")
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", errors.New("catalog: tenant id contains NUL")
	}
	return TenantID(value), nil
}

func decodeID(value string, dst []byte) error {
	if len(value) != hex.EncodedLen(len(dst)) {
		return fmt.Errorf("want %d hexadecimal bytes, got %d", hex.EncodedLen(len(dst)), len(value))
	}
	if _, err := hex.Decode(dst, []byte(value)); err != nil {
		return err
	}
	return nil
}
