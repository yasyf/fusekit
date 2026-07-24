package causal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strings"
)

// ActivationChangeID identifies one exact tenant serving-pointer activation.
type ActivationChangeID [16]byte

// PresentationID identifies one exact presentation instance.
type PresentationID string

// Backend identifies the presentation implementation that observes an activation.
type Backend uint8

const (
	// BackendMount is the native mount presentation.
	BackendMount Backend = iota + 1
	// BackendFileProvider is the File Provider presentation.
	BackendFileProvider
)

// SourceCause retains one immutable source publication covered by an activation.
type SourceCause struct {
	PublicationID      OperationID
	ChangeID           ChangeID
	SourceRevision     Revision
	OperationID        OperationID
	Cause              Cause
	AffectedKeysDigest [sha256.Size]byte
}

// ActivationEvent is the exact tenant-presentation work item consumed by convergence.
type ActivationEvent struct {
	ActivationChangeID ActivationChangeID
	TenantID           TenantID
	TenantGeneration   Generation
	ActivationRevision Revision
	PresentationID     PresentationID
	Backend            Backend
	CatalogHead        CatalogRevision
	HeadDigest         [sha256.Size]byte
	Causes             []SourceCause
}

const activationChangeIdentityDomain = "fusekit.tenant-activation-change.v1\x00"

// DeriveActivationChangeID returns the deterministic identity for one tenant activation.
func DeriveActivationChangeID(
	tenant TenantID,
	generation Generation,
	activationRevision uint64,
	newHeadDigest [sha256.Size]byte,
	publicationIDs []OperationID,
) (ActivationChangeID, error) {
	if tenant == "" || generation == 0 || activationRevision == 0 ||
		newHeadDigest == ([sha256.Size]byte{}) || len(publicationIDs) == 0 {
		return ActivationChangeID{}, errors.New("causal: incomplete tenant activation identity")
	}
	seen := make(map[OperationID]struct{}, len(publicationIDs))
	for _, publication := range publicationIDs {
		if publication == (OperationID{}) {
			return ActivationChangeID{}, errors.New("causal: empty activation cause publication")
		}
		if _, duplicate := seen[publication]; duplicate {
			return ActivationChangeID{}, errors.New("causal: duplicate activation cause publication")
		}
		seen[publication] = struct{}{}
	}

	digest := sha256.New()
	_, _ = digest.Write([]byte(activationChangeIdentityDomain))
	writeActivationIdentityField(digest, []byte(tenant))
	writeActivationIdentityUint64(digest, uint64(generation))
	writeActivationIdentityUint64(digest, activationRevision)
	writeActivationIdentityField(digest, newHeadDigest[:])
	writeActivationIdentityUint64(digest, uint64(len(publicationIDs)))
	for _, publication := range publicationIDs {
		writeActivationIdentityField(digest, publication[:])
	}
	var identity ActivationChangeID
	copy(identity[:], digest.Sum(nil))
	return identity, nil
}

// ValidateActivationEvent verifies the complete hard-cut convergence contract.
func ValidateActivationEvent(event ActivationEvent) error {
	if event.ActivationChangeID == (ActivationChangeID{}) || event.TenantID == "" ||
		event.TenantGeneration == 0 || event.ActivationRevision == 0 ||
		event.PresentationID == "" || strings.IndexByte(string(event.PresentationID), 0) >= 0 ||
		(event.Backend != BackendMount && event.Backend != BackendFileProvider) ||
		event.CatalogHead == 0 || event.HeadDigest == ([sha256.Size]byte{}) || len(event.Causes) == 0 {
		return errors.New("causal: incomplete activation event")
	}
	publicationIDs := make([]OperationID, len(event.Causes))
	for index, cause := range event.Causes {
		if cause.PublicationID == (OperationID{}) || cause.ChangeID == (ChangeID{}) ||
			cause.SourceRevision == 0 || cause.OperationID == (OperationID{}) ||
			cause.AffectedKeysDigest == ([sha256.Size]byte{}) || !validSourceCause(cause.Cause) {
			return errors.New("causal: incomplete activation source cause")
		}
		if index > 0 {
			previous := event.Causes[index-1]
			if cause.SourceRevision < previous.SourceRevision ||
				(cause.SourceRevision == previous.SourceRevision &&
					bytes.Compare(cause.PublicationID[:], previous.PublicationID[:]) <= 0) {
				return errors.New("causal: activation source causes are not strictly ordered")
			}
		}
		publicationIDs[index] = cause.PublicationID
	}
	expected, err := DeriveActivationChangeID(
		event.TenantID,
		event.TenantGeneration,
		uint64(event.ActivationRevision),
		event.HeadDigest,
		publicationIDs,
	)
	if err != nil {
		return err
	}
	if expected != event.ActivationChangeID {
		return errors.New("causal: activation change identity does not match event")
	}
	return nil
}

func validSourceCause(cause Cause) bool {
	switch cause {
	case CauseProviderMutation, CauseDaemonWrite, CauseExternalUnattributed, CauseBootstrap:
		return true
	default:
		return false
	}
}

type activationIdentityWriter interface {
	Write([]byte) (int, error)
}

func writeActivationIdentityField(digest activationIdentityWriter, value []byte) {
	writeActivationIdentityUint64(digest, uint64(len(value)))
	_, _ = digest.Write(value)
}

func writeActivationIdentityUint64(digest activationIdentityWriter, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}
