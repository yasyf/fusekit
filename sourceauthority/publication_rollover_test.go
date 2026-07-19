package sourceauthority

import (
	"context"
	"errors"
	"testing"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

type publicationRecoveryStore struct {
	Store
	commits     int
	quarantines int
}

func (s *publicationRecoveryStore) CommitSourcePublicationStage(
	_ context.Context,
	ref catalog.SourcePublicationStageRef,
) (catalog.SourcePublicationStageResult, error) {
	s.commits++
	return catalog.SourcePublicationStageResult{
		Authority: ref.Authority, FleetOwner: ref.FleetOwner,
		FleetGeneration: ref.FleetGeneration, DriverID: ref.DriverID,
		DeclarationDigest: ref.DeclarationDigest, Operation: ref.Operation,
		First: ref.Revision, Last: ref.Revision, Count: 1, Digest: ref.Digest,
	}, nil
}

func (s *publicationRecoveryStore) QuarantineSourceObserver(
	context.Context,
	causal.SourceAuthorityID,
	string,
) error {
	s.quarantines++
	return nil
}

func (s *publicationRecoveryStore) PendingSourceDriverStage(
	context.Context,
	causal.SourceAuthorityID,
) (*catalog.SourceDriverStageState, error) {
	return nil, nil
}

func (s *publicationRecoveryStore) SourceWatermark(
	context.Context,
	causal.SourceAuthorityID,
) (causal.Revision, error) {
	return 1, nil
}

func TestPendingPublicationStageRequiresExactFleetIdentity(t *testing.T) {
	currentDeclaration := [32]byte{2}
	runtime := &Runtime{
		authority:         "authority",
		fleetOwner:        "owner",
		fleetGeneration:   2,
		driverID:          "driver",
		declarationDigest: currentDeclaration,
	}
	base := catalog.SourcePublicationStageRef{
		Authority:         "authority",
		FleetOwner:        "owner",
		FleetGeneration:   2,
		DriverID:          "driver",
		DeclarationDigest: currentDeclaration,
		Operation:         causal.OperationID{1},
	}
	if err := runtime.validatePendingPublicationStage(base); err != nil {
		t.Fatalf("current exact identity: %v", err)
	}
	prior := base
	prior.FleetGeneration = 1
	prior.DeclarationDigest = [32]byte{1}
	if err := runtime.validatePendingPublicationStage(prior); err != nil {
		t.Fatalf("prior exact identity must remain recoverable: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*catalog.SourcePublicationStageRef)
	}{
		{name: "authority", mutate: func(ref *catalog.SourcePublicationStageRef) { ref.Authority = "other" }},
		{name: "owner", mutate: func(ref *catalog.SourcePublicationStageRef) { ref.FleetOwner = "other" }},
		{name: "zero-generation", mutate: func(ref *catalog.SourcePublicationStageRef) { ref.FleetGeneration = 0 }},
		{name: "driver", mutate: func(ref *catalog.SourcePublicationStageRef) { ref.DriverID = "other" }},
		{name: "invalid-driver", mutate: func(ref *catalog.SourcePublicationStageRef) { ref.DriverID = "" }},
		{name: "zero-declaration", mutate: func(ref *catalog.SourcePublicationStageRef) { ref.DeclarationDigest = [32]byte{} }},
		{name: "future-generation", mutate: func(ref *catalog.SourcePublicationStageRef) { ref.FleetGeneration = 3 }},
		{name: "current-declaration", mutate: func(ref *catalog.SourcePublicationStageRef) { ref.DeclarationDigest = [32]byte{9} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ref := base
			test.mutate(&ref)
			if err := runtime.validatePendingPublicationStage(ref); !errors.Is(err, ErrQuarantined) {
				t.Fatalf("validation = %v, want quarantine", err)
			}
		})
	}
}

func TestPriorGenerationPublicationIsRecoveredUnderItsPersistedFence(t *testing.T) {
	store := &publicationRecoveryStore{}
	runtime := &Runtime{
		catalog:           store,
		authority:         "authority",
		fleetOwner:        "owner",
		fleetGeneration:   2,
		driverID:          "driver",
		declarationDigest: [32]byte{2},
	}
	ref := catalog.SourcePublicationStageRef{
		Authority:         "authority",
		FleetOwner:        "owner",
		FleetGeneration:   1,
		DriverID:          "old-driver",
		DeclarationDigest: [32]byte{1},
		Operation:         causal.OperationID{1},
		Revision:          1,
	}
	if err := runtime.recoverPendingPublicationStage(t.Context(), ref); err != nil {
		t.Fatalf("recover prior generation publication: %v", err)
	}
	if store.commits != 1 {
		t.Fatalf("commit calls = %d, want one", store.commits)
	}

	ref.FleetGeneration = 3
	if err := runtime.recoverPendingPublicationStage(t.Context(), ref); !errors.Is(err, ErrQuarantined) {
		t.Fatalf("future generation recovery = %v, want quarantine", err)
	}
	if store.commits != 1 {
		t.Fatalf("future identity reached commit: %d calls", store.commits)
	}
	if store.quarantines != 1 {
		t.Fatalf("future identity quarantine calls = %d, want one", store.quarantines)
	}
}

func TestCommittedPublicationResultRetainsExactStageIdentity(t *testing.T) {
	ref := catalog.SourcePublicationStageRef{
		Authority: "authority", FleetOwner: "owner", FleetGeneration: 2,
		DriverID: "driver", DeclarationDigest: [32]byte{2},
		Operation: causal.OperationID{1}, Revision: 4, Digest: [32]byte{3},
	}
	result := catalog.SourcePublicationStageResult{
		Authority: ref.Authority, FleetOwner: ref.FleetOwner,
		FleetGeneration: ref.FleetGeneration, DriverID: ref.DriverID,
		DeclarationDigest: ref.DeclarationDigest, Operation: ref.Operation,
		First: 3, Last: ref.Revision, Count: 2, Digest: ref.Digest,
	}
	if err := validateCommittedPublicationResult(ref, result); err != nil {
		t.Fatalf("exact result: %v", err)
	}
	result.DriverID = "other"
	if err := validateCommittedPublicationResult(ref, result); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("changed result identity = %v, want integrity error", err)
	}
	result.DriverID = ref.DriverID
	result.Count = 3
	if err := validateCommittedPublicationResult(ref, result); !errors.Is(err, catalog.ErrIntegrity) {
		t.Fatalf("discontinuous result = %v, want integrity error", err)
	}
}
