// Package sourcedriverruntime reconciles one semantic source authority across
// one immutable generation-fenced FuseKit tenant declaration.
package sourcedriverruntime

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourcedriver"
)

const defaultPageLimit = 128

const sourceSettlementTimeout = 5 * time.Second

// ErrClosed means the semantic driver lane no longer admits work.
var ErrClosed = errors.New("source driver runtime: closed")

var errProgressPending = errors.New("source driver runtime: durable progress pending")

// ErrRetainedMutationLiability means a durable source mutation cannot yet be
// settled, so the authority remains closed to newer work.
var ErrRetainedMutationLiability = errors.New("source driver runtime: retained mutation liability")

type retainedMutationPendingError struct {
	Mutation catalog.MutationID
}

func (e *retainedMutationPendingError) Error() string {
	return fmt.Sprintf("%v: mutation %x awaits exact retry", ErrRetainedMutationLiability, e.Mutation)
}

func (e *retainedMutationPendingError) Unwrap() error { return ErrRetainedMutationLiability }

// Store is the catalog-owned durable state used by one semantic driver lane.
type Store interface {
	SourceDriverCheckpoint(context.Context, causal.SourceAuthorityID) (catalog.SourceDriverCheckpoint, error)
	SourceDriverTargetCheckpoint(context.Context, causal.SourceAuthorityID, catalog.TenantID, catalog.Generation) (catalog.SourceDriverTargetCheckpoint, error)
	PreparedMutation(context.Context, catalog.TenantID, catalog.MutationID) (catalog.PreparedMutation, error)
	ValidateSourceDriverTargetEpoch(context.Context, causal.SourceAuthorityID, uint64) error
	SourceDriverTargetEpoch(context.Context, causal.SourceAuthorityID) (uint64, error)
	PendingSourceDriverStage(context.Context, causal.SourceAuthorityID) (*catalog.SourceDriverStageState, error)
	RequireSourceDriverSnapshot(context.Context, causal.SourceAuthorityID, string, catalog.SourceDriverSnapshotReason) (catalog.SourceDriverCheckpoint, error)
	RebindSourceDriverCheckpoint(context.Context, catalog.SourceDriverCheckpointRebind) (catalog.SourceDriverCheckpoint, error)
	BeginSourceDriverStage(context.Context, catalog.SourceDriverStageIdentity) error
	PrepareSourceDriverTargetDeclarationBatch(context.Context, catalog.SourceDriverStageIdentity) (catalog.SourceDriverTargetDeclarationState, error)
	SourceDriverStageTargets(context.Context, causal.SourceAuthorityID, causal.OperationID, catalog.TenantID, int) ([]catalog.SourceDriverTarget, error)
	ReserveSourceDriverMutation(context.Context, catalog.SourceDriverMutationReservationRequest) (catalog.SourceDriverMutationReservation, error)
	SourceDriverMutationReservation(context.Context, catalog.MutationID) (catalog.SourceDriverMutationReservation, error)
	ActiveSourceDriverMutationReservation(context.Context, causal.SourceAuthorityID) (*catalog.SourceDriverMutationReservation, error)
	PrepareSourceDriverMutationReservationBatch(context.Context, catalog.MutationID, catalog.MutationClaim) (catalog.SourceDriverMutationReservation, error)
	BindSourceDriverMutationRequest(context.Context, catalog.MutationID, catalog.MutationClaim, [sha256.Size]byte) (catalog.SourceDriverMutationReservation, error)
	ReleaseUnboundSourceDriverMutationReservation(context.Context, catalog.MutationID, catalog.MutationClaim, uint64) error
	SourceDriverMutationReservationTargets(context.Context, catalog.MutationID, catalog.TenantID, int) (catalog.SourceDriverTargetPage, error)
	RecordSourceDriverMutationReceipt(context.Context, catalog.MutationID, catalog.MutationClaim, catalog.SourceDriverMutationReceiptProof) (catalog.SourceDriverMutationReservation, error)
	AppendSourceDriverStage(context.Context, catalog.SourceDriverStageIdentity, catalog.SourceDriverStagePage) (catalog.SourceDriverStageState, error)
	PrepareSourceDriverPublicationBatch(context.Context, catalog.SourceDriverStageIdentity) (catalog.SourceDriverPreparationState, error)
	CommitSourceDriverStage(context.Context, catalog.SourceDriverStageState) (catalog.SourceDriverStageResult, error)
	CommitSourceDriverMutation(context.Context, catalog.SourceDriverStageState) (catalog.SourceDriverStageResult, error)
	AbortSourceDriverStage(context.Context, catalog.SourceDriverStageIdentity) error
	PendingSourceDriverCommittedReceipt(context.Context, causal.SourceAuthorityID) (*catalog.SourceDriverCommittedReceipt, error)
	CommittedSourceDriverMutation(context.Context, causal.SourceAuthorityID, catalog.MutationID) (*catalog.SourceDriverCommittedReceipt, error)
	AcknowledgeSourceDriverCommittedReceipt(context.Context, catalog.SourceDriverStageResult) error
	ForgetSourceDriverCommittedReceipt(context.Context, catalog.SourceDriverStageResult) error
	StageOwnedContent(context.Context, contentstream.Source) (catalog.ContentRef, error)
	ReleaseUnclaimedContent(context.Context, []catalog.ContentRef) error
}

// Config binds one source authority to one immutable multi-tenant declaration.
type Config struct {
	Store               Store
	Driver              sourcedriver.Driver
	Authority           causal.SourceAuthorityID
	FleetOwner          catalog.SourceAuthorityFleetOwnerID
	AuthorityGeneration causal.Generation
	DeclarationDigest   [sha256.Size]byte
	Targets             []catalog.SourceDriverTarget
	PageLimit           int

	targetsDigest [sha256.Size]byte
}

// ReconcileResult is one durable driver checkpoint outcome.
type ReconcileResult struct {
	Changed    bool
	Checkpoint catalog.SourceDriverCheckpoint
	Stage      *catalog.SourceDriverStageResult
}

// MutationResult is one source receipt and its atomic catalog settlement.
type MutationResult struct {
	Receipt sourcedriver.MutationReceipt
	Stage   catalog.SourceDriverStageResult
}

// Runtime owns one serialized semantic lane for one authority.
type Runtime struct {
	config Config
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	refreshes   chan refreshRequest
	mutations   chan mutationRequest
	settlements chan settlementRequest

	closeOnce sync.Once

	retainedMutation *catalog.MutationID
}

type refreshRequest struct {
	result chan refreshResponse
}

type refreshResponse struct {
	result ReconcileResult
	err    error
}

type mutationRequest struct {
	prepared catalog.PreparedMutation
	content  contentstream.Source
	result   chan mutationResponse
}

type mutationResponse struct {
	result MutationResult
	err    error
}

type settlementRequest struct {
	mutation *catalog.MutationID
	result   chan settlementResponse
}

type settlementResponse struct {
	result MutationResult
	err    error
}

func validateConfig(config Config) error {
	if config.Store == nil || config.Driver == nil {
		return errors.New("source driver runtime: store and driver are required")
	}
	if err := causal.ValidateSourceAuthorityID(config.Authority); err != nil {
		return fmt.Errorf("source driver runtime: authority: %w", err)
	}
	if config.FleetOwner == "" || config.AuthorityGeneration == 0 ||
		config.DeclarationDigest == ([sha256.Size]byte{}) {
		return errors.New("source driver runtime: generation fence is incomplete")
	}
	targetsDigest, err := validateTargets(config.Targets)
	if err != nil {
		return err
	}
	if config.targetsDigest != ([sha256.Size]byte{}) && config.targetsDigest != targetsDigest {
		return errors.New("source driver runtime: configured target digest differs")
	}
	if config.PageLimit == 0 {
		config.PageLimit = defaultPageLimit
	}
	if config.PageLimit < 1 || config.PageLimit > sourcedriver.MaxPageItems {
		return errors.New("source driver runtime: page limit is invalid")
	}
	return nil
}

func normalizeConfig(config Config) Config {
	if config.PageLimit == 0 {
		config.PageLimit = defaultPageLimit
	}
	config.Targets = append([]catalog.SourceDriverTarget(nil), config.Targets...)
	config.targetsDigest, _ = validateTargets(config.Targets)
	return config
}

func validateTargets(targets []catalog.SourceDriverTarget) ([sha256.Size]byte, error) {
	catalogDigest, err := catalog.SourceDriverTargetsDigest(targets)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("source driver runtime: targets: %w", err)
	}
	driverTargets := make([]sourcedriver.TargetDeclaration, len(targets))
	for index, target := range targets {
		if index > 0 && targets[index-1].Tenant == target.Tenant {
			return [sha256.Size]byte{}, errors.New("source driver runtime: target tenant is duplicated")
		}
		driverTargets[index] = sourcedriver.TargetDeclaration{
			Tenant: target.Tenant, Generation: causal.Generation(target.Generation),
		}
	}
	driverDigest, err := sourcedriver.TargetsDigest(driverTargets)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	if catalogDigest != driverDigest {
		return [sha256.Size]byte{}, errors.New("source driver runtime: catalog and driver target digests differ")
	}
	return catalogDigest, nil
}

func (c Config) targetSetRef(targetEpoch uint64) (sourcedriver.TargetSetRef, error) {
	return sourcedriver.NewTargetSetRefForDigest(
		c.Authority, c.AuthorityGeneration, targetEpoch, c.DeclarationDigest,
		uint64(len(c.Targets)), c.targetsDigest,
	)
}

func (c Config) target(tenant catalog.TenantID) (catalog.SourceDriverTarget, bool) {
	index, found := slices.BinarySearchFunc(c.Targets, tenant, func(target catalog.SourceDriverTarget, tenant catalog.TenantID) int {
		if target.Tenant < tenant {
			return -1
		}
		if target.Tenant > tenant {
			return 1
		}
		return 0
	})
	if !found {
		return catalog.SourceDriverTarget{}, false
	}
	return c.Targets[index], true
}

func settleUnused(source contentstream.Source, cause error) error {
	if source == nil {
		return nil
	}
	settleErr := source.Settle(cause)
	waitCtx, cancel := context.WithTimeout(context.Background(), sourceSettlementTimeout)
	defer cancel()
	waitErr := source.Wait(waitCtx)
	return errors.Join(settleErr, waitErr)
}

func abortUnused(source contentstream.Source, cause error) error {
	if source == nil {
		return nil
	}
	_ = source.Settle(cause)
	waitCtx, cancel := context.WithTimeout(context.Background(), sourceSettlementTimeout)
	defer cancel()
	return source.Wait(waitCtx)
}

func settleTransferred(source contentstream.Source, cause error) error {
	if source == nil {
		return nil
	}
	settleErr := source.Settle(cause)
	waitCtx, cancel := context.WithTimeout(context.Background(), sourceSettlementTimeout)
	defer cancel()
	waitErr := source.Wait(waitCtx)
	if cause != nil {
		settleErr = nil
		if errors.Is(waitErr, cause) {
			waitErr = nil
		}
	}
	return errors.Join(settleErr, waitErr)
}
