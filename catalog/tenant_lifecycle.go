package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/yasyf/fusekit/causal"
)

var (
	// ErrTenantLifecycleStale means an operation no longer names current intent or activation.
	ErrTenantLifecycleStale = errors.New("catalog: tenant lifecycle operation is stale")
	// ErrTenantLifecycleRetryDeferred means a quarantined transition cannot yet retry.
	ErrTenantLifecycleRetryDeferred = errors.New("catalog: tenant lifecycle retry is deferred")
	// ErrTenantMutationConflict means an operation ID was reused for different request bytes.
	ErrTenantMutationConflict = errors.New("catalog: tenant mutation operation conflict")
	// ErrTenantTargetingChanged means durable delivery eligibility changed before activation.
	ErrTenantTargetingChanged = errors.New("catalog: tenant presentation targeting changed")
	// ErrTenantPreparationOwnershipConflict means an unproven holder still owns staged work.
	ErrTenantPreparationOwnershipConflict = errors.New("catalog: tenant preparation ownership conflict")
)

// TenantIntentRevision fences one exact Present or Absent intent.
type TenantIntentRevision uint64

// TenantActivationRevision fences one exact active-generation pointer.
type TenantActivationRevision uint64

// TenantApplicationVersion fences one shared staged-view row.
type TenantApplicationVersion uint64

// PresentationMaterializationVersion fences one backend receipt row.
type PresentationMaterializationVersion uint64

// TenantIntentKind is the desired lifecycle state.
type TenantIntentKind uint8

const (
	TenantIntentPresent TenantIntentKind = iota + 1
	TenantIntentAbsent
)

// TenantMutationKind identifies a domain-separated journal request.
type TenantMutationKind uint8

const (
	TenantMutationDeclareIntent TenantMutationKind = iota + 1
	TenantMutationStageApplication
	TenantMutationRecordPresentation
	TenantMutationActivate
	TenantMutationRetirePresentation
	TenantMutationRetireApplication
	TenantMutationClearActivation
	TenantMutationRecoverOwnerLost
)

// TenantMutationState is the durable mutation journal phase.
type TenantMutationState uint8

const (
	TenantMutationPending TenantMutationState = iota + 1
	TenantMutationCommitted
	TenantMutationFailed
)

// TenantApplicationPhase is one generation's shared staged-view phase.
type TenantApplicationPhase uint8

const (
	TenantApplicationPending TenantApplicationPhase = iota + 1
	TenantApplicationApplying
	TenantApplicationStaged
	TenantApplicationRetiring
	TenantApplicationRetired
	TenantApplicationQuarantined
)

// PresentationMaterializationPhase is one generation/backend receipt phase.
type PresentationMaterializationPhase uint8

const (
	PresentationMaterializationPending PresentationMaterializationPhase = iota + 1
	PresentationMaterializationApplying
	PresentationMaterializationApplied
	PresentationMaterializationActive
	PresentationMaterializationRetiring
	PresentationMaterializationRetired
	PresentationMaterializationQuarantined
)

// TenantBackend is a concrete presentation backend.
type TenantBackend uint8

const (
	TenantBackendNative TenantBackend = iota + 1
	TenantBackendBroker
)

// TenantBackendSet is an exact compact set of required backends.
type TenantBackendSet uint8

const (
	tenantBackendNativeBit TenantBackendSet = 1 << iota
	tenantBackendBrokerBit
)

// Has reports whether backend belongs to the set.
func (s TenantBackendSet) Has(backend TenantBackend) bool {
	switch backend {
	case TenantBackendNative:
		return s&tenantBackendNativeBit != 0
	case TenantBackendBroker:
		return s&tenantBackendBrokerBit != 0
	default:
		return false
	}
}

// Backends returns the exact canonical backend order.
func (s TenantBackendSet) Backends() []TenantBackend {
	result := make([]TenantBackend, 0, 2)
	for _, backend := range []TenantBackend{TenantBackendNative, TenantBackendBroker} {
		if s.Has(backend) {
			result = append(result, backend)
		}
	}
	return result
}

func (s TenantBackendSet) valid() bool {
	return s > 0 && s <= tenantBackendNativeBit|tenantBackendBrokerBit
}

// TenantMutation binds one exact journaled lifecycle operation.
type TenantMutation struct {
	OperationID             TenantOperationID
	HolderRuntimeGeneration string
	OwnerID                 string
	ExpectedIntentRevision  TenantIntentRevision
}

// TenantFailure is one structured durable lifecycle failure.
type TenantFailure struct {
	Code            string
	Detail          string
	RetryEligibleAt *time.Time
}

// RetryEligible reports whether retry is permitted at now.
func (f TenantFailure) RetryEligible(now time.Time) bool {
	return f.RetryEligibleAt != nil && !now.Before(*f.RetryEligibleAt)
}

// TenantGeneration is one immutable canonical tenant generation.
type TenantGeneration struct {
	Definition       TenantProvision
	CanonicalSpec    []byte
	SpecHash         [sha256.Size]byte
	RequiredBackends TenantBackendSet
}

// TenantIntent is the one mutable desired pointer for a tenant.
type TenantIntent struct {
	Tenant           TenantID
	Kind             TenantIntentKind
	TargetGeneration Generation
	Revision         TenantIntentRevision
	CurrentOperation TenantOperationID
	Version          uint64
}

// TenantActivation is the sole serving pointer.
type TenantActivation struct {
	Tenant               TenantID
	ActiveGeneration     Generation
	ActiveView           StagedViewID
	ActiveCatalogHead    Revision
	ActiveSourceRevision causal.Revision
	Revision             TenantActivationRevision
	Retiring             bool
	Version              uint64
	LastOperation        TenantOperationID
}

// Active reports whether the pointer names a serving generation.
func (a TenantActivation) Active() bool { return a.ActiveGeneration != 0 }

// TenantApplication is one generation-bound immutable staged view and its shared lifecycle.
type TenantApplication struct {
	Tenant                   TenantID
	Generation               Generation
	IntentRevision           TenantIntentRevision
	TransitionIntentRevision TenantIntentRevision
	ContentSourceID          string
	Phase                    TenantApplicationPhase
	SourceAuthority          causal.SourceAuthorityID
	SourcePublication        causal.OperationID
	ViewID                   StagedViewID
	ViewDigest               [sha256.Size]byte
	StagedCatalogHead        Revision
	StagedHeadDigest         [sha256.Size]byte
	StagedSourceRevision     causal.Revision
	PublicationDigest        [sha256.Size]byte
	HolderRuntimeGeneration  string
	OperationID              TenantOperationID
	Version                  TenantApplicationVersion
	Failure                  *TenantFailure
}

// PresentationMaterialization is one exact backend receipt.
type PresentationMaterialization struct {
	Tenant                   TenantID
	Generation               Generation
	Backend                  TenantBackend
	IntentRevision           TenantIntentRevision
	TransitionIntentRevision TenantIntentRevision
	Phase                    PresentationMaterializationPhase
	ViewID                   StagedViewID
	ViewDigest               [sha256.Size]byte
	BackendGeneration        string
	ObservedRevision         Revision
	HolderRuntimeGeneration  string
	OperationID              TenantOperationID
	Version                  PresentationMaterializationVersion
	Failure                  *TenantFailure
}

// StagedViewLease is the only capability passed to sealed presentation preparation.
type StagedViewLease struct {
	Tenant                  TenantID
	Generation              Generation
	IntentRevision          TenantIntentRevision
	ViewID                  StagedViewID
	ViewDigest              [sha256.Size]byte
	CatalogHead             Revision
	HeadDigest              [sha256.Size]byte
	SourceRevision          causal.Revision
	HolderRuntimeGeneration string
	OperationID             TenantOperationID
	SourceAuthority         causal.SourceAuthorityID
	SourcePublication       causal.OperationID
}

// TenantLifecycleState is the complete intent, activation, and generation state.
type TenantLifecycleState struct {
	OwnerID       string
	Intent        TenantIntent
	Target        *TenantGeneration
	Active        *TenantGeneration
	Activation    TenantActivation
	Applications  []TenantApplication
	Presentations []PresentationMaterialization
}

// Ready reports exact desired activation with no outstanding retirement.
func (s TenantLifecycleState) Ready() bool {
	if s.Intent.Kind == TenantIntentAbsent {
		return !s.Activation.Active() && !s.hasOutstandingRetirement()
	}
	if s.Target == nil || !s.Activation.Active() || s.Activation.Retiring ||
		s.Activation.ActiveGeneration != s.Target.Definition.Generation {
		return false
	}
	application, ok := s.application(s.Target.Definition.Generation)
	if !ok || application.IntentRevision != s.Intent.Revision ||
		application.Phase != TenantApplicationStaged || application.ViewID != s.Activation.ActiveView ||
		application.StagedCatalogHead != s.Activation.ActiveCatalogHead {
		return false
	}
	required := s.Target.RequiredBackends.Backends()
	targetRows := make([]PresentationMaterialization, 0, len(required))
	for _, row := range s.Presentations {
		if row.Generation == s.Target.Definition.Generation {
			targetRows = append(targetRows, row)
		}
	}
	if len(targetRows) != len(required) {
		return false
	}
	for _, backend := range required {
		var row PresentationMaterialization
		found := false
		for _, candidate := range targetRows {
			if candidate.Backend == backend {
				row, found = candidate, true
				break
			}
		}
		if !found || row.Phase != PresentationMaterializationActive ||
			row.IntentRevision != s.Intent.Revision ||
			row.ViewID != application.ViewID || row.ViewDigest != application.ViewDigest ||
			row.ObservedRevision != application.StagedCatalogHead {
			return false
		}
	}
	return !s.hasOutstandingRetirement()
}

func (s TenantLifecycleState) application(generation Generation) (TenantApplication, bool) {
	for _, row := range s.Applications {
		if row.Generation == generation {
			return row, true
		}
	}
	return TenantApplication{}, false
}

func (s TenantLifecycleState) hasOutstandingRetirement() bool {
	for _, row := range s.Applications {
		if row.Phase == TenantApplicationRetiring || row.Phase == TenantApplicationQuarantined {
			return true
		}
	}
	for _, row := range s.Presentations {
		if row.Phase == PresentationMaterializationRetiring || row.Phase == PresentationMaterializationQuarantined {
			return true
		}
	}
	return false
}

// StageApplicationRequest binds one source-publication target to an opaque staged view.
type StageApplicationRequest struct {
	Mutation          TenantMutation
	Tenant            TenantID
	Generation        Generation
	Authority         causal.SourceAuthorityID
	Publication       causal.OperationID
	PublicationDigest [sha256.Size]byte
}

// PresentationReceipt binds one sealed backend observation to a staged view.
type PresentationReceipt struct {
	Mutation          TenantMutation
	Lease             StagedViewLease
	Backend           TenantBackend
	BackendGeneration string
	ObservedRevision  Revision
}

// ActivateTenantRequest is the complete expected-old-pointer activation CAS.
type ActivateTenantRequest struct {
	Mutation                   TenantMutation
	Tenant                     TenantID
	Generation                 Generation
	ViewID                     StagedViewID
	ViewDigest                 [sha256.Size]byte
	ExpectedActivationRevision TenantActivationRevision
	ExpectedActiveGeneration   Generation
	CausePublications          []causal.OperationID
	ExpectedTargetingRevision  uint64
}

// TenantPresentationTarget is one exact signal-capable presentation selected for delivery.
type TenantPresentationTarget struct {
	PresentationID      causal.PresentationID
	Backend             causal.Backend
	ProviderFingerprint [sha256.Size]byte
	SignalTargets       []FileProviderSignalTarget
	SignalTargetCount   uint64
	SignalTargetDigest  [sha256.Size]byte
	SignalsCoalesced    bool
}

// TenantActivationResult is one committed serving-pointer flip and its causal identity.
type TenantActivationResult struct {
	State    TenantLifecycleState
	ChangeID causal.ActivationChangeID
	Causes   []causal.SourceCause
	Targets  []TenantPresentationTarget
}

// TenantPreparationRecoveryRequest is the already-authenticated semantic
// owner fence for one holder-start recovery transaction.
type TenantPreparationRecoveryRequest struct {
	CurrentHolderRuntimeGeneration  string
	SettledHolderRuntimeGenerations []string
}

// TenantPreparationRecoveryResult reports the exact abandoned attempts reset.
type TenantPreparationRecoveryResult struct {
	ResetApplications  uint64
	ResetPresentations uint64
}

// RetirementRequest settles one old generation component.
type RetirementRequest struct {
	Mutation   TenantMutation
	Tenant     TenantID
	Generation Generation
	Backend    TenantBackend
}

// EnsureTenantNamespaceRequest fences creation of one generation's retained catalog namespace.
type EnsureTenantNamespaceRequest struct {
	OwnerID        string
	Tenant         TenantID
	Generation     Generation
	IntentRevision TenantIntentRevision
}

// TenantNamespace is retained catalog identity prepared before source staging.
type TenantNamespace struct {
	Root            ObjectID
	CatalogRevision Revision
}

type canonicalTenantSpec struct {
	OwnerID                 string `json:"owner_id"`
	Tenant                  string `json:"tenant"`
	MountPresentationRoot   string `json:"mount_presentation_root"`
	BackingRoot             string `json:"backing_root"`
	ContentSourceID         string `json:"content_source_id"`
	Access                  uint8  `json:"access"`
	CasePolicy              uint8  `json:"case_policy"`
	Presentations           uint8  `json:"presentations"`
	FileProviderInstanceID  string `json:"file_provider_instance_id"`
	FileProviderDisplayName string `json:"file_provider_display_name"`
	Generation              uint64 `json:"generation"`
}

func canonicalizeTenantProvision(definition TenantProvision) (TenantGeneration, error) {
	definition.Root = ObjectID{}
	if err := validateTenantProvision(definition); err != nil {
		return TenantGeneration{}, err
	}
	backends := TenantBackendSet(0)
	if definition.Presentations.Has(PresentationMount) {
		backends |= tenantBackendNativeBit
	}
	if definition.Presentations.Has(PresentationFileProvider) {
		backends |= tenantBackendBrokerBit
	}
	if !backends.valid() {
		return TenantGeneration{}, ErrInvalidObject
	}
	encoded, err := json.Marshal(canonicalTenantSpec{
		OwnerID: definition.OwnerID, Tenant: string(definition.Tenant),
		MountPresentationRoot: definition.Mount.PresentationRoot, BackingRoot: definition.BackingRoot,
		ContentSourceID: definition.ContentSourceID, Access: uint8(definition.Access),
		CasePolicy: uint8(definition.CasePolicy), Presentations: uint8(definition.Presentations),
		FileProviderInstanceID:  definition.FileProvider.PresentationInstanceID,
		FileProviderDisplayName: definition.FileProvider.DisplayName, Generation: uint64(definition.Generation),
	})
	if err != nil {
		return TenantGeneration{}, fmt.Errorf("catalog: encode canonical tenant generation: %w", err)
	}
	return TenantGeneration{
		Definition: definition, CanonicalSpec: encoded, SpecHash: sha256.Sum256(encoded), RequiredBackends: backends,
	}, nil
}

// EnsureTenantNamespace creates retained catalog identity without changing the serving pointer.
func (c *Catalog) EnsureTenantNamespace(ctx context.Context, request EnsureTenantNamespaceRequest) (TenantNamespace, error) {
	state, found, err := loadTenantLifecycle(ctx, c.readDB, request.Tenant)
	if err != nil {
		return TenantNamespace{}, err
	}
	if !found || state.OwnerID != request.OwnerID || state.Intent.Kind != TenantIntentPresent ||
		state.Intent.TargetGeneration != request.Generation || state.Intent.Revision != request.IntentRevision ||
		state.Target == nil {
		return TenantNamespace{}, ErrTenantLifecycleStale
	}
	root, policy, presentations, retained, err := retainedTenant(ctx, c.readDB, request.Tenant)
	if err != nil {
		return TenantNamespace{}, err
	}
	if retained {
		if policy != state.Target.Definition.CasePolicy || presentations != state.Target.Definition.Presentations {
			return TenantNamespace{}, ErrTenantProvisionConflict
		}
	} else {
		created, err := c.CreateTenant(ctx, request.Tenant, state.Target.Definition.CasePolicy, state.Target.Definition.Presentations)
		if err != nil {
			return TenantNamespace{}, err
		}
		root = created.ID
	}
	current, found, err := loadTenantLifecycle(ctx, c.readDB, request.Tenant)
	if err != nil {
		return TenantNamespace{}, err
	}
	if !found || current.OwnerID != request.OwnerID || current.Intent.Kind != TenantIntentPresent ||
		current.Intent.TargetGeneration != request.Generation || current.Intent.Revision != request.IntentRevision {
		return TenantNamespace{}, ErrTenantLifecycleStale
	}
	head, _, err := revisionState(ctx, c.readDB, request.Tenant)
	if err != nil {
		return TenantNamespace{}, err
	}
	return TenantNamespace{Root: root, CatalogRevision: head}, nil
}

// SetTenantPresent durably points intent at one immutable generation without changing serving state.
func (c *Catalog) SetTenantPresent(
	ctx context.Context,
	mutation TenantMutation,
	definition TenantProvision,
) (TenantLifecycleState, error) {
	generation, err := canonicalizeTenantProvision(definition)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if err := validateTenantMutation(mutation, definition.OwnerID, definition.Tenant); err != nil {
		return TenantLifecycleState{}, err
	}
	requestHash, err := tenantMutationRequestHash(TenantMutationDeclareIntent, mutation, definition.Tenant, struct {
		Generation Generation
		SpecHash   [sha256.Size]byte
	}{definition.Generation, generation.SpecHash})
	if err != nil {
		return TenantLifecycleState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantLifecycleState{}, fmt.Errorf("catalog: begin present tenant intent: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if replay, found, err := beginTenantMutation(ctx, tx, TenantMutationDeclareIntent, mutation,
		definition.Tenant, requestHash); err != nil {
		return TenantLifecycleState{}, err
	} else if found {
		var state TenantLifecycleState
		if err := json.Unmarshal(replay, &state); err != nil {
			return TenantLifecycleState{}, ErrIntegrity
		}
		if err := tx.Commit(); err != nil {
			return TenantLifecycleState{}, err
		}
		return state, nil
	}
	current, found, err := loadTenantLifecycle(ctx, tx, definition.Tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if found && current.Intent.Revision != mutation.ExpectedIntentRevision ||
		!found && mutation.ExpectedIntentRevision != 0 {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	if active, err := activeTenantMutationReservation(ctx, tx, definition.Tenant, mutation.OperationID); err != nil {
		return TenantLifecycleState{}, err
	} else if active {
		return TenantLifecycleState{}, ErrMutationActive
	}
	if active, err := activeSourceDriverMutationReservation(ctx, tx, definition.Tenant); err != nil {
		return TenantLifecycleState{}, err
	} else if active {
		return TenantLifecycleState{}, ErrMutationActive
	}
	stored, generationFound, err := loadTenantGeneration(ctx, tx, definition.Tenant, definition.Generation)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if generationFound {
		if stored.SpecHash != generation.SpecHash || !bytes.Equal(stored.CanonicalSpec, generation.CanonicalSpec) {
			return TenantLifecycleState{}, ErrTenantProvisionConflict
		}
	} else {
		var maximum sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT MAX(generation) FROM tenant_generations WHERE tenant_id = ?`,
			string(definition.Tenant)).Scan(&maximum); err != nil {
			return TenantLifecycleState{}, err
		}
		if maximum.Valid && definition.Generation <= Generation(maximum.Int64) {
			return TenantLifecycleState{}, ErrTenantProvisionConflict
		}
		if err := insertTenantGeneration(ctx, tx, generation); err != nil {
			return TenantLifecycleState{}, err
		}
	}
	if found && current.Intent.Kind == TenantIntentPresent &&
		current.Intent.TargetGeneration == definition.Generation {
		if err := commitTenantMutation(ctx, tx, mutation.OperationID, current); err != nil {
			return TenantLifecycleState{}, err
		}
		if err := tx.Commit(); err != nil {
			return TenantLifecycleState{}, err
		}
		return current, nil
	}
	var previousAuthority string
	if found && current.Intent.Kind == TenantIntentPresent {
		if current.Target == nil {
			return TenantLifecycleState{}, ErrIntegrity
		}
		previousAuthority = current.Target.Definition.ContentSourceID
	}
	revision := mutation.ExpectedIntentRevision + 1
	if found {
		result, err := tx.ExecContext(ctx, `
UPDATE tenant_intents SET
    state = ?, target_generation = ?, intent_revision = ?, current_operation_id = ?, version = version + 1
WHERE tenant_id = ? AND intent_revision = ?`, uint8(TenantIntentPresent), uint64(definition.Generation),
			uint64(revision), mutation.OperationID[:], string(definition.Tenant), uint64(mutation.ExpectedIntentRevision))
		if err != nil {
			return TenantLifecycleState{}, mapConstraint(err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return TenantLifecycleState{}, ErrTenantLifecycleStale
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE tenant_targeting_heads SET revision = revision + 1 WHERE tenant_id = ?`,
			string(definition.Tenant)); err != nil {
			return TenantLifecycleState{}, mapConstraint(err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_intents(
    tenant_id, state, target_generation, intent_revision, current_operation_id, version
) VALUES (?, ?, ?, 1, ?, 1)`, string(definition.Tenant), uint8(TenantIntentPresent),
			uint64(definition.Generation), mutation.OperationID[:]); err != nil {
			return TenantLifecycleState{}, mapConstraint(err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_activations(
    tenant_id, active_generation, active_view_id, active_catalog_head, source_revision,
    activation_revision, retiring, version, last_operation_id
) VALUES (?, NULL, NULL, 0, 0, 0, 0, 1, ?)`, string(definition.Tenant), mutation.OperationID[:]); err != nil {
			return TenantLifecycleState{}, mapConstraint(err)
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_targeting_heads(tenant_id, revision) VALUES (?, 1)`,
			string(definition.Tenant)); err != nil {
			return TenantLifecycleState{}, mapConstraint(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_applications(
    tenant_id, generation, intent_revision, transition_intent_revision, content_source_id, phase, source_authority,
    source_publication_id, staged_view_id, staged_view_digest, staged_catalog_head,
    staged_head_digest, staged_source_revision, publication_digest, holder_runtime_generation, operation_id, version
) VALUES (?, ?, ?, ?, ?, ?, '', NULL, NULL, NULL, 0, NULL, 0, NULL, '', x'', 1)`,
		string(definition.Tenant), uint64(definition.Generation), uint64(revision), uint64(revision), definition.ContentSourceID,
		uint8(TenantApplicationPending)); err != nil {
		return TenantLifecycleState{}, mapConstraint(err)
	}
	for _, backend := range generation.RequiredBackends.Backends() {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO presentation_materializations(
    tenant_id, generation, backend, intent_revision, transition_intent_revision, phase, staged_view_id, staged_view_digest,
    backend_generation, observed_revision, holder_runtime_generation, operation_id, version
) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, '', 0, '', x'', 1)`, string(definition.Tenant),
			uint64(definition.Generation), uint8(backend), uint64(revision), uint64(revision),
			uint8(PresentationMaterializationPending)); err != nil {
			return TenantLifecycleState{}, mapConstraint(err)
		}
	}
	if err := transitionSourceDriverTargetEpoch(ctx, tx, previousAuthority, definition.ContentSourceID); err != nil {
		return TenantLifecycleState{}, err
	}
	state, _, err := loadTenantLifecycle(ctx, tx, definition.Tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if err := commitTenantMutation(ctx, tx, mutation.OperationID, state); err != nil {
		return TenantLifecycleState{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantLifecycleState{}, fmt.Errorf("catalog: commit present tenant intent: %w", err)
	}
	return state, nil
}

// SetTenantAbsent records Absent while retaining the active pointer until cleanup settles.
func (c *Catalog) SetTenantAbsent(
	ctx context.Context,
	mutation TenantMutation,
	tenant TenantID,
) (TenantLifecycleState, error) {
	if err := validateTenantMutation(mutation, mutation.OwnerID, tenant); err != nil {
		return TenantLifecycleState{}, err
	}
	requestHash, err := tenantMutationRequestHash(TenantMutationDeclareIntent, mutation, tenant, struct {
		Kind TenantIntentKind
	}{TenantIntentAbsent})
	if err != nil {
		return TenantLifecycleState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if replay, found, err := beginTenantMutation(ctx, tx, TenantMutationDeclareIntent, mutation, tenant, requestHash); err != nil {
		return TenantLifecycleState{}, err
	} else if found {
		var state TenantLifecycleState
		if err := json.Unmarshal(replay, &state); err != nil {
			return TenantLifecycleState{}, ErrIntegrity
		}
		if err := tx.Commit(); err != nil {
			return TenantLifecycleState{}, err
		}
		return state, nil
	}
	current, found, err := loadTenantLifecycle(ctx, tx, tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if !found || current.Intent.Revision != mutation.ExpectedIntentRevision {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	owner, err := lifecycleOwner(current)
	if err != nil || owner != mutation.OwnerID {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	if active, err := activeTenantMutationReservation(ctx, tx, tenant, mutation.OperationID); err != nil {
		return TenantLifecycleState{}, err
	} else if active {
		return TenantLifecycleState{}, ErrMutationActive
	}
	if active, err := activeSourceDriverMutationReservation(ctx, tx, tenant); err != nil {
		return TenantLifecycleState{}, err
	} else if active {
		return TenantLifecycleState{}, ErrMutationActive
	}
	if current.Intent.Kind == TenantIntentAbsent {
		if err := commitTenantMutation(ctx, tx, mutation.OperationID, current); err != nil {
			return TenantLifecycleState{}, err
		}
		if err := tx.Commit(); err != nil {
			return TenantLifecycleState{}, err
		}
		return current, nil
	}
	if current.Target == nil {
		return TenantLifecycleState{}, ErrIntegrity
	}
	revision := current.Intent.Revision + 1
	result, err := tx.ExecContext(ctx, `
UPDATE tenant_intents SET
    state = ?, target_generation = NULL, intent_revision = ?, current_operation_id = ?, version = version + 1
WHERE tenant_id = ? AND intent_revision = ?`, uint8(TenantIntentAbsent), uint64(revision),
		mutation.OperationID[:], string(tenant), uint64(current.Intent.Revision))
	if err != nil {
		return TenantLifecycleState{}, mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	if _, err := tx.ExecContext(ctx, `
UPDATE tenant_targeting_heads SET revision = revision + 1 WHERE tenant_id = ?`, string(tenant)); err != nil {
		return TenantLifecycleState{}, mapConstraint(err)
	}
	if current.Activation.Active() {
		if _, err := tx.ExecContext(ctx, `
UPDATE tenant_activations SET retiring = 1, version = version + 1, last_operation_id = ?
WHERE tenant_id = ? AND active_generation = ? AND activation_revision = ?`, mutation.OperationID[:],
			string(tenant), uint64(current.Activation.ActiveGeneration), uint64(current.Activation.Revision)); err != nil {
			return TenantLifecycleState{}, err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE tenant_applications SET phase = ?, transition_intent_revision = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND phase = ?`, uint8(TenantApplicationRetiring), uint64(revision),
			string(tenant), uint64(current.Activation.ActiveGeneration), uint8(TenantApplicationStaged)); err != nil {
			return TenantLifecycleState{}, err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE presentation_materializations SET phase = ?, transition_intent_revision = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND phase = ?`, uint8(PresentationMaterializationRetiring),
			uint64(revision), string(tenant), uint64(current.Activation.ActiveGeneration),
			uint8(PresentationMaterializationActive)); err != nil {
			return TenantLifecycleState{}, err
		}
	}
	if err := retireSourceDriverTargetEpoch(ctx, tx, current.Target.Definition.ContentSourceID); err != nil {
		return TenantLifecycleState{}, err
	}
	state, _, err := loadTenantLifecycle(ctx, tx, tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if err := commitTenantMutation(ctx, tx, mutation.OperationID, state); err != nil {
		return TenantLifecycleState{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantLifecycleState{}, err
	}
	return state, nil
}

// StageApplication binds one prepared source target to a new immutable staged view.
func (c *Catalog) StageApplication(
	ctx context.Context,
	request StageApplicationRequest,
) (StagedViewLease, TenantLifecycleState, error) {
	if request.Tenant == "" || request.Generation == 0 || request.Authority == "" ||
		request.Publication == (causal.OperationID{}) || request.PublicationDigest == ([sha256.Size]byte{}) {
		return StagedViewLease{}, TenantLifecycleState{}, ErrInvalidObject
	}
	if err := validateTenantMutation(request.Mutation, request.Mutation.OwnerID, request.Tenant); err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	requestHash, err := tenantMutationRequestHash(TenantMutationStageApplication, request.Mutation, request.Tenant, struct {
		Generation        Generation
		Authority         causal.SourceAuthorityID
		Publication       causal.OperationID
		PublicationDigest [sha256.Size]byte
	}{request.Generation, request.Authority, request.Publication, request.PublicationDigest})
	if err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if replay, found, err := beginTenantMutation(ctx, tx, TenantMutationStageApplication, request.Mutation,
		request.Tenant, requestHash); err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	} else if found {
		var state TenantLifecycleState
		if err := json.Unmarshal(replay, &state); err != nil {
			return StagedViewLease{}, TenantLifecycleState{}, ErrIntegrity
		}
		lease, err := stagedViewLease(state, request.Generation)
		if err != nil {
			return StagedViewLease{}, TenantLifecycleState{}, err
		}
		if err := tx.Commit(); err != nil {
			return StagedViewLease{}, TenantLifecycleState{}, err
		}
		return lease, state, nil
	}
	state, found, err := loadTenantLifecycle(ctx, tx, request.Tenant)
	if err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	if !found || state.OwnerID != request.Mutation.OwnerID || state.Intent.Kind != TenantIntentPresent ||
		state.Intent.Revision != request.Mutation.ExpectedIntentRevision ||
		state.Intent.TargetGeneration != request.Generation || state.Target == nil ||
		state.Target.Definition.ContentSourceID != string(request.Authority) {
		return StagedViewLease{}, TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	if active, err := activeTenantMutationReservation(ctx, tx, request.Tenant, request.Mutation.OperationID); err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	} else if active {
		return StagedViewLease{}, TenantLifecycleState{}, ErrMutationActive
	}
	application, ok := state.application(request.Generation)
	if !ok || application.IntentRevision != state.Intent.Revision || application.Phase != TenantApplicationPending {
		return StagedViewLease{}, TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	var sourceRevision, catalogHead uint64
	var stageDigest, headDigest []byte
	var publicationPrepared, targetPrepared uint8
	if err := tx.QueryRowContext(ctx, `
SELECT publication.source_revision, publication.stage_digest, publication.prepared,
       target.catalog_head, target.catalog_fingerprint, target.prepared
FROM source_driver_publications publication
JOIN source_driver_publication_targets target
  ON target.source_authority = publication.source_authority
 AND target.publication_id = publication.publication_id
WHERE publication.source_authority = ? AND publication.publication_id = ?
  AND target.tenant = ? AND target.generation = ?`, string(request.Authority), request.Publication[:],
		string(request.Tenant), uint64(request.Generation)).Scan(
		&sourceRevision, &stageDigest, &publicationPrepared, &catalogHead, &headDigest, &targetPrepared,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return StagedViewLease{}, TenantLifecycleState{}, ErrTenantLifecycleStale
		}
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	if publicationPrepared == 0 || targetPrepared == 0 || catalogHead == 0 ||
		!bytes.Equal(stageDigest, request.PublicationDigest[:]) || len(headDigest) != sha256.Size {
		return StagedViewLease{}, TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	viewID, err := NewStagedViewID()
	if err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	var exactHeadDigest [sha256.Size]byte
	copy(exactHeadDigest[:], headDigest)
	viewDigest := stagedViewDigest(request.Tenant, request.Generation, state.Intent.Revision,
		Revision(catalogHead), causal.Revision(sourceRevision), exactHeadDigest, request.PublicationDigest,
		request.Authority, request.Publication, request.Mutation.HolderRuntimeGeneration,
		request.Mutation.OperationID, viewID)
	result, err := tx.ExecContext(ctx, `
UPDATE tenant_applications SET
    phase = ?, source_authority = ?, source_publication_id = ?, staged_view_id = ?,
    staged_view_digest = ?, staged_catalog_head = ?, staged_head_digest = ?, staged_source_revision = ?,
    publication_digest = ?, holder_runtime_generation = ?, operation_id = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND intent_revision = ? AND phase = ?`,
		uint8(TenantApplicationStaged), string(request.Authority), request.Publication[:], viewID[:],
		viewDigest[:], catalogHead, exactHeadDigest[:], sourceRevision, request.PublicationDigest[:],
		request.Mutation.HolderRuntimeGeneration, request.Mutation.OperationID[:], string(request.Tenant),
		uint64(request.Generation), uint64(state.Intent.Revision), uint8(TenantApplicationPending))
	if err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return StagedViewLease{}, TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	state, _, err = loadTenantLifecycle(ctx, tx, request.Tenant)
	if err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	if err := commitTenantMutation(ctx, tx, request.Mutation.OperationID, state); err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	lease, err := stagedViewLease(state, request.Generation)
	if err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	if err := tx.Commit(); err != nil {
		return StagedViewLease{}, TenantLifecycleState{}, err
	}
	return lease, state, nil
}

func stagedViewDigest(
	tenant TenantID,
	generation Generation,
	intent TenantIntentRevision,
	head Revision,
	source causal.Revision,
	headDigest [sha256.Size]byte,
	publicationDigest [sha256.Size]byte,
	authority causal.SourceAuthorityID,
	publication causal.OperationID,
	holder string,
	operation TenantOperationID,
	view StagedViewID,
) [sha256.Size]byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("github.com/yasyf/fusekit/catalog/staged-view/v1\x00"))
	writeTenantLifecycleDigestField(digest, []byte(tenant))
	writeTenantLifecycleDigestUint64(digest, uint64(generation))
	writeTenantLifecycleDigestUint64(digest, uint64(intent))
	writeTenantLifecycleDigestUint64(digest, uint64(head))
	writeTenantLifecycleDigestUint64(digest, uint64(source))
	writeTenantLifecycleDigestField(digest, headDigest[:])
	writeTenantLifecycleDigestField(digest, publicationDigest[:])
	writeTenantLifecycleDigestField(digest, []byte(authority))
	writeTenantLifecycleDigestField(digest, publication[:])
	writeTenantLifecycleDigestField(digest, []byte(holder))
	writeTenantLifecycleDigestField(digest, operation[:])
	writeTenantLifecycleDigestField(digest, view[:])
	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func writeTenantLifecycleDigestField(digest interface{ Write([]byte) (int, error) }, value []byte) {
	writeTenantLifecycleDigestUint64(digest, uint64(len(value)))
	_, _ = digest.Write(value)
}

func writeTenantLifecycleDigestUint64(digest interface{ Write([]byte) (int, error) }, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}

func stagedViewLease(state TenantLifecycleState, generation Generation) (StagedViewLease, error) {
	application, found := state.application(generation)
	if !found || application.Phase != TenantApplicationStaged || application.ViewID == (StagedViewID{}) {
		return StagedViewLease{}, ErrTenantLifecycleStale
	}
	return StagedViewLease{
		Tenant: application.Tenant, Generation: application.Generation, IntentRevision: application.IntentRevision,
		ViewID: application.ViewID, ViewDigest: application.ViewDigest, CatalogHead: application.StagedCatalogHead,
		HeadDigest: application.StagedHeadDigest, SourceRevision: application.StagedSourceRevision,
		HolderRuntimeGeneration: application.HolderRuntimeGeneration, OperationID: application.OperationID,
		SourceAuthority: application.SourceAuthority, SourcePublication: application.SourcePublication,
	}, nil
}

// RecordPresentation records one exact backend receipt for an immutable staged view.
func (c *Catalog) RecordPresentation(
	ctx context.Context,
	receipt PresentationReceipt,
) (TenantLifecycleState, error) {
	if receipt.BackendGeneration == "" || receipt.ObservedRevision == 0 ||
		receipt.ObservedRevision != receipt.Lease.CatalogHead || receipt.Lease.Tenant == "" ||
		receipt.Lease.Generation == 0 || receipt.Lease.ViewID == (StagedViewID{}) ||
		receipt.Lease.ViewDigest == ([sha256.Size]byte{}) || receipt.Lease.HeadDigest == ([sha256.Size]byte{}) {
		return TenantLifecycleState{}, ErrInvalidObject
	}
	if err := validateTenantMutation(receipt.Mutation, receipt.Mutation.OwnerID, receipt.Lease.Tenant); err != nil {
		return TenantLifecycleState{}, err
	}
	requestHash, err := tenantMutationRequestHash(TenantMutationRecordPresentation, receipt.Mutation,
		receipt.Lease.Tenant, struct {
			Lease             StagedViewLease
			Backend           TenantBackend
			BackendGeneration string
			ObservedRevision  Revision
		}{receipt.Lease, receipt.Backend, receipt.BackendGeneration, receipt.ObservedRevision})
	if err != nil {
		return TenantLifecycleState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if replay, found, err := beginTenantMutation(ctx, tx, TenantMutationRecordPresentation,
		receipt.Mutation, receipt.Lease.Tenant, requestHash); err != nil {
		return TenantLifecycleState{}, err
	} else if found {
		var state TenantLifecycleState
		if err := json.Unmarshal(replay, &state); err != nil {
			return TenantLifecycleState{}, ErrIntegrity
		}
		if err := tx.Commit(); err != nil {
			return TenantLifecycleState{}, err
		}
		return state, nil
	}
	state, found, err := loadTenantLifecycle(ctx, tx, receipt.Lease.Tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if !found || state.OwnerID != receipt.Mutation.OwnerID || state.Intent.Kind != TenantIntentPresent ||
		state.Intent.Revision != receipt.Mutation.ExpectedIntentRevision ||
		state.Intent.Revision != receipt.Lease.IntentRevision ||
		state.Intent.TargetGeneration != receipt.Lease.Generation || state.Target == nil ||
		!state.Target.RequiredBackends.Has(receipt.Backend) {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	if active, err := activeTenantMutationReservation(ctx, tx, receipt.Lease.Tenant,
		receipt.Mutation.OperationID); err != nil {
		return TenantLifecycleState{}, err
	} else if active {
		return TenantLifecycleState{}, ErrMutationActive
	}
	application, ok := state.application(receipt.Lease.Generation)
	if !ok || application.Phase != TenantApplicationStaged || application.IntentRevision != receipt.Lease.IntentRevision ||
		application.ViewID != receipt.Lease.ViewID || application.ViewDigest != receipt.Lease.ViewDigest ||
		application.StagedCatalogHead != receipt.Lease.CatalogHead ||
		application.StagedHeadDigest != receipt.Lease.HeadDigest ||
		application.StagedSourceRevision != receipt.Lease.SourceRevision ||
		application.SourceAuthority != receipt.Lease.SourceAuthority ||
		application.SourcePublication != receipt.Lease.SourcePublication ||
		application.HolderRuntimeGeneration != receipt.Lease.HolderRuntimeGeneration ||
		application.OperationID != receipt.Lease.OperationID {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	result, err := tx.ExecContext(ctx, `
UPDATE presentation_materializations SET
    phase = ?, staged_view_id = ?, staged_view_digest = ?, backend_generation = ?,
    observed_revision = ?, holder_runtime_generation = ?, operation_id = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND backend = ? AND intent_revision = ? AND phase = ?`,
		uint8(PresentationMaterializationApplied), receipt.Lease.ViewID[:], receipt.Lease.ViewDigest[:],
		receipt.BackendGeneration, uint64(receipt.ObservedRevision), receipt.Mutation.HolderRuntimeGeneration,
		receipt.Mutation.OperationID[:], string(receipt.Lease.Tenant), uint64(receipt.Lease.Generation),
		uint8(receipt.Backend), uint64(receipt.Lease.IntentRevision), uint8(PresentationMaterializationPending))
	if err != nil {
		return TenantLifecycleState{}, mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	state, _, err = loadTenantLifecycle(ctx, tx, receipt.Lease.Tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if err := commitTenantMutation(ctx, tx, receipt.Mutation.OperationID, state); err != nil {
		return TenantLifecycleState{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantLifecycleState{}, err
	}
	return state, nil
}

// ActivateTenant atomically flips the sole serving pointer after exact presentation receipts.
func (c *Catalog) ActivateTenant(
	ctx context.Context,
	request ActivateTenantRequest,
) (TenantActivationResult, error) {
	if request.Tenant == "" || request.Generation == 0 || request.ViewID == (StagedViewID{}) ||
		request.ViewDigest == ([sha256.Size]byte{}) || len(request.CausePublications) == 0 {
		return TenantActivationResult{}, ErrInvalidObject
	}
	if err := validateTenantMutation(request.Mutation, request.Mutation.OwnerID, request.Tenant); err != nil {
		return TenantActivationResult{}, err
	}
	canonicalPublications, err := canonicalActivationPublications(request.CausePublications)
	if err != nil {
		return TenantActivationResult{}, err
	}
	requestHash, err := tenantMutationRequestHash(TenantMutationActivate, request.Mutation, request.Tenant, struct {
		Generation                 Generation
		ViewID                     StagedViewID
		ViewDigest                 [sha256.Size]byte
		ExpectedActivationRevision TenantActivationRevision
		ExpectedActiveGeneration   Generation
		CausePublications          []causal.OperationID
		ExpectedTargetingRevision  uint64
	}{request.Generation, request.ViewID, request.ViewDigest, request.ExpectedActivationRevision,
		request.ExpectedActiveGeneration, canonicalPublications, request.ExpectedTargetingRevision})
	if err != nil {
		return TenantActivationResult{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantActivationResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if replay, found, err := beginTenantMutation(ctx, tx, TenantMutationActivate, request.Mutation,
		request.Tenant, requestHash); err != nil {
		return TenantActivationResult{}, err
	} else if found {
		var state TenantLifecycleState
		if err := json.Unmarshal(replay, &state); err != nil {
			return TenantActivationResult{}, ErrIntegrity
		}
		result, err := loadTenantActivationResult(ctx, tx, state)
		if err != nil {
			return TenantActivationResult{}, err
		}
		if err := tx.Commit(); err != nil {
			return TenantActivationResult{}, err
		}
		return result, nil
	}
	state, found, err := loadTenantLifecycle(ctx, tx, request.Tenant)
	if err != nil {
		return TenantActivationResult{}, err
	}
	if !found || state.OwnerID != request.Mutation.OwnerID || state.Intent.Kind != TenantIntentPresent ||
		state.Intent.Revision != request.Mutation.ExpectedIntentRevision ||
		state.Intent.TargetGeneration != request.Generation || state.Target == nil ||
		state.Activation.Revision != request.ExpectedActivationRevision ||
		state.Activation.ActiveGeneration != request.ExpectedActiveGeneration {
		return TenantActivationResult{}, ErrTenantLifecycleStale
	}
	if active, err := activeTenantMutationReservation(ctx, tx, request.Tenant,
		request.Mutation.OperationID); err != nil {
		return TenantActivationResult{}, err
	} else if active {
		return TenantActivationResult{}, ErrMutationActive
	}
	application, ok := state.application(request.Generation)
	if !ok || application.Phase != TenantApplicationStaged || application.IntentRevision != state.Intent.Revision ||
		application.ViewID != request.ViewID || application.ViewDigest != request.ViewDigest ||
		application.StagedHeadDigest == ([sha256.Size]byte{}) {
		return TenantActivationResult{}, ErrTenantLifecycleStale
	}
	if err := validateActivationPresentations(state, *state.Target, application); err != nil {
		return TenantActivationResult{}, err
	}
	causes, causeAuthorities, err := loadActivationCauses(ctx, tx, request.Tenant, request.Generation,
		application, canonicalPublications)
	if err != nil {
		return TenantActivationResult{}, err
	}
	var targetingRevision uint64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM tenant_targeting_heads WHERE tenant_id = ?`,
		string(request.Tenant)).Scan(&targetingRevision); err != nil {
		return TenantActivationResult{}, err
	}
	if targetingRevision != request.ExpectedTargetingRevision {
		return TenantActivationResult{}, ErrTenantTargetingChanged
	}
	targets, err := deriveTenantPresentationTargets(ctx, tx, state, application, causes)
	if err != nil {
		return TenantActivationResult{}, err
	}
	nextRevision := state.Activation.Revision + 1
	publicationIDs := make([]causal.OperationID, len(causes))
	for index := range causes {
		publicationIDs[index] = causes[index].PublicationID
	}
	changeID, err := causal.DeriveActivationChangeID(
		causal.TenantID(request.Tenant), causal.Generation(request.Generation), uint64(nextRevision),
		application.StagedHeadDigest, publicationIDs,
	)
	if err != nil {
		return TenantActivationResult{}, err
	}
	activation, err := tx.ExecContext(ctx, `
UPDATE tenant_activations SET
    active_generation = ?, active_view_id = ?, active_catalog_head = ?, source_revision = ?,
    activation_revision = ?, retiring = 0, version = version + 1, last_operation_id = ?
WHERE tenant_id = ? AND activation_revision = ?
  AND ((? = 0 AND active_generation IS NULL) OR active_generation = ?)`, uint64(request.Generation),
		request.ViewID[:], uint64(application.StagedCatalogHead), uint64(application.StagedSourceRevision),
		uint64(nextRevision), request.Mutation.OperationID[:], string(request.Tenant),
		uint64(request.ExpectedActivationRevision), uint64(request.ExpectedActiveGeneration),
		uint64(request.ExpectedActiveGeneration))
	if err != nil {
		return TenantActivationResult{}, mapConstraint(err)
	}
	if changed, _ := activation.RowsAffected(); changed != 1 {
		return TenantActivationResult{}, ErrTenantLifecycleStale
	}
	if request.ExpectedActiveGeneration != 0 && request.ExpectedActiveGeneration != request.Generation {
		if _, err := tx.ExecContext(ctx, `
UPDATE tenant_applications SET phase = ?, transition_intent_revision = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND phase = ?`, uint8(TenantApplicationRetiring),
			uint64(state.Intent.Revision), string(request.Tenant), uint64(request.ExpectedActiveGeneration),
			uint8(TenantApplicationStaged)); err != nil {
			return TenantActivationResult{}, err
		}
		if _, err := tx.ExecContext(ctx, `
UPDATE presentation_materializations SET phase = ?, transition_intent_revision = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND phase = ?`, uint8(PresentationMaterializationRetiring),
			uint64(state.Intent.Revision), string(request.Tenant), uint64(request.ExpectedActiveGeneration),
			uint8(PresentationMaterializationActive)); err != nil {
			return TenantActivationResult{}, err
		}
	}
	presentations, err := tx.ExecContext(ctx, `
UPDATE presentation_materializations SET phase = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND intent_revision = ? AND phase = ?
  AND staged_view_id = ? AND staged_view_digest = ? AND observed_revision = ?`,
		uint8(PresentationMaterializationActive), string(request.Tenant), uint64(request.Generation),
		uint64(state.Intent.Revision), uint8(PresentationMaterializationApplied), request.ViewID[:],
		request.ViewDigest[:], uint64(application.StagedCatalogHead))
	if err != nil {
		return TenantActivationResult{}, mapConstraint(err)
	}
	if changed, _ := presentations.RowsAffected(); changed != int64(len(state.Target.RequiredBackends.Backends())) {
		return TenantActivationResult{}, ErrTenantLifecycleStale
	}
	var currentHead uint64
	if err := tx.QueryRowContext(ctx, `SELECT head FROM tenants WHERE tenant = ?`, string(request.Tenant)).Scan(&currentHead); err != nil {
		return TenantActivationResult{}, err
	}
	if Revision(currentHead) > application.StagedCatalogHead {
		return TenantActivationResult{}, ErrTenantLifecycleStale
	}
	if Revision(currentHead) < application.StagedCatalogHead {
		if _, err := tx.ExecContext(ctx, `UPDATE tenants SET head = ? WHERE tenant = ? AND head = ?`,
			uint64(application.StagedCatalogHead), string(request.Tenant), currentHead); err != nil {
			return TenantActivationResult{}, mapConstraint(err)
		}
	}
	tenantDelta := int64(0)
	if request.ExpectedActiveGeneration == 0 {
		tenantDelta = 1
	}
	if _, err := advanceTopologyTx(ctx, tx, SourceAuthorityFleetOwnerID(state.OwnerID),
		TopologyChangeTenant, request.Tenant, 0, tenantDelta); err != nil {
		return TenantActivationResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_activation_changes(
    activation_change_id, tenant_id, generation, staged_view_id, activation_revision,
    catalog_head, new_head_digest, operation_id, cause_count
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, changeID[:], string(request.Tenant), uint64(request.Generation),
		request.ViewID[:], uint64(nextRevision), uint64(application.StagedCatalogHead),
		application.StagedHeadDigest[:], request.Mutation.OperationID[:], len(causes)); err != nil {
		return TenantActivationResult{}, mapConstraint(err)
	}
	for index, cause := range causes {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_activation_causes(
    activation_change_id, position, source_authority, publication_id, source_revision,
    source_operation_id, change_id
) VALUES (?, ?, ?, ?, ?, ?, ?)`, changeID[:], index, string(causeAuthorities[index]),
			cause.PublicationID[:], uint64(cause.SourceRevision), cause.OperationID[:], cause.ChangeID[:]); err != nil {
			return TenantActivationResult{}, mapConstraint(err)
		}
	}
	for _, target := range targets {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO activation_outbox(
    activation_change_id, presentation_id, tenant_id, tenant_generation, backend,
    expected_activation_revision, expected_catalog_head, expected_head_digest,
    provider_fingerprint, signal_target_count, signal_target_digest, signal_coalesced,
    state, outcome, holder_runtime_generation, holder_operation_id, claim_token,
    attempt_count, claimed_unix_nano, ack_deadline_unix_nano,
    last_error_code, last_error_detail, retry_eligible,
    observed_activation_revision, observed_catalog_head, observed_head_digest,
    satisfied_by_activation_change_id, version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 0, NULL, NULL, NULL, 0, 0, 0,
          NULL, NULL, 0, NULL, NULL, NULL, NULL, 1)`, changeID[:], string(target.PresentationID),
			string(request.Tenant), uint64(request.Generation), uint8(target.Backend), uint64(nextRevision),
			uint64(application.StagedCatalogHead), application.StagedHeadDigest[:], target.ProviderFingerprint[:], target.SignalTargetCount,
			target.SignalTargetDigest[:], boolInt(target.SignalsCoalesced)); err != nil {
			return TenantActivationResult{}, mapConstraint(err)
		}
		for sequence, signal := range target.SignalTargets {
			kind := uint8(1)
			parent := []byte{}
			if !signal.WorkingSet {
				kind = 2
				parent = signal.Parent[:]
			}
			if _, err := tx.ExecContext(ctx, `
INSERT INTO activation_outbox_signal_targets(
    activation_change_id, presentation_id, sequence, kind, parent_id
) VALUES (?, ?, ?, ?, ?)`, changeID[:], string(target.PresentationID), sequence, kind, parent); err != nil {
				return TenantActivationResult{}, mapConstraint(err)
			}
		}
	}
	state, _, err = loadTenantLifecycle(ctx, tx, request.Tenant)
	if err != nil {
		return TenantActivationResult{}, err
	}
	if err := commitTenantMutation(ctx, tx, request.Mutation.OperationID, state); err != nil {
		return TenantActivationResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantActivationResult{}, err
	}
	c.topology.signal()
	return TenantActivationResult{State: state, ChangeID: changeID, Causes: causes, Targets: targets}, nil
}

type abandonedTenantPreparation struct {
	Tenant         TenantID
	Generation     Generation
	OwnerID        string
	IntentRevision TenantIntentRevision
	LostHolder     string
	Application    TenantOperationID
	ViewID         StagedViewID
	Presentations  []abandonedTenantPresentation
}

type abandonedTenantPresentation struct {
	Backend    TenantBackend                    `json:"backend"`
	Phase      PresentationMaterializationPhase `json:"phase"`
	LostHolder string                           `json:"lost_holder"`
	Operation  TenantOperationID                `json:"operation"`
}

// RecoverTenantPreparations resets only non-active staged applications whose
// prior holders were proven settled by the caller's sealed runtime boundary.
func (c *Catalog) RecoverTenantPreparations(
	ctx context.Context,
	request TenantPreparationRecoveryRequest,
) (TenantPreparationRecoveryResult, error) {
	settled, err := canonicalSettledHolderGenerations(request)
	if err != nil {
		return TenantPreparationRecoveryResult{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantPreparationRecoveryResult{}, err
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `
SELECT application.tenant_id, application.generation, generation.owner_id,
       intent.intent_revision, application.holder_runtime_generation,
       application.operation_id, application.staged_view_id
FROM tenant_applications application
JOIN tenant_intents intent
  ON intent.tenant_id = application.tenant_id
JOIN tenant_generations generation
  ON generation.tenant_id = application.tenant_id
 AND generation.generation = application.generation
JOIN tenant_activations activation
  ON activation.tenant_id = application.tenant_id
WHERE intent.state = ? AND intent.target_generation = application.generation
  AND application.phase = ?
  AND (activation.active_generation IS NULL OR activation.active_generation <> application.generation)
ORDER BY application.tenant_id, application.generation`, uint8(TenantIntentPresent), uint8(TenantApplicationStaged))
	if err != nil {
		return TenantPreparationRecoveryResult{}, err
	}
	var abandoned []abandonedTenantPreparation
	for rows.Next() {
		var candidate abandonedTenantPreparation
		var generation, intent uint64
		var operation, view []byte
		if err := rows.Scan(&candidate.Tenant, &generation, &candidate.OwnerID, &intent,
			&candidate.LostHolder, &operation, &view); err != nil {
			_ = rows.Close()
			return TenantPreparationRecoveryResult{}, err
		}
		candidate.Generation = Generation(generation)
		candidate.IntentRevision = TenantIntentRevision(intent)
		if err := copyExactID(candidate.Application[:], operation); err != nil {
			_ = rows.Close()
			return TenantPreparationRecoveryResult{}, ErrIntegrity
		}
		if err := copyExactID(candidate.ViewID[:], view); err != nil {
			_ = rows.Close()
			return TenantPreparationRecoveryResult{}, ErrIntegrity
		}
		if candidate.LostHolder == request.CurrentHolderRuntimeGeneration {
			continue
		}
		if _, proven := settled[candidate.LostHolder]; !proven {
			_ = rows.Close()
			return TenantPreparationRecoveryResult{}, fmt.Errorf(
				"%w: tenant %q generation %d is owned by unproven holder %q",
				ErrTenantPreparationOwnershipConflict, candidate.Tenant, candidate.Generation, candidate.LostHolder,
			)
		}
		abandoned = append(abandoned, candidate)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return TenantPreparationRecoveryResult{}, err
	}
	if err := rows.Close(); err != nil {
		return TenantPreparationRecoveryResult{}, err
	}
	for index := range abandoned {
		presentations, err := abandonedTenantPresentations(
			ctx, tx, abandoned[index], request.CurrentHolderRuntimeGeneration, settled,
		)
		if err != nil {
			return TenantPreparationRecoveryResult{}, err
		}
		abandoned[index].Presentations = presentations
	}
	result := TenantPreparationRecoveryResult{}
	for _, candidate := range abandoned {
		for _, presentation := range candidate.Presentations {
			presentationResult, err := tx.ExecContext(ctx, `
UPDATE presentation_materializations SET
    phase = ?, staged_view_id = NULL, staged_view_digest = NULL,
    backend_generation = '', observed_revision = 0,
    holder_runtime_generation = '', operation_id = x'', version = version + 1,
    failure_code = NULL, failure_detail = NULL, retry_eligible_at = NULL
WHERE tenant_id = ? AND generation = ? AND backend = ? AND intent_revision = ?
  AND phase = ? AND holder_runtime_generation = ? AND operation_id = ?`,
				uint8(PresentationMaterializationPending), string(candidate.Tenant), uint64(candidate.Generation),
				uint8(presentation.Backend), uint64(candidate.IntentRevision), uint8(presentation.Phase),
				presentation.LostHolder, presentation.Operation[:])
			if err != nil {
				return TenantPreparationRecoveryResult{}, mapConstraint(err)
			}
			presentations, err := presentationResult.RowsAffected()
			if err != nil {
				return TenantPreparationRecoveryResult{}, err
			}
			if presentations != 1 {
				return TenantPreparationRecoveryResult{}, ErrTenantLifecycleStale
			}
			result.ResetPresentations++
		}
		applicationResult, err := tx.ExecContext(ctx, `
UPDATE tenant_applications SET
    phase = ?, source_authority = '', source_publication_id = NULL,
    staged_view_id = NULL, staged_view_digest = NULL, staged_catalog_head = 0,
    staged_head_digest = NULL, staged_source_revision = 0, publication_digest = NULL,
    holder_runtime_generation = '', operation_id = x'', version = version + 1,
    failure_stage = NULL, failure_code = NULL, failure_detail = NULL, retry_eligible_at = NULL
WHERE tenant_id = ? AND generation = ? AND intent_revision = ? AND phase = ?
  AND holder_runtime_generation = ? AND operation_id = ? AND staged_view_id = ?`,
			uint8(TenantApplicationPending), string(candidate.Tenant), uint64(candidate.Generation),
			uint64(candidate.IntentRevision), uint8(TenantApplicationStaged), candidate.LostHolder,
			candidate.Application[:], candidate.ViewID[:])
		if err != nil {
			return TenantPreparationRecoveryResult{}, mapConstraint(err)
		}
		applications, err := applicationResult.RowsAffected()
		if err != nil {
			return TenantPreparationRecoveryResult{}, err
		}
		if applications != 1 {
			return TenantPreparationRecoveryResult{}, ErrTenantLifecycleStale
		}
		if err := insertOwnerLostTenantMutation(ctx, tx, request.CurrentHolderRuntimeGeneration, candidate); err != nil {
			return TenantPreparationRecoveryResult{}, err
		}
		result.ResetApplications++
	}
	if err := tx.Commit(); err != nil {
		return TenantPreparationRecoveryResult{}, err
	}
	return result, nil
}

func abandonedTenantPresentations(
	ctx context.Context,
	tx *sql.Tx,
	candidate abandonedTenantPreparation,
	current string,
	settled map[string]struct{},
) ([]abandonedTenantPresentation, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT backend, phase, holder_runtime_generation, operation_id
FROM presentation_materializations
WHERE tenant_id = ? AND generation = ? AND intent_revision = ?
ORDER BY backend`, string(candidate.Tenant), uint64(candidate.Generation), uint64(candidate.IntentRevision))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var abandoned []abandonedTenantPresentation
	for rows.Next() {
		var presentation abandonedTenantPresentation
		var backend, phase uint8
		var operation []byte
		if err := rows.Scan(&backend, &phase, &presentation.LostHolder, &operation); err != nil {
			return nil, err
		}
		presentation.Backend = TenantBackend(backend)
		presentation.Phase = PresentationMaterializationPhase(phase)
		switch presentation.Phase {
		case PresentationMaterializationPending:
			continue
		case PresentationMaterializationApplying, PresentationMaterializationApplied,
			PresentationMaterializationQuarantined:
		default:
			return nil, fmt.Errorf("%w: non-active target has presentation phase %d", ErrIntegrity, phase)
		}
		if presentation.LostHolder == current {
			return nil, fmt.Errorf(
				"%w: tenant %q generation %d backend %d is owned by current holder %q",
				ErrTenantPreparationOwnershipConflict, candidate.Tenant, candidate.Generation,
				presentation.Backend, presentation.LostHolder,
			)
		}
		if _, proven := settled[presentation.LostHolder]; !proven {
			return nil, fmt.Errorf(
				"%w: tenant %q generation %d backend %d is owned by unproven holder %q",
				ErrTenantPreparationOwnershipConflict, candidate.Tenant, candidate.Generation,
				presentation.Backend, presentation.LostHolder,
			)
		}
		if err := copyExactID(presentation.Operation[:], operation); err != nil {
			return nil, ErrIntegrity
		}
		abandoned = append(abandoned, presentation)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return abandoned, nil
}

func canonicalSettledHolderGenerations(request TenantPreparationRecoveryRequest) (map[string]struct{}, error) {
	if request.CurrentHolderRuntimeGeneration == "" {
		return nil, fmt.Errorf("%w: current holder generation is required", ErrInvalidObject)
	}
	settled := make(map[string]struct{}, len(request.SettledHolderRuntimeGenerations))
	previous := ""
	for _, generation := range request.SettledHolderRuntimeGenerations {
		if generation == "" || generation == request.CurrentHolderRuntimeGeneration ||
			(previous != "" && generation <= previous) {
			return nil, fmt.Errorf("%w: settled holder generations are not exact and ordered", ErrInvalidObject)
		}
		settled[generation] = struct{}{}
		previous = generation
	}
	return settled, nil
}

func insertOwnerLostTenantMutation(
	ctx context.Context,
	tx *sql.Tx,
	current string,
	candidate abandonedTenantPreparation,
) error {
	operation := ownerLostTenantOperationID(current, candidate)
	audit := struct {
		Cause          string                        `json:"cause"`
		Tenant         TenantID                      `json:"tenant"`
		Generation     Generation                    `json:"generation"`
		IntentRevision TenantIntentRevision          `json:"intent_revision"`
		LostHolder     string                        `json:"lost_holder"`
		CurrentHolder  string                        `json:"current_holder"`
		Application    TenantOperationID             `json:"application_operation"`
		ViewID         StagedViewID                  `json:"view_id"`
		Presentations  []abandonedTenantPresentation `json:"presentations"`
	}{
		Cause: "owner_lost", Tenant: candidate.Tenant, Generation: candidate.Generation,
		IntentRevision: candidate.IntentRevision, LostHolder: candidate.LostHolder,
		CurrentHolder: current, Application: candidate.Application, ViewID: candidate.ViewID,
		Presentations: candidate.Presentations,
	}
	encoded, err := json.Marshal(audit)
	if err != nil {
		return err
	}
	requestHash := sha256.Sum256(encoded)
	resultHash := sha256.Sum256(encoded)
	_, err = tx.ExecContext(ctx, `
INSERT INTO tenant_mutations(
    operation_id, tenant_id, kind, request_hash, state, holder_runtime_generation, owner_id,
    expected_intent_revision, result_intent_revision, result_activation_revision,
    result_code, result_bytes, result_hash
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'owner_lost', ?, ?)`, operation[:], string(candidate.Tenant),
		uint8(TenantMutationRecoverOwnerLost), requestHash[:], uint8(TenantMutationCommitted), current,
		candidate.OwnerID, uint64(candidate.IntentRevision), uint64(candidate.IntentRevision), encoded, resultHash[:])
	if err == nil {
		return nil
	}
	return mapConstraint(err)
}

func ownerLostTenantOperationID(current string, candidate abandonedTenantPreparation) TenantOperationID {
	digest := sha256.New()
	_, _ = digest.Write([]byte("github.com/yasyf/fusekit/catalog/tenant-owner-lost/v1\x00"))
	writeTenantLifecycleDigestField(digest, []byte(current))
	writeTenantLifecycleDigestField(digest, []byte(candidate.LostHolder))
	writeTenantLifecycleDigestField(digest, []byte(candidate.Tenant))
	writeTenantLifecycleDigestUint64(digest, uint64(candidate.Generation))
	writeTenantLifecycleDigestUint64(digest, uint64(candidate.IntentRevision))
	_, _ = digest.Write(candidate.Application[:])
	_, _ = digest.Write(candidate.ViewID[:])
	for _, presentation := range candidate.Presentations {
		writeTenantLifecycleDigestUint64(digest, uint64(presentation.Backend))
		writeTenantLifecycleDigestUint64(digest, uint64(presentation.Phase))
		writeTenantLifecycleDigestField(digest, []byte(presentation.LostHolder))
		_, _ = digest.Write(presentation.Operation[:])
	}
	var result TenantOperationID
	copy(result[:], digest.Sum(nil))
	return result
}

func canonicalActivationPublications(values []causal.OperationID) ([]causal.OperationID, error) {
	result := append([]causal.OperationID(nil), values...)
	sort.Slice(result, func(left, right int) bool { return bytes.Compare(result[left][:], result[right][:]) < 0 })
	for index, value := range result {
		if value == (causal.OperationID{}) || index > 0 && value == result[index-1] {
			return nil, ErrInvalidObject
		}
	}
	return result, nil
}

// TenantTargetingRevision returns the exact durable eligibility fence for one tenant.
func (c *Catalog) TenantTargetingRevision(ctx context.Context, tenant TenantID) (uint64, error) {
	if tenant == "" {
		return 0, ErrInvalidObject
	}
	var revision uint64
	if err := c.readDB.QueryRowContext(ctx, `
SELECT revision FROM tenant_targeting_heads WHERE tenant_id = ?`, string(tenant)).Scan(&revision); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, ErrNotFound
		}
		return 0, err
	}
	return revision, nil
}

func deriveTenantPresentationTargets(
	ctx context.Context,
	tx *sql.Tx,
	state TenantLifecycleState,
	application TenantApplication,
	causes []causal.SourceCause,
) ([]TenantPresentationTarget, error) {
	if state.Target == nil || !state.Target.RequiredBackends.Has(TenantBackendBroker) {
		return nil, nil
	}
	var providerChanged uint8
	var providerFingerprint []byte
	if err := tx.QueryRowContext(ctx, `
SELECT provider_changed, file_provider_fingerprint
FROM source_driver_publication_targets
WHERE source_authority = ? AND publication_id = ? AND tenant = ? AND generation = ? AND prepared = 1`,
		string(application.SourceAuthority), application.SourcePublication[:], string(application.Tenant),
		uint64(application.Generation)).Scan(&providerChanged, &providerFingerprint); err != nil {
		return nil, err
	}
	if providerChanged == 0 {
		return nil, nil
	}
	if len(providerFingerprint) != sha256.Size {
		return nil, ErrIntegrity
	}
	var domainID, presentationInstance string
	var domainGeneration uint64
	var registered uint8
	if err := tx.QueryRowContext(ctx, `
SELECT domain_id, presentation_instance_id, generation, registered
FROM file_provider_domains WHERE tenant = ?`, string(application.Tenant)).Scan(
		&domainID, &presentationInstance, &domainGeneration, &registered,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if registered == 0 || Generation(domainGeneration) != application.Generation ||
		presentationInstance != state.Target.Definition.FileProvider.PresentationInstanceID {
		return nil, nil
	}
	var liveLease uint8
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM file_provider_leases
    WHERE tenant = ? AND domain_id = ? AND generation = ? AND expires_unix_nano > ?
)`, string(application.Tenant), domainID, domainGeneration, time.Now().UnixNano()).Scan(&liveLease); err != nil {
		return nil, err
	}
	if liveLease == 0 {
		return nil, nil
	}
	presentation, found := presentationForLifecycleBackend(state.Presentations, application.Generation, TenantBackendBroker)
	if !found || presentation.Phase != PresentationMaterializationApplied ||
		presentation.ViewID != application.ViewID || presentation.ViewDigest != application.ViewDigest ||
		presentation.ObservedRevision != application.StagedCatalogHead {
		return nil, ErrTenantLifecycleStale
	}
	signals, found, err := fileProviderSignalTargetsTx(ctx, tx, application.Tenant,
		causal.DomainID(domainID), application.Generation, application.StagedCatalogHead)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	workingSet := false
	for _, target := range signals.Targets {
		workingSet = workingSet || target.WorkingSet
	}
	if workingSet {
		var materialized uint8
		if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1
    FROM file_provider_materialization_heads head
    JOIN file_provider_materialized_containers container
      ON container.tenant = head.tenant AND container.domain_id = head.domain_id
     AND container.generation = head.generation
     AND container.backing_store_identity = head.backing_store_identity
    WHERE head.tenant = ? AND head.domain_id = ? AND head.generation = ?
      AND head.eligible = 1
)`, string(application.Tenant), domainID, domainGeneration).Scan(&materialized); err != nil {
			return nil, err
		}
		if materialized == 0 && len(signals.Targets) == 1 {
			return nil, nil
		}
	}
	suppressed, err := activationOriginSuppressed(ctx, tx, causes, causal.DomainID(domainID),
		causal.Generation(domainGeneration))
	if err != nil {
		return nil, err
	}
	if suppressed {
		return nil, nil
	}
	var fingerprint [sha256.Size]byte
	copy(fingerprint[:], providerFingerprint)
	return []TenantPresentationTarget{{
		PresentationID: causal.PresentationID(domainID), Backend: causal.BackendFileProvider,
		ProviderFingerprint: fingerprint, SignalTargets: signals.Targets,
		SignalTargetCount: signals.ExactCount, SignalTargetDigest: signals.ExactDigest,
		SignalsCoalesced: signals.Coalesced,
	}}, nil
}

func presentationForLifecycleBackend(
	rows []PresentationMaterialization,
	generation Generation,
	backend TenantBackend,
) (PresentationMaterialization, bool) {
	for _, row := range rows {
		if row.Generation == generation && row.Backend == backend {
			return row, true
		}
	}
	return PresentationMaterialization{}, false
}

type fileProviderSignalTargets struct {
	Targets     []FileProviderSignalTarget
	ExactCount  uint64
	ExactDigest [sha256.Size]byte
	Coalesced   bool
}

func fileProviderSignalTargetsTx(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	domain causal.DomainID,
	generation Generation,
	revision Revision,
) (fileProviderSignalTargets, bool, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT scope_kind, scope_parent
FROM changes
WHERE tenant = ? AND revision = ? AND presentation = ?
  AND ((scope_kind = ? AND scope_domain = ? AND scope_generation = ?)
    OR (scope_kind = ? AND scope_domain = '' AND scope_generation = 0 AND EXISTS (
        SELECT 1
        FROM file_provider_materialization_heads head
        JOIN file_provider_materialized_containers container
          ON container.tenant = head.tenant AND container.domain_id = head.domain_id
         AND container.generation = head.generation
         AND container.backing_store_identity = head.backing_store_identity
        WHERE head.tenant = changes.tenant AND head.domain_id = ? AND head.generation = ?
          AND head.eligible = 1 AND container.container_id = changes.scope_parent
    )))
ORDER BY scope_kind, scope_parent`, string(tenant), uint64(revision), uint8(PresentationFileProvider),
		uint8(EnumerationWorkingSet), string(domain), uint64(generation), uint8(EnumerationContainer),
		string(domain), uint64(generation))
	if err != nil {
		return fileProviderSignalTargets{}, false, err
	}
	defer rows.Close()
	digest := sha256.New()
	plan := fileProviderSignalTargets{}
	for rows.Next() {
		var kind uint8
		var raw []byte
		if err := rows.Scan(&kind, &raw); err != nil {
			return fileProviderSignalTargets{}, false, err
		}
		var target FileProviderSignalTarget
		switch EnumerationScopeKind(kind) {
		case EnumerationWorkingSet:
			target.WorkingSet = true
		case EnumerationContainer:
			parent, err := objectID(raw)
			if err != nil {
				return fileProviderSignalTargets{}, false, err
			}
			target.Parent = parent
		default:
			return fileProviderSignalTargets{}, false, ErrIntegrity
		}
		_, _ = digest.Write([]byte{kind})
		if !target.WorkingSet {
			_, _ = digest.Write(target.Parent[:])
		}
		plan.ExactCount++
		if len(plan.Targets) < MaxFileProviderSignalTargets {
			plan.Targets = append(plan.Targets, target)
		}
	}
	if err := rows.Err(); err != nil {
		return fileProviderSignalTargets{}, false, err
	}
	if plan.ExactCount == 0 {
		return fileProviderSignalTargets{}, false, nil
	}
	copy(plan.ExactDigest[:], digest.Sum(nil))
	if plan.ExactCount > MaxFileProviderSignalTargets {
		plan.Targets = []FileProviderSignalTarget{{WorkingSet: true}}
		plan.Coalesced = true
	}
	return plan, true, nil
}

func activationOriginSuppressed(
	ctx context.Context,
	tx *sql.Tx,
	causes []causal.SourceCause,
	domain causal.DomainID,
	generation causal.Generation,
) (bool, error) {
	if len(causes) == 0 {
		return false, ErrIntegrity
	}
	for _, cause := range causes {
		var storedCause, originDomain string
		var originGeneration uint64
		if err := tx.QueryRowContext(ctx, `
SELECT cause, origin_domain, origin_generation
FROM source_driver_publications WHERE publication_id = ?`, cause.PublicationID[:]).Scan(
			&storedCause, &originDomain, &originGeneration,
		); err != nil {
			return false, err
		}
		if causal.Cause(storedCause) != causal.CauseProviderMutation ||
			causal.DomainID(originDomain) != domain || causal.Generation(originGeneration) != generation {
			return false, nil
		}
	}
	return true, nil
}

func validateActivationPresentations(
	state TenantLifecycleState,
	target TenantGeneration,
	application TenantApplication,
) error {
	required := target.RequiredBackends.Backends()
	rows := make(map[TenantBackend]PresentationMaterialization, len(required))
	for _, row := range state.Presentations {
		if row.Generation != target.Definition.Generation {
			continue
		}
		if _, duplicate := rows[row.Backend]; duplicate {
			return ErrIntegrity
		}
		rows[row.Backend] = row
	}
	if len(rows) != len(required) {
		return ErrTenantLifecycleStale
	}
	for _, backend := range required {
		row, found := rows[backend]
		if !found || row.Phase != PresentationMaterializationApplied ||
			row.IntentRevision != state.Intent.Revision || row.ViewID != application.ViewID ||
			row.ViewDigest != application.ViewDigest || row.ObservedRevision != application.StagedCatalogHead {
			return ErrTenantLifecycleStale
		}
	}
	return nil
}

func loadActivationCauses(
	ctx context.Context,
	tx *sql.Tx,
	tenant TenantID,
	generation Generation,
	application TenantApplication,
	publications []causal.OperationID,
) ([]causal.SourceCause, []causal.SourceAuthorityID, error) {
	type record struct {
		cause     causal.SourceCause
		authority causal.SourceAuthorityID
	}
	records := make([]record, 0, len(publications))
	applicationFound := false
	for _, publication := range publications {
		var record record
		var rawPublication, changeID, operationID, affectedDigest []byte
		var sourceRevision uint64
		var cause, authority string
		var publicationPrepared, targetPrepared uint8
		if err := tx.QueryRowContext(ctx, `
SELECT publication.publication_id, publication.change_id, publication.source_revision,
       publication.source_operation_id, publication.cause, publication.affected_keys_digest,
       publication.source_authority, publication.prepared, target.prepared
FROM source_driver_publications publication
JOIN source_driver_publication_targets target
  ON target.source_authority = publication.source_authority
 AND target.publication_id = publication.publication_id
WHERE publication.publication_id = ? AND target.tenant = ? AND target.generation = ?`,
			publication[:], string(tenant), uint64(generation)).Scan(
			&rawPublication, &changeID, &sourceRevision, &operationID, &cause, &affectedDigest,
			&authority, &publicationPrepared, &targetPrepared,
		); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil, ErrTenantLifecycleStale
			}
			return nil, nil, err
		}
		if publicationPrepared == 0 || targetPrepared == 0 ||
			causal.SourceAuthorityID(authority) != application.SourceAuthority {
			return nil, nil, ErrTenantLifecycleStale
		}
		if err := copyExactID(record.cause.PublicationID[:], rawPublication); err != nil {
			return nil, nil, err
		}
		if err := copyExactID(record.cause.ChangeID[:], changeID); err != nil {
			return nil, nil, err
		}
		if err := copyExactID(record.cause.OperationID[:], operationID); err != nil {
			return nil, nil, err
		}
		if err := copyExactID(record.cause.AffectedKeysDigest[:], affectedDigest); err != nil {
			return nil, nil, err
		}
		record.cause.SourceRevision, record.cause.Cause = causal.Revision(sourceRevision), causal.Cause(cause)
		record.authority = causal.SourceAuthorityID(authority)
		if record.cause.PublicationID == application.SourcePublication {
			applicationFound = true
		}
		records = append(records, record)
	}
	sort.Slice(records, func(left, right int) bool {
		if records[left].cause.SourceRevision != records[right].cause.SourceRevision {
			return records[left].cause.SourceRevision < records[right].cause.SourceRevision
		}
		return bytes.Compare(records[left].cause.PublicationID[:], records[right].cause.PublicationID[:]) < 0
	})
	if !applicationFound || records[len(records)-1].cause.SourceRevision != application.StagedSourceRevision {
		return nil, nil, ErrTenantLifecycleStale
	}
	causes := make([]causal.SourceCause, len(records))
	authorities := make([]causal.SourceAuthorityID, len(records))
	for index := range records {
		causes[index], authorities[index] = records[index].cause, records[index].authority
	}
	return causes, authorities, nil
}

func loadTenantActivationResult(
	ctx context.Context,
	query tenantLifecycleQueryer,
	state TenantLifecycleState,
) (TenantActivationResult, error) {
	var changeID []byte
	if err := query.QueryRowContext(ctx, `
SELECT activation_change_id FROM tenant_activation_changes
WHERE tenant_id = ? AND activation_revision = ?`, string(state.Intent.Tenant),
		uint64(state.Activation.Revision)).Scan(&changeID); err != nil {
		return TenantActivationResult{}, err
	}
	var result TenantActivationResult
	result.State = state
	if err := copyExactID(result.ChangeID[:], changeID); err != nil {
		return TenantActivationResult{}, err
	}
	rows, err := query.QueryContext(ctx, `
SELECT publication.publication_id, publication.change_id, publication.source_revision,
       publication.source_operation_id, publication.cause, publication.affected_keys_digest
FROM tenant_activation_causes cause
JOIN source_driver_publications publication
  ON publication.source_authority = cause.source_authority
 AND publication.publication_id = cause.publication_id
WHERE cause.activation_change_id = ? ORDER BY cause.position`, result.ChangeID[:])
	if err != nil {
		return TenantActivationResult{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var cause causal.SourceCause
		var publication, change, operation, digest []byte
		var revision uint64
		var kind string
		if err := rows.Scan(&publication, &change, &revision, &operation, &kind, &digest); err != nil {
			return TenantActivationResult{}, err
		}
		if copyExactID(cause.PublicationID[:], publication) != nil || copyExactID(cause.ChangeID[:], change) != nil ||
			copyExactID(cause.OperationID[:], operation) != nil || copyExactID(cause.AffectedKeysDigest[:], digest) != nil {
			return TenantActivationResult{}, ErrIntegrity
		}
		cause.SourceRevision, cause.Cause = causal.Revision(revision), causal.Cause(kind)
		result.Causes = append(result.Causes, cause)
	}
	if err := rows.Err(); err != nil {
		return TenantActivationResult{}, err
	}
	targetRows, err := query.QueryContext(ctx, `
SELECT presentation_id, backend, provider_fingerprint, signal_target_count,
       signal_target_digest, signal_coalesced
FROM activation_outbox WHERE activation_change_id = ? ORDER BY presentation_id, backend`, result.ChangeID[:])
	if err != nil {
		return TenantActivationResult{}, err
	}
	defer targetRows.Close()
	for targetRows.Next() {
		var target TenantPresentationTarget
		var presentationID string
		var backend, coalesced uint8
		var fingerprint, digest []byte
		if err := targetRows.Scan(&presentationID, &backend, &fingerprint,
			&target.SignalTargetCount, &digest, &coalesced); err != nil {
			return TenantActivationResult{}, err
		}
		target.PresentationID, target.Backend = causal.PresentationID(presentationID), causal.Backend(backend)
		target.SignalsCoalesced = coalesced != 0
		if copyExactID(target.ProviderFingerprint[:], fingerprint) != nil ||
			copyExactID(target.SignalTargetDigest[:], digest) != nil {
			return TenantActivationResult{}, ErrIntegrity
		}
		signals, err := query.QueryContext(ctx, `
SELECT kind, parent_id FROM activation_outbox_signal_targets
WHERE activation_change_id = ? AND presentation_id = ? ORDER BY sequence`, result.ChangeID[:], presentationID)
		if err != nil {
			return TenantActivationResult{}, err
		}
		for signals.Next() {
			var kind uint8
			var parent []byte
			if err := signals.Scan(&kind, &parent); err != nil {
				_ = signals.Close()
				return TenantActivationResult{}, err
			}
			var signal FileProviderSignalTarget
			switch kind {
			case 1:
				if len(parent) != 0 {
					_ = signals.Close()
					return TenantActivationResult{}, ErrIntegrity
				}
				signal.WorkingSet = true
			case 2:
				if copyExactID(signal.Parent[:], parent) != nil {
					_ = signals.Close()
					return TenantActivationResult{}, ErrIntegrity
				}
			default:
				_ = signals.Close()
				return TenantActivationResult{}, ErrIntegrity
			}
			target.SignalTargets = append(target.SignalTargets, signal)
		}
		if err := signals.Close(); err != nil {
			return TenantActivationResult{}, err
		}
		result.Targets = append(result.Targets, target)
	}
	if err := targetRows.Err(); err != nil {
		return TenantActivationResult{}, err
	}
	return result, nil
}

// RetirePresentation records terminal teardown of one old presentation backend.
func (c *Catalog) RetirePresentation(ctx context.Context, request RetirementRequest) (TenantLifecycleState, error) {
	if request.Tenant == "" || request.Generation == 0 ||
		(request.Backend != TenantBackendNative && request.Backend != TenantBackendBroker) {
		return TenantLifecycleState{}, ErrInvalidObject
	}
	return c.settleTenantRetirement(ctx, TenantMutationRetirePresentation, request, func(
		ctx context.Context, tx *sql.Tx, state TenantLifecycleState,
	) error {
		result, err := tx.ExecContext(ctx, `
UPDATE presentation_materializations SET phase = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND backend = ? AND transition_intent_revision = ? AND phase = ?`,
			uint8(PresentationMaterializationRetired), string(request.Tenant), uint64(request.Generation),
			uint8(request.Backend), uint64(state.Intent.Revision), uint8(PresentationMaterializationRetiring))
		if err != nil {
			return mapConstraint(err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrTenantLifecycleStale
		}
		return nil
	})
}

// RetireApplication records terminal teardown after every old backend settled.
func (c *Catalog) RetireApplication(ctx context.Context, request RetirementRequest) (TenantLifecycleState, error) {
	if request.Tenant == "" || request.Generation == 0 || request.Backend != 0 {
		return TenantLifecycleState{}, ErrInvalidObject
	}
	return c.settleTenantRetirement(ctx, TenantMutationRetireApplication, request, func(
		ctx context.Context, tx *sql.Tx, state TenantLifecycleState,
	) error {
		var unsettled uint64
		if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM presentation_materializations
WHERE tenant_id = ? AND generation = ? AND phase <> ?`, string(request.Tenant),
			uint64(request.Generation), uint8(PresentationMaterializationRetired)).Scan(&unsettled); err != nil {
			return err
		}
		if unsettled != 0 {
			return ErrTenantLifecycleStale
		}
		result, err := tx.ExecContext(ctx, `
UPDATE tenant_applications SET phase = ?, version = version + 1
WHERE tenant_id = ? AND generation = ? AND transition_intent_revision = ? AND phase = ?`,
			uint8(TenantApplicationRetired), string(request.Tenant), uint64(request.Generation),
			uint64(state.Intent.Revision), uint8(TenantApplicationRetiring))
		if err != nil {
			return mapConstraint(err)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			return ErrTenantLifecycleStale
		}
		return nil
	})
}

func (c *Catalog) settleTenantRetirement(
	ctx context.Context,
	kind TenantMutationKind,
	request RetirementRequest,
	settle func(context.Context, *sql.Tx, TenantLifecycleState) error,
) (TenantLifecycleState, error) {
	if err := validateTenantMutation(request.Mutation, request.Mutation.OwnerID, request.Tenant); err != nil {
		return TenantLifecycleState{}, err
	}
	requestHash, err := tenantMutationRequestHash(kind, request.Mutation, request.Tenant, struct {
		Generation Generation
		Backend    TenantBackend
	}{request.Generation, request.Backend})
	if err != nil {
		return TenantLifecycleState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if replay, found, err := beginTenantMutation(ctx, tx, kind, request.Mutation, request.Tenant, requestHash); err != nil {
		return TenantLifecycleState{}, err
	} else if found {
		var state TenantLifecycleState
		if err := json.Unmarshal(replay, &state); err != nil {
			return TenantLifecycleState{}, ErrIntegrity
		}
		if err := tx.Commit(); err != nil {
			return TenantLifecycleState{}, err
		}
		return state, nil
	}
	state, found, err := loadTenantLifecycle(ctx, tx, request.Tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if !found || state.OwnerID != request.Mutation.OwnerID ||
		state.Intent.Revision != request.Mutation.ExpectedIntentRevision {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	if active, err := activeTenantMutationReservation(ctx, tx, request.Tenant,
		request.Mutation.OperationID); err != nil {
		return TenantLifecycleState{}, err
	} else if active {
		return TenantLifecycleState{}, ErrMutationActive
	}
	if err := settle(ctx, tx, state); err != nil {
		return TenantLifecycleState{}, err
	}
	state, _, err = loadTenantLifecycle(ctx, tx, request.Tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if err := commitTenantMutation(ctx, tx, request.Mutation.OperationID, state); err != nil {
		return TenantLifecycleState{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantLifecycleState{}, err
	}
	return state, nil
}

// ClearTenantActivation removes the final absent shared state after teardown settles.
func (c *Catalog) ClearTenantActivation(ctx context.Context, request RetirementRequest) (TenantLifecycleState, error) {
	if request.Tenant == "" || request.Generation == 0 || request.Backend != 0 {
		return TenantLifecycleState{}, ErrInvalidObject
	}
	if err := validateTenantMutation(request.Mutation, request.Mutation.OwnerID, request.Tenant); err != nil {
		return TenantLifecycleState{}, err
	}
	requestHash, err := tenantMutationRequestHash(TenantMutationClearActivation, request.Mutation,
		request.Tenant, struct{ Generation Generation }{request.Generation})
	if err != nil {
		return TenantLifecycleState{}, err
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	defer func() { _ = tx.Rollback() }()
	if replay, found, err := beginTenantMutation(ctx, tx, TenantMutationClearActivation,
		request.Mutation, request.Tenant, requestHash); err != nil {
		return TenantLifecycleState{}, err
	} else if found {
		var state TenantLifecycleState
		if err := json.Unmarshal(replay, &state); err != nil {
			return TenantLifecycleState{}, ErrIntegrity
		}
		if err := tx.Commit(); err != nil {
			return TenantLifecycleState{}, err
		}
		return state, nil
	}
	state, found, err := loadTenantLifecycle(ctx, tx, request.Tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if !found || state.OwnerID != request.Mutation.OwnerID || state.Intent.Kind != TenantIntentAbsent ||
		state.Intent.Revision != request.Mutation.ExpectedIntentRevision ||
		!state.Activation.Active() || state.Activation.ActiveGeneration != request.Generation ||
		!state.Activation.Retiring {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	if active, err := activeTenantMutationReservation(ctx, tx, request.Tenant,
		request.Mutation.OperationID); err != nil {
		return TenantLifecycleState{}, err
	} else if active {
		return TenantLifecycleState{}, ErrMutationActive
	}
	application, ok := state.application(request.Generation)
	if !ok || application.Phase != TenantApplicationRetired ||
		application.TransitionIntentRevision != state.Intent.Revision {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	for _, row := range state.Presentations {
		if row.Generation == request.Generation && (row.Phase != PresentationMaterializationRetired ||
			row.TransitionIntentRevision != state.Intent.Revision) {
			return TenantLifecycleState{}, ErrTenantLifecycleStale
		}
	}
	nextRevision := state.Activation.Revision + 1
	activation, err := tx.ExecContext(ctx, `
UPDATE tenant_activations SET
    active_generation = NULL, active_view_id = NULL, active_catalog_head = 0, source_revision = 0,
    activation_revision = ?, retiring = 0, version = version + 1, last_operation_id = ?
WHERE tenant_id = ? AND active_generation = ? AND activation_revision = ?`, uint64(nextRevision),
		request.Mutation.OperationID[:], string(request.Tenant), uint64(request.Generation),
		uint64(state.Activation.Revision))
	if err != nil {
		return TenantLifecycleState{}, mapConstraint(err)
	}
	if changed, _ := activation.RowsAffected(); changed != 1 {
		return TenantLifecycleState{}, ErrTenantLifecycleStale
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tenant_applications WHERE tenant_id = ? AND generation = ?`,
		string(request.Tenant), uint64(request.Generation)); err != nil {
		return TenantLifecycleState{}, mapConstraint(err)
	}
	if _, err := advanceTopologyTx(ctx, tx, SourceAuthorityFleetOwnerID(state.OwnerID),
		TopologyChangeTenant, request.Tenant, 0, -1); err != nil {
		return TenantLifecycleState{}, err
	}
	state, _, err = loadTenantLifecycle(ctx, tx, request.Tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if err := commitTenantMutation(ctx, tx, request.Mutation.OperationID, state); err != nil {
		return TenantLifecycleState{}, err
	}
	if err := tx.Commit(); err != nil {
		return TenantLifecycleState{}, err
	}
	c.topology.signal()
	return state, nil
}

type tenantLifecycleQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func validateTenantMutation(mutation TenantMutation, owner string, tenant TenantID) error {
	if mutation.OperationID == (TenantOperationID{}) || mutation.HolderRuntimeGeneration == "" ||
		mutation.OwnerID == "" || mutation.OwnerID != owner || tenant == "" {
		return fmt.Errorf("%w: incomplete tenant mutation identity", ErrInvalidObject)
	}
	return nil
}

func tenantMutationRequestHash(kind TenantMutationKind, mutation TenantMutation, tenant TenantID, payload any) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(struct {
		Domain                  string               `json:"domain"`
		Kind                    TenantMutationKind   `json:"kind"`
		Tenant                  TenantID             `json:"tenant"`
		OwnerID                 string               `json:"owner_id"`
		HolderRuntimeGeneration string               `json:"holder_runtime_generation"`
		ExpectedIntentRevision  TenantIntentRevision `json:"expected_intent_revision"`
		Payload                 any                  `json:"payload"`
	}{
		Domain: "github.com/yasyf/fusekit/catalog/tenant-mutation/v1", Kind: kind,
		Tenant: tenant, OwnerID: mutation.OwnerID,
		HolderRuntimeGeneration: mutation.HolderRuntimeGeneration,
		ExpectedIntentRevision:  mutation.ExpectedIntentRevision, Payload: payload,
	})
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("catalog: encode tenant mutation request: %w", err)
	}
	return sha256.Sum256(encoded), nil
}

func beginTenantMutation(
	ctx context.Context,
	tx *sql.Tx,
	kind TenantMutationKind,
	mutation TenantMutation,
	tenant TenantID,
	requestHash [sha256.Size]byte,
) ([]byte, bool, error) {
	var storedTenant, holder, owner, resultCode string
	var storedKind, state uint8
	var storedHash, resultBytes, resultHash []byte
	var expected, resultIntent, resultActivation uint64
	err := tx.QueryRowContext(ctx, `
SELECT tenant_id, kind, request_hash, state, holder_runtime_generation, owner_id,
       expected_intent_revision, result_intent_revision, result_activation_revision,
       result_code, result_bytes, result_hash
FROM tenant_mutations WHERE operation_id = ?`, mutation.OperationID[:]).Scan(
		&storedTenant, &storedKind, &storedHash, &state, &holder, &owner, &expected,
		&resultIntent, &resultActivation, &resultCode, &resultBytes, &resultHash,
	)
	if err == nil {
		if storedTenant != string(tenant) || TenantMutationKind(storedKind) != kind ||
			!bytes.Equal(storedHash, requestHash[:]) || holder != mutation.HolderRuntimeGeneration ||
			owner != mutation.OwnerID || TenantIntentRevision(expected) != mutation.ExpectedIntentRevision {
			return nil, false, ErrTenantMutationConflict
		}
		switch TenantMutationState(state) {
		case TenantMutationPending:
			return nil, false, ErrTenantLifecycleRetryDeferred
		case TenantMutationCommitted:
			if resultCode != "ok" || len(resultHash) != sha256.Size ||
				sha256.Sum256(resultBytes) != [sha256.Size]byte(resultHash) {
				return nil, false, ErrIntegrity
			}
			return append([]byte(nil), resultBytes...), true, nil
		case TenantMutationFailed:
			return nil, false, ErrTenantLifecycleRetryDeferred
		default:
			return nil, false, ErrIntegrity
		}
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	if _, err := tx.ExecContext(ctx, `
INSERT INTO tenant_mutations(
    operation_id, tenant_id, kind, request_hash, state, holder_runtime_generation, owner_id,
    expected_intent_revision, result_intent_revision, result_activation_revision,
    result_code, result_bytes, result_hash
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, '', x'', x'')`, mutation.OperationID[:], string(tenant),
		uint8(kind), requestHash[:], uint8(TenantMutationPending), mutation.HolderRuntimeGeneration,
		mutation.OwnerID, uint64(mutation.ExpectedIntentRevision)); err != nil {
		return nil, false, mapConstraint(err)
	}
	return nil, false, nil
}

func commitTenantMutation(ctx context.Context, tx *sql.Tx, operation TenantOperationID, state TenantLifecycleState) error {
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("catalog: encode tenant mutation result: %w", err)
	}
	digest := sha256.Sum256(encoded)
	result, err := tx.ExecContext(ctx, `
UPDATE tenant_mutations SET
    state = ?, result_intent_revision = ?, result_activation_revision = ?,
    result_code = 'ok', result_bytes = ?, result_hash = ?
WHERE operation_id = ? AND state = ?`, uint8(TenantMutationCommitted), uint64(state.Intent.Revision),
		uint64(state.Activation.Revision), encoded, digest[:], operation[:], uint8(TenantMutationPending))
	if err != nil {
		return mapConstraint(err)
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return ErrTenantLifecycleStale
	}
	return nil
}

func activeTenantMutationReservation(ctx context.Context, tx *sql.Tx, tenant TenantID, current TenantOperationID) (bool, error) {
	var count uint64
	if err := tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM tenant_mutations
WHERE tenant_id = ? AND state = ? AND operation_id <> ?`, string(tenant), uint8(TenantMutationPending), current[:]).Scan(&count); err != nil {
		return false, err
	}
	return count != 0, nil
}

func activeSourceDriverMutationReservation(ctx context.Context, tx *sql.Tx, tenant TenantID) (bool, error) {
	var active uint8
	if err := tx.QueryRowContext(ctx, `
SELECT EXISTS(
    SELECT 1 FROM source_driver_mutation_reservations
    WHERE mutation_tenant = ? AND committed = 0
)`, string(tenant)).Scan(&active); err != nil {
		return false, err
	}
	return active != 0, nil
}

func insertTenantGeneration(ctx context.Context, tx *sql.Tx, generation TenantGeneration) error {
	d := generation.Definition
	_, err := tx.ExecContext(ctx, `
INSERT INTO tenant_generations(
    tenant_id, generation, owner_id, spec, spec_hash, required_backends,
    mount_presentation_root, backing_root, content_source_id,
    file_provider_presentation_instance_id, file_provider_display_name,
    access_mode, case_policy, presentation_set
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, string(d.Tenant), uint64(d.Generation), d.OwnerID,
		generation.CanonicalSpec, generation.SpecHash[:], uint8(generation.RequiredBackends),
		d.Mount.PresentationRoot, d.BackingRoot, d.ContentSourceID,
		d.FileProvider.PresentationInstanceID, d.FileProvider.DisplayName,
		uint8(d.Access), uint8(d.CasePolicy), uint8(d.Presentations))
	if err != nil {
		return mapConstraint(err)
	}
	return nil
}

func loadTenantGeneration(
	ctx context.Context,
	query tenantLifecycleQueryer,
	tenant TenantID,
	generation Generation,
) (TenantGeneration, bool, error) {
	var stored TenantGeneration
	var d TenantProvision
	var rawRoot, spec, hash []byte
	var required, access, policy, presentations uint8
	var storedGeneration uint64
	err := query.QueryRowContext(ctx, `
SELECT g.owner_id, g.tenant_id, COALESCE(t.root_id, x''), g.mount_presentation_root,
       g.backing_root, g.content_source_id, g.file_provider_presentation_instance_id,
       g.file_provider_display_name, g.access_mode, g.case_policy, g.presentation_set,
       g.generation, g.spec, g.spec_hash, g.required_backends
FROM tenant_generations g
LEFT JOIN tenants t ON t.tenant = g.tenant_id
WHERE g.tenant_id = ? AND g.generation = ?`, string(tenant), uint64(generation)).Scan(
		&d.OwnerID, &d.Tenant, &rawRoot, &d.Mount.PresentationRoot, &d.BackingRoot,
		&d.ContentSourceID, &d.FileProvider.PresentationInstanceID, &d.FileProvider.DisplayName,
		&access, &policy, &presentations, &storedGeneration, &spec, &hash, &required,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return TenantGeneration{}, false, nil
	}
	if err != nil {
		return TenantGeneration{}, false, err
	}
	if len(rawRoot) != 0 {
		root, err := objectID(rawRoot)
		if err != nil {
			return TenantGeneration{}, false, err
		}
		d.Root = root
	}
	if len(hash) != sha256.Size {
		return TenantGeneration{}, false, ErrIntegrity
	}
	d.Access, d.CasePolicy, d.Presentations, d.Generation = TenantAccessMode(access), CasePolicy(policy), PresentationSet(presentations), Generation(storedGeneration)
	stored.Definition = d
	stored.CanonicalSpec = append([]byte(nil), spec...)
	copy(stored.SpecHash[:], hash)
	stored.RequiredBackends = TenantBackendSet(required)
	if !stored.RequiredBackends.valid() || validateTenantProvision(d) != nil {
		return TenantGeneration{}, false, ErrIntegrity
	}
	return stored, true, nil
}

// TenantLifecycle returns one exact tenant intent, activation, and generation state.
func (c *Catalog) TenantLifecycle(ctx context.Context, owner string, tenant TenantID) (TenantLifecycleState, error) {
	state, found, err := loadTenantLifecycle(ctx, c.readDB, tenant)
	if err != nil {
		return TenantLifecycleState{}, err
	}
	if !found {
		return TenantLifecycleState{}, ErrNotFound
	}
	if state.OwnerID != owner {
		return TenantLifecycleState{}, ErrTenantOwnerMismatch
	}
	return state, nil
}

func loadTenantLifecycle(ctx context.Context, query tenantLifecycleQueryer, tenant TenantID) (TenantLifecycleState, bool, error) {
	var state TenantLifecycleState
	var target, active sql.NullInt64
	var intentKind uint8
	var intentRevision, intentVersion uint64
	var currentOperation, activeView, lastOperation []byte
	var catalogHead, sourceRevision, activationRevision, activationVersion uint64
	var retiring uint8
	err := query.QueryRowContext(ctx, `
SELECT i.state, i.target_generation, i.intent_revision, i.current_operation_id, i.version,
       a.active_generation, a.active_view_id, a.active_catalog_head, a.source_revision,
       a.activation_revision, a.retiring, a.version, a.last_operation_id
FROM tenant_intents i
JOIN tenant_activations a ON a.tenant_id = i.tenant_id
WHERE i.tenant_id = ?`, string(tenant)).Scan(
		&intentKind, &target, &intentRevision, &currentOperation, &intentVersion,
		&active, &activeView, &catalogHead, &sourceRevision, &activationRevision,
		&retiring, &activationVersion, &lastOperation,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return TenantLifecycleState{}, false, nil
	}
	if err != nil {
		return TenantLifecycleState{}, false, err
	}
	currentID, err := tenantOperationID(currentOperation)
	if err != nil {
		return TenantLifecycleState{}, false, err
	}
	lastID, err := tenantOperationID(lastOperation)
	if err != nil {
		return TenantLifecycleState{}, false, err
	}
	state.Intent = TenantIntent{
		Tenant: tenant, Kind: TenantIntentKind(intentKind), Revision: TenantIntentRevision(intentRevision),
		CurrentOperation: currentID, Version: intentVersion,
	}
	if target.Valid {
		state.Intent.TargetGeneration = Generation(target.Int64)
	}
	state.Activation = TenantActivation{
		Tenant: tenant, ActiveCatalogHead: Revision(catalogHead), ActiveSourceRevision: causal.Revision(sourceRevision),
		Revision: TenantActivationRevision(activationRevision), Retiring: retiring != 0,
		Version: activationVersion, LastOperation: lastID,
	}
	if active.Valid {
		if len(activeView) != len(state.Activation.ActiveView) {
			return TenantLifecycleState{}, false, ErrIntegrity
		}
		state.Activation.ActiveGeneration = Generation(active.Int64)
		copy(state.Activation.ActiveView[:], activeView)
	} else if len(activeView) != 0 {
		return TenantLifecycleState{}, false, ErrIntegrity
	}
	var distinctOwners uint64
	if err := query.QueryRowContext(ctx, `
SELECT MIN(owner_id), COUNT(DISTINCT owner_id)
FROM tenant_generations WHERE tenant_id = ?`, string(tenant)).Scan(&state.OwnerID, &distinctOwners); err != nil {
		return TenantLifecycleState{}, false, err
	}
	if state.OwnerID == "" || distinctOwners != 1 {
		return TenantLifecycleState{}, false, ErrIntegrity
	}
	if target.Valid {
		generation, found, err := loadTenantGeneration(ctx, query, tenant, Generation(target.Int64))
		if err != nil {
			return TenantLifecycleState{}, false, err
		}
		if !found {
			return TenantLifecycleState{}, false, ErrIntegrity
		}
		state.Target = &generation
	}
	if active.Valid {
		generation, found, err := loadTenantGeneration(ctx, query, tenant, Generation(active.Int64))
		if err != nil {
			return TenantLifecycleState{}, false, err
		}
		if !found {
			return TenantLifecycleState{}, false, ErrIntegrity
		}
		state.Active = &generation
	}
	applications, err := query.QueryContext(ctx, `
SELECT tenant_id, generation, intent_revision, transition_intent_revision, content_source_id,
       phase, source_authority, source_publication_id, staged_view_id, staged_view_digest,
       staged_catalog_head, staged_head_digest, staged_source_revision, publication_digest, holder_runtime_generation,
       operation_id, version, failure_code, failure_detail, retry_eligible_at
FROM tenant_applications WHERE tenant_id = ? ORDER BY generation`, string(tenant))
	if err != nil {
		return TenantLifecycleState{}, false, err
	}
	defer applications.Close()
	for applications.Next() {
		application, err := scanTenantApplication(applications)
		if err != nil {
			return TenantLifecycleState{}, false, err
		}
		state.Applications = append(state.Applications, application)
	}
	if err := applications.Err(); err != nil {
		return TenantLifecycleState{}, false, err
	}
	presentations, err := query.QueryContext(ctx, `
SELECT tenant_id, generation, backend, intent_revision, transition_intent_revision, phase,
       staged_view_id, staged_view_digest, backend_generation, observed_revision,
       holder_runtime_generation, operation_id, version, failure_code, failure_detail, retry_eligible_at
FROM presentation_materializations WHERE tenant_id = ? ORDER BY generation, backend`, string(tenant))
	if err != nil {
		return TenantLifecycleState{}, false, err
	}
	defer presentations.Close()
	for presentations.Next() {
		presentation, err := scanPresentationMaterialization(presentations)
		if err != nil {
			return TenantLifecycleState{}, false, err
		}
		state.Presentations = append(state.Presentations, presentation)
	}
	if err := presentations.Err(); err != nil {
		return TenantLifecycleState{}, false, err
	}
	return state, true, nil
}

type tenantLifecycleScanner interface{ Scan(...any) error }

func scanTenantApplication(scanner tenantLifecycleScanner) (TenantApplication, error) {
	var row TenantApplication
	var tenant, contentSource, sourceAuthority, holder string
	var generation, intentRevision, transitionRevision, catalogHead, sourceRevision, version uint64
	var phase uint8
	var sourcePublication, viewID, viewDigest, headDigest, publicationDigest, operationID []byte
	var failureCode, failureDetail sql.NullString
	var retryAt sql.NullInt64
	if err := scanner.Scan(
		&tenant, &generation, &intentRevision, &transitionRevision, &contentSource,
		&phase, &sourceAuthority, &sourcePublication, &viewID, &viewDigest,
		&catalogHead, &headDigest, &sourceRevision, &publicationDigest, &holder, &operationID, &version,
		&failureCode, &failureDetail, &retryAt,
	); err != nil {
		return TenantApplication{}, err
	}
	row.Tenant, row.Generation = TenantID(tenant), Generation(generation)
	row.IntentRevision, row.TransitionIntentRevision = TenantIntentRevision(intentRevision), TenantIntentRevision(transitionRevision)
	row.ContentSourceID, row.Phase = contentSource, TenantApplicationPhase(phase)
	row.SourceAuthority, row.HolderRuntimeGeneration = causal.SourceAuthorityID(sourceAuthority), holder
	row.StagedCatalogHead, row.StagedSourceRevision = Revision(catalogHead), causal.Revision(sourceRevision)
	row.Version = TenantApplicationVersion(version)
	if err := copyExactID(row.SourcePublication[:], sourcePublication); err != nil && len(sourcePublication) != 0 {
		return TenantApplication{}, err
	}
	if err := copyExactID(row.ViewID[:], viewID); err != nil && len(viewID) != 0 {
		return TenantApplication{}, err
	}
	if err := copyExactID(row.ViewDigest[:], viewDigest); err != nil && len(viewDigest) != 0 {
		return TenantApplication{}, err
	}
	if err := copyExactID(row.StagedHeadDigest[:], headDigest); err != nil && len(headDigest) != 0 {
		return TenantApplication{}, err
	}
	if err := copyExactID(row.PublicationDigest[:], publicationDigest); err != nil && len(publicationDigest) != 0 {
		return TenantApplication{}, err
	}
	if err := copyExactID(row.OperationID[:], operationID); err != nil && len(operationID) != 0 {
		return TenantApplication{}, err
	}
	if failureCode.Valid || failureDetail.Valid || retryAt.Valid {
		if !failureCode.Valid || !failureDetail.Valid {
			return TenantApplication{}, ErrIntegrity
		}
		row.Failure = &TenantFailure{Code: failureCode.String, Detail: failureDetail.String}
		if retryAt.Valid {
			value := time.Unix(0, retryAt.Int64).UTC()
			row.Failure.RetryEligibleAt = &value
		}
	}
	return row, nil
}

func scanPresentationMaterialization(scanner tenantLifecycleScanner) (PresentationMaterialization, error) {
	var row PresentationMaterialization
	var tenant, backendGeneration, holder string
	var generation, intentRevision, transitionRevision, observed, version uint64
	var backend, phase uint8
	var viewID, viewDigest, operationID []byte
	var failureCode, failureDetail sql.NullString
	var retryAt sql.NullInt64
	if err := scanner.Scan(
		&tenant, &generation, &backend, &intentRevision, &transitionRevision, &phase,
		&viewID, &viewDigest, &backendGeneration, &observed, &holder, &operationID, &version,
		&failureCode, &failureDetail, &retryAt,
	); err != nil {
		return PresentationMaterialization{}, err
	}
	row.Tenant, row.Generation, row.Backend = TenantID(tenant), Generation(generation), TenantBackend(backend)
	row.IntentRevision, row.TransitionIntentRevision = TenantIntentRevision(intentRevision), TenantIntentRevision(transitionRevision)
	row.Phase, row.BackendGeneration = PresentationMaterializationPhase(phase), backendGeneration
	row.ObservedRevision, row.HolderRuntimeGeneration = Revision(observed), holder
	row.Version = PresentationMaterializationVersion(version)
	if err := copyExactID(row.ViewID[:], viewID); err != nil && len(viewID) != 0 {
		return PresentationMaterialization{}, err
	}
	if err := copyExactID(row.ViewDigest[:], viewDigest); err != nil && len(viewDigest) != 0 {
		return PresentationMaterialization{}, err
	}
	if err := copyExactID(row.OperationID[:], operationID); err != nil && len(operationID) != 0 {
		return PresentationMaterialization{}, err
	}
	if failureCode.Valid || failureDetail.Valid || retryAt.Valid {
		if !failureCode.Valid || !failureDetail.Valid {
			return PresentationMaterialization{}, ErrIntegrity
		}
		row.Failure = &TenantFailure{Code: failureCode.String, Detail: failureDetail.String}
		if retryAt.Valid {
			value := time.Unix(0, retryAt.Int64).UTC()
			row.Failure.RetryEligibleAt = &value
		}
	}
	return row, nil
}

func tenantOperationID(raw []byte) (TenantOperationID, error) {
	var id TenantOperationID
	if err := copyExactID(id[:], raw); err != nil {
		return TenantOperationID{}, err
	}
	return id, nil
}

func copyExactID(destination, source []byte) error {
	if len(destination) != len(source) {
		return ErrIntegrity
	}
	copy(destination, source)
	return nil
}

func lifecycleOwner(state TenantLifecycleState) (string, error) {
	if state.OwnerID == "" {
		return "", ErrIntegrity
	}
	return state.OwnerID, nil
}
