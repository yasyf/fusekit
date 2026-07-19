package catalogproto

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const MaxPageSize uint32 = 1_000

var (
	ErrInvalidMessage = errors.New("catalog protocol: invalid message")
	ErrProtocol       = errors.New("catalog protocol: unsupported protocol")
	ErrForbiddenPath  = errors.New("catalog protocol: app group path forbidden")
)

// Encode validates and returns the canonical JSON encoding of one protocol value.
func Encode(value any) ([]byte, error) {
	if err := Validate(value); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("catalog protocol: encode: %w", err)
	}
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return nil, err
	}
	return canonical, nil
}

// Decode strictly decodes and validates exactly one protocol value.
func Decode(data []byte, dst any) error {
	if dst == nil || reflect.ValueOf(dst).Kind() != reflect.Pointer || reflect.ValueOf(dst).IsNil() {
		return invalid("decode destination must be a non-nil pointer")
	}
	if err := inspectJSON(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return invalid("decode: %v", err)
	}
	if err := requireEOF(decoder); err != nil {
		return err
	}
	return Validate(dst)
}

// Validate checks one generated message or nested protocol value.
func Validate(value any) error {
	if value == nil {
		return invalid("nil value")
	}
	v := reflect.ValueOf(value)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return invalid("nil value")
		}
		v = v.Elem()
	}
	switch message := v.Interface().(type) {
	case CatalogObject:
		return validateCatalogObject(message)
	case Change:
		return validateChange(message)
	case ChangeCursor:
		return validateChangeCursor(message)
	case CatalogLaneProof:
		return validateCatalogLaneProof(message)
	case DomainObservation:
		return validateDomainObservation(message)
	case PreparationProof:
		return validatePreparationProof(message)
	case SignalTarget:
		return validateSignalTarget(message)
	case EnumerationScope:
		return validateEnumerationScope(message)
	case BrokerForwardContext:
		return validateBrokerForwardContext(message)
	case SourceCommit:
		return validateSourceCommit(message)
	case SourceTenantRecord:
		return validateSourceTenantRecord(message)
	case SourceObjectRecord:
		return validateSourceObjectRecord(message)
	case SourceDeleteRecord:
		return validateSourceDeleteRecord(message)
	case ConvergenceNotification:
		return validateConvergenceNotification(message)
	case DomainRegistration:
		return validateDomainRegistration(message)
	case RegisteredDomain:
		return validateRegisteredDomain(message)
	case DomainCutoverAccount:
		return validateDomainCutoverAccount(message)
	case DomainCutoverPlan:
		return validateDomainCutoverPlan(message)
	case DomainCutoverRecoveryAccount:
		return validateDomainCutoverRecoveryAccount(message)
	case DomainCutoverRecoveryKey:
		return validateDomainCutoverRecoveryKey(message)
	case DomainCutoverObservation:
		return validateDomainCutoverObservation(message)
	case DomainCutoverResult:
		return validateDomainCutoverResult(message)
	case DomainAbsenceProof:
		return validateDomainAbsenceProof(message)
	case BrokerPeerProof:
		return validateBrokerPeerProof(message)
	case DomainCutoverClaim:
		return validateDomainCutoverClaim(message)
	case DomainCutoverReceipt:
		return validateDomainCutoverReceipt(message)
	case BrokerOpenRequest:
		return validateProtocol(message.Protocol)
	case BrokerOpenResponse:
		return validateResponse(message.Protocol, message.Code, message.Message)
	case BrokerBindDomainRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		if err := validateDomainID(message.DomainID); err != nil {
			return err
		}
		if err := validateOpaque(string(message.TenantID)); err != nil {
			return err
		}
		if message.Generation == 0 {
			return invalid("broker domain binding generation is zero")
		}
		return nil
	case BrokerBindDomainResponse:
		return validateResponse(message.Protocol, message.Code, message.Message)
	case BrokerForwardRequest:
		return validateBrokerForwardRequest(message)
	case BrokerCommand:
		return validateBrokerCommand(message)
	case BrokerResult:
		return validateBrokerResult(message)
	case CutoverDomainsRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateDomainCutoverPlan(message.Plan)
	case CutoverDomainsResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code == ErrorCodeOk {
			if message.Proof == nil {
				return invalid("successful domain cutover response has no proof")
			}
			return validateDomainAbsenceProof(*message.Proof)
		}
		if message.Proof != nil {
			return invalid("failed domain cutover response carries a proof")
		}
		return nil
	case ClaimDomainCutoverRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateDomainAbsenceProof(message.Proof)
	case ClaimDomainCutoverResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code == ErrorCodeOk {
			if message.Claim == nil {
				return invalid("successful domain cutover claim response has no claim")
			}
			return validateDomainCutoverClaim(*message.Claim)
		}
		if message.Claim != nil {
			return invalid("failed domain cutover claim response carries a claim")
		}
		return nil
	case RecoverDomainCutoverClaimRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateDomainAbsenceProof(message.Proof)
	case RecoverDomainCutoverClaimResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code == ErrorCodeOk {
			if message.Claim == nil {
				return invalid("successful domain cutover claim recovery has no claim")
			}
			return validateDomainCutoverClaim(*message.Claim)
		}
		if message.Claim != nil {
			return invalid("failed domain cutover claim recovery carries a claim")
		}
		return nil
	case RecoverDomainCutoverReceiptRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateDomainCutoverRecoveryKey(message.Key)
	case RecoverDomainCutoverReceiptResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code == ErrorCodeOk {
			if message.Receipt == nil {
				return invalid("successful domain cutover receipt recovery has no receipt")
			}
			return validateDomainCutoverReceipt(*message.Receipt)
		}
		if message.Receipt != nil {
			return invalid("failed domain cutover receipt recovery carries a receipt")
		}
		return nil
	case ProveBrokerPeerRequest:
		return validateProtocol(message.Protocol)
	case ProveBrokerPeerResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code == ErrorCodeOk {
			if message.Proof == nil {
				return invalid("successful broker peer proof response has no proof")
			}
			return validateBrokerPeerProof(*message.Proof)
		}
		if message.Proof != nil {
			return invalid("failed broker peer proof response carries a proof")
		}
		return nil
	case RootRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateGeneration(message.Generation)
	case HeadRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateGeneration(message.Generation)
	case HeadResponse:
		return validateHeadResponse(message)
	case SnapshotRequest:
		return validateSnapshotRequest(message)
	case SnapshotResponse:
		return validateSnapshotResponse(message)
	case ChangesSinceRequest:
		return validateChangesSinceRequest(message)
	case ChangesSinceResponse:
		return validateChangesSinceResponse(message)
	case LookupRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		if err := validateGeneration(message.Generation); err != nil {
			return err
		}
		return validateObjectID(message.ObjectID)
	case LookupNameRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		if err := validateGeneration(message.Generation); err != nil {
			return err
		}
		if err := validateObjectID(message.ParentID); err != nil {
			return err
		}
		return validateName(message.Name)
	case LookupResponse:
		return validateLookupResponse(message)
	case OpenAtRequest:
		return validateOpenAtRequest(message)
	case OpenAtResponse:
		return validateOpenAtResponse(message)
	case MutationRequest:
		return validateMutationRequest(message)
	case MutationResponse:
		return validateMutationResponse(message)
	case SourceReconcileRequest:
		return validateSourceReconcileRequest(message)
	case SourceReconcileResponse:
		return validateSourceReconcileResponse(message)
	case PrepareTenantRequest:
		return validatePrepareTenantRequest(message)
	case PrepareTenantResponse:
		return validatePrepareTenantResponse(message)
	case AckConvergenceRequest:
		return validateAckConvergenceRequest(message)
	case AckConvergenceResponse:
		return validateAckConvergenceResponse(message)
	default:
		return invalid("unsupported value type %T", value)
	}
}

func validateProtocol(version uint16) error {
	if version != Version {
		return fmt.Errorf("%w: got %d, want %d", ErrProtocol, version, Version)
	}
	return nil
}

func validateResponse(protocol uint16, code ErrorCode, message string) error {
	if err := validateProtocol(protocol); err != nil {
		return err
	}
	if !validErrorCode(code) {
		return invalid("unknown error code %q", code)
	}
	if code == ErrorCodeOk && message != "" {
		return invalid("successful response carries an error message")
	}
	if code != ErrorCodeOk && message == "" {
		return invalid("error response has no message")
	}
	return nil
}

func validateCatalogObject(object CatalogObject) error {
	if err := validateObjectID(object.ID); err != nil {
		return err
	}
	if err := validateObjectID(object.ParentID); err != nil {
		return err
	}
	if object.Revision == 0 || object.MetadataRevision == 0 {
		return invalid("object revision is zero")
	}
	if object.MetadataRevision > object.Revision || object.ContentRevision > object.Revision {
		return invalid("object component revision exceeds object revision")
	}
	if object.Name == "" {
		if object.ID != object.ParentID || object.Kind != ObjectKindDirectory {
			return invalid("non-root object name is empty")
		}
	} else if err := validateName(object.Name); err != nil {
		return err
	}
	if !validObjectKind(object.Kind) {
		return invalid("unknown object kind %q", object.Kind)
	}
	if object.Kind == ObjectKindDirectory && (object.ContentRevision != 0 || object.Size != 0 || object.Hash != "" || object.LinkTarget != "") {
		return invalid("directory carries file content")
	}
	if object.Kind == ObjectKindFile {
		if object.ContentRevision == 0 || object.LinkTarget != "" {
			return invalid("file content is invalid")
		}
		if err := validateHash(object.Hash); err != nil {
			return err
		}
	}
	if object.Kind == ObjectKindSymlink {
		if err := validateSymlinkContent(object.LinkTarget, object.ContentRevision, object.Size, object.Hash); err != nil {
			return err
		}
	}
	if object.Applied > object.Verified || object.Verified > object.Observed || object.Observed > object.Desired {
		return invalid("object convergence proof order")
	}
	return nil
}

func validateChange(change Change) error {
	if change.Revision == 0 {
		return invalid("change revision is zero")
	}
	if change.Sequence == ChangeCursorCompleteSequence {
		return invalid("change sequence uses the complete-cursor sentinel")
	}
	if !validChangeKind(change.Kind) {
		return invalid("unknown change kind %q", change.Kind)
	}
	if err := validateCatalogObject(change.Object); err != nil {
		return err
	}
	if change.Object.Revision > change.Revision {
		return invalid("change object revision exceeds journal revision")
	}
	return nil
}

func validateCatalogLaneProof(proof CatalogLaneProof) error {
	if err := validateOpaque(string(proof.Tenant)); err != nil {
		return err
	}
	if proof.Generation == 0 || proof.Requested == 0 {
		return invalid("convergence generation or requested revision is zero")
	}
	if proof.Applied > proof.Verified || proof.Verified > proof.Observed || proof.Observed > proof.Desired || proof.Desired > proof.Requested {
		return invalid("convergence proof order")
	}
	return nil
}

func validateDomainObservation(observation DomainObservation) error {
	if err := validateOpaque(string(observation.TenantID)); err != nil {
		return err
	}
	if err := validateDomainID(observation.DomainID); err != nil {
		return err
	}
	if err := validateOpaque(string(observation.SourceAuthority)); err != nil {
		return err
	}
	if observation.Generation == 0 || observation.RequestedRevision == 0 || observation.CatalogRevision == 0 || observation.SourceRevision == 0 {
		return invalid("domain observation revision identity is zero")
	}
	if observation.ObservedRevision > observation.RequestedRevision {
		return invalid("domain observation exceeds requested engine revision")
	}
	if err := validateChangeID(observation.ChangeID); err != nil {
		return err
	}
	return validateMutationID(observation.OperationID)
}

func validatePreparationProof(proof PreparationProof) error {
	if err := validateCatalogLaneProof(proof.Catalog); err != nil {
		return err
	}
	if err := validateDomainObservation(proof.Domain); err != nil {
		return err
	}
	if proof.Catalog.Tenant != proof.Domain.TenantID || proof.Catalog.Generation != proof.Domain.Generation || proof.Catalog.Requested != proof.Domain.CatalogRevision {
		return invalid("preparation proof lanes do not identify the same request")
	}
	return nil
}

func validateSignalTarget(target SignalTarget) error {
	switch target.Kind {
	case SignalTargetKindWorkingSet:
		if target.ParentID != nil {
			return invalid("working-set target carries a parent")
		}
	case SignalTargetKindContainer:
		if target.ParentID == nil {
			return invalid("container target has no parent")
		}
		if err := validateObjectID(*target.ParentID); err != nil {
			return err
		}
	default:
		return invalid("unknown signal target kind %q", target.Kind)
	}
	return nil
}

func validateEnumerationScope(scope EnumerationScope) error {
	switch scope.Kind {
	case EnumerationScopeKindWorkingSet:
		if scope.ParentID != nil {
			return invalid("working-set scope carries a parent")
		}
	case EnumerationScopeKindContainer:
		if scope.ParentID == nil {
			return invalid("container scope has no parent")
		}
		return validateObjectID(*scope.ParentID)
	default:
		return invalid("unknown enumeration scope kind %q", scope.Kind)
	}
	return nil
}

func validateChangeCursor(cursor ChangeCursor) error {
	return nil
}

func validateBrokerForwardContext(bound BrokerForwardContext) error {
	if err := validateDomainID(bound.DomainID); err != nil {
		return err
	}
	if err := validateOpaque(string(bound.TenantID)); err != nil {
		return err
	}
	return validateGeneration(bound.Generation)
}

func validateBrokerForwardRequest(request BrokerForwardRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateBrokerForwardContext(request.Context); err != nil {
		return err
	}
	if !forwardableOperation(request.Operation) {
		return invalid("operation %q cannot be broker-forwarded", request.Operation)
	}
	if len(request.Payload) == 0 {
		return invalid("broker-forward payload is empty")
	}
	return nil
}

func validateConvergenceNotification(notification ConvergenceNotification) error {
	if err := validateProtocol(notification.Protocol); err != nil {
		return err
	}
	if err := validateOpaque(string(notification.TenantID)); err != nil {
		return err
	}
	if err := validateDomainID(notification.DomainID); err != nil {
		return err
	}
	if err := validateOpaque(string(notification.SourceAuthority)); err != nil {
		return err
	}
	if notification.Generation == 0 || notification.Revision == 0 || notification.CatalogRevision == 0 || notification.SourceRevision == 0 {
		return invalid("notification revision is zero")
	}
	if err := validateChangeID(notification.ChangeID); err != nil {
		return err
	}
	if err := validateMutationID(notification.OperationID); err != nil {
		return err
	}
	if !validConvergenceCause(notification.Cause) {
		return invalid("unknown convergence cause %q", notification.Cause)
	}
	if !sort.StringsAreSorted(notification.AffectedKeys) || hasAdjacentDuplicates(notification.AffectedKeys) {
		return invalid("affected keys are not sorted and unique")
	}
	if len(notification.Targets) == 0 {
		return invalid("notification has no signal targets")
	}
	keys := make([]string, len(notification.Targets))
	for i, target := range notification.Targets {
		if err := validateSignalTarget(target); err != nil {
			return err
		}
		keys[i] = signalTargetKey(target)
	}
	if !sort.StringsAreSorted(keys) || hasAdjacentDuplicates(keys) {
		return invalid("signal targets are not sorted and unique")
	}
	return nil
}

func validateDomainRegistration(registration DomainRegistration) error {
	if err := validateDomainID(registration.DomainID); err != nil {
		return err
	}
	if err := validateOpaque(string(registration.OwnerID)); err != nil {
		return err
	}
	if err := validateOpaque(string(registration.TenantID)); err != nil {
		return err
	}
	if registration.Generation == 0 {
		return invalid("domain registration generation is zero")
	}
	if err := validateObjectID(registration.RootID); err != nil {
		return err
	}
	if err := validateOpaque(string(registration.AccountInstanceID)); err != nil {
		return err
	}
	derived, err := DeriveDomainID(registration.OwnerID, registration.AccountInstanceID)
	if err != nil {
		return err
	}
	if registration.DomainID != derived {
		return invalid("domain id is not derived from owner and account instance")
	}
	if registration.DisplayName == "" {
		return invalid("domain display name is empty")
	}
	return nil
}

func validateRegisteredDomain(domain RegisteredDomain) error {
	if err := validateDomainRegistration(DomainRegistration{
		DomainID: domain.DomainID, OwnerID: domain.OwnerID, TenantID: domain.TenantID,
		Generation: domain.Generation, RootID: domain.RootID, AccountInstanceID: domain.AccountInstanceID, DisplayName: domain.DisplayName,
	}); err != nil {
		return err
	}
	return validatePublicPath(domain.PublicPath)
}

func validateDomainCutoverAccount(account DomainCutoverAccount) error {
	if account.AccountID == 0 {
		return invalid("domain cutover account id is zero")
	}
	if err := validateHash(account.ImmutableIdentity); err != nil {
		return invalid("domain cutover immutable identity is invalid: %v", err)
	}
	if account.LegacyDomainID != fmt.Sprintf("acct-%02d", account.AccountID) {
		return invalid("domain cutover legacy domain id does not match account id")
	}
	if account.AccountInstanceID != nil {
		return validateOpaque(string(*account.AccountInstanceID))
	}
	return nil
}

func validateDomainCutoverPlan(plan DomainCutoverPlan) error {
	if err := validateMutationID(plan.OperationID); err != nil {
		return err
	}
	if err := validateOpaque(string(plan.OwnerID)); err != nil {
		return err
	}
	if len(plan.Accounts) == 0 {
		return invalid("domain cutover plan has no accounts")
	}
	var previous uint64
	instances := make(map[AccountInstanceID]struct{}, len(plan.Accounts))
	for _, account := range plan.Accounts {
		if err := validateDomainCutoverAccount(account); err != nil {
			return err
		}
		if account.AccountID <= previous {
			return invalid("domain cutover accounts are not sorted and unique")
		}
		previous = account.AccountID
		if account.AccountInstanceID != nil {
			if _, exists := instances[*account.AccountInstanceID]; exists {
				return invalid("domain cutover account instance ids are not unique")
			}
			instances[*account.AccountInstanceID] = struct{}{}
		}
	}
	return nil
}

func validateDomainCutoverRecoveryAccount(account DomainCutoverRecoveryAccount) error {
	if account.AccountID == 0 {
		return invalid("domain cutover recovery account id is zero")
	}
	if err := validateHash(account.ImmutableIdentity); err != nil {
		return invalid("domain cutover recovery immutable identity is invalid: %v", err)
	}
	return nil
}

func validateDomainCutoverRecoveryKey(key DomainCutoverRecoveryKey) error {
	if err := validateOpaque(string(key.OwnerID)); err != nil {
		return err
	}
	if len(key.Accounts) == 0 {
		return invalid("domain cutover recovery key has no accounts")
	}
	var previous uint64
	for _, account := range key.Accounts {
		if err := validateDomainCutoverRecoveryAccount(account); err != nil {
			return err
		}
		if account.AccountID <= previous {
			return invalid("domain cutover recovery accounts are not sorted and unique")
		}
		previous = account.AccountID
	}
	return nil
}

func validateDomainCutoverObservation(observation DomainCutoverObservation) error {
	if observation.DomainID == "" || strings.ContainsAny(observation.DomainID, "/\\") || observation.AccountID == 0 {
		return invalid("domain cutover observation identity is invalid")
	}
	if err := validateHash(observation.ImmutableIdentity); err != nil {
		return invalid("domain cutover observation immutable identity is invalid: %v", err)
	}
	if observation.Legacy {
		if observation.Generation != 0 || observation.AccountInstanceID != nil ||
			observation.DomainID != fmt.Sprintf("acct-%02d", observation.AccountID) {
			return invalid("legacy domain cutover observation has current-domain state")
		}
		return nil
	}
	if observation.AccountInstanceID == nil {
		return invalid("current domain cutover observation has no account instance")
	}
	return validateOpaque(string(*observation.AccountInstanceID))
}

func validateDomainCutoverResult(result DomainCutoverResult) error {
	if err := validateDomainCutoverPlan(result.Plan); err != nil {
		return err
	}
	if result.FinalEnumerationRevision == 0 || result.FinalEnumeratedAtUnixNano <= 0 {
		return invalid("domain cutover final enumeration identity is invalid")
	}
	accounts := make(map[uint64]DomainCutoverAccount, len(result.Plan.Accounts))
	for _, account := range result.Plan.Accounts {
		accounts[account.AccountID] = account
	}
	previous := ""
	for _, observation := range result.ObservedDomains {
		if err := validateDomainCutoverObservation(observation); err != nil {
			return err
		}
		if previous != "" && observation.DomainID <= previous {
			return invalid("domain cutover observations are not sorted and unique")
		}
		previous = observation.DomainID
		account, ok := accounts[observation.AccountID]
		if !ok || account.ImmutableIdentity != observation.ImmutableIdentity {
			return invalid("domain cutover observation is not bound to a planned account")
		}
		if observation.Legacy {
			if observation.DomainID != account.LegacyDomainID {
				return invalid("legacy domain cutover observation id changed")
			}
			continue
		}
		if account.AccountInstanceID == nil || observation.AccountInstanceID == nil ||
			*account.AccountInstanceID != *observation.AccountInstanceID {
			return invalid("current domain cutover observation account instance changed")
		}
		derived, err := DeriveDomainID(result.Plan.OwnerID, *account.AccountInstanceID)
		if err != nil || observation.DomainID != string(derived) {
			return invalid("current domain cutover observation id is not derived")
		}
	}
	return nil
}

func validateDomainAbsenceProof(proof DomainAbsenceProof) error {
	if err := validateDomainCutoverResult(proof.Result); err != nil {
		return err
	}
	return validateBrokerPeerProof(BrokerPeerProof{
		BrokerProductBuild: proof.BrokerProductBuild, BrokerPID: proof.BrokerPID, BrokerUID: proof.BrokerUID,
		BrokerStartTime: proof.BrokerStartTime, BrokerBoot: proof.BrokerBoot, BrokerComm: proof.BrokerComm,
		BrokerExecutable: proof.BrokerExecutable, BrokerDesignatedRequirement: proof.BrokerDesignatedRequirement,
		BrokerAuditTokenDigest:            proof.BrokerAuditTokenDigest,
		BrokerEntitlementValidationDigest: proof.BrokerEntitlementValidationDigest,
	})
}

func validateBrokerPeerProof(proof BrokerPeerProof) error {
	if proof.BrokerProductBuild == "" || proof.BrokerPID <= 1 || proof.BrokerUID < 0 ||
		proof.BrokerStartTime == "" || proof.BrokerBoot == "" || proof.BrokerComm == "" ||
		!filepath.IsAbs(proof.BrokerExecutable) || filepath.Clean(proof.BrokerExecutable) != proof.BrokerExecutable ||
		proof.BrokerDesignatedRequirement == "" || validateHash(proof.BrokerAuditTokenDigest) != nil ||
		validateHash(proof.BrokerEntitlementValidationDigest) != nil {
		return invalid("broker peer proof has invalid identity")
	}
	return nil
}

func validateDomainCutoverClaim(claim DomainCutoverClaim) error {
	if err := validateMutationID(claim.OperationID); err != nil {
		return err
	}
	if err := validateHash(claim.ProofDigest); err != nil {
		return invalid("domain cutover claim proof digest is invalid: %v", err)
	}
	if claim.ClaimedAtUnixNano <= 0 {
		return invalid("domain cutover claim time is invalid")
	}
	return nil
}

func validateDomainCutoverReceipt(receipt DomainCutoverReceipt) error {
	if err := validateDomainAbsenceProof(receipt.Proof); err != nil {
		return err
	}
	if err := validateDomainCutoverClaim(receipt.Claim); err != nil {
		return err
	}
	if receipt.Claim.OperationID != receipt.Proof.Result.Plan.OperationID {
		return invalid("domain cutover receipt operation changed")
	}
	encoded, err := json.Marshal(receipt.Proof)
	if err != nil {
		return invalid("domain cutover receipt proof cannot be encoded")
	}
	digest := sha256.Sum256(encoded)
	if receipt.Claim.ProofDigest != hex.EncodeToString(digest[:]) {
		return invalid("domain cutover receipt proof digest changed")
	}
	return nil
}

func validateBrokerCommand(command BrokerCommand) error {
	if err := validateProtocol(command.Protocol); err != nil {
		return err
	}
	if command.CommandID == 0 {
		return invalid("broker command id is zero")
	}
	switch command.Kind {
	case BrokerCommandKindRegisterDomain:
		if command.Registration == nil || command.DomainID != nil || command.Notification != nil || command.Cutover != nil {
			return invalid("register-domain command has the wrong shape")
		}
		return validateDomainRegistration(*command.Registration)
	case BrokerCommandKindRemoveDomain:
		if command.Registration != nil || command.DomainID == nil || command.Notification != nil || command.Cutover != nil {
			return invalid("remove-domain command has the wrong shape")
		}
		return validateDomainID(*command.DomainID)
	case BrokerCommandKindListDomains:
		if command.Registration != nil || command.DomainID != nil || command.Notification != nil || command.Cutover != nil {
			return invalid("list-domains command has the wrong shape")
		}
		return nil
	case BrokerCommandKindSignalDomain:
		if command.Registration != nil || command.DomainID != nil || command.Notification == nil || command.Cutover != nil {
			return invalid("signal-domain command has the wrong shape")
		}
		return validateConvergenceNotification(*command.Notification)
	case BrokerCommandKindCutoverDomains:
		if command.Registration != nil || command.DomainID != nil || command.Notification != nil || command.Cutover == nil {
			return invalid("cutover-domains command has the wrong shape")
		}
		return validateDomainCutoverPlan(*command.Cutover)
	default:
		return invalid("unknown broker command kind %q", command.Kind)
	}
}

func validateBrokerResult(result BrokerResult) error {
	if err := validateResponse(result.Protocol, result.Code, result.Message); err != nil {
		return err
	}
	if result.CommandID == 0 || !validBrokerCommandKind(result.Kind) {
		return invalid("broker result identity is invalid")
	}
	if result.Code != ErrorCodeOk {
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted != nil || result.CutoverResult != nil {
			return invalid("failed broker result carries success payload")
		}
		return nil
	}
	switch result.Kind {
	case BrokerCommandKindRegisterDomain:
		if result.Registered == nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted != nil || result.CutoverResult != nil {
			return invalid("register-domain result has the wrong shape")
		}
		return validateRegisteredDomain(*result.Registered)
	case BrokerCommandKindRemoveDomain:
		if result.Registered != nil || result.ConfirmedAbsent == nil || !*result.ConfirmedAbsent || result.Domains != nil || result.SignalAccepted != nil || result.CutoverResult != nil {
			return invalid("remove-domain result does not confirm absence")
		}
	case BrokerCommandKindListDomains:
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains == nil || result.SignalAccepted != nil || result.CutoverResult != nil {
			return invalid("list-domains result has the wrong shape")
		}
		for _, domain := range *result.Domains {
			if err := validateRegisteredDomain(domain); err != nil {
				return err
			}
		}
	case BrokerCommandKindSignalDomain:
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted == nil || !*result.SignalAccepted || result.CutoverResult != nil {
			return invalid("signal-domain result does not confirm acceptance")
		}
	case BrokerCommandKindCutoverDomains:
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted != nil || result.CutoverResult == nil {
			return invalid("cutover-domains result has the wrong shape")
		}
		return validateDomainCutoverResult(*result.CutoverResult)
	default:
		return invalid("unknown broker result kind %q", result.Kind)
	}
	return nil
}

func validateHeadResponse(response HeadResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code == ErrorCodeOk && response.Revision == 0 {
		return invalid("head revision is zero")
	}
	return nil
}

func validateSnapshotRequest(request SnapshotRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateGeneration(request.Generation); err != nil {
		return err
	}
	if request.Revision == 0 {
		return invalid("snapshot revision is zero")
	}
	if err := validateEnumerationScope(request.Scope); err != nil {
		return err
	}
	if request.After != nil {
		if err := validateObjectID(*request.After); err != nil {
			return err
		}
	}
	return validateLimit(request.Limit)
}

func validateSnapshotResponse(response SnapshotResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code != ErrorCodeOk {
		return nil
	}
	if response.Revision == 0 {
		return invalid("snapshot revision is zero")
	}
	for _, object := range response.Objects {
		if err := validateCatalogObject(object); err != nil {
			return err
		}
		if object.Revision > response.Revision {
			return invalid("snapshot object exceeds pinned revision")
		}
	}
	if response.Next != nil {
		return validateObjectID(*response.Next)
	}
	return nil
}

func validateChangesSinceRequest(request ChangesSinceRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateGeneration(request.Generation); err != nil {
		return err
	}
	if err := validateChangeCursor(request.Cursor); err != nil {
		return err
	}
	if err := validateEnumerationScope(request.Scope); err != nil {
		return err
	}
	return validateLimit(request.Limit)
}

func validateChangesSinceResponse(response ChangesSinceResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code != ErrorCodeOk {
		return nil
	}
	if response.Head == 0 || response.Floor > response.Head || response.Next.Revision > response.Head || response.Next.Revision < response.Floor {
		return invalid("changes response bounds are invalid")
	}
	if err := validateChangeCursor(response.Next); err != nil {
		return err
	}
	if response.Complete {
		if response.Next.Revision != response.Head || response.Next.Sequence != ChangeCursorCompleteSequence {
			return invalid("complete changes response has a non-terminal cursor")
		}
	} else if response.Next.Sequence == ChangeCursorCompleteSequence || len(response.Changes) == 0 {
		return invalid("incomplete changes response has no resumable row cursor")
	}
	previousRevision := uint64(0)
	previousSequence := uint32(0)
	for _, change := range response.Changes {
		if err := validateChange(change); err != nil {
			return err
		}
		if change.Revision < previousRevision || change.Revision == previousRevision && change.Sequence <= previousSequence {
			return invalid("changes are not strictly ordered")
		}
		previousRevision, previousSequence = change.Revision, change.Sequence
	}
	if !response.Complete {
		last := response.Changes[len(response.Changes)-1]
		if response.Next.Revision != last.Revision || response.Next.Sequence != last.Sequence {
			return invalid("changes next cursor does not name the last emitted row")
		}
	}
	return nil
}

func validateLookupResponse(response LookupResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code == ErrorCodeOk && response.Object == nil {
		return invalid("successful lookup has no object")
	}
	if response.Object != nil {
		return validateCatalogObject(*response.Object)
	}
	return nil
}

func validateOpenAtRequest(request OpenAtRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if request.Generation == 0 || request.Revision == 0 {
		return invalid("open generation or revision is zero")
	}
	return validateObjectID(request.ObjectID)
}

func validateOpenAtResponse(response OpenAtResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code == ErrorCodeOk && response.Object == nil {
		return invalid("successful open has no object")
	}
	if response.Object != nil {
		return validateCatalogObject(*response.Object)
	}
	return nil
}

func validateMutationRequest(request MutationRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateMutationID(request.OperationID); err != nil {
		return err
	}
	if request.Generation == 0 || request.ExpectedRevision == 0 {
		return invalid("mutation generation or expected revision is zero")
	}
	switch request.Kind {
	case MutationKindCreate:
		if request.ObjectKind == nil || request.ObjectID != nil || request.ParentID == nil || request.TargetID != nil || request.Name == nil || request.Mode == nil {
			return invalid("create mutation has the wrong shape")
		}
		if !validObjectKind(*request.ObjectKind) {
			return invalid("create mutation has unknown object kind %q", *request.ObjectKind)
		}
		if err := validateObjectID(*request.ParentID); err != nil {
			return err
		}
		if err := validateName(*request.Name); err != nil {
			return err
		}
		if *request.ObjectKind == ObjectKindDirectory && (request.HasContent || request.ContentRevision != nil || request.LinkTarget != nil) {
			return invalid("directory create carries content")
		}
		if *request.ObjectKind == ObjectKindFile && (!request.HasContent || request.ContentRevision == nil || *request.ContentRevision == 0 || request.LinkTarget != nil) {
			return invalid("file create has no content revision")
		}
		if *request.ObjectKind == ObjectKindSymlink {
			if request.HasContent || request.ContentRevision == nil || *request.ContentRevision == 0 || request.LinkTarget == nil {
				return invalid("symlink create has invalid content")
			}
			if err := validateLinkTarget(*request.LinkTarget); err != nil {
				return err
			}
		}
	case MutationKindRevise:
		if request.ObjectKind != nil || request.ObjectID == nil || request.ParentID == nil || request.TargetID != nil || request.Name == nil || request.Mode == nil || request.LinkTarget != nil {
			return invalid("revise mutation has the wrong shape")
		}
		if err := validateObjectID(*request.ObjectID); err != nil {
			return err
		}
		if err := validateObjectID(*request.ParentID); err != nil {
			return err
		}
		if err := validateName(*request.Name); err != nil {
			return err
		}
		if request.HasContent != (request.ContentRevision != nil) || request.ContentRevision != nil && *request.ContentRevision == 0 {
			return invalid("revise mutation content intent is inconsistent")
		}
	case MutationKindDelete:
		if request.ObjectKind != nil || request.HasContent || request.ObjectID == nil || request.ParentID != nil || request.TargetID != nil || request.Name != nil || request.Mode != nil || request.ContentRevision != nil || request.LinkTarget != nil {
			return invalid("delete mutation has the wrong shape")
		}
		return validateObjectID(*request.ObjectID)
	case MutationKindReplace:
		if request.ObjectKind != nil || request.ObjectID == nil || request.TargetID == nil || request.LinkTarget != nil {
			return invalid("replace mutation has the wrong shape")
		}
		if err := validateObjectID(*request.ObjectID); err != nil {
			return err
		}
		if err := validateObjectID(*request.TargetID); err != nil {
			return err
		}
		if request.ParentID != nil {
			if err := validateObjectID(*request.ParentID); err != nil {
				return err
			}
		}
		if request.Name != nil {
			if err := validateName(*request.Name); err != nil {
				return err
			}
		}
		if request.HasContent != (request.ContentRevision != nil) || request.ContentRevision != nil && *request.ContentRevision == 0 {
			return invalid("replace mutation content intent is inconsistent")
		}
	default:
		return invalid("unknown mutation kind %q", request.Kind)
	}
	return nil
}

func validateMutationResponse(response MutationResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code != ErrorCodeOk {
		return nil
	}
	if response.OperationID == nil || response.Revision == 0 {
		return invalid("successful mutation has no identity or revision")
	}
	if err := validateMutationID(*response.OperationID); err != nil {
		return err
	}
	if response.PrimaryID != nil {
		if err := validateObjectID(*response.PrimaryID); err != nil {
			return err
		}
	}
	if response.SecondaryID != nil {
		return validateObjectID(*response.SecondaryID)
	}
	return nil
}

func validateSourceCommit(commit SourceCommit) error {
	if err := validateOpaque(string(commit.TenantID)); err != nil {
		return err
	}
	if commit.CatalogRevision == 0 {
		return invalid("source commit catalog revision is zero")
	}
	return nil
}

func validateSourceTenantRecord(record SourceTenantRecord) error {
	if err := validateOpaque(string(record.TenantID)); err != nil {
		return err
	}
	if err := validateOpaque(record.RootKey); err != nil {
		return err
	}
	if record.Generation == 0 {
		return invalid("source tenant generation is zero")
	}
	return nil
}

func validateSourceObjectRecord(record SourceObjectRecord) error {
	if err := validateOpaque(record.SourceKey); err != nil {
		return err
	}
	if record.ParentKey != "" {
		if err := validateOpaque(record.ParentKey); err != nil {
			return err
		}
	}
	if record.ParentKey == record.SourceKey {
		return invalid("source object is its own parent")
	}
	if err := validateName(record.Name); err != nil {
		return err
	}
	if !record.MountVisible && !record.FileProviderVisible {
		return invalid("source object is invisible")
	}
	switch record.Kind {
	case ObjectKindDirectory:
		if record.ContentRevision != 0 || record.Size != 0 || record.Hash != "" || record.LinkTarget != "" {
			return invalid("source directory carries content")
		}
	case ObjectKindFile:
		if record.ContentRevision == 0 || record.LinkTarget != "" {
			return invalid("source file content revision is zero")
		}
		if record.Size > uint64(^uint64(0)>>1) {
			return invalid("source file size exceeds int64")
		}
		if err := validateHash(record.Hash); err != nil {
			return err
		}
	case ObjectKindSymlink:
		if err := validateSymlinkContent(record.LinkTarget, record.ContentRevision, record.Size, record.Hash); err != nil {
			return err
		}
	default:
		return invalid("unknown source object kind %q", record.Kind)
	}
	return nil
}

func validateSourceDeleteRecord(record SourceDeleteRecord) error {
	return validateOpaque(record.SourceKey)
}

func validateSourceReconcileRequest(request SourceReconcileRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if request.Mode != SourceModeSnapshot && request.Mode != SourceModeDelta {
		return invalid("unknown source mode %q", request.Mode)
	}
	if err := validateOpaque(string(request.SourceAuthority)); err != nil {
		return err
	}
	if request.SourceRevision == 0 {
		return invalid("source reconciliation revision is zero")
	}
	if request.TenantCount == 0 && request.Mode != SourceModeSnapshot {
		return invalid("zero-tenant source reconciliation is not a snapshot")
	}
	if request.Mode == SourceModeSnapshot && request.PredecessorRevision != 0 {
		return invalid("source snapshot predecessor is not zero")
	}
	if request.Mode == SourceModeDelta && request.PredecessorRevision == 0 {
		return invalid("source delta predecessor is zero")
	}
	if err := validateChangeID(request.ChangeID); err != nil {
		return err
	}
	if err := validateMutationID(request.OperationID); err != nil {
		return err
	}
	if !validConvergenceCause(request.Cause) || request.Cause == ConvergenceCauseOnDemand {
		return invalid("invalid source reconciliation cause %q", request.Cause)
	}
	domainScoped := request.Cause == ConvergenceCauseProviderMutation
	if domainScoped {
		if err := validateDomainID(request.OriginDomain); err != nil {
			return err
		}
		if request.OriginGeneration == 0 {
			return invalid("source provider origin generation is zero")
		}
	} else if request.OriginDomain != "" || request.OriginGeneration != 0 {
		return invalid("non-provider source reconciliation carries an origin")
	}
	if len(request.AffectedKeys) == 0 || !sort.StringsAreSorted(request.AffectedKeys) || hasAdjacentDuplicates(request.AffectedKeys) {
		return invalid("source affected keys are not sorted and unique")
	}
	for _, key := range request.AffectedKeys {
		if err := validateOpaque(key); err != nil {
			return err
		}
	}
	return nil
}

func validateSourceReconcileResponse(response SourceReconcileResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code != ErrorCodeOk {
		if response.SourceAuthority != "" || response.SourceRevision != 0 || response.ChangeID != "" || response.OperationID != "" || len(response.Commits) != 0 {
			return invalid("failed source response carries a result")
		}
		return nil
	}
	if err := validateOpaque(string(response.SourceAuthority)); err != nil {
		return err
	}
	if response.SourceRevision == 0 {
		return invalid("source response is incomplete")
	}
	if err := validateChangeID(response.ChangeID); err != nil {
		return err
	}
	if err := validateMutationID(response.OperationID); err != nil {
		return err
	}
	for index, commit := range response.Commits {
		if err := validateSourceCommit(commit); err != nil {
			return err
		}
		if index > 0 && response.Commits[index-1].TenantID >= commit.TenantID {
			return invalid("source commits are not sorted and unique")
		}
	}
	return nil
}

func validatePrepareTenantRequest(request PrepareTenantRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateDomainID(request.DomainID); err != nil {
		return err
	}
	if err := validateOpaque(string(request.SourceAuthority)); err != nil {
		return err
	}
	if request.Generation == 0 || request.CatalogRevision == 0 || request.SourceRevision == 0 {
		return invalid("prepare revision identity is zero")
	}
	if err := validateChangeID(request.ChangeID); err != nil {
		return err
	}
	return validateMutationID(request.OperationID)
}

func validatePrepareTenantResponse(response PrepareTenantResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code == ErrorCodeOk && response.Proof == nil {
		return invalid("successful prepare has no proof")
	}
	if response.Proof != nil {
		return validatePreparationProof(*response.Proof)
	}
	return nil
}

func validateAckConvergenceRequest(request AckConvergenceRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateDomainID(request.DomainID); err != nil {
		return err
	}
	if err := validateOpaque(string(request.SourceAuthority)); err != nil {
		return err
	}
	if request.Generation == 0 || request.Revision == 0 || request.CatalogRevision == 0 || request.SourceRevision == 0 {
		return invalid("convergence ack generation or revision is zero")
	}
	if err := validateChangeID(request.ChangeID); err != nil {
		return err
	}
	return validateMutationID(request.OperationID)
}

func validateAckConvergenceResponse(response AckConvergenceResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code == ErrorCodeOk && response.Observation == nil {
		return invalid("successful convergence ack has no proof")
	}
	if response.Observation != nil {
		return validateDomainObservation(*response.Observation)
	}
	return nil
}

func validateObjectID(id ObjectID) error     { return validateHexID(string(id), "object id") }
func validateMutationID(id MutationID) error { return validateHexID(string(id), "mutation id") }
func validateChangeID(id ChangeID) error     { return validateHexID(string(id), "change id") }

func validateDomainID(id DomainID) error {
	value := string(id)
	if len(value) != len("fk-")+sha256.Size*2 || !strings.HasPrefix(value, "fk-") {
		return invalid("domain id is not a versioned derived identifier")
	}
	for _, character := range value[len("fk-"):] {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return invalid("domain id is not a versioned derived identifier")
		}
	}
	return nil
}

func validateHexID(value, field string) error {
	if len(value) != 32 {
		return invalid("%s is not 32 lowercase hexadecimal characters", field)
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return invalid("%s is not 32 lowercase hexadecimal characters", field)
		}
	}
	return nil
}

func validateHash(value string) error {
	if len(value) != 64 {
		return invalid("content hash has length %d, want 64", len(value))
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return invalid("content hash is not lowercase hexadecimal")
		}
	}
	return nil
}

func validateOpaque(value string) error {
	if value == "" || len(value) > 255 || strings.ContainsAny(value, "/\\") {
		return invalid("opaque identifier is invalid")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return invalid("opaque identifier is invalid")
		}
	}
	return nil
}

func validateName(name string) error {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return invalid("name is invalid")
	}
	return nil
}

func validateLimit(limit uint32) error {
	if limit == 0 || limit > MaxPageSize {
		return invalid("page limit %d is outside 1..%d", limit, MaxPageSize)
	}
	return nil
}

func validateGeneration(generation uint64) error {
	if generation == 0 {
		return invalid("generation is zero")
	}
	return nil
}

func validatePublicPath(value string) error {
	if hasGroupContainerPath(value) {
		return ErrForbiddenPath
	}
	if !filepath.IsAbs(value) || filepath.Clean(value) != value || !strings.Contains(strings.ToLower(value), "/library/cloudstorage/") {
		return invalid("public presentation path is invalid")
	}
	return nil
}

func signalTargetKey(target SignalTarget) string {
	if target.ParentID == nil {
		return string(target.Kind)
	}
	return string(target.Kind) + ":" + string(*target.ParentID)
}

func hasAdjacentDuplicates(values []string) bool {
	for i := 1; i < len(values); i++ {
		if values[i] == values[i-1] {
			return true
		}
	}
	return false
}

func validErrorCode(value ErrorCode) bool {
	switch value {
	case ErrorCodeOk, ErrorCodeInvalidRequest, ErrorCodeStaleAnchor, ErrorCodeNotFound, ErrorCodeConflict, ErrorCodeQuarantined, ErrorCodeIntegrity, ErrorCodeUnavailable:
		return true
	default:
		return false
	}
}

func validObjectKind(value ObjectKind) bool {
	return value == ObjectKindDirectory || value == ObjectKindFile || value == ObjectKindSymlink
}

func validateLinkTarget(target string) error {
	if target == "" || len(target) > 4096 || !utf8.ValidString(target) || strings.IndexByte(target, 0) >= 0 {
		return invalid("invalid symlink target")
	}
	return nil
}

func validateSymlinkContent(target string, revision, size uint64, hash string) error {
	if err := validateLinkTarget(target); err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(target))
	if revision == 0 || size != uint64(len([]byte(target))) || hash != fmt.Sprintf("%x", digest[:]) {
		return invalid("symlink content is invalid")
	}
	return nil
}
func validChangeKind(value ChangeKind) bool {
	return value == ChangeKindDelete || value == ChangeKindUpsert
}

func validConvergenceCause(value ConvergenceCause) bool {
	switch value {
	case ConvergenceCauseProviderMutation, ConvergenceCauseDaemonWrite, ConvergenceCauseExternalUnattributed, ConvergenceCauseMigration, ConvergenceCauseOnDemand:
		return true
	default:
		return false
	}
}

func validBrokerCommandKind(value BrokerCommandKind) bool {
	switch value {
	case BrokerCommandKindRegisterDomain, BrokerCommandKindRemoveDomain, BrokerCommandKindListDomains, BrokerCommandKindSignalDomain, BrokerCommandKindCutoverDomains:
		return true
	default:
		return false
	}
}

func forwardableOperation(value Operation) bool {
	switch value {
	case OperationCatalogHead, OperationCatalogSnapshot, OperationCatalogChangesSince, OperationCatalogLookup,
		OperationCatalogLookupName, OperationCatalogOpenAt, OperationCatalogMutate, OperationTenantPrepare,
		OperationConvergenceAck:
		return true
	default:
		return false
	}
}

func canonicalJSON(raw []byte) ([]byte, error) {
	if err := inspectJSON(raw); err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, invalid("canonicalize: %v", err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("catalog protocol: canonicalize: %w", err)
	}
	return canonical, nil
}

func inspectJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := inspectJSONValue(decoder); err != nil {
		return err
	}
	return requireEOF(decoder)
}

func inspectJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return invalid("decode JSON: %v", err)
	}
	switch value := token.(type) {
	case json.Delim:
		switch value {
		case '{':
			keys := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return invalid("decode object key: %v", err)
				}
				key, ok := keyToken.(string)
				if !ok {
					return invalid("object key is not a string")
				}
				if _, exists := keys[key]; exists {
					return invalid("duplicate key %q", key)
				}
				keys[key] = struct{}{}
				if err := inspectJSONValue(decoder); err != nil {
					return err
				}
			}
		case '[':
			for decoder.More() {
				if err := inspectJSONValue(decoder); err != nil {
					return err
				}
			}
		default:
			return invalid("unexpected JSON delimiter %q", value)
		}
		if _, err := decoder.Token(); err != nil {
			return invalid("decode closing delimiter: %v", err)
		}
	case string:
		if hasGroupContainerPath(value) {
			return ErrForbiddenPath
		}
	}
	return nil
}

func hasGroupContainerPath(value string) bool {
	lower := strings.ToLower(strings.ReplaceAll(value, "\\", "/"))
	return strings.Contains(lower, "/library/group containers/") || strings.HasPrefix(lower, "library/group containers/")
}

func requireEOF(decoder *json.Decoder) error {
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return invalid("trailing JSON value")
		}
		return invalid("trailing JSON: %v", err)
	}
	return nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidMessage, fmt.Sprintf(format, args...))
}
