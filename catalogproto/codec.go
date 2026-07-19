package catalogproto

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"unicode"
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
	case ConvergenceNotification:
		return validateConvergenceNotification(message)
	case DomainRegistration:
		return validateDomainRegistration(message)
	case RegisteredDomain:
		return validateRegisteredDomain(message)
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
	if object.Kind == ObjectKindDirectory && (object.ContentRevision != 0 || object.Size != 0 || object.Hash != "") {
		return invalid("directory carries file content")
	}
	if object.Kind == ObjectKindFile && object.ContentRevision == 0 {
		return invalid("file content revision is zero")
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
		Generation: domain.Generation, AccountInstanceID: domain.AccountInstanceID, DisplayName: domain.DisplayName,
	}); err != nil {
		return err
	}
	return validatePublicPath(domain.PublicPath)
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
		if command.Registration == nil || command.DomainID != nil || command.Notification != nil {
			return invalid("register-domain command has the wrong shape")
		}
		return validateDomainRegistration(*command.Registration)
	case BrokerCommandKindRemoveDomain:
		if command.Registration != nil || command.DomainID == nil || command.Notification != nil {
			return invalid("remove-domain command has the wrong shape")
		}
		return validateDomainID(*command.DomainID)
	case BrokerCommandKindListDomains:
		if command.Registration != nil || command.DomainID != nil || command.Notification != nil {
			return invalid("list-domains command has the wrong shape")
		}
		return nil
	case BrokerCommandKindSignalDomain:
		if command.Registration != nil || command.DomainID != nil || command.Notification == nil {
			return invalid("signal-domain command has the wrong shape")
		}
		return validateConvergenceNotification(*command.Notification)
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
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted != nil {
			return invalid("failed broker result carries success payload")
		}
		return nil
	}
	switch result.Kind {
	case BrokerCommandKindRegisterDomain:
		if result.Registered == nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted != nil {
			return invalid("register-domain result has the wrong shape")
		}
		return validateRegisteredDomain(*result.Registered)
	case BrokerCommandKindRemoveDomain:
		if result.Registered != nil || result.ConfirmedAbsent == nil || !*result.ConfirmedAbsent || result.Domains != nil || result.SignalAccepted != nil {
			return invalid("remove-domain result does not confirm absence")
		}
	case BrokerCommandKindListDomains:
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains == nil || result.SignalAccepted != nil {
			return invalid("list-domains result has the wrong shape")
		}
		for _, domain := range *result.Domains {
			if err := validateRegisteredDomain(domain); err != nil {
				return err
			}
		}
	case BrokerCommandKindSignalDomain:
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted == nil || !*result.SignalAccepted {
			return invalid("signal-domain result does not confirm acceptance")
		}
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
		if *request.ObjectKind == ObjectKindDirectory && (request.HasContent || request.ContentRevision != nil) {
			return invalid("directory create carries content")
		}
		if *request.ObjectKind == ObjectKindFile && (!request.HasContent || request.ContentRevision == nil || *request.ContentRevision == 0) {
			return invalid("file create has no content revision")
		}
	case MutationKindRevise:
		if request.ObjectKind != nil || request.ObjectID == nil || request.ParentID == nil || request.TargetID != nil || request.Name == nil || request.Mode == nil {
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
		if request.ObjectKind != nil || request.HasContent || request.ObjectID == nil || request.ParentID != nil || request.TargetID != nil || request.Name != nil || request.Mode != nil || request.ContentRevision != nil {
			return invalid("delete mutation has the wrong shape")
		}
		return validateObjectID(*request.ObjectID)
	case MutationKindReplace:
		if request.ObjectKind != nil || request.ObjectID == nil || request.TargetID == nil {
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

func validatePrepareTenantRequest(request PrepareTenantRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateDomainID(request.DomainID); err != nil {
		return err
	}
	if request.Generation == 0 || request.CatalogRevision == 0 || request.Revision == 0 || request.SourceRevision == 0 {
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
	return value == ObjectKindDirectory || value == ObjectKindFile
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
	case BrokerCommandKindRegisterDomain, BrokerCommandKindRemoveDomain, BrokerCommandKindListDomains, BrokerCommandKindSignalDomain:
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
