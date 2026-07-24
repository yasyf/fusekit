package catalogproto

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

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
	case CriticalObjectRequirement:
		return validateCriticalIdentity(message.LogicalID, message.Role)
	case ResolvedCriticalObjectProof:
		return validateResolvedCriticalObjectProof(message)
	case CriticalReadinessProof:
		return invalid("critical readiness proof requires its preparation envelope")
	case FileProviderLeaseReceipt:
		return validateFileProviderLeaseReceipt(message)
	case TenantPreparationProof:
		return validateTenantPreparationProof(message)
	case PresentationProof:
		return validatePresentationProof(message)
	case MountPresentationProof:
		return validateMountPresentationProof(message)
	case FileProviderPresentationProof:
		return validateFileProviderPresentationProof(message)
	case SignalTarget:
		return validateSignalTarget(message)
	case EnumerationScope:
		return validateEnumerationScope(message)
	case BrokerForwardContext:
		return validateBrokerForwardContext(message)
	case ActivationSourceCause:
		return validateActivationSourceCause(message)
	case ActivationNotification:
		return validateActivationNotification(message)
	case DomainRegistration:
		return validateDomainRegistration(message)
	case RegisteredDomain:
		return validateRegisteredDomain(message)
	case ObservedDomain:
		return validateObservedDomain(message)
	case SourceAuthorityDeclaration:
		return validateSourceAuthorityDeclaration(message)
	case DesiredSourceFleetState:
		return validateDesiredSourceFleetState(message)
	case PublishDesiredSourceFleetRequest:
		return validatePublishDesiredSourceFleetRequest(message)
	case PublishDesiredSourceFleetResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if (message.Code == ErrorCodeOk) != (message.State != nil) {
			return invalid("desired source fleet response state does not match result")
		}
		if message.State != nil {
			return validateDesiredSourceFleetState(*message.State)
		}
		return nil
	case ReadDesiredSourceFleetRequest:
		return validateReadDesiredSourceFleetRequest(message)
	case ReadDesiredSourceFleetResponse:
		return validateReadDesiredSourceFleetResponse(message)
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
	case CommitFileProviderLeaseRequest:
		return validateFileProviderLeaseRequest(message.Protocol, message.Lease, FileProviderLeaseStateCommitted)
	case CommitFileProviderLeaseResponse:
		return validateFileProviderLeaseResponse(message.Protocol, message.Code, message.Message, message.Lease)
	case RenewFileProviderLeaseRequest:
		return validateFileProviderLeaseRequest(message.Protocol, message.Lease, FileProviderLeaseStateCommitted)
	case RenewFileProviderLeaseResponse:
		return validateFileProviderLeaseResponse(message.Protocol, message.Code, message.Message, message.Lease)
	case ReleaseFileProviderLeaseRequest:
		return validateFileProviderLeaseReleaseRequest(message)
	case ReleaseFileProviderLeaseResponse:
		return validateFileProviderLeaseResponse(message.Protocol, message.Code, message.Message, message.Lease)
	case AckActivationRequest:
		return validateAckActivationRequest(message)
	case AckActivationResponse:
		return validateResponse(message.Protocol, message.Code, message.Message)
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
	if len(message) > int(MaxErrorMessageBytes) || !utf8.ValidString(message) {
		return invalid("response message is outside bounds")
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
	if object.Size > uint64(math.MaxInt64) {
		return invalid("object size exceeds signed presentation range")
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
	if proof.Desired != proof.Requested || proof.Observed != proof.Requested ||
		proof.Verified != proof.Requested || proof.Applied != proof.Requested {
		return invalid("catalog preparation lane is not exact")
	}
	return nil
}

func validateTenantPreparationProof(proof TenantPreparationProof) error {
	if err := validateCatalogLaneProof(proof.Catalog); err != nil {
		return err
	}
	if err := validateOpaque(string(proof.SourceAuthority)); err != nil {
		return err
	}
	if err := validatePresentationProof(proof.Presentation); err != nil {
		return err
	}
	tenant, generation := presentationIdentity(proof.Presentation)
	if tenant != proof.Catalog.Tenant || generation != proof.Catalog.Generation {
		return invalid("tenant preparation presentation identity does not match catalog proof")
	}
	if proof.SourceRevision == 0 || proof.CatalogRevision == 0 || proof.Catalog.Requested != proof.CatalogRevision {
		return invalid("tenant preparation proof revisions are incomplete")
	}
	if err := validateChangeID(proof.ChangeID); err != nil {
		return err
	}
	if err := validateOperationID(proof.SourcePublication); err != nil {
		return err
	}
	if err := validateOperationID(proof.OperationID); err != nil {
		return err
	}
	if proof.Presentation.Kind == PresentationKindFileProvider {
		if proof.CriticalReadiness == nil {
			return invalid("File Provider preparation has no critical readiness proof")
		}
		return validateCriticalReadinessProof(*proof.CriticalReadiness, proof)
	}
	if proof.CriticalReadiness != nil {
		return invalid("mount preparation carries a critical readiness proof")
	}
	return nil
}

func validateCriticalReadinessProof(readiness CriticalReadinessProof, preparation TenantPreparationProof) error {
	if err := validateHash(readiness.PolicyDigest); err != nil {
		return err
	}
	if err := validateHash(readiness.ResolutionDigest); err != nil {
		return err
	}
	if readiness.CatalogHead != preparation.CatalogRevision || readiness.SourceRevision != preparation.SourceRevision ||
		readiness.TenantGeneration != preparation.Catalog.Generation || readiness.CatalogHead == 0 || readiness.SourceRevision == 0 {
		return invalid("critical readiness revision fence changed")
	}
	fileProvider := preparation.Presentation.FileProvider
	if fileProvider == nil || readiness.DomainID != fileProvider.DomainID ||
		readiness.PresentationInstanceID != fileProvider.PresentationInstanceID || readiness.RootID != fileProvider.RootID ||
		readiness.ActivationGeneration != fileProvider.ActivationGeneration {
		return invalid("critical readiness presentation fence changed")
	}
	if err := validateFileProviderLeaseReceipt(readiness.Lease); err != nil {
		return err
	}
	if readiness.Lease.State != FileProviderLeaseStateProvisional ||
		readiness.Lease.TenantID != preparation.Catalog.Tenant || readiness.Lease.DomainID != readiness.DomainID ||
		readiness.Lease.Generation != readiness.TenantGeneration || readiness.Lease.RootID != readiness.RootID ||
		readiness.Lease.PresentationInstanceID != readiness.PresentationInstanceID ||
		readiness.Lease.PolicyDigest != readiness.PolicyDigest || readiness.Lease.ResolutionDigest != readiness.ResolutionDigest ||
		readiness.Lease.CatalogHead != readiness.CatalogHead || readiness.Lease.SourceAuthority != preparation.SourceAuthority ||
		readiness.Lease.SourcePublication != preparation.SourcePublication || readiness.Lease.SourceRevision != readiness.SourceRevision ||
		readiness.Lease.ActivationGeneration != readiness.ActivationGeneration {
		return invalid("critical readiness lease fence changed")
	}
	if len(readiness.Objects) == 0 || len(readiness.Objects) > 32 {
		return invalid("critical readiness object count is invalid")
	}
	previous := ""
	for _, object := range readiness.Objects {
		if err := validateCriticalIdentity(object.LogicalID, object.Role); err != nil {
			return err
		}
		if previous != "" && object.LogicalID <= previous {
			return invalid("critical readiness objects are not uniquely ordered")
		}
		previous = object.LogicalID
		if err := validateObjectID(object.ObjectID); err != nil {
			return err
		}
		if object.ObjectRevision == 0 || object.ContentRevision == 0 {
			return invalid("critical readiness object revisions are zero")
		}
		if err := validateHash(object.Hash); err != nil {
			return err
		}
	}
	return nil
}

func validateResolvedCriticalObjectProof(object ResolvedCriticalObjectProof) error {
	if err := validateCriticalIdentity(object.LogicalID, object.Role); err != nil {
		return err
	}
	if err := validateObjectID(object.ObjectID); err != nil {
		return err
	}
	if object.ObjectRevision == 0 || object.ContentRevision == 0 {
		return invalid("critical readiness object revisions are zero")
	}
	return validateHash(object.Hash)
}

func validateFileProviderLeaseReceipt(lease FileProviderLeaseReceipt) error {
	if err := validateOpaque(lease.LeaseID); err != nil {
		return err
	}
	if err := validateOpaque(string(lease.TenantID)); err != nil {
		return err
	}
	if err := validateDomainID(lease.DomainID); err != nil {
		return err
	}
	if lease.Generation == 0 || lease.CatalogHead == 0 || lease.SourceRevision == 0 ||
		lease.ExpiresUnixNano == 0 || lease.ExpiresUnixNano > math.MaxInt64 {
		return invalid("File Provider lease revision or expiry is invalid")
	}
	if err := validateObjectID(lease.RootID); err != nil {
		return err
	}
	if err := validateOpaque(string(lease.PresentationInstanceID)); err != nil {
		return err
	}
	if err := validateHash(lease.PolicyDigest); err != nil {
		return err
	}
	if err := validateHash(lease.ResolutionDigest); err != nil {
		return err
	}
	if err := validateOpaque(string(lease.SourceAuthority)); err != nil {
		return err
	}
	if err := validateOperationID(lease.SourcePublication); err != nil {
		return err
	}
	if err := validateOpaque(lease.ActivationGeneration); err != nil {
		return err
	}
	switch lease.State {
	case FileProviderLeaseStateProvisional:
		if lease.SessionID != "" || lease.ProcessIdentity != "" {
			return invalid("provisional File Provider lease has session identity")
		}
	case FileProviderLeaseStateCommitted:
		if err := validateOpaque(lease.SessionID); err != nil {
			return err
		}
		if lease.ProcessIdentity == "" || len(lease.ProcessIdentity) > 255 || strings.ContainsRune(lease.ProcessIdentity, 0) {
			return invalid("committed File Provider process identity is invalid")
		}
	case FileProviderLeaseStateReleased:
		if (lease.SessionID == "") != (lease.ProcessIdentity == "") {
			return invalid("released File Provider session identity is partial")
		}
	default:
		return invalid("unknown File Provider lease state %q", lease.State)
	}
	return nil
}

func validateFileProviderLeaseRequest(protocol uint16, lease FileProviderLeaseReceipt, state FileProviderLeaseState) error {
	if err := validateProtocol(protocol); err != nil {
		return err
	}
	if err := validateFileProviderLeaseReceipt(lease); err != nil {
		return err
	}
	if lease.State != state {
		return invalid("File Provider lease request state is %q, want %q", lease.State, state)
	}
	return nil
}

func validateFileProviderLeaseReleaseRequest(request ReleaseFileProviderLeaseRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateFileProviderLeaseReceipt(request.Lease); err != nil {
		return err
	}
	if request.Lease.State != FileProviderLeaseStateProvisional && request.Lease.State != FileProviderLeaseStateCommitted &&
		request.Lease.State != FileProviderLeaseStateReleased {
		return invalid("File Provider release state is invalid")
	}
	return nil
}

func validateFileProviderLeaseResponse(
	protocol uint16,
	code ErrorCode,
	message string,
	lease *FileProviderLeaseReceipt,
) error {
	if err := validateResponse(protocol, code, message); err != nil {
		return err
	}
	if (code == ErrorCodeOk) != (lease != nil) {
		return invalid("File Provider lease response receipt does not match result")
	}
	if lease != nil {
		return validateFileProviderLeaseReceipt(*lease)
	}
	return nil
}

func validatePresentationProof(proof PresentationProof) error {
	switch proof.Kind {
	case PresentationKindMount:
		if proof.Mount == nil || proof.FileProvider != nil {
			return invalid("mount presentation proof has the wrong shape")
		}
		return validateMountPresentationProof(*proof.Mount)
	case PresentationKindFileProvider:
		if proof.FileProvider == nil || proof.Mount != nil {
			return invalid("File Provider presentation proof has the wrong shape")
		}
		return validateFileProviderPresentationProof(*proof.FileProvider)
	default:
		return invalid("unknown presentation kind %q", proof.Kind)
	}
}

func validateMountPresentationProof(proof MountPresentationProof) error {
	if err := validateOpaque(string(proof.TenantID)); err != nil {
		return err
	}
	if proof.Generation == 0 {
		return invalid("presentation generation is zero")
	}
	if !filepath.IsAbs(proof.PublicPath) || filepath.Clean(proof.PublicPath) != proof.PublicPath ||
		strings.ContainsRune(proof.PublicPath, 0) {
		return invalid("presentation public path is not exact absolute")
	}
	return validateOpaque(proof.ActivationGeneration)
}

func validateFileProviderPresentationProof(proof FileProviderPresentationProof) error {
	if err := validateMountPresentationProof(MountPresentationProof{
		TenantID: proof.TenantID, Generation: proof.Generation, PublicPath: proof.PublicPath,
		ActivationGeneration: proof.ActivationGeneration,
	}); err != nil {
		return err
	}
	if err := validateDomainID(proof.DomainID); err != nil {
		return err
	}
	if err := validateOpaque(string(proof.PresentationInstanceID)); err != nil {
		return err
	}
	if err := validateObjectID(proof.RootID); err != nil {
		return err
	}
	return validateOpaque(proof.ActivationGeneration)
}

func presentationIdentity(proof PresentationProof) (TenantID, uint64) {
	if proof.Mount != nil {
		return proof.Mount.TenantID, proof.Mount.Generation
	}
	if proof.FileProvider != nil {
		return proof.FileProvider.TenantID, proof.FileProvider.Generation
	}
	return "", 0
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
	if len(request.Payload) == 0 || len(request.Payload) > int(MaxBrokerForwardPayloadBytes) {
		return invalid("broker-forward payload is outside bounds")
	}
	return nil
}

func validateActivationSourceCause(cause ActivationSourceCause) error {
	if err := validateOperationID(cause.PublicationID); err != nil {
		return err
	}
	if err := validateChangeID(cause.ChangeID); err != nil {
		return err
	}
	if cause.SourceRevision == 0 {
		return invalid("activation source revision is zero")
	}
	if err := validateOperationID(cause.OperationID); err != nil {
		return err
	}
	if !validActivationCause(cause.Cause) {
		return invalid("unknown activation cause %q", cause.Cause)
	}
	if err := validateHash(cause.AffectedKeysDigest); err != nil {
		return invalid("activation affected-keys digest: %v", err)
	}
	return nil
}

func validateActivationNotification(notification ActivationNotification) error {
	if err := validateProtocol(notification.Protocol); err != nil {
		return err
	}
	if err := validateActivationChangeID(notification.ActivationChangeID); err != nil {
		return err
	}
	if err := validateOpaque(string(notification.TenantID)); err != nil {
		return err
	}
	if err := validateDomainID(notification.DomainID); err != nil {
		return err
	}
	if notification.Generation == 0 || notification.ActivationRevision == 0 || notification.CatalogHead == 0 {
		return invalid("activation notification revision is zero")
	}
	if err := validateHash(notification.HeadDigest); err != nil {
		return invalid("activation head digest: %v", err)
	}
	if err := validateHash(notification.ProviderFingerprint); err != nil {
		return invalid("activation provider fingerprint: %v", err)
	}
	if len(notification.Causes) == 0 {
		return invalid("activation notification has no causes")
	}
	for index, cause := range notification.Causes {
		if err := validateActivationSourceCause(cause); err != nil {
			return err
		}
		if index > 0 {
			previous := notification.Causes[index-1]
			if cause.SourceRevision < previous.SourceRevision ||
				(cause.SourceRevision == previous.SourceRevision && cause.PublicationID <= previous.PublicationID) {
				return invalid("activation causes are not strictly ordered")
			}
		}
	}
	if notification.TargetCount == 0 {
		return invalid("notification target count is zero")
	}
	if err := validateHash(notification.TargetDigest); err != nil {
		return invalid("notification target digest: %v", err)
	}
	if len(notification.Targets) == 0 || len(notification.Targets) > int(MaxSignalTargets) {
		return invalid("notification signal target count is outside bounds")
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
	if notification.TargetsCoalesced {
		if notification.TargetCount <= uint64(MaxSignalTargets) || len(notification.Targets) != 1 ||
			notification.Targets[0].Kind != SignalTargetKindWorkingSet {
			return invalid("coalesced notification does not carry one coarse working-set target")
		}
	} else if notification.TargetCount != uint64(len(notification.Targets)) {
		return invalid("exact notification target count does not match targets")
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
	if !validTenantAccessMode(registration.AccessMode) {
		return invalid("unknown tenant access mode %q", registration.AccessMode)
	}
	if err := validateOpaque(string(registration.PresentationInstanceID)); err != nil {
		return err
	}
	derived, err := DeriveDomainID(registration.OwnerID, registration.PresentationInstanceID)
	if err != nil {
		return err
	}
	if registration.DomainID != derived {
		return invalid("domain id is not derived from owner and presentation instance")
	}
	if err := validateBoundedText(registration.DisplayName, int(MaxDisplayNameBytes), "domain display name"); err != nil {
		return err
	}
	return nil
}

func validateRegisteredDomain(domain RegisteredDomain) error {
	if err := validateDomainRegistration(DomainRegistration{
		DomainID: domain.DomainID, OwnerID: domain.OwnerID, TenantID: domain.TenantID,
		Generation: domain.Generation, RootID: domain.RootID, AccessMode: domain.AccessMode,
		PresentationInstanceID: domain.PresentationInstanceID, DisplayName: domain.DisplayName,
	}); err != nil {
		return err
	}
	return validatePublicPath(domain.PublicPath)
}

func validateObservedDomain(domain ObservedDomain) error {
	if err := validateObservedDomainID(domain.ObservedID); err != nil {
		return err
	}
	if domain.Managed == nil {
		return nil
	}
	if err := validateRegisteredDomain(*domain.Managed); err != nil {
		return err
	}
	return nil
}

func validateSourceAuthorityDeclaration(declaration SourceAuthorityDeclaration) error {
	if err := validateOpaque(string(declaration.Authority)); err != nil {
		return err
	}
	if err := validateSourceDriverID(declaration.DriverID); err != nil {
		return err
	}
	if len(declaration.DriverConfig) > int(MaxSourceDriverConfigBytes) {
		return invalid("source driver config is outside bounds")
	}
	return validateHash(declaration.DeclarationDigest)
}

func validateDesiredSourceFleetState(state DesiredSourceFleetState) error {
	if err := validateSourceIdentity(state.Owner, "source fleet owner"); err != nil {
		return err
	}
	if state.Generation == 0 || state.AuthorityCount > uint64(MaxSourceFleetDeclarations) {
		return invalid("desired source fleet state is outside bounds")
	}
	if err := validateHash(state.AuthoritiesDigest); err != nil {
		return err
	}
	return validateHash(state.DeclarationsDigest)
}

func validatePublishDesiredSourceFleetRequest(request PublishDesiredSourceFleetRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateSourceIdentity(request.Owner, "source fleet owner"); err != nil {
		return err
	}
	if request.Generation == 0 || request.Generation <= request.ExpectedGeneration ||
		len(request.Declarations) > int(MaxSourceFleetDeclarations) {
		return invalid("desired source fleet request is outside bounds")
	}
	total := 0
	for index, declaration := range request.Declarations {
		if err := validateSourceAuthorityDeclaration(declaration); err != nil {
			return err
		}
		if index != 0 && request.Declarations[index-1].Authority >= declaration.Authority {
			return invalid("source authority declarations are not sorted and unique")
		}
		total += len(declaration.Authority) + len(declaration.DriverID) + len(declaration.DriverConfig) + 32
		if total > int(MaxSourceFleetBytes) {
			return invalid("desired source fleet exceeds byte budget")
		}
	}
	return nil
}

func validateReadDesiredSourceFleetRequest(request ReadDesiredSourceFleetRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateSourceIdentity(request.Owner, "source fleet owner"); err != nil {
		return err
	}
	if request.Limit == 0 || request.Limit > MaxSourceFleetDeclarations {
		return invalid("desired source fleet page limit is outside bounds")
	}
	if request.Generation == 0 {
		if request.SnapshotDigest != nil || request.After != nil {
			return invalid("head desired source fleet request carries a snapshot cursor")
		}
		return nil
	}
	if request.SnapshotDigest == nil {
		return invalid("pinned desired source fleet request has no snapshot digest")
	}
	if err := validateHash(*request.SnapshotDigest); err != nil {
		return err
	}
	if request.After != nil {
		return validateOpaque(string(*request.After))
	}
	return nil
}

func validateReadDesiredSourceFleetResponse(response ReadDesiredSourceFleetResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if (response.Code == ErrorCodeOk) != (response.State != nil) ||
		response.Code != ErrorCodeOk && (len(response.Declarations) != 0 || response.Next != nil) {
		return invalid("desired source fleet page does not match result")
	}
	if response.State != nil {
		if err := validateDesiredSourceFleetState(*response.State); err != nil {
			return err
		}
	}
	if len(response.Declarations) > int(MaxSourceFleetDeclarations) {
		return invalid("desired source fleet page is outside bounds")
	}
	total := 0
	for index, declaration := range response.Declarations {
		if err := validateSourceAuthorityDeclaration(declaration); err != nil {
			return err
		}
		if index != 0 && response.Declarations[index-1].Authority >= declaration.Authority {
			return invalid("desired source fleet page is not ordered")
		}
		total += len(declaration.Authority) + len(declaration.DriverID) + len(declaration.DriverConfig) + 32
		if total > int(MaxSourceFleetBytes) {
			return invalid("desired source fleet page exceeds byte budget")
		}
	}
	if response.Next != nil && (len(response.Declarations) == 0 || response.Declarations[len(response.Declarations)-1].Authority != *response.Next) {
		return invalid("desired source fleet continuation does not match final declaration")
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
		if command.Registration == nil || command.ObservedID != nil || command.Notification != nil || command.AfterObservedID != nil {
			return invalid("register-domain command has the wrong shape")
		}
		return validateDomainRegistration(*command.Registration)
	case BrokerCommandKindRemoveDomain:
		if command.Registration != nil || command.ObservedID == nil || command.Notification != nil || command.AfterObservedID != nil {
			return invalid("remove-domain command has the wrong shape")
		}
		return validateObservedDomainID(*command.ObservedID)
	case BrokerCommandKindListDomains:
		if command.Registration != nil || command.ObservedID != nil || command.Notification != nil {
			return invalid("list-domains command has the wrong shape")
		}
		if command.AfterObservedID != nil {
			return validateObservedDomainID(*command.AfterObservedID)
		}
		return nil
	case BrokerCommandKindSignalDomain:
		if command.Registration != nil || command.ObservedID != nil || command.Notification == nil || command.AfterObservedID != nil {
			return invalid("signal-domain command has the wrong shape")
		}
		return validateActivationNotification(*command.Notification)
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
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted != nil || result.NextAfterObservedID != nil {
			return invalid("failed broker result carries success payload")
		}
		return nil
	}
	switch result.Kind {
	case BrokerCommandKindRegisterDomain:
		if result.Registered == nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted != nil || result.NextAfterObservedID != nil {
			return invalid("register-domain result has the wrong shape")
		}
		return validateRegisteredDomain(*result.Registered)
	case BrokerCommandKindRemoveDomain:
		if result.Registered != nil || result.ConfirmedAbsent == nil || !*result.ConfirmedAbsent || result.Domains != nil || result.SignalAccepted != nil || result.NextAfterObservedID != nil {
			return invalid("remove-domain result does not confirm absence")
		}
	case BrokerCommandKindListDomains:
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains == nil || result.SignalAccepted != nil {
			return invalid("list-domains result has the wrong shape")
		}
		if len(*result.Domains) > int(MaxBrokerDomainPageSize) {
			return invalid("list-domains result exceeds page bound")
		}
		var prior ObservedDomainID
		for index, domain := range *result.Domains {
			if err := validateObservedDomain(domain); err != nil {
				return err
			}
			if index > 0 && domain.ObservedID <= prior {
				return invalid("list-domains result is not sorted and unique")
			}
			prior = domain.ObservedID
		}
		if result.NextAfterObservedID != nil {
			if len(*result.Domains) != int(MaxBrokerDomainPageSize) ||
				(*result.Domains)[len(*result.Domains)-1].ObservedID != *result.NextAfterObservedID {
				return invalid("list-domains result has an invalid continuation cursor")
			}
		}
	case BrokerCommandKindSignalDomain:
		if result.Registered != nil || result.ConfirmedAbsent != nil || result.Domains != nil || result.SignalAccepted == nil || !*result.SignalAccepted || result.NextAfterObservedID != nil {
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
		if change.Revision < response.Floor || change.Revision > response.Head {
			return invalid("change revision falls outside response bounds")
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
	if err := validateMutationRequestID(request.RequestID); err != nil {
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
	if response.RequestID == nil || response.MutationID == nil || response.Revision == 0 {
		return invalid("successful mutation has no identity or revision")
	}
	if err := validateMutationRequestID(*response.RequestID); err != nil {
		return err
	}
	if err := validateMutationID(*response.MutationID); err != nil {
		return err
	}
	target, err := strconv.ParseUint(string(*response.MutationID)[:16], 16, 64)
	if err != nil || target != response.Revision {
		return invalid("mutation id target revision does not match response")
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
	if err := validateGeneration(request.Generation); err != nil {
		return err
	}
	if request.Presentation != PresentationKindMount && request.Presentation != PresentationKindFileProvider {
		return invalid("unknown requested presentation %q", request.Presentation)
	}
	if err := validateOpaque(request.ActivationGeneration); err != nil {
		return err
	}
	if request.Presentation == PresentationKindMount {
		if request.CriticalPolicyDigest != "" || len(request.CriticalObjects) != 0 || request.LeaseID != "" || request.LeaseExpiresUnixNano != 0 {
			return invalid("mount preparation carries critical File Provider policy")
		}
		return nil
	}
	if err := validateHash(request.CriticalPolicyDigest); err != nil {
		return err
	}
	if len(request.CriticalObjects) == 0 || len(request.CriticalObjects) > 32 {
		return invalid("critical object policy count is invalid")
	}
	previous := ""
	for _, object := range request.CriticalObjects {
		if err := validateCriticalIdentity(object.LogicalID, object.Role); err != nil {
			return err
		}
		if previous != "" && object.LogicalID <= previous {
			return invalid("critical object policy is not uniquely ordered")
		}
		previous = object.LogicalID
	}
	if err := validateOpaque(request.LeaseID); err != nil {
		return err
	}
	if request.LeaseExpiresUnixNano == 0 || request.LeaseExpiresUnixNano > math.MaxInt64 {
		return invalid("provisional File Provider lease expiry is invalid")
	}
	return nil
}

func validateCriticalIdentity(logicalID, role string) error {
	if logicalID == "" || len(logicalID) > 255 || !utf8.ValidString(logicalID) || strings.ContainsRune(logicalID, 0) ||
		role == "" || len(role) > 128 || !utf8.ValidString(role) || strings.ContainsRune(role, 0) {
		return invalid("critical logical identity is invalid")
	}
	for _, character := range logicalID + role {
		if unicode.IsControl(character) {
			return invalid("critical logical identity is invalid")
		}
	}
	return nil
}

func validatePrepareTenantResponse(response PrepareTenantResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code == ErrorCodeOk && response.Proof == nil {
		return invalid("successful prepare has no proof")
	}
	if response.Proof != nil {
		return validateTenantPreparationProof(*response.Proof)
	}
	return nil
}

func validateAckActivationRequest(request AckActivationRequest) error {
	if err := validateProtocol(request.Protocol); err != nil {
		return err
	}
	if err := validateActivationChangeID(request.ActivationChangeID); err != nil {
		return err
	}
	if err := validateDomainID(request.DomainID); err != nil {
		return err
	}
	if request.Generation == 0 || request.ActivationRevision == 0 || request.CatalogHead == 0 {
		return invalid("activation acknowledgement revision is zero")
	}
	return validateHash(request.HeadDigest)
}

func validateObjectID(id ObjectID) error { return validateHexID(string(id), "object id") }

func validateMutationRequestID(id MutationRequestID) error {
	return validateHexID(string(id), "mutation request id")
}

func validateMutationID(id MutationID) error {
	return validateHexIDBytes(string(id), "mutation id", 32)
}

func validateOperationID(id OperationID) error {
	return validateHexID(string(id), "operation id")
}

func validateChangeID(id ChangeID) error { return validateHexID(string(id), "change id") }

func validateActivationChangeID(id ActivationChangeID) error {
	return validateHexID(string(id), "activation change id")
}

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

func validateObservedDomainID(id ObservedDomainID) error {
	_, err := DecodeObservedDomainID(id)
	return err
}

func validateHexID(value, field string) error {
	return validateHexIDBytes(value, field, 16)
}

func validateHexIDBytes(value, field string, size int) error {
	characters := size * 2
	if len(value) != characters {
		return invalid("%s is not %d lowercase hexadecimal characters", field, characters)
	}
	for _, character := range value {
		if !strings.ContainsRune("0123456789abcdef", character) {
			return invalid("%s is not %d lowercase hexadecimal characters", field, characters)
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
	if name == "" || len(name) > int(MaxNameBytes) || !utf8.ValidString(name) ||
		name == "." || name == ".." || strings.ContainsAny(name, "/\x00") {
		return invalid("name is invalid")
	}
	for _, character := range name {
		if unicode.IsControl(character) {
			return invalid("name is invalid")
		}
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
	if err := validateBoundedText(value, int(MaxPublicPathBytes), "public presentation path"); err != nil {
		return err
	}
	if hasGroupContainerPath(value) {
		return ErrForbiddenPath
	}
	if !filepath.IsAbs(value) || filepath.Clean(value) != value || !strings.Contains(strings.ToLower(value), "/library/cloudstorage/") {
		return invalid("public presentation path is invalid")
	}
	return nil
}

func validateBoundedText(value string, limit int, field string) error {
	if value == "" || len(value) > limit || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return invalid("%s is outside bounds", field)
	}
	return nil
}

func validateSourceIdentity(value, field string) error {
	return validateBoundedText(value, 255, field)
}

func validateSourceDriverID(value string) error {
	if value == "" || len(value) > int(MaxSourceDriverIDBytes) {
		return invalid("source driver id is outside bounds")
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || character == '.' || character == '_' || character == '-' {
			continue
		}
		return invalid("source driver id is outside bounds")
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

func validTenantAccessMode(value TenantAccessMode) bool {
	return value == TenantAccessModeReadOnly || value == TenantAccessModeReadWrite
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

func validActivationCause(value ActivationCause) bool {
	switch value {
	case ActivationCauseProviderMutation, ActivationCauseDaemonWrite, ActivationCauseExternalUnattributed, ActivationCauseBootstrap:
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
		OperationCatalogLookupName, OperationCatalogOpenAt, OperationCatalogMutate, OperationActivationAck:
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
