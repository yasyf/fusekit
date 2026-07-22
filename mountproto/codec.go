// Package mountproto defines the exact FuseKit tenant-control protocol.
package mountproto

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"unicode"
)

var (
	// ErrInvalidMessage means a protocol value violates the generated schema.
	ErrInvalidMessage = errors.New("mount protocol: invalid message")
	// ErrProtocol means the peer selected a protocol other than the exact protocol.
	ErrProtocol = errors.New("mount protocol: unsupported protocol")
	// ErrForbiddenPath means a request attempted to route Go through an App Group container.
	ErrForbiddenPath = errors.New("mount protocol: app group path forbidden")
)

const (
	// MaxNativeRoutePageSize bounds one native route-page response.
	MaxNativeRoutePageSize  = 32
	maxNativeRoutePageBytes = 24 << 10
	// NativeMountFilesystem is the exact mounted filesystem identity.
	NativeMountFilesystem = "nfs"
)

// NativeMountSource returns FUSE-T's mounted source identity for an exact presentation root.
func NativeMountSource(presentationRoot string) (string, error) {
	if err := validatePath(presentationRoot, "native mount presentation root"); err != nil {
		return "", err
	}
	leaf := filepath.Base(presentationRoot)
	if leaf == "" || leaf == "." || leaf == string(filepath.Separator) {
		return "", invalid("native mount presentation root has no leaf")
	}
	return "fuse-t:/" + leaf, nil
}

// Encode validates and returns the canonical JSON encoding of one protocol value.
func Encode(value any) ([]byte, error) {
	if err := Validate(value); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("mount protocol: encode: %w", err)
	}
	return canonicalJSON(raw)
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
	current := reflect.ValueOf(value)
	for current.Kind() == reflect.Pointer {
		if current.IsNil() {
			return invalid("nil value")
		}
		current = current.Elem()
	}
	switch message := current.Interface().(type) {
	case TenantDefinition:
		return validateDefinition(message)
	case MountRoute:
		return validateMountRoute(message)
	case NativeMountProof:
		return validateNativeMountProof(message)
	case Quarantine:
		return validateQuarantine(message)
	case TenantState:
		return validateState(message)
	case ProvisionTenantRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateDefinition(message.Definition)
	case ProvisionTenantResponse:
		return validateAcknowledgement(message.Protocol, message.Code, message.Message, message.TenantID, message.Generation)
	case ReplaceTenantRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		if err := validateGeneration(message.ExpectedGeneration); err != nil {
			return err
		}
		if err := validateDefinition(message.Definition); err != nil {
			return err
		}
		if message.Definition.Generation <= message.ExpectedGeneration {
			return invalid("replacement generation must exceed expected generation")
		}
		return nil
	case ReplaceTenantResponse:
		return validateAcknowledgement(message.Protocol, message.Code, message.Message, message.TenantID, message.Generation)
	case RemoveTenantRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateGeneration(message.Generation)
	case RemoveTenantResponse:
		if err := validateAcknowledgement(message.Protocol, message.Code, message.Message, message.TenantID, message.Generation); err != nil {
			return err
		}
		if message.Code == ErrorCodeOk && !message.FileProviderAbsent {
			return invalid("successful removal response does not prove File Provider absence")
		}
		if message.Code != ErrorCodeOk && message.FileProviderAbsent {
			return invalid("failed removal response carries File Provider absence proof")
		}
		return nil
	case StateRequest:
		return validateProtocol(message.Protocol)
	case StateResponse:
		return validateStateResponse(message.Protocol, message.Code, message.Message, message.State, false)
	case RuntimeHealthRequest:
		return validateProtocol(message.Protocol)
	case RuntimeHealthResponse:
		return validateRuntimeHealthResponse(message)
	case NativeBindRequest:
		return validateProtocol(message.Protocol)
	case NativeBindResponse:
		return validateResponse(message.Protocol, message.Code, message.Message)
	case NativeReadyRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateNativeMountProof(message.Mount)
	case NativeReadyResponse:
		return validateResponse(message.Protocol, message.Code, message.Message)
	case NativeUnbindRequest:
		return validateProtocol(message.Protocol)
	case NativeUnbindResponse:
		return validateResponse(message.Protocol, message.Code, message.Message)
	case NativeRoutePageRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		if message.Limit == 0 || message.Limit > MaxNativeRoutePageSize {
			return invalid("route page limit is invalid")
		}
		if message.Snapshot == 0 && message.After != "" {
			return invalid("initial route page carries a cursor")
		}
		if message.After != "" {
			return validateRootName(message.After)
		}
		return nil
	case NativeRoutePageResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code != ErrorCodeOk {
			if message.Snapshot != 0 || len(message.Routes) != 0 || message.Next != "" {
				return invalid("failed route page carries snapshot data")
			}
			return nil
		}
		if message.Snapshot == 0 {
			return invalid("successful route page has no snapshot")
		}
		if len(message.Routes) > MaxNativeRoutePageSize {
			return invalid("route page exceeds the item limit")
		}
		for index, route := range message.Routes {
			if err := validateMountRoute(route); err != nil {
				return err
			}
			if index > 0 && strings.Compare(message.Routes[index-1].Name, route.Name) >= 0 {
				return invalid("route page is not strictly name ordered")
			}
		}
		if message.Next != "" {
			if err := validateRootName(message.Next); err != nil {
				return err
			}
			if len(message.Routes) == 0 || message.Next != message.Routes[len(message.Routes)-1].Name {
				return invalid("route page cursor does not match its last route")
			}
		}
		raw, err := json.Marshal(message)
		if err != nil || len(raw) > maxNativeRoutePageBytes {
			return invalid("route page exceeds the encoded budget")
		}
		return nil
	case NativePinRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateRootName(message.Name)
	case NativePinResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code != ErrorCodeOk {
			if message.Token != "" || message.OwnerID != "" || message.Route != nil || message.Definition != nil {
				return invalid("failed pin response carries a pin")
			}
			return nil
		}
		if err := validateOpaque(message.Token, "pin token"); err != nil {
			return err
		}
		if err := validateOpaque(string(message.OwnerID), "owner id"); err != nil {
			return err
		}
		if message.Route == nil || message.Definition == nil {
			return invalid("successful pin response is incomplete")
		}
		if err := validateMountRoute(*message.Route); err != nil {
			return err
		}
		if err := validateDefinition(*message.Definition); err != nil {
			return err
		}
		if message.Route.Generation != message.Definition.Generation {
			return invalid("pin route and definition generation differ")
		}
		return nil
	case NativeReleaseRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateOpaque(message.Token, "pin token")
	case NativeReleaseResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code != ErrorCodeOk {
			if message.Token != "" {
				return invalid("failed release response carries a token")
			}
			return nil
		}
		return validateOpaque(message.Token, "pin token")
	case NativeObject:
		return validateNativeObject(message)
	case NativeSnapshotOpenRequest:
		return validateNativeObjectOpen(message.Protocol, message.TenantID, message.Generation, message.ObjectID, message.Revision)
	case NativeSnapshotOpenResponse:
		return validateNativeHandleObjectResponse(message.Protocol, message.Code, message.Message, message.Handle, message.Object)
	case NativeSnapshotReadRequest:
		return validateNativeReadRequest(message.Protocol, message.Handle, message.Offset, message.Length)
	case NativeSnapshotReadResponse:
		return validateNativeReadResponse(message.Protocol, message.Code, message.Message, message.Data, message.EOF)
	case NativeSnapshotCloseRequest:
		return validateNativeHandleRequest(message.Protocol, message.Handle)
	case NativeSnapshotCloseResponse:
		return validateNativeHandleResponse(message.Protocol, message.Code, message.Message, message.Handle)
	case NativeWriteOpenRequest:
		return validateNativeObjectOpen(message.Protocol, message.TenantID, message.Generation, message.ObjectID, message.Revision)
	case NativeWriteOpenResponse:
		return validateNativeHandleObjectResponse(message.Protocol, message.Code, message.Message, message.Handle, message.Object)
	case NativeWriteReadRequest:
		return validateNativeReadRequest(message.Protocol, message.Handle, message.Offset, message.Length)
	case NativeWriteReadResponse:
		return validateNativeReadResponse(message.Protocol, message.Code, message.Message, message.Data, message.EOF)
	case NativeWriteWriteRequest:
		if err := validateNativeHandleRequest(message.Protocol, message.Handle); err != nil {
			return err
		}
		if message.Offset < 0 || len(message.Data) == 0 || len(message.Data) > maxNativeChunk {
			return invalid("native write range is invalid")
		}
		return nil
	case NativeWriteWriteResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code != ErrorCodeOk && message.Written != 0 {
			return invalid("failed write response carries a byte count")
		}
		if message.Code == ErrorCodeOk && (message.Written == 0 || message.Written > maxNativeChunk) {
			return invalid("successful write response has an invalid byte count")
		}
		return nil
	case NativeWriteTruncateRequest:
		if err := validateNativeHandleRequest(message.Protocol, message.Handle); err != nil {
			return err
		}
		if message.Size < 0 {
			return invalid("native truncate size is negative")
		}
		return nil
	case NativeWriteTruncateResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code != ErrorCodeOk && message.Size != 0 {
			return invalid("failed truncate response carries a size")
		}
		if message.Code == ErrorCodeOk && message.Size < 0 {
			return invalid("successful truncate response has a negative size")
		}
		return nil
	case NativeWriteSyncRequest:
		return validateNativeHandleRequest(message.Protocol, message.Handle)
	case NativeWriteSyncResponse:
		return validateNativeHandleResponse(message.Protocol, message.Code, message.Message, message.Handle)
	case NativeWriteCommitRequest:
		return validateNativeHandleRequest(message.Protocol, message.Handle)
	case NativeWriteCommitResponse:
		if err := validateNativeHandleObjectResponse(message.Protocol, message.Code, message.Message, message.Handle, message.Object); err != nil {
			return err
		}
		if message.Code != ErrorCodeOk {
			if message.MutationID != "" {
				return invalid("failed native commit response carries a mutation id")
			}
			return nil
		}
		return validateHexID(string(message.MutationID), 32, "mutation id")
	case NativeWriteAbortRequest:
		return validateNativeHandleRequest(message.Protocol, message.Handle)
	case NativeWriteAbortResponse:
		return validateNativeHandleResponse(message.Protocol, message.Code, message.Message, message.Handle)
	default:
		return invalid("unsupported value type %T", value)
	}
}

const maxNativeChunk = 1 << 20

func validateNativeObjectOpen(protocol uint16, tenant TenantID, generation uint64, objectID string, revision uint64) error {
	if err := validateProtocol(protocol); err != nil {
		return err
	}
	if err := validateTenantID(tenant); err != nil {
		return err
	}
	if err := validateGeneration(generation); err != nil {
		return err
	}
	if err := validateHexID(objectID, 16, "object id"); err != nil {
		return err
	}
	return validateRevision(revision)
}

func validateNativeHandleObjectResponse(protocol uint16, code ErrorCode, message, handle string, object *NativeObject) error {
	if err := validateResponse(protocol, code, message); err != nil {
		return err
	}
	if code != ErrorCodeOk {
		if handle != "" || object != nil {
			return invalid("failed native handle response carries a handle")
		}
		return nil
	}
	if err := validateOpaque(handle, "native handle"); err != nil {
		return err
	}
	if object == nil {
		return invalid("successful native handle response has no object")
	}
	return validateNativeObject(*object)
}

func validateNativeHandleRequest(protocol uint16, handle string) error {
	if err := validateProtocol(protocol); err != nil {
		return err
	}
	return validateOpaque(handle, "native handle")
}

func validateNativeHandleResponse(protocol uint16, code ErrorCode, message, handle string) error {
	if err := validateResponse(protocol, code, message); err != nil {
		return err
	}
	if code != ErrorCodeOk {
		if handle != "" {
			return invalid("failed native response carries a handle")
		}
		return nil
	}
	return validateOpaque(handle, "native handle")
}

func validateNativeReadRequest(protocol uint16, handle string, offset int64, length uint32) error {
	if err := validateNativeHandleRequest(protocol, handle); err != nil {
		return err
	}
	if offset < 0 || length == 0 || length > maxNativeChunk {
		return invalid("native read range is invalid")
	}
	return nil
}

func validateNativeReadResponse(protocol uint16, code ErrorCode, message string, data []byte, eof bool) error {
	if err := validateResponse(protocol, code, message); err != nil {
		return err
	}
	if code != ErrorCodeOk {
		if len(data) != 0 || eof {
			return invalid("failed native read response carries data")
		}
		return nil
	}
	if len(data) > maxNativeChunk {
		return invalid("native read response exceeds the chunk limit")
	}
	if len(data) == 0 && !eof {
		return invalid("native read response made no progress")
	}
	return nil
}

func validateNativeObject(object NativeObject) error {
	if err := validateHexID(object.ID, 16, "object id"); err != nil {
		return err
	}
	if err := validateHexID(object.ParentID, 16, "parent id"); err != nil {
		return err
	}
	if err := validateOpaque(object.Name, "object name"); err != nil {
		return err
	}
	switch object.Kind {
	case ObjectKindDirectory:
		if object.Size != 0 || object.Hash != "" || object.LinkTarget != "" {
			return invalid("directory object carries file metadata")
		}
	case ObjectKindFile:
		if object.Size < 0 || object.LinkTarget != "" {
			return invalid("file object metadata is invalid")
		}
		if err := validateHexID(object.Hash, 32, "content hash"); err != nil {
			return err
		}
	case ObjectKindSymlink:
		if object.Size != 0 || object.Hash != "" || object.LinkTarget == "" {
			return invalid("symlink object metadata is invalid")
		}
	default:
		return invalid("object kind %q is invalid", object.Kind)
	}
	if err := validateRevision(object.Revision); err != nil {
		return err
	}
	if err := validateRevision(object.MetadataRevision); err != nil {
		return err
	}
	if object.Kind != ObjectKindDirectory && object.ContentRevision == 0 {
		return invalid("object content revision is zero")
	}
	return nil
}

func validateHexID(value string, size int, field string) error {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != size || hex.EncodeToString(decoded) != value {
		return invalid("%s is invalid", field)
	}
	return nil
}

func validateMountRoute(route MountRoute) error {
	if err := validateRootName(route.Name); err != nil {
		return err
	}
	if err := validateTenantID(route.TenantID); err != nil {
		return err
	}
	return validateGeneration(route.Generation)
}

func validateRootName(name string) error {
	if err := validateOpaque(name, "root name"); err != nil {
		return err
	}
	if name == "." || name == ".." {
		return invalid("root name is invalid")
	}
	return nil
}

func validateDefinition(definition TenantDefinition) error {
	if err := validatePath(definition.PresentationRoot, "presentation root"); err != nil {
		return err
	}
	if err := validatePath(definition.BackingRoot, "backing root"); err != nil {
		return err
	}
	if err := validateOpaque(definition.ContentSourceID, "content source id"); err != nil {
		return err
	}
	if definition.AccessMode != AccessModeReadOnly && definition.AccessMode != AccessModeReadWrite {
		return invalid("access mode %q is invalid", definition.AccessMode)
	}
	if definition.CasePolicy != CasePolicySensitive && definition.CasePolicy != CasePolicyInsensitive {
		return invalid("case policy %q is invalid", definition.CasePolicy)
	}
	if len(definition.Presentations) == 0 || len(definition.Presentations) > 2 {
		return invalid("presentations must contain one or two values")
	}
	for index, presentation := range definition.Presentations {
		if presentation != PresentationMount && presentation != PresentationFileProvider {
			return invalid("presentation %q is invalid", presentation)
		}
		if index > 0 && presentationRank(definition.Presentations[index-1]) >= presentationRank(presentation) {
			return invalid("presentations must be unique and schema ordered")
		}
	}
	fileProvider := slices.Contains(definition.Presentations, PresentationFileProvider)
	if fileProvider != (definition.FileProviderAccountID != "" && definition.FileProviderDisplayName != "") {
		return invalid("File Provider metadata does not match presentation set")
	}
	if fileProvider {
		if err := validateOpaque(definition.FileProviderAccountID, "File Provider account id"); err != nil {
			return err
		}
		if err := validateOpaque(definition.FileProviderDisplayName, "File Provider display name"); err != nil {
			return err
		}
	}
	return validateGeneration(definition.Generation)
}

func presentationRank(presentation Presentation) int {
	switch presentation {
	case PresentationMount:
		return 1
	case PresentationFileProvider:
		return 2
	default:
		return 0
	}
}

func validateQuarantine(quarantine Quarantine) error {
	switch quarantine.Lane {
	case QuarantineLaneCatalogMutation, QuarantineLaneMaterialization, QuarantineLaneEnumeration, QuarantineLaneMountLifecycle:
	default:
		return invalid("quarantine lane %q is invalid", quarantine.Lane)
	}
	switch quarantine.Cause {
	case QuarantineCauseConflict, QuarantineCauseIntegrity, QuarantineCauseUnsettled, QuarantineCauseUnavailable:
	default:
		return invalid("quarantine cause %q is invalid", quarantine.Cause)
	}
	if err := validateRevision(quarantine.Revision); err != nil {
		return err
	}
	if quarantine.Detail == "" || len(quarantine.Detail) > 4_096 {
		return invalid("quarantine detail is invalid")
	}
	if quarantine.SinceUnixNano <= 0 {
		return invalid("quarantine timestamp is invalid")
	}
	return nil
}

func validateState(state TenantState) error {
	if err := validateOpaque(string(state.OwnerID), "owner id"); err != nil {
		return err
	}
	if err := validateTenantID(state.TenantID); err != nil {
		return err
	}
	if err := validateGeneration(state.Generation); err != nil {
		return err
	}
	if state.ActivatedGeneration != 0 && state.ActivatedGeneration != state.Generation {
		return invalid("activated generation does not match tenant generation")
	}
	if state.StateVersion == 0 {
		return invalid("state version is zero")
	}
	if state.Quarantine != nil {
		return validateQuarantine(*state.Quarantine)
	}
	return nil
}

func validateAcknowledgement(protocol uint16, code ErrorCode, message string, tenant TenantID, generation uint64) error {
	if err := validateResponse(protocol, code, message); err != nil {
		return err
	}
	if code != ErrorCodeOk {
		if tenant != "" || generation != 0 {
			return invalid("failed acknowledgement carries tenant state")
		}
		return nil
	}
	if err := validateTenantID(tenant); err != nil {
		return err
	}
	return validateGeneration(generation)
}

func validateStateResponse(protocol uint16, code ErrorCode, message string, state *TenantState, prepared bool) error {
	if err := validateResponse(protocol, code, message); err != nil {
		return err
	}
	if code != ErrorCodeOk {
		if state != nil {
			return invalid("failed response carries tenant state")
		}
		return nil
	}
	if state == nil {
		return invalid("successful response has no tenant state")
	}
	if err := validateState(*state); err != nil {
		return err
	}
	if prepared && state.Requested == 0 {
		return invalid("prepared state has no requested revision")
	}
	return nil
}

func validateNativeMountProof(proof NativeMountProof) error {
	if err := validatePath(proof.PresentationRoot, "native mount presentation root"); err != nil {
		return err
	}
	source, err := NativeMountSource(proof.PresentationRoot)
	if err != nil {
		return err
	}
	if proof.Filesystem != NativeMountFilesystem {
		return invalid("native mount filesystem %q is invalid", proof.Filesystem)
	}
	if proof.Source != source {
		return invalid("native mount source %q is invalid", proof.Source)
	}
	if proof.CatalogEpoch == 0 {
		return invalid("native mount catalog epoch is zero")
	}
	return nil
}

func validateRuntimeHealthResponse(response RuntimeHealthResponse) error {
	if err := validateResponse(response.Protocol, response.Code, response.Message); err != nil {
		return err
	}
	if response.Code != ErrorCodeOk {
		if response.ActivationGeneration != "" || response.NativePhase != "" || response.NativeMount != nil {
			return invalid("failed runtime health response carries health state")
		}
		return nil
	}
	if err := validateOpaque(response.ActivationGeneration, "activation generation"); err != nil {
		return err
	}
	switch response.NativePhase {
	case NativePhaseIdle, NativePhaseStarting, NativePhaseLive, NativePhaseFailed, NativePhaseClosing, NativePhaseClosed:
	default:
		return invalid("native phase %q is invalid", response.NativePhase)
	}
	if response.NativeMount != nil {
		if err := validateNativeMountProof(*response.NativeMount); err != nil {
			return err
		}
	}
	if response.NativePhase == NativePhaseLive && response.NativeMount == nil {
		return invalid("live native phase has no mount proof")
	}
	return nil
}

func validateProtocol(protocol uint16) error {
	if protocol != Version {
		return fmt.Errorf("%w: got %d, want %d", ErrProtocol, protocol, Version)
	}
	return nil
}

func validateResponse(protocol uint16, code ErrorCode, message string) error {
	if err := validateProtocol(protocol); err != nil {
		return err
	}
	switch code {
	case ErrorCodeOk, ErrorCodeInvalidRequest, ErrorCodeUnauthorized, ErrorCodeNotFound, ErrorCodeConflict, ErrorCodeQuarantined, ErrorCodeCanceled, ErrorCodeUnavailable:
	default:
		return invalid("error code %q is invalid", code)
	}
	if code == ErrorCodeOk && message != "" {
		return invalid("successful response has an error message")
	}
	if code != ErrorCodeOk && message == "" {
		return invalid("failed response has no error message")
	}
	return nil
}

func validateTenantID(tenant TenantID) error { return validateOpaque(string(tenant), "tenant id") }

func validateOpaque(value, field string) error {
	if value == "" || len(value) > 255 || strings.ContainsAny(value, "/\\") {
		return invalid("%s is invalid", field)
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return invalid("%s is invalid", field)
		}
	}
	return nil
}

func validatePath(path, field string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, '\x00') {
		return invalid("%s is not a clean absolute path", field)
	}
	if hasGroupContainerPath(path) {
		return ErrForbiddenPath
	}
	return nil
}

func validateGeneration(generation uint64) error {
	if generation == 0 {
		return invalid("generation is zero")
	}
	return nil
}

func validateRevision(revision uint64) error {
	if revision == 0 {
		return invalid("revision is zero")
	}
	return nil
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
		return nil, fmt.Errorf("mount protocol: canonicalize: %w", err)
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
