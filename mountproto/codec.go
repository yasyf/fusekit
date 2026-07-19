// Package mountproto defines the exact FuseKit tenant-control protocol.
package mountproto

import (
	"bytes"
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
		return validateAcknowledgement(message.Protocol, message.Code, message.Message, message.TenantID, message.Generation)
	case StateRequest:
		if err := validateProtocol(message.Protocol); err != nil {
			return err
		}
		return validateGeneration(message.Generation)
	case StateResponse:
		return validateStateResponse(message.Protocol, message.Code, message.Message, message.State, false)
	case NativeBindRequest:
		return validateProtocol(message.Protocol)
	case NativeBindResponse:
		return validateResponse(message.Protocol, message.Code, message.Message)
	case NativeReadyRequest:
		return validateProtocol(message.Protocol)
	case NativeReadyResponse:
		return validateResponse(message.Protocol, message.Code, message.Message)
	case NativeRoutesRequest:
		return validateProtocol(message.Protocol)
	case NativeRoutesResponse:
		if err := validateResponse(message.Protocol, message.Code, message.Message); err != nil {
			return err
		}
		if message.Code != ErrorCodeOk && len(message.Routes) != 0 {
			return invalid("failed routes response carries routes")
		}
		for _, route := range message.Routes {
			if err := validateMountRoute(route); err != nil {
				return err
			}
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
	default:
		return invalid("unsupported value type %T", value)
	}
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
