package catalogproto

import (
	"encoding/base64"
	"strings"
	"unicode/utf8"
)

const observedDomainIDPrefix = "fp1-"

// EncodeObservedDomainID returns the canonical wire token for an exact File
// Provider domain identifier.
func EncodeObservedDomainID(identifier string) (ObservedDomainID, error) {
	if identifier == "" || len(identifier) > int(MaxObservedDomainIdentifierBytes) || !utf8.ValidString(identifier) {
		return "", invalid("observed domain identifier is outside bounds")
	}
	return ObservedDomainID(observedDomainIDPrefix + base64.RawURLEncoding.EncodeToString([]byte(identifier))), nil
}

// DecodeObservedDomainID returns the exact File Provider domain identifier
// carried by a canonical wire token.
func DecodeObservedDomainID(id ObservedDomainID) (string, error) {
	value := string(id)
	if len(value) > int(MaxObservedDomainIDBytes) || !strings.HasPrefix(value, observedDomainIDPrefix) {
		return "", invalid("observed domain id is outside bounds")
	}
	payload := strings.TrimPrefix(value, observedDomainIDPrefix)
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(payload)
	if err != nil || len(decoded) == 0 || len(decoded) > int(MaxObservedDomainIdentifierBytes) ||
		!utf8.Valid(decoded) || base64.RawURLEncoding.EncodeToString(decoded) != payload {
		return "", invalid("observed domain id is outside bounds")
	}
	return string(decoded), nil
}
