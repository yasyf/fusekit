package mountmux

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
)

const (
	nativeChildMode    = "fusekit-native-v1"
	nativeChildVersion = 1
)

// NativeChildConfig is the exact fixed-app child-mode contract.
type NativeChildConfig struct {
	Socket  string   `json:"socket"`
	Root    string   `json:"root"`
	Options []string `json:"options,omitempty"`
}

type nativeChildEnvelope struct {
	Protocol int               `json:"protocol"`
	Config   NativeChildConfig `json:"config"`
}

// NativeChildArguments encodes one exact native-child invocation.
func NativeChildArguments(config NativeChildConfig) ([]string, error) {
	if err := validateNativeChildConfig(config); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(nativeChildEnvelope{Protocol: nativeChildVersion, Config: config})
	if err != nil {
		return nil, fmt.Errorf("mountmux: encode native child arguments: %w", err)
	}
	return []string{nativeChildMode, base64.RawURLEncoding.EncodeToString(payload)}, nil
}

// ParseNativeChildArguments recognizes and decodes one exact native-child invocation.
func ParseNativeChildArguments(arguments []string) (NativeChildConfig, bool, error) {
	if len(arguments) == 0 || arguments[0] != nativeChildMode {
		return NativeChildConfig{}, false, nil
	}
	if len(arguments) != 2 {
		return NativeChildConfig{}, true, errors.New("mountmux: native child arguments are invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(arguments[1])
	if err != nil {
		return NativeChildConfig{}, true, fmt.Errorf("mountmux: decode native child arguments: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var envelope nativeChildEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return NativeChildConfig{}, true, fmt.Errorf("mountmux: decode native child contract: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return NativeChildConfig{}, true, errors.New("mountmux: native child contract has trailing data")
	}
	if envelope.Protocol != nativeChildVersion {
		return NativeChildConfig{}, true, fmt.Errorf("mountmux: native child protocol %d is unsupported", envelope.Protocol)
	}
	if err := validateNativeChildConfig(envelope.Config); err != nil {
		return NativeChildConfig{}, true, err
	}
	return envelope.Config, true, nil
}

func validateNativeChildConfig(config NativeChildConfig) error {
	socket := filepath.Clean(config.Socket)
	if !filepath.IsAbs(socket) || socket != config.Socket || strings.ContainsRune(socket, 0) {
		return fmt.Errorf("mountmux: native child socket %q is not an exact absolute path", config.Socket)
	}
	root := filepath.Clean(config.Root)
	if !filepath.IsAbs(root) || root != config.Root || strings.ContainsRune(root, 0) {
		return fmt.Errorf("mountmux: native child root %q is not an exact absolute path", config.Root)
	}
	for _, option := range config.Options {
		if option == "" || strings.ContainsRune(option, 0) {
			return errors.New("mountmux: native child mount option is invalid")
		}
	}
	return nil
}
