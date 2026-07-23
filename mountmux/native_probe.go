package mountmux

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	nativeProbeChildMode = "fusekit-native-probe-v1"
	nativeProbePrefix    = "._fusekit-native-readiness-"
	nativeProbeTokenSize = 32
)

// NativeProbeChildConfig is one exact disposable root-probe invocation.
type NativeProbeChildConfig struct {
	Root  string
	Token string
}

// NewNativeProbeToken returns one unguessable single-use readiness token.
func NewNativeProbeToken() (string, error) {
	value := make([]byte, nativeProbeTokenSize)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("mountmux: generate native probe token: %w", err)
	}
	return hex.EncodeToString(value), nil
}

// NativeProbeChildArguments encodes one exact disposable probe invocation.
func NativeProbeChildArguments(config NativeProbeChildConfig) ([]string, error) {
	if err := validateNativeProbeChildConfig(config); err != nil {
		return nil, err
	}
	return []string{nativeProbeChildMode, config.Root, config.Token}, nil
}

// ParseNativeProbeChildArguments recognizes one exact disposable probe invocation.
func ParseNativeProbeChildArguments(arguments []string) (NativeProbeChildConfig, bool, error) {
	if len(arguments) == 0 || arguments[0] != nativeProbeChildMode {
		return NativeProbeChildConfig{}, false, nil
	}
	if len(arguments) != 3 {
		return NativeProbeChildConfig{}, true, errors.New("mountmux: native probe child arguments are invalid")
	}
	config := NativeProbeChildConfig{Root: arguments[1], Token: arguments[2]}
	if err := validateNativeProbeChildConfig(config); err != nil {
		return NativeProbeChildConfig{}, true, err
	}
	return config, true, nil
}

// RunNativeProbeChild performs one lstat through the mounted root and requires ENOENT.
func RunNativeProbeChild(ctx context.Context, config NativeProbeChildConfig) error {
	if err := validateNativeProbeChildConfig(config); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := nativeProbeFilesystemPath(config.Root, config.Token)
	if err != nil {
		return err
	}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("mountmux: lstat native probe path: %w", err)
	}
	return errors.New("mountmux: native probe path unexpectedly exists")
}

func nativeProbeFilesystemPath(root, token string) (string, error) {
	if err := validateNativeProbeToken(token); err != nil {
		return "", err
	}
	return filepath.Join(root, nativeProbePrefix+token), nil
}

func validateNativeProbeChildConfig(config NativeProbeChildConfig) error {
	root := filepath.Clean(config.Root)
	if !filepath.IsAbs(root) || root != config.Root || strings.ContainsRune(root, 0) {
		return errors.New("mountmux: native probe root is invalid")
	}
	return validateNativeProbeToken(config.Token)
}

func validateNativeProbeToken(token string) error {
	value, err := hex.DecodeString(token)
	if err != nil || len(value) != nativeProbeTokenSize || token != strings.ToLower(token) {
		return errors.New("mountmux: native probe token is invalid")
	}
	return nil
}
