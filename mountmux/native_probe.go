package mountmux

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

// NativeProbeID derives a non-secret correlation identifier from one probe token.
func NativeProbeID(token string) (string, error) {
	if err := validateNativeProbeToken(token); err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(token))
	return hex.EncodeToString(digest[:]), nil
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
	return runNativeProbeChild(ctx, config, os.Lstat, os.Stderr)
}

func runNativeProbeChild(
	ctx context.Context,
	config NativeProbeChildConfig,
	lstat func(string) (os.FileInfo, error),
	log io.Writer,
) error {
	if err := validateNativeProbeChildConfig(config); err != nil {
		return err
	}
	if lstat == nil {
		return errors.New("mountmux: native probe lstat is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	probeID, err := NativeProbeID(config.Token)
	if err != nil {
		return err
	}
	writeNativeReadinessEvent(log, "probe_child_start", probeID, "begin", 0)
	path, err := nativeProbeFilesystemPath(config.Root, config.Token)
	if err != nil {
		return err
	}
	if _, err := lstat(path); errors.Is(err, os.ErrNotExist) {
		writeNativeReadinessEvent(log, "probe_child_lstat", probeID, "enoent", 0)
		return nil
	} else if err != nil {
		writeNativeReadinessEvent(log, "probe_child_lstat", probeID, "error", 0)
		return fmt.Errorf("mountmux: lstat native probe path: %w", err)
	}
	writeNativeReadinessEvent(log, "probe_child_lstat", probeID, "exists", 0)
	return errors.New("mountmux: native probe path unexpectedly exists")
}

func writeNativeReadinessEvent(log io.Writer, phase, probeID, result string, epoch uint64) {
	if log == nil {
		return
	}
	if epoch == 0 {
		_, _ = fmt.Fprintf(log, "fusekit.native_readiness phase=%s probe_id=%s result=%s\n", phase, probeID, result)
		return
	}
	_, _ = fmt.Fprintf(
		log,
		"fusekit.native_readiness phase=%s probe_id=%s result=%s root_read_epoch=%d\n",
		phase,
		probeID,
		result,
		epoch,
	)
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
