//go:build darwin && cgo && fuse

package mountmux

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
)

const nativeLibraryEnvironmentKey = "CGOFUSE_LIBFUSE_PATH"

func validateNativeLibrary(path, digest string) error {
	if err := validateNativeChildConfig(NativeChildConfig{
		Socket: "/private/tmp/fusekit-validation.sock", Root: "/Volumes/FuseKit",
		Library: path, LibrarySHA256: digest,
	}); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect bundled fuse-t library: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("bundled fuse-t library is not a regular non-symlink file")
	}
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open bundled fuse-t library: %w", err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return fmt.Errorf("hash bundled fuse-t library: %w", err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != digest {
		return errors.New("bundled fuse-t library SHA-256 mismatch")
	}
	return nil
}
