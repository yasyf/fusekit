//go:build !darwin || !cgo || !fuse

package mountmux

import (
	"context"
	"errors"
)

// ErrNativeMount means native FUSE support is unavailable in this build.
var ErrNativeMount = errors.New("mountmux: native child mount failed")

// RunNativeChild rejects native child mode when the fuse-tagged Darwin backend is absent.
func RunNativeChild(context.Context, NativeChildConfig) error { return ErrNativeMount }
