//go:build !darwin && !linux

// Package presentationroot owns the exact native presentation-root invariant.
package presentationroot

import (
	"errors"
	"fmt"
	"runtime"
)

// ErrInvalid means the native presentation root is not safe to mount.
var ErrInvalid = errors.New("presentation root is invalid")

// Prepare rejects platforms without an exact mount-table and ownership implementation.
func Prepare(string) error {
	return fmt.Errorf("%w: unsupported platform %s", ErrInvalid, runtime.GOOS)
}

// Validate rejects platforms without an exact mount-table and ownership implementation.
func Validate(string) error {
	return fmt.Errorf("%w: unsupported platform %s", ErrInvalid, runtime.GOOS)
}
