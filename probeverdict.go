package fusekit

import (
	"errors"
	"fmt"
	"io/fs"
)

// Untagged deliberately: consumers classify probe errors in every build
// variant, on both sides of the holder process boundary.

var (
	// ErrProbeMissing means the wedge-probe file is absent (ENOENT) on an
	// existing mount: the mount predates the probe (an old holder). No verdict
	// — never a wedge, so such mounts survive upgrades until remounted.
	ErrProbeMissing = errors.New("probe file missing")

	// ErrProbeDenied means the mount refused the probe with EPERM/EACCES: an
	// orphaned go-nfsv4 whose holder died answers every op that way. A dead
	// verdict, never folded into ErrProbeMissing's no-verdict.
	ErrProbeDenied = errors.New("probe denied by mount")
)

// ProbeOpenVerdict classifies err from opening or statting an existing
// mount's wedge-probe file: ENOENT wraps ErrProbeMissing, EPERM/EACCES wraps
// ErrProbeDenied, anything else (nil included) returns nil — no verdict here,
// the caller owns the error.
func ProbeOpenVerdict(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%w: %v", ErrProbeMissing, err)
	case errors.Is(err, fs.ErrPermission):
		return fmt.Errorf("%w: %v", ErrProbeDenied, err)
	}
	return nil
}
