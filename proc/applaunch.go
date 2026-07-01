package proc

import "errors"

// ErrAppLaunchUnsupported is the non-darwin refusal from LaunchApp — a
// permanent platform condition; never fold it into a transient "unavailable"
// sentinel, or a platform with no launch path is retried forever.
var ErrAppLaunchUnsupported = errors.New("launching a .app bundle is only supported on macOS")
