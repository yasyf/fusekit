package proc

import "errors"

// ErrAppLaunchUnsupported is the non-darwin refusal from LaunchApp: a signed
// .app bundle (a cask holder, a File Provider companion) is launched via macOS
// `open`, which exists only on macOS. It is a permanent platform condition —
// callers that fold launch failures into a transient "unavailable" sentinel must
// keep this one DISTINCT (never errors.Is-match it) so a platform with no launch
// path at all is not retried like an app that simply has not come up yet.
var ErrAppLaunchUnsupported = errors.New("launching a .app bundle is only supported on macOS")
