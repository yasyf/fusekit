// Package version holds the build metadata of the binary that links it,
// injected at link time via -ldflags. The values are the CONSUMER's, never
// fusekit's own: a consumer that supervises a versioned child (proc.Supervisor,
// mountd.Server) needs one canonical version string, and every fusekit consumer
// would otherwise re-derive the same ldflags-and-BuildInfo dance. fusekit
// itself never reads these — its own module version stays off every wire, per
// the mountd protocol freeze.
//
// A consumer injects them in its release build:
//
//	go build -ldflags "-X github.com/yasyf/fusekit/version.Version=v1.2.3 \
//	                    -X github.com/yasyf/fusekit/version.Commit=abc1234"
package version

import "runtime/debug"

var (
	// Version is the semantic version of the linking binary, set by -ldflags at
	// release time. It falls back to the module version go build embeds (see
	// String) when left unset.
	Version = "dev"
	// Commit is the short git SHA of the linking binary, set by -ldflags at
	// release time. Empty when unset.
	Commit = ""
)

// String renders a human-readable version line: Version — falling back to the
// module version go build embeds when Version is still "dev" — with the commit
// appended in parentheses when set.
func String() string {
	v := Version
	if v == "dev" {
		if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
			v = bi.Main.Version
		}
	}
	if Commit != "" {
		v += " (" + Commit + ")"
	}
	return v
}
