// Package version holds the build metadata of the binary that links it,
// injected at link time via -ldflags. The values are the CONSUMER's, never
// fusekit's own; fusekit itself never reads them — its module version stays
// off every wire, per the mountd protocol freeze.
//
// A consumer injects them in its release build:
//
//	go build -ldflags "-X github.com/yasyf/fusekit/version.Version=v1.2.3 \
//	                    -X github.com/yasyf/fusekit/version.Commit=abc1234"
package version

import "runtime/debug"

var (
	// Version is the linking binary's semantic version, set by -ldflags at
	// release time.
	Version = "dev"
	// Commit is the linking binary's short git SHA, set by -ldflags at release time.
	Commit = ""
)

// String renders the version line: Version — or the module version go build
// embeds while Version is "dev" — with Commit appended in parens when set.
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
