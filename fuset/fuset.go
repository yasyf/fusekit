// Package fuset holds the install-time facts about FUSE-T
// (https://github.com/macos-fuse-t/fuse-t), the kext-less macOS FUSE
// implementation cgofuse dlopens: where its library lives, the Homebrew cask
// that installs it, whether it is installed, and how to install it. These are
// macOS-specific and shared by every fusekit consumer that offers to set
// fuse-t up, so they live here rather than being re-derived per consumer.
//
// This is distinct from the RUNTIME library pin (CGOFUSE_LIBFUSE_PATH), which
// stays consumer-side because it is per-platform (libfuse-t on macOS, libfuse3
// on Linux) — see the package comment in mount.go. A consumer pins Dylib into
// that variable itself; fuset only states the facts.
package fuset

import (
	"io"
	"os"

	"github.com/yasyf/fusekit/service"
)

// Cask is the Homebrew cask reference that installs fuse-t. `brew install
// --cask <Cask>` auto-taps macos-fuse-t/homebrew-cask. fuse-t ships only as a
// cask (never a formula), which is why a consuming formula cannot depend on it
// and a consumer installs it explicitly via Install instead.
const Cask = "macos-fuse-t/homebrew-cask/fuse-t"

// Dylib is the path cgofuse dlopens for fuse-t on macOS. A consumer also pins
// it into CGOFUSE_LIBFUSE_PATH so cgofuse loads fuse-t rather than a
// kext-backed macFUSE that may sit alongside it.
const Dylib = "/usr/local/lib/libfuse-t.dylib"

// Installed reports whether fuse-t is present on this machine — its library
// exists at Dylib. It is a cheap stat (no dlopen, no probe mount), so any code
// path can gate on it. Off macOS it answers false.
func Installed() bool { return installed(Dylib) }

func installed(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// FSKitModuleBundle is fuse-t's FSKit module extension, present once the fuse-t
// cask is installed on macOS 26+. FSKitAvailable stats it. fuse-t hosts the
// FSKit filesystem in this bundle, registered with the system's fskitd.
const FSKitModuleBundle = "/Applications/fuse-t.app/Contents/Extensions/FskitSrvModule.appex"

// FSKitAvailable reports whether fuse-t's FSKit backend can be used on this
// machine: fuse-t is installed, the OS is macOS 26+ (FSKit is macOS-26-only),
// and fuse-t's FSKit module bundle is present on disk. It does NOT check whether
// the user has ENABLED the extension in System Settings — there is no cheap
// syscall for that, so a mount attempt remains the source of truth for
// enablement. Off macOS it answers false.
func FSKitAvailable() bool { return fskitAvailable() }

// Install installs the fuse-t cask via Homebrew, streaming brew's output to out
// and errOut. It errors when Homebrew is absent or the install fails. It does
// not re-check Installed afterwards — the caller does that when it matters.
func Install(out, errOut io.Writer) error {
	return service.InstallCask(Cask, out, errOut)
}
