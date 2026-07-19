// Package fuset holds the install-time facts about FUSE-T
// (https://github.com/macos-fuse-t/fuse-t), the kext-less macOS FUSE
// implementation cgofuse dlopens: where its library lives, the Homebrew cask
// that installs it, whether it is installed, and how to install it. These are
// macOS-specific and shared by every fusekit consumer that offers to set
// fuse-t up.
//
// Distinct from the runtime library pin (CGOFUSE_LIBFUSE_PATH), which the
// signed native child fixes before cgofuse loads.
package fuset

import (
	"context"
	"io"
	"os"

	"github.com/yasyf/daemonkit/service"
)

// Cask is the Homebrew cask reference that installs fuse-t. fuse-t ships only
// as a cask (never a formula), so a consuming formula cannot depend on it; a
// consumer installs it explicitly via Install.
const Cask = "macos-fuse-t/homebrew-cask/fuse-t"

// Dylib is the path cgofuse dlopens for fuse-t on macOS. A consumer also pins
// it into CGOFUSE_LIBFUSE_PATH so cgofuse loads fuse-t, not a kext-backed
// macFUSE alongside it.
const Dylib = "/usr/local/lib/libfuse-t.dylib"

// Installed reports whether fuse-t's library exists at Dylib — a cheap stat,
// no dlopen or probe mount, so any code path can gate on it. Off macOS it
// answers false.
func Installed() bool { return installed(Dylib) }

func installed(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// FSKitModuleBundle is fuse-t's FSKit module extension, present once the fuse-t
// cask is installed on macOS 26+.
const FSKitModuleBundle = "/Applications/fuse-t.app/Contents/Extensions/FskitSrvModule.appex"

// FSKitAvailable reports whether fuse-t's FSKit backend can be used here: fuse-t
// installed, macOS 26+ (FSKit is macOS-26-only), and the FSKit module bundle on
// disk. It does NOT check whether the user has ENABLED the extension in System
// Settings — no cheap syscall exists, so a mount attempt stays the source of
// truth for enablement. Off macOS it answers false.
func FSKitAvailable() bool { return fskitAvailable() }

// Install installs the fuse-t cask via Homebrew, streaming brew's output to out
// and errOut. It does not re-check Installed afterwards — the caller does that.
func Install(out, errOut io.Writer) error {
	return service.InstallCask(context.Background(), Cask, out, errOut)
}
