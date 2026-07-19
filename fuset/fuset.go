// Package fuset holds the install-time facts about FUSE-T
// (https://github.com/macos-fuse-t/fuse-t): where the cask installs its source
// library, whether it is installed, and how to install it. These are
// macOS-specific and shared by every fusekit consumer that packages FUSE-T.
//
// The installed library is packaging input, not a runtime load path. The
// signed application embeds the reviewed library in Contents/Frameworks, and
// the signed native child pins that exact bundled path before cgofuse loads.
package fuset

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const installOutputLimit = 1 << 20

var errInstallOutputLimit = errors.New("fuset: install output exceeded limit")

// Cask is the Homebrew cask reference that installs fuse-t. fuse-t ships only
// as a cask (never a formula), so a consuming formula cannot depend on it; a
// consumer installs it explicitly via Install.
const Cask = "macos-fuse-t/homebrew-cask/fuse-t"

// CaskVersion is the exact reviewed FUSE-T cask artifact version.
const CaskVersion = "1.2.7"

// CaskDylib is the install-time reviewed regular file supplied by the FUSE-T
// cask. The unversioned path is a symlink and is intentionally never accepted
// as packaging input. Runtime children never load either cask path.
const CaskDylib = "/usr/local/lib/libfuse-t-" + CaskVersion + ".dylib"

// Installed reports whether fuse-t's source library exists at CaskDylib — a
// cheap stat, no dlopen or probe mount, so any code path can gate on it. Off
// macOS it answers false.
func Installed() bool { return installed(CaskDylib) }

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

// Install installs the fuse-t cask through the holder-owned disposable task
// runner, streaming brew's bounded output to out and errOut. It does not
// re-check Installed afterwards — the caller does that.
func Install(
	ctx context.Context,
	runner supervise.TaskRunner,
	out, errOut io.Writer,
) error {
	if runner == nil {
		return errors.New("fuset: disposable task runner is required")
	}
	brew, err := exec.LookPath("brew")
	if err != nil {
		return fmt.Errorf("fuset: find brew: %w", err)
	}
	return install(ctx, runner, brew, out, errOut)
}

func install(
	ctx context.Context,
	runner supervise.TaskRunner,
	brew string,
	out, errOut io.Writer,
) error {
	if runner == nil {
		return errors.New("fuset: disposable task runner is required")
	}
	if !filepath.IsAbs(brew) || filepath.Clean(brew) != brew {
		return fmt.Errorf("fuset: brew path %q is not exact and absolute", brew)
	}
	stdout := &boundedInstallOutput{writer: out, remaining: installOutputLimit}
	stderr := &boundedInstallOutput{writer: errOut, remaining: installOutputLimit}
	runErr := runner.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          brew,
		Args:          []string{"install", "-y", "--cask", Cask},
		Env:           installEnvironment(os.Environ()),
		Stdout:        stdout,
		Stderr:        stderr,
	})
	return errors.Join(runErr, stdout.Err(), stderr.Err())
}

func installEnvironment(environment []string) []string {
	const nativeLibrary = "CGOFUSE_LIBFUSE_PATH="
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		if !strings.HasPrefix(entry, nativeLibrary) {
			result = append(result, entry)
		}
	}
	return result
}

type boundedInstallOutput struct {
	mu        sync.Mutex
	writer    io.Writer
	remaining int
	overflow  bool
	err       error
}

func (w *boundedInstallOutput) Write(payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	accepted := min(len(payload), w.remaining)
	w.remaining -= accepted
	if accepted != len(payload) {
		w.overflow = true
	}
	if accepted != 0 && w.writer != nil {
		written, err := w.writer.Write(payload[:accepted])
		if written != accepted && err == nil {
			err = io.ErrShortWrite
		}
		w.err = errors.Join(w.err, err)
	}
	return len(payload), nil
}

func (w *boundedInstallOutput) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.overflow {
		return errors.Join(w.err, errInstallOutputLimit)
	}
	return w.err
}
