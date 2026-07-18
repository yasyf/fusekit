//go:build !darwin

package mountd

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
)

func TestEnsureRunningAppLaunchUnsupportedClassifiedHolderUnavailable(t *testing.T) {
	exe := filepath.Join(t.TempDir(), "fusekit-holder.app", "Contents", "MacOS", "fusekit-holder")
	if err := os.MkdirAll(filepath.Dir(exe), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(exe, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(shortSockDir(t), "m.sock")

	err := (Spawn{Socket: socket, ExecPath: exe, Timeout: time.Second}).EnsureRunning()
	if !errors.Is(err, proc.ErrAppLaunchUnsupported) {
		t.Errorf("error = %v, want errors.Is ErrAppLaunchUnsupported", err)
	}
	if !errors.Is(err, ErrHolderUnavailable) {
		t.Errorf("error = %v, want errors.Is ErrHolderUnavailable", err)
	}
}
