package holder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/fusekit/mountmux"
)

func runNativeMountProbe(
	ctx context.Context,
	runner supervise.TaskRunner,
	executable string,
	root string,
	token string,
) error {
	if runner == nil {
		return errors.New("holder: native mount probe runner is required")
	}
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable || strings.ContainsRune(executable, 0) {
		return errors.New("holder: native mount probe executable is invalid")
	}
	arguments, err := mountmux.NativeProbeChildArguments(mountmux.NativeProbeChildConfig{
		Root: root, Token: token,
	})
	if err != nil {
		return err
	}
	if err := runner.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          executable,
		Args:          arguments,
		Env:           sanitizedChildEnvironment(os.Environ()),
	}); err != nil {
		return fmt.Errorf("holder: run native mount probe: %w", err)
	}
	return nil
}
