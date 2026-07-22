package holder

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
)

const nativeProbeExecutable = "/bin/ls"

func runNativeMountProbe(ctx context.Context, runner supervise.TaskRunner, root string) error {
	if runner == nil {
		return errors.New("holder: native mount probe runner is required")
	}
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root || strings.ContainsRune(root, 0) {
		return errors.New("holder: native mount probe root is invalid")
	}
	if err := runner.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          nativeProbeExecutable,
		Args:          []string{"-A", "--", root},
	}); err != nil {
		return fmt.Errorf("holder: run native mount probe: %w", err)
	}
	return nil
}
