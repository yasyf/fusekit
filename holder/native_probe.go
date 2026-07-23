package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	stderr io.Writer,
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
	probeID, err := mountmux.NativeProbeID(token)
	if err != nil {
		return err
	}
	writeHolderNativeReadinessEvent(stderr, "probe_task_dispatch", probeID, "begin", 0)
	if err := runner.Run(ctx, supervise.Task{
		RecoveryClass: proc.RecoveryTask,
		Path:          executable,
		Args:          arguments,
		Env:           sanitizedChildEnvironment(os.Environ()),
	}); err != nil {
		writeHolderNativeReadinessEvent(stderr, "probe_task_settled", probeID, "error", 0)
		return fmt.Errorf("holder: run native mount probe: %w", err)
	}
	writeHolderNativeReadinessEvent(stderr, "probe_task_settled", probeID, "ok", 0)
	return nil
}

func writeHolderNativeReadinessEvent(stderr io.Writer, phase, probeID, result string, epoch uint64) {
	if stderr == nil {
		return
	}
	if epoch == 0 {
		_, _ = fmt.Fprintf(stderr, "fusekit.native_readiness phase=%s probe_id=%s result=%s\n", phase, probeID, result)
		return
	}
	_, _ = fmt.Fprintf(
		stderr,
		"fusekit.native_readiness phase=%s probe_id=%s result=%s root_read_epoch=%d\n",
		phase,
		probeID,
		result,
		epoch,
	)
}
