package holder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/worker"
	"github.com/yasyf/fusekit/mountmux"
)

const nativeProbeTotalTimeout = 15 * time.Second

func runNativeMountProbe(
	ctx context.Context,
	runner workerRunner,
	executable string,
	root string,
	token string,
	stderr io.Writer,
) error {
	if runner == nil {
		return errors.New("FuseKit runtime: native mount probe runner is required")
	}
	if !filepath.IsAbs(executable) || filepath.Clean(executable) != executable || strings.ContainsRune(executable, 0) {
		return errors.New("FuseKit runtime: native mount probe executable is invalid")
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
	if _, err := runner.Run(ctx, worker.CommandRequest{
		Path: executable, Dir: "/", Args: arguments,
		Env: workerChildEnvironment(os.Environ()), TotalTimeout: nativeProbeTotalTimeout,
	}); err != nil {
		writeHolderNativeReadinessEvent(stderr, "probe_task_settled", probeID, "error", 0)
		return fmt.Errorf("FuseKit runtime: run native mount probe: %w", err)
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
