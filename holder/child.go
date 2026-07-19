package holder

import (
	"context"
	"io"

	"github.com/yasyf/fusekit/mountmux"
	"github.com/yasyf/fusekit/tenant"
)

// RunChild recognizes and runs one exact FuseKit child mode in the fixed signed executable.
func RunChild(ctx context.Context, arguments []string, stdout io.Writer) (bool, error) {
	if config, recognized, err := mountmux.ParseNativeChildArguments(arguments); recognized {
		if err != nil {
			return true, err
		}
		return true, mountmux.RunNativeChild(ctx, config)
	}
	config, recognized, err := tenant.ParseMaterializationWorkerArguments(arguments)
	if !recognized {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return true, tenant.RunMaterializationWorker(ctx, config, stdout)
}
