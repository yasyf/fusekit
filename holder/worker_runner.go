package holder

import (
	"context"
	"strings"

	"github.com/yasyf/daemonkit"
)

// workerRunner is the disposable-command lane. daemonkit.Ctx and
// daemonkit.Owned share it, so a product running under Serve and a scope opened
// with OwnProcesses drive the same code.
type workerRunner interface {
	Run(context.Context, daemonkit.Cmd) (daemonkit.RunResult, error)
}

func workerChildEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, ok := strings.Cut(entry, "=")
		if ok && (key == "PATH" || key == "LANG" || key == "CGOFUSE_LIBFUSE_PATH") {
			continue
		}
		result = append(result, entry)
	}
	return result
}

var (
	_ workerRunner = daemonkit.Ctx{}
	_ workerRunner = (*daemonkit.Owned)(nil)
)
