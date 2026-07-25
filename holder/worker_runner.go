package holder

import (
	"context"
	"strings"

	"github.com/yasyf/daemonkit/worker"
)

// WorkerRunner executes one bounded disposable command.
type WorkerRunner interface {
	Run(context.Context, worker.CommandRequest) (worker.CommandResult, error)
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

var _ WorkerRunner = (*worker.Pool)(nil)
