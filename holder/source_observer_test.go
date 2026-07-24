package holder

import (
	"context"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/fusekit/sourceauthority"
)

func TestSourceProcessLauncherRequiresManagedExactInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		launcher sourceProcessLauncher
		args     []string
		want     string
	}{
		{
			name: "manager", launcher: sourceProcessLauncher{executable: "/fixed/runtime"},
			args: []string{"--child"}, want: "process manager is required",
		},
		{
			name: "executable", launcher: sourceProcessLauncher{manager: &proc.Manager{}},
			args: []string{"--child"}, want: "executable",
		},
		{
			name: "arguments", launcher: sourceProcessLauncher{manager: &proc.Manager{}, executable: "/fixed/runtime"},
			want: "arguments are required",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := test.launcher.LaunchSourceObserver(t.Context(), sourceauthority.ObserverProcessSpec{
				Arguments: test.args,
			})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LaunchSourceObserver = %v, want %q", err, test.want)
			}
		})
	}
}

func TestSourceProcessLauncherRejectsCanceledDispatchBeforeOwnership(t *testing.T) {
	manager, err := proc.NewManager(1, &proc.Reaper{
		Store: &proc.FileStore{Path: t.TempDir() + "/children.db"}, Generation: holderOwnerGeneration("source-launcher-test"),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = (sourceProcessLauncher{
		manager: manager, executable: "/fixed/runtime", signature: proc.SignatureDigest{1},
	}).LaunchSourceTask(ctx, sourceauthority.SourceTaskProcessSpec{Arguments: []string{"--child"}})
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("LaunchSourceTask = %v, want cancellation", err)
	}
}

func TestSourceChildEnvironmentIsSanitized(t *testing.T) {
	t.Setenv("CGOFUSE_LIBFUSE_PATH", "/usr/local/lib/libfuse-t.dylib")
	t.Setenv("FUSEKIT_CHILD_ENV_SENTINEL", "preserved")
	environment := sanitizedChildEnvironment([]string{
		"CGOFUSE_LIBFUSE_PATH=/usr/local/lib/libfuse-t.dylib",
		"FUSEKIT_CHILD_ENV_SENTINEL=preserved",
	})
	if len(environment) != 1 || environment[0] != "FUSEKIT_CHILD_ENV_SENTINEL=preserved" {
		t.Fatalf("sanitized environment = %q", environment)
	}
}
