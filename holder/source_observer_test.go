package holder

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/internal/recoveryid"
	"github.com/yasyf/fusekit/sourceauthority"
)

// TestSourceChildProcessStopSettlesUnderItsOwnBudget proves the detached stop
// states its own budget: it runs on context.Background(), which daemonkit's
// Stop refuses, and the stopOnce latches that refusal — no later
// correctly-bounded Stop could ever settle the child.
func TestSourceChildProcessStopSettlesUnderItsOwnBudget(t *testing.T) {
	owner := testManagedProcessOwner(t)
	child, err := owner.spawn(managedProcessTestContext(t), managedSpawnConfig{
		id: recoveryid.SourceObserver,
		cmd: daemonkit.Cmd{
			Path: "/bin/sleep", Args: []string{"120"},
			Exec: daemonkit.ServingSameUser(), Session: true,
		},
		channel: daemonkit.ChannelNone,
	}, io.Discard)
	if err != nil {
		t.Fatalf("spawn source child: %v", err)
	}
	process := &sourceChildProcess{child: child, stopDone: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	if err := process.Stop(ctx); err != nil {
		t.Fatalf("stop source child: %v", err)
	}
	if _, ok := child.Exit(); !ok {
		t.Fatal("stopped source child has no exit result")
	}
}

func TestSourceProcessLauncherRequiresManagedExactInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		launcher sourceProcessLauncher
		args     []string
		want     string
	}{
		{
			name: "owner", launcher: sourceProcessLauncher{executable: "/fixed/runtime"},
			args: []string{"--child"}, want: "source child process owner is required",
		},
		{
			name: "executable", launcher: sourceProcessLauncher{owner: &processOwner{}},
			args: []string{"--child"}, want: "source child executable",
		},
		{
			name:     "arguments",
			launcher: sourceProcessLauncher{owner: &processOwner{}, executable: "/fixed/runtime"},
			want:     "source child arguments are required",
		},
	}
	for _, test := range tests {
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
