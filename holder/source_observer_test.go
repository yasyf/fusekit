package holder

import (
	"strings"
	"testing"

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
