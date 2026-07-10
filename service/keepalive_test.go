package service

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// keepAliveGolden is the exact plist AppKeepAlive generates for the shared
// cask holder — a frozen artifact: a failing compare is a behavior change,
// not a literal to update casually. `-W` (block until exit, attach to a
// running instance) is what keeps launchd's KeepAlive from spinning against
// an already-running holder.
const keepAliveGolden = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.yasyf.fusekit-holder</string>
    <key>Program</key>
    <string>/usr/bin/open</string>
    <key>ProgramArguments</key>
    <array>
        <string>/usr/bin/open</string>
        <string>-g</string>
        <string>-W</string>
        <string>/Applications/fusekit-holder.app</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
`

func TestAppKeepAliveGoldenPlist(t *testing.T) {
	k := AppKeepAlive{Label: "com.yasyf.fusekit-holder", AppPath: "/Applications/fusekit-holder.app"}
	body, err := k.plist()
	if err != nil {
		t.Fatalf("plist() = %v", err)
	}
	if string(body) != keepAliveGolden {
		t.Fatalf("rendered plist drifted from the golden artifact:\n--- got ---\n%s\n--- want ---\n%s", body, keepAliveGolden)
	}
}

func TestAppKeepAlivePlistEscapesAppPath(t *testing.T) {
	k := AppKeepAlive{Label: "com.example.holder", AppPath: "/Apps/a&b<c>.app"}
	body, err := k.plist()
	if err != nil {
		t.Fatalf("plist() = %v", err)
	}
	s := string(body)
	if !strings.Contains(s, "<string>/Apps/a&amp;b&lt;c&gt;.app</string>") {
		t.Errorf("app path not XML-escaped:\n%s", s)
	}
	if strings.Contains(s, "a&b<c>") {
		t.Errorf("raw unescaped app path leaked into the plist:\n%s", s)
	}
}

func TestAppKeepAliveValidation(t *testing.T) {
	cases := []struct {
		name    string
		agent   AppKeepAlive
		wantErr string
	}{
		{"empty label", AppKeepAlive{AppPath: "/Applications/x.app"}, "Label is required"},
		{"relative app path", AppKeepAlive{Label: "com.example.x", AppPath: "x.app"}, "must be an absolute"},
		{"empty app path", AppKeepAlive{Label: "com.example.x"}, "must be an absolute"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.agent.plist(); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("plist() err = %v, want it to contain %q", err, tc.wantErr)
			}
			if _, err := tc.agent.WritePlist(); err == nil {
				t.Fatal("WritePlist() accepted an invalid agent")
			}
		})
	}
}

func stubLaunchctl(t *testing.T, fn func(args ...string) (string, error)) {
	t.Helper()
	orig := launchctl
	launchctl = fn
	t.Cleanup(func() { launchctl = orig })
}

// shExit fabricates a genuine *exec.ExitError with the given exit code.
func shExit(t *testing.T, code int) error {
	t.Helper()
	err := exec.Command("/bin/sh", "-c", fmt.Sprintf("exit %d", code)).Run()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != code {
		t.Fatalf("fabricate exit %d: %v", code, err)
	}
	return err
}

func TestAppKeepAliveUninstallBootout(t *testing.T) {
	errDenied := errors.New("bootout: Operation not permitted")
	cases := []struct {
		name       string
		bootoutErr error
		wantGone   bool
	}{
		{"exit 3 not loaded succeeds and removes plist", shExit(t, 3), true},
		{"other exit code fails and keeps plist", shExit(t, 5), false},
		{"non-exit failure fails and keeps plist", errDenied, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			k := AppKeepAlive{Label: "com.example.holder", AppPath: "/Applications/x.app"}
			plist, err := k.WritePlist()
			if err != nil {
				t.Fatalf("WritePlist() = %v", err)
			}
			var gotArgs []string
			stubLaunchctl(t, func(args ...string) (string, error) {
				gotArgs = args
				return "launchctl output", tc.bootoutErr
			})
			err = k.Uninstall()
			if want := []string{"bootout", serviceTarget(k.Label)}; !slices.Equal(gotArgs, want) {
				t.Errorf("launchctl args = %q, want %q", gotArgs, want)
			}
			_, statErr := os.Stat(plist)
			if tc.wantGone {
				if err != nil {
					t.Fatalf("Uninstall() = %v, want nil", err)
				}
				if !os.IsNotExist(statErr) {
					t.Errorf("plist not removed: stat err = %v", statErr)
				}
				return
			}
			if !errors.Is(err, tc.bootoutErr) {
				t.Fatalf("Uninstall() = %v, want errors.Is-wrapped %v", err, tc.bootoutErr)
			}
			if statErr != nil {
				t.Errorf("plist removed despite bootout failure: stat err = %v", statErr)
			}
		})
	}
}

func TestAppKeepAliveInstallEnableBeforeBootstrap(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	k := AppKeepAlive{Label: "com.example.holder", AppPath: "/Applications/x.app"}

	var verbs []string
	stubLaunchctl(t, func(args ...string) (string, error) {
		verbs = append(verbs, args[0])
		return "", nil
	})
	if err := k.Install(); err != nil {
		t.Fatalf("Install() = %v", err)
	}
	if want := []string{"bootout", "enable", "bootstrap", "kickstart"}; !slices.Equal(verbs, want) {
		t.Errorf("launchctl verbs = %q, want %q", verbs, want)
	}

	errDisabled := errors.New("enable: Input/output error")
	verbs = nil
	stubLaunchctl(t, func(args ...string) (string, error) {
		verbs = append(verbs, args[0])
		if args[0] == "enable" {
			return "", errDisabled
		}
		return "", nil
	})
	if err := k.Install(); !errors.Is(err, errDisabled) {
		t.Fatalf("Install() = %v, want errors.Is-wrapped %v", err, errDisabled)
	}
	if want := []string{"bootout", "enable"}; !slices.Equal(verbs, want) {
		t.Errorf("launchctl verbs after enable failure = %q, want %q", verbs, want)
	}
}

func TestAppKeepAliveWritePlist(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	k := AppKeepAlive{Label: "com.yasyf.fusekit-holder", AppPath: "/Applications/fusekit-holder.app"}
	path, err := k.WritePlist()
	if err != nil {
		t.Fatalf("WritePlist() = %v", err)
	}
	if want := filepath.Join(home, "Library", "LaunchAgents", "com.yasyf.fusekit-holder.plist"); path != want {
		t.Errorf("plist path = %q, want %q", path, want)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	if string(body) != keepAliveGolden {
		t.Fatalf("written plist differs from the golden artifact:\n%s", body)
	}
}
