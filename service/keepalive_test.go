package service

import (
	"os"
	"path/filepath"
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
