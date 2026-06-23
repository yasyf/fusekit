package service

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestStatusLines(t *testing.T) {
	cases := []struct {
		id   string
		got  []string
		want []string
	}{
		{
			id:   "brew managed with info",
			got:  brewStatus("cc-pool (homebrew.mxcl.cc-pool)\nRunning: ✔", true),
			want: []string{"Management: Homebrew (brew services)", "cc-pool (homebrew.mxcl.cc-pool)\nRunning: ✔"},
		},
		{
			id:   "brew managed but info unavailable",
			got:  brewStatus("", false),
			want: []string{"Management: Homebrew (brew services)"},
		},
		{
			id:   "self managed and loaded",
			got:  []string{selfStatus(true)},
			want: []string{"Management: self-managed LaunchAgent (loaded: true)"},
		},
		{
			id:   "self managed and not loaded",
			got:  []string{selfStatus(false)},
			want: []string{"Management: self-managed LaunchAgent (loaded: false)"},
		},
	}
	for _, tc := range cases {
		if !slices.Equal(tc.got, tc.want) {
			t.Errorf("%s: got %q, want %q", tc.id, tc.got, tc.want)
		}
	}
}

func TestAgentPathIsBrewManaged(t *testing.T) {
	t.Setenv("HOMEBREW_PREFIX", "/opt/homebrew")
	a := Agent{Formula: "cc-pool"}
	cases := []struct {
		path string
		want bool
	}{
		{path: "/opt/homebrew/Cellar/cc-pool/1.2.3/bin/cc-pool", want: true}, // versioned Cellar
		{path: "/opt/homebrew/opt/cc-pool/bin/cc-pool", want: true},          // opt symlink tree
		{path: "/opt/homebrew/bin/cc-pool", want: true},                      // brew bin symlink
		{path: "/Users/x/go/bin/cc-pool", want: false},                       // go install
		{path: "/usr/local/bin/other-tool", want: false},                     // unrelated binary
	}
	for _, tc := range cases {
		if got := a.pathIsBrewManaged(tc.path); got != tc.want {
			t.Errorf("pathIsBrewManaged(%q) = %v, want %v", tc.path, got, tc.want)
		}
	}
}

func TestBrewPrefixesHonorsEnv(t *testing.T) {
	t.Setenv("HOMEBREW_PREFIX", "/custom/brew")
	if got := brewPrefixes(); len(got) != 1 || got[0] != "/custom/brew" {
		t.Errorf("brewPrefixes() = %v, want [/custom/brew]", got)
	}
}

func TestWritePlistRendersAgent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	logPath := filepath.Join(home, ".cc-pool", "daemon.log")
	a := Agent{
		Label:   "com.yasyf.cc-pool",
		Formula: "cc-pool",
		Program: "/opt/homebrew/bin/cc-pool",
		Args:    []string{"daemon"},
		LogPath: logPath,
		Env: map[string]string{
			"PATH":      "/usr/bin",
			"AMPERSAND": "a&b<c", // must be XML-escaped, not emitted raw
		},
	}
	path, err := a.WritePlist()
	if err != nil {
		t.Fatalf("WritePlist() = %v", err)
	}
	if want := filepath.Join(home, "Library", "LaunchAgents", "com.yasyf.cc-pool.plist"); path != want {
		t.Errorf("plist path = %q, want %q", path, want)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read plist: %v", err)
	}
	s := string(body)
	for _, want := range []string{
		"<string>com.yasyf.cc-pool</string>",
		"<string>/opt/homebrew/bin/cc-pool</string>",
		"<string>daemon</string>",
		"<string>" + logPath + "</string>",
		"<key>PATH</key>",
		"<string>/usr/bin</string>",
		"<key>KeepAlive</key>",
		"a&amp;b&lt;c", // escaping happened
	} {
		if !strings.Contains(s, want) {
			t.Errorf("rendered plist missing %q\n---\n%s", want, s)
		}
	}
	if strings.Contains(s, "a&b<c") {
		t.Errorf("rendered plist contains an unescaped env value\n---\n%s", s)
	}
	if _, err := os.Stat(filepath.Dir(logPath)); err != nil {
		t.Errorf("log dir was not created: %v", err)
	}
}
