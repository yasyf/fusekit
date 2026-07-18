//go:build darwin

package mountd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const releasePlist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>CFBundleIdentifier</key><string>com.yasyf.fusekit-holder</string>
  <key>CFBundleName</key><string>fusekit-holder</string>
  <key>CFBundleShortVersionString</key><string>0.38.0</string>
  <key>CFBundleVersion</key><string>123</string>
  <key>LSBackgroundOnly</key><true/>
</dict></plist>
`

func writeAppBundle(t *testing.T, content string) string {
	t.Helper()
	app := filepath.Join(t.TempDir(), "fusekit-holder.app")
	plist := filepath.Join(app, "Contents", "Info.plist")
	if err := os.MkdirAll(filepath.Dir(plist), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(plist, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return app
}

func TestAppSkew(t *testing.T) {
	app := writeAppBundle(t, releasePlist) // installed 0.38.0
	cases := []struct {
		name     string
		compiled string
		want     bool
	}{
		{name: "same version v-prefixed", compiled: "v0.38.0", want: false},
		{name: "same version bare", compiled: "0.38.0", want: false},
		{name: "older build skews to the newer install", compiled: "v0.37.0", want: true},
		{name: "newer build never skews to an older install", compiled: "v0.39.0", want: false},
		{name: "dev build never skews", compiled: "dev", want: false},
		{name: "empty never skews", compiled: "", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			skewed, reason, err := appSkew(tc.compiled, app)()
			if err != nil {
				t.Fatal(err)
			}
			if skewed != tc.want {
				t.Fatalf("skewed = %v, want %v", skewed, tc.want)
			}
			if skewed && (!strings.Contains(reason, "0.38.0") || !strings.Contains(reason, "0.37.0")) {
				t.Fatalf("reason %q does not name both versions", reason)
			}
		})
	}
	// A dev build must not even need the bundle.
	if skewed, _, err := appSkew("dev", filepath.Join(t.TempDir(), "missing.app"))(); skewed || err != nil {
		t.Fatalf("dev against a missing bundle = (%v, %v), want inert", skewed, err)
	}
	// An unreadable bundle fails safe: an error, never a retire.
	if skewed, _, err := appSkew("v0.38.0", filepath.Join(t.TempDir(), "missing.app"))(); skewed || err == nil {
		t.Fatalf("missing bundle = (%v, %v), want (false, error)", skewed, err)
	}
}
