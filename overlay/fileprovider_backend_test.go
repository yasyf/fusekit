package overlay

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestParseFileProvider pins that Parse accepts the new fileprovider value and
// still rejects unknowns strictly.
func TestParseFileProvider(t *testing.T) {
	got, err := Parse("fileprovider")
	if err != nil {
		t.Fatalf("Parse(fileprovider) = %v, want nil", err)
	}
	if got != BackendFileProvider {
		t.Errorf("Parse(fileprovider) = %q, want %q", got, BackendFileProvider)
	}
	// Strictness is preserved: a near-miss still errors.
	if _, err := Parse("FileProvider"); err == nil {
		t.Error("Parse(FileProvider) = nil error, want ErrUnknownBackend (case-sensitive)")
	}
}

// TestFileProviderAvailable pins the spec-routed availability gate via the
// enabled-check seam: nil wiring and an empty bundle id are unavailable; a wired
// spec defers to the seam (enabled vs disabled).
func TestFileProviderAvailable(t *testing.T) {
	cases := map[string]struct {
		spec    Spec
		enabled bool
		want    bool
	}{
		"nil file provider is unavailable": {
			spec: Spec{FileProvider: nil}, enabled: true, want: false,
		},
		"empty bundle id is unavailable": {
			spec:    Spec{FileProvider: &FileProviderSpec{ExtensionBundleID: ""}},
			enabled: true, want: false,
		},
		"wired and enabled is available": {
			spec:    Spec{FileProvider: &FileProviderSpec{ExtensionBundleID: "com.example.fp"}},
			enabled: true, want: true,
		},
		"wired but disabled is unavailable": {
			spec:    Spec{FileProvider: &FileProviderSpec{ExtensionBundleID: "com.example.fp"}},
			enabled: false, want: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			prev := fileProviderEnabled
			var queried string
			fileProviderEnabled = func(id string) bool { queried = id; return tc.enabled }
			defer func() { fileProviderEnabled = prev }()

			if got := FileProviderAvailable(tc.spec); got != tc.want {
				t.Errorf("FileProviderAvailable = %v, want %v", got, tc.want)
			}
			// The seam is consulted ONLY with a non-empty bundle id.
			if tc.spec.FileProvider != nil && tc.spec.FileProvider.ExtensionBundleID != "" {
				if queried != tc.spec.FileProvider.ExtensionBundleID {
					t.Errorf("seam queried %q, want the spec bundle id %q", queried, tc.spec.FileProvider.ExtensionBundleID)
				}
			} else if queried != "" {
				t.Errorf("seam queried %q, want no query for nil/empty wiring", queried)
			}
		})
	}
}

// TestFileProviderEnablement pins the FP enablement copy: a needed grant naming
// the File Providers pane with deep-link URLs, and OpenSettings driving them.
func TestFileProviderEnablement(t *testing.T) {
	en := BackendFileProvider.Enablement()
	if !en.Needed || en.Pane == "" || en.Guidance == "" || len(en.URLs) == 0 {
		t.Fatalf("fileprovider Enablement() incomplete: %+v", en)
	}
	if !strings.Contains(en.Pane, "File Providers") {
		t.Errorf("fileprovider Enablement().Pane = %q, want it to name File Providers", en.Pane)
	}
}

// TestFileProviderOpenSettingsTriesURLs pins that OpenSettings walks the FP
// Enablement URLs in order via the openRunner seam (no System Settings launched).
func TestFileProviderOpenSettingsTriesURLs(t *testing.T) {
	prev := openRunner
	defer func() { openRunner = prev }()

	var tried []string
	openRunner = func(_ context.Context, url string) error {
		tried = append(tried, url)
		if len(tried) < 2 {
			return errors.New("first anchor missing on this macOS")
		}
		return nil
	}
	if err := BackendFileProvider.OpenSettings(context.Background()); err != nil {
		t.Fatalf("OpenSettings: %v", err)
	}
	want := BackendFileProvider.Enablement().URLs[:2]
	if len(tried) != 2 || tried[0] != want[0] || tried[1] != want[1] {
		t.Errorf("tried URLs = %v, want the first two of %v in order", tried, BackendFileProvider.Enablement().URLs)
	}
}
