package overlay

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestParseFileProvider pins that Parse accepts the fileprovider value and still
// rejects unknowns strictly.
func TestParseFileProvider(t *testing.T) {
	got, err := Parse("fileprovider")
	if err != nil {
		t.Fatalf("Parse(fileprovider) = %v, want nil", err)
	}
	if got != BackendFileProvider {
		t.Errorf("Parse(fileprovider) = %q, want %q", got, BackendFileProvider)
	}
	if _, err := Parse("FileProvider"); err == nil {
		t.Error("Parse(FileProvider) = nil error, want ErrUnknownBackend (case-sensitive)")
	}
}

// TestFileProviderAvailable pins the availability gate: nil wiring and an empty
// bundle id are unavailable; a wired spec defers to the enabled-check seam.
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

// TestTryEnableFileProvider pins the election orchestrator over its two seams:
// election is always attempted with the bundle id; a successful election that
// re-checks enabled returns nil; a successful election still disabled on re-check
// returns ErrFileProviderElectionIneffective; a pluginkit failure surfaces loud
// (wrapping the exec error, never the sentinel) and never consults the re-check.
func TestTryEnableFileProvider(t *testing.T) {
	const bundleID = "com.example.fp"
	pluginkitErr := errors.New("pluginkit -e use -i com.example.fp: exit status 1")

	cases := map[string]struct {
		electErr     error
		enabled      bool
		wantErr      bool
		wantSentinel bool
		wantChecks   int
	}{
		"elected and enabled on re-check": {
			electErr: nil, enabled: true,
			wantErr: false, wantSentinel: false, wantChecks: 1,
		},
		"elected but still disabled returns sentinel": {
			electErr: nil, enabled: false,
			wantErr: true, wantSentinel: true, wantChecks: 1,
		},
		"pluginkit error surfaces and skips re-check": {
			// enabled is irrelevant: the re-check must not run after a failed election.
			electErr: pluginkitErr, enabled: true,
			wantErr: true, wantSentinel: false, wantChecks: 0,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			prevElect, prevEnabled := fileProviderElect, fileProviderEnabled
			defer func() { fileProviderElect, fileProviderEnabled = prevElect, prevEnabled }()

			var electedID, checkedID string
			var electCalls, checkCalls int
			fileProviderElect = func(id string) error { electCalls++; electedID = id; return tc.electErr }
			fileProviderEnabled = func(id string) bool { checkCalls++; checkedID = id; return tc.enabled }

			err := TryEnableFileProvider(bundleID)

			// Election is attempted exactly once, with the given bundle id.
			if electCalls != 1 || electedID != bundleID {
				t.Errorf("elect seam calls=%d id=%q, want 1 call with %q", electCalls, electedID, bundleID)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("TryEnableFileProvider err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := errors.Is(err, ErrFileProviderElectionIneffective); got != tc.wantSentinel {
				t.Errorf("errors.Is(err, ErrFileProviderElectionIneffective) = %v, want %v (err=%v)", got, tc.wantSentinel, err)
			}
			// The re-check runs only after a successful election; a pluginkit
			// failure surfaces loud without consulting it.
			if checkCalls != tc.wantChecks {
				t.Errorf("re-check seam calls=%d, want %d", checkCalls, tc.wantChecks)
			}
			if tc.wantChecks > 0 && checkedID != bundleID {
				t.Errorf("re-check seam queried %q, want %q", checkedID, bundleID)
			}
			// A pluginkit failure wraps the underlying exec error, not the sentinel.
			if tc.electErr != nil && !errors.Is(err, tc.electErr) {
				t.Errorf("err = %v, want it to wrap the pluginkit error %v", err, tc.electErr)
			}
		})
	}
}

// TestFileProviderEnablement pins the FP enablement copy: a needed grant naming
// the File Providers pane with deep-link URLs.
func TestFileProviderEnablement(t *testing.T) {
	en := BackendFileProvider.Enablement()
	if !en.Needed || en.Pane == "" || en.Guidance == "" || len(en.URLs) == 0 {
		t.Fatalf("fileprovider Enablement() incomplete: %+v", en)
	}
	if !strings.Contains(en.Pane, "File Providers") {
		t.Errorf("fileprovider Enablement().Pane = %q, want it to name File Providers", en.Pane)
	}
}

// TestFileProviderOpenSettingsTriesURLs pins OpenSettings walking the FP
// Enablement URLs in order via the openRunner seam.
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
