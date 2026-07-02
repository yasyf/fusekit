package overlay

import (
	"strings"
	"testing"
)

// testExcluded mirrors cc-pool's ExcludedEntries: top-level dirs that must NOT
// be shared across accounts, each becoming a private empty per-account dir.
var testExcluded = map[string]bool{
	"daemon":  true,
	"ide":     true,
	"backups": true,
}

// testShared mirrors cc-pool's SharedEntries: dirs always materialized + linked
// even when absent from base (plan-mode plans).
var testShared = map[string]bool{
	"plans": true,
}

// testSkip mirrors cc-pool's skipEntries: OS cruft never touched.
var testSkip = map[string]bool{
	".DS_Store": true,
}

// testSkipPrefixes mirrors cc-pool's skip prefixes: AppleDouble "._*" litter
// never linked, mirrored, or moved (the motivating SkipPrefixes case).
var testSkipPrefixes = []string{"._"}

// testIsPrivate mirrors cc-pool's PrivateEntry: the excluded dirs plus the
// identity/state/credential file families and their atomic-write temp siblings.
func testIsPrivate(name string) bool {
	return testExcluded[name] ||
		name == ".claude.json" || strings.HasPrefix(name, ".claude.json.") ||
		name == ".credentials.json" || strings.HasPrefix(name, ".credentials.json.") ||
		strings.HasPrefix(name, ".last-update-result") ||
		name == "remote-settings.json" || strings.HasPrefix(name, "remote-settings.json.")
}

// testSpec drives the providers and migration primitives — a faithful copy of
// cc-pool's classification, proving this package matches the original.
func testSpec() Spec {
	return Spec{
		IsPrivate:    testIsPrivate,
		Excluded:     testExcluded,
		Shared:       testShared,
		Skip:         testSkip,
		SkipPrefixes: testSkipPrefixes,
	}
}

// TestSkipped pins the Skipped classification: exact Skip membership plus
// SkipPrefixes prefix matches, and nothing else — no substring, suffix, or
// all-dotfile over-matching, and a zero Spec skips nothing.
func TestSkipped(t *testing.T) {
	both := Spec{
		Skip:         map[string]bool{".DS_Store": true},
		SkipPrefixes: []string{"._"},
	}
	cases := []struct {
		name  string
		spec  Spec
		entry string
		want  bool
	}{
		{name: "exact Skip match", spec: both, entry: ".DS_Store", want: true},
		{name: "prefix match", spec: both, entry: "._x", want: true},
		{name: "bare prefix itself matches", spec: both, entry: "._", want: true},
		{name: "Skip match with nil prefixes", spec: Spec{Skip: map[string]bool{".DS_Store": true}}, entry: ".DS_Store", want: true},
		{name: "prefix match with nil Skip", spec: Spec{SkipPrefixes: []string{"._"}}, entry: "._x", want: true},
		{name: "dotfile matching neither", spec: both, entry: ".foo", want: false},
		{name: "prefix mid-name is not a match", spec: both, entry: "x._y", want: false},
		{name: "Skip entry is not a suffix match", spec: both, entry: "DS_Store", want: false},
		{name: "unrelated name", spec: both, entry: "projects", want: false},
		{name: "nil Skip and SkipPrefixes skip nothing", spec: Spec{}, entry: ".DS_Store", want: false},
		{name: "empty Skip and SkipPrefixes skip nothing", spec: Spec{Skip: map[string]bool{}, SkipPrefixes: []string{}}, entry: "._x", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.spec.Skipped(tc.entry); got != tc.want {
				t.Errorf("Skipped(%q) = %v, want %v", tc.entry, got, tc.want)
			}
		})
	}
}
