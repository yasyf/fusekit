package overlay

import "strings"

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

// testIsPrivate mirrors cc-pool's PrivateEntry: the excluded dirs plus the
// identity/state/credential file families and their atomic-write temp siblings.
func testIsPrivate(name string) bool {
	return testExcluded[name] ||
		name == ".claude.json" || strings.HasPrefix(name, ".claude.json.") ||
		name == ".credentials.json" || strings.HasPrefix(name, ".credentials.json.") ||
		strings.HasPrefix(name, ".last-update-result") ||
		name == "remote-settings.json" || strings.HasPrefix(name, "remote-settings.json.")
}

// testSpec is the Spec the lifted tests drive the providers and migration
// primitives with — a faithful copy of cc-pool's package-level classification,
// proving the package's Spec-driven behavior matches the original.
func testSpec() Spec {
	return Spec{
		IsPrivate: testIsPrivate,
		Excluded:  testExcluded,
		Shared:    testShared,
		Skip:      testSkip,
	}
}
