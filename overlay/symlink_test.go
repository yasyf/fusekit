package overlay

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func makeBase(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	for _, d := range []string{"projects", "skills", "daemon", "ide", "backups"} {
		if err := os.MkdirAll(filepath.Join(base, d), 0o750); err != nil {
			t.Fatal(err)
		}
	}
	// A backup in base must never become visible to accounts.
	if err := os.WriteFile(filepath.Join(base, "backups", "seed.bak"), []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	// A .claude.json in base (private file) must never be linked into accounts.
	if err := os.WriteFile(filepath.Join(base, ".claude.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	// Plain claude's plaintext credential fallback: linking it would leak the
	// live OAuth token.
	if err := os.WriteFile(filepath.Join(base, ".credentials.json"), []byte(`{"claudeAiOauth":{"accessToken":"plain-claude"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	// Per-subscription settings cache; must never be linked into accounts.
	if err := os.WriteFile(filepath.Join(base, "remote-settings.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, ".DS_Store"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	return base
}

func TestSymlinkReconcileSharesAndExcludes(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}

	for _, name := range []string{"projects", "skills", "settings.json"} {
		target, err := os.Readlink(filepath.Join(acct, name))
		if err != nil {
			t.Fatalf("%s not a symlink: %v", name, err)
		}
		if target != filepath.Join(base, name) {
			t.Errorf("%s -> %q, want %q", name, target, filepath.Join(base, name))
		}
	}

	for _, name := range []string{"daemon", "ide", "backups"} {
		fi, err := os.Lstat(filepath.Join(acct, name))
		if err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			t.Errorf("%s should be a private dir, not a symlink", name)
		}
	}

	if _, err := os.Stat(filepath.Join(acct, "backups", "seed.bak")); !os.IsNotExist(err) {
		t.Errorf("base backup leaked into the account's private backups dir")
	}

	if _, err := os.Lstat(filepath.Join(acct, ".DS_Store")); !os.IsNotExist(err) {
		t.Errorf(".DS_Store should be skipped")
	}

	if _, err := os.Lstat(filepath.Join(acct, ".claude.json")); !os.IsNotExist(err) {
		t.Errorf("base .claude.json should not be linked into the account dir")
	}

	if _, err := os.Lstat(filepath.Join(acct, ".credentials.json")); !os.IsNotExist(err) {
		t.Errorf("base .credentials.json must never be visible in an account dir")
	}

	if _, err := os.Lstat(filepath.Join(acct, "remote-settings.json")); !os.IsNotExist(err) {
		t.Errorf("base remote-settings.json should not be linked into the account dir")
	}

	if err := p.Check(t.Context(), base, acct); err != nil {
		t.Fatalf("Check after reconcile: %v", err)
	}
}

// TestReconcileAndCheckSkipAppleDoubleLitter pins the SkipPrefixes sweep in Reconcile and
// Check: AppleDouble "._*" litter in base is never linked into the account dir
// and never trips Check, while a dotfile matching no skip rule is linked and
// health-checked exactly as any shared entry.
func TestReconcileAndCheckSkipAppleDoubleLitter(t *testing.T) {
	base := makeBase(t)
	writeFile(t, filepath.Join(base, "._litter"), "cruft")
	writeFile(t, filepath.Join(base, ".foo"), "dot")
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(acct, "._litter")); !os.IsNotExist(err) {
		t.Errorf("._litter should be skipped, not linked; Lstat err = %v", err)
	}
	target, err := os.Readlink(filepath.Join(acct, ".foo"))
	if err != nil {
		t.Fatalf(".foo (matches no skip rule) not linked: %v", err)
	}
	if target != filepath.Join(base, ".foo") {
		t.Errorf(".foo -> %q, want %q", target, filepath.Join(base, ".foo"))
	}
	if err := p.Check(t.Context(), base, acct); err != nil {
		t.Fatalf("Check must skip ._litter, got: %v", err)
	}
	// Litter appearing in base after setup must never trip Check...
	writeFile(t, filepath.Join(base, "._more"), "cruft")
	if err := p.Check(t.Context(), base, acct); err != nil {
		t.Fatalf("Check must skip late ._more, got: %v", err)
	}
	// ...but a non-matching dotfile appearing unlinked must, as any shared entry.
	writeFile(t, filepath.Join(base, ".bar"), "dot")
	if err := p.Check(t.Context(), base, acct); err == nil {
		t.Fatal("Check must flag the unlinked non-skipped .bar")
	}
	// Reconcile links the dotfile, still never the litter, and base litter is intact.
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatalf("Reconcile after new entries: %v", err)
	}
	if _, err := os.Readlink(filepath.Join(acct, ".bar")); err != nil {
		t.Errorf(".bar not linked by Reconcile: %v", err)
	}
	for _, name := range []string{"._litter", "._more"} {
		if _, err := os.Lstat(filepath.Join(acct, name)); !os.IsNotExist(err) {
			t.Errorf("%s appeared in the account dir; Lstat err = %v", name, err)
		}
		if got := readFile(t, filepath.Join(base, name)); got != "cruft" {
			t.Errorf("base %s disturbed: %q", name, got)
		}
	}
}

// TestCredentialsFileNeverShared pins the safety fix: linking plain claude's
// plaintext credential file (Keychain-unavailable fallback) would let
// `claude /login` adopt plain claude's login and a refresh mutate it.
func TestCredentialsFileNeverShared(t *testing.T) {
	base := makeBase(t)
	want, err := os.ReadFile(filepath.Join(base, ".credentials.json")) //nolint:gosec // G304: base is under the test's own t.TempDir()
	if err != nil {
		t.Fatal(err)
	}
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(acct, ".credentials.json")); !os.IsNotExist(err) {
		t.Fatalf("plain claude's .credentials.json was shared into the account dir")
	}
	// Re-sync (daemon poll) must keep ignoring it.
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(acct, ".credentials.json")); !os.IsNotExist(err) {
		t.Fatalf("Reconcile linked .credentials.json into the account dir")
	}
	if got, _ := os.ReadFile(filepath.Join(base, ".credentials.json")); string(got) != string(want) { //nolint:gosec // G304: base is under the test's own t.TempDir()
		t.Fatalf("base .credentials.json was modified: got %q, want %q", got, want)
	}
}

// TestBackupsIsPrivatePerAccount pins that a write into the account's backups
// dir never appears in base's backups.
func TestBackupsIsPrivatePerAccount(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(acct, "backups", ".claude.json.backup.1"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "backups", ".claude.json.backup.1")); !os.IsNotExist(err) {
		t.Fatalf("account backup leaked into base backups dir")
	}
}

// TestPrivateEntry pins the test Spec's private-name predicate (mirroring
// cc-pool's), including the atomic-write temp files.
func TestPrivateEntry(t *testing.T) {
	cases := map[string]bool{
		".claude.json":                   true,
		".claude.json.tmp.ab12cd34":      true,
		".claude.json.backup.123":        true,
		".credentials.json":              true,
		".credentials.json.tmp.ab12cd34": true,
		".credentials.json.lock":         true,
		".last-update-result.json":       true,
		".last-update-result.json.tmp.x": true,
		"remote-settings.json":           true,
		"remote-settings.json.tmp.ab12":  true,
		"daemon":                         true,
		"ide":                            true,
		"backups":                        true,
		"plans":                          false,
		"projects":                       false,
		"settings.json":                  false,
		".claude":                        false,
		"claude.json":                    false,
		"credentials.json":               false,
		"remote-settings":                false,
		"remote-settings.jsonx":          false,
	}
	for name, want := range cases {
		if got := testIsPrivate(name); got != want {
			t.Errorf("testIsPrivate(%q) = %v, want %v", name, got, want)
		}
	}
}

// TestReconcileSkipsPreexistingLastUpdateResult: claude rewrites
// .last-update-result.json atomically, replacing the overlay symlink with a
// real file; because it is a PrivateEntry, Reconcile must skip it without erroring.
func TestReconcileSkipsPreexistingLastUpdateResult(t *testing.T) {
	base := makeBase(t)
	// Base needs its own copy so Reconcile iterates over the name.
	if err := os.WriteFile(filepath.Join(base, ".last-update-result.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	// Simulate claude's atomic write: a real file in the account dir.
	if err := os.WriteFile(filepath.Join(acct, ".last-update-result.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatalf("Reconcile must skip the private .last-update-result.json, got: %v", err)
	}
	fi, err := os.Lstat(filepath.Join(acct, ".last-update-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		t.Error(".last-update-result.json should stay a private real file, not a symlink")
	}
}

// TestReconcileSkipsPreexistingRemoteSettings pins the acct-01/acct-02 incident:
// claude wrote a real remote-settings.json into the account dir before base had
// the name; Reconcile must skip the private name, not error on the real file.
func TestReconcileSkipsPreexistingRemoteSettings(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	// Simulate claude's direct write into $CONFIG_DIR.
	if err := os.WriteFile(filepath.Join(acct, "remote-settings.json"), []byte(`{"acct":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatalf("Reconcile must skip the private remote-settings.json, got: %v", err)
	}
	fi, err := os.Lstat(filepath.Join(acct, "remote-settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		t.Error("remote-settings.json should stay a private real file, not a symlink")
	}
	if got, _ := os.ReadFile(filepath.Join(acct, "remote-settings.json")); string(got) != `{"acct":1}` { //nolint:gosec // G304: acct is under the test's own t.TempDir()
		t.Errorf("account remote-settings.json content = %q, want %q", got, `{"acct":1}`)
	}
}

// TestReconcileRemovesStaleLinkAtPrivateName pins the acct-03 incident: a stale
// shared link at a now-private name. Check must flag it, Reconcile must remove it
// (claude rewrites the file itself), base untouched.
func TestReconcileRemovesStaleLinkAtPrivateName(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(acct, "remote-settings.json")
	if err := os.Symlink(filepath.Join(base, "remote-settings.json"), dst); err != nil {
		t.Fatal(err)
	}
	if err := p.Check(t.Context(), base, acct); err == nil {
		t.Fatal("Check must flag a symlink at a private name")
	}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatalf("Reconcile must self-heal the stale private link, got: %v", err)
	}
	if _, err := os.Lstat(dst); !os.IsNotExist(err) {
		t.Errorf("stale private link should be removed, Lstat err = %v", err)
	}
	if err := p.Check(t.Context(), base, acct); err != nil {
		t.Fatalf("Check after self-heal: %v", err)
	}
	if got, _ := os.ReadFile(filepath.Join(base, "remote-settings.json")); string(got) != "{}" { //nolint:gosec // G304: base is under the test's own t.TempDir()
		t.Errorf("base remote-settings.json content = %q, want %q", got, "{}")
	}
}

func TestReconcileRemovesStaleOwnedLinkAfterBaseEntryDeleted(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(acct, "projects")
	if err := os.Remove(filepath.Join(base, "projects")); err != nil {
		t.Fatal(err)
	}
	if err := p.Check(t.Context(), base, acct); err == nil {
		t.Fatal("Check with a stale provider-owned link = nil, want drift")
	}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatalf("Reconcile stale provider-owned link: %v", err)
	}
	if _, err := os.Lstat(dst); !os.IsNotExist(err) {
		t.Fatalf("stale provider-owned link remains: %v", err)
	}
	if err := p.Check(t.Context(), base, acct); err != nil {
		t.Fatalf("Check after stale-link repair: %v", err)
	}
}

func TestForeignSymlinkAtDeletedEntrySurvivesReconcileAndTeardown(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(acct, "projects")
	if err := os.Remove(dst); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(base, "projects")); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(t.TempDir(), "foreign")
	if err := os.Symlink(foreign, dst); err != nil {
		t.Fatal(err)
	}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatalf("Reconcile with an unconstrained foreign link: %v", err)
	}
	if _, err := p.Teardown(t.Context(), base, acct); err != nil {
		t.Fatalf("Teardown with a foreign link: %v", err)
	}
	if target, err := os.Readlink(dst); err != nil || target != foreign {
		t.Fatalf("foreign link after Reconcile and Teardown = %q, %v; want %q", target, err, foreign)
	}
}

func TestReconcileRefusesForeignSymlinksAtDesiredAndPrivateNames(t *testing.T) {
	for _, name := range []string{"projects", "remote-settings.json"} {
		t.Run(name, func(t *testing.T) {
			base := makeBase(t)
			acct := filepath.Join(t.TempDir(), "acct-01")
			p := &SymlinkProvider{Spec: testSpec()}
			if err := p.Reconcile(t.Context(), base, acct); err != nil {
				t.Fatal(err)
			}
			dst := filepath.Join(acct, name)
			if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
				t.Fatal(err)
			}
			foreign := filepath.Join(t.TempDir(), "foreign")
			if err := os.Symlink(foreign, dst); err != nil {
				t.Fatal(err)
			}
			if err := p.Reconcile(t.Context(), base, acct); err == nil {
				t.Fatal("Reconcile with a foreign symlink = nil, want fail-closed refusal")
			}
			if target, err := os.Readlink(dst); err != nil || target != foreign {
				t.Fatalf("foreign link after Reconcile = %q, %v; want %q", target, err, foreign)
			}
			if _, err := p.Teardown(t.Context(), base, acct); err != nil {
				t.Fatalf("Teardown with a foreign link: %v", err)
			}
			if target, err := os.Readlink(dst); err != nil || target != foreign {
				t.Fatalf("foreign link after Teardown = %q, %v; want %q", target, err, foreign)
			}
		})
	}
}

// TestReconcileContinuesPastConflictAndJoinsErrors pins error aggregation: a
// pre-existing real file at one shared name neither blocks entries sorting
// after it nor masks a second conflict; Reconcile reports both and links the rest.
func TestReconcileContinuesPastConflictAndJoinsErrors(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	// Claude lazily writes two real files into the account dir...
	for _, name := range []string{"aaa.json", "mmm.json"} {
		if err := os.WriteFile(filepath.Join(acct, name), []byte("acct"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// ...then base gains the same names plus one that sorts after both.
	for _, name := range []string{"aaa.json", "mmm.json", "zzz.json"} {
		if err := os.WriteFile(filepath.Join(base, name), []byte("base"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	err := p.Reconcile(t.Context(), base, acct)
	if err == nil {
		t.Fatal("Reconcile must report the conflicting real files")
	}
	for _, name := range []string{"aaa.json", "mmm.json"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("Reconcile error does not name conflict %q: %v", name, err)
		}
	}
	if target, lerr := os.Readlink(filepath.Join(acct, "zzz.json")); lerr != nil || target != filepath.Join(base, "zzz.json") {
		t.Errorf("zzz.json not linked past the conflicts: target=%q err=%v", target, lerr)
	}
	for _, name := range []string{"aaa.json", "mmm.json"} {
		if got, _ := os.ReadFile(filepath.Join(acct, name)); string(got) != "acct" { //nolint:gosec // G304: acct is under the test's own t.TempDir()
			t.Errorf("conflict file %q clobbered: content = %q, want %q", name, got, "acct")
		}
	}
}

func TestWriteThroughSymlinkLandsInBase(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(acct, "projects", "x.json"), []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "projects", "x.json")); err != nil {
		t.Fatalf("write did not pass through to base: %v", err)
	}
}

// TestReconcileSharesPlans: claude writes plan-mode plans into $CONFIG_DIR/plans,
// which would otherwise scatter as per-account dirs. Reconcile creates
// ~/.claude/plans (absent from base) and links each account to it, so one
// account's plan reaches all.
func TestReconcileSharesPlans(t *testing.T) {
	base := makeBase(t)
	if _, err := os.Lstat(filepath.Join(base, "plans")); !os.IsNotExist(err) {
		t.Fatalf("precondition: base must start without a plans dir")
	}
	acct1 := filepath.Join(t.TempDir(), "acct-01")
	acct2 := filepath.Join(t.TempDir(), "acct-02")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct1); err != nil {
		t.Fatal(err)
	}
	if err := p.Reconcile(t.Context(), base, acct2); err != nil {
		t.Fatal(err)
	}

	if fi, err := os.Lstat(filepath.Join(base, "plans")); err != nil || !fi.IsDir() {
		t.Fatalf("Reconcile did not create base plans dir: fi=%v err=%v", fi, err)
	}
	for _, acct := range []string{acct1, acct2} {
		target, err := os.Readlink(filepath.Join(acct, "plans"))
		if err != nil {
			t.Fatalf("%s/plans not a symlink: %v", acct, err)
		}
		if target != filepath.Join(base, "plans") {
			t.Errorf("plans -> %q, want %q", target, filepath.Join(base, "plans"))
		}
	}

	if err := os.WriteFile(filepath.Join(acct1, "plans", "p.md"), []byte("plan"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(acct2, "plans", "p.md")) //nolint:gosec // G304: acct2 is under the test's own t.TempDir()
	if err != nil {
		t.Fatalf("plan not visible to the second account: %v", err)
	}
	if string(got) != "plan" {
		t.Errorf("shared plan content = %q, want %q", got, "plan")
	}
	if _, err := os.Stat(filepath.Join(base, "plans", "p.md")); err != nil {
		t.Fatalf("plan did not land in base: %v", err)
	}
}

func TestReconcilePicksUpNewEntry(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(base, "plugins"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := p.Check(t.Context(), base, acct); err == nil {
		t.Fatal("Check should report missing link for new entry")
	}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Readlink(filepath.Join(acct, "plugins")); err != nil {
		t.Fatalf("Reconcile did not link new entry: %v", err)
	}
}

func TestTeardownRemovesOverlayNotBase(t *testing.T) {
	base := makeBase(t)
	acct := filepath.Join(t.TempDir(), "acct-01")
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Teardown(t.Context(), base, acct); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(base, "settings.json")); err != nil {
		t.Fatalf("base content destroyed: %v", err)
	}
	if _, err := os.Lstat(filepath.Join(acct, "projects")); !os.IsNotExist(err) {
		t.Errorf("overlay link not removed")
	}
}

func TestTeardownRefusesBase(t *testing.T) {
	base := makeBase(t)
	p := &SymlinkProvider{Spec: testSpec()}
	if _, err := p.Teardown(t.Context(), base, base); err == nil {
		t.Fatal("Teardown must refuse to operate on base")
	}
}

// TestReconcileRefusesBase pins the guard on the mutating path: self-overlay
// would replace the user's real ~/.claude entries with self-referential links.
func TestReconcileRefusesBase(t *testing.T) {
	base := makeBase(t)
	p := &SymlinkProvider{Spec: testSpec()}
	if err := p.Reconcile(t.Context(), base, base); err == nil {
		t.Fatal("Reconcile must refuse to overlay base onto itself")
	}
	if err := p.Reconcile(t.Context(), base, ""); err == nil {
		t.Fatal("Reconcile must refuse an empty account dir")
	}
	// The refusal must come BEFORE any mutation: base's entries are intact.
	for _, name := range []string{"projects", "settings.json", "backups"} {
		fi, err := os.Lstat(filepath.Join(base, name))
		if err != nil {
			t.Fatalf("base entry %s damaged: %v", name, err)
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			t.Fatalf("base entry %s replaced with a symlink", name)
		}
	}
}
