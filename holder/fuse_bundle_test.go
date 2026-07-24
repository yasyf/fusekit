package holder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/daemonkit/worker"
)

type fakeFUSEBundleTools struct {
	app                   SignedApplication
	source                string
	order                 []string
	nestedTeam            string
	nestedEnts            map[string]bool
	outerTeam             string
	outerEnts             map[string]bool
	badInstall            bool
	signed                bool
	manifestSeen          bool
	appSigned             bool
	dropOuterMetadata     bool
	rejectRequirement     bool
	missingOuterHardened  bool
	missingNestedHardened bool
}

func (f *fakeFUSEBundleTools) InspectLibrary(_ context.Context, path string) (FUSELibraryInspection, error) {
	f.order = append(f.order, "inspect-library")
	inspection := FUSELibraryInspection{
		Architectures: []string{"x86_64", "arm64"}, InstallName: FUSEInstallName,
		Dependencies: slices.Clone(expectedFUSEDependencies),
	}
	if f.badInstall {
		inspection.InstallName = "/usr/local/lib/libfuse-t.dylib"
	}
	if path == f.source {
		return inspection, nil
	}
	if !f.signed {
		return FUSELibraryInspection{}, errors.New("nested library was inspected before signing")
	}
	team := f.nestedTeam
	if team == "" {
		team = f.app.TeamID
	}
	identifier := f.app.BundleID + ".fuse-t"
	inspection.Code = BundleCodeIdentity{
		TeamID: team, SigningIdentifier: identifier,
		Entitlements: f.nestedEnts, HardenedRuntime: !f.missingNestedHardened,
	}
	return inspection, nil
}

func (f *fakeFUSEBundleTools) InspectApplication(context.Context, string) (BundleCodeIdentity, error) {
	f.order = append(f.order, "inspect-app")
	team := f.outerTeam
	if team == "" {
		team = f.app.TeamID
	}
	entitlementsDigest := strings.Repeat("a", 64)
	if f.appSigned && f.dropOuterMetadata {
		entitlementsDigest = strings.Repeat("b", 64)
	}
	return BundleCodeIdentity{
		TeamID: team, SigningIdentifier: f.app.Runtime.SigningIdentifier,
		EntitlementsSHA256: entitlementsDigest, Entitlements: f.outerEnts,
		HardenedRuntime: !f.missingOuterHardened,
	}, nil
}

func (f *fakeFUSEBundleTools) VerifyCodeRequirement(context.Context, string, string) error {
	f.order = append(f.order, "verify-code")
	if f.rejectRequirement {
		return errors.New("requirement mismatch")
	}
	return nil
}

func (f *fakeFUSEBundleTools) SignNestedLibrary(_ context.Context, path, _ string) error {
	f.order = append(f.order, "sign-nested")
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteString("-consumer-signature")
	f.signed = true
	return errors.Join(writeErr, file.Close())
}

func (f *fakeFUSEBundleTools) SignApplication(_ context.Context, path string) error {
	f.order = append(f.order, "sign-app")
	_, err := os.Stat(filepath.Join(path, FUSEManifestRelativePath))
	f.manifestSeen = err == nil
	f.appSigned = true
	return err
}

func TestPackageFUSEBundleSignsInsideOutAndPinsPostSignBytes(t *testing.T) {
	app, source, digest := fuseBundleFixture(t)
	tools := &fakeFUSEBundleTools{app: app, source: source}
	manifest, err := packageFUSEBundle(t.Context(), app, source, digest, tools)
	if err != nil {
		t.Fatal(err)
	}
	if !tools.manifestSeen {
		t.Fatal("outer application was signed before the manifest existed")
	}
	nestedRequirement, err := bundleCodeRequirement(app.TeamID, app.BundleID+".fuse-t")
	if err != nil {
		t.Fatal(err)
	}
	outerRequirement, err := bundleCodeRequirement(app.TeamID, app.Runtime.SigningIdentifier)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.DesignatedRequirement != nestedRequirement ||
		manifest.OuterDesignatedRequirement != outerRequirement {
		t.Fatalf("manifest requirements = (%q, %q), want (%q, %q)",
			manifest.DesignatedRequirement, manifest.OuterDesignatedRequirement,
			nestedRequirement, outerRequirement)
	}
	wantOrder := []string{
		"inspect-app", "verify-code", "inspect-library", "sign-nested", "inspect-library", "verify-code",
		"sign-app", "inspect-app", "verify-code", "inspect-library", "verify-code",
	}
	if !slices.Equal(tools.order, wantOrder) {
		t.Fatalf("sign/verify order = %v, want %v", tools.order, wantOrder)
	}
	library := filepath.Join(app.AppPath, FUSELibraryRelativePath)
	if got, err := fileSHA256(library); err != nil || got != manifest.SignedSHA256 {
		t.Fatalf("post-sign digest = %q, %v; manifest = %q", got, err, manifest.SignedSHA256)
	}
	license, err := os.ReadFile(filepath.Join(app.AppPath, FUSELicenseRelativePath))
	if err != nil || !slices.Equal(license, fuseLicense) {
		t.Fatalf("bundled license differs from reviewed upstream bytes: %v", err)
	}
}

func TestPackageFUSEBundleRejectsTamperSymlinkAndForeignIdentity(t *testing.T) {
	t.Run("source-tamper", func(t *testing.T) {
		app, source, digest := fuseBundleFixture(t)
		file, err := os.OpenFile(source, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = file.WriteString("tamper")
		_ = file.Close()
		if _, err := packageFUSEBundle(t.Context(), app, source, digest, &fakeFUSEBundleTools{app: app, source: source}); err == nil {
			t.Fatal("tampered reviewed source accepted")
		}
	})
	t.Run("source-symlink", func(t *testing.T) {
		app, source, digest := fuseBundleFixture(t)
		link := filepath.Join(t.TempDir(), "libfuse-t.dylib")
		if err := os.Symlink(source, link); err != nil {
			t.Fatal(err)
		}
		if _, err := packageFUSEBundle(t.Context(), app, link, digest, &fakeFUSEBundleTools{app: app, source: link}); err == nil {
			t.Fatal("source symlink accepted")
		}
	})
	t.Run("frameworks-path-escape", func(t *testing.T) {
		app, source, digest := fuseBundleFixture(t)
		external := t.TempDir()
		contents := filepath.Join(app.AppPath, "Contents")
		if err := os.MkdirAll(contents, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(external, filepath.Join(contents, "Frameworks")); err != nil {
			t.Fatal(err)
		}
		if _, err := packageFUSEBundle(t.Context(), app, source, digest, &fakeFUSEBundleTools{app: app, source: source}); err == nil {
			t.Fatal("symlinked Frameworks path accepted")
		}
	})
	t.Run("foreign-team", func(t *testing.T) {
		app, source, digest := fuseBundleFixture(t)
		tools := &fakeFUSEBundleTools{app: app, source: source, nestedTeam: "OTHERTEAM1"}
		if _, err := packageFUSEBundle(t.Context(), app, source, digest, tools); err == nil || !strings.Contains(err.Error(), "same-Team") {
			t.Fatalf("foreign nested identity = %v", err)
		}
	})
	t.Run("requirement-mismatch", func(t *testing.T) {
		app, source, digest := fuseBundleFixture(t)
		tools := &fakeFUSEBundleTools{app: app, source: source, rejectRequirement: true}
		if _, err := packageFUSEBundle(t.Context(), app, source, digest, tools); err == nil ||
			!strings.Contains(err.Error(), "designated requirement") {
			t.Fatalf("nonmatching requirement = %v", err)
		}
		if !slices.Equal(tools.order, []string{"inspect-app", "verify-code"}) {
			t.Fatalf("requirement mismatch mutated bundle: order = %v", tools.order)
		}
		if _, err := os.Stat(filepath.Join(app.AppPath, FUSELibraryRelativePath)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("requirement mismatch created nested library: %v", err)
		}
	})
	for _, entitlement := range injectionEntitlements {
		t.Run("forbidden-"+entitlement, func(t *testing.T) {
			app, source, digest := fuseBundleFixture(t)
			tools := &fakeFUSEBundleTools{
				app: app, source: source,
				nestedEnts: map[string]bool{entitlement: true},
			}
			if _, err := packageFUSEBundle(t.Context(), app, source, digest, tools); err == nil ||
				!strings.Contains(err.Error(), entitlement) {
				t.Fatalf("injection entitlement %q = %v", entitlement, err)
			}
		})
	}
	t.Run("missing-outer-hardened-runtime", func(t *testing.T) {
		app, source, digest := fuseBundleFixture(t)
		tools := &fakeFUSEBundleTools{app: app, source: source, missingOuterHardened: true}
		if _, err := packageFUSEBundle(t.Context(), app, source, digest, tools); err == nil ||
			!strings.Contains(err.Error(), "Hardened Runtime") {
			t.Fatalf("unhardened outer identity = %v", err)
		}
	})
	t.Run("missing-nested-hardened-runtime", func(t *testing.T) {
		app, source, digest := fuseBundleFixture(t)
		tools := &fakeFUSEBundleTools{app: app, source: source, missingNestedHardened: true}
		if _, err := packageFUSEBundle(t.Context(), app, source, digest, tools); err == nil ||
			!strings.Contains(err.Error(), "Hardened Runtime") {
			t.Fatalf("unhardened nested identity = %v", err)
		}
	})
	t.Run("reviewed-mach-o", func(t *testing.T) {
		app, source, digest := fuseBundleFixture(t)
		tools := &fakeFUSEBundleTools{app: app, source: source, badInstall: true}
		if _, err := packageFUSEBundle(t.Context(), app, source, digest, tools); err == nil {
			t.Fatal("unexpected install name accepted")
		}
	})
	t.Run("outer-metadata-loss", func(t *testing.T) {
		app, source, digest := fuseBundleFixture(t)
		tools := &fakeFUSEBundleTools{app: app, source: source, dropOuterMetadata: true}
		if _, err := packageFUSEBundle(t.Context(), app, source, digest, tools); err == nil ||
			!strings.Contains(err.Error(), "metadata changed") {
			t.Fatalf("outer entitlement loss = %v", err)
		}
	})
}

func TestValidateFUSEBundleRejectsLicenseAndLibraryTamper(t *testing.T) {
	for _, target := range []string{FUSELicenseRelativePath, FUSELibraryRelativePath} {
		t.Run(filepath.Base(target), func(t *testing.T) {
			app, source, digest := fuseBundleFixture(t)
			tools := &fakeFUSEBundleTools{app: app, source: source}
			if _, err := packageFUSEBundle(t.Context(), app, source, digest, tools); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(app.AppPath, target)
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, _ = file.WriteString("tamper")
			_ = file.Close()
			if _, err := validateFUSEBundle(t.Context(), app, digest, tools); err == nil {
				t.Fatalf("tampered %s accepted", target)
			}
		})
	}
}

type recordingFUSEWorkerRunner struct {
	tasks []worker.CommandRequest
}

type fuseWorkerRunnerFunc func(context.Context, worker.CommandRequest) (worker.CommandResult, error)

func (f fuseWorkerRunnerFunc) Run(ctx context.Context, request worker.CommandRequest) (worker.CommandResult, error) {
	return f(ctx, request)
}

func TestCodeDirectoryRuntimeFlagParsingIsExact(t *testing.T) {
	if !hasCodeDirectoryFlag("CodeDirectory flags=0x10000(runtime) hashes=1", "runtime") {
		t.Fatal("runtime flag was not recognized")
	}
	for _, value := range []string{
		"CodeDirectory flags=0x0(none) hashes=1",
		"CodeDirectory flags=0x0(hard-runtime) hashes=1",
		"CodeDirectory flags=0x10000 hashes=1",
	} {
		if hasCodeDirectoryFlag(value, "runtime") {
			t.Fatalf("non-runtime CodeDirectory flags accepted: %q", value)
		}
	}
}

func TestCanonicalEntitlementsIgnoreToolFormattingAndCoverAppGroup(t *testing.T) {
	compact := []byte(`<?xml version="1.0"?><plist version="1.0"><dict><key>com.apple.security.application-groups</key><array><string>group.example</string></array></dict></plist>`)
	formatted := []byte(`<?xml version="1.0"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "https://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>com.apple.security.application-groups</key>
    <array><string>group.example</string></array>
  </dict>
</plist>`)
	first, keys, err := canonicalEntitlements(compact)
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := canonicalEntitlements(formatted)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || !keys["com.apple.security.application-groups"] {
		t.Fatalf("canonical entitlement digests differ: %q != %q, keys=%v", first, second, keys)
	}
	changed := strings.ReplaceAll(string(compact), "group.example", "group.other")
	third, _, err := canonicalEntitlements([]byte(changed))
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("App Group change did not change canonical entitlement digest")
	}
}

func (r *recordingFUSEWorkerRunner) Run(_ context.Context, task worker.CommandRequest) (worker.CommandResult, error) {
	r.tasks = append(r.tasks, task)
	var result worker.CommandResult
	stdout := func(value string) { result.Stdout = append(result.Stdout, value...) }
	stderr := func(value string) { result.Stderr = append(result.Stderr, value...) }
	switch {
	case task.Path == "/usr/bin/lipo":
		stdout("x86_64 arm64\n")
	case task.Path == "/usr/bin/otool" && task.Args[0] == "-D":
		for _, architecture := range []string{"x86_64", "arm64"} {
			stdout(task.Args[1] + " (architecture " + architecture + "):\n" + FUSEInstallName + "\n")
		}
	case task.Path == "/usr/bin/otool" && task.Args[0] == "-L":
		for _, architecture := range []string{"x86_64", "arm64"} {
			stdout(task.Args[1] + " (architecture " + architecture + "):\n\t" + FUSEInstallName + " (compatibility version 1.0.0, current version 1.0.0)\n")
			for _, dependency := range expectedFUSEDependencies {
				stdout("\t" + dependency + " (compatibility version 1.0.0, current version 1.0.0)\n")
			}
		}
	case task.Path == "/usr/bin/codesign" && slices.Contains(task.Args, "--verbose=4") && slices.Contains(task.Args, "--display"):
		stderr("Identifier=com.example.product.fuse-t\nCodeDirectory v=20500 size=1 flags=0x10000(runtime) hashes=1+0 location=embedded\nTeamIdentifier=ABCDE12345\n")
	case task.Path == "/usr/bin/codesign" && slices.Contains(task.Args, "--entitlements"):
		stdout("<?xml version=\"1.0\"?><plist><dict/></plist>\n")
	}
	return result, nil
}

func TestProductionFUSEToolchainUsesBoundedDisposableExactCommands(t *testing.T) {
	t.Setenv("CGOFUSE_LIBFUSE_PATH", "/usr/local/lib/libfuse-t.dylib")
	t.Setenv("FUSEKIT_CHILD_ENV_SENTINEL", "preserved")
	runner := &recordingFUSEWorkerRunner{}
	packager, err := newFUSEPackager(runner, "Developer ID Application: Example")
	if err != nil {
		t.Fatal(err)
	}
	tools := packager.tools
	appPath := "/Users/example/Applications/ProductHelper.app"
	path := filepath.Join(appPath, FUSELibraryRelativePath)
	if err := tools.SignNestedLibrary(t.Context(), path, "com.example.product.fuse-t"); err != nil {
		t.Fatal(err)
	}
	if err := tools.SignApplication(t.Context(), appPath); err != nil {
		t.Fatal(err)
	}
	if _, err := tools.InspectLibrary(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	requirement, err := bundleCodeRequirement("ABCDE12345", "com.example.product.fuse-t")
	if err != nil {
		t.Fatal(err)
	}
	if err := tools.VerifyCodeRequirement(t.Context(), path, requirement); err != nil {
		t.Fatal(err)
	}
	if len(runner.tasks) != 9 {
		t.Fatalf("production task count = %d, want 9", len(runner.tasks))
	}
	wantNested := []string{"--force", "--sign", "Developer ID Application: Example", "--identifier", "com.example.product.fuse-t", "--options", "runtime", "--timestamp", path}
	if !slices.Equal(runner.tasks[0].Args, wantNested) {
		t.Fatalf("nested sign arguments = %q, want %q", runner.tasks[0].Args, wantNested)
	}
	wantOuter := []string{
		"--force", "--sign", "Developer ID Application: Example",
		"--preserve-metadata=entitlements,requirements", "--options", "runtime", "--timestamp",
		appPath,
	}
	if !slices.Equal(runner.tasks[1].Args, wantOuter) {
		t.Fatalf("outer sign arguments = %q, want %q", runner.tasks[1].Args, wantOuter)
	}
	wantRequirement := []string{
		"--verify", "--strict", "--verbose=4", "--test-requirement", "=" + requirement, path,
	}
	if !slices.Equal(runner.tasks[8].Args, wantRequirement) {
		t.Fatalf("requirement verification arguments = %q, want %q", runner.tasks[8].Args, wantRequirement)
	}
	for _, task := range runner.tasks {
		if slices.Contains(task.Args, "--deep") {
			t.Fatalf("production command uses forbidden --deep: %s %q", task.Path, task.Args)
		}
		foundSentinel := false
		for _, entry := range task.Env {
			if strings.HasPrefix(entry, "CGOFUSE_LIBFUSE_PATH=") {
				t.Fatalf("native-only loader path leaked into packaging worker: %q", entry)
			}
			foundSentinel = foundSentinel || entry == "FUSEKIT_CHILD_ENV_SENTINEL=preserved"
		}
		if !foundSentinel {
			t.Fatal("unrelated packaging worker environment was not preserved")
		}
		if task.Dir != "/" || task.TotalTimeout != fuseToolTotalTimeout {
			t.Fatalf("packaging command policy = dir %q timeout %s", task.Dir, task.TotalTimeout)
		}
		for _, entry := range task.Env {
			if strings.HasPrefix(entry, "PATH=") || strings.HasPrefix(entry, "LANG=") {
				t.Fatalf("daemonkit-owned environment leaked into worker command: %q", entry)
			}
		}
	}
	if fuseToolTotalTimeout != 15*time.Minute || fuseToolOutputLimit != 1<<20 {
		t.Fatalf("FUSE tool policy = timeout %s output %d", fuseToolTotalTimeout, fuseToolOutputLimit)
	}
}

func TestFUSEToolchainRejectsOutputAboveItsOwnLimit(t *testing.T) {
	runner := fuseWorkerRunnerFunc(func(context.Context, worker.CommandRequest) (worker.CommandResult, error) {
		return worker.CommandResult{Stdout: bytes.Repeat([]byte("x"), fuseToolOutputLimit+1)}, nil
	})
	tools, err := newCommandFUSETools(runner, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := tools.run(t.Context(), "/usr/bin/true"); err == nil || !strings.Contains(err.Error(), "output exceeded limit") {
		t.Fatalf("oversized FUSE tool output error = %v", err)
	}
}

func TestRuntimePlanRejectsDisableLibraryValidationPolicy(t *testing.T) {
	home := t.TempDir()
	spec := RuntimePlanSpec{
		Application: SignedApplication{
			AppPath: testHelperAppPath(home), BundleID: "com.example.product", TeamID: "ABCDE12345",
			Runtime: SignedExecutable{ExecutableName: "ProductHelper", SigningIdentifier: "com.example.product"},
		},
		RuntimeDirectory: filepath.Join(home, "runtime"),
		Native:           testNativeRuntimeSpec(filepath.Join(home, "presentation")), BuildID: testBuildID,
		Readiness: StandardReadinessContract(),
		RuntimePolicy: EntitlementPolicy{RequiredEntitlements: map[string]trust.EntitlementRequirement{
			disableLibraryValidationEntitlement: {Match: trust.EntitlementBoolean, Boolean: true},
		}},
	}
	if _, err := newRuntimePlan(spec, home); err == nil || !strings.Contains(err.Error(), "forbidden") {
		t.Fatalf("disable-library-validation runtime policy = %v", err)
	}
}

func fuseBundleFixture(t *testing.T) (SignedApplication, string, string) {
	t.Helper()
	root := t.TempDir()
	app := SignedApplication{
		AppPath: filepath.Join(root, "Example.app"), BundleID: "com.example.product", TeamID: "ABCDE12345",
		Runtime: SignedExecutable{ExecutableName: "Example", SigningIdentifier: "com.example.product"},
	}
	if err := os.MkdirAll(app.AppPath, 0o755); err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "reviewed-libfuse-t.dylib")
	if err := os.WriteFile(source, []byte("reviewed-fuse-t"), 0o755); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte("reviewed-fuse-t"))
	return app, source, hex.EncodeToString(hash[:])
}
