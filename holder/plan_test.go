package holder

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/deploy"
	"github.com/yasyf/daemonkit/launchd"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

const testBuildID = "test-build"

func testHelperAppPath(home string) string {
	return filepath.Join(home, "Applications", "ProductHelper.app")
}

func testNativeRuntimeSpec(root string) *NativeRuntimeSpec {
	return &NativeRuntimeSpec{PresentationRoot: root}
}

func testNativeDeploymentSpec(root string) *NativeDeploymentSpec {
	return &NativeDeploymentSpec{PresentationRoot: root}
}

func TestRuntimePlanKeepsConcretePolicyOnSignedSide(t *testing.T) {
	home := "/Users/example"
	policy := testEntitlementPolicy()
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.product", "ProductHelper"),
		RuntimeDirectory: filepath.Join(home, "Library", "Application Support", "Example", "FuseKit"),
		Native:           testNativeRuntimeSpec(filepath.Join(home, "Library", "Application Support", "Example", "Files")),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     policy, RuntimePolicy: policy,
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	deployment := plan.Deployment()
	broker, ok := deployment.Broker()
	if !ok || broker.CodeIdentity.SigningIdentifier != "com.example.product" ||
		broker.PolicyDigest == "" {
		t.Fatalf("deployment broker = %#v enabled=%t", broker, ok)
	}
	runtimeBroker, ok := plan.Broker()
	if !ok {
		t.Fatal("runtime broker is disabled")
	}
	requirement := runtimeBroker.Requirement
	if requirement.RequiredAppGroup != policy.RequiredAppGroup ||
		!requirement.RequiredEntitlements["com.example.filesystem-runtime"].Boolean {
		t.Fatalf("runtime requirement = %#v", requirement)
	}
	policy.RequiredEntitlements["com.example.filesystem-runtime"] = daemonkit.EntitlementRequirement{}
	requirement.RequiredEntitlements["com.example.filesystem-runtime"] = daemonkit.EntitlementRequirement{}
	immutable, _ := plan.Broker()
	if !immutable.Requirement.RequiredEntitlements["com.example.filesystem-runtime"].Boolean {
		t.Fatal("runtime plan entitlement policy mutated through caller map")
	}
}

func TestNativeOnlyPlanHasNoBrokerIdentity(t *testing.T) {
	home := "/Users/example"
	application := testSignedApplication(testHelperAppPath(home), "com.example.notes", "ProductHelper")
	application.Broker = SignedExecutable{}
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application: application, RuntimeDirectory: filepath.Join(home, "runtime"),
		Native: testNativeRuntimeSpec(filepath.Join(home, "presentation")), BuildID: testBuildID,
		Readiness: StandardReadinessContract(),
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	if broker, ok := plan.Broker(); ok {
		t.Fatalf("native-only runtime broker = %#v", broker)
	}
	if broker, ok := plan.Deployment().Broker(); ok || broker != (DeploymentBroker{}) {
		t.Fatalf("native-only deployment broker = %#v enabled=%t", broker, ok)
	}
	if plan.RuntimeExecutable() != filepath.Join(testHelperAppPath(home), "Contents", "MacOS", "ProductHelper") {
		t.Fatalf("runtime executable = %q", plan.RuntimeExecutable())
	}
	if err := plan.validate(); err != nil {
		t.Fatalf("validate native-only plan: %v", err)
	}
}

func TestFileProviderOnlyPlanOmitsNativePresentation(t *testing.T) {
	home := "/Users/example"
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.product", "ProductHelper"),
		RuntimeDirectory: filepath.Join(home, "runtime"),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     testEntitlementPolicy(),
		RuntimePolicy:    testEntitlementPolicy(),
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	if native, ok := plan.NativePresentation(); ok || native != (NativePresentation{}) {
		t.Fatalf("File Provider-only native presentation = %#v enabled=%t", native, ok)
	}
	if plan.Paths().PresentationRoot != "" {
		t.Fatalf("File Provider-only presentation root = %q", plan.Paths().PresentationRoot)
	}
	if library, digest, ok := plan.FUSELibrary(); ok || library != "" || digest != "" {
		t.Fatalf("File Provider-only FUSE library = %q %q enabled=%t", library, digest, ok)
	}
	if attestation, ok := plan.FUSEAttestation(); ok || attestation != (FUSEAttestation{}) {
		t.Fatalf("File Provider-only FUSE attestation = %#v enabled=%t", attestation, ok)
	}
	if err := plan.validate(); err != nil {
		t.Fatalf("validate File Provider-only plan: %v", err)
	}

	application := plan.Application()
	application.Broker = SignedExecutable{}
	if _, err := newRuntimePlan(RuntimePlanSpec{
		Application: application, RuntimeDirectory: filepath.Join(home, "other-runtime"),
		BuildID: testBuildID, Readiness: StandardReadinessContract(),
	}, home); err == nil {
		t.Fatal("plan without native or File Provider presentation was accepted")
	}
}

func TestRuntimePlanFUSEAttestationIsExactImmutableAndDigestSensitive(t *testing.T) {
	plan := runtimeTestPlan(t)
	plan.fuse.SignedSHA256 = strings.Repeat("12", 32)
	plan.fuse.OuterEntitlementsSHA256 = strings.Repeat("ab", 32)

	attestation, ok := plan.FUSEAttestation()
	if !ok {
		t.Fatal("native runtime has no FUSE attestation")
	}
	if got, want := attestation.SignedLibrarySHA256, [32]byte{
		0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12,
		0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12,
		0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12,
		0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12, 0x12,
	}; got != want {
		t.Fatalf("signed library digest = %x, want %x", got, want)
	}
	if got, want := attestation.OuterEntitlementsSHA256, [32]byte{
		0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
		0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
		0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
		0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab, 0xab,
	}; got != want {
		t.Fatalf("outer entitlement digest = %x, want %x", got, want)
	}

	attestation.SignedLibrarySHA256[0]++
	attestation.OuterEntitlementsSHA256[0]++
	again, ok := plan.FUSEAttestation()
	if !ok || again.SignedLibrarySHA256[0] != 0x12 || again.OuterEntitlementsSHA256[0] != 0xab {
		t.Fatalf("runtime attestation mutated through returned value: %#v enabled=%t", again, ok)
	}

	mutations := map[string]func(*RuntimePlan){
		"signed library": func(candidate *RuntimePlan) { candidate.fuse.SignedSHA256 = strings.Repeat("34", 32) },
		"outer entitlements": func(candidate *RuntimePlan) {
			candidate.fuse.OuterEntitlementsSHA256 = strings.Repeat("cd", 32)
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			candidate := plan
			mutate(&candidate)
			changed, ok := candidate.FUSEAttestation()
			if !ok || changed == again {
				t.Fatalf("%s mutation did not change attestation: %#v enabled=%t", name, changed, ok)
			}
		})
	}

	for name, mutate := range map[string]func(*RuntimePlan){
		"signed library": func(candidate *RuntimePlan) { candidate.fuse.SignedSHA256 = "not-a-digest" },
		"outer entitlements": func(candidate *RuntimePlan) {
			candidate.fuse.OuterEntitlementsSHA256 = strings.Repeat("ab", 33)
		},
	} {
		t.Run("malformed "+name, func(t *testing.T) {
			candidate := plan
			mutate(&candidate)
			if attestation, ok := candidate.FUSEAttestation(); ok || attestation != (FUSEAttestation{}) {
				t.Fatalf("malformed %s attestation = %#v enabled=%t", name, attestation, ok)
			}
		})
	}
}

func TestNativeOnlyPlanRejectsBrokerResidue(t *testing.T) {
	home := "/Users/example"
	application := testSignedApplication(testHelperAppPath(home), "com.example.notes", "ProductHelper")
	application.Broker = SignedExecutable{}
	runtimeSpec := RuntimePlanSpec{
		Application: application, RuntimeDirectory: filepath.Join(home, "runtime"),
		Native: testNativeRuntimeSpec(filepath.Join(home, "presentation")), BuildID: testBuildID,
		Readiness:    StandardReadinessContract(),
		BrokerPolicy: testEntitlementPolicy(),
	}
	if _, err := newRuntimePlan(runtimeSpec, home); err == nil {
		t.Fatal("native-only runtime accepted broker entitlement policy")
	}
	valid, err := newRuntimePlan(RuntimePlanSpec{
		Application: application, RuntimeDirectory: filepath.Join(home, "runtime"),
		Native: testNativeRuntimeSpec(filepath.Join(home, "presentation")), BuildID: testBuildID,
		Readiness: StandardReadinessContract(),
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	deploymentSpec := DeploymentPlanSpec{
		Application: application, RuntimeDirectory: filepath.Join(home, "runtime"),
		Native:              testNativeDeploymentSpec(filepath.Join(home, "presentation")),
		BuildID:             testBuildID,
		Readiness:           StandardReadinessContract(),
		RuntimePolicyDigest: valid.Deployment().RuntimePolicyDigest(),
		BrokerPolicyDigest:  daemonkit.PolicyDigest(strings.Repeat("1", 64)),
	}
	if _, err := newDeploymentPlan(deploymentSpec, home); err == nil {
		t.Fatal("native-only deployment accepted broker policy digest")
	}
}

func TestDeploymentPlanContainsOnlyCodeIdentityAndOpaqueDigests(t *testing.T) {
	runtime := runtimeTestPlan(t)
	deployment := runtime.Deployment()
	broker, ok := deployment.Broker()
	if !ok {
		t.Fatal("deployment broker is disabled")
	}
	rebuilt, err := newDeploymentPlan(DeploymentPlanSpec{
		Application: deployment.Application(), RuntimeDirectory: deployment.Paths().Directory,
		Native:              testNativeDeploymentSpec(deployment.Paths().PresentationRoot),
		BuildID:             deployment.BuildID(),
		Readiness:           deployment.Readiness(),
		BrokerPolicyDigest:  broker.PolicyDigest,
		RuntimePolicyDigest: deployment.RuntimePolicyDigest(),
	}, deployment.home)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(rebuilt, deployment) {
		t.Fatalf("deployment round trip = %#v, want %#v", rebuilt, deployment)
	}
}

func TestDeploymentPlanUsesRequiredExactPresentationRoot(t *testing.T) {
	home := "/Users/example"
	spec := deploymentTestSpec(home)
	spec.Native.PresentationRoot = filepath.Join(home, "accounts")
	plan, err := newDeploymentPlan(spec, home)
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.Paths().PresentationRoot; got != spec.Native.PresentationRoot {
		t.Fatalf("presentation root = %q, want %q", got, spec.Native.PresentationRoot)
	}
	if plan.Paths().PresentationRoot == filepath.Join(plan.Paths().Directory, "mount") {
		t.Fatal("presentation root was derived from the runtime directory")
	}

	otherSpec := spec
	otherSpec.Native = testNativeDeploymentSpec(filepath.Join(home, "other-accounts"))
	other, err := newDeploymentPlan(otherSpec, home)
	if err != nil {
		t.Fatal(err)
	}
	if plan.integrity == other.integrity {
		t.Fatal("different presentation roots produced identical deployment integrity")
	}
}

func TestDeploymentPlanRejectsUnsafePresentationRootTopology(t *testing.T) {
	home := "/Users/example"
	valid := deploymentTestSpec(home)
	userApp := filepath.Join(home, "Applications", "Example.app")
	tests := []struct {
		name   string
		mutate func(*DeploymentPlanSpec)
	}{
		{"missing", func(s *DeploymentPlanSpec) { s.Native.PresentationRoot = "" }},
		{"relative", func(s *DeploymentPlanSpec) { s.Native.PresentationRoot = "accounts" }},
		{"unclean", func(s *DeploymentPlanSpec) { s.Native.PresentationRoot = home + "/accounts/../presentation" }},
		{"nul", func(s *DeploymentPlanSpec) { s.Native.PresentationRoot = filepath.Join(home, "accounts") + "\x00" }},
		{"outside home", func(s *DeploymentPlanSpec) { s.Native.PresentationRoot = "/var/tmp/example" }},
		{"user home", func(s *DeploymentPlanSpec) { s.Native.PresentationRoot = home }},
		{"equal runtime", func(s *DeploymentPlanSpec) { s.Native.PresentationRoot = s.RuntimeDirectory }},
		{"below runtime", func(s *DeploymentPlanSpec) { s.Native.PresentationRoot = filepath.Join(s.RuntimeDirectory, "mount") }},
		{"contains runtime", func(s *DeploymentPlanSpec) {
			s.Native.PresentationRoot = filepath.Join(home, "container")
			s.RuntimeDirectory = filepath.Join(s.Native.PresentationRoot, "runtime")
		}},
		{"case-folded runtime", func(s *DeploymentPlanSpec) {
			s.RuntimeDirectory = filepath.Join(home, "State")
			s.Native.PresentationRoot = filepath.Join(home, "state", "mount")
		}},
		{"normalization-folded runtime", func(s *DeploymentPlanSpec) {
			s.RuntimeDirectory = filepath.Join(home, "Caf\u00e9")
			s.Native.PresentationRoot = filepath.Join(home, "Cafe\u0301", "mount")
		}},
		{"contains app", func(s *DeploymentPlanSpec) {
			s.Application.AppPath = userApp
			s.Native.PresentationRoot = filepath.Dir(userApp)
		}},
		{"below app", func(s *DeploymentPlanSpec) {
			s.Application.AppPath = userApp
			s.Native.PresentationRoot = filepath.Join(userApp, "Files")
		}},
		{"runtime contains app", func(s *DeploymentPlanSpec) {
			s.Application.AppPath = userApp
			s.RuntimeDirectory = filepath.Dir(userApp)
		}},
		{"runtime below app", func(s *DeploymentPlanSpec) {
			s.Application.AppPath = userApp
			s.RuntimeDirectory = filepath.Join(userApp, "Runtime")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := valid
			test.mutate(&spec)
			if _, err := newDeploymentPlan(spec, home); err == nil {
				t.Fatal("unsafe plan topology accepted")
			}
		})
	}
}

func TestValidatePresentationRootAncestorsRejectsSymlink(t *testing.T) {
	home := shortTempDir(t)
	target := shortTempDir(t)
	link := filepath.Join(home, "redirect")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := validatePresentationRootAncestors(home, filepath.Join(link, "accounts")); err == nil {
		t.Fatal("symlink presentation-root ancestor accepted")
	}
}

func TestSourceCapabilityPropagatesAndChangesIntegrity(t *testing.T) {
	home := "/Users/example"
	base := RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.product", "ProductHelper"),
		RuntimeDirectory: filepath.Join(home, "runtime"),
		Native:           testNativeRuntimeSpec(filepath.Join(home, "presentation")),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     testEntitlementPolicy(), RuntimePolicy: testEntitlementPolicy(),
	}
	mountOnly, err := newRuntimePlan(base, home)
	if err != nil {
		t.Fatal(err)
	}
	base.SourceCapable = true
	sourceCapable, err := newRuntimePlan(base, home)
	if err != nil {
		t.Fatal(err)
	}
	if mountOnly.SourceCapable() || mountOnly.Deployment().SourceCapable() {
		t.Fatal("mount-only plan reports source capability")
	}
	if !sourceCapable.SourceCapable() || !sourceCapable.Deployment().SourceCapable() {
		t.Fatal("source capability did not propagate to both plans")
	}
	if mountOnly.Deployment().integrity == sourceCapable.Deployment().integrity {
		t.Fatal("source capability did not change deployment integrity")
	}
	if err := sourceCapable.validate(); err != nil {
		t.Fatalf("validate source-capable plan: %v", err)
	}
}

func TestRuntimeAndDeploymentPlansRejectDrift(t *testing.T) {
	runtime := runtimeTestPlan(t)
	runtime.broker.RequiredAppGroup = "changed"
	if err := runtime.validate(); err == nil {
		t.Fatal("runtime plan accepted changed concrete policy")
	}
	tests := []struct {
		name   string
		mutate func(*DeploymentPlan)
	}{
		{"broker policy digest", func(plan *DeploymentPlan) { plan.brokerDigest += "0" }},
		{"runtime policy digest", func(plan *DeploymentPlan) { plan.runtimeDigest += "0" }},
		{"build identity", func(plan *DeploymentPlan) { plan.buildID = "changed-build" }},
		{"readiness contract", func(plan *DeploymentPlan) { plan.readiness.startup++ }},
		{"source capability", func(plan *DeploymentPlan) { plan.sourceCapable = !plan.sourceCapable }},
		{"broker code identity", func(plan *DeploymentPlan) { plan.brokerCode.SigningIdentifier = "com.example.changed" }},
		{"runtime executable path", func(plan *DeploymentPlan) { plan.application.Runtime.ExecutableName = "Changed" }},
		{"presentation root", func(plan *DeploymentPlan) { plan.paths.PresentationRoot += "-changed" }},
		{"launch agent environment", func(plan *DeploymentPlan) {
			plan.agent.Env["FUSEKIT_BUILD_ID"] = "changed-build"
		}},
		{"launch agent bundle attribution", func(plan *DeploymentPlan) {
			plan.agent.AssociatedBundleIdentifiers[0] = "com.example.changed"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			deployment := runtimeTestPlan(t).Deployment()
			test.mutate(&deployment)
			if err := deployment.validate(); err == nil {
				t.Fatal("deployment plan accepted daemon-facing contract drift")
			}
		})
	}
}

func TestDeploymentPlanRejectsUnstableIdentityPathsAndMissingDigest(t *testing.T) {
	home := "/Users/example"
	valid := deploymentTestSpec(home)
	tests := []struct {
		name   string
		mutate func(*DeploymentPlanSpec)
	}{
		{"relative app", func(s *DeploymentPlanSpec) { s.Application.AppPath = "Example.app" }},
		{"temporary app", func(s *DeploymentPlanSpec) { s.Application.AppPath = "/tmp/Example.app" }},
		{"system application", func(s *DeploymentPlanSpec) { s.Application.AppPath = "/Applications/ProductHelper.app" }},
		{"holder-named application", func(s *DeploymentPlanSpec) {
			s.Application.AppPath = filepath.Join(home, "Applications", "ProductHolderHelper.app")
		}},
		{"wrong bundle", func(s *DeploymentPlanSpec) { s.Application.Broker.SigningIdentifier = "com.example.other" }},
		{"invalid team", func(s *DeploymentPlanSpec) { s.Application.TeamID = "abc" }},
		{"missing build identity", func(s *DeploymentPlanSpec) { s.BuildID = "" }},
		{"control build identity", func(s *DeploymentPlanSpec) { s.BuildID = "bad\nbuild" }},
		{"invalid utf8 build identity", func(s *DeploymentPlanSpec) { s.BuildID = string([]byte{0xff}) }},
		{"oversized build identity", func(s *DeploymentPlanSpec) { s.BuildID = strings.Repeat("b", 256) }},
		{"runtime outside home", func(s *DeploymentPlanSpec) { s.RuntimeDirectory = "/var/run/example" }},
		{"missing broker digest", func(s *DeploymentPlanSpec) { s.BrokerPolicyDigest = "" }},
		{"socket too long", func(s *DeploymentPlanSpec) {
			s.RuntimeDirectory = filepath.Join(home, strings.Repeat("x", maxUnixSocketPath))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := valid
			test.mutate(&spec)
			if _, err := newDeploymentPlan(spec, home); err == nil {
				t.Fatal("invalid deployment plan accepted")
			}
		})
	}
}

func TestDeploymentPlanAcceptsMeaningfulProductApplication(t *testing.T) {
	home := "/Users/example"
	spec := deploymentTestSpec(home)
	spec.Application.AppPath = filepath.Join(home, "Applications", "CCPoolStatus.app")
	if _, err := newDeploymentPlan(spec, home); err != nil {
		t.Fatalf("meaningful product app rejected: %v", err)
	}
}

func TestDeploymentPlanErrorsUsePublicRuntimeName(t *testing.T) {
	home := "/Users/example"
	spec := deploymentTestSpec(home)
	spec.Application.AppPath = "/Applications/ProductHelper.app"
	_, err := newDeploymentPlan(spec, home)
	if err == nil {
		t.Fatal("system application was accepted")
	}
	if message := err.Error(); !strings.HasPrefix(message, "FuseKit runtime:") || strings.Contains(strings.ToLower(message), "holder") {
		t.Fatalf("public runtime error = %q", message)
	}
}

func TestDeploymentPlanChecksWorstCaseSourceAuthoritySocketPath(t *testing.T) {
	home := "/Users/example"
	suffix := filepath.Join("source-observer-0000000000", "observer.sock")
	for _, test := range []struct {
		name    string
		length  int
		wantErr bool
	}{
		{name: "99 bytes accepted", length: 99},
		{name: "100 bytes rejected", length: 100, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			padding := test.length - len(home) - len(suffix) - 2
			runtimeDirectory := filepath.Join(home, strings.Repeat("r", padding))
			if got := len(filepath.Join(runtimeDirectory, suffix)); got != test.length {
				t.Fatalf("source socket length = %d, want %d", got, test.length)
			}
			spec := deploymentTestSpec(home)
			spec.RuntimeDirectory = runtimeDirectory
			_, err := newDeploymentPlan(spec, home)
			if (err != nil) != test.wantErr {
				t.Fatalf("newDeploymentPlan() error = %v, wantErr %t", err, test.wantErr)
			}
		})
	}
}

func TestRuntimePlanRejectsDifferentPoliciesForOneExecutable(t *testing.T) {
	home := "/Users/example"
	spec := RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.product", "ProductHelper"),
		RuntimeDirectory: filepath.Join(home, "runtime"),
		Native:           testNativeRuntimeSpec(filepath.Join(home, "presentation")),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     testEntitlementPolicy(), RuntimePolicy: testEntitlementPolicy(),
	}
	spec.RuntimePolicy.RequiredAppGroup = "ABCDE12345.changed"
	if _, err := newRuntimePlan(spec, home); err == nil {
		t.Fatal("one executable accepted different entitlement policies")
	}
}

func TestNewDeploymentPlanRejectsMissingInstalledExecutable(t *testing.T) {
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	spec := deploymentTestSpec(account.HomeDir)
	spec.RuntimeDirectory = filepath.Join(account.HomeDir, ".fusekit-plan-account-home-test")
	if _, err := NewDeploymentPlan(spec); err == nil {
		t.Fatal("missing installed application executable accepted")
	}
}

func TestNewCandidatePlanAcceptsMissingFixedTargetOnlyForPackagePlanning(t *testing.T) {
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	suffix := filepath.Base(root)
	spec := deploymentTestSpec(account.HomeDir)
	spec.Application.AppPath = filepath.Join(
		account.HomeDir,
		"Applications",
		"FuseKitCandidateHelper-"+suffix+".app",
	)
	spec.RuntimeDirectory = filepath.Join(account.HomeDir, ".fusekit-candidate-"+suffix)
	spec.Native.PresentationRoot = filepath.Join(account.HomeDir, "FuseKitCandidate-"+suffix)
	if _, err := os.Lstat(spec.Application.AppPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fixed target unexpectedly exists: %v", err)
	}

	source := filepath.Join(root, "FuseKitCandidateHelper.app")
	writeCandidateApplication(t, source, spec.Application)
	candidate, err := NewCandidatePlan(spec, source)
	if err != nil {
		t.Fatalf("plan missing-target candidate: %v", err)
	}
	digest, err := bundleTreeDigest(source)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Candidate() != (deploy.Candidate{
		Source: source, Version: testCandidateVersion, Digest: digest,
	}) {
		t.Fatalf("sealed candidate = %#v", candidate.Candidate())
	}
	plan, err := newDeploymentPlan(spec, account.HomeDir)
	if err != nil {
		t.Fatal(err)
	}
	agents := candidate.Agents()
	installedProgram := bundle.ExePath(spec.Application.AppPath, spec.Application.Runtime.ExecutableName)
	if len(agents) != 1 || !sameAgent(agents[0], plan.Agent()) || agents[0].Program != installedProgram {
		t.Fatalf("candidate agents = %#v, want one agent programmed at %q", agents, installedProgram)
	}
	if !reflect.DeepEqual(candidate.Requirement(), daemonkit.Requirement{
		TeamID:            spec.Application.TeamID,
		SigningIdentifier: spec.Application.Runtime.SigningIdentifier,
	}) {
		t.Fatalf("candidate requirement = %#v", candidate.Requirement())
	}
	if candidate.PolicyDigest() != spec.RuntimePolicyDigest {
		t.Fatalf("candidate policy digest = %q, want %q", candidate.PolicyDigest(), spec.RuntimePolicyDigest)
	}
	if _, err := NewDeploymentPlan(spec); err == nil {
		t.Fatal("installed deployment planning accepted the missing fixed target")
	}
	if _, err := NewRuntimePlan(RuntimePlanSpec{
		Application: spec.Application, RuntimeDirectory: spec.RuntimeDirectory,
		Native:  &NativeRuntimeSpec{PresentationRoot: spec.Native.PresentationRoot},
		BuildID: spec.BuildID, Readiness: spec.Readiness, SourceCapable: spec.SourceCapable,
		BrokerPolicy: testEntitlementPolicy(), RuntimePolicy: testEntitlementPolicy(),
	}); err == nil {
		t.Fatal("installed runtime planning accepted the missing fixed target")
	}
	writeCandidateApplication(t, spec.Application.AppPath, spec.Application)
	t.Cleanup(func() {
		if err := os.RemoveAll(spec.Application.AppPath); err != nil {
			t.Errorf("remove fixed target fixture: %v", err)
		}
	})
	if _, err := NewDeploymentPlan(spec); err != nil {
		t.Fatalf("plan installed fixed target: %v", err)
	}
}

func TestNewCandidatePlanRejectsInexactPackagedSource(t *testing.T) {
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spec := deploymentTestSpec(account.HomeDir)
	spec.Application.AppPath = filepath.Join(
		account.HomeDir,
		"Applications",
		"FuseKitCandidateHelper-"+filepath.Base(root)+".app",
	)
	spec.RuntimeDirectory = filepath.Join(account.HomeDir, ".fusekit-candidate-"+filepath.Base(root))
	spec.Native.PresentationRoot = filepath.Join(account.HomeDir, "FuseKitCandidate-"+filepath.Base(root))

	missingExecutable := filepath.Join(root, "MissingExecutable.app")
	if err := os.MkdirAll(missingExecutable, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewCandidatePlan(spec, missingExecutable); err == nil {
		t.Fatal("candidate without the declared runtime executable was accepted")
	}

	wrongKind := filepath.Join(root, "NotAnApplication")
	writeCandidateApplication(t, wrongKind, spec.Application)
	if _, err := NewCandidatePlan(spec, wrongKind); err == nil {
		t.Fatal("candidate without an .app root was accepted")
	}

	realSource := filepath.Join(root, "RealCandidate.app")
	writeCandidateApplication(t, realSource, spec.Application)
	symlinkSource := filepath.Join(root, "SymlinkCandidate.app")
	if err := os.Symlink(realSource, symlinkSource); err != nil {
		t.Fatal(err)
	}
	if _, err := NewCandidatePlan(spec, symlinkSource); err == nil {
		t.Fatal("candidate with a symbolic-link application root was accepted")
	}
}

// deploy.Candidate demands a bundle-tree digest computed by an algorithm the
// package does not export, so this golden is the cross-implementation pin: it
// is what deploy's own bundleTreeDigest produced for this exact tree.
func TestBundleTreeDigestMatchesDeployGoldenTree(t *testing.T) {
	const golden = "adaf3a6cda02e61533b1b937387fd5604cc5ade42654853f7e825ed2c7b85a7a"
	digest, err := bundleTreeDigest(writeGoldenBundleTree(t))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := deploy.ParseSHA256(golden)
	if err != nil {
		t.Fatal(err)
	}
	if digest != parsed {
		t.Fatalf("bundle tree digest = %s, want deploy's %s", digest, golden)
	}
}

func TestBundleTreeDigestCoversSymlinkTargets(t *testing.T) {
	root := writeGoldenBundleTree(t)
	link := filepath.Join(root, "Contents", "Current")
	if err := os.Symlink("MacOS/Golden", link); err != nil {
		t.Fatal(err)
	}
	first, err := bundleTreeDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("MacOS/Other", link); err != nil {
		t.Fatal(err)
	}
	second, err := bundleTreeDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("retargeted symlink produced an identical bundle tree digest")
	}
}

func writeGoldenBundleTree(t *testing.T) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "Golden.app")
	for _, dir := range []string{root, filepath.Join(root, "Contents"), filepath.Join(root, "Contents", "MacOS")} {
		if err := os.Mkdir(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for _, file := range []struct {
		path string
		body string
		mode os.FileMode
	}{
		{filepath.Join(root, "Contents", "Info.plist"), "<plist/>\n", 0o644},
		{filepath.Join(root, "Contents", "MacOS", "Golden"), "fixture", 0o755},
	} {
		if err := os.WriteFile(file.path, []byte(file.body), file.mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(file.path, file.mode); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

const testCandidateVersion = "4.2.1"

func writeCandidateApplication(t *testing.T, appPath string, application SignedApplication) {
	t.Helper()
	info := filepath.Join(appPath, "Contents", "Info.plist")
	if err := os.MkdirAll(filepath.Dir(info), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(info, []byte(`<?xml version="1.0" encoding="UTF-8"?>
<plist version="1.0"><dict>
	<key>CFBundleShortVersionString</key><string>`+testCandidateVersion+`</string>
</dict></plist>
`), 0o644); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]struct{}, 2)
	for _, executable := range []SignedExecutable{application.Runtime, application.Broker} {
		if executable == (SignedExecutable{}) {
			continue
		}
		path := bundle.ExePath(appPath, executable.ExecutableName)
		if _, exists := seen[path]; exists {
			continue
		}
		seen[path] = struct{}{}
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("fixture"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
}

func TestValidateInstalledApplicationRequiresRealExecutablePath(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	application := testSignedApplication(filepath.Join(root, "Example.app"), "com.example.product", "Example")
	executable := bundle.ExePath(application.AppPath, application.Runtime.ExecutableName)
	if err := os.MkdirAll(filepath.Dir(executable), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := validateInstalledApplication(application); err != nil {
		t.Fatalf("validate real executable: %v", err)
	}

	if err := os.Remove(executable); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, executable); err != nil {
		t.Fatal(err)
	}
	if err := validateInstalledApplication(application); err == nil {
		t.Fatal("symbolic-link executable accepted")
	}
}

func TestValidateInstalledApplicationRejectsSymlinkAncestor(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "real-app")
	executable := filepath.Join(target, "Contents", "MacOS", "Example")
	if err := os.MkdirAll(filepath.Dir(executable), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(executable, []byte("fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	application := testSignedApplication(filepath.Join(root, "Example.app"), "com.example.product", "Example")
	if err := os.Symlink(target, application.AppPath); err != nil {
		t.Fatal(err)
	}
	if err := validateInstalledApplication(application); err == nil {
		t.Fatal("symbolic-link application accepted")
	}
}

func TestDeploymentAgentIsExactDetachedFixedApplicationDesiredState(t *testing.T) {
	deployment := runtimeTestPlan(t).Deployment()
	agent := deployment.Agent()
	application := deployment.Application()
	wantLog := filepath.Join(deployment.Paths().Directory, "holder.log")
	if agent.Label != application.BundleID+".fusekit" || agent.Program != deployment.RuntimeExecutable() ||
		len(agent.Args) != 0 ||
		agent.Env["FUSEKIT_BUILD_ID"] != deployment.BuildID() ||
		agent.LogPath != wantLog || agent.RestartPolicy != launchd.RestartAlways ||
		len(agent.AssociatedBundleIdentifiers) != 1 || agent.AssociatedBundleIdentifiers[0] != application.BundleID {
		t.Fatalf("agent = %#v", agent)
	}
	plist, err := agent.Plist()
	if err != nil {
		t.Fatalf("render desired agent: %v", err)
	}
	if strings.Contains(string(plist), "LimitLoadToSessionType") {
		t.Fatalf("desired agent restricts session type: %s", plist)
	}
	agent.Args = append(agent.Args, "mutated")
	agent.Env["FUSEKIT_BUILD_ID"] = "mutated"
	agent.AssociatedBundleIdentifiers[0] = "com.example.mutated"
	if len(deployment.Agent().Args) != 0 || deployment.Agent().Env["FUSEKIT_BUILD_ID"] != deployment.BuildID() ||
		deployment.Agent().AssociatedBundleIdentifiers[0] != application.BundleID {
		t.Fatal("caller mutated deployment agent")
	}
}

func TestDeploymentBuildIdentityChangesOnlyReloadDesiredState(t *testing.T) {
	home := "/Users/example"
	firstSpec := deploymentTestSpec(home)
	secondSpec := firstSpec
	secondSpec.BuildID = "next-build"
	first, err := newDeploymentPlan(firstSpec, home)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newDeploymentPlan(secondSpec, home)
	if err != nil {
		t.Fatal(err)
	}
	if first.integrity == second.integrity {
		t.Fatal("different build identities produced identical deployment integrity")
	}
	if first.RuntimeExecutable() != second.RuntimeExecutable() {
		t.Fatal("build identity changed fixed runtime executable")
	}
	if first.Agent().Env["FUSEKIT_BUILD_ID"] == second.Agent().Env["FUSEKIT_BUILD_ID"] {
		t.Fatal("build identity did not change desired launch state")
	}
}

func TestRuntimeUsesPlanPathsAndPrivateSocket(t *testing.T) {
	directory := shortTempDir(t)
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	native := newTestNative(nil)
	config := testConfig(directory, "v1.0.0", native)
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitRuntimeReady(t, runtime, done)

	paths := config.Plan.Paths()
	if _, err := os.Stat(paths.Catalog); err != nil {
		t.Fatalf("derived path %q: %v", paths.Catalog, err)
	}
	directoryInfo, err := os.Stat(paths.Directory)
	if err != nil {
		t.Fatal(err)
	}
	if directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("runtime directory mode = %#o, want 0700", directoryInfo.Mode().Perm())
	}
	socketInfo, err := os.Stat(runtime.socket)
	if err != nil {
		t.Fatalf("daemon socket %q: %v", runtime.socket, err)
	}
	if socketInfo.Mode().Type() != os.ModeSocket || socketInfo.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %#o, want a 0600 socket", socketInfo.Mode())
	}
	closeRuntime(t, runtime, done)
}

func TestRuntimeRejectsSymlinkRuntimeDirectory(t *testing.T) {
	parent := shortTempDir(t)
	target := filepath.Join(parent, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(parent, "runtime")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	config := testConfig(symlink, "v1.0.0", newTestNative(nil))
	if _, err := New(t.Context(), config); err == nil {
		t.Fatal("symlink runtime directory accepted")
	}
}

func TestRuntimeRejectsSymlinkRuntimeDirectoryAncestor(t *testing.T) {
	home := shortTempDir(t)
	target := shortTempDir(t)
	link := filepath.Join(home, "redirect")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	runtimeDirectory := filepath.Join(link, "runtime")
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.holder", "ProductHelper"),
		RuntimeDirectory: runtimeDirectory,
		Native:           testNativeRuntimeSpec(filepath.Join(home, "presentation")),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     testEntitlementPolicy(), RuntimePolicy: testEntitlementPolicy(),
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	config := testConfig(filepath.Join(home, "safe"), "v1.0.0", newTestNative(nil))
	config.Plan = plan
	if _, err := New(t.Context(), config); err == nil {
		t.Fatal("symlink runtime directory ancestor accepted")
	}
	if _, err := os.Stat(filepath.Join(target, "runtime")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("escaped runtime directory created: %v", err)
	}
}

func TestProcessLedgerStampsAFreshGenerationOnDurableRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "processes.db")
	ledger, err := openProcessLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if ledger.Generation() == (catalog.ProcessGeneration{}) {
		t.Fatal("process ledger generation is zero")
	}
	record, err := ledger.RegisterOwner(recoveryid.NativeMount)
	if err != nil {
		t.Fatal(err)
	}
	if record.Generation != ledger.Generation() || record.PID != os.Getpid() ||
		record.RecoveryID != recoveryid.NativeMount || record.ProcessGroup || record.SessionID != 0 {
		t.Fatalf("owner record = %#v, want this process under the ledger generation", record)
	}
	reopened, err := openProcessLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Generation() == ledger.Generation() {
		t.Fatalf("reopened generation = %x, want a generation distinct from %x", reopened.Generation(), ledger.Generation())
	}
	if len(reopened.state.Records) != 1 || reopened.state.Records[0] != record {
		t.Fatalf("reopened ledger records = %+v, want the durable owner record", reopened.state.Records)
	}
}

func TestNativeProcessIdentityRequiresDedicatedSession(t *testing.T) {
	valid := catalog.ProcessRecord{
		PID: 42, StartTime: "start", Boot: "boot", Generation: holderOwnerGeneration("generation"),
		RecoveryID: recoveryid.NativeMount, ProcessGroup: true, SessionID: 42,
	}
	if err := validateNativeProcessRecord(valid); err != nil {
		t.Fatal(err)
	}
	missingGeneration := valid
	missingGeneration.Generation = catalog.ProcessGeneration{}
	if err := validateNativeProcessRecord(missingGeneration); !errors.Is(err, catalog.ErrInvalidObject) {
		t.Fatalf("missing generation = %v", err)
	}
	wrongSession := valid
	wrongSession.SessionID++
	if err := validateNativeProcessRecord(wrongSession); err == nil {
		t.Fatal("foreign process session accepted")
	}
	noGroup := valid
	noGroup.ProcessGroup = false
	noGroup.SessionID = 0
	if err := validateNativeProcessRecord(noGroup); err == nil {
		t.Fatal("non-group native process accepted")
	}
}

func testSignedApplication(path, bundleID, executable string) SignedApplication {
	role := SignedExecutable{ExecutableName: executable, SigningIdentifier: bundleID}
	return SignedApplication{
		AppPath: path, BundleID: bundleID, TeamID: "ABCDE12345",
		Broker: role, Runtime: role,
	}
}

func testEntitlementPolicy() EntitlementPolicy {
	return EntitlementPolicy{
		RequiredAppGroup: "ABCDE12345.example",
		RequiredEntitlements: map[string]daemonkit.EntitlementRequirement{
			"com.example.filesystem-runtime": {Match: daemonkit.EntitlementBoolean, Boolean: true},
		},
	}
}

func runtimeTestPlan(t *testing.T) RuntimePlan {
	t.Helper()
	home := shortTempDir(t)
	plan, err := newRuntimePlan(RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.product", "ProductHelper"),
		RuntimeDirectory: filepath.Join(home, "fusekit"),
		Native:           testNativeRuntimeSpec(filepath.Join(home, "presentation")),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     testEntitlementPolicy(), RuntimePolicy: testEntitlementPolicy(),
	}, home)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func deploymentTestSpec(home string) DeploymentPlanSpec {
	runtime, err := newRuntimePlan(RuntimePlanSpec{
		Application:      testSignedApplication(testHelperAppPath(home), "com.example.product", "ProductHelper"),
		RuntimeDirectory: filepath.Join(home, "runtime"),
		Native:           testNativeRuntimeSpec(filepath.Join(home, "presentation")),
		BuildID:          testBuildID,
		Readiness:        StandardReadinessContract(),
		BrokerPolicy:     testEntitlementPolicy(), RuntimePolicy: testEntitlementPolicy(),
	}, home)
	if err != nil {
		panic(err)
	}
	deployment := runtime.Deployment()
	broker, ok := deployment.Broker()
	if !ok {
		panic("test deployment broker is disabled")
	}
	return DeploymentPlanSpec{
		Application: deployment.Application(), RuntimeDirectory: deployment.Paths().Directory,
		Native:              testNativeDeploymentSpec(deployment.Paths().PresentationRoot),
		BuildID:             deployment.BuildID(),
		Readiness:           deployment.Readiness(),
		SourceCapable:       deployment.SourceCapable(),
		BrokerPolicyDigest:  broker.PolicyDigest,
		RuntimePolicyDigest: deployment.RuntimePolicyDigest(),
	}
}
