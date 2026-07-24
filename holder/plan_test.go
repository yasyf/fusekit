package holder

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/trust"
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
		broker.PolicyDigest == (codeidentity.PolicyDigest{}) {
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
	policy.RequiredEntitlements["com.example.filesystem-runtime"] = trust.EntitlementRequirement{}
	requirement.RequiredEntitlements["com.example.filesystem-runtime"] = trust.EntitlementRequirement{}
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
		BrokerPolicyDigest:  codeidentity.PolicyDigest{1},
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
		{"broker policy digest", func(plan *DeploymentPlan) { plan.brokerDigest[0]++ }},
		{"runtime policy digest", func(plan *DeploymentPlan) { plan.runtimeDigest[0]++ }},
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
		{"missing broker digest", func(s *DeploymentPlanSpec) { s.BrokerPolicyDigest = codeidentity.PolicyDigest{} }},
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
		agent.LogPath != wantLog || agent.RestartPolicy != service.RestartAlways ||
		agent.LimitLoadToSessionType != service.SessionTypeAqua ||
		len(agent.AssociatedBundleIdentifiers) != 1 || agent.AssociatedBundleIdentifiers[0] != application.BundleID {
		t.Fatalf("agent = %#v", agent)
	}
	if _, err := agent.Plist(); err != nil {
		t.Fatalf("render desired agent: %v", err)
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
	for _, path := range []string{paths.Catalog, paths.Socket} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("derived path %q: %v", path, err)
		}
	}
	directoryInfo, err := os.Stat(paths.Directory)
	if err != nil {
		t.Fatal(err)
	}
	if directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("runtime directory mode = %#o, want 0700", directoryInfo.Mode().Perm())
	}
	socketInfo, err := os.Stat(paths.Socket)
	if err != nil {
		t.Fatal(err)
	}
	if socketInfo.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %#o, want 0600", socketInfo.Mode().Perm())
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

func TestProcessRegistryUsesProcessGeneration(t *testing.T) {
	want, err := proc.ProcessGeneration()
	if err != nil {
		t.Fatal(err)
	}
	registry, err := processRegistry(filepath.Join(t.TempDir(), "processes.db"))
	if err != nil {
		t.Fatal(err)
	}
	if registry.Generation != want {
		t.Fatalf("generation = %q, want %q", registry.Generation, want)
	}
	if _, ok := registry.Store.(*proc.FileStore); !ok {
		t.Fatalf("process store = %T, want *proc.FileStore", registry.Store)
	}
}

func TestNativeProcessIdentityRequiresDedicatedSession(t *testing.T) {
	valid := proc.Record{
		PID: 42, StartTime: "start", Boot: "boot", Generation: holderOwnerGeneration("generation"),
		RecoveryID: recoveryid.NativeMount, ProcessGroup: true, SessionID: 42,
	}
	if err := validateNativeProcessRecord(valid); err != nil {
		t.Fatal(err)
	}
	missingGeneration := valid
	missingGeneration.Generation = proc.OwnerGeneration{}
	if err := validateNativeProcessRecord(missingGeneration); !errors.Is(err, proc.ErrInvalidRecord) {
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
		RequiredEntitlements: map[string]trust.EntitlementRequirement{
			"com.example.filesystem-runtime": {Match: trust.EntitlementBoolean, Boolean: true},
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
