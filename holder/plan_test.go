package holder

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/trust"
	"github.com/yasyf/fusekit/catalog"
)

func TestPlanDerivesImmutableFixedApplicationContract(t *testing.T) {
	home := "/Users/example"
	entitlements := map[string]trust.EntitlementRequirement{
		"com.apple.security.application-groups": {
			Match: trust.EntitlementStringArrayContains, String: "ABCDE12345.example",
		},
		"com.example.filesystem-runtime": {Match: trust.EntitlementBoolean, Boolean: true},
	}
	plan, err := newPlan(PlanSpec{
		Application: SignedApplication{
			AppPath: "/Applications/Example.app", BundleID: "com.example.product",
			TeamID: "ABCDE12345", ExecutableName: "ExampleRuntime", SigningIdentifier: "com.example.runtime",
			RequiredEntitlements: entitlements,
		},
		RuntimeDirectory: filepath.Join(home, "Library", "Application Support", "Example", "FuseKit"),
	}, home)
	if err != nil {
		t.Fatal(err)
	}

	wantDirectory := filepath.Join(home, "Library", "Application Support", "Example", "FuseKit")
	wantPaths := RuntimePaths{
		Directory: wantDirectory, Socket: filepath.Join(wantDirectory, "fusekit.sock"),
		Catalog:          filepath.Join(wantDirectory, "catalog.sqlite"),
		PresentationRoot: filepath.Join(wantDirectory, "mount"),
		ProcessStore:     filepath.Join(wantDirectory, "processes.json"),
	}
	if got := plan.Paths(); got != wantPaths {
		t.Fatalf("paths = %#v, want %#v", got, wantPaths)
	}
	if got, want := plan.Executable(), "/Applications/Example.app/Contents/MacOS/ExampleRuntime"; got != want {
		t.Fatalf("executable = %q, want %q", got, want)
	}
	keepAlive := plan.Service()
	if keepAlive.Label != "com.example.product.fusekit" || keepAlive.AppPath != "/Applications/Example.app" ||
		keepAlive.BundleID != "com.example.product" || keepAlive.RestartPolicy != service.RestartAlways {
		t.Fatalf("service = %#v", keepAlive)
	}
	requirement := plan.Requirement()
	if requirement.TeamID != "ABCDE12345" || requirement.SigningIdentifier != "com.example.runtime" {
		t.Fatalf("requirement identity = %#v", requirement)
	}
	if _, err := requirement.DRString(); err != nil {
		t.Fatalf("designated requirement: %v", err)
	}

	entitlements["com.example.filesystem-runtime"] = trust.EntitlementRequirement{Match: trust.EntitlementBoolean}
	application := plan.Application()
	delete(application.RequiredEntitlements, "com.apple.security.application-groups")
	requirement.RequiredEntitlements["com.example.filesystem-runtime"] = trust.EntitlementRequirement{}
	if got := plan.Application().RequiredEntitlements; len(got) != 2 || !got["com.example.filesystem-runtime"].Boolean {
		t.Fatalf("plan application mutated through caller map: %#v", got)
	}
	if got := plan.Requirement().RequiredEntitlements; len(got) != 2 || !got["com.example.filesystem-runtime"].Boolean {
		t.Fatalf("plan requirement mutated through caller map: %#v", got)
	}
}

func TestPlanRejectsUnstableIdentityAndRuntimePaths(t *testing.T) {
	home := "/Users/example"
	valid := PlanSpec{
		Application: SignedApplication{
			AppPath: "/Applications/Example.app", BundleID: "com.example.product",
			TeamID: "ABCDE12345", ExecutableName: "Example", SigningIdentifier: "com.example.runtime",
		},
		RuntimeDirectory: filepath.Join(home, "Library", "Application Support", "Example", "FuseKit"),
	}
	tests := []struct {
		name   string
		mutate func(*PlanSpec)
	}{
		{name: "relative app", mutate: func(s *PlanSpec) { s.Application.AppPath = "Example.app" }},
		{name: "temporary app", mutate: func(s *PlanSpec) { s.Application.AppPath = "/tmp/Example.app" }},
		{name: "nested application", mutate: func(s *PlanSpec) { s.Application.AppPath = "/Applications/releases/Example.app" }},
		{name: "undotted bundle", mutate: func(s *PlanSpec) { s.Application.BundleID = "Example" }},
		{name: "undotted signing identifier", mutate: func(s *PlanSpec) { s.Application.SigningIdentifier = "Example" }},
		{name: "invalid team", mutate: func(s *PlanSpec) { s.Application.TeamID = "abc" }},
		{name: "executable path", mutate: func(s *PlanSpec) { s.Application.ExecutableName = "bin/Example" }},
		{name: "runtime is home", mutate: func(s *PlanSpec) { s.RuntimeDirectory = home }},
		{name: "runtime outside home", mutate: func(s *PlanSpec) { s.RuntimeDirectory = "/var/run/example" }},
		{name: "unclean runtime", mutate: func(s *PlanSpec) { s.RuntimeDirectory += "/../FuseKit" }},
		{name: "invalid entitlement", mutate: func(s *PlanSpec) {
			s.Application.RequiredEntitlements = map[string]trust.EntitlementRequirement{
				"com.example.role": {Match: trust.EntitlementString},
			}
		}},
		{name: "socket too long", mutate: func(s *PlanSpec) {
			s.RuntimeDirectory = filepath.Join(home, strings.Repeat("x", maxUnixSocketPath))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := valid
			test.mutate(&spec)
			if _, err := newPlan(spec, home); err == nil {
				t.Fatal("invalid plan accepted")
			}
		})
	}
}

func TestPlanAllowsPerUserApplicationsDirectory(t *testing.T) {
	home := "/Users/example"
	spec := PlanSpec{
		Application: SignedApplication{
			AppPath: filepath.Join(home, "Applications", "Example.app"), BundleID: "com.example.product",
			TeamID: "ABCDE12345", ExecutableName: "Example", SigningIdentifier: "com.example.runtime",
		},
		RuntimeDirectory: filepath.Join(home, "Library", "Application Support", "Example", "FuseKit"),
	}
	if _, err := newPlan(spec, home); err != nil {
		t.Fatal(err)
	}
}

func TestNewPlanUsesAccountHomeInsteadOfEnvironment(t *testing.T) {
	account, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	runtimeDirectory := filepath.Join(account.HomeDir, ".fusekit-plan-account-home-test")
	plan, err := NewPlan(PlanSpec{
		Application: SignedApplication{
			AppPath: "/Applications/Example.app", BundleID: "com.example.product",
			TeamID: "ABCDE12345", ExecutableName: "Example", SigningIdentifier: "com.example.runtime",
		},
		RuntimeDirectory: runtimeDirectory,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Paths().Directory != runtimeDirectory {
		t.Fatalf("runtime directory = %q, want account-anchored %q", plan.Paths().Directory, runtimeDirectory)
	}
}

func TestPlanServiceWritesPrivatePerUserKeepAlive(t *testing.T) {
	userHome := t.TempDir()
	t.Setenv("HOME", userHome)
	planHome := "/tmp/fusekit-plan-home"
	plan, err := newPlan(PlanSpec{
		Application: SignedApplication{
			AppPath: "/Applications/Example.app", BundleID: "com.example.product",
			TeamID: "ABCDE12345", ExecutableName: "Example", SigningIdentifier: "com.example.runtime",
		},
		RuntimeDirectory: filepath.Join(planHome, "runtime"),
	}, planHome)
	if err != nil {
		t.Fatal(err)
	}
	path, err := plan.Service().WritePlist()
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("plist mode = %#o, want 0600", info.Mode().Perm())
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"/usr/bin/open", "/Applications/Example.app", "com.example.product"} {
		if !strings.Contains(string(body), required) {
			t.Fatalf("plist does not contain %q", required)
		}
	}
}

func TestRuntimeUsesPlanPathsAndPrivateSocket(t *testing.T) {
	directory := shortTempDir(t)
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	native := newTestNative(nil)
	config := testConfig(directory, "v1.0.0", native)
	config.workerRegistry = nil
	config.generation = func() (string, error) { return "test-generation", nil }
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	done := runRuntime(t, runtime)
	waitNativeStart(t, native, done)

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
	plan, err := newPlan(PlanSpec{
		Application: SignedApplication{
			AppPath: "/Applications/Example.app", BundleID: "com.example.holder",
			TeamID: "ABCDE12345", ExecutableName: "Example", SigningIdentifier: "com.example.holder",
		},
		RuntimeDirectory: runtimeDirectory,
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

func TestRuntimeFailsClosedBeforeTenantStartupWhenProcessRecoveryFails(t *testing.T) {
	directory := shortTempDir(t)
	native := newTestNative(nil)
	want := errors.New("process store unavailable")
	registry := &recoveryRegistry{err: want}
	config := testConfig(directory, "v1.0.0", native)
	config.workerRegistry = registry
	runtime, err := New(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if registry.callCount() != 0 {
		t.Fatal("worker recovery ran before daemon ownership")
	}
	if err := runtime.Run(t.Context()); !errors.Is(err, want) {
		t.Fatalf("Run error = %v, want recovery failure", err)
	}
	if registry.callCount() != 1 {
		t.Fatalf("recovery calls = %d, want one", registry.callCount())
	}
	if starts, _ := native.counts(); starts != 0 {
		t.Fatalf("native starts after recovery failure = %d", starts)
	}
	database, err := catalog.Open(t.Context(), config.Plan.Paths().Catalog)
	if err != nil {
		t.Fatalf("catalog remained owned after recovery failure: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessRegistryRequiresFreshGeneration(t *testing.T) {
	want := errors.New("entropy unavailable")
	if _, err := processRegistry(filepath.Join(t.TempDir(), "processes.json"), func() (string, error) {
		return "", want
	}); !errors.Is(err, want) {
		t.Fatalf("processRegistry error = %v", err)
	}
	if _, err := processRegistry(filepath.Join(t.TempDir(), "processes.json"), func() (string, error) {
		return "", nil
	}); err == nil {
		t.Fatal("empty process generation accepted")
	}
	registry, err := processRegistry(filepath.Join(t.TempDir(), "processes.json"), func() (string, error) {
		return "fresh-generation", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if registry.Generation != "fresh-generation" {
		t.Fatalf("generation = %q", registry.Generation)
	}
	if _, ok := registry.Store.(*proc.FileStore); !ok {
		t.Fatalf("process store = %T, want *proc.FileStore", registry.Store)
	}
}

func TestNativeProcessIdentityRequiresDedicatedSession(t *testing.T) {
	valid := proc.Record{
		PID: 42, StartTime: "start", Boot: "boot", Generation: "generation",
		ProcessGroup: true, SessionID: 42,
	}
	if err := validateNativeProcessRecord(valid); err != nil {
		t.Fatal(err)
	}
	missingGeneration := valid
	missingGeneration.Generation = ""
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

type recoveryRegistry struct {
	testRegistry
	mu       sync.Mutex
	err      error
	calls    int
	recorder func()
}

func (r *recoveryRegistry) Reap(context.Context) error {
	r.mu.Lock()
	r.calls++
	recorder := r.recorder
	err := r.err
	r.mu.Unlock()
	if recorder != nil {
		recorder()
	}
	return err
}

func (r *recoveryRegistry) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}
