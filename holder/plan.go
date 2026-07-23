package holder

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"maps"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/codeidentity"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/trust"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
)

const (
	maxUnixSocketPath            = 103
	maxSourceAuthoritySocketPath = 99
)

// SignedExecutable is one exact code identity inside the fixed application.
type SignedExecutable struct {
	ExecutableName    string
	SigningIdentifier string
}

// SignedApplication is one consumer's immutable installed code identity.
// Broker is zero for mount-only applications.
type SignedApplication struct {
	AppPath  string
	BundleID string
	TeamID   string
	Broker   SignedExecutable
	Runtime  SignedExecutable
}

// EntitlementPolicy is one concrete signed-side entitlement contract.
type EntitlementPolicy struct {
	RequiredAppGroup     string
	RequiredEntitlements map[string]trust.EntitlementRequirement
}

// NativeRuntimeSpec declares one signed native presentation and its bundle verifier.
type NativeRuntimeSpec struct {
	PresentationRoot string
	FUSEVerifier     *FUSEVerifier
}

// NativeDeploymentSpec declares one daemon-facing native presentation root.
type NativeDeploymentSpec struct {
	PresentationRoot string
}

// NativePresentation is one configured native presentation.
type NativePresentation struct {
	PresentationRoot string
}

// RuntimePlanSpec declares the concrete contract embedded only by the signed app.
type RuntimePlanSpec struct {
	Application      SignedApplication
	RuntimeDirectory string
	Native           *NativeRuntimeSpec
	BuildID          string
	Readiness        ReadinessContract
	SourceCapable    bool
	BrokerPolicy     EntitlementPolicy
	RuntimePolicy    EntitlementPolicy
}

// DeploymentPlanSpec declares the daemon-facing fixed-app contract. Entitlement
// values are represented only by their canonical opaque policy digests.
type DeploymentPlanSpec struct {
	Application         SignedApplication
	RuntimeDirectory    string
	Native              *NativeDeploymentSpec
	BuildID             string
	Readiness           ReadinessContract
	SourceCapable       bool
	BrokerPolicyDigest  codeidentity.PolicyDigest
	RuntimePolicyDigest codeidentity.PolicyDigest
}

// RuntimePaths are the complete FuseKit-owned runtime paths. PresentationRoot
// is empty when native presentation is disabled.
type RuntimePaths struct {
	Directory        string
	Socket           string
	Catalog          string
	PresentationRoot string
	ProcessStore     string
}

// DeploymentPlan is the unsigned daemon's immutable fixed-app contract.
type DeploymentPlan struct {
	application   SignedApplication
	home          string
	paths         RuntimePaths
	buildID       string
	readiness     ReadinessContract
	sourceCapable bool
	nativeEnabled bool
	brokerEnabled bool
	brokerCode    codeidentity.CodeIdentity
	runtimeCode   codeidentity.CodeIdentity
	brokerDigest  codeidentity.PolicyDigest
	runtimeDigest codeidentity.PolicyDigest
	agent         service.Agent
	integrity     [32]byte
}

// RuntimePlan is the signed app's complete runtime contract.
type RuntimePlan struct {
	deployment DeploymentPlan
	broker     trust.Requirement
	runtime    trust.Requirement
	fuse       FUSEBundleManifest
}

// DeploymentBroker is one optional File Provider broker's daemon-safe identity.
type DeploymentBroker struct {
	Executable   string
	CodeIdentity codeidentity.CodeIdentity
	PolicyDigest codeidentity.PolicyDigest
}

// RuntimeBroker is one optional File Provider broker's concrete signed identity.
type RuntimeBroker struct {
	Deployment  DeploymentBroker
	Requirement trust.Requirement
}

// NewRuntimePlan validates concrete signed policy and derives its daemon-safe deployment plan.
func NewRuntimePlan(spec RuntimePlanSpec) (RuntimePlan, error) {
	account, err := user.Current()
	if err != nil {
		return RuntimePlan{}, fmt.Errorf("holder: resolve current account: %w", err)
	}
	if !exactAbsolutePath(account.HomeDir) {
		return RuntimePlan{}, fmt.Errorf("holder: account home %q is not an exact absolute path", account.HomeDir)
	}
	plan, err := newRuntimePlan(spec, account.HomeDir)
	if err != nil {
		return RuntimePlan{}, err
	}
	if err := validateInstalledApplication(plan.deployment.application); err != nil {
		return RuntimePlan{}, err
	}
	if spec.Native != nil {
		if spec.Native.FUSEVerifier == nil {
			return RuntimePlan{}, errors.New("holder: native presentation FUSE verifier is required")
		}
		manifest, err := spec.Native.FUSEVerifier.Verify(context.Background(), plan.deployment.application)
		if err != nil {
			return RuntimePlan{}, err
		}
		plan.fuse = manifest
	}
	if err := validateRuntimeAncestors(plan.deployment.home, plan.deployment.paths.Directory); err != nil {
		return RuntimePlan{}, err
	}
	if plan.deployment.nativeEnabled {
		if err := validatePresentationRootAncestors(plan.deployment.home, plan.deployment.paths.PresentationRoot); err != nil {
			return RuntimePlan{}, err
		}
	}
	return plan, nil
}

// NewDeploymentPlan validates one code-only deployment contract.
func NewDeploymentPlan(spec DeploymentPlanSpec) (DeploymentPlan, error) {
	account, err := user.Current()
	if err != nil {
		return DeploymentPlan{}, fmt.Errorf("holder: resolve current account: %w", err)
	}
	if !exactAbsolutePath(account.HomeDir) {
		return DeploymentPlan{}, fmt.Errorf("holder: account home %q is not an exact absolute path", account.HomeDir)
	}
	plan, err := newDeploymentPlan(spec, account.HomeDir)
	if err != nil {
		return DeploymentPlan{}, err
	}
	if err := validateInstalledApplication(plan.application); err != nil {
		return DeploymentPlan{}, err
	}
	if err := validateRuntimeAncestors(plan.home, plan.paths.Directory); err != nil {
		return DeploymentPlan{}, err
	}
	if plan.nativeEnabled {
		if err := validatePresentationRootAncestors(plan.home, plan.paths.PresentationRoot); err != nil {
			return DeploymentPlan{}, err
		}
	}
	return plan, nil
}

func newRuntimePlan(spec RuntimePlanSpec, home string) (RuntimePlan, error) {
	app := spec.Application
	if _, exists := spec.RuntimePolicy.RequiredEntitlements[disableLibraryValidationEntitlement]; exists {
		return RuntimePlan{}, errors.New("holder: runtime disable-library-validation entitlement is forbidden")
	}
	if _, exists := spec.BrokerPolicy.RequiredEntitlements[disableLibraryValidationEntitlement]; exists {
		return RuntimePlan{}, errors.New("holder: broker disable-library-validation entitlement is forbidden")
	}
	runtime := policyRequirement(app, app.Runtime, spec.RuntimePolicy)
	if _, err := runtime.DRString(); err != nil {
		return RuntimePlan{}, fmt.Errorf("holder: signed runtime requirement: %w", err)
	}
	brokerEnabled := app.Broker != (SignedExecutable{})
	var broker trust.Requirement
	var brokerDigest codeidentity.PolicyDigest
	if brokerEnabled {
		broker = policyRequirement(app, app.Broker, spec.BrokerPolicy)
		if _, err := broker.DRString(); err != nil {
			return RuntimePlan{}, fmt.Errorf("holder: signed broker requirement: %w", err)
		}
		if app.Broker.ExecutableName == app.Runtime.ExecutableName &&
			!sameEntitlementPolicy(spec.BrokerPolicy, spec.RuntimePolicy) {
			return RuntimePlan{}, errors.New("holder: one executable cannot have different entitlement policies")
		}
		var err error
		brokerDigest, err = broker.ValidationDigest()
		if err != nil {
			return RuntimePlan{}, fmt.Errorf("holder: digest broker entitlement policy: %w", err)
		}
	} else if !emptyEntitlementPolicy(spec.BrokerPolicy) {
		return RuntimePlan{}, errors.New("holder: native-only application cannot declare broker entitlement policy")
	}
	runtimeDigest, err := runtime.ValidationDigest()
	if err != nil {
		return RuntimePlan{}, fmt.Errorf("holder: digest runtime entitlement policy: %w", err)
	}
	var native *NativeDeploymentSpec
	if spec.Native != nil {
		native = &NativeDeploymentSpec{PresentationRoot: spec.Native.PresentationRoot}
	}
	deployment, err := newDeploymentPlan(DeploymentPlanSpec{
		Application: app, RuntimeDirectory: spec.RuntimeDirectory,
		Native:              native,
		BuildID:             spec.BuildID,
		Readiness:           spec.Readiness,
		SourceCapable:       spec.SourceCapable,
		BrokerPolicyDigest:  brokerDigest,
		RuntimePolicyDigest: runtimeDigest,
	}, home)
	if err != nil {
		return RuntimePlan{}, err
	}
	return RuntimePlan{
		deployment: deployment, broker: broker, runtime: runtime,
		fuse: func() FUSEBundleManifest {
			if spec.Native == nil {
				return FUSEBundleManifest{}
			}
			return fusePlanManifest(app)
		}(),
	}, nil
}

func newDeploymentPlan(spec DeploymentPlanSpec, home string) (DeploymentPlan, error) {
	app := spec.Application
	if err := validateSignedApplication(app, home); err != nil {
		return DeploymentPlan{}, err
	}
	brokerEnabled := app.Broker != (SignedExecutable{})
	nativeEnabled := spec.Native != nil
	if !nativeEnabled && !brokerEnabled {
		return DeploymentPlan{}, errors.New("holder: runtime plan requires native or File Provider presentation")
	}
	if brokerEnabled {
		if err := spec.BrokerPolicyDigest.Validate(); err != nil {
			return DeploymentPlan{}, fmt.Errorf("holder: broker opaque policy digest: %w", err)
		}
	} else if spec.BrokerPolicyDigest != (codeidentity.PolicyDigest{}) {
		return DeploymentPlan{}, errors.New("holder: native-only application cannot declare broker opaque policy digest")
	}
	if err := spec.RuntimePolicyDigest.Validate(); err != nil {
		return DeploymentPlan{}, fmt.Errorf("holder: runtime opaque policy digest: %w", err)
	}
	if err := validateBuildID(spec.BuildID); err != nil {
		return DeploymentPlan{}, err
	}
	if err := spec.Readiness.validate(); err != nil {
		return DeploymentPlan{}, err
	}
	if !exactAbsolutePath(spec.RuntimeDirectory) {
		return DeploymentPlan{}, fmt.Errorf("holder: runtime directory %q is not an exact absolute path", spec.RuntimeDirectory)
	}
	if !strictDescendant(home, spec.RuntimeDirectory) {
		return DeploymentPlan{}, fmt.Errorf("holder: runtime directory %q is not below user home %q", spec.RuntimeDirectory, home)
	}
	presentationRoot := ""
	if nativeEnabled {
		presentationRoot = spec.Native.PresentationRoot
		if !exactAbsolutePath(presentationRoot) {
			return DeploymentPlan{}, fmt.Errorf("holder: presentation root %q is not an exact absolute path", presentationRoot)
		}
		if !strictDescendant(home, presentationRoot) {
			return DeploymentPlan{}, fmt.Errorf("holder: presentation root %q is not below user home %q", presentationRoot, home)
		}
		if pathsOverlap(spec.RuntimeDirectory, presentationRoot) {
			return DeploymentPlan{}, fmt.Errorf(
				"holder: runtime directory %q overlaps presentation root %q",
				spec.RuntimeDirectory, presentationRoot,
			)
		}
	}
	if pathsOverlap(app.AppPath, spec.RuntimeDirectory) {
		return DeploymentPlan{}, fmt.Errorf("holder: runtime directory %q overlaps app path %q", spec.RuntimeDirectory, app.AppPath)
	}
	if nativeEnabled && pathsOverlap(app.AppPath, presentationRoot) {
		return DeploymentPlan{}, fmt.Errorf("holder: presentation root %q overlaps app path %q", presentationRoot, app.AppPath)
	}
	paths := RuntimePaths{
		Directory:        spec.RuntimeDirectory,
		Socket:           filepath.Join(spec.RuntimeDirectory, "fusekit.sock"),
		Catalog:          filepath.Join(spec.RuntimeDirectory, "catalog.sqlite"),
		PresentationRoot: presentationRoot,
		ProcessStore:     filepath.Join(spec.RuntimeDirectory, "processes.db"),
	}
	if len([]byte(paths.Socket)) > maxUnixSocketPath {
		return DeploymentPlan{}, fmt.Errorf("holder: runtime socket path is %d bytes, maximum is %d", len([]byte(paths.Socket)), maxUnixSocketPath)
	}
	sourceSocket := filepath.Join(spec.RuntimeDirectory, "source-observer-0000000000", "observer.sock")
	if len([]byte(sourceSocket)) > maxSourceAuthoritySocketPath {
		return DeploymentPlan{}, fmt.Errorf(
			"holder: source authority socket path is %d bytes, maximum is %d",
			len([]byte(sourceSocket)), maxSourceAuthoritySocketPath,
		)
	}
	var brokerCode codeidentity.CodeIdentity
	if brokerEnabled {
		brokerCode = codeidentity.CodeIdentity{TeamID: app.TeamID, SigningIdentifier: app.Broker.SigningIdentifier}
		if _, err := brokerCode.DRString(); err != nil {
			return DeploymentPlan{}, fmt.Errorf("holder: broker code identity: %w", err)
		}
	}
	runtimeCode := codeidentity.CodeIdentity{TeamID: app.TeamID, SigningIdentifier: app.Runtime.SigningIdentifier}
	if _, err := runtimeCode.DRString(); err != nil {
		return DeploymentPlan{}, fmt.Errorf("holder: runtime code identity: %w", err)
	}
	agent := service.Agent{
		Label:                       app.BundleID + ".fusekit",
		Program:                     bundle.ExePath(app.AppPath, app.Runtime.ExecutableName),
		LogPath:                     filepath.Join(spec.RuntimeDirectory, "holder.log"),
		Env:                         map[string]string{"FUSEKIT_BUILD_ID": spec.BuildID},
		AssociatedBundleIdentifiers: []string{app.BundleID},
		RestartPolicy:               service.RestartAlways,
		LimitLoadToSessionType:      service.SessionTypeAqua,
	}
	if _, err := agent.Plist(); err != nil {
		return DeploymentPlan{}, fmt.Errorf("holder: fixed application agent: %w", err)
	}
	plan := DeploymentPlan{
		application: app, home: home, paths: paths,
		buildID:       spec.BuildID,
		readiness:     spec.Readiness,
		sourceCapable: spec.SourceCapable,
		nativeEnabled: nativeEnabled,
		brokerEnabled: brokerEnabled, brokerCode: brokerCode, runtimeCode: runtimeCode,
		brokerDigest:  spec.BrokerPolicyDigest,
		runtimeDigest: spec.RuntimePolicyDigest,
		agent:         agent,
	}
	plan.integrity = deploymentPlanIntegrity(plan)
	return plan, nil
}

func policyRequirement(
	app SignedApplication,
	executable SignedExecutable,
	policy EntitlementPolicy,
) trust.Requirement {
	return trust.Requirement{
		TeamID: app.TeamID, SigningIdentifier: executable.SigningIdentifier,
		RequiredAppGroup:     policy.RequiredAppGroup,
		RequiredEntitlements: cloneEntitlements(policy.RequiredEntitlements),
	}
}

// Deployment returns the daemon-safe plan for this signed runtime.
func (p RuntimePlan) Deployment() DeploymentPlan { return p.deployment }

// Application returns the signed runtime's code-only application identity.
func (p RuntimePlan) Application() SignedApplication { return p.deployment.Application() }

// Application returns the immutable code-only application identity.
func (p DeploymentPlan) Application() SignedApplication { return p.application }

// Paths returns every runtime path declared or derived by the deployment.
func (p DeploymentPlan) Paths() RuntimePaths { return p.paths }

// BuildID returns the immutable consumer artifact identity that owns this runtime.
func (p DeploymentPlan) BuildID() string { return p.buildID }

// Readiness returns the exact signed-runtime and service-observer deadline budget.
func (p DeploymentPlan) Readiness() ReadinessContract { return p.readiness }

// SourceCapable reports whether the fixed runtime owns source-authority processes.
func (p DeploymentPlan) SourceCapable() bool { return p.sourceCapable }

// NativePresentation returns the configured native presentation, when present.
func (p DeploymentPlan) NativePresentation() (NativePresentation, bool) {
	if !p.nativeEnabled {
		return NativePresentation{}, false
	}
	return NativePresentation{PresentationRoot: p.paths.PresentationRoot}, true
}

// RuntimeExecutable returns the fixed runtime executable path.
func (p DeploymentPlan) RuntimeExecutable() string {
	return bundle.ExePath(p.application.AppPath, p.application.Runtime.ExecutableName)
}

// Broker returns the optional File Provider broker's daemon-safe identity.
func (p DeploymentPlan) Broker() (DeploymentBroker, bool) {
	if !p.brokerEnabled {
		return DeploymentBroker{}, false
	}
	return DeploymentBroker{
		Executable:   bundle.ExePath(p.application.AppPath, p.application.Broker.ExecutableName),
		CodeIdentity: p.brokerCode,
		PolicyDigest: p.brokerDigest,
	}, true
}

// RuntimeCodeIdentity returns the daemon-safe runtime code identity.
func (p DeploymentPlan) RuntimeCodeIdentity() codeidentity.CodeIdentity { return p.runtimeCode }

// RuntimePolicyDigest returns the runtime's opaque signed policy digest.
func (p DeploymentPlan) RuntimePolicyDigest() codeidentity.PolicyDigest { return p.runtimeDigest }

// Agent returns a detached desired LaunchAgent for the fixed signed app.
func (p DeploymentPlan) Agent() service.Agent {
	agent := p.agent
	agent.Args = slices.Clone(agent.Args)
	agent.Env = maps.Clone(agent.Env)
	agent.AssociatedBundleIdentifiers = slices.Clone(agent.AssociatedBundleIdentifiers)
	return agent
}

// Paths returns the signed runtime's daemon-safe paths.
func (p RuntimePlan) Paths() RuntimePaths { return p.deployment.Paths() }

// BuildID returns the immutable consumer artifact identity that owns this runtime.
func (p RuntimePlan) BuildID() string { return p.deployment.BuildID() }

// Readiness returns the exact signed-runtime and service-observer deadline budget.
func (p RuntimePlan) Readiness() ReadinessContract { return p.deployment.Readiness() }

// SourceCapable reports whether the fixed runtime owns source-authority processes.
func (p RuntimePlan) SourceCapable() bool { return p.deployment.SourceCapable() }

// NativePresentation returns the configured native presentation, when present.
func (p RuntimePlan) NativePresentation() (NativePresentation, bool) {
	return p.deployment.NativePresentation()
}

// RuntimeExecutable returns the signed runtime's fixed runtime executable.
func (p RuntimePlan) RuntimeExecutable() string { return p.deployment.RuntimeExecutable() }

// Broker returns the optional File Provider broker's concrete signed identity.
func (p RuntimePlan) Broker() (RuntimeBroker, bool) {
	deployment, ok := p.deployment.Broker()
	if !ok {
		return RuntimeBroker{}, false
	}
	requirement := p.broker
	requirement.RequiredEntitlements = cloneEntitlements(requirement.RequiredEntitlements)
	return RuntimeBroker{Deployment: deployment, Requirement: requirement}, true
}

// RuntimeRequirement returns a detached concrete signed-side requirement.
func (p RuntimePlan) RuntimeRequirement() trust.Requirement {
	requirement := p.runtime
	requirement.RequiredEntitlements = cloneEntitlements(requirement.RequiredEntitlements)
	return requirement
}

// FUSELibrary returns the exact bundled library leaf and post-sign byte digest
// when native presentation is configured.
func (p RuntimePlan) FUSELibrary() (string, string, bool) {
	if !p.deployment.nativeEnabled {
		return "", "", false
	}
	return filepath.Join(p.Application().AppPath, FUSELibraryRelativePath), p.fuse.SignedSHA256, true
}

func (p DeploymentPlan) validate() error {
	if p.application.AppPath == "" || p.paths.Directory == "" {
		return errors.New("holder: deployment plan is required")
	}
	if p.integrity == ([32]byte{}) || p.integrity != deploymentPlanIntegrity(p) {
		return errors.New("holder: deployment plan integrity changed")
	}
	var native *NativeDeploymentSpec
	if p.nativeEnabled {
		native = &NativeDeploymentSpec{PresentationRoot: p.paths.PresentationRoot}
	}
	rebuilt, err := newDeploymentPlan(DeploymentPlanSpec{
		Application: p.application, RuntimeDirectory: p.paths.Directory,
		Native:              native,
		BuildID:             p.buildID,
		Readiness:           p.readiness,
		SourceCapable:       p.sourceCapable,
		BrokerPolicyDigest:  p.brokerDigest,
		RuntimePolicyDigest: p.runtimeDigest,
	}, p.home)
	if err != nil {
		return err
	}
	if rebuilt.paths != p.paths || rebuilt.buildID != p.buildID || rebuilt.readiness != p.readiness || rebuilt.RuntimeExecutable() != p.RuntimeExecutable() || !sameAgent(rebuilt.agent, p.agent) ||
		rebuilt.sourceCapable != p.sourceCapable || rebuilt.nativeEnabled != p.nativeEnabled || rebuilt.brokerEnabled != p.brokerEnabled ||
		rebuilt.brokerCode != p.brokerCode || rebuilt.runtimeCode != p.runtimeCode {
		return errors.New("holder: deployment plan is not internally consistent")
	}
	return nil
}

func deploymentPlanIntegrity(plan DeploymentPlan) [32]byte {
	digest := sha256.New()
	_, _ = digest.Write([]byte("fusekit.holder.deployment-plan.v1\x00"))
	writeDeploymentPlanString(digest, plan.application.AppPath)
	writeDeploymentPlanString(digest, plan.application.BundleID)
	writeDeploymentPlanString(digest, plan.application.TeamID)
	writeDeploymentPlanString(digest, plan.application.Broker.ExecutableName)
	writeDeploymentPlanString(digest, plan.application.Broker.SigningIdentifier)
	writeDeploymentPlanString(digest, plan.application.Runtime.ExecutableName)
	writeDeploymentPlanString(digest, plan.application.Runtime.SigningIdentifier)
	writeDeploymentPlanString(digest, plan.home)
	writeDeploymentPlanString(digest, plan.paths.Directory)
	writeDeploymentPlanString(digest, plan.paths.Socket)
	writeDeploymentPlanString(digest, plan.paths.Catalog)
	writeDeploymentPlanString(digest, plan.paths.PresentationRoot)
	writeDeploymentPlanString(digest, plan.paths.ProcessStore)
	writeDeploymentPlanString(digest, plan.buildID)
	writeDeploymentPlanUint64(digest, uint64(plan.readiness.startup))
	writeDeploymentPlanUint64(digest, uint64(plan.readiness.settlement))
	writeDeploymentPlanUint64(digest, uint64(plan.readiness.observation))
	if plan.sourceCapable {
		_, _ = digest.Write([]byte{1})
	} else {
		_, _ = digest.Write([]byte{0})
	}
	if plan.nativeEnabled {
		_, _ = digest.Write([]byte{1})
	} else {
		_, _ = digest.Write([]byte{0})
	}
	if plan.brokerEnabled {
		_, _ = digest.Write([]byte{1})
	} else {
		_, _ = digest.Write([]byte{0})
	}
	writeDeploymentPlanString(digest, plan.brokerCode.TeamID)
	writeDeploymentPlanString(digest, plan.brokerCode.SigningIdentifier)
	writeDeploymentPlanString(digest, plan.runtimeCode.TeamID)
	writeDeploymentPlanString(digest, plan.runtimeCode.SigningIdentifier)
	_, _ = digest.Write(plan.brokerDigest[:])
	_, _ = digest.Write(plan.runtimeDigest[:])
	writeDeploymentPlanString(digest, plan.agent.Label)
	writeDeploymentPlanString(digest, plan.agent.Program)
	writeDeploymentPlanUint64(digest, uint64(len(plan.agent.Args)))
	for _, value := range plan.agent.Args {
		writeDeploymentPlanString(digest, value)
	}
	writeDeploymentPlanString(digest, plan.agent.LogPath)
	writeDeploymentPlanUint64(digest, uint64(len(plan.agent.Env)))
	for _, key := range slices.Sorted(maps.Keys(plan.agent.Env)) {
		writeDeploymentPlanString(digest, key)
		writeDeploymentPlanString(digest, plan.agent.Env[key])
	}
	writeDeploymentPlanUint64(digest, uint64(len(plan.agent.AssociatedBundleIdentifiers)))
	for _, value := range plan.agent.AssociatedBundleIdentifiers {
		writeDeploymentPlanString(digest, value)
	}
	var policy [11]byte
	policy[0] = byte(plan.agent.RestartPolicy)
	binary.BigEndian.PutUint64(policy[1:9], uint64(plan.agent.StartInterval))
	policy[9] = byte(plan.agent.ProcessType)
	policy[10] = byte(plan.agent.LimitLoadToSessionType)
	_, _ = digest.Write(policy[:])
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result
}

func sameAgent(left, right service.Agent) bool {
	return left.Label == right.Label && left.Program == right.Program &&
		slices.Equal(left.Args, right.Args) && left.LogPath == right.LogPath &&
		maps.Equal(left.Env, right.Env) &&
		slices.Equal(left.AssociatedBundleIdentifiers, right.AssociatedBundleIdentifiers) &&
		left.RestartPolicy == right.RestartPolicy && left.StartInterval == right.StartInterval &&
		left.ProcessType == right.ProcessType &&
		left.LimitLoadToSessionType == right.LimitLoadToSessionType
}

func writeDeploymentPlanString(digest hash.Hash, value string) {
	writeDeploymentPlanUint64(digest, uint64(len(value)))
	_, _ = digest.Write([]byte(value))
}

func writeDeploymentPlanUint64(digest hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = digest.Write(encoded[:])
}

func (p RuntimePlan) validate() error {
	if err := p.deployment.validate(); err != nil {
		return err
	}
	if p.runtime.CodeIdentity() != p.deployment.runtimeCode {
		return errors.New("holder: runtime plan code identity is not internally consistent")
	}
	if p.deployment.brokerEnabled {
		if p.broker.CodeIdentity() != p.deployment.brokerCode {
			return errors.New("holder: runtime plan broker code identity is not internally consistent")
		}
		brokerDigest, err := p.broker.ValidationDigest()
		if err != nil {
			return err
		}
		if brokerDigest != p.deployment.brokerDigest {
			return errors.New("holder: runtime plan broker entitlement policy is not internally consistent")
		}
	} else if !emptyRequirement(p.broker) || p.deployment.brokerCode != (codeidentity.CodeIdentity{}) ||
		p.deployment.brokerDigest != (codeidentity.PolicyDigest{}) {
		return errors.New("holder: native-only runtime plan contains broker identity")
	}
	runtimeDigest, err := p.runtime.ValidationDigest()
	if err != nil {
		return err
	}
	if runtimeDigest != p.deployment.runtimeDigest {
		return errors.New("holder: runtime plan entitlement policy is not internally consistent")
	}
	if p.deployment.nativeEnabled {
		if err := validateFUSEPlanManifest(p.Application(), p.fuse); err != nil {
			return err
		}
	} else if !reflect.DeepEqual(p.fuse, FUSEBundleManifest{}) {
		return errors.New("holder: File Provider-only runtime plan contains FUSE manifest")
	}
	return nil
}

func validateSignedApplication(app SignedApplication, home string) error {
	if !exactAbsolutePath(app.AppPath) || filepath.Ext(app.AppPath) != ".app" {
		return fmt.Errorf("holder: app path %q is not an exact absolute .app path", app.AppPath)
	}
	if filepath.Dir(app.AppPath) != filepath.Join(home, "Applications") {
		return fmt.Errorf("holder: app path %q is not a fixed user application", app.AppPath)
	}
	name := strings.TrimSuffix(filepath.Base(app.AppPath), ".app")
	if name == "" || strings.Contains(strings.ToLower(name), "holder") {
		return fmt.Errorf("holder: app path %q must use a meaningful product name without holder terminology", app.AppPath)
	}
	if err := validateIdentifier("bundle ID", app.BundleID); err != nil {
		return err
	}
	if app.Broker != (SignedExecutable{}) {
		if err := validateSignedExecutable("broker", app.Broker); err != nil {
			return err
		}
		if app.Broker.SigningIdentifier != app.BundleID {
			return errors.New("holder: broker signing identifier must equal the application bundle ID")
		}
	}
	if err := validateSignedExecutable("runtime", app.Runtime); err != nil {
		return err
	}
	if app.Broker != (SignedExecutable{}) && app.Broker.ExecutableName == app.Runtime.ExecutableName &&
		app.Broker.SigningIdentifier != app.Runtime.SigningIdentifier {
		return errors.New("holder: one executable cannot have different code identities")
	}
	if len(app.TeamID) != 10 || strings.IndexFunc(app.TeamID, func(r rune) bool {
		return !unicode.IsUpper(r) && !unicode.IsDigit(r)
	}) >= 0 {
		return fmt.Errorf("holder: Team ID %q is not ten uppercase alphanumeric characters", app.TeamID)
	}
	return nil
}

func validateSignedExecutable(role string, executable SignedExecutable) error {
	if err := validateIdentifier(role+" signing identifier", executable.SigningIdentifier); err != nil {
		return err
	}
	if executable.ExecutableName == "" || filepath.Base(executable.ExecutableName) != executable.ExecutableName ||
		strings.ContainsAny(executable.ExecutableName, `/\`) || strings.ContainsRune(executable.ExecutableName, 0) {
		return fmt.Errorf(
			"holder: %s executable name %q is not one bundle executable name",
			role, executable.ExecutableName,
		)
	}
	return nil
}

func validateInstalledApplication(app SignedApplication) error {
	checked := make(map[string]struct{}, 2)
	for _, installed := range []struct {
		role       string
		executable SignedExecutable
	}{
		{role: "runtime", executable: app.Runtime},
		{role: "broker", executable: app.Broker},
	} {
		role, executable := installed.role, installed.executable
		if executable == (SignedExecutable{}) {
			continue
		}
		path := bundle.ExePath(app.AppPath, executable.ExecutableName)
		if _, exists := checked[path]; exists {
			continue
		}
		checked[path] = struct{}{}
		if err := requireRealExecutablePath(path); err != nil {
			return fmt.Errorf("holder: installed %s executable: %w", role, err)
		}
	}
	return nil
}

func requireRealExecutablePath(path string) error {
	if !exactAbsolutePath(path) {
		return fmt.Errorf("path %q is not exact and absolute", path)
	}
	volume := filepath.VolumeName(path)
	current := volume + string(filepath.Separator)
	relative := strings.TrimPrefix(path[len(volume):], string(filepath.Separator))
	components := strings.Split(relative, string(filepath.Separator))
	for index, component := range components {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("inspect %q: %w", current, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("path component %q is a symbolic link", current)
		}
		if index < len(components)-1 {
			if !info.IsDir() {
				return fmt.Errorf("path component %q is not a directory", current)
			}
			continue
		}
		if !info.Mode().IsRegular() || info.Mode()&0o111 == 0 {
			return fmt.Errorf("%q is not an executable regular file", current)
		}
	}
	return nil
}

func validateBuildID(value string) error {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) ||
		strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("holder: build ID %q is invalid", value)
	}
	return nil
}

func validateIdentifier(name, value string) error {
	if value == "" || strings.TrimSpace(value) != value || strings.Count(value, ".") < 1 {
		return fmt.Errorf("holder: %s %q is not a reverse-DNS identifier", name, value)
	}
	for _, part := range strings.Split(value, ".") {
		if part == "" || part[0] == '-' || part[len(part)-1] == '-' || strings.IndexFunc(part, func(r rune) bool {
			return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '-'
		}) >= 0 {
			return fmt.Errorf("holder: %s %q is not a reverse-DNS identifier", name, value)
		}
	}
	return nil
}

func strictDescendant(parent, child string) bool {
	if !exactAbsolutePath(parent) {
		return false
	}
	relative, err := filepath.Rel(parent, child)
	return err == nil && relative != "." && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func pathsOverlap(left, right string) bool {
	left = pathRelationKey(left)
	right = pathRelationKey(right)
	return left == right || strictDescendant(left, right) || strictDescendant(right, left)
}

func pathRelationKey(path string) string {
	return norm.NFD.String(cases.Fold().String(norm.NFD.String(path)))
}

func exactAbsolutePath(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value && !strings.ContainsRune(value, 0)
}

func cloneEntitlements(values map[string]trust.EntitlementRequirement) map[string]trust.EntitlementRequirement {
	if values == nil {
		return nil
	}
	cloned := make(map[string]trust.EntitlementRequirement, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func sameEntitlementPolicy(left, right EntitlementPolicy) bool {
	return left.RequiredAppGroup == right.RequiredAppGroup &&
		maps.Equal(left.RequiredEntitlements, right.RequiredEntitlements)
}

func emptyEntitlementPolicy(policy EntitlementPolicy) bool {
	return policy.RequiredAppGroup == "" && len(policy.RequiredEntitlements) == 0
}

func emptyRequirement(requirement trust.Requirement) bool {
	return requirement.TeamID == "" && requirement.SigningIdentifier == "" &&
		requirement.RequiredAppGroup == "" && len(requirement.RequiredEntitlements) == 0
}
