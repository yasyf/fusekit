package holder

import (
	"errors"
	"fmt"
	"os/user"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/yasyf/daemonkit/bundle"
	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/daemonkit/trust"
)

const maxUnixSocketPath = 103

// SignedApplication is one consumer's immutable installed code identity.
type SignedApplication struct {
	AppPath              string
	BundleID             string
	TeamID               string
	ExecutableName       string
	SigningIdentifier    string
	RequiredEntitlements map[string]trust.EntitlementRequirement
}

// PlanSpec declares the product-owned inputs for one fixed signed holder.
type PlanSpec struct {
	Application      SignedApplication
	RuntimeDirectory string
}

// RuntimePaths are the complete FuseKit-owned paths below one product runtime directory.
type RuntimePaths struct {
	Directory        string
	Socket           string
	Catalog          string
	PresentationRoot string
	ProcessStore     string
}

// Plan is an immutable fixed-app deployment and runtime contract.
type Plan struct {
	application SignedApplication
	home        string
	paths       RuntimePaths
	requirement trust.Requirement
	service     service.AppKeepAlive
}

// NewPlan validates one installed signed-app identity and derives its complete runtime plan.
func NewPlan(spec PlanSpec) (Plan, error) {
	account, err := user.Current()
	if err != nil {
		return Plan{}, fmt.Errorf("holder: resolve current account: %w", err)
	}
	if !exactAbsolutePath(account.HomeDir) {
		return Plan{}, fmt.Errorf("holder: account home %q is not an exact absolute path", account.HomeDir)
	}
	plan, err := newPlan(spec, account.HomeDir)
	if err != nil {
		return Plan{}, err
	}
	if err := validateRuntimeAncestors(plan.home, plan.paths.Directory); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

func newPlan(spec PlanSpec, home string) (Plan, error) {
	app := cloneApplication(spec.Application)
	if err := validateSignedApplication(app, home); err != nil {
		return Plan{}, err
	}
	if !exactAbsolutePath(spec.RuntimeDirectory) {
		return Plan{}, fmt.Errorf("holder: runtime directory %q is not an exact absolute path", spec.RuntimeDirectory)
	}
	if !strictDescendant(home, spec.RuntimeDirectory) {
		return Plan{}, fmt.Errorf("holder: runtime directory %q is not below user home %q", spec.RuntimeDirectory, home)
	}
	paths := RuntimePaths{
		Directory:        spec.RuntimeDirectory,
		Socket:           filepath.Join(spec.RuntimeDirectory, "fusekit.sock"),
		Catalog:          filepath.Join(spec.RuntimeDirectory, "catalog.sqlite"),
		PresentationRoot: filepath.Join(spec.RuntimeDirectory, "mount"),
		ProcessStore:     filepath.Join(spec.RuntimeDirectory, "processes.json"),
	}
	if len([]byte(paths.Socket)) > maxUnixSocketPath {
		return Plan{}, fmt.Errorf("holder: runtime socket path is %d bytes, maximum is %d", len([]byte(paths.Socket)), maxUnixSocketPath)
	}
	requirement := trust.Requirement{
		TeamID: app.TeamID, SigningIdentifier: app.SigningIdentifier,
		RequiredEntitlements: cloneEntitlements(app.RequiredEntitlements),
	}
	if _, err := requirement.DRString(); err != nil {
		return Plan{}, fmt.Errorf("holder: signed application requirement: %w", err)
	}
	keepAlive := service.AppKeepAlive{
		Label: app.BundleID + ".fusekit", AppPath: app.AppPath,
		BundleID: app.BundleID, RestartPolicy: service.RestartAlways,
	}
	return Plan{application: app, home: home, paths: paths, requirement: requirement, service: keepAlive}, nil
}

// Application returns a detached copy of the plan's signed application identity.
func (p Plan) Application() SignedApplication { return cloneApplication(p.application) }

// Paths returns every runtime path derived by the plan.
func (p Plan) Paths() RuntimePaths { return p.paths }

// Executable returns the fixed inner executable path derived from the app bundle.
func (p Plan) Executable() string {
	return bundle.ExePath(p.application.AppPath, p.application.ExecutableName)
}

// Requirement returns a detached copy of the signed peer trust requirement.
func (p Plan) Requirement() trust.Requirement {
	requirement := p.requirement
	requirement.RequiredEntitlements = cloneEntitlements(requirement.RequiredEntitlements)
	return requirement
}

// Service returns the daemonkit per-user KeepAlive contract for the fixed app.
func (p Plan) Service() service.AppKeepAlive { return p.service }

func (p Plan) validate() error {
	if p.application.AppPath == "" || p.paths.Directory == "" {
		return errors.New("holder: plan is required")
	}
	rebuilt, err := newPlan(PlanSpec{Application: p.application, RuntimeDirectory: p.paths.Directory}, p.home)
	if err != nil {
		return err
	}
	if rebuilt.paths != p.paths || rebuilt.Executable() != p.Executable() || rebuilt.service != p.service {
		return errors.New("holder: plan is not internally consistent")
	}
	return nil
}

func validateSignedApplication(app SignedApplication, home string) error {
	if !exactAbsolutePath(app.AppPath) || filepath.Ext(app.AppPath) != ".app" {
		return fmt.Errorf("holder: app path %q is not an exact absolute .app path", app.AppPath)
	}
	applications := filepath.Dir(app.AppPath)
	if applications != "/Applications" && applications != filepath.Join(home, "Applications") {
		return fmt.Errorf("holder: app path %q is not a fixed installed application", app.AppPath)
	}
	if err := validateIdentifier("bundle ID", app.BundleID); err != nil {
		return err
	}
	if err := validateIdentifier("signing identifier", app.SigningIdentifier); err != nil {
		return err
	}
	if len(app.TeamID) != 10 || strings.IndexFunc(app.TeamID, func(r rune) bool {
		return !unicode.IsUpper(r) && !unicode.IsDigit(r)
	}) >= 0 {
		return fmt.Errorf("holder: Team ID %q is not ten uppercase alphanumeric characters", app.TeamID)
	}
	if app.ExecutableName == "" || filepath.Base(app.ExecutableName) != app.ExecutableName ||
		strings.ContainsAny(app.ExecutableName, `/\\`) || strings.ContainsRune(app.ExecutableName, 0) {
		return fmt.Errorf("holder: executable name %q is not one bundle executable name", app.ExecutableName)
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

func exactAbsolutePath(value string) bool {
	return filepath.IsAbs(value) && filepath.Clean(value) == value && !strings.ContainsRune(value, 0)
}

func cloneApplication(app SignedApplication) SignedApplication {
	app.RequiredEntitlements = cloneEntitlements(app.RequiredEntitlements)
	return app
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
