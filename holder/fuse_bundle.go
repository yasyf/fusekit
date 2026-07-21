package holder

import (
	"bytes"
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/supervise"
	"github.com/yasyf/daemonkit/trust"
)

const (
	FUSELibraryRelativePath  = "Contents/Frameworks/libfuse-t.dylib"
	FUSELicenseRelativePath  = "Contents/Resources/ThirdPartyLicenses/FUSE-T.txt"
	FUSEManifestRelativePath = "Contents/Resources/FuseKit/libfuse-t.manifest.json"
	FUSESourceSHA256         = "d1f0c160a941835a171133dbd58f9d5fe381b520be890f3843c573644cb17735"
	FUSELicenseSHA256        = "f3693b71cd51df8fe489238a65d5407a8fc7b6c573c3f169cbfc4e22521c70e3"
	FUSEInstallName          = "@rpath/libfuse-t.dylib"

	disableLibraryValidationEntitlement = "com.apple.security.cs.disable-library-validation"
	fuseManifestVersion                 = 1
)

var injectionEntitlements = []string{
	disableLibraryValidationEntitlement,
	"com.apple.security.cs.allow-dyld-environment-variables",
	"com.apple.security.cs.allow-unsigned-executable-memory",
	"com.apple.security.cs.allow-jit",
	"com.apple.security.cs.disable-executable-page-protection",
	"com.apple.security.get-task-allow",
}

var expectedFUSEDependencies = []string{
	"/System/Library/Frameworks/CoreFoundation.framework/Versions/A/CoreFoundation",
	"/System/Library/Frameworks/DiskArbitration.framework/Versions/A/DiskArbitration",
	"/usr/lib/libSystem.B.dylib",
	"/usr/lib/libiconv.2.dylib",
}

//go:embed third_party/FUSE-T.txt
var fuseLicense []byte

// BundleCodeIdentity is one inspected static Mach-O signature.
type BundleCodeIdentity struct {
	TeamID             string
	SigningIdentifier  string
	EntitlementsSHA256 string
	Entitlements       map[string]bool
	HardenedRuntime    bool
}

// FUSELibraryInspection is the complete reviewed dylib contract.
type FUSELibraryInspection struct {
	Architectures []string
	InstallName   string
	Dependencies  []string
	Code          BundleCodeIdentity
}

type fuseBundleToolchain interface {
	InspectLibrary(context.Context, string) (FUSELibraryInspection, error)
	InspectApplication(context.Context, string) (BundleCodeIdentity, error)
	VerifyCodeRequirement(context.Context, string, string) error
	SignNestedLibrary(context.Context, string, string) error
	SignApplication(context.Context, string) error
}

// FUSEPackager owns the complete reviewed nested-library packaging and signing flow.
type FUSEPackager struct{ tools fuseBundleToolchain }

// FUSEVerifier owns static signature and byte validation of one installed bundle.
type FUSEVerifier struct{ tools fuseBundleToolchain }

type commandFUSETools struct {
	runner          supervise.TaskRunner
	signingIdentity string
}

const fuseToolOutputLimit = 64 << 10

type fuseToolBuffer struct{ bytes.Buffer }

func (b *fuseToolBuffer) Write(payload []byte) (int, error) {
	if len(payload) > fuseToolOutputLimit-b.Len() {
		return 0, errors.New("holder: FUSE packaging tool output exceeded limit")
	}
	return b.Buffer.Write(payload)
}

func newCommandFUSETools(runner supervise.TaskRunner, signingIdentity string) (*commandFUSETools, error) {
	if runner == nil {
		return nil, errors.New("holder: FUSE packaging task runner is required")
	}
	return &commandFUSETools{runner: runner, signingIdentity: strings.TrimSpace(signingIdentity)}, nil
}

// NewFUSEPackager creates a production packager backed by killable daemonkit tasks.
func NewFUSEPackager(runner supervise.TaskRunner, signingIdentity string) (*FUSEPackager, error) {
	if strings.TrimSpace(signingIdentity) == "" {
		return nil, errors.New("holder: FUSE signing identity is required")
	}
	tools, err := newCommandFUSETools(runner, signingIdentity)
	if err != nil {
		return nil, err
	}
	return &FUSEPackager{tools: tools}, nil
}

func (t *commandFUSETools) InspectLibrary(ctx context.Context, path string) (FUSELibraryInspection, error) {
	architectures, _, err := t.run(ctx, "/usr/bin/lipo", "-archs", path)
	if err != nil {
		return FUSELibraryInspection{}, err
	}
	installOutput, _, err := t.run(ctx, "/usr/bin/otool", "-D", path)
	if err != nil {
		return FUSELibraryInspection{}, err
	}
	dependencyOutput, _, err := t.run(ctx, "/usr/bin/otool", "-L", path)
	if err != nil {
		return FUSELibraryInspection{}, err
	}
	code, err := t.inspectCode(ctx, path)
	if err != nil {
		return FUSELibraryInspection{}, err
	}
	architectureList := strings.Fields(string(architectures))
	installName, err := parseOtoolInstallNames(path, architectureList, installOutput)
	if err != nil {
		return FUSELibraryInspection{}, err
	}
	dependencies, err := parseOtoolDependencies(path, architectureList, installName, dependencyOutput)
	if err != nil {
		return FUSELibraryInspection{}, err
	}
	return FUSELibraryInspection{
		Architectures: architectureList, InstallName: installName,
		Dependencies: dependencies, Code: code,
	}, nil
}

func parseOtoolInstallNames(path string, architectures []string, output []byte) (string, error) {
	lines := nonemptyLines(string(output))
	if len(lines) != len(architectures)*2 {
		return "", errors.New("holder: unexpected otool install-name output")
	}
	var installName string
	for index, architecture := range architectures {
		if lines[index*2] != path+" (architecture "+architecture+"):" {
			return "", errors.New("holder: unexpected otool install-name architecture section")
		}
		if installName == "" {
			installName = lines[index*2+1]
		} else if lines[index*2+1] != installName {
			return "", errors.New("holder: FUSE install name differs across architectures")
		}
	}
	return installName, nil
}

func parseOtoolDependencies(path string, architectures []string, installName string, output []byte) ([]string, error) {
	lines := nonemptyLines(string(output))
	offset := 0
	var expected []string
	for _, architecture := range architectures {
		if offset >= len(lines) || lines[offset] != path+" (architecture "+architecture+"):" {
			return nil, errors.New("holder: unexpected otool dependency architecture section")
		}
		offset++
		var dependencies []string
		for offset < len(lines) && !strings.HasPrefix(lines[offset], path+" (architecture ") {
			name := strings.TrimSpace(strings.SplitN(lines[offset], " (compatibility version", 2)[0])
			if name != installName {
				dependencies = append(dependencies, name)
			}
			offset++
		}
		if expected == nil {
			expected = dependencies
		} else if !slices.Equal(expected, dependencies) {
			return nil, errors.New("holder: FUSE dependencies differ across architectures")
		}
	}
	if offset != len(lines) || len(expected) == 0 {
		return nil, errors.New("holder: unexpected otool dependency output")
	}
	return expected, nil
}

func (t *commandFUSETools) InspectApplication(ctx context.Context, path string) (BundleCodeIdentity, error) {
	return t.inspectCode(ctx, path)
}

func (t *commandFUSETools) VerifyCodeRequirement(ctx context.Context, path, requirement string) error {
	_, _, err := t.run(ctx, "/usr/bin/codesign", "--verify", "--strict", "--verbose=4",
		"--test-requirement", "="+requirement, path)
	return err
}

func (t *commandFUSETools) SignNestedLibrary(ctx context.Context, path, signingIdentifier string) error {
	if t.signingIdentity == "" {
		return errors.New("holder: FUSE verifier cannot sign")
	}
	_, _, err := t.run(ctx, "/usr/bin/codesign", "--force", "--sign", t.signingIdentity,
		"--identifier", signingIdentifier, "--options", "runtime", "--timestamp", path)
	return err
}

func (t *commandFUSETools) SignApplication(ctx context.Context, path string) error {
	if t.signingIdentity == "" {
		return errors.New("holder: FUSE verifier cannot sign")
	}
	_, _, err := t.run(ctx, "/usr/bin/codesign", "--force", "--sign", t.signingIdentity,
		"--preserve-metadata=entitlements,requirements", "--options", "runtime", "--timestamp", path)
	return err
}

func (t *commandFUSETools) inspectCode(ctx context.Context, path string) (BundleCodeIdentity, error) {
	if _, _, err := t.run(ctx, "/usr/bin/codesign", "--verify", "--strict", "--verbose=4", path); err != nil {
		return BundleCodeIdentity{}, err
	}
	stdout, stderr, err := t.run(ctx, "/usr/bin/codesign", "--display", "--verbose=4", path)
	if err != nil {
		return BundleCodeIdentity{}, err
	}
	details := string(append(stdout, stderr...))
	identity := BundleCodeIdentity{
		TeamID: fieldLine(details, "TeamIdentifier="), SigningIdentifier: fieldLine(details, "Identifier="),
		Entitlements:    make(map[string]bool),
		HardenedRuntime: hasCodeDirectoryFlag(details, "runtime"),
	}
	entitlements, _, err := t.run(ctx, "/usr/bin/codesign", "--display", "--entitlements", ":-", path)
	if err != nil {
		return BundleCodeIdentity{}, err
	}
	identity.EntitlementsSHA256, identity.Entitlements, err = canonicalEntitlements(entitlements)
	if err != nil {
		return BundleCodeIdentity{}, fmt.Errorf("holder: decode signed entitlements: %w", err)
	}
	if identity.TeamID == "" || identity.SigningIdentifier == "" {
		return BundleCodeIdentity{}, errors.New("holder: incomplete codesign identity output")
	}
	return identity, nil
}

func (t *commandFUSETools) run(ctx context.Context, path string, arguments ...string) ([]byte, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	taskCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var stdout, stderr fuseToolBuffer
	err := t.runner.Run(taskCtx, supervise.Task{
		RecoveryClass: proc.RecoveryTask, Path: path, Args: append([]string(nil), arguments...),
		Env: sanitizedChildEnvironment(os.Environ()), Stdout: &stdout, Stderr: &stderr,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("holder: run FUSE packaging tool %s: %w: %s", filepath.Base(path), err, strings.TrimSpace(stderr.String()))
	}
	return slices.Clone(stdout.Bytes()), slices.Clone(stderr.Bytes()), nil
}

func nonemptyLines(value string) []string {
	var result []string
	for _, line := range strings.Split(value, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			result = append(result, line)
		}
	}
	return result
}

func fieldLine(value, prefix string) string {
	for _, line := range nonemptyLines(value) {
		if result, ok := strings.CutPrefix(line, prefix); ok {
			return result
		}
	}
	return ""
}

func hasCodeDirectoryFlag(value, flag string) bool {
	for _, line := range nonemptyLines(value) {
		marker := strings.Index(line, "flags=")
		if marker < 0 {
			continue
		}
		flags := line[marker+len("flags="):]
		open := strings.IndexByte(flags, '(')
		close := strings.IndexByte(flags, ')')
		if open < 0 || close <= open {
			continue
		}
		for _, candidate := range strings.Split(flags[open+1:close], ",") {
			if strings.TrimSpace(candidate) == flag {
				return true
			}
		}
	}
	return false
}

func canonicalEntitlements(payload []byte) (string, map[string]bool, error) {
	digest := sha256.New()
	keys := make(map[string]bool)
	if len(bytes.TrimSpace(payload)) == 0 {
		return hex.EncodeToString(digest.Sum(nil)), keys, nil
	}
	writePart := func(kind byte, values ...string) {
		_, _ = digest.Write([]byte{kind})
		var size [8]byte
		for _, value := range values {
			binary.BigEndian.PutUint64(size[:], uint64(len(value)))
			_, _ = digest.Write(size[:])
			_, _ = digest.Write([]byte(value))
		}
	}
	decoder := xml.NewDecoder(bytes.NewReader(payload))
	var key strings.Builder
	inKey := false
	sawPlist := false
	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", nil, err
		}
		switch value := token.(type) {
		case xml.StartElement:
			if value.Name.Local == "plist" {
				sawPlist = true
			}
			attributes := make([]string, 0, len(value.Attr))
			for _, attribute := range value.Attr {
				attributes = append(attributes, attribute.Name.Space+"\x00"+attribute.Name.Local+"\x00"+attribute.Value)
			}
			slices.Sort(attributes)
			writePart('S', value.Name.Space, value.Name.Local, strings.Join(attributes, "\x00"))
			if value.Name.Local == "key" {
				key.Reset()
				inKey = true
			}
		case xml.CharData:
			text := strings.TrimSpace(string(value))
			if text != "" {
				writePart('T', text)
				if inKey {
					key.WriteString(text)
				}
			}
		case xml.EndElement:
			writePart('E', value.Name.Space, value.Name.Local)
			if value.Name.Local == "key" {
				if key.Len() == 0 {
					return "", nil, errors.New("empty entitlement key")
				}
				keys[key.String()] = true
				inKey = false
			}
		}
	}
	if !sawPlist || inKey {
		return "", nil, errors.New("incomplete entitlement property list")
	}
	return hex.EncodeToString(digest.Sum(nil)), keys, nil
}

// NewFUSEVerifier creates a production verifier backed by killable daemonkit tasks.
func NewFUSEVerifier(runner supervise.TaskRunner) (*FUSEVerifier, error) {
	tools, err := newCommandFUSETools(runner, "")
	if err != nil {
		return nil, err
	}
	return &FUSEVerifier{tools: tools}, nil
}

// FUSEBundleManifest pins the exact post-sign bytes and code identity shipped in an app.
type FUSEBundleManifest struct {
	Version                    int      `json:"version"`
	SourceSHA256               string   `json:"source_sha256"`
	SignedSHA256               string   `json:"signed_sha256"`
	LicenseSHA256              string   `json:"license_sha256"`
	Architectures              []string `json:"architectures"`
	InstallName                string   `json:"install_name"`
	Dependencies               []string `json:"dependencies"`
	TeamID                     string   `json:"team_id"`
	SigningIdentifier          string   `json:"signing_identifier"`
	DesignatedRequirement      string   `json:"designated_requirement"`
	OuterDesignatedRequirement string   `json:"outer_designated_requirement"`
	OuterEntitlementsSHA256    string   `json:"outer_entitlements_sha256"`
}

// Package installs, signs, manifests, and verifies the reviewed FUSE-T dylib.
func (p *FUSEPackager) Package(ctx context.Context, app SignedApplication, source string) (FUSEBundleManifest, error) {
	if p == nil {
		return FUSEBundleManifest{}, errors.New("holder: FUSE packager is required")
	}
	return packageFUSEBundle(ctx, app, source, FUSESourceSHA256, p.tools)
}

func packageFUSEBundle(ctx context.Context, app SignedApplication, source, sourceDigest string, tools fuseBundleToolchain) (FUSEBundleManifest, error) {
	if tools == nil {
		return FUSEBundleManifest{}, errors.New("holder: FUSE bundle toolchain is required")
	}
	if !exactAbsolutePath(app.AppPath) || filepath.Ext(app.AppPath) != ".app" {
		return FUSEBundleManifest{}, errors.New("holder: FUSE application path is not an exact absolute .app path")
	}
	outerBefore, err := tools.InspectApplication(ctx, app.AppPath)
	if err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: inspect pre-package outer application: %w", err)
	}
	if err := verifyBundleCode(ctx, tools, app.AppPath, outerBefore, app.TeamID, app.Runtime.SigningIdentifier); err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: pre-package outer application identity: %w", err)
	}
	if err := requireRegularNonSymlink(source); err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: FUSE source library: %w", err)
	}
	if digest, err := fileSHA256(source); err != nil || digest != sourceDigest {
		return FUSEBundleManifest{}, errors.Join(errors.New("holder: reviewed FUSE source SHA-256 mismatch"), err)
	}
	initial, err := tools.InspectLibrary(ctx, source)
	if err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: inspect FUSE source library: %w", err)
	}
	if err := validateFUSEMachO(initial); err != nil {
		return FUSEBundleManifest{}, err
	}

	library := filepath.Join(app.AppPath, FUSELibraryRelativePath)
	license := filepath.Join(app.AppPath, FUSELicenseRelativePath)
	manifestPath := filepath.Join(app.AppPath, FUSEManifestRelativePath)
	for _, path := range []string{library, license, manifestPath} {
		if !strictDescendant(app.AppPath, path) {
			return FUSEBundleManifest{}, errors.New("holder: FUSE bundle output escapes the application")
		}
		if err := makeRealDirectory(app.AppPath, filepath.Dir(path)); err != nil {
			return FUSEBundleManifest{}, err
		}
	}
	if err := copyExactFile(source, library, 0o755); err != nil {
		return FUSEBundleManifest{}, err
	}
	if err := writeExactFile(license, fuseLicense, 0o644); err != nil {
		return FUSEBundleManifest{}, err
	}
	identifier := app.BundleID + ".fuse-t"
	if err := tools.SignNestedLibrary(ctx, library, identifier); err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: sign nested FUSE library: %w", err)
	}
	signed, err := tools.InspectLibrary(ctx, library)
	if err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: verify nested FUSE library: %w", err)
	}
	if err := validateFUSEMachO(signed); err != nil {
		return FUSEBundleManifest{}, err
	}
	if err := verifyBundleCode(ctx, tools, library, signed.Code, app.TeamID, identifier); err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: nested FUSE identity: %w", err)
	}
	signedDigest, err := fileSHA256(library)
	if err != nil {
		return FUSEBundleManifest{}, err
	}
	nestedRequirement, err := bundleCodeRequirement(app.TeamID, identifier)
	if err != nil {
		return FUSEBundleManifest{}, err
	}
	outerRequirement, err := bundleCodeRequirement(app.TeamID, app.Runtime.SigningIdentifier)
	if err != nil {
		return FUSEBundleManifest{}, err
	}
	manifest := FUSEBundleManifest{
		Version: fuseManifestVersion, SourceSHA256: sourceDigest, SignedSHA256: signedDigest,
		LicenseSHA256: FUSELicenseSHA256, Architectures: []string{"arm64", "x86_64"},
		InstallName: FUSEInstallName, Dependencies: slices.Clone(expectedFUSEDependencies),
		TeamID: app.TeamID, SigningIdentifier: identifier, DesignatedRequirement: nestedRequirement,
		OuterDesignatedRequirement: outerRequirement,
		OuterEntitlementsSHA256:    outerBefore.EntitlementsSHA256,
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: encode FUSE manifest: %w", err)
	}
	payload = append(payload, '\n')
	if err := writeExactFile(manifestPath, payload, 0o644); err != nil {
		return FUSEBundleManifest{}, err
	}
	if err := tools.SignApplication(ctx, app.AppPath); err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: sign outer application: %w", err)
	}
	if _, err := validateFUSEBundle(ctx, app, sourceDigest, tools); err != nil {
		return FUSEBundleManifest{}, err
	}
	return manifest, nil
}

// Verify validates the outer app, nested signature, manifest, license, and bytes.
func (v *FUSEVerifier) Verify(ctx context.Context, app SignedApplication) (FUSEBundleManifest, error) {
	if v == nil {
		return FUSEBundleManifest{}, errors.New("holder: FUSE verifier is required")
	}
	return validateFUSEBundle(ctx, app, FUSESourceSHA256, v.tools)
}

func validateFUSEBundle(ctx context.Context, app SignedApplication, sourceDigest string, tools fuseBundleToolchain) (FUSEBundleManifest, error) {
	if tools == nil {
		return FUSEBundleManifest{}, errors.New("holder: FUSE bundle toolchain is required")
	}
	outer, err := tools.InspectApplication(ctx, app.AppPath)
	if err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: verify outer application: %w", err)
	}
	if err := verifyBundleCode(ctx, tools, app.AppPath, outer, app.TeamID, app.Runtime.SigningIdentifier); err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: outer application identity: %w", err)
	}
	library := filepath.Join(app.AppPath, FUSELibraryRelativePath)
	license := filepath.Join(app.AppPath, FUSELicenseRelativePath)
	manifestPath := filepath.Join(app.AppPath, FUSEManifestRelativePath)
	for _, path := range []string{library, license, manifestPath} {
		if !strictDescendant(app.AppPath, path) {
			return FUSEBundleManifest{}, errors.New("holder: FUSE bundle path escapes the application")
		}
		if err := requireRegularNonSymlink(path); err != nil {
			return FUSEBundleManifest{}, err
		}
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: read FUSE manifest: %w", err)
	}
	var manifest FUSEBundleManifest
	decoder := json.NewDecoder(strings.NewReader(string(manifestBytes)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: decode FUSE manifest: %w", err)
	}
	if manifest.Version != fuseManifestVersion || manifest.SourceSHA256 != sourceDigest ||
		manifest.LicenseSHA256 != FUSELicenseSHA256 || manifest.InstallName != FUSEInstallName ||
		!slices.Equal(manifest.Architectures, []string{"arm64", "x86_64"}) ||
		!slices.Equal(manifest.Dependencies, expectedFUSEDependencies) {
		return FUSEBundleManifest{}, errors.New("holder: FUSE manifest does not match the reviewed contract")
	}
	outerRequirement, err := bundleCodeRequirement(app.TeamID, app.Runtime.SigningIdentifier)
	if err != nil {
		return FUSEBundleManifest{}, err
	}
	if manifest.OuterDesignatedRequirement != outerRequirement ||
		manifest.OuterEntitlementsSHA256 != outer.EntitlementsSHA256 {
		return FUSEBundleManifest{}, errors.New("holder: outer application metadata changed after FUSE packaging")
	}
	if got, err := fileSHA256(license); err != nil || got != FUSELicenseSHA256 {
		return FUSEBundleManifest{}, errors.Join(errors.New("holder: FUSE license mismatch"), err)
	}
	if got, err := fileSHA256(library); err != nil || got != manifest.SignedSHA256 {
		return FUSEBundleManifest{}, errors.Join(errors.New("holder: signed FUSE library SHA-256 mismatch"), err)
	}
	inspection, err := tools.InspectLibrary(ctx, library)
	if err != nil {
		return FUSEBundleManifest{}, fmt.Errorf("holder: inspect bundled FUSE library: %w", err)
	}
	if err := validateFUSEMachO(inspection); err != nil {
		return FUSEBundleManifest{}, err
	}
	identifier := app.BundleID + ".fuse-t"
	nestedRequirement, err := bundleCodeRequirement(app.TeamID, identifier)
	if err != nil {
		return FUSEBundleManifest{}, err
	}
	if manifest.TeamID != app.TeamID || manifest.SigningIdentifier != identifier ||
		manifest.DesignatedRequirement != nestedRequirement {
		return FUSEBundleManifest{}, errors.New("holder: FUSE manifest code identity mismatch")
	}
	if err := verifyBundleCode(ctx, tools, library, inspection.Code, app.TeamID, identifier); err != nil {
		return FUSEBundleManifest{}, err
	}
	return manifest, nil
}

func validateFUSEMachO(inspection FUSELibraryInspection) error {
	architectures := slices.Clone(inspection.Architectures)
	dependencies := slices.Clone(inspection.Dependencies)
	slices.Sort(architectures)
	slices.Sort(dependencies)
	if !slices.Equal(architectures, []string{"arm64", "x86_64"}) || inspection.InstallName != FUSEInstallName ||
		!slices.Equal(dependencies, expectedFUSEDependencies) {
		return errors.New("holder: FUSE library architecture, install name, or dependencies differ from the reviewed artifact")
	}
	return nil
}

func fusePlanManifest(app SignedApplication) FUSEBundleManifest {
	identifier := app.BundleID + ".fuse-t"
	dr, _ := bundleCodeRequirement(app.TeamID, identifier)
	outerDR, _ := bundleCodeRequirement(app.TeamID, app.Runtime.SigningIdentifier)
	return FUSEBundleManifest{
		Version: fuseManifestVersion, SourceSHA256: FUSESourceSHA256,
		SignedSHA256: strings.Repeat("0", sha256.Size*2), LicenseSHA256: FUSELicenseSHA256,
		Architectures: []string{"arm64", "x86_64"}, InstallName: FUSEInstallName,
		Dependencies: slices.Clone(expectedFUSEDependencies), TeamID: app.TeamID,
		SigningIdentifier: identifier, DesignatedRequirement: dr,
		OuterDesignatedRequirement: outerDR,
		OuterEntitlementsSHA256:    strings.Repeat("0", sha256.Size*2),
	}
}

func validateFUSEPlanManifest(app SignedApplication, manifest FUSEBundleManifest) error {
	want := fusePlanManifest(app)
	want.SignedSHA256 = manifest.SignedSHA256
	want.OuterEntitlementsSHA256 = manifest.OuterEntitlementsSHA256
	if manifest.Version != want.Version || manifest.SourceSHA256 != want.SourceSHA256 ||
		manifest.SignedSHA256 != want.SignedSHA256 || manifest.LicenseSHA256 != want.LicenseSHA256 ||
		manifest.InstallName != want.InstallName || manifest.TeamID != want.TeamID ||
		manifest.SigningIdentifier != want.SigningIdentifier ||
		manifest.DesignatedRequirement != want.DesignatedRequirement ||
		manifest.OuterDesignatedRequirement != want.OuterDesignatedRequirement ||
		manifest.OuterEntitlementsSHA256 != want.OuterEntitlementsSHA256 ||
		!slices.Equal(manifest.Architectures, want.Architectures) ||
		!slices.Equal(manifest.Dependencies, want.Dependencies) {
		return errors.New("holder: runtime FUSE manifest is internally inconsistent")
	}
	digest, err := hex.DecodeString(manifest.SignedSHA256)
	if err != nil || len(digest) != sha256.Size {
		return errors.New("holder: runtime FUSE signed digest is invalid")
	}
	entitlementsDigest, err := hex.DecodeString(manifest.OuterEntitlementsSHA256)
	if err != nil || len(entitlementsDigest) != sha256.Size {
		return errors.New("holder: runtime outer entitlement digest is invalid")
	}
	return nil
}

func validateBundleCode(code BundleCodeIdentity, teamID, identifier string) error {
	if code.TeamID != teamID || code.SigningIdentifier != identifier {
		return errors.New("code signature does not match the exact same-Team identity")
	}
	if !code.HardenedRuntime {
		return errors.New("code signature does not enable the Hardened Runtime")
	}
	for _, entitlement := range injectionEntitlements {
		if _, exists := code.Entitlements[entitlement]; exists {
			return fmt.Errorf("code-injection entitlement %q is forbidden", entitlement)
		}
	}
	return nil
}

func verifyBundleCode(
	ctx context.Context,
	tools fuseBundleToolchain,
	path string,
	code BundleCodeIdentity,
	teamID, identifier string,
) error {
	if err := validateBundleCode(code, teamID, identifier); err != nil {
		return err
	}
	requirement, err := bundleCodeRequirement(teamID, identifier)
	if err != nil {
		return err
	}
	if err := tools.VerifyCodeRequirement(ctx, path, requirement); err != nil {
		return fmt.Errorf("code signature does not satisfy the exact same-Team designated requirement: %w", err)
	}
	return nil
}

func bundleCodeRequirement(teamID, identifier string) (string, error) {
	return (trust.Requirement{TeamID: teamID, SigningIdentifier: identifier}).DRString()
}

func makeRealDirectory(root, path string) error {
	if !strictDescendant(root, path) {
		return errors.New("holder: FUSE bundle directory escapes application")
	}
	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("holder: create FUSE bundle directory: %w", err)
	}
	for current := path; ; current = filepath.Dir(current) {
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("holder: FUSE bundle directory %q is not a real directory", current)
		}
		if current == root {
			break
		}
	}
	return nil
}

func requireRegularNonSymlink(path string) error {
	if !exactAbsolutePath(path) {
		return fmt.Errorf("path %q is not exact and absolute", path)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%q is not a regular non-symlink file", path)
	}
	return nil
}

func copyExactFile(source, destination string, mode os.FileMode) error {
	if err := rejectUnsafeDestination(destination); err != nil {
		return err
	}
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	output, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	return errors.Join(copyErr, output.Close())
}

func writeExactFile(path string, payload []byte, mode os.FileMode) error {
	if err := rejectUnsafeDestination(path); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(payload)
	return errors.Join(writeErr, file.Close())
}

func rejectUnsafeDestination(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("holder: FUSE bundle destination %q is not a regular non-symlink file", path)
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func validateBundledFUSEBytes(path, digest string) error {
	if filepath.Base(path) != "libfuse-t.dylib" || filepath.Base(filepath.Dir(path)) != "Frameworks" {
		return errors.New("holder: bundled FUSE library is not the exact Frameworks leaf")
	}
	if err := requireRegularNonSymlink(path); err != nil {
		return err
	}
	frameworks, err := os.Lstat(filepath.Dir(path))
	if err != nil || !frameworks.IsDir() || frameworks.Mode()&os.ModeSymlink != 0 {
		return errors.Join(errors.New("holder: bundled FUSE Frameworks directory is not real"), err)
	}
	got, err := fileSHA256(path)
	if err != nil {
		return err
	}
	if got != digest {
		return errors.New("holder: bundled FUSE library SHA-256 mismatch")
	}
	return nil
}
