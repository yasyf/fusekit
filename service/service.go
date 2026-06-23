// Package service installs and manages a consumer's long-lived background
// daemon as a macOS user LaunchAgent — a per-user agent, not a root daemon, so
// it can reach the user's login Keychain — and reconciles with a
// Homebrew-managed install when the binary came from `brew`. The launchctl
// choreography (bootout → bootstrap → enable → kickstart) and the brew
// detection are generic; an Agent carries only what varies between consumers —
// its label, formula name, program arguments, log path, and environment — so
// cc-pool and cc-squash share one lifecycle primitive. The launchctl/brew
// calls are macOS-only at runtime; the package builds on every platform.
package service

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/template"
)

const plistTemplateText = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{{.Label}}</string>
    <key>ProgramArguments</key>
    <array>
{{range .Args}}        <string>{{.}}</string>
{{end}}    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>10</integer>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>{{.Log}}</string>
    <key>StandardErrorPath</key>
    <string>{{.Log}}</string>
    <key>EnvironmentVariables</key>
    <dict>
{{range .Env}}        <key>{{.Key}}</key>
        <string>{{.Value}}</string>
{{end}}    </dict>
</dict>
</plist>
`

var plistTemplate = template.Must(template.New("plist").Parse(plistTemplateText))

// xmlEscape escapes a value for safe interpolation into the plist XML: a path
// containing <, >, or & (all legal on APFS) would otherwise produce a malformed
// plist that launchctl rejects.
func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

type plistKV struct{ Key, Value string }

type plistData struct {
	Label string
	Args  []string
	Log   string
	Env   []plistKV
}

// Agent is a consumer's background daemon as a macOS user LaunchAgent. The
// launchctl/brew mechanics are generic; the fields are everything that varies
// between consumers.
type Agent struct {
	// Label is the LaunchAgent label / reverse-DNS identifier, e.g.
	// "com.yasyf.cc-pool". It names the plist
	// (~/Library/LaunchAgents/<Label>.plist) and the launchctl service target.
	// Required.
	Label string
	// Formula is the Homebrew formula name (e.g. "cc-pool") used to detect a
	// brew-managed install and to drive `brew services`. Required for the brew
	// methods; the launchctl methods ignore it.
	Formula string
	// Program is the absolute path launchd execs. Empty means the running
	// binary (os.Executable, deliberately WITHOUT EvalSymlinks so a Homebrew
	// /opt/homebrew/bin symlink stays a constant launchd program path across
	// `brew upgrade` instead of churning to each new Cellar path).
	Program string
	// Args are the arguments passed after Program (e.g. {"daemon"}).
	Args []string
	// LogPath is the file launchd points StandardOut and StandardError at; its
	// parent directory is created 0700 before the plist is written.
	LogPath string
	// Env are EnvironmentVariables entries written into the plist (e.g. PATH and
	// a consumer's fuse library path). Keys are emitted in sorted order so the
	// rendered plist is reproducible.
	Env map[string]string
}

// PlistPath is the LaunchAgent plist location
// (~/Library/LaunchAgents/<Label>.plist).
func (a Agent) PlistPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, "Library", "LaunchAgents", a.Label+".plist"), nil
}

// WritePlist renders and writes the LaunchAgent plist for this Agent, returning
// the path written. The program binary defaults to os.Executable() when Program
// is empty; every interpolated value is XML-escaped.
func (a Agent) WritePlist() (string, error) {
	bin := a.Program
	if bin == "" {
		exe, err := os.Executable()
		if err != nil {
			return "", fmt.Errorf("resolve executable: %w", err)
		}
		bin = exe
	}
	path, err := a.PlistPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("ensure LaunchAgents dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(a.LogPath), 0o700); err != nil {
		return "", fmt.Errorf("ensure log dir: %w", err)
	}
	args := make([]string, 0, len(a.Args)+1)
	args = append(args, xmlEscape(bin))
	for _, arg := range a.Args {
		args = append(args, xmlEscape(arg))
	}
	env := make([]plistKV, 0, len(a.Env))
	for _, k := range slices.Sorted(maps.Keys(a.Env)) {
		env = append(env, plistKV{Key: xmlEscape(k), Value: xmlEscape(a.Env[k])})
	}
	var buf bytes.Buffer
	if err := plistTemplate.Execute(&buf, plistData{Label: xmlEscape(a.Label), Args: args, Log: xmlEscape(a.LogPath), Env: env}); err != nil {
		return "", fmt.Errorf("render plist: %w", err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		return "", fmt.Errorf("write plist: %w", err)
	}
	return path, nil
}

func domainTarget() string { return "gui/" + strconv.Itoa(os.Getuid()) }

func (a Agent) serviceTarget() string { return domainTarget() + "/" + a.Label }

func launchctl(args ...string) (string, error) {
	out, err := exec.Command("launchctl", args...).CombinedOutput()
	return string(out), err
}

// Install writes the plist and (re)bootstraps the agent so it runs now and at
// every login. Idempotent: an existing instance is booted out first.
func (a Agent) Install() error {
	plist, err := a.WritePlist()
	if err != nil {
		return err
	}
	// Best-effort remove any previous instance so bootstrap does not conflict.
	_, _ = launchctl("bootout", a.serviceTarget())
	if out, err := launchctl("bootstrap", domainTarget(), plist); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w: %s", err, out)
	}
	_, _ = launchctl("enable", a.serviceTarget())
	// bootstrap already started the agent (RunAtLoad); plain `kickstart` (no
	// `-k`) only covers the loaded-but-not-running race and is a no-op when it is
	// already running, so we don't kill and cold-start it a second time.
	if out, err := launchctl("kickstart", a.serviceTarget()); err != nil {
		return fmt.Errorf("launchctl kickstart: %w: %s", err, out)
	}
	return nil
}

// Uninstall boots out the agent and removes its plist. A missing plist is not
// an error.
func (a Agent) Uninstall() error {
	_, _ = launchctl("bootout", a.serviceTarget())
	path, err := a.PlistPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Loaded reports whether launchd currently knows about the agent.
func (a Agent) Loaded() bool {
	_, err := launchctl("print", a.serviceTarget())
	return err == nil
}

// IsBrewManaged reports whether the running binary was installed via Homebrew,
// in which case the daemon should be managed with `brew services` rather than
// the self-rolled launchctl path. It inspects the executable path only (no
// shelling out), so it is cheap on any code path.
func (a Agent) IsBrewManaged() bool {
	exe, err := os.Executable()
	if err != nil {
		return false
	}
	if a.pathIsBrewManaged(exe) {
		return true
	}
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		return a.pathIsBrewManaged(resolved)
	}
	return false
}

// pathIsBrewManaged reports whether an executable path indicates a Homebrew
// install of this Agent's formula (Cellar/opt/bin under a brew prefix).
func (a Agent) pathIsBrewManaged(p string) bool {
	if strings.Contains(p, "/Cellar/"+a.Formula+"/") {
		return true
	}
	for _, prefix := range brewPrefixes() {
		if strings.HasPrefix(p, prefix+"/opt/"+a.Formula+"/") || p == filepath.Join(prefix, "bin", a.Formula) {
			return true
		}
	}
	return false
}

// brewPrefixes returns candidate Homebrew prefixes (HOMEBREW_PREFIX if set,
// else the standard arm64 and Intel locations).
func brewPrefixes() []string {
	if v := os.Getenv("HOMEBREW_PREFIX"); v != "" {
		return []string{v}
	}
	return []string{"/opt/homebrew", "/usr/local"}
}

func (a Agent) brewLabel() string { return "homebrew.mxcl." + a.Formula }

func (a Agent) brewServices(action string) error {
	cmd := exec.Command("brew", "services", action, a.Formula)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

// BrewStart starts the daemon via `brew services` (installs the user agent).
func (a Agent) BrewStart() error { return a.brewServices("start") }

// BrewStop stops and unloads the brew-managed agent.
func (a Agent) BrewStop() error { return a.brewServices("stop") }

// BrewKickstart ensures the brew-managed daemon is actually running. `brew
// services start` only bootstraps the job; on a stop/start bootout race it can
// leave the job loaded-but-never-running (RunAtLoad fires only at bootstrap),
// so we kick it explicitly. Plain `kickstart` (no `-k`) starts a
// loaded-but-not-running job and is a no-op when it is already running.
func (a Agent) BrewKickstart() error {
	target := domainTarget() + "/" + a.brewLabel()
	if out, err := launchctl("kickstart", target); err != nil {
		return fmt.Errorf("launchctl kickstart %s: %w: %s", target, err, out)
	}
	return nil
}

// BrewInfo returns `brew services info <formula>` output for status display.
func (a Agent) BrewInfo() (string, error) {
	out, err := exec.Command("brew", "services", "info", a.Formula).CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// BrewReinstall runs `brew reinstall <formula>`, streaming brew's output to out
// and errOut. A consumer that ships build variants selected at formula-install
// time (e.g. cc-pool's pure vs. -tags fuse binary, chosen by what is present on
// the machine) calls this after installing the missing dependency so the
// formula re-runs its install logic and swaps in the right variant. Errors when
// Homebrew is absent or the reinstall fails.
func (a Agent) BrewReinstall(out, errOut io.Writer) error {
	return brewStream(out, errOut, "reinstall", a.Formula)
}

// InstallCask runs `brew install --cask <ref>`, streaming brew's output to out
// and errOut. ref may carry a tap (e.g. "macos-fuse-t/homebrew-cask/fuse-t"),
// which brew auto-taps. `-y` disables the interactive confirmation so the
// install runs unattended. Errors when Homebrew is absent or the install fails;
// it does not verify the cask afterwards (the caller checks when that matters).
func InstallCask(ref string, out, errOut io.Writer) error {
	return brewStream(out, errOut, "install", "-y", "--cask", ref)
}

// brewStream runs `brew <args...>` with its stdout/stderr wired to out/errOut.
// It fails fast and loud when Homebrew is not on PATH — the one shared cause
// that makes every brew call here impossible — rather than letting exec surface
// an opaque "executable not found".
func brewStream(out, errOut io.Writer, args ...string) error {
	if _, err := exec.LookPath("brew"); err != nil {
		return fmt.Errorf("Homebrew (brew) is not installed or not on PATH: %w", err)
	}
	//nolint:gosec // G204: args are the caller's own fixed brew subcommand, not user input
	cmd := exec.Command("brew", args...)
	cmd.Stdout, cmd.Stderr = out, errOut
	return cmd.Run()
}
