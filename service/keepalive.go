package service

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// openPath is where macOS ships /usr/bin/open; the LaunchAgent's Program.
const openPath = "/usr/bin/open"

// AppKeepAlive is a per-user KeepAlive LaunchAgent that keeps a long-lived
// .app bundle — the shared cask mount-holder at mountd.HolderApp — running
// for the login session. The agent's Program is `/usr/bin/open -g -W <app>`:
// -W makes open BLOCK until the app exits AND attach to an already-running
// instance rather than launching a second copy, so launchd's KeepAlive
// relaunches the app only on a real exit and never spins against a live
// holder (-g keeps the launch in the background). A private, cask-less holder
// binary is not a bundle open can adopt this way; it stays CLI-spawned by its
// consumer daemon (proc.Spawn) with no LaunchAgent.
type AppKeepAlive struct {
	// Label is the LaunchAgent label / reverse-DNS identifier (e.g.
	// "com.yasyf.fusekit-holder"): it names the plist and the launchctl
	// service target. Required.
	Label string
	// AppPath is the absolute .app bundle path open launches. Required.
	AppPath string
}

const keepAlivePlist = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>%s</string>
    <key>Program</key>
    <string>%s</string>
    <key>ProgramArguments</key>
    <array>
        <string>%s</string>
        <string>-g</string>
        <string>-W</string>
        <string>%s</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
</dict>
</plist>
`

func (k AppKeepAlive) validate() error {
	if k.Label == "" {
		return errors.New("keepalive agent: Label is required")
	}
	if !filepath.IsAbs(k.AppPath) {
		return fmt.Errorf("keepalive agent: AppPath %q must be an absolute .app bundle path", k.AppPath)
	}
	return nil
}

// plist renders the LaunchAgent plist; every interpolated value is XML-escaped.
func (k AppKeepAlive) plist() ([]byte, error) {
	if err := k.validate(); err != nil {
		return nil, err
	}
	label, app := xmlEscape(k.Label), xmlEscape(k.AppPath)
	return fmt.Appendf(nil, keepAlivePlist, label, openPath, openPath, app), nil
}

// PlistPath is the LaunchAgent plist location
// (~/Library/LaunchAgents/<Label>.plist).
func (k AppKeepAlive) PlistPath() (string, error) {
	return plistPath(k.Label)
}

// WritePlist renders and writes the LaunchAgent plist, returning the path
// written.
func (k AppKeepAlive) WritePlist() (string, error) {
	body, err := k.plist()
	if err != nil {
		return "", err
	}
	path, err := k.PlistPath()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return "", fmt.Errorf("ensure LaunchAgents dir: %w", err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		return "", fmt.Errorf("write plist: %w", err)
	}
	return path, nil
}

// Install writes the plist and (re)bootstraps the agent so the app runs now
// and at every login. Idempotent: an existing instance is booted out first —
// that kills only the blocked `open -W` waiter, never the app it waits on —
// and with the app already running, the fresh open attaches via -W instead of
// starting a second copy.
func (k AppKeepAlive) Install() error {
	plist, err := k.WritePlist()
	if err != nil {
		return err
	}
	// Best-effort remove any previous instance so bootstrap does not conflict.
	_, _ = launchctl("bootout", serviceTarget(k.Label))
	// enable before bootstrap: it clears a user/MDM disable regardless of load
	// state, and a disabled label fails bootstrap before enable could self-heal.
	if out, err := launchctl("enable", serviceTarget(k.Label)); err != nil {
		return fmt.Errorf("launchctl enable: %w: %s", err, out)
	}
	if out, err := launchctl("bootstrap", domainTarget(), plist); err != nil {
		return fmt.Errorf("launchctl bootstrap: %w: %s", err, out)
	}
	// bootstrap already started it (RunAtLoad); plain `kickstart` (no `-k`)
	// covers the loaded-but-not-running race and no-ops when already running.
	if out, err := launchctl("kickstart", serviceTarget(k.Label)); err != nil {
		return fmt.Errorf("launchctl kickstart: %w: %s", err, out)
	}
	return nil
}

// Uninstall boots out the agent and removes its plist; the app itself keeps
// running (bootout kills the open waiter, not the app). A missing plist is
// not an error. A bootout failure other than "not loaded" aborts before the
// plist is removed, so a still-loaded agent never becomes an on-disk-invisible
// orphan.
func (k AppKeepAlive) Uninstall() error {
	if err := k.validate(); err != nil {
		return err
	}
	if out, err := launchctl("bootout", serviceTarget(k.Label)); err != nil && !notLoaded(err) {
		return fmt.Errorf("launchctl bootout: %w: %s", err, out)
	}
	path, err := k.PlistPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// notLoaded reports whether a launchctl error means the label is not loaded:
// bootout exits 3 ("No such process") for an unloaded service target.
func notLoaded(err error) bool {
	var exit *exec.ExitError
	return errors.As(err, &exit) && exit.ExitCode() == 3
}

// Loaded reports whether launchd currently knows about the agent.
func (k AppKeepAlive) Loaded() bool {
	_, err := launchctl("print", serviceTarget(k.Label))
	return err == nil
}
