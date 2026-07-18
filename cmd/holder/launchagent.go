package main

import (
	"context"
	"fmt"

	"github.com/yasyf/daemonkit/service"
	"github.com/yasyf/fusekit/mountd"
)

// holderLabel is the cask holder's LaunchAgent label.
const holderLabel = "com.yasyf.fusekit-holder"

// holderKeepAlive is the cask-owned KeepAlive agent that relaunches the
// self-retiring holder: `open -g -W` against the stable cask bundle path.
func holderKeepAlive() service.AppKeepAlive {
	return service.AppKeepAlive{Label: holderLabel, AppPath: mountd.HolderApp}
}

// keepAliver is the AppKeepAlive surface launchAgentRun drives; a seam so
// tests never touch launchctl or ~/Library/LaunchAgents.
type keepAliver interface {
	Install(ctx context.Context) error
	Uninstall(ctx context.Context) error
}

// launchAgentRun executes the launchagent flag action, reporting whether one
// was requested; when handled, the caller exits instead of serving.
func launchAgentRun(ctx context.Context, install, uninstall bool, k keepAliver) (handled bool, err error) {
	switch {
	case install:
		if err := k.Install(ctx); err != nil {
			return true, fmt.Errorf("install launchagent: %w", err)
		}
		return true, nil
	case uninstall:
		if err := k.Uninstall(ctx); err != nil {
			return true, fmt.Errorf("uninstall launchagent: %w", err)
		}
		return true, nil
	}
	return false, nil
}
