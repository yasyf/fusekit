// Package appgroup resolves a macOS App Group's shared container directory.
//
// Prompt-free access to a group container (TCC's kTCCServiceSystemPolicyAppData)
// is contingent on resolving it via -[NSFileManager
// containerURLForSecurityApplicationGroupIdentifier:]; a hand-built path (a raw
// ~/Library/Group Containers join) forfeits that contract and triggers a consent
// prompt. This package never constructs the path — a missing container fails
// loud with ErrNoGroupContainer.
package appgroup

import (
	"errors"
	"fmt"
)

// ErrNoGroupContainer means the OS reported no container for the app group:
// the group is not an application-groups entitlement the running binary
// claims, or the platform has no App Group support (any non-darwin GOOS).
var ErrNoGroupContainer = errors.New("no app-group container")

// resolveContainer is the platform seam; tests override it to exercise
// GroupContainerDir without the Objective-C runtime or a real container.
var resolveContainer = platformResolveContainer

// GroupContainerDir returns the App Group's shared-container path, resolved via
// -[NSFileManager containerURLForSecurityApplicationGroupIdentifier:] (the
// mandatory path for prompt-free access). A nil container returns
// ErrNoGroupContainer wrapped with the group id; it never falls back to a
// hand-built path.
func GroupContainerDir(group string) (string, error) {
	dir, err := resolveContainer(group)
	if err != nil {
		return "", fmt.Errorf("app-group container %q: %w", group, err)
	}
	return dir, nil
}
