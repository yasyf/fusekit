package holder

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yasyf/daemonkit/proc"
)

// RecoverProcesses reaps every exact prior-generation process recorded by the
// plan before product authority or runtime state is opened.
func RecoverProcesses(ctx context.Context, plan DeploymentPlan) error {
	paths := plan.Paths()
	if err := prepareRuntimeDirectory(plan.home, paths.Directory); err != nil {
		return err
	}
	registry, err := processRegistry(paths.ProcessStore, nil)
	if err != nil {
		return err
	}
	err = registry.Recover(ctx)
	if err != nil {
		return fmt.Errorf("holder: recover durable processes: %w", err)
	}
	return nil
}

type durableProcessRegistry struct {
	*proc.Reaper
}

func processRegistry(path string, generation func() (string, error)) (*durableProcessRegistry, error) {
	if generation == nil {
		generation = newProcessGeneration
	}
	value, err := generation()
	if err != nil {
		return nil, fmt.Errorf("holder: create process generation: %w", err)
	}
	if value == "" {
		return nil, errors.New("holder: process generation is empty")
	}
	return &durableProcessRegistry{Reaper: &proc.Reaper{
		Store: &proc.FileStore{Path: path}, Generation: value,
	}}, nil
}

func (r *durableProcessRegistry) Recover(ctx context.Context) error {
	return r.Reap(ctx)
}

func (r *durableProcessRegistry) RegisterOwner(
	ctx context.Context,
	identity proc.Identity,
	class proc.RecoveryClass,
) (proc.Record, error) {
	return r.TrackIdentity(ctx, identity, class)
}

func newProcessGeneration() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "fusekit-" + hex.EncodeToString(random[:]), nil
}

func prepareRuntimeDirectory(home, path string) error {
	if err := validateRuntimePath(home, path, true); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("holder: protect runtime directory: %w", err)
	}
	if err := validateRuntimeAncestors(home, path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("holder: verify runtime directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("holder: runtime directory %q is not a private real directory", path)
	}
	return nil
}

func validateRuntimeAncestors(home, path string) error {
	return validateRuntimePath(home, path, false)
}

func validateRuntimePath(home, path string, create bool) error {
	if !strictDescendant(home, path) {
		return fmt.Errorf("holder: runtime directory %q is not below user home %q", path, home)
	}
	if err := requireRealDirectory(home, "user home"); err != nil {
		return err
	}
	relative, err := filepath.Rel(home, path)
	if err != nil {
		return fmt.Errorf("holder: resolve runtime directory: %w", err)
	}
	current := home
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, statErr := os.Lstat(current)
		if errors.Is(statErr, os.ErrNotExist) {
			if !create {
				return nil
			}
			if err := os.Mkdir(current, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
				return fmt.Errorf("holder: create runtime directory component %q: %w", current, err)
			}
			info, statErr = os.Lstat(current)
		}
		if statErr != nil {
			return fmt.Errorf("holder: inspect runtime directory component %q: %w", current, statErr)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("holder: runtime directory component %q is not a real directory", current)
		}
	}
	return nil
}

func requireRealDirectory(path, name string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("holder: inspect %s %q: %w", name, path, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("holder: %s %q is not a real directory", name, path)
	}
	return nil
}
