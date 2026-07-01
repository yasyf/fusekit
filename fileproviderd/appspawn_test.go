package fileproviderd

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func withLaunchApp(t *testing.T, fn func(ctx context.Context, appPath string) error) {
	t.Helper()
	prev := launchApp
	launchApp = fn
	t.Cleanup(func() { launchApp = prev })
}

func TestAppSpawnShortCircuitsWhenAvailable(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "control.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	var launched bool
	withLaunchApp(t, func(context.Context, string) error { launched = true; return nil })

	if err := (AppSpawn{AppPath: "/Apps/X.app", ControlSocket: socket, Timeout: time.Second}).EnsureRunning(context.Background()); err != nil {
		t.Fatalf("EnsureRunning with a live socket = %v, want nil", err)
	}
	if launched {
		t.Error("launched the app despite a live control socket; want a short-circuit")
	}
}

func TestAppSpawnLaunchesAndWaits(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "control.sock")
	var mu sync.Mutex
	var gotAppPath string
	withLaunchApp(t, func(_ context.Context, appPath string) error {
		mu.Lock()
		gotAppPath = appPath
		mu.Unlock()
		go func() {
			ln, err := net.Listen("unix", socket)
			if err != nil {
				return
			}
			t.Cleanup(func() { ln.Close() })
		}()
		return nil
	})

	err := (AppSpawn{AppPath: "/Apps/CCPoolStatus.app", ControlSocket: socket, Timeout: 2 * time.Second}).EnsureRunning(context.Background())
	if err != nil {
		t.Fatalf("EnsureRunning = %v, want nil once the app's socket comes up", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if gotAppPath != "/Apps/CCPoolStatus.app" {
		t.Errorf("launched %q, want the AppPath", gotAppPath)
	}
}

// TestAppSpawnSocketNeverComesUp pins that a socket that never appears fails
// transient (ErrAppUnavailable, naming the socket), not the retreat condition.
func TestAppSpawnSocketNeverComesUp(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "control.sock") // nothing ever binds it
	withLaunchApp(t, func(context.Context, string) error { return nil })

	err := (AppSpawn{AppPath: "/Apps/X.app", ControlSocket: socket, Timeout: 300 * time.Millisecond}).EnsureRunning(context.Background())
	if err == nil {
		t.Fatal("EnsureRunning with an app that never serves succeeded, want a timeout error")
	}
	if !errors.Is(err, ErrAppUnavailable) {
		t.Errorf("err = %v, want errors.Is ErrAppUnavailable", err)
	}
	if errors.Is(err, ErrCannotControl) {
		t.Errorf("err = %v, want a slow launch NOT classified as the retreat condition", err)
	}
	if !contains(err.Error(), socket) {
		t.Errorf("err = %q, want it to name the socket %s", err, socket)
	}
}

// TestAppSpawnLaunchError pins that a launch error wraps the transient
// ErrAppUnavailable, so the caller retries rather than retreats.
func TestAppSpawnLaunchError(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "control.sock")
	withLaunchApp(t, func(context.Context, string) error { return errors.New("bundle not found") })

	err := (AppSpawn{AppPath: "/Apps/Missing.app", ControlSocket: socket, Timeout: time.Second}).EnsureRunning(context.Background())
	if !errors.Is(err, ErrAppUnavailable) {
		t.Fatalf("err = %v, want errors.Is ErrAppUnavailable", err)
	}
	if !contains(err.Error(), "bundle not found") {
		t.Errorf("err = %q, want the underlying launch error in the chain", err)
	}
}

// TestAppSpawnLaunchUnsupportedFlowsThrough pins that the non-darwin permanent
// refusal is NOT folded into the transient ErrAppUnavailable.
func TestAppSpawnLaunchUnsupportedFlowsThrough(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "control.sock")
	withLaunchApp(t, func(context.Context, string) error { return ErrAppLaunchUnsupported })

	err := (AppSpawn{AppPath: "/Apps/X.app", ControlSocket: socket, Timeout: time.Second}).EnsureRunning(context.Background())
	if !errors.Is(err, ErrAppLaunchUnsupported) {
		t.Fatalf("err = %v, want errors.Is ErrAppLaunchUnsupported", err)
	}
	if errors.Is(err, ErrAppUnavailable) {
		t.Errorf("err = %v, want the permanent platform refusal NOT classified as transient-unavailable", err)
	}
}

func TestAppSpawnValidatesArgs(t *testing.T) {
	if err := (AppSpawn{ControlSocket: "/s.sock"}).EnsureRunning(context.Background()); !errors.Is(err, ErrAppUnavailable) {
		t.Errorf("missing AppPath err = %v, want ErrAppUnavailable", err)
	}
	if err := (AppSpawn{AppPath: "/Apps/X.app"}).EnsureRunning(context.Background()); !errors.Is(err, ErrAppUnavailable) {
		t.Errorf("missing ControlSocket err = %v, want ErrAppUnavailable", err)
	}
}
