//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/catalog"
)

const (
	defaultMountWait      = 30 * time.Second
	defaultFirstMountWait = 5 * time.Minute
	defaultServeExitWait  = 5 * time.Second
)

type nativeHandle interface {
	Unmount() error
	Done() <-chan struct{}
}

type mountCall func(fusekit.Config) (nativeHandle, error)

var nativeMount mountCall = func(config fusekit.Config) (nativeHandle, error) { return fusekit.Mount(config) }

// NativeMountConfig configures the one process-lifetime catalog mount.
type NativeMountConfig struct {
	Catalog       *catalog.Catalog
	Options       []string
	Wait          time.Duration
	FirstWait     time.Duration
	ServeExitWait time.Duration
	Ready         <-chan struct{}
	ReArmSignals  func()
}

// NativeMount owns the sole low-level fusekit mount handle.
type NativeMount struct {
	config NativeMountConfig

	mu      sync.Mutex
	closeMu sync.Mutex
	started bool
	closed  bool
	handle  nativeHandle
	fs      *FuseFS
}

// NewNativeMount constructs an unstarted single-root native adapter.
func NewNativeMount(config NativeMountConfig) (*NativeMount, error) {
	if config.Catalog == nil {
		return nil, errors.New("mountmux: native mount catalog is required")
	}
	if config.Wait <= 0 {
		config.Wait = defaultMountWait
	}
	if config.FirstWait <= 0 {
		config.FirstWait = defaultFirstMountWait
	}
	if config.ServeExitWait <= 0 {
		config.ServeExitWait = defaultServeExitWait
	}
	return &NativeMount{config: config}, nil
}

// Start establishes the fixed native root exactly once.
func (mount *NativeMount) Start(ctx context.Context, root string, resolver Resolver) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mount.mu.Lock()
	if mount.started {
		mount.mu.Unlock()
		return ErrStarted
	}
	if mount.closed {
		mount.mu.Unlock()
		return ErrClosed
	}
	mount.started = true
	mount.mu.Unlock()

	callbacks, err := NewFuseFS(mount.config.Catalog, resolver)
	if err != nil {
		return err
	}
	options := mount.config.Options
	if len(options) == 0 {
		options = fusekit.MountOptions{
			Volname: "FuseKit", NoBrowse: true, Extra: []string{"rwsize=1048576"},
		}.Build()
	}
	ready := func() bool {
		if !fusekit.Mounted(root) {
			return false
		}
		if mount.config.Ready == nil {
			return true
		}
		select {
		case <-mount.config.Ready:
			return true
		default:
			return false
		}
	}
	handle, err := nativeMount(fusekit.Config{
		Base: filepath.Dir(root), Dir: root, FS: callbacks, Options: options,
		Ready: ready, ProbePath: root, Wait: mount.config.Wait,
		FirstWait: mount.config.FirstWait, ReArmSignals: mount.config.ReArmSignals,
	})
	if err != nil {
		return fmt.Errorf("mountmux: establish fixed native root: %w", err)
	}
	mount.mu.Lock()
	mount.handle = handle
	mount.fs = callbacks
	mount.mu.Unlock()
	return nil
}

// Close gracefully unmounts the fixed native root and waits bounded for its server exit.
func (mount *NativeMount) Close() error {
	mount.closeMu.Lock()
	defer mount.closeMu.Unlock()
	mount.mu.Lock()
	if mount.closed {
		mount.mu.Unlock()
		return ErrClosed
	}
	handle := mount.handle
	mount.mu.Unlock()
	if handle == nil {
		mount.mu.Lock()
		mount.closed = true
		mount.mu.Unlock()
		return nil
	}
	if err := handle.Unmount(); err != nil {
		return err
	}
	timer := time.NewTimer(mount.config.ServeExitWait)
	defer timer.Stop()
	select {
	case <-handle.Done():
		mount.mu.Lock()
		mount.closed = true
		mount.mu.Unlock()
		return nil
	case <-timer.C:
		return fmt.Errorf("mountmux: native server did not exit within %s", mount.config.ServeExitWait)
	}
}

// Filesystem returns the callback adapter after a successful Start.
func (mount *NativeMount) Filesystem() *FuseFS {
	mount.mu.Lock()
	defer mount.mu.Unlock()
	return mount.fs
}

var _ NativeRoot = (*NativeMount)(nil)
