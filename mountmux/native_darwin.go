//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/fuset"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/transportproto"
)

// ErrNativeMount means the killable child failed to establish or retain the native root.
var ErrNativeMount = errors.New("mountmux: native child mount failed")

// RunNativeChild owns cgofuse only inside the disposable fixed-app child process.
// It returns only after cgofuse has exited; a wedged startup or unmount remains inside
// this process so the holder can TERM/KILL and reap the whole process group.
func RunNativeChild(ctx context.Context, config NativeChildConfig) error {
	if err := validateNativeChildConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrNativeMount, err)
	}
	root := filepath.Clean(config.Root)
	if !fuset.Installed() {
		return fmt.Errorf("%w: fuse-t is not installed", ErrNativeMount)
	}
	if configured := os.Getenv("CGOFUSE_LIBFUSE_PATH"); configured != "" && configured != fuset.Dylib {
		return fmt.Errorf("%w: CGOFUSE_LIBFUSE_PATH names %q", ErrNativeMount, configured)
	}
	if err := os.Setenv("CGOFUSE_LIBFUSE_PATH", fuset.Dylib); err != nil {
		return fmt.Errorf("%w: pin fuse-t library: %v", ErrNativeMount, err)
	}
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Build: transportproto.Build,
		Dial:  wire.UnixDialer(config.Socket),
	})
	if err != nil {
		return fmt.Errorf("%w: open holder session: %v", ErrNativeMount, err)
	}
	defer client.Close()
	mountClient, err := mountservice.NewClientOn(client)
	if err != nil {
		return err
	}
	catalogClient, err := catalogservice.NewClientOn(client)
	if err != nil {
		return err
	}
	binding, err := mountClient.BindNative(ctx)
	if err != nil {
		return fmt.Errorf("%w: bind holder session: %v", ErrNativeMount, err)
	}
	defer binding.Close()
	resolver, err := NewRemoteResolver(mountClient)
	if err != nil {
		return err
	}
	catalog, err := NewRemoteNativeCatalog(catalogClient)
	if err != nil {
		return err
	}
	callbacks, err := NewFuseFS(catalog, resolver)
	if err != nil {
		return err
	}
	host := fuse.NewFileSystemHost(callbacks)
	mounted := make(chan bool, 1)
	go func() { mounted <- host.Mount(root, append([]string(nil), config.Options...)) }()

	select {
	case live := <-mounted:
		if !live {
			return ErrNativeMount
		}
		return fmt.Errorf("%w: host exited before initialization", ErrNativeMount)
	case <-callbacks.initialized:
	case <-ctx.Done():
		live := <-mounted
		if !live {
			return errors.Join(ErrNativeMount, ctx.Err())
		}
		return ctx.Err()
	}
	if err := mountClient.NativeReady(ctx); err != nil {
		_ = host.Unmount()
		<-mounted
		return fmt.Errorf("%w: acknowledge readiness: %v", ErrNativeMount, err)
	}
	select {
	case live := <-mounted:
		if !live {
			return ErrNativeMount
		}
		return nil
	case <-ctx.Done():
		if !host.Unmount() {
			return fmt.Errorf("%w: native unmount rejected", ErrNativeMount)
		}
		live := <-mounted
		if !live {
			return errors.Join(ErrNativeMount, ctx.Err())
		}
		return ctx.Err()
	}
}
