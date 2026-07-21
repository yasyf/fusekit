//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/internal/presentationroot"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/transportproto"
	"golang.org/x/sys/unix"
)

// ErrNativeMount means the killable child failed to establish or retain the native root.
var ErrNativeMount = errors.New("mountmux: native child mount failed")

// RunNativeChild owns cgofuse only inside the disposable fixed-app child process.
// It returns only after cgofuse has exited; daemonkit kills and reaps a wedged child.
func RunNativeChild(ctx context.Context, config NativeChildConfig) (result error) {
	if err := validateNativeChildConfig(config); err != nil {
		return fmt.Errorf("%w: %v", ErrNativeMount, err)
	}
	if err := presentationroot.Validate(config.Root); err != nil {
		return fmt.Errorf("%w: revalidate presentation root: %v", ErrNativeMount, err)
	}
	root := config.Root
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("%w: resolve native executable: %v", ErrNativeMount, err)
	}
	expectedLibrary, err := bundledNativeLibrary(executable)
	if err != nil || config.Library != expectedLibrary {
		return fmt.Errorf("%w: native library is not the exact bundled leaf: %v", ErrNativeMount, err)
	}
	if configured := os.Getenv(nativeLibraryEnvironmentKey); configured != config.Library {
		return fmt.Errorf("%w: CGOFUSE_LIBFUSE_PATH names %q", ErrNativeMount, configured)
	}
	if err := validateNativeLibrary(config.Library, config.LibrarySHA256); err != nil {
		return fmt.Errorf("%w: %v", ErrNativeMount, err)
	}
	client, err := wire.NewClient(ctx, wire.ClientConfig{
		Build: transportproto.Build,
		Dial:  wire.UnixDialer(config.Socket),
	})
	if err != nil {
		return fmt.Errorf("%w: open holder session: %v", ErrNativeMount, err)
	}
	defer func() { result = errors.Join(result, client.Close()) }()
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
	defer func() { result = errors.Join(result, binding.Close()) }()
	resolver, err := NewRemoteResolver(mountClient)
	if err != nil {
		return err
	}
	catalog, err := NewRemoteNativeCatalog(catalogClient, mountClient)
	if err != nil {
		return err
	}
	callbacks, err := NewFuseFS(catalog, resolver)
	if err != nil {
		return err
	}
	host := fuse.NewFileSystemHost(callbacks)
	mount := startNativeMount(host, root, config.Options)
	if err := awaitNativeInitialization(ctx, mount.done, callbacks.initialized); err != nil {
		return errors.Join(err, mount.settle(root, unix.Unmount))
	}
	lifetime, stopSignals := rearmNativeSignals(ctx)
	defer stopSignals()
	readiness := systemNativeReadinessOps()
	ready := make(chan nativeReadinessResult, 1)
	go func() {
		proof, proofErr := awaitNativeReadiness(
			lifetime, root, callbacks.initialized, callbacks.rootReadEpoch, readiness,
		)
		ready <- nativeReadinessResult{proof: proof, err: proofErr}
	}()
	proof, err := awaitNativeProof(lifetime, mount.done, ready)
	if err != nil {
		return errors.Join(ErrNativeMount, err, mount.settle(root, unix.Unmount))
	}
	if err := validateNativeLibrary(config.Library, config.LibrarySHA256); err != nil {
		return errors.Join(
			fmt.Errorf("%w: revalidate fuse-t before readiness: %v", ErrNativeMount, err),
			mount.settle(root, unix.Unmount),
		)
	}
	if err := requireExactNativeMount(root, readiness.mountTable); err != nil {
		return errors.Join(ErrNativeMount, err, mount.settle(root, unix.Unmount))
	}
	if err := rejectExitedNative(mount.done, "readiness acknowledgement"); err != nil {
		return errors.Join(err, mount.settle(root, unix.Unmount))
	}
	if err := mountClient.NativeReady(lifetime, mountservice.NativeMountProof{
		PresentationRoot: proof.presentationRoot,
		Filesystem:       proof.filesystem,
		Source:           proof.source,
		CatalogEpoch:     proof.catalogEpoch,
	}); err != nil {
		return errors.Join(
			fmt.Errorf("%w: acknowledge readiness: %v", ErrNativeMount, err),
			mount.settle(root, unix.Unmount),
		)
	}
	select {
	case <-mount.done:
		return mount.err()
	case <-lifetime.Done():
		return errors.Join(lifetime.Err(), mount.settle(root, unix.Unmount))
	}
}

type nativeMount struct {
	done   chan struct{}
	result bool
}

func startNativeMount(host *fuse.FileSystemHost, root string, options []string) *nativeMount {
	mount := &nativeMount{done: make(chan struct{})}
	go func() {
		mount.result = host.Mount(root, append([]string(nil), options...))
		close(mount.done)
	}()
	return mount
}

func (m *nativeMount) err() error {
	if !m.result {
		return ErrNativeMount
	}
	return nil
}

func (m *nativeMount) settle(root string, unmount func(string, int) error) error {
	select {
	case <-m.done:
		return m.err()
	default:
	}
	unmountErr := unmount(root, 0)
	<-m.done
	if unmountErr != nil {
		unmountErr = errors.Join(ErrNativeMount, fmt.Errorf("regular native unmount: %w", unmountErr))
	}
	return errors.Join(m.err(), unmountErr)
}

func rearmNativeSignals(parent context.Context) (context.Context, context.CancelFunc) {
	signals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	signal.Reset(signals...)
	return signal.NotifyContext(parent, signals...)
}
