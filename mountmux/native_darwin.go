//go:build darwin && cgo && fuse

package mountmux

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalogservice"
	"github.com/yasyf/fusekit/internal/presentationroot"
	"github.com/yasyf/fusekit/mountservice"
	"github.com/yasyf/fusekit/transportproto"
	"github.com/yasyf/fusekit/trustroles"
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
		WireBuild: transportproto.WireBuild,
		Dial:      wire.UnixDialer(config.Socket),
		Role:      trustroles.NativeChild,
	})
	if err != nil {
		return fmt.Errorf("%w: open runtime session: %v", ErrNativeMount, err)
	}
	defer func() { _ = client.Abort(ErrNativeMount) }()
	mountClient, err := mountservice.NewClientOn(client)
	if err != nil {
		return err
	}
	catalogClient, err := catalogservice.NewClientOn(client)
	if err != nil {
		return err
	}
	_, err = mountClient.BindNative(ctx)
	if err != nil {
		return fmt.Errorf("%w: bind runtime session: %v", ErrNativeMount, err)
	}
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
	settlement := nativeSettlementOps{unmount: unix.Unmount, after: time.After}
	if err := awaitNativeInitialization(ctx, mount.done, callbacks.initialized); err != nil {
		return errors.Join(err, mount.settle(root, settlement))
	}
	lifetime, stopSignals := rearmNativeSignals(ctx)
	defer stopSignals()
	readiness := systemNativeReadinessOps()
	ready := make(chan nativeReadinessResult, 1)
	go func() {
		identity, identityErr := awaitNativeMountIdentity(lifetime, root, callbacks.initialized, readiness)
		ready <- nativeReadinessResult{mount: identity, err: identityErr}
	}()
	identity, err := awaitNativeIdentity(lifetime, mount.done, ready)
	if err != nil {
		return errors.Join(ErrNativeMount, err, mount.settle(root, settlement))
	}
	probeToken, err := NewNativeProbeToken()
	if err != nil {
		return errors.Join(ErrNativeMount, err, mount.settle(root, settlement))
	}
	if err := callbacks.beginNativeProbe(probeToken); err != nil {
		return errors.Join(ErrNativeMount, err, mount.settle(root, settlement))
	}
	probeID, err := NativeProbeID(probeToken)
	if err != nil {
		return errors.Join(ErrNativeMount, err, mount.settle(root, settlement))
	}
	writeNativeReadinessEvent(os.Stderr, "native_mounted_request", probeID, "begin", 0)
	probeActive := true
	defer func() {
		if probeActive {
			callbacks.cancelNativeProbe(probeToken)
		}
	}()
	if err := mountClient.NativeMounted(lifetime, mountservice.NativeMountIdentity{
		PresentationRoot: identity.presentationRoot,
		Filesystem:       identity.filesystem,
		Source:           identity.source,
	}, probeToken); err != nil {
		writeNativeReadinessEvent(os.Stderr, "native_mounted_ack", probeID, "error", 0)
		callbacks.cancelNativeProbe(probeToken)
		probeActive = false
		return errors.Join(
			fmt.Errorf("%w: runtime causal root probe: %v", ErrNativeMount, err),
			mount.settle(root, settlement),
		)
	}
	writeNativeReadinessEvent(os.Stderr, "native_mounted_ack", probeID, "ok", 0)
	rootReadEpoch, err := callbacks.finishNativeProbe(probeToken)
	if err != nil {
		writeNativeReadinessEvent(os.Stderr, "root_callback_fence", probeID, "error", 0)
		callbacks.cancelNativeProbe(probeToken)
		probeActive = false
		return errors.Join(ErrNativeMount, err, mount.settle(root, settlement))
	}
	writeNativeReadinessEvent(os.Stderr, "root_callback_fence", probeID, "ok", rootReadEpoch)
	probeActive = false
	if err := validateNativeLibrary(config.Library, config.LibrarySHA256); err != nil {
		return errors.Join(
			fmt.Errorf("%w: revalidate fuse-t before readiness: %v", ErrNativeMount, err),
			mount.settle(root, settlement),
		)
	}
	if err := requireExactNativeMount(root, readiness.mountTable); err != nil {
		return errors.Join(ErrNativeMount, err, mount.settle(root, settlement))
	}
	if err := rejectExitedNative(mount.done, "readiness acknowledgement"); err != nil {
		return errors.Join(err, mount.settle(root, settlement))
	}
	writeNativeReadinessEvent(os.Stderr, "native_ready_request", probeID, "begin", rootReadEpoch)
	if err := mountClient.NativeReady(lifetime, mountservice.NativeMountProof{
		PresentationRoot: identity.presentationRoot,
		Filesystem:       identity.filesystem,
		Source:           identity.source,
		RootReadEpoch:    rootReadEpoch,
	}); err != nil {
		writeNativeReadinessEvent(os.Stderr, "native_ready_ack", probeID, "error", rootReadEpoch)
		return errors.Join(
			fmt.Errorf("%w: acknowledge readiness: %v", ErrNativeMount, err),
			mount.settle(root, settlement),
		)
	}
	writeNativeReadinessEvent(os.Stderr, "native_ready_ack", probeID, "ok", rootReadEpoch)
	select {
	case <-mount.done:
		return mount.err()
	case <-lifetime.Done():
		return errors.Join(lifetime.Err(), mount.settle(root, settlement))
	}
}

type nativeMount struct {
	done   chan struct{}
	result bool
}

const nativeSettlementTimeout = 250 * time.Millisecond

type nativeSettlementOps struct {
	unmount func(string, int) error
	after   func(time.Duration) <-chan time.Time
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

func (m *nativeMount) settle(root string, ops nativeSettlementOps) error {
	select {
	case <-m.done:
		return m.err()
	default:
	}
	deadline := ops.after(nativeSettlementTimeout)
	unmounted := make(chan error, 1)
	// Never join a wedged syscall; returning exits the disposable child.
	go func() { unmounted <- ops.unmount(root, 0) }()
	var unmountErr error
	select {
	case <-m.done:
		return m.err()
	case unmountErr = <-unmounted:
	case <-deadline:
		return errors.Join(
			ErrNativeMount,
			fmt.Errorf("regular native unmount within %s: %w", nativeSettlementTimeout, context.DeadlineExceeded),
		)
	}
	if unmountErr != nil {
		unmountErr = errors.Join(ErrNativeMount, fmt.Errorf("regular native unmount: %w", unmountErr))
	}
	select {
	case <-m.done:
		return errors.Join(m.err(), unmountErr)
	case <-deadline:
		return errors.Join(
			ErrNativeMount,
			unmountErr,
			fmt.Errorf("native host settlement within %s: %w", nativeSettlementTimeout, context.DeadlineExceeded),
		)
	}
}

func rearmNativeSignals(parent context.Context) (context.Context, context.CancelFunc) {
	signals := []os.Signal{os.Interrupt, syscall.SIGTERM}
	signal.Reset(signals...)
	return signal.NotifyContext(parent, signals...)
}
