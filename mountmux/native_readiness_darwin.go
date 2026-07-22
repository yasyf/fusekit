//go:build darwin

package mountmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/fusekit/mountproto"
	"golang.org/x/sys/unix"
)

const (
	nativeReadinessPollInterval = 10 * time.Millisecond
)

type nativeMountEntry struct {
	mountpoint string
	filesystem string
	source     string
}

type nativeReadinessOps struct {
	mountTable   func() ([]nativeMountEntry, error)
	pollInterval time.Duration
}

type nativeMountIdentity struct {
	presentationRoot string
	filesystem       string
	source           string
}

type nativeReadinessResult struct {
	mount nativeMountIdentity
	err   error
}

func systemNativeReadinessOps() nativeReadinessOps {
	return nativeReadinessOps{
		mountTable:   readNativeMountTable,
		pollInterval: nativeReadinessPollInterval,
	}
}

func awaitNativeMountIdentity(
	ctx context.Context,
	root string,
	initialized <-chan struct{},
	ops nativeReadinessOps,
) (nativeMountIdentity, error) {
	if initialized == nil {
		return nativeMountIdentity{}, errors.New("mountmux: native mount identity is incomplete")
	}
	select {
	case <-initialized:
	case <-ctx.Done():
		return nativeMountIdentity{}, fmt.Errorf("mountmux: await native initialization: %w", ctx.Err())
	}

	if ops.mountTable == nil || ops.pollInterval <= 0 {
		return nativeMountIdentity{}, errors.New("mountmux: native readiness operations are incomplete")
	}
	expectedSource, err := mountproto.NativeMountSource(root)
	if err != nil {
		return nativeMountIdentity{}, fmt.Errorf("mountmux: derive native mount source: %w", err)
	}
	ticker := time.NewTicker(ops.pollInterval)
	defer ticker.Stop()
	for {
		table, err := ops.mountTable()
		if err != nil {
			return nativeMountIdentity{}, fmt.Errorf("mountmux: read native mount table: %w", err)
		}
		mounted, err := exactNativeMount(root, table)
		if err != nil {
			return nativeMountIdentity{}, err
		}
		if mounted {
			return nativeMountIdentity{
				presentationRoot: filepath.Clean(root),
				filesystem:       mountproto.NativeMountFilesystem,
				source:           expectedSource,
			}, nil
		}
		select {
		case <-ctx.Done():
			return nativeMountIdentity{}, fmt.Errorf("mountmux: await exact native mount: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// ConfirmNativeMount drives one catalog-backed traversal from outside the
// native child. Canceling the readiness authority returns immediately; child
// termination releases any in-flight kernel call.
func ConfirmNativeMount(ctx context.Context, root string) error {
	return confirmNativeMount(ctx, root, nativeThroughMountStat, nativeThroughMountRead)
}

func confirmNativeMount(
	ctx context.Context,
	root string,
	statRoot func(string) error,
	readRoot func(string) error,
) error {
	if statRoot == nil || readRoot == nil {
		return errors.New("mountmux: external native proof operations are incomplete")
	}
	result := make(chan error, 1)
	go func() {
		if err := statRoot(root); err != nil {
			result <- fmt.Errorf("mountmux: through-mount stat: %w", err)
			return
		}
		if err := readRoot(root); err != nil {
			result <- fmt.Errorf("mountmux: through-mount readdir: %w", err)
			return
		}
		result <- nil
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("mountmux: confirm native mount: %w", ctx.Err())
	}
}

func catalogEpochAfterExternalProof(before uint64, current func() uint64) (uint64, error) {
	if current == nil {
		return 0, errors.New("mountmux: catalog epoch source is missing")
	}
	served := current()
	if served == 0 || served <= before {
		return 0, errors.New("mountmux: holder traversal did not reach the catalog")
	}
	return served, nil
}

func awaitNativeInitialization(ctx context.Context, mountDone <-chan struct{}, initialized <-chan struct{}) error {
	select {
	case <-mountDone:
		return fmt.Errorf("%w: host exited before initialization", ErrNativeMount)
	case <-initialized:
		return rejectExitedNative(mountDone, "initialization")
	case <-ctx.Done():
		return fmt.Errorf("mountmux: await native initialization: %w", ctx.Err())
	}
}

func awaitNativeIdentity(
	ctx context.Context,
	mountDone <-chan struct{},
	ready <-chan nativeReadinessResult,
) (nativeMountIdentity, error) {
	select {
	case <-mountDone:
		return nativeMountIdentity{}, fmt.Errorf("%w: host exited before mount identity", ErrNativeMount)
	case outcome := <-ready:
		if outcome.err != nil {
			return nativeMountIdentity{}, outcome.err
		}
		return outcome.mount, rejectExitedNative(mountDone, "mount identity")
	case <-ctx.Done():
		return nativeMountIdentity{}, fmt.Errorf("mountmux: await native mount identity: %w", ctx.Err())
	}
}

func rejectExitedNative(mountDone <-chan struct{}, phase string) error {
	select {
	case <-mountDone:
		return fmt.Errorf("%w: host exited during %s", ErrNativeMount, phase)
	default:
		return nil
	}
}

func requireExactNativeMount(root string, mountTable func() ([]nativeMountEntry, error)) error {
	table, err := mountTable()
	if err != nil {
		return fmt.Errorf("mountmux: re-read native mount table: %w", err)
	}
	mounted, err := exactNativeMount(root, table)
	if err != nil {
		return err
	}
	if !mounted {
		return errors.New("mountmux: exact native mount disappeared before readiness acknowledgement")
	}
	return nil
}

func exactNativeMount(root string, table []nativeMountEntry) (bool, error) {
	expectedSource, err := mountproto.NativeMountSource(root)
	if err != nil {
		return false, fmt.Errorf("mountmux: derive native mount source: %w", err)
	}
	candidates := nativeMountCandidates(root)
	found := 0
	for _, entry := range table {
		for _, candidate := range candidates {
			if entry.mountpoint != candidate {
				continue
			}
			found++
			if entry.filesystem != mountproto.NativeMountFilesystem || entry.source != expectedSource {
				return false, fmt.Errorf(
					"mountmux: native root has filesystem %q from %q, want %q from %q",
					entry.filesystem, entry.source, mountproto.NativeMountFilesystem, expectedSource,
				)
			}
		}
	}
	if found > 1 {
		return false, errors.New("mountmux: native root appears more than once in the mount table")
	}
	return found == 1, nil
}

func nativeMountCandidates(root string) []string {
	root = filepath.Clean(root)
	candidates := []string{root}
	parent, err := filepath.EvalSymlinks(filepath.Dir(root))
	if err == nil {
		if alternate := filepath.Join(parent, filepath.Base(root)); alternate != root {
			candidates = append(candidates, alternate)
		}
	}
	return candidates
}

func readNativeMountTable() ([]nativeMountEntry, error) {
	count, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return nil, err
	}
	table := make([]unix.Statfs_t, count+4)
	count, err = unix.Getfsstat(table, unix.MNT_NOWAIT)
	if err != nil {
		return nil, err
	}
	entries := make([]nativeMountEntry, 0, count)
	for _, item := range table[:count] {
		entries = append(entries, nativeMountEntry{
			mountpoint: unix.ByteSliceToString(item.Mntonname[:]),
			filesystem: unix.ByteSliceToString(item.Fstypename[:]),
			source:     unix.ByteSliceToString(item.Mntfromname[:]),
		})
	}
	return entries, nil
}

func nativeThroughMountStat(root string) error {
	info, err := os.Stat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("native root is not a directory")
	}
	return nil
}

func nativeThroughMountRead(root string) (result error) {
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, directory.Close()) }()
	_, err = directory.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}
