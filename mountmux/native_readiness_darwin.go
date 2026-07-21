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

	"golang.org/x/sys/unix"
)

const (
	nativeMountFilesystem       = "nfs"
	nativeMountSource           = "fuse-t:/mount"
	nativeReadinessPollInterval = 10 * time.Millisecond
	nativeThroughMountTimeout   = 2 * time.Second
)

type nativeMountEntry struct {
	mountpoint string
	filesystem string
	source     string
}

type nativeReadinessOps struct {
	mountTable     func() ([]nativeMountEntry, error)
	statRoot       func(string) error
	readRoot       func(string) error
	pollInterval   time.Duration
	throughTimeout time.Duration
}

func systemNativeReadinessOps() nativeReadinessOps {
	return nativeReadinessOps{
		mountTable: readNativeMountTable,
		statRoot:   nativeThroughMountStat, readRoot: nativeThroughMountRead,
		pollInterval: nativeReadinessPollInterval, throughTimeout: nativeThroughMountTimeout,
	}
}

func awaitNativeReadiness(
	ctx context.Context,
	root string,
	initialized <-chan struct{},
	catalogEpoch func() uint64,
	ops nativeReadinessOps,
) error {
	if initialized == nil || catalogEpoch == nil {
		return errors.New("mountmux: native readiness proof is incomplete")
	}
	select {
	case <-initialized:
	case <-ctx.Done():
		return fmt.Errorf("mountmux: await native initialization: %w", ctx.Err())
	}

	if ops.mountTable == nil || ops.statRoot == nil || ops.readRoot == nil || ops.pollInterval <= 0 || ops.throughTimeout <= 0 {
		return errors.New("mountmux: native readiness operations are incomplete")
	}
	ticker := time.NewTicker(ops.pollInterval)
	defer ticker.Stop()
	for {
		table, err := ops.mountTable()
		if err != nil {
			return fmt.Errorf("mountmux: read native mount table: %w", err)
		}
		mounted, err := exactNativeMount(root, table)
		if err != nil {
			return err
		}
		if mounted {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("mountmux: await exact native mount: %w", ctx.Err())
		case <-ticker.C:
		}
	}

	beforeCatalog := catalogEpoch()
	result := make(chan error, 1)
	go func() {
		if err := ops.statRoot(root); err != nil {
			result <- fmt.Errorf("mountmux: through-mount stat: %w", err)
			return
		}
		if err := ops.readRoot(root); err != nil {
			result <- fmt.Errorf("mountmux: through-mount readdir: %w", err)
			return
		}
		if catalogEpoch() <= beforeCatalog {
			result <- errors.New("mountmux: through-mount readdir did not reach the catalog")
			return
		}
		result <- nil
	}()
	timer := time.NewTimer(ops.throughTimeout)
	defer timer.Stop()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return fmt.Errorf("mountmux: confirm native mount: %w", ctx.Err())
	case <-timer.C:
		return fmt.Errorf("mountmux: confirm native mount within %s: %w", ops.throughTimeout, context.DeadlineExceeded)
	}
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

func awaitNativeProof(ctx context.Context, mountDone <-chan struct{}, ready <-chan error) error {
	select {
	case <-mountDone:
		return fmt.Errorf("%w: host exited before readiness proof", ErrNativeMount)
	case err := <-ready:
		if err != nil {
			return err
		}
		return rejectExitedNative(mountDone, "readiness proof")
	case <-ctx.Done():
		return fmt.Errorf("mountmux: await native readiness proof: %w", ctx.Err())
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
	candidates := nativeMountCandidates(root)
	found := 0
	for _, entry := range table {
		for _, candidate := range candidates {
			if entry.mountpoint != candidate {
				continue
			}
			found++
			if entry.filesystem != nativeMountFilesystem || entry.source != nativeMountSource {
				return false, fmt.Errorf(
					"mountmux: native root has filesystem %q from %q, want %q from %q",
					entry.filesystem, entry.source, nativeMountFilesystem, nativeMountSource,
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
