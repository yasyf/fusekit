//go:build darwin && cgo && fuse

package mountmux

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

func validateNativePresentationRoot(root string) error {
	return requireNativePresentationRoot(root, nativeMountpointPresent, os.Lstat)
}

func requireNativePresentationRoot(
	root string,
	mounted func(string) (bool, error),
	lstat func(string) (os.FileInfo, error),
) error {
	live, err := mounted(root)
	if err != nil {
		return fmt.Errorf("mountmux: inspect native mount table: %w", err)
	}
	if live {
		return fmt.Errorf("mountmux: native root %q is already mounted", root)
	}
	info, err := lstat(root)
	if err != nil {
		return fmt.Errorf("mountmux: inspect native root %q: %w", root, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("mountmux: native root %q is not a real directory", root)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("mountmux: native root %q is not owned by the native user", root)
	}
	if info.Mode().Perm() != 0o700 {
		return fmt.Errorf("mountmux: native root %q mode is %#o, want 0700", root, info.Mode().Perm())
	}
	return nil
}

func nativeMountpointPresent(root string) (bool, error) {
	count, err := unix.Getfsstat(nil, unix.MNT_NOWAIT)
	if err != nil {
		return false, err
	}
	for {
		mounts := make([]unix.Statfs_t, count+8)
		count, err = unix.Getfsstat(mounts, unix.MNT_NOWAIT)
		if err != nil {
			return false, err
		}
		if count <= len(mounts) {
			for index := 0; index < count; index++ {
				if filepath.Clean(statfsMountpoint(mounts[index])) == root {
					return true, nil
				}
			}
			return false, nil
		}
	}
}

func statfsMountpoint(stat unix.Statfs_t) string {
	value := make([]byte, 0, len(stat.Mntonname))
	for _, character := range stat.Mntonname {
		if character == 0 {
			break
		}
		value = append(value, byte(character))
	}
	return string(value)
}
