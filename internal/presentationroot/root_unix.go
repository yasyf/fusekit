//go:build darwin || linux

// Package presentationroot owns the exact native presentation-root invariant.
package presentationroot

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// ErrInvalid means the native presentation root is not safe to mount.
var ErrInvalid = errors.New("presentation root is invalid")

var (
	currentUID = os.Geteuid
	mountedAt  = platformMounted
)

// Prepare creates an absent presentation-root leaf and validates the exact result.
func Prepare(path string) error {
	if err := validateExact(path); err != nil {
		return err
	}
	if err := requireUnmounted(path); err != nil {
		return err
	}
	if err := requireRealDirectories(filepath.Dir(path)); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("%w: create %q: %v", ErrInvalid, path, err)
	}
	return Validate(path)
}

// Validate proves that path is one empty, private, unmounted directory owned by this user.
func Validate(path string) error {
	if err := validateExact(path); err != nil {
		return err
	}
	if err := requireUnmounted(path); err != nil {
		return err
	}
	if err := requireRealDirectories(filepath.Dir(path)); err != nil {
		return err
	}
	file, err := openDirectory(path)
	if err != nil {
		return fmt.Errorf("%w: open exact directory %q: %v", ErrInvalid, path, err)
	}
	defer file.Close()

	var status unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &status); err != nil {
		return fmt.Errorf("%w: inspect %q: %v", ErrInvalid, path, err)
	}
	uid := currentUID()
	if uid < 0 || status.Uid != uint32(uid) {
		return fmt.Errorf("%w: %q is owned by uid %d, want %d", ErrInvalid, path, status.Uid, uid)
	}
	if status.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fmt.Errorf("%w: %q is not a directory", ErrInvalid, path)
	}
	if status.Mode&0o7777 != 0o700 {
		return fmt.Errorf("%w: %q has mode %#o, want 0700", ErrInvalid, path, status.Mode&0o7777)
	}
	if _, err := file.Readdirnames(1); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: %q is not empty", ErrInvalid, path)
		}
		return fmt.Errorf("%w: read %q: %v", ErrInvalid, path, err)
	}
	return requireUnmounted(path)
}

func validateExact(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) {
		return fmt.Errorf("%w: %q is not an exact absolute path", ErrInvalid, path)
	}
	return nil
}

func requireRealDirectories(path string) error {
	relative, err := filepath.Rel(string(filepath.Separator), path)
	if err != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("%w: resolve ancestors for %q", ErrInvalid, path)
	}
	current := string(filepath.Separator)
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if err != nil {
			return fmt.Errorf("%w: inspect ancestor %q: %v", ErrInvalid, current, err)
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: ancestor %q is not a real directory", ErrInvalid, current)
		}
	}
	return nil
}

func requireUnmounted(path string) error {
	mounted, err := mountedAt(path)
	if err != nil {
		return fmt.Errorf("%w: inspect mount table for %q: %v", ErrInvalid, path, err)
	}
	if mounted {
		return fmt.Errorf("%w: %q is already mounted", ErrInvalid, path)
	}
	return nil
}

func openDirectory(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), path), nil
}
