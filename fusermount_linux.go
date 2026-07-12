//go:build linux

package fusekit

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"
)

// fusermountUnmount gracefully unmounts a FUSE dir as a non-root user via
// fusermount3 -u, then fusermount -u — NEVER -z, which is MNT_DETACH by
// another name. A busy refusal surfaces wrapping syscall.EBUSY so
// Handle.Unmount classifies it retryable.
func fusermountUnmount(dir string) error {
	var missing []error
	for _, tool := range []string{"fusermount3", "fusermount"} {
		out, err := exec.Command(tool, "-u", dir).CombinedOutput()
		if err == nil {
			return nil
		}
		if errors.Is(err, exec.ErrNotFound) {
			missing = append(missing, err)
			continue
		}
		msg := strings.TrimSpace(string(out))
		if strings.Contains(strings.ToLower(msg), "busy") {
			return fmt.Errorf("%s -u %s: %s: %w", tool, dir, msg, syscall.EBUSY)
		}
		return fmt.Errorf("%s -u %s: %s: %w", tool, dir, msg, err)
	}
	return fmt.Errorf("unmount %s: umount2 answered EPERM and no fusermount is available: %w", dir, errors.Join(missing...))
}
