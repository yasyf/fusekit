//go:build linux

package fusekit

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

// fusermountUnmount gracefully unmounts a FUSE dir as a non-root user via
// fusermount3 -u, then fusermount -u — NEVER -z, which is MNT_DETACH by
// another name. The helper runs with LC_ALL=C/LANG=C so its output is
// unlocalized: the busy match below keys on the English word. A busy refusal
// surfaces wrapping syscall.EBUSY so Handle.Unmount classifies it retryable;
// any other non-zero exit with the mountpoint still mounted is a wedge with
// the errno unknown — never clean.
func fusermountUnmount(dir string) error {
	var missing []error
	for _, tool := range []string{"fusermount3", "fusermount"} {
		cmd := exec.Command(tool, "-u", dir)
		cmd.Env = append(os.Environ(), "LC_ALL=C", "LANG=C")
		out, err := cmd.CombinedOutput()
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
		if Mounted(dir) {
			return fmt.Errorf("%s -u %s exited non-zero with the dir still mounted (errno unknown): %s: %w", tool, dir, msg, err)
		}
		return fmt.Errorf("%s -u %s: %s: %w", tool, dir, msg, err)
	}
	return fmt.Errorf("unmount %s: umount2 answered EPERM and no fusermount is available: %w", dir, errors.Join(missing...))
}
