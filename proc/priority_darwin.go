//go:build darwin

package proc

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// PRIO_DARWIN_PROCESS and PRIO_DARWIN_BG from <sys/resource.h>; x/sys/unix
// does not define Darwin's private setpriority extensions.
const (
	prioDarwinProcess = 4      // `which`: target is a whole process (who = pid, 0 = self)
	prioDarwinBG      = 0x1000 // `prio`: enter the "background" resource band; 0 revokes
)

// SetBackgroundPriority moves the calling process into Darwin's "background"
// resource band — setpriority(PRIO_DARWIN_PROCESS, 0, PRIO_DARWIN_BG), the
// process-wide syscall equivalent of QOS_CLASS_BACKGROUND: lowest CPU
// scheduling band, throttled disk I/O, and background-class network traffic,
// inherited by child processes (for a mount holder, the per-mount NFS servers
// fuse-t spawns). A detached child launched via `open -g` has no LaunchAgent
// plist, so launchd's Nice/ProcessType keys cannot apply; demoting in-process
// is the only lever to keep it from competing with foreground work.
func SetBackgroundPriority() error {
	if err := unix.Setpriority(prioDarwinProcess, 0, prioDarwinBG); err != nil {
		return fmt.Errorf("set darwin background priority: %w", err)
	}
	return nil
}
