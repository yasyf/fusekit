//go:build darwin

package proc

import (
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// spawnNprocHeadroom is how many processes above the current per-UID count a
// freshly spawned child (and its descendants) may create before fork() starts
// returning EAGAIN. A holder forks ~0 children, so this headroom is generous for
// legitimate use while still starving an exponential spawn loop long before it
// exhausts the per-UID process table and freezes the machine.
const spawnNprocHeadroom = 400

// spawnRlimitMu serializes the lower-RLIMIT_NPROC / fork / restore window: the cap
// is applied to THIS process only long enough for the child to inherit it across
// fork, so a concurrent spawn must not fork while the parent's limit is lowered.
var spawnRlimitMu sync.Mutex

// withChildNprocCap runs spawn (which must perform the fork — exec.Cmd.Start) with
// RLIMIT_NPROC lowered to (current per-UID process count + headroom). The forked
// child inherits the lowered cap, and so does every descendant (the limit is per
// real-UID), so a runaway re-spawn loop in the child subtree hits EAGAIN within the
// headroom instead of fork-bombing the host. The parent's own limit is restored
// immediately after the fork. It only ever LOWERS the limit (never raises it), so a
// system already running tighter than the computed cap is left untouched.
func withChildNprocCap(spawn func() error) error {
	spawnRlimitMu.Lock()
	defer spawnRlimitMu.Unlock()

	procs, err := unix.SysctlKinfoProcSlice("kern.proc.uid", os.Getuid())
	if err != nil {
		return fmt.Errorf("count uid processes for spawn nproc cap: %w", err)
	}
	var orig unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NPROC, &orig); err != nil {
		return fmt.Errorf("read RLIMIT_NPROC: %w", err)
	}
	capped := orig
	if want := uint64(len(procs) + spawnNprocHeadroom); want < capped.Cur {
		capped.Cur = want
	}
	if err := unix.Setrlimit(unix.RLIMIT_NPROC, &capped); err != nil {
		return fmt.Errorf("lower RLIMIT_NPROC for spawn: %w", err)
	}
	defer func() { _ = unix.Setrlimit(unix.RLIMIT_NPROC, &orig) }()

	return spawn()
}
