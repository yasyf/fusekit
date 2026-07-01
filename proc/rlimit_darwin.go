//go:build darwin

package proc

import (
	"fmt"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

// spawnNprocHeadroom is how far above the current per-UID process count the
// spawned subtree may fork before EAGAIN: generous for a holder forking ~0
// children, yet starves a runaway spawn loop before it exhausts the process
// table and freezes the machine.
const spawnNprocHeadroom = 400

// spawnRlimitMu serializes the lower/fork/restore window: no concurrent spawn
// may fork while THIS process's RLIMIT_NPROC is lowered.
var spawnRlimitMu sync.Mutex

// withChildNprocCap runs spawn (which must perform the fork — exec.Cmd.Start)
// with RLIMIT_NPROC temporarily lowered so the child inherits it. The limit is
// per real UID: every descendant inherits it and a runaway re-spawn loop hits
// EAGAIN instead of fork-bombing the host. It only ever LOWERS the limit.
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
