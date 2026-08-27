package holder

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

// Ported from daemonkit v0.20 proc (reaper_darwin.go, boot_darwin.go): the
// kern.proc identity read that captures and revalidates {PID, StartTime, Boot}.

const darwinZombieState = 5

var (
	errNoProcess    = errors.New("FuseKit runtime: no such process")
	errProcessAlive = errors.New("FuseKit runtime: recorded process is still alive")
)

type processProbe struct {
	startTime string
	comm      string
	zombie    bool
}

func probeProcess(pid int) (processProbe, error) {
	procs, err := unix.SysctlKinfoProcSlice("kern.proc.pid", pid)
	if err != nil {
		return processProbe{}, fmt.Errorf("sysctl kern.proc.pid %d: %w", pid, err)
	}
	if len(procs) == 0 {
		return processProbe{}, errNoProcess
	}
	kp := procs[0]
	st := kp.Proc.P_starttime
	comm := string(kp.Proc.P_comm[:])
	if index := strings.IndexByte(comm, 0); index >= 0 {
		comm = comm[:index]
	}
	return processProbe{
		startTime: fmt.Sprintf("%d.%06d", st.Sec, st.Usec),
		comm:      comm,
		zombie:    kp.Proc.P_stat == darwinZombieState,
	}, nil
}

// bootSession reads kern.bootsessionuuid rather than kern.boottime, which
// darwin slews as the clock is adjusted: one boot renders several distinct
// sec.usec values, and truncating to whole seconds only narrows the window
// because a slew still crosses a second boundary. The boot session UUID is
// minted once per boot and never moves.
func bootSession() (string, error) {
	session, err := unix.Sysctl("kern.bootsessionuuid")
	if err != nil {
		return "", fmt.Errorf("sysctl kern.bootsessionuuid: %w", err)
	}
	if session == "" {
		return "", errors.New("FuseKit runtime: boot session is empty")
	}
	return session, nil
}

func captureProcessRecord(
	pid int,
	executable string,
	id recoveryid.ID,
	generation catalog.ProcessGeneration,
	processGroup bool,
) (catalog.ProcessRecord, error) {
	probe, err := probeProcess(pid)
	if err != nil {
		return catalog.ProcessRecord{}, err
	}
	boot, err := bootSession()
	if err != nil {
		return catalog.ProcessRecord{}, err
	}
	record := catalog.ProcessRecord{
		RecoveryID: id, PID: pid, StartTime: probe.startTime, Boot: boot,
		Comm: probe.comm, Executable: executable,
		Generation: generation, ProcessGroup: processGroup,
	}
	if processGroup {
		record.SessionID = pid
	}
	if err := record.Validate(); err != nil {
		return catalog.ProcessRecord{}, fmt.Errorf("FuseKit runtime: validate captured process record: %w", err)
	}
	return record, nil
}

func captureCurrentProcessRecord(
	id recoveryid.ID,
	generation catalog.ProcessGeneration,
) (catalog.ProcessRecord, error) {
	executable, err := os.Executable()
	if err != nil {
		return catalog.ProcessRecord{}, fmt.Errorf("FuseKit runtime: resolve current executable: %w", err)
	}
	return captureProcessRecord(os.Getpid(), executable, id, generation, false)
}

// classifyRecordedProcess reads the process table before the boot session, so
// a boot identity can never retire a process that is demonstrably alive: the
// exact recorded {PID, StartTime} answering from the live table outranks it,
// and the boot session only separates a cross-boot retirement from a reused
// PID once the record is already known not to be live.
func classifyRecordedProcess(record catalog.ProcessRecord) (catalog.ReapOutcome, error) {
	probe, err := probeProcess(record.PID)
	if errors.Is(err, errNoProcess) {
		return catalog.ReapAbsent, nil
	}
	if err != nil {
		return 0, err
	}
	if probe.startTime != record.StartTime {
		boot, bootErr := bootSession()
		if bootErr != nil {
			return 0, bootErr
		}
		if record.Boot != boot {
			return catalog.ReapCrossBoot, nil
		}
		return catalog.ReapIdentityReused, nil
	}
	if probe.zombie {
		return catalog.ReapAbsent, nil
	}
	return 0, fmt.Errorf("%w: pid %d", errProcessAlive, record.PID)
}
