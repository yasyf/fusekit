//go:build fuse && cgo && darwin

// Command holder is the dedicated, serve-only fuse mount-holder.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/holderfs"
	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/proc"
	"github.com/yasyf/fusekit/version"
)

// holderNice is the holder's nice value: polite under contention while never
// entering a starvation band. ~1/3 CPU weight when foreground work is busy.
const holderNice = 5

// killGroupEnv gates process-group isolation: setpgid at startup plus a
// group SIGKILL on abnormal exit, so spawned go-nfsv4 servers die with the
// holder instead of surviving as orphans.
// TODO(vm-gate): default ON only after scenarios/repro-holder-crash-orphan.sh
// proves 10 kill cycles with the KeepAlive respawn intact.
const killGroupEnv = "FUSEKIT_HOLDER_KILL_GROUP"

func main() {
	socket := flag.String("socket", "", "unix socket path to serve (default ~/.fusekit/holder.sock)")
	logPath := flag.String("log", "", "append serve logs to this file (optional; default stderr)")
	var reapRoots stringList
	flag.Var(&reapRoots, "reap-root", "consumer mount root: at startup, kill prior-generation go-nfsv4 orphans serving confirmed-carcass mountpoints under it (repeatable)")
	flag.Parse()

	// Soft nice only; the Darwin background band is contraindicated for this
	// data plane — see ccn doc 130274e.
	if err := proc.Nice(holderNice); err != nil {
		log.Fatalf("fusekit-holder: set nice: %v", err)
	}

	sock := *socket
	if sock == "" {
		sock = mountd.DefaultHolderSocket()
	}
	if err := os.MkdirAll(filepath.Dir(sock), 0o700); err != nil {
		log.Fatalf("fusekit-holder: create socket dir: %v", err)
	}

	var logger *log.Logger
	if *logPath != "" {
		// os.OpenFile is O_CLOEXEC, so this fd never leaks into spawned servers.
		f, err := os.OpenFile(*logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			log.Fatalf("fusekit-holder: open --log %s: %v", *logPath, err)
		}
		defer f.Close()
		logger = log.New(f, "fusekit-holder ", log.LstdFlags|log.Lmsgprefix)
		// Route our output (crash traces included) at the O_CLOEXEC fd, then
		// null stdio so spawned servers cannot inherit a handle on the log.
		log.SetOutput(f)
		if err := debug.SetCrashOutput(f, debug.CrashOptions{}); err != nil {
			log.Printf("fusekit-holder: set crash output: %v", err)
		}
		if err := proc.SuppressStdio(); err != nil {
			log.Fatalf("fusekit-holder: suppress stdio: %v", err)
		}
	}

	grouped := false
	if os.Getenv(killGroupEnv) == "1" {
		if err := syscall.Setpgid(0, 0); err != nil {
			log.Fatalf("fusekit-holder: setpgid: %v", err)
		}
		grouped = true
	}

	// Before any mount: a dead prior generation's orphans still hold the old
	// sockets and answer EPERM through every mount they backed.
	if len(reapRoots) > 0 {
		if pids := fusekit.ReapOrphanedServers(reapRoots); len(pids) > 0 {
			logf(logger, "reaped %d orphaned go-nfsv4 server(s) from a prior holder generation: %v", len(pids), pids)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := &mountd.Server{
		Socket:  sock,
		Host:    holderfs.Host(),
		Probe:   fusekit.HostProbe,
		Version: version.String(),
		Log:     logger,
	}
	if err := s.Run(ctx); err != nil {
		if grouped {
			logf(logger, "abnormal exit (%v); killing the holder process group", err)
			killGroup()
		}
		log.Fatalf("fusekit-holder: serve %s: %v", sock, err)
	}
}

// killGroup SIGKILLs the holder's own process group, self included; callers
// gate on grouped, so the group is never the spawning daemon's.
func killGroup() {
	pgid, err := syscall.Getpgid(0)
	if err != nil {
		return
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
}

// stringList is a repeatable string flag.
type stringList []string

func (l *stringList) String() string { return strings.Join(*l, ",") }

func (l *stringList) Set(v string) error {
	*l = append(*l, v)
	return nil
}

// logf writes to the --log logger when one exists, else the default stderr
// logger.
func logf(logger *log.Logger, format string, args ...any) {
	if logger != nil {
		logger.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}
