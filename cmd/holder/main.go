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
	"syscall"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/version"
	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/holderfs"
	"github.com/yasyf/fusekit/lease"
	"github.com/yasyf/fusekit/mountd"
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

// buildVersion and buildCommit are the holder's build stamp, injected at release
// time via -ldflags "-X main.buildVersion=v1.2.3 -X main.buildCommit=abc1234". They
// are the holder's own, never fusekit's — the module version stays off every wire.
var (
	buildVersion = "dev"
	buildCommit  string
)

// holderVersion renders the holder's version line for the wire and RetireSkew:
// version.Running resolves a dev build to its stat-stamped sentinel, with the short
// commit appended when set.
func holderVersion() string {
	v := version.Running(buildVersion)
	if buildCommit != "" {
		v += " (" + buildCommit + ")"
	}
	return v
}

func main() {
	// FIRST: drop every inherited non-CLOEXEC descriptor. A lazily spawned
	// holder inherits the spawning session's lease fd and would pin that
	// lease for its whole (long) lifetime.
	if err := proc.CloseInheritedFDs(); err != nil {
		log.Fatalf("fusekit-holder: close inherited fds: %v", err)
	}

	socket := flag.String("socket", "", "unix socket path to serve (default ~/.fusekit/holder.sock)")
	logPath := flag.String("log", "", "append serve logs to this file (optional; default stderr)")
	installLA := flag.Bool("install-launchagent", false, "install the cask KeepAlive LaunchAgent and exit")
	uninstallLA := flag.Bool("uninstall-launchagent", false, "remove the cask KeepAlive LaunchAgent and exit")
	flag.Parse()

	if handled, err := launchAgentRun(context.Background(), *installLA, *uninstallLA, holderKeepAlive()); handled {
		if err != nil {
			log.Fatalf("fusekit-holder: %v", err)
		}
		return
	}

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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s, err := newServer(sock, logger)
	if err != nil {
		log.Fatalf("fusekit-holder: %v", err)
	}
	if err := s.Run(ctx); err != nil {
		if grouped {
			logf(logger, "abnormal exit (%v); killing the holder process group", err)
			killGroup()
		}
		log.Fatalf("fusekit-holder: serve %s: %v", sock, err)
	}
}

// newServer builds the cask holder's Server, Validate-clean by construction.
// The spec journal beside the socket makes this holder self-owning: Run
// replays it on start — clearing prior-generation carcasses and reaping
// their orphaned go-nfsv4 servers, which replaces the old --reap-root
// one-shot flag — before serving; RetireSkew makes it self-retiring on
// version skew against the installed bundle, lease-gated per mount.
func newServer(sock string, logger *log.Logger) (*mountd.Server, error) {
	leaseDir, err := lease.DefaultRoot()
	if err != nil {
		return nil, err
	}
	s := &mountd.Server{
		Socket:      sock,
		Host:        holderfs.Host(),
		Probe:       fusekit.HostProbe,
		Version:     holderVersion(),
		Log:         logger,
		JournalPath: mountd.DefaultJournalPath(sock),
		RetireSkew:  mountd.SkewCheck(buildVersion),
		LeaseDir:    leaseDir,
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
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

// logf writes to the --log logger when one exists, else the default stderr
// logger.
func logf(logger *log.Logger, format string, args ...any) {
	if logger != nil {
		logger.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}
