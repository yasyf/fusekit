//go:build darwin

// Command p5util is the guest-side driver for the Phase-5 holder-v2 VM
// validation scenarios (scripts/vm/scenarios/p5-*.sh): a raw protocol client,
// a lease holder, a proc start-time reader matching cc-pool's lease-agent
// stamp, and bounded stat/read probes for wedge assertions. Scenario support
// only — never part of a release; like vmstress it refuses to run outside a
// VM (exit 86).
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yasyf/fusekit/lease"
	"github.com/yasyf/fusekit/mountd"
	"golang.org/x/sys/unix"
)

const exitNotVM = 86

func main() {
	requireVM()
	if len(os.Args) < 2 {
		die("usage: p5util <req|starttime|leasehold|stat|readfile> ...")
	}
	var err error
	switch os.Args[1] {
	case "req":
		err = cmdReq(os.Args[2:])
	case "starttime":
		err = cmdStartTime(os.Args[2:])
	case "leasehold":
		err = cmdLeaseHold(os.Args[2:])
	case "stat":
		err = cmdStat(os.Args[2:])
	case "readfile":
		err = cmdReadFile(os.Args[2:])
	default:
		die("unknown subcommand: " + os.Args[1])
	}
	if err != nil {
		die(err.Error())
	}
}

func requireVM() {
	v, err := unix.SysctlUint32("kern.hv_vmm_present")
	if err != nil || v != 1 {
		fmt.Fprintln(os.Stderr, "p5util: REFUSING TO RUN: not a VM (kern.hv_vmm_present != 1)")
		os.Exit(exitNotVM)
	}
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, "p5util: "+msg)
	os.Exit(1)
}

// cmdReq sends one newline-delimited JSON request to the holder socket and
// prints the one-line response. The request is passed verbatim — scenarios
// assert on the raw response text.
func cmdReq(args []string) error {
	fs := flag.NewFlagSet("req", flag.ExitOnError)
	socket := fs.String("socket", mountd.DefaultHolderSocket(), "holder unix socket")
	timeout := fs.Duration("timeout", 30*time.Second, "dial+response deadline")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: p5util req [--socket S] '<json>'")
	}
	conn, err := net.DialTimeout("unix", *socket, *timeout)
	if err != nil {
		return fmt.Errorf("dial %s: %w", *socket, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(*timeout))
	if _, err := fmt.Fprintln(conn, fs.Arg(0)); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	fmt.Print(line)
	return nil
}

// cmdStartTime prints pid's start-time stamp with cc-pool's encoding
// (start-second * 1e6 + microsecond, via KERN_PROC_PID) so scenarios can hand
// a live `ccp lease-agent` its --start flag.
func cmdStartTime(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: p5util starttime PID")
	}
	var pid int
	if _, err := fmt.Sscanf(args[0], "%d", &pid); err != nil {
		return fmt.Errorf("bad pid %q: %w", args[0], err)
	}
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		return fmt.Errorf("kern.proc.pid %d: %w", pid, err)
	}
	tv := kp.Proc.P_starttime
	fmt.Println(tv.Sec*1_000_000 + int64(tv.Usec))
	return nil
}

// cmdLeaseHold acquires a shared session lease on --dir and holds it until
// SIGTERM/SIGINT (or --seconds elapses), then releases and exits 0. It prints
// "held <lease-file>" once acquired so scenarios can gate on acquisition.
func cmdLeaseHold(args []string) error {
	fs := flag.NewFlagSet("leasehold", flag.ExitOnError)
	root := fs.String("root", "", "lease root (default: the fleet root)")
	dir := fs.String("dir", "", "dir whose lease key to hold")
	owner := fs.String("owner", "p5util", "advisory owner recorded in the header")
	seconds := fs.Int("seconds", 0, "auto-release after N seconds (0: until signaled)")
	_ = fs.Parse(args)
	if *dir == "" {
		return fmt.Errorf("leasehold: --dir is required")
	}
	r := *root
	if r == "" {
		var err error
		if r, err = lease.DefaultRoot(); err != nil {
			return err
		}
	}
	h, err := lease.Acquire(r, *dir, *owner)
	if err != nil {
		return fmt.Errorf("acquire %s: %w", *dir, err)
	}
	fmt.Printf("held %s\n", h.Path())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	if *seconds > 0 {
		select {
		case <-sig:
		case <-time.After(time.Duration(*seconds) * time.Second):
		}
	} else {
		<-sig
	}
	return h.Close()
}

// cmdStat prints "ok", "hung", or "errno:<E>" for a bounded stat of PATH —
// the wedge assertion primitive (macOS ships no timeout(1)).
func cmdStat(args []string) error {
	fs := flag.NewFlagSet("stat", flag.ExitOnError)
	timeout := fs.Duration("timeout", 3*time.Second, "stat deadline")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: p5util stat [--timeout T] PATH")
	}
	return boundedOp(*timeout, func() error {
		_, err := os.Stat(fs.Arg(0))
		return err
	})
}

// cmdReadFile is cmdStat for a full sequential read — catches the partial
// wedge where metadata answers but bulk reads hang.
func cmdReadFile(args []string) error {
	fs := flag.NewFlagSet("readfile", flag.ExitOnError)
	timeout := fs.Duration("timeout", 5*time.Second, "read deadline")
	_ = fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: p5util readfile [--timeout T] PATH")
	}
	return boundedOp(*timeout, func() error {
		_, err := os.ReadFile(fs.Arg(0))
		return err
	})
}

// boundedOp races op against the deadline: "ok"/"errno:%v" verdicts exit 0,
// a hang prints "hung" and exits 0 too — scenarios branch on the WORD, and
// only infrastructure failures exit nonzero. The hung goroutine is abandoned
// (the process exits immediately after).
func boundedOp(timeout time.Duration, op func() error) error {
	done := make(chan error, 1)
	go func() { done <- op() }()
	select {
	case err := <-done:
		if err == nil {
			fmt.Println("ok")
		} else {
			fmt.Printf("errno:%v\n", err)
		}
	case <-time.After(timeout):
		fmt.Println("hung")
	}
	return nil
}
