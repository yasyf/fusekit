//go:build ignore

// Demo daemon for docs/scripts/demo.sh: mounts <root>/src at <root>/mnt
// through a detached holder spawned from <root>/fusekit-holder, then sleeps
// until killed — the holder keeps the mount alive. Run with -cleanup to
// gracefully retire the holder and sweep its mounts.
//
// Everything lives under the demo's own scratch root and socket; it never
// touches ~/.fusekit or a live holder.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yasyf/fusekit/mountd"
)

func main() {
	root := flag.String("root", "", "demo scratch root (holds src/, mnt/, holder.sock, fusekit-holder)")
	cleanup := flag.Bool("cleanup", false, "shut the holder down and sweep its mounts")
	flag.Parse()

	if *root == "" {
		fmt.Fprintln(os.Stderr, "daemon: --root is required")
		os.Exit(2)
	}
	socket := filepath.Join(*root, "holder.sock")

	if *cleanup {
		cl := mountd.NewClient(socket)
		if !cl.Available() {
			return // no holder to retire
		}
		failed, err := cl.Shutdown()
		if err != nil {
			fmt.Fprintf(os.Stderr, "shutdown: %v\n", err)
			os.Exit(1)
		}
		if len(failed) > 0 {
			fmt.Fprintf(os.Stderr, "shutdown left wedged dirs: %+v\n", failed)
			os.Exit(1)
		}
		if !cl.WaitGone(5 * time.Second) {
			fmt.Fprintln(os.Stderr, "holder socket still live after shutdown")
			os.Exit(1)
		}
		fmt.Println("holder retired, mounts swept")
		return
	}

	host := &mountd.RemoteHost{
		Socket:   socket,
		LogPath:  filepath.Join(*root, "holder.log"),
		ExecPath: filepath.Join(*root, "fusekit-holder"),
		Args:     []string{"--socket", socket},
	}
	if err := host.Setup(filepath.Join(*root, "src"), filepath.Join(*root, "mnt")); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: setup: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("daemon[%d]: serving %s through the detached holder\n", os.Getpid(), filepath.Join(*root, "mnt"))
	select {}
}
