// Command demo-daemon backs docs/scripts/demo.sh: it mounts <root>/src at
// <root>/mnt through a detached holder spawned from <root>/fusekit-holder,
// then sleeps until killed — the holder keeps the mount alive. Run with
// -cleanup to reclaim the demo's mounts (the holder itself is stopped by the
// script with a plain SIGTERM; there is no wire shutdown in proto 2).
//
// Everything lives under the demo's own scratch root and socket; it never
// touches ~/.fusekit or a live holder.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/yasyf/fusekit/mountd"
)

func main() {
	root := flag.String("root", "", "demo scratch root (holds src/, mnt/, holder.sock, fusekit-holder)")
	cleanup := flag.Bool("cleanup", false, "reclaim the demo owner's mounts")
	flag.Parse()

	if *root == "" {
		fmt.Fprintln(os.Stderr, "daemon: --root is required")
		os.Exit(2)
	}
	socket := filepath.Join(*root, "holder.sock")
	const owner = "fusekit-demo"

	if *cleanup {
		cl := mountd.NewClient(socket)
		cl.Owner = owner
		if !cl.Available() {
			return // no holder, no mounts
		}
		failed, err := cl.Reclaim()
		if err != nil {
			fmt.Fprintf(os.Stderr, "reclaim: %v\n", err)
			os.Exit(1)
		}
		if len(failed) > 0 {
			fmt.Fprintf(os.Stderr, "reclaim left wedged dirs: %+v\n", failed)
			os.Exit(1)
		}
		fmt.Println("demo mounts reclaimed")
		return
	}

	host := &mountd.RemoteHost{
		Socket:   socket,
		LogPath:  filepath.Join(*root, "holder.log"),
		ExecPath: filepath.Join(*root, "fusekit-holder"),
		Args:     []string{"--socket", socket},
		Owner:    owner,
	}
	if err := host.Setup(filepath.Join(*root, "src"), filepath.Join(*root, "mnt")); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: setup: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("daemon[%d]: serving %s through the detached holder\n", os.Getpid(), filepath.Join(*root, "mnt"))
	select {}
}
