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
	"syscall"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/holderfs"
	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/proc"
	"github.com/yasyf/fusekit/version"
)

func main() {
	socket := flag.String("socket", "", "unix socket path to serve (default ~/.fusekit/holder.sock)")
	logPath := flag.String("log", "", "append serve logs to this file (optional; default stderr)")
	flag.Parse()

	if err := proc.SetBackgroundPriority(); err != nil {
		log.Fatalf("fusekit-holder: set background priority: %v", err)
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
		f, err := os.OpenFile(*logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			log.Fatalf("fusekit-holder: open --log %s: %v", *logPath, err)
		}
		defer f.Close()
		logger = log.New(f, "fusekit-holder ", log.LstdFlags|log.Lmsgprefix)
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
		log.Fatalf("fusekit-holder: serve %s: %v", sock, err)
	}
}
