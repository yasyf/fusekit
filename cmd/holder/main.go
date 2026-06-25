//go:build fuse && cgo

// Command holder is the dedicated, serve-only fuse mount-holder: it binds the
// holder socket and runs mountd.Server.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/mountd"
	"github.com/yasyf/fusekit/version"
)

func main() {
	socket := flag.String("socket", "", "unix socket path to serve (required)")
	logPath := flag.String("log", "", "append serve logs to this file (optional; default stderr)")
	// content-socket is reserved for ContentSource-over-RPC (Phase 3).
	_ = flag.String("content-socket", "", "reserved for ContentSource-over-RPC (ignored)")
	flag.Parse()

	if *socket == "" {
		log.Fatal("fusekit-holder: --socket is required")
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
		Socket:  *socket,
		Host:    fusekit.PassthroughHost(),
		Probe:   fusekit.HostProbe,
		Version: version.String(),
		Log:     logger,
	}
	if err := s.Run(ctx); err != nil {
		log.Fatalf("fusekit-holder: serve %s: %v", *socket, err)
	}
}
