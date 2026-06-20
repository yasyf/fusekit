//go:build darwin

package mountd

import (
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
)

// TestPeerPID pins the getsockopt(LOCAL_PEERPID) plumbing: against an
// in-process listener the peer is us, so the call must return our own pid.
// macOS caps sun_path at 104 bytes, so the socket lives under a short /tmp dir.
func TestPeerPID(t *testing.T) {
	sockDir, err := os.MkdirTemp("/tmp", "ccp-pp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	socket := filepath.Join(sockDir, "p.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return // listener closed: defined exit
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn) // hold the peer end open until the dialer hangs up
	}()

	pid, err := (&Client{Socket: socket}).PeerPID()
	if err != nil {
		t.Fatalf("PeerPID: %v", err)
	}
	if pid != os.Getpid() {
		t.Fatalf("PeerPID = %d, want our own pid %d (the listener is in-process)", pid, os.Getpid())
	}
}

// TestPeerPIDUnreachable pins that a missing socket reports ErrUnreachable
// rather than a bogus pid.
func TestPeerPIDUnreachable(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "ccp-pp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if _, err := (&Client{Socket: filepath.Join(dir, "nope.sock")}).PeerPID(); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("PeerPID on a missing socket: err = %v, want ErrUnreachable", err)
	}
}
