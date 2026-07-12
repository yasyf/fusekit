package content

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"syscall"
	"testing"
)

// TestBridgeClientDialClassification pins the dial-refusal classification the
// holder's replay deferral rides on: only ECONNREFUSED and ENOENT on the
// socket path read ErrBridgeDialRefused; every other bridge failure stays
// plain ErrBridgeUnavailable.
func TestBridgeClientDialClassification(t *testing.T) {
	tests := []struct {
		name        string
		socket      func(t *testing.T) string
		wantRefused bool
	}{
		{
			name:        "absent socket path (ENOENT) is dial-refused",
			socket:      func(t *testing.T) string { return deadSocket(t) },
			wantRefused: true,
		},
		{
			name: "bound but unlistened socket (ECONNREFUSED) is dial-refused",
			socket: func(t *testing.T) string {
				path := filepath.Join(shortSockDir(t), "bound.sock")
				fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { syscall.Close(fd) })
				if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
					t.Fatal(err)
				}
				return path
			},
			wantRefused: true,
		},
		{
			name: "connection dropped mid-op is unavailable, never dial-refused",
			socket: func(t *testing.T) string {
				path := filepath.Join(shortSockDir(t), "drop.sock")
				ln, err := net.Listen("unix", path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { ln.Close() })
				go func() {
					for {
						conn, err := ln.Accept()
						if err != nil {
							return
						}
						conn.Close()
					}
				}()
				return path
			},
			wantRefused: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewBridgeClient(tc.socket(t)).Manifest(context.Background(), "d1")
			if err == nil {
				t.Fatal("Manifest = nil, want an error")
			}
			if !errors.Is(err, ErrBridgeUnavailable) {
				t.Fatalf("err = %v, want ErrBridgeUnavailable in the chain", err)
			}
			if got := errors.Is(err, ErrBridgeDialRefused); got != tc.wantRefused {
				t.Fatalf("errors.Is(err, ErrBridgeDialRefused) = %v, want %v (err: %v)", got, tc.wantRefused, err)
			}
		})
	}
}
