package content

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type selfTestSource struct {
	entries []Entry
	data    []byte
	readErr error
}

func (s selfTestSource) Manifest(string) ([]Entry, error) { return s.entries, nil }
func (s selfTestSource) ReadSynth(string, string) ([]byte, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	return s.data, nil
}
func (selfTestSource) WriteThrough(string, string, []byte) error { return nil }
func (selfTestSource) Classify(string) EntryKind                 { return EntrySynth }

func TestBridgeClientSelfTest(t *testing.T) {
	const (
		domain = "d1"
		name   = "health"
	)

	t.Run("healthy round trip", func(t *testing.T) {
		src := selfTestSource{
			entries: []Entry{{Name: name, Kind: EntrySynth}},
			data:    []byte("ok"),
		}
		client := NewBridgeClient(serveBridge(t, src))
		if err := client.SelfTest(context.Background(), domain, name); err != nil {
			t.Fatalf("SelfTest = %v, want nil", err)
		}
	})

	t.Run("entry missing from manifest", func(t *testing.T) {
		src := selfTestSource{entries: []Entry{{Name: "other", Kind: EntrySynth}}}
		client := NewBridgeClient(serveBridge(t, src))
		err := client.SelfTest(context.Background(), domain, name)
		if err == nil {
			t.Fatal("SelfTest = nil, want an error")
		}
		if errors.Is(err, ErrBridgeUnavailable) {
			t.Fatalf("err = %v; a served manifest miss is a content verdict, never ErrBridgeUnavailable", err)
		}
		if !strings.Contains(err.Error(), domain) || !strings.Contains(err.Error(), name) {
			t.Fatalf("err = %q, want domain %q and name %q", err, domain, name)
		}
	})

	t.Run("read failure", func(t *testing.T) {
		src := selfTestSource{
			entries: []Entry{{Name: name, Kind: EntrySynth}},
			readErr: errors.New("read failed"),
		}
		client := NewBridgeClient(serveBridge(t, src))
		err := client.SelfTest(context.Background(), domain, name)
		if err == nil {
			t.Fatal("SelfTest = nil, want an error")
		}
		if !strings.Contains(err.Error(), "selftest: read") || !strings.Contains(err.Error(), "read failed") {
			t.Fatalf("err = %q, want the selftest read wrap and the server's message", err)
		}
		if errors.Is(err, ErrBridgeDialRefused) {
			t.Fatalf("err = %v, do not want ErrBridgeDialRefused in the chain", err)
		}
	})

	t.Run("bound but unresponsive", func(t *testing.T) {
		path := filepath.Join(shortSockDir(t), "hang.sock")
		listener, err := net.Listen("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		t.Cleanup(func() {
			close(done)
			listener.Close()
		})
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			<-done
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		err = NewBridgeClient(path).SelfTest(ctx, domain, name)
		if err == nil {
			t.Fatal("SelfTest = nil, want an error")
		}
		if !errors.Is(err, ErrBridgeUnavailable) {
			t.Fatalf("err = %v, want ErrBridgeUnavailable in the chain", err)
		}
		if errors.Is(err, ErrBridgeDialRefused) {
			t.Fatalf("err = %v, do not want ErrBridgeDialRefused in the chain", err)
		}
	})

	t.Run("nothing listening", func(t *testing.T) {
		err := NewBridgeClient(deadSocket(t)).SelfTest(context.Background(), domain, name)
		if err == nil {
			t.Fatal("SelfTest = nil, want an error")
		}
		if !errors.Is(err, ErrBridgeDialRefused) {
			t.Fatalf("err = %v, want ErrBridgeDialRefused in the chain", err)
		}
	})
}
