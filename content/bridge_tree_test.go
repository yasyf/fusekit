package content

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func shortSockDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ct")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// serveBridge runs a BridgeServer over src and returns its socket once it accepts.
func serveBridge(t *testing.T, src Source) string {
	t.Helper()
	socket := filepath.Join(shortSockDir(t), "b.sock")
	srv := &BridgeServer{Socket: socket, Source: src, Version: "v-test"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("BridgeServer.Run did not exit within 2s")
		}
	})
	cl := NewBridgeClient(socket)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cl.Available() {
			return socket
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("bridge socket never came up")
	return ""
}

// classErr is a ClassedError test double.
type classErr struct {
	msg, class string
}

func (e classErr) Error() string { return e.msg }
func (e classErr) Class() string { return e.class }

// fakeTree is an in-memory content.Tree. statErr forces Stat to fail with a
// classed error, exercising ErrClass propagation.
type fakeTree struct {
	statErr error
}

func (fakeTree) Manifest(string) ([]Entry, error)          { return nil, nil }
func (fakeTree) ReadSynth(string, string) ([]byte, error)  { return nil, nil }
func (fakeTree) WriteThrough(string, string, []byte) error { return nil }
func (fakeTree) Classify(string) EntryKind                 { return EntrySynth }
func (t fakeTree) Stat(domain, name string) (Entry, error) {
	if t.statErr != nil {
		return Entry{}, t.statErr
	}
	return Entry{Name: name, Kind: EntrySynth, Version: "v9", Size: 7}, nil
}
func (fakeTree) List(domain, name string) ([]Entry, error) {
	return []Entry{{Name: "a", Kind: EntrySynth}, {Name: "b", Kind: EntrySymlink}}, nil
}
func (fakeTree) ReadAt(domain, name string, ofst int64, size int) ([]byte, error) {
	data := []byte("0123456789")
	if ofst >= int64(len(data)) {
		return nil, nil
	}
	end := ofst + int64(size)
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	return data[ofst:end], nil
}
func (fakeTree) Readlink(domain, name string) (string, error) { return "/abs/" + name, nil }

func TestBridgeTreeRoundTrip(t *testing.T) {
	cl := NewBridgeClient(serveBridge(t, fakeTree{}))
	ctx := context.Background()

	t.Run("stat", func(t *testing.T) {
		got, err := cl.Stat(ctx, "d", "x")
		if err != nil {
			t.Fatalf("Stat = %v", err)
		}
		want := Entry{Name: "x", Kind: EntrySynth, Version: "v9", Size: 7}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Stat = %+v, want %+v", got, want)
		}
	})
	t.Run("list", func(t *testing.T) {
		got, err := cl.List(ctx, "d", "/")
		if err != nil || len(got) != 2 || got[0].Name != "a" || got[1].Kind != EntrySymlink {
			t.Fatalf("List = %+v, %v", got, err)
		}
	})
	t.Run("readat", func(t *testing.T) {
		got, err := cl.ReadAt(ctx, "d", "x", 3, 4)
		if err != nil || string(got) != "3456" {
			t.Fatalf("ReadAt(3,4) = %q, %v; want \"3456\"", got, err)
		}
	})
	t.Run("readlink", func(t *testing.T) {
		got, err := cl.Readlink(ctx, "d", "lnk")
		if err != nil || got != "/abs/lnk" {
			t.Fatalf("Readlink = %q, %v", got, err)
		}
	})
}

// fakeSourceOnly implements Source but NOT Tree, so the server answers Tree ops
// with an unknown-op error.
type fakeSourceOnly struct{}

func (fakeSourceOnly) Manifest(string) ([]Entry, error)          { return nil, nil }
func (fakeSourceOnly) ReadSynth(string, string) ([]byte, error)  { return nil, nil }
func (fakeSourceOnly) WriteThrough(string, string, []byte) error { return nil }
func (fakeSourceOnly) Classify(string) EntryKind                 { return EntrySymlink }

func TestBridgeTreeOpsUnknownOnSourceOnly(t *testing.T) {
	cl := NewBridgeClient(serveBridge(t, fakeSourceOnly{}))
	if _, err := cl.Stat(context.Background(), "d", "x"); err == nil || !contains(err.Error(), "unknown op") {
		t.Fatalf("Stat on a Source-only consumer = %v, want an unknown-op error", err)
	}
}

func TestBridgeErrClassPropagates(t *testing.T) {
	cl := NewBridgeClient(serveBridge(t, fakeTree{statErr: classErr{msg: "bad name", class: ClassInvalid}}))
	_, err := cl.Stat(context.Background(), "d", "x")
	if err == nil {
		t.Fatal("Stat = nil, want a classed error")
	}
	var ce ClassedError
	if !errors.As(err, &ce) {
		t.Fatalf("err %v is not a ClassedError", err)
	}
	if ce.Class() != ClassInvalid {
		t.Fatalf("class = %q, want %q", ce.Class(), ClassInvalid)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
