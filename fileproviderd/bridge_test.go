package fileproviderd

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

// fakeSource is an in-memory ContentSource stand-in. WriteThrough records into
// the synth store so a round-trip reads its own write back; errOn forces a named
// method to fail.
type fakeSource struct {
	mu       sync.Mutex
	manifest map[string][]Entry
	synth    map[string][]byte // key "domain/name"
	classify map[string]EntryKind
	errOn    string
}

func newFakeSource() *fakeSource {
	return &fakeSource{
		manifest: map[string][]Entry{},
		synth:    map[string][]byte{},
		classify: map[string]EntryKind{},
	}
}

func (s *fakeSource) Manifest(domain string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.errOn == "manifest" {
		return nil, errors.New("manifest boom")
	}
	return s.manifest[domain], nil
}

func (s *fakeSource) ReadSynth(domain, name string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.errOn == "read" {
		return nil, errors.New("read boom")
	}
	data, ok := s.synth[domain+"/"+name]
	if !ok {
		return nil, errors.New("no such synth entry")
	}
	return data, nil
}

func (s *fakeSource) WriteThrough(domain, name string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.errOn == "write" {
		return errors.New("write boom")
	}
	s.synth[domain+"/"+name] = append([]byte(nil), data...)
	return nil
}

func (s *fakeSource) Classify(name string) EntryKind {
	s.mu.Lock()
	defer s.mu.Unlock()
	if k, ok := s.classify[name]; ok {
		return k
	}
	return EntrySymlink
}

// startBridge runs a BridgeServer over src on a short socket and returns its
// path once it is accepting. Run's ctx is cancelled and waited on cleanup.
func startBridge(t *testing.T, src ContentSource) string {
	t.Helper()
	socket := filepath.Join(shortSockDir(t), "bridge.sock")
	srv := &BridgeServer{Socket: socket, Source: src, Version: "v-test"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("BridgeServer.Run did not exit within 2s of ctx cancel")
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

func TestBridgeRoundTrip(t *testing.T) {
	src := newFakeSource()
	src.manifest["acct-01"] = []Entry{
		{Name: "projects", Kind: EntrySymlink, Version: "v1"},
		{Name: ".config.json", Kind: EntrySynth, Version: "merge:abc", Size: 12},
		{Name: "identity", Kind: EntryPrivate, Version: "v2"},
	}
	src.synth["acct-01/.config.json"] = []byte(`{"merged":true}`)
	src.classify["newfile"] = EntrySynth
	socket := startBridge(t, src)
	cl := NewBridgeClient(socket)
	ctx := context.Background()

	t.Run("manifest", func(t *testing.T) {
		got, err := cl.Manifest(ctx, "acct-01")
		if err != nil {
			t.Fatalf("Manifest = %v", err)
		}
		if !reflect.DeepEqual(got, src.manifest["acct-01"]) {
			t.Fatalf("Manifest = %+v, want %+v", got, src.manifest["acct-01"])
		}
	})

	t.Run("read synth", func(t *testing.T) {
		got, err := cl.Read(ctx, "acct-01", ".config.json")
		if err != nil {
			t.Fatalf("Read = %v", err)
		}
		if string(got) != `{"merged":true}` {
			t.Fatalf("Read = %s, want the synth bytes", got)
		}
	})

	t.Run("write through is read back", func(t *testing.T) {
		payload := []byte(`{"key":"val"}`)
		if err := cl.Write(ctx, "acct-01", ".config.json", payload); err != nil {
			t.Fatalf("Write = %v", err)
		}
		got, err := cl.Read(ctx, "acct-01", ".config.json")
		if err != nil || string(got) != string(payload) {
			t.Fatalf("read-after-write = %s, %v; want %s", got, err, payload)
		}
	})

	t.Run("classify", func(t *testing.T) {
		k, err := cl.Classify(ctx, "newfile")
		if err != nil || k != EntrySynth {
			t.Fatalf("Classify(newfile) = %q, %v; want synth", k, err)
		}
		k, err = cl.Classify(ctx, "unknown")
		if err != nil || k != EntrySymlink {
			t.Fatalf("Classify(unknown) = %q, %v; want the default symlink", k, err)
		}
	})
}

// TestBridgeWritePreservesBinary pins that arbitrary bytes survive the wire
// (Go's []byte JSON encoding is base64, so a non-UTF8 payload must round-trip).
func TestBridgeWritePreservesBinary(t *testing.T) {
	src := newFakeSource()
	socket := startBridge(t, src)
	cl := NewBridgeClient(socket)
	payload := []byte{0x00, 0xff, 0x10, 0x80, '\n', '{'}
	if err := cl.Write(context.Background(), "acct-01", "blob", payload); err != nil {
		t.Fatalf("Write = %v", err)
	}
	got, err := cl.Read(context.Background(), "acct-01", "blob")
	if err != nil || !reflect.DeepEqual(got, payload) {
		t.Fatalf("binary round-trip = %v, %v; want %v", got, err, payload)
	}
}

// TestBridgeSourceErrorsPropagate pins that a ContentSource failure surfaces as
// a plain error on the client (the bridge wire carries no classes).
func TestBridgeSourceErrorsPropagate(t *testing.T) {
	tests := []struct {
		name   string
		errOn  string
		invoke func(cl *BridgeClient) error
		want   string
	}{
		{
			name:   "manifest error",
			errOn:  "manifest",
			invoke: func(cl *BridgeClient) error { _, err := cl.Manifest(context.Background(), "d"); return err },
			want:   "manifest boom",
		},
		{
			name:   "read error",
			errOn:  "read",
			invoke: func(cl *BridgeClient) error { _, err := cl.Read(context.Background(), "d", "n"); return err },
			want:   "read boom",
		},
		{
			name:   "write error",
			errOn:  "write",
			invoke: func(cl *BridgeClient) error { return cl.Write(context.Background(), "d", "n", nil) },
			want:   "write boom",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			src := newFakeSource()
			src.errOn = tc.errOn
			socket := startBridge(t, src)
			err := tc.invoke(NewBridgeClient(socket))
			if err == nil || !contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestBridgeUnavailable pins that a dead bridge socket maps to ErrBridgeUnavailable.
func TestBridgeUnavailable(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "absent.sock")
	_, err := NewBridgeClient(socket).Manifest(context.Background(), "d")
	if !errors.Is(err, ErrBridgeUnavailable) {
		t.Fatalf("err = %v, want errors.Is ErrBridgeUnavailable", err)
	}
}

// TestBridgeRunRequiresSource pins the fail-loud refusal: a BridgeServer with no
// ContentSource cannot serve.
func TestBridgeRunRequiresSource(t *testing.T) {
	srv := &BridgeServer{Socket: filepath.Join(shortSockDir(t), "b.sock")}
	if err := srv.Run(context.Background()); err == nil || !contains(err.Error(), "ContentSource") {
		t.Fatalf("Run with no source = %v, want a fail-loud ContentSource error", err)
	}
}

// TestBridgeWireFreezeProtoVersion pins the bridge proto version is frozen at 1.
func TestBridgeWireFreezeProtoVersion(t *testing.T) {
	if BridgeProtoVersion != 1 {
		t.Fatalf("BridgeProtoVersion = %d; proto-1 is frozen", BridgeProtoVersion)
	}
}

// TestBridgeWireFreezeOps pins the frozen bridge op strings.
func TestBridgeWireFreezeOps(t *testing.T) {
	want := map[string]BridgeOp{
		"manifest": BridgeOpManifest,
		"read":     BridgeOpRead,
		"write":    BridgeOpWrite,
		"classify": BridgeOpClassify,
	}
	for frozen, op := range want {
		if string(op) != frozen {
			t.Errorf("bridge op drifted: %q, frozen value is %q", op, frozen)
		}
	}
}

// TestBridgeWireFreezeEntryKinds pins the frozen EntryKind strings.
func TestBridgeWireFreezeEntryKinds(t *testing.T) {
	want := map[string]EntryKind{
		"symlink": EntrySymlink,
		"synth":   EntrySynth,
		"private": EntryPrivate,
	}
	for frozen, k := range want {
		if string(k) != frozen {
			t.Errorf("entry kind drifted: %q, frozen value is %q", k, frozen)
		}
	}
}

// TestBridgeWireFreezeManifestBytes pins the exact bytes a manifest request and
// reply put on the wire, independent of the typed server.
func TestBridgeWireFreezeManifestBytes(t *testing.T) {
	req := BridgeRequest{Proto: 1, Op: BridgeOpManifest, Domain: "acct-01"}
	got, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"proto":1,"op":"manifest","domain":"acct-01"}`
	if string(got) != want {
		t.Fatalf("request = %s\nwant     %s", got, want)
	}
	entry := Entry{Name: "projects", Kind: EntrySymlink, Version: "v1", Size: 42}
	got, err = json.Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	if w := `{"name":"projects","kind":"symlink","version":"v1","size":42}`; string(got) != w {
		t.Fatalf("entry = %s\nwant  %s", got, w)
	}
}

// TestBridgeServerRebindsStaleSocket pins that the server rebinds a stale socket
// file left by a crashed prior daemon (no live peer).
func TestBridgeServerRebindsStaleSocket(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "bridge.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ln.Close() // socket file lingers, no peer

	src := newFakeSource()
	srv := &BridgeServer{Socket: socket, Source: src, Version: "v"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	cl := NewBridgeClient(socket)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !cl.Available() {
		time.Sleep(20 * time.Millisecond)
	}
	if !cl.Available() {
		t.Fatal("server did not rebind the stale socket")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("Run did not exit after cancel")
	}
}
