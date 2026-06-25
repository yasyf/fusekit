package holderfs

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit/content"
)

// fakeContent is a controllable content.Source served over a real bridge so the
// synthView exercises the genuine RPC path. readBlock, when non-nil, hangs every
// ReadSynth until it is closed — modeling a stuck consumer.
type fakeContent struct {
	mu        sync.Mutex
	readBytes []byte
	readBlock chan struct{}
	reads     int
	writes    [][]byte
}

func (f *fakeContent) Manifest(string) ([]content.Entry, error) { return nil, nil }

func (f *fakeContent) ReadSynth(string, string) ([]byte, error) {
	f.mu.Lock()
	f.reads++
	blk, b := f.readBlock, f.readBytes
	f.mu.Unlock()
	if blk != nil {
		<-blk
	}
	return b, nil
}

func (f *fakeContent) WriteThrough(_, _ string, data []byte) error {
	f.mu.Lock()
	f.writes = append(f.writes, append([]byte(nil), data...))
	f.mu.Unlock()
	return nil
}

func (f *fakeContent) Classify(string) content.EntryKind { return content.EntrySynth }

func (f *fakeContent) readCount() int  { f.mu.Lock(); defer f.mu.Unlock(); return f.reads }
func (f *fakeContent) wrote() [][]byte { f.mu.Lock(); defer f.mu.Unlock(); return f.writes }

func serveContent(t *testing.T, src content.Source) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "hf")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "b.sock")
	srv := &content.BridgeServer{Socket: socket, Source: src, Version: "v"}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("bridge did not exit")
		}
	})
	cl := content.NewBridgeClient(socket)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cl.Available() {
			return socket
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("bridge never came up")
	return ""
}

// freshFile creates a freshness file and returns its path plus a touch func that
// changes its mtime/size so the synthView sees it as stale.
func freshFile(t *testing.T) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "f")
	if err := os.WriteFile(p, []byte("0"), 0o600); err != nil {
		t.Fatal(err)
	}
	n := 0
	return p, func() {
		n++
		if err := os.WriteFile(p, []byte(growStr(n)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func growStr(n int) string {
	b := make([]byte, n+1)
	for i := range b {
		b[i] = 'x'
	}
	return string(b)
}

func TestSynthViewPrewarmAndServeCached(t *testing.T) {
	fc := &fakeContent{readBytes: []byte("v1")}
	fresh, _ := freshFile(t)
	v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), "/dev/null", []string{fresh})

	v.refreshOnce() // pre-warm (off the fuse path, as Build does)
	buf, ok := v.currentBytes()
	if !ok || string(buf) != "v1" {
		t.Fatalf("currentBytes after pre-warm = %q, ok=%v; want v1", buf, ok)
	}
	// No freshness change ⇒ no extra RPC.
	if _, _ = v.currentBytes(); fc.readCount() != 1 {
		t.Fatalf("steady-state read count = %d, want 1 (freshness gates the RPC)", fc.readCount())
	}
}

func TestSynthViewEmptyFreshnessRefreshes(t *testing.T) {
	fc := &fakeContent{readBytes: []byte("v1")}
	// No freshness files ⇒ the holder has no local change signal, so every access
	// must reschedule a refresh (never freeze the cache at the warm-up snapshot).
	v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), "/dev/null", nil)
	v.refreshOnce()
	if buf, ok := v.currentBytes(); !ok || string(buf) != "v1" {
		t.Fatalf("after pre-warm = %q, ok=%v; want v1", buf, ok)
	}
	fc.mu.Lock()
	fc.readBytes = []byte("v2")
	fc.mu.Unlock()
	waitCache(t, v, "v2") // would hang/fail if the empty-freshness cache were frozen
}

func TestSynthViewOffHandlerStaleButServed(t *testing.T) {
	block := make(chan struct{})
	fc := &fakeContent{readBytes: []byte("v1"), readBlock: block}
	fresh, touch := freshFile(t)
	v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), "/dev/null", []string{fresh})

	// Pre-warm in the background (it will hang on the blocked read); wait until
	// the consumer has been hit, then unblock just the pre-warm.
	go v.refreshOnce()
	waitReads(t, fc, 1)
	close(block)
	waitCache(t, v, "v1")

	// Now hang every future read and mark the cache stale.
	fc.mu.Lock()
	fc.readBlock = make(chan struct{})
	fc.readBytes = []byte("v2")
	fc.mu.Unlock()
	touch()

	// currentBytes MUST return immediately with the stale value while the
	// background refresh is parked on the hung consumer.
	doneCh := make(chan []byte, 1)
	go func() { b, _ := v.currentBytes(); doneCh <- b }()
	select {
	case b := <-doneCh:
		if string(b) != "v1" {
			t.Fatalf("stale read = %q, want last-good v1", b)
		}
	case <-time.After(time.Second):
		t.Fatal("currentBytes blocked on the hung consumer — read is NOT off-handler")
	}
}

func TestSynthViewWriteThroughAndDrain(t *testing.T) {
	fc := &fakeContent{readBytes: []byte("v1")}
	writePath := filepath.Join(t.TempDir(), "backing")
	if err := os.WriteFile(writePath, []byte("COMMITTED"), 0o600); err != nil {
		t.Fatal(err)
	}
	v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), writePath, nil)

	v.markDirty(42)
	if !v.takeDirty(42) {
		t.Fatal("takeDirty(42) = false after markDirty")
	}
	if v.takeDirty(42) {
		t.Fatal("takeDirty(42) = true on the second call; the flag must clear")
	}
	v.scheduleWriteThrough()
	if !v.flushWithin(2 * time.Second) {
		t.Fatal("flushWithin did not drain the write-through")
	}
	w := fc.wrote()
	if len(w) != 1 || string(w[0]) != "COMMITTED" {
		t.Fatalf("write-through payloads = %q, want one COMMITTED (re-read from the durable file)", w)
	}
}

func waitReads(t *testing.T, fc *fakeContent, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fc.readCount() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("consumer saw %d reads, want >= %d", fc.readCount(), n)
}

func waitCache(t *testing.T, v *synthView, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if buf, ok := v.currentBytes(); ok && string(buf) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("cache never reached %q", want)
}
