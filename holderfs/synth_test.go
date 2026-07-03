package holderfs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yasyf/fusekit/content"
)

// fakeContent is a controllable content.Source served over a real bridge so the
// synthView exercises the genuine RPC path. readBlock, when non-nil, hangs every
// ReadSynth until it is closed — modeling a stuck consumer. onRead, when non-nil,
// runs inside every ReadSynth after the reply bytes are captured — modeling a
// writer mutating the freshness file while the consumer renders.
type fakeContent struct {
	mu        sync.Mutex
	readBytes []byte
	readBlock chan struct{}
	onRead    func()
	reads     int
	writes    [][]byte
}

func (f *fakeContent) Manifest(string) ([]content.Entry, error) { return nil, nil }

func (f *fakeContent) ReadSynth(string, string) ([]byte, error) {
	f.mu.Lock()
	f.reads++
	blk, b, hook := f.readBlock, f.readBytes, f.onRead
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
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

	// Pre-warm hangs on the blocked read; unblock once the consumer is hit.
	go v.refreshOnce()
	waitReads(t, fc, 1)
	close(block)
	waitCache(t, v, "v1")

	// Now hang every future read and mark the cache stale.
	hang := make(chan struct{})
	fc.mu.Lock()
	fc.readBlock = hang
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

	// Release the parked read so the bridge handler drains and the eventual
	// refresh lands v2 (and the test leaks no goroutine at cleanup).
	close(hang)
	waitCache(t, v, "v2")
}

// TestSynthViewRefreshDiscardsTornRead pins the TOCTOU guard in refreshOnce: a
// bridge read that straddles a freshness-file rewrite returns torn (here: EMPTY)
// bytes, and the moved post-read signature must discard them and re-read — the
// retry serves the writer's settled bytes, and the empty snapshot is never
// installed. Without the guard the empty read would be cached under the pre-read
// signature and served until the freshness file next changed.
func TestSynthViewRefreshDiscardsTornRead(t *testing.T) {
	fresh, touch := freshFile(t)
	// First read renders EMPTY (the truncate-window observation) and mutates the
	// freshness file mid-render; every later read renders the settled bytes.
	fc := &fakeContent{readBytes: []byte{}}
	var once sync.Once
	fc.mu.Lock()
	fc.onRead = func() {
		once.Do(func() {
			touch()
			fc.mu.Lock()
			fc.readBytes = []byte("settled")
			fc.mu.Unlock()
		})
	}
	fc.mu.Unlock()
	v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), "/dev/null", []string{fresh})

	v.refreshOnce()
	if got := fc.readCount(); got != 2 {
		t.Fatalf("consumer reads = %d, want 2 (torn read discarded, one retry)", got)
	}
	buf, ok := v.currentBytes()
	if !ok || string(buf) != "settled" {
		t.Fatalf("cache after torn-read refresh = %q, ok=%v; want the settled bytes, never the empty torn snapshot", buf, ok)
	}
	v.mu.Lock()
	rerr := v.readErr
	v.mu.Unlock()
	if rerr != nil {
		t.Fatalf("readErr = %v after a converged refresh, want nil", rerr)
	}
}

// TestSynthViewRefreshExhaustionKeepsLastGood pins the bounded-retry failure
// mode: when the freshness file moves under every read in a pass, refreshOnce
// must give up loudly after refreshRetries attempts, keep the last-good cache
// (never install unattributable bytes), and still converge on the next pass once
// the writer quiesces.
func TestSynthViewRefreshExhaustionKeepsLastGood(t *testing.T) {
	fresh, touch := freshFile(t)
	fc := &fakeContent{readBytes: []byte("v1")}
	v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), "/dev/null", []string{fresh})
	v.refreshOnce() // 1 read: installs v1 under a stable signature
	v.mu.Lock()
	goodSig := v.cacheSig
	v.mu.Unlock()

	// Every read now returns empty bytes AND moves the freshness file mid-render.
	fc.mu.Lock()
	fc.readBytes = []byte{}
	fc.onRead = touch
	fc.mu.Unlock()

	start := time.Now()
	v.refreshOnce()
	if elapsed := time.Since(start); elapsed < (refreshRetries-1)*refreshRetryDelay {
		t.Fatalf("exhausted refresh pass took %v, want >= %v — retries must back off, never hot-loop the bridge", elapsed, (refreshRetries-1)*refreshRetryDelay)
	}
	if got := fc.readCount(); got != 1+refreshRetries {
		t.Fatalf("consumer reads = %d, want %d (initial + %d bounded retries)", got, 1+refreshRetries, refreshRetries)
	}
	v.mu.Lock()
	buf, sig, ok, rerr := v.cacheBuf, v.cacheSig, v.cacheOK, v.readErr
	v.mu.Unlock()
	if !ok || string(buf) != "v1" || sig != goodSig {
		t.Fatalf("cache after exhausted refresh = %q (sig %q, ok=%v); want last-good v1 under its original signature", buf, sig, ok)
	}
	if rerr == nil || !strings.Contains(rerr.Error(), "freshness signature moved") {
		t.Fatalf("readErr = %v, want the loud freshness-instability error", rerr)
	}

	// Writer quiesces: the next pass converges on the settled bytes.
	fc.mu.Lock()
	fc.readBytes = []byte("v2")
	fc.onRead = nil
	fc.mu.Unlock()
	v.refreshOnce()
	v.mu.Lock()
	buf, rerr = v.cacheBuf, v.readErr
	v.mu.Unlock()
	if string(buf) != "v2" || rerr != nil {
		t.Fatalf("cache after quiesced refresh = %q (readErr %v), want v2 with the error cleared", buf, rerr)
	}
}

// TestSynthViewRefreshRejectsInWindowMtimeTie pins the clock-tie leg of the
// TOCTOU guard (mtimeInWindow): a writer whose truncate-then-rewrite completes
// inside the read window and lands back on the freshness file's prior
// (mtime, size) leaves the pre- and post-read signatures EQUAL — comparison
// alone cannot attribute the torn bytes. The guard must treat a pass whose
// window covers a freshness mtime as ambiguous and retry; only the retry,
// whose window starts past the stamp, may install. The tie is planted
// deterministically: the freshness file is stamped slightly in the future and
// the mid-render rewrite waits until the wall clock has passed the stamp
// before restoring it, so the first pass's [t0, t1] provably covers the stamp
// — the observable state a real same-quantum truncate-then-rewrite leaves.
func TestSynthViewRefreshRejectsInWindowMtimeTie(t *testing.T) {
	fresh := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(fresh, []byte("00"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The first render observes the truncate window (EMPTY bytes) while the
	// writer completes a same-(mtime, size) rewrite inside the read window:
	// wait out the stamp, replace the content same-size, restore the stamp.
	// stampCh hands the stamp to the bridge handler goroutine and orders the
	// plant before the read. Later renders return the settled bytes untouched.
	stampCh := make(chan time.Time, 1)
	fc := &fakeContent{readBytes: []byte{}}
	var once sync.Once
	fc.mu.Lock()
	fc.onRead = func() {
		once.Do(func() {
			stamp := <-stampCh
			for !time.Now().After(stamp) {
				time.Sleep(time.Millisecond)
			}
			if err := os.WriteFile(fresh, []byte("11"), 0o600); err != nil {
				t.Error(err)
				return
			}
			if err := os.Chtimes(fresh, stamp, stamp); err != nil {
				t.Error(err)
				return
			}
			fc.mu.Lock()
			fc.readBytes = []byte("settled")
			fc.mu.Unlock()
		})
	}
	fc.mu.Unlock()
	v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), "/dev/null", []string{fresh})

	stamp := time.Now().Add(50 * time.Millisecond)
	if err := os.Chtimes(fresh, stamp, stamp); err != nil {
		t.Fatal(err)
	}
	stampCh <- stamp
	v.refreshOnce()
	if got := fc.readCount(); got != 2 {
		t.Fatalf("consumer reads = %d, want 2 (in-window mtime tie discarded, one retry)", got)
	}
	buf, ok := v.currentBytes()
	if !ok || string(buf) != "settled" {
		t.Fatalf("cache after tied refresh = %q, ok=%v; want the settled bytes — a signature-preserving torn snapshot must never install", buf, ok)
	}
	v.mu.Lock()
	rerr := v.readErr
	v.mu.Unlock()
	if rerr != nil {
		t.Fatalf("readErr = %v after a converged refresh, want nil", rerr)
	}
}

// TestSynthViewRefreshFutureStampedFreshnessInstalls pins the guard's far
// side: a freshness file stamped in the FUTURE (clock skew, a restored
// backup) sits outside the pass's window — beyond t1 — and must install
// first-pass; treating it as perpetually ambiguous would wedge refresh at the
// last-good cache forever. It stays safe without the window check: an
// in-window rewrite of such a file restamps at the current clock, BELOW the
// future stamp, so signature comparison catches it.
func TestSynthViewRefreshFutureStampedFreshnessInstalls(t *testing.T) {
	fresh := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(fresh, []byte("0"), 0o600); err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(fresh, future, future); err != nil {
		t.Fatal(err)
	}
	fc := &fakeContent{readBytes: []byte("v1")}
	v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), "/dev/null", []string{fresh})

	v.refreshOnce()
	if got := fc.readCount(); got != 1 {
		t.Fatalf("consumer reads = %d, want 1 — a future stamp beyond the window is not ambiguity", got)
	}
	buf, ok := v.currentBytes()
	if !ok || string(buf) != "v1" {
		t.Fatalf("cache = %q, ok=%v; want v1 installed despite the future-stamped freshness file", buf, ok)
	}
	v.mu.Lock()
	rerr := v.readErr
	v.mu.Unlock()
	if rerr != nil {
		t.Fatalf("readErr = %v, want nil", rerr)
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
