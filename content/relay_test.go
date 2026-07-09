package content

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer is a concurrency-safe log sink: the Replay goroutine writes while
// the test reads.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// relayFake is a controllable Source behind an upstream BridgeServer. WriteThrough
// reflects into reads so a post-replay proxied read observes the pushed bytes.
type relayFake struct {
	mu        sync.Mutex
	manifests map[string][]Entry
	reads     map[string][]byte
	writes    map[string][]byte
	classes   map[string]EntryKind
	manErr    error
}

func newRelayFake() *relayFake {
	return &relayFake{
		manifests: map[string][]Entry{},
		reads:     map[string][]byte{},
		writes:    map[string][]byte{},
		classes:   map[string]EntryKind{},
	}
}

func (f *relayFake) Manifest(domain string) ([]Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.manErr != nil {
		return nil, f.manErr
	}
	return f.manifests[domain], nil
}

func (f *relayFake) ReadSynth(domain, name string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.reads[domain+"\x00"+name]
	if !ok {
		return nil, errors.New("read: no such entry")
	}
	return append([]byte(nil), d...), nil
}

func (f *relayFake) WriteThrough(domain, name string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := domain + "\x00" + name
	f.writes[key] = append([]byte(nil), data...)
	f.reads[key] = append([]byte(nil), data...)
	return nil
}

func (f *relayFake) Classify(name string) EntryKind {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.classes[name]
}

func (f *relayFake) setRead(domain, name, data string) {
	f.mu.Lock()
	f.reads[domain+"\x00"+name] = []byte(data)
	f.mu.Unlock()
}

func (f *relayFake) written(domain, name string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.writes[domain+"\x00"+name]
	return string(d), ok
}

func (f *relayFake) readValue(domain, name string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return string(f.reads[domain+"\x00"+name])
}

// deadSocket returns a socket path with no listener, so a dial fails with
// ErrBridgeUnavailable — the "upstream down" condition.
func deadSocket(t *testing.T) string {
	t.Helper()
	return filepath.Join(shortSockDir(t), "dead.sock")
}

func newRelayAt(t *testing.T, upstream, spoolDir string, prefixes []string) *RelaySource {
	t.Helper()
	r, err := NewRelaySource(RelayConfig{Owner: "o", SpoolDir: spoolDir, Upstream: upstream, PrivatePrefixes: prefixes})
	if err != nil {
		t.Fatalf("NewRelaySource: %v", err)
	}
	return r
}

func newRelay(t *testing.T, upstream string, prefixes []string) *RelaySource {
	t.Helper()
	return newRelayAt(t, upstream, filepath.Join(shortSockDir(t), "spool"), prefixes)
}

func TestRelayManifestProxyThenCacheFallback(t *testing.T) {
	fake := newRelayFake()
	fake.manifests["d1"] = []Entry{{Name: ".claude.json", Kind: EntrySynth, Version: "v1"}}
	relay := newRelay(t, serveBridge(t, fake), nil)

	got, err := relay.Manifest("d1")
	if err != nil {
		t.Fatalf("proxy Manifest: %v", err)
	}
	if !reflect.DeepEqual(got, fake.manifests["d1"]) {
		t.Fatalf("proxy Manifest = %+v, want %+v", got, fake.manifests["d1"])
	}

	relay.Adopt(deadSocket(t), nil) // upstream now unreachable
	cached, err := relay.Manifest("d1")
	if err != nil {
		t.Fatalf("cache-fallback Manifest: %v", err)
	}
	if !reflect.DeepEqual(cached, got) {
		t.Fatalf("cache-fallback Manifest = %+v, want last-good %+v", cached, got)
	}

	if _, err := relay.Manifest("d2"); !errors.Is(err, ErrBridgeUnavailable) {
		t.Fatalf("cold-cache Manifest err = %v, want ErrBridgeUnavailable", err)
	}
}

func TestRelayManifestContentErrorDoesNotFallBack(t *testing.T) {
	fake := newRelayFake()
	fake.manErr = errors.New("manifest boom")
	relay := newRelay(t, serveBridge(t, fake), nil)

	_, err := relay.Manifest("d1")
	if err == nil {
		t.Fatal("content-error Manifest = nil, want the upstream error")
	}
	if errors.Is(err, ErrBridgeUnavailable) {
		t.Fatalf("content error misread as ErrBridgeUnavailable: %v", err)
	}
}

func TestRelayReadSynthCacheAndReadYourWrites(t *testing.T) {
	fake := newRelayFake()
	fake.setRead("d1", "a", "v1")
	relay := newRelay(t, serveBridge(t, fake), nil)

	if got, err := relay.ReadSynth("d1", "a"); err != nil || string(got) != "v1" {
		t.Fatalf("proxy ReadSynth = %q, %v; want v1", got, err)
	}

	if err := relay.WriteThrough("d1", "a", []byte("v2")); err != nil {
		t.Fatalf("WriteThrough: %v", err)
	}
	// Spool overlay wins even while the upstream is up but the replay has not landed.
	if got, err := relay.ReadSynth("d1", "a"); err != nil || string(got) != "v2" {
		t.Fatalf("read-your-writes ReadSynth = %q, %v; want v2", got, err)
	}
	if v := fake.readValue("d1", "a"); v != "v1" {
		t.Fatalf("upstream saw the write prematurely: %q, want still v1", v)
	}

	relay.Drain(context.Background()) // push the spooled write upstream
	if v, ok := fake.written("d1", "a"); !ok || v != "v2" {
		t.Fatalf("after drain upstream write = (%q, %v), want v2", v, ok)
	}
	if got, err := relay.ReadSynth("d1", "a"); err != nil || string(got) != "v2" {
		t.Fatalf("post-drain proxy ReadSynth = %q, %v; want v2", got, err)
	}

	relay.Adopt(deadSocket(t), nil)
	if got, err := relay.ReadSynth("d1", "a"); err != nil || string(got) != "v2" {
		t.Fatalf("cache-fallback ReadSynth = %q, %v; want last-good v2", got, err)
	}
	if _, err := relay.ReadSynth("d1", "missing"); !errors.Is(err, ErrBridgeUnavailable) {
		t.Fatalf("cold-cache ReadSynth err = %v, want ErrBridgeUnavailable", err)
	}
}

func TestRelayClassify(t *testing.T) {
	fake := newRelayFake()
	fake.classes["online"] = EntrySymlink
	fake.manifests["d1"] = []Entry{
		{Name: ".claude.json", Kind: EntrySynth},
		{Name: "CLAUDE.md", Kind: EntrySymlink},
		{Name: ".credentials", Kind: EntryPrivate},
	}
	up := serveBridge(t, fake)
	relay := newRelay(t, up, []string{"secret"})

	t.Run("proxied", func(t *testing.T) {
		if k, err := relay.ClassifyErr("online"); err != nil || k != EntrySymlink {
			t.Fatalf("proxied ClassifyErr = %q, %v; want symlink", k, err)
		}
	})

	// Warm the manifest cache, then go offline for the fallback cases.
	if _, err := relay.Manifest("d1"); err != nil {
		t.Fatalf("warm Manifest: %v", err)
	}
	relay.Adopt(deadSocket(t), []string{"secret"})

	// Positive signals: an exact manifest hit returns that entry's kind; a
	// PrivatePrefixes match returns EntryPrivate.
	positive := []struct {
		name string
		in   string
		want EntryKind
	}{
		{"offline exact synth", ".claude.json", EntrySynth},
		{"offline exact symlink", "CLAUDE.md", EntrySymlink},
		{"offline exact private", ".credentials", EntryPrivate},
		{"offline appledouble trim", "._CLAUDE.md", EntrySymlink},
		{"offline prefix private", "secret-key", EntryPrivate},
	}
	for _, tc := range positive {
		t.Run(tc.name, func(t *testing.T) {
			got, err := relay.ClassifyErr(tc.in)
			if err != nil {
				t.Fatalf("ClassifyErr(%q) = err %v", tc.in, err)
			}
			if got != tc.want {
				t.Fatalf("ClassifyErr(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}

	// Fail closed: an unknown name — including cc-pool's wire-absent glob/case
	// private names — must REFUSE, never resolve to shared/passthrough.
	for _, in := range []string{"random-litter", "foo.lock", "SESSION.LOCK", ".Credentials.json", "Backups"} {
		t.Run("offline refuse "+in, func(t *testing.T) {
			got, err := relay.ClassifyErr(in)
			if !errors.Is(err, ErrClassifyUnavailable) {
				t.Fatalf("ClassifyErr(%q) = (%q, %v), want ErrClassifyUnavailable (fail closed)", in, got, err)
			}
		})
	}
}

func TestRelayClassifyColdCacheUnavailable(t *testing.T) {
	relay := newRelay(t, deadSocket(t), []string{"secret"}) // never warmed, upstream down
	if _, err := relay.ClassifyErr("anything"); !errors.Is(err, ErrClassifyUnavailable) {
		t.Fatalf("cold-cache ClassifyErr err = %v, want ErrClassifyUnavailable", err)
	}
}

func TestRelaySpoolLatestWinsAndReplay(t *testing.T) {
	fake := newRelayFake()
	relay := newRelay(t, serveBridge(t, fake), nil)

	if err := relay.WriteThrough("d", "a", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := relay.WriteThrough("d", "a", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if n := relay.PendingWrites(); n != 1 {
		t.Fatalf("PendingWrites = %d, want 1 (latest-wins coalesces (d,a))", n)
	}

	relay.Drain(context.Background())
	if n := relay.PendingWrites(); n != 0 {
		t.Fatalf("PendingWrites after drain = %d, want 0", n)
	}
	if v, ok := fake.written("d", "a"); !ok || v != "v2" {
		t.Fatalf("replayed write = (%q, %v), want latest v2", v, ok)
	}
}

func TestRelayWriteAcceptsWhenUpstreamDown(t *testing.T) {
	relay := newRelay(t, deadSocket(t), nil)
	if err := relay.WriteThrough("d", "a", []byte("v1")); err != nil {
		t.Fatalf("WriteThrough with a down upstream = %v, want accept (nil)", err)
	}
	if n := relay.PendingWrites(); n != 1 {
		t.Fatalf("PendingWrites = %d, want 1 (spooled)", n)
	}
}

func TestRelaySpoolSurvivesRestart(t *testing.T) {
	spoolDir := filepath.Join(shortSockDir(t), "spool")

	down := newRelayAt(t, deadSocket(t), spoolDir, nil)
	if err := down.WriteThrough("d", "a", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := down.WriteThrough("d", "b", []byte("w1")); err != nil {
		t.Fatal(err)
	}

	// A successor over the same spool dir recovers the pending writes and drains
	// them once its upstream is reachable.
	fake := newRelayFake()
	up := newRelayAt(t, serveBridge(t, fake), spoolDir, nil)
	if n := up.PendingWrites(); n != 2 {
		t.Fatalf("successor PendingWrites = %d, want 2 (recovered from disk)", n)
	}
	up.Drain(context.Background())
	if v, ok := fake.written("d", "a"); !ok || v != "v1" {
		t.Fatalf("recovered write (d,a) = (%q,%v), want v1", v, ok)
	}
	if v, ok := fake.written("d", "b"); !ok || v != "w1" {
		t.Fatalf("recovered write (d,b) = (%q,%v), want w1", v, ok)
	}
	if n := up.PendingWrites(); n != 0 {
		t.Fatalf("PendingWrites after successor drain = %d, want 0", n)
	}
}

func TestRelayReplayDrainsOnReconnect(t *testing.T) {
	restore := replayMinBackoff
	restoreMax := replayMaxBackoff
	replayMinBackoff, replayMaxBackoff = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { replayMinBackoff, replayMaxBackoff = restore, restoreMax })

	fake := newRelayFake()
	relay := newRelay(t, deadSocket(t), nil)

	if err := relay.WriteThrough("d", "a", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go relay.Replay(ctx)

	relay.Adopt(serveBridge(t, fake), nil) // upstream recovers

	deadline := time.Now().Add(2 * time.Second)
	for relay.PendingWrites() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("replay did not drain after reconnect; PendingWrites = %d", relay.PendingWrites())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if v, ok := fake.written("d", "a"); !ok || v != "v1" {
		t.Fatalf("replayed write = (%q,%v), want v1", v, ok)
	}
}

func TestRelaySpoolWriteDropInterleaveKeepsLatest(t *testing.T) {
	fake := newRelayFake()
	spoolDir := filepath.Join(shortSockDir(t), "spool")
	relay := newRelayAt(t, deadSocket(t), spoolDir, nil)

	if err := relay.WriteThrough("d", "a", []byte("v1")); err != nil { // first write → seq 1
		t.Fatal(err)
	}
	// When v2's file is durable but not yet published, simulate an in-flight
	// replay of v1 (seq 1) calling dropSpool. With seq-qualified files this must
	// unlink v1's file, never v2's, so v2 stays durable.
	var once sync.Once
	prev := afterSpoolFileWrite
	afterSpoolFileWrite = func() {
		once.Do(func() { relay.dropSpool(spoolKey("d", "a"), 1) })
	}
	t.Cleanup(func() { afterSpoolFileWrite = prev })

	if err := relay.WriteThrough("d", "a", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	afterSpoolFileWrite = prev

	if got, ok := relay.spooled("d", "a"); !ok || string(got) != "v2" {
		t.Fatalf("after drop-interleave, spooled = (%q, %v), want v2", got, ok)
	}
	if n := relay.PendingWrites(); n != 1 {
		t.Fatalf("PendingWrites = %d, want 1", n)
	}
	// v2 must be durable on disk: a successor over the same spool recovers it.
	up := newRelayAt(t, serveBridge(t, fake), spoolDir, nil)
	if n := up.PendingWrites(); n != 1 {
		t.Fatalf("successor PendingWrites = %d, want 1 (v2 lost from disk?)", n)
	}
	up.Drain(context.Background())
	if v, ok := fake.written("d", "a"); !ok || v != "v2" {
		t.Fatalf("recovered write = (%q, %v), want v2", v, ok)
	}
}

func TestRelaySpoolEntryCap(t *testing.T) {
	prevE, prevB := spoolMaxEntries, spoolMaxBytes
	spoolMaxEntries, spoolMaxBytes = 3, 1<<20
	t.Cleanup(func() { spoolMaxEntries, spoolMaxBytes = prevE, prevB })

	relay := newRelay(t, deadSocket(t), nil)
	for i := 0; i < 3; i++ {
		if err := relay.WriteThrough("d", fmt.Sprintf("n%d", i), []byte("x")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	if err := relay.WriteThrough("d", "n3", []byte("x")); !errors.Is(err, ErrSpoolFull) {
		t.Fatalf("over-entry-cap write = %v, want ErrSpoolFull", err)
	}
	// A latest-wins overwrite of an existing key is NOT a new entry.
	if err := relay.WriteThrough("d", "n0", []byte("yy")); err != nil {
		t.Fatalf("overwrite existing key at cap = %v, want accepted", err)
	}
	if n := relay.PendingWrites(); n != 3 {
		t.Fatalf("PendingWrites = %d, want 3", n)
	}
}

func TestRelaySpoolByteCap(t *testing.T) {
	prevE, prevB := spoolMaxEntries, spoolMaxBytes
	spoolMaxEntries, spoolMaxBytes = 100, 10
	t.Cleanup(func() { spoolMaxEntries, spoolMaxBytes = prevE, prevB })

	relay := newRelay(t, deadSocket(t), nil)
	if err := relay.WriteThrough("d", "a", []byte("12345")); err != nil { // 5 bytes
		t.Fatal(err)
	}
	if err := relay.WriteThrough("d", "b", []byte("67890")); err != nil { // 10 total
		t.Fatal(err)
	}
	if err := relay.WriteThrough("d", "c", []byte("1")); !errors.Is(err, ErrSpoolFull) { // 11 > 10
		t.Fatalf("over-byte-cap write = %v, want ErrSpoolFull", err)
	}
	// Shrinking an existing key frees bytes for a new write.
	if err := relay.WriteThrough("d", "a", []byte("1")); err != nil { // a: 5->1, total 6
		t.Fatalf("shrink overwrite = %v, want accepted", err)
	}
	if err := relay.WriteThrough("d", "c", []byte("1234")); err != nil { // total 10
		t.Fatalf("write after freeing bytes = %v, want accepted", err)
	}
}

func TestRelayRejectsNulAndEmptyTarget(t *testing.T) {
	relay := newRelay(t, deadSocket(t), nil)
	for _, tc := range []struct{ d, n string }{
		{"d\x00x", "a"},
		{"d", "a\x00b"},
		{"", "a"},
		{"d", ""},
	} {
		if err := relay.WriteThrough(tc.d, tc.n, []byte("v")); !errors.Is(err, ErrInvalidSpoolKey) {
			t.Errorf("WriteThrough(%q,%q) = %v, want ErrInvalidSpoolKey", tc.d, tc.n, err)
		}
	}
}

func TestRelayReplayLogsStallAndRecoveryOncePerTransition(t *testing.T) {
	restore, restoreMax := replayMinBackoff, replayMaxBackoff
	replayMinBackoff, replayMaxBackoff = 5*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { replayMinBackoff, replayMaxBackoff = restore, restoreMax })

	var buf syncBuffer
	fake := newRelayFake()
	r, err := NewRelaySource(RelayConfig{
		Owner: "o", SpoolDir: filepath.Join(shortSockDir(t), "spool"),
		Upstream: deadSocket(t), Log: log.New(&buf, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.WriteThrough("d", "a", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	replayDone := make(chan struct{})
	go func() { r.Replay(ctx); close(replayDone) }()

	waitContains(t, &buf, "stalled")   // upstream down: one stall log
	r.Adopt(serveBridge(t, fake), nil) // upstream recovers
	waitContains(t, &buf, "recovered") // one recovery log

	// The stall must be logged once per transition, never per retry attempt.
	if got := strings.Count(buf.String(), "stalled"); got != 1 {
		t.Errorf("stalled logged %d times, want exactly 1 (per-attempt flood?)", got)
	}
	if got := strings.Count(buf.String(), "recovered"); got != 1 {
		t.Errorf("recovered logged %d times, want exactly 1", got)
	}

	// Join the Replay goroutine before returning so the var-restore Cleanup can
	// never race its read of the backoff vars.
	cancel()
	<-replayDone
}

func waitContains(t *testing.T, b *syncBuffer, sub string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(b.String(), sub) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("log never contained %q; got:\n%s", sub, b.String())
}

func TestRelaySpoolKeyRoundTrips(t *testing.T) {
	cases := []struct{ domain, name string }{
		{"acct-01", ".claude.json"},
		{"weird/domain", "name with spaces"},
		{"", ""},
		{"d", "a/b/c"},
	}
	for _, tc := range cases {
		key := spoolKey(tc.domain, tc.name)
		d, n, ok := parseSpoolKey(key)
		if !ok || d != tc.domain || n != tc.name {
			t.Fatalf("parseSpoolKey(spoolKey(%q,%q)) = (%q,%q,%v), want round-trip", tc.domain, tc.name, d, n, ok)
		}
	}
	if _, _, ok := parseSpoolKey("not-a-valid-key.tmp.1234"); ok {
		t.Error("parseSpoolKey accepted a temp/mangled name; load would ingest it")
	}
}
