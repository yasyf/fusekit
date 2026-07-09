package content

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

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

	cases := []struct {
		name string
		in   string
		want EntryKind
	}{
		{"offline exact synth", ".claude.json", EntrySynth},
		{"offline exact symlink", "CLAUDE.md", EntrySymlink},
		{"offline exact private", ".credentials", EntryPrivate},
		{"offline appledouble trim", "._CLAUDE.md", EntrySymlink},
		{"offline prefix private", "secret-key", EntryPrivate},
		{"offline passthrough", "random-litter", EntryKind("")},
	}
	for _, tc := range cases {
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
