package content

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/fusekit/state"
)

// replayMinBackoff and replayMaxBackoff bound the async spool-replay retry when
// the upstream is unreachable. Vars, not consts, so tests can shrink the ladder
// off multi-second waits.
var (
	replayMinBackoff = 500 * time.Millisecond
	replayMaxBackoff = 30 * time.Second
)

// ErrClassifyUnavailable is the relay's verdict that it cannot classify a name
// offline: the upstream is unreachable AND no manifest has ever been cached (a
// cold cache), so there is nothing to answer from and guessing could leak a
// private entry. Transient (ClassTransient) — a content bridge coming back
// resolves it — never a content verdict, so a caller retries rather than
// converts. It is safe to reply to the sandboxed appex, which throws on any
// not-OK response.
var ErrClassifyUnavailable ClassedError = classifyUnavailableErr{}

type classifyUnavailableErr struct{}

func (classifyUnavailableErr) Error() string {
	return "relay: no cached classification available (upstream unreachable, cold cache)"
}
func (classifyUnavailableErr) Class() string { return ClassTransient }

// RelayConfig configures a RelaySource.
type RelayConfig struct {
	// Owner scopes the relay to one consumer; it names the on-disk spool dir.
	Owner string
	// SpoolDir is the directory holding this relay's durable write spool. Loaded
	// on construction so a successor holder (or an adopt) drains writes a crashed
	// generation never pushed.
	SpoolDir string
	// Upstream is the consumer daemon's bridge socket the relay dials.
	Upstream string
	// PrivatePrefixes are the top-level name prefixes an offline Classify routes
	// to the private store — the same set the fuse holder classifies with.
	PrivatePrefixes []string
}

// RelaySource is a caching, write-spooling content.Source that proxies a
// consumer daemon's content bridge over a BridgeClient. It lets the shared
// holder host the consumer's File-Provider-facing bridge across the daemon's
// restarts: Manifest and ReadSynth serve the last-good cache when the upstream
// is unreachable, Classify answers offline from cached manifests plus private
// prefixes, and WriteThrough always accepts — persisting to a disk spool and
// replaying it upstream asynchronously so a save never fails while the daemon
// is mid-restart. It implements content.Source (and Classifier) only; the
// consumer it fronts (cc-pool's PoolContentSource) is itself Source-only, so
// the Tree ops answer ClassUnsupported unchanged.
type RelaySource struct {
	owner    string
	spoolDir string

	// mu guards the upstream client, the private prefixes, and the read caches.
	// It is never held across a bridge round-trip (the client is read out under
	// the lock, the I/O runs lock-free, the cache is updated under the lock).
	mu        sync.RWMutex
	client    *BridgeClient
	prefixes  []string
	manifests map[string][]Entry // domain -> last-good manifest
	synth     map[string][]byte  // domain\x00name -> last-good synth bytes

	// spoolMu guards the in-memory spool index and its sequence counter; the
	// on-disk files are written/removed outside it (local, atomic temp+rename).
	spoolMu sync.Mutex
	seq     uint64
	spool   map[string]spoolEntry // spoolKey -> pending write

	replayCh chan struct{}
}

// spoolEntry is one pending write. seq orders latest-wins: a replay deletes an
// entry only if its seq is unchanged since the replay captured it, so a write
// that landed while the push was in flight is never dropped.
type spoolEntry struct {
	data []byte
	seq  uint64
}

// NewRelaySource builds a RelaySource for cfg and loads any spool left on disk
// by a prior holder generation (or a prior incarnation of this one), so pending
// writes survive a crash or a holder handoff.
func NewRelaySource(cfg RelayConfig) (*RelaySource, error) {
	if cfg.Owner == "" {
		return nil, errors.New("content: RelaySource requires an owner")
	}
	if cfg.SpoolDir == "" {
		return nil, errors.New("content: RelaySource requires a spool dir")
	}
	if cfg.Upstream == "" {
		return nil, errors.New("content: RelaySource requires an upstream socket")
	}
	r := &RelaySource{
		owner:     cfg.Owner,
		spoolDir:  cfg.SpoolDir,
		client:    NewBridgeClient(cfg.Upstream),
		prefixes:  append([]string(nil), cfg.PrivatePrefixes...),
		manifests: map[string][]Entry{},
		synth:     map[string][]byte{},
		spool:     map[string]spoolEntry{},
		replayCh:  make(chan struct{}, 1),
	}
	if err := r.loadSpool(); err != nil {
		return nil, fmt.Errorf("content: load spool %s: %w", cfg.SpoolDir, err)
	}
	return r, nil
}

// Adopt re-points the relay at a fresh upstream and prefix set in place, keeping
// the read caches and the write spool warm. It is the same-owner re-add path: a
// consumer daemon restart re-asserts its bridge and the relay keeps serving
// stale reads and pending writes across the gap.
func (r *RelaySource) Adopt(upstream string, prefixes []string) {
	r.mu.Lock()
	r.client = NewBridgeClient(upstream)
	r.prefixes = append([]string(nil), prefixes...)
	r.mu.Unlock()
	r.kickReplay()
}

func (r *RelaySource) upstreamClient() *BridgeClient {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.client
}

// Manifest proxies the upstream, caching the result; on an unreachable upstream
// it serves the last-good manifest for the domain, and a cold cache propagates
// the error.
func (r *RelaySource) Manifest(domain string) ([]Entry, error) {
	entries, err := r.upstreamClient().Manifest(context.Background(), domain)
	if err == nil {
		r.cacheManifest(domain, entries)
		return entries, nil
	}
	if errors.Is(err, ErrBridgeUnavailable) {
		if cached, ok := r.cachedManifest(domain); ok {
			return cached, nil
		}
	}
	return nil, err
}

// ReadSynth serves a pending spooled write for (domain, name) ahead of
// everything — read-your-writes even while the upstream is up but the replay has
// not landed — then proxies the upstream, caching the result; on an unreachable
// upstream it serves the last-good bytes, and a cold cache propagates the error.
func (r *RelaySource) ReadSynth(domain, name string) ([]byte, error) {
	if data, ok := r.spooled(domain, name); ok {
		return data, nil
	}
	data, err := r.upstreamClient().Read(context.Background(), domain, name)
	if err == nil {
		r.cacheSynth(domain, name, data)
		return data, nil
	}
	if errors.Is(err, ErrBridgeUnavailable) {
		if cached, ok := r.cachedSynth(domain, name); ok {
			return cached, nil
		}
	}
	return nil, err
}

// WriteThrough always accepts: it persists the write to the durable spool
// (latest-wins per (domain, name)), primes the read cache for read-your-writes,
// and returns nil, leaving the async replay loop to push it upstream with capped
// backoff. Only a spool-persistence failure (a full or unwritable disk) is
// returned; an unreachable upstream never fails the write. Known accepted
// window: a spooled write replayed after the upstream recovers can carry staler
// shareable keys than a fuse-side write that landed post-recovery — the replay
// drains eagerly on reconnect to keep that window at seconds, the daemon-side
// WriteThrough mutex still serializes the actual merges, and the pre-relay
// alternative was the write being lost outright.
func (r *RelaySource) WriteThrough(domain, name string, data []byte) error {
	if err := r.spoolWrite(domain, name, data); err != nil {
		return err
	}
	r.cacheSynth(domain, name, data)
	r.kickReplay()
	return nil
}

// Classify satisfies the Source contract; the BridgeServer prefers ClassifyErr,
// so this is only reached for a caller that never upgraded to Classifier. It
// swallows the offline-unavailable verdict into a passthrough default.
func (r *RelaySource) Classify(name string) EntryKind {
	kind, _ := r.ClassifyErr(name)
	return kind
}

// ClassifyErr proxies the upstream, and on an unreachable upstream answers
// offline — FAIL CLOSED — only from a positive signal: a cached-manifest entry's
// own kind, or a PrivatePrefixes match as EntryPrivate. Anything else returns
// ErrClassifyUnavailable, never a shared/passthrough verdict. A genuine
// content-verdict error from the upstream propagates unchanged; only an
// unreachable upstream falls back.
//
// The fail-closed default is a privacy invariant, not caution: cc-pool routes
// some names private by glob (e.g. *.lock) or case-variant family, and those
// PrivatePatterns never cross the wire, so an unknown name could be private.
// Serving it as shared would leak; refusing reproduces exactly the appex's
// behavior against a down bridge today. The .claude.json save path is unaffected
// — it is a cached synth entry, a positive manifest hit.
//
// Classify carries no domain (the Source contract has none, and the sandboxed
// appex sends none on the wire), and cc-pool's own classification is
// domain-agnostic, so the offline verdict consults the union of every cached
// domain's manifest — a top-level name's kind is invariant across domains.
func (r *RelaySource) ClassifyErr(name string) (EntryKind, error) {
	kind, err := r.upstreamClient().Classify(context.Background(), name)
	if err == nil {
		return kind, nil
	}
	if !errors.Is(err, ErrBridgeUnavailable) {
		return "", err
	}
	return r.classifyOffline(name)
}

func (r *RelaySource) classifyOffline(name string) (EntryKind, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	n := strings.TrimPrefix(name, "._")
	for _, entries := range r.manifests {
		for _, e := range entries {
			if e.Name == n {
				return e.Kind, nil
			}
		}
	}
	for _, p := range r.prefixes {
		if strings.HasPrefix(n, p) {
			return EntryPrivate, nil
		}
	}
	// Fail closed: no positive private/kind signal, so refuse rather than guess
	// shared — an unknown name may be one of cc-pool's wire-absent glob/case
	// private names, and serving it shared would leak.
	return "", ErrClassifyUnavailable
}

func (r *RelaySource) cacheManifest(domain string, entries []Entry) {
	r.mu.Lock()
	r.manifests[domain] = entries
	r.mu.Unlock()
}

func (r *RelaySource) cachedManifest(domain string) ([]Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.manifests[domain]
	return e, ok
}

func (r *RelaySource) cacheSynth(domain, name string, data []byte) {
	r.mu.Lock()
	r.synth[synthKey(domain, name)] = append([]byte(nil), data...)
	r.mu.Unlock()
}

func (r *RelaySource) cachedSynth(domain, name string) ([]byte, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	d, ok := r.synth[synthKey(domain, name)]
	return d, ok
}

// PendingWrites reports how many writes are spooled but not yet replayed —
// surfaced in the holder's bridge listing.
func (r *RelaySource) PendingWrites() int {
	r.spoolMu.Lock()
	defer r.spoolMu.Unlock()
	return len(r.spool)
}

// Replay drains the spool upstream until ctx is cancelled: it pushes every
// pending write, deletes each on success, and on an unreachable upstream backs
// off (capped) before retrying, waking immediately on a fresh write. It never
// pushes stale bytes — a write superseded since it was captured stays spooled.
func (r *RelaySource) Replay(ctx context.Context) {
	backoff := replayMinBackoff
	for {
		pending := r.snapshotSpool()
		if len(pending) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-r.replayCh:
				continue
			}
		}
		failed := false
		for key, e := range pending {
			if ctx.Err() != nil {
				return
			}
			if err := r.pushOne(ctx, key, e); err != nil {
				failed = true
				break
			}
		}
		if !failed {
			backoff = replayMinBackoff
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = minDuration(backoff*2, replayMaxBackoff)
	}
}

// Drain makes one bounded best-effort pass over the spool, leaving any write it
// could not push on disk. It is the RemoveBridge/Reclaim/Shutdown teardown path:
// the durable spool survives for a successor holder.
func (r *RelaySource) Drain(ctx context.Context) {
	for key, e := range r.snapshotSpool() {
		if ctx.Err() != nil {
			return
		}
		_ = r.pushOne(ctx, key, e)
	}
}

func (r *RelaySource) pushOne(ctx context.Context, key string, e spoolEntry) error {
	domain, name, ok := parseSpoolKey(key)
	if !ok {
		r.dropSpool(key, e.seq)
		return nil
	}
	if err := r.upstreamClient().Write(ctx, domain, name, e.data); err != nil {
		return err
	}
	r.dropSpool(key, e.seq)
	return nil
}

func (r *RelaySource) spoolWrite(domain, name string, data []byte) error {
	key := spoolKey(domain, name)
	if err := state.AtomicWrite(filepath.Join(r.spoolDir, key), data, 0o600); err != nil {
		return fmt.Errorf("spool %s/%s: %w", domain, name, err)
	}
	r.spoolMu.Lock()
	r.seq++
	r.spool[key] = spoolEntry{data: append([]byte(nil), data...), seq: r.seq}
	r.spoolMu.Unlock()
	return nil
}

func (r *RelaySource) spooled(domain, name string) ([]byte, bool) {
	r.spoolMu.Lock()
	defer r.spoolMu.Unlock()
	e, ok := r.spool[spoolKey(domain, name)]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), e.data...), true
}

func (r *RelaySource) dropSpool(key string, seq uint64) {
	r.spoolMu.Lock()
	defer r.spoolMu.Unlock()
	if cur, ok := r.spool[key]; ok && cur.seq == seq {
		delete(r.spool, key)
		_ = os.Remove(filepath.Join(r.spoolDir, key))
	}
}

func (r *RelaySource) snapshotSpool() map[string]spoolEntry {
	r.spoolMu.Lock()
	defer r.spoolMu.Unlock()
	out := make(map[string]spoolEntry, len(r.spool))
	for k, v := range r.spool {
		out[k] = v
	}
	return out
}

func (r *RelaySource) loadSpool() error {
	entries, err := os.ReadDir(r.spoolDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		key := de.Name()
		if _, _, ok := parseSpoolKey(key); !ok {
			continue // a temp file or hand-mangled name; never a valid entry
		}
		data, rerr := os.ReadFile(filepath.Join(r.spoolDir, key))
		if rerr != nil {
			return rerr
		}
		r.seq++
		r.spool[key] = spoolEntry{data: data, seq: r.seq}
	}
	return nil
}

func (r *RelaySource) kickReplay() {
	select {
	case r.replayCh <- struct{}{}:
	default:
	}
}

// spoolKey reversibly encodes (domain, name) into one filesystem-safe file name,
// so a re-write of the same entry overwrites in place (latest-wins) and a load
// recovers (domain, name) for replay. base64url over "domain\x00name" avoids
// path traversal and the ".."/"/" hazards a readable encoding would carry.
func spoolKey(domain, name string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(domain + "\x00" + name))
}

func parseSpoolKey(key string) (domain, name string, ok bool) {
	raw, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		return "", "", false
	}
	i := bytes.IndexByte(raw, 0)
	if i < 0 {
		return "", "", false
	}
	return string(raw[:i]), string(raw[i+1:]), true
}

func synthKey(domain, name string) string { return domain + "\x00" + name }

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
