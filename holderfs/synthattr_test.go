package holderfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/fusekit/content"
)

// deadClient returns a bridge client whose socket does not exist, so any
// scheduled refresh fails fast — modeling an unreachable consumer.
func deadClient(t *testing.T) *content.BridgeClient {
	t.Helper()
	return content.NewBridgeClient(filepath.Join(t.TempDir(), "no.sock"))
}

// TestSynthViewSeedFromWritePath pins W2's cold→warm flap fix: writePath IS
// the durable last-committed content, so it warms the cache at Build — but
// only a cold one, and never so thoroughly that the consumer's answer stops
// mattering.
func TestSynthViewSeedFromWritePath(t *testing.T) {
	t.Run("writePath bytes warm a cold cache", func(t *testing.T) {
		writePath := filepath.Join(t.TempDir(), "backing")
		if err := os.WriteFile(writePath, []byte("COMMITTED"), 0o600); err != nil {
			t.Fatal(err)
		}
		v := newSynthView(".x", "d", deadClient(t), writePath, nil)
		v.seedFromWritePath()
		buf, ok := v.currentBytes()
		if !ok || string(buf) != "COMMITTED" {
			t.Fatalf("currentBytes after seed = %q, ok=%v; want COMMITTED with a warm cache", buf, ok)
		}
	})

	t.Run("missing writePath stays cold", func(t *testing.T) {
		v := newSynthView(".x", "d", deadClient(t), filepath.Join(t.TempDir(), "absent"), nil)
		v.seedFromWritePath()
		if buf, ok := v.currentBytes(); ok {
			t.Fatalf("currentBytes = %q with a warm cache; a missing writePath must stay cold (ENOENT/unlisted)", buf)
		}
	})

	t.Run("seed never overwrites consumer bytes", func(t *testing.T) {
		fc := &fakeContent{readBytes: []byte("MERGED")}
		writePath := filepath.Join(t.TempDir(), "backing")
		if err := os.WriteFile(writePath, []byte("COMMITTED"), 0o600); err != nil {
			t.Fatal(err)
		}
		v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), writePath, nil)
		v.refreshOnce()
		v.seedFromWritePath()
		if buf, ok := v.currentBytes(); !ok || string(buf) != "MERGED" {
			t.Fatalf("currentBytes = %q, ok=%v; the consumer's answer must win over the seed", buf, ok)
		}
	})

	t.Run("seed leaves the signature stale so a refresh still lands", func(t *testing.T) {
		fc := &fakeContent{readBytes: []byte("MERGED-LONGER")}
		dir := t.TempDir()
		writePath := filepath.Join(dir, "backing")
		fresh := filepath.Join(dir, "fresh")
		for _, p := range []string{writePath, fresh} {
			if err := os.WriteFile(p, []byte("COMMITTED"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		v := newSynthView(".x", "d", content.NewBridgeClient(serveContent(t, fc)), writePath, []string{fresh})
		v.seedFromWritePath()
		if buf, ok := v.currentBytes(); !ok || string(buf) != "COMMITTED" {
			t.Fatalf("first access = %q, ok=%v; want the seeded bytes served immediately", buf, ok)
		}
		waitCache(t, v, "MERGED-LONGER") // would time out if seeding froze the cache fresh
	})
}

// TestSynthViewServedMtimeMonotonic pins W2's monotonic-mtime rule: the served
// mtime is the max of writePath's and the freshness files' mtimes, floored at
// the highest value ever served — a vanished or backdated freshness file must
// never rewind it, since a rewind reads as a change and re-triggers page
// invalidation on open files.
func TestSynthViewServedMtimeMonotonic(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1, t2, t3 := t0.Add(time.Hour), t0.Add(2*time.Hour), t0.Add(3*time.Hour)

	dir := t.TempDir()
	writePath := filepath.Join(dir, "backing")
	fresh := filepath.Join(dir, "fresh")
	writeAt := func(p string, at time.Time) {
		t.Helper()
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, at, at); err != nil {
			t.Fatal(err)
		}
	}
	writeAt(writePath, t1)
	v := newSynthView(".x", "d", deadClient(t), writePath, []string{fresh})

	if got := v.servedMtime(); !got.Equal(t1) {
		t.Fatalf("servedMtime with only writePath = %v, want writePath's %v", got, t1)
	}
	writeAt(fresh, t2)
	if got := v.servedMtime(); !got.Equal(t2) {
		t.Fatalf("servedMtime with a newer freshness file = %v, want %v", got, t2)
	}
	if err := os.Remove(fresh); err != nil {
		t.Fatal(err)
	}
	if got := v.servedMtime(); !got.Equal(t2) {
		t.Fatalf("servedMtime after the freshness file vanished = %v, want the %v high-water mark (mtime must never regress)", got, t2)
	}
	writeAt(fresh, t3)
	if got := v.servedMtime(); !got.Equal(t3) {
		t.Fatalf("servedMtime after the freshness file advanced = %v, want %v", got, t3)
	}
	writeAt(fresh, t0) // backdated below every value served so far
	if got := v.servedMtime(); !got.Equal(t3) {
		t.Fatalf("servedMtime after backdating the freshness file = %v, want the %v high-water mark", got, t3)
	}
}

// TestSynthViewPinNeverRetreats pins the open-handle attr pin's forward-only
// rule: a newer open's snapshot replaces the pin, and the pin holds that
// snapshot even when the newer handle closes before an elder one — retreating
// to the elder's older snapshot would serve a size/mtime regression under a
// still-open file. Only the last close clears the pin.
func TestSynthViewPinNeverRetreats(t *testing.T) {
	tA := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	tB := tA.Add(time.Hour)
	v := newSynthView(".x", "d", deadClient(t), filepath.Join(t.TempDir(), "absent"), nil)

	if _, _, ok := v.pinnedAttrs(); ok {
		t.Fatal("pinnedAttrs = ok with no open handles")
	}
	v.pinOpen(2, tA)
	if size, mtime, ok := v.pinnedAttrs(); !ok || size != 2 || !mtime.Equal(tA) {
		t.Fatalf("pinnedAttrs after first open = (%d, %v, %v), want (2, %v, true)", size, mtime, ok, tA)
	}
	v.pinOpen(9, tB) // a newer open with a refreshed snapshot
	if size, mtime, ok := v.pinnedAttrs(); !ok || size != 9 || !mtime.Equal(tB) {
		t.Fatalf("pinnedAttrs after newer open = (%d, %v, %v), want (9, %v, true)", size, mtime, ok, tB)
	}
	v.unpinOpen() // the NEWER handle closes first
	if size, mtime, ok := v.pinnedAttrs(); !ok || size != 9 || !mtime.Equal(tB) {
		t.Fatalf("pinnedAttrs after the newer close = (%d, %v, %v); the pin must never retreat to the elder's (2, %v)", size, mtime, ok, tA)
	}
	v.unpinOpen() // last close clears the pin
	if _, _, ok := v.pinnedAttrs(); ok {
		t.Fatal("pinnedAttrs = ok after the last close; the pin must clear")
	}
}
