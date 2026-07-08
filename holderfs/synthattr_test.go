package holderfs

import (
	"os"
	"path/filepath"
	"syscall"
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
// invalidation on open files. The historical dates also pin the
// first-incarnation contract: with no earlier incarnation, there is no floor,
// and the genuine on-disk mtimes serve untouched.
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

// TestSynthViewIncarnationAttrsAdvance pins both halves of the incarnation
// contract at the view level. A FIRST incarnation has no floor: the genuine
// on-disk timestamps serve untouched, however stale — a production mount must
// never floor a pre-existing file's attrs to mount-start time. A RE-ATTACH
// (same writePath, same freshness files, on-disk state UNTOUCHED between
// builds) serves strictly increasing mtime and ctime across the rebuild, while
// each incarnation's own served attrs hold stable between calls. go-nfsv4
// mints the NFSv4 change attribute from the served ctime and reuses path-keyed
// fileids across a mux detach/re-attach, so a repeated (size, ctime) pair
// validates the PREVIOUS incarnation's cached pages (VM-proven: validate-mux
// fileid-cycle 1 served cycle 0's payload); strict advance is the load-bearing
// invariant.
func TestSynthViewIncarnationAttrsAdvance(t *testing.T) {
	dir := t.TempDir()
	writePath := filepath.Join(dir, "backing")
	fresh := filepath.Join(dir, "fresh")
	// Stale on-disk state: mtimes well in the past, so only the incarnation
	// floor can move the served attrs across the rebuild — the hard case, where
	// a detached-victim mutation rewrote same-size content without touching
	// writePath.
	past := time.Now().Add(-24 * time.Hour).Truncate(time.Second)
	for _, p := range []string{writePath, fresh} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatal(err)
		}
	}
	realCtime := func() time.Time {
		t.Helper()
		fi, err := os.Lstat(writePath)
		if err != nil {
			t.Fatal(err)
		}
		st := fi.Sys().(*syscall.Stat_t)
		return statCtime(st)
	}

	v1 := newSynthView(".x", "d", deadClient(t), writePath, []string{fresh})
	m1, c1 := v1.servedMtime(), v1.servedCtime(realCtime())
	if !m1.Equal(past) {
		t.Fatalf("incarnation 1 served mtime %v, want the genuine on-disk %v — a first incarnation has no floor", m1, past)
	}
	if !c1.Equal(realCtime()) {
		t.Fatalf("incarnation 1 served ctime %v, want the genuine on-disk %v — a first incarnation has no floor", c1, realCtime())
	}
	// Within the incarnation the served attrs are stable: repeated calls with
	// unchanged disk state return identical values — attr churn under a live
	// client is the panic-adjacent behavior the stabilization forbids.
	for i := 0; i < 3; i++ {
		if got := v1.servedMtime(); !got.Equal(m1) {
			t.Fatalf("incarnation 1 servedMtime call %d = %v, want stable %v", i, got, m1)
		}
		if got := v1.servedCtime(realCtime()); !got.Equal(c1) {
			t.Fatalf("incarnation 1 servedCtime call %d = %v, want stable %v", i, got, c1)
		}
	}

	// Deliberately back-to-back — no sleep. Strict advance may not lean on the
	// wall clock ticking between builds: a detach immediately followed by an
	// attach can construct both views within one clock quantum (mintAttrFloor's
	// value-chained per-writePath floor owns the guarantee, clock-free).
	v2 := newSynthView(".x", "d", deadClient(t), writePath, []string{fresh})
	m2, c2 := v2.servedMtime(), v2.servedCtime(realCtime())
	if !m2.After(m1) {
		t.Fatalf("incarnation 2 served mtime %v, want strictly after incarnation 1's %v (a repeated baseline validates stale client pages)", m2, m1)
	}
	if !c2.After(c1) {
		t.Fatalf("incarnation 2 served ctime %v, want strictly after incarnation 1's %v (change = ctime for go-nfsv4; a repeat validates stale client pages)", c2, c1)
	}

	// A real writePath ctime NEWER than the incarnation floor wins: a mount-side
	// atomic save must still surface as a change signal, not be floored away.
	future := time.Now().Add(time.Hour)
	if got := v2.servedCtime(future); !got.Equal(future) {
		t.Fatalf("servedCtime with a newer real ctime = %v, want the real %v", got, future)
	}
}

// TestSynthViewServedCtimeMonotonic pins servedCtime's high-water mark: within
// one incarnation the served ctime never decreases, even when a later
// write-through lands with a SMALLER real ctime because the wall clock stepped
// backward (NTP correction — routine on the VMs the coherence bug was
// reproduced in) between two saves. go-nfsv4 mints the NFSv4 change attribute
// from the served ctime and NFS_CHANGED compares it for inequality, so a
// rewind reads as a change and lands an invalidation on a file the client may
// hold open — the vinvalbuf2-adjacent churn the attr stabilization forbids.
func TestSynthViewServedCtimeMonotonic(t *testing.T) {
	v := newSynthView(".x", "d", deadClient(t), filepath.Join(t.TempDir(), "absent"), nil)

	// First write-through: with no floor (first incarnation), the real ctime
	// serves as-is and raises the mark.
	t1 := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	if got := v.servedCtime(t1); !got.Equal(t1) {
		t.Fatalf("servedCtime(first write %v) = %v, want the real ctime", t1, got)
	}
	// Second write-through after a backward clock step: below the value already
	// served. The mark must hold — no rewind.
	t2 := t1.Add(-time.Hour)
	if got := v.servedCtime(t2); !got.Equal(t1) {
		t.Fatalf("servedCtime(backdated write %v) = %v, want the %v high-water mark (served ctime must never regress)", t2, got, t1)
	}
	// Unchanged input stays inert at the mark — no attr churn absent a change.
	if got := v.servedCtime(t2); !got.Equal(t1) {
		t.Fatalf("servedCtime(repeat %v) = %v, want the stable %v", t2, got, t1)
	}
	// A genuinely newer replacement still surfaces and advances the mark.
	t3 := t1.Add(time.Hour)
	if got := v.servedCtime(t3); !got.Equal(t3) {
		t.Fatalf("servedCtime(newer write %v) = %v, want the real ctime", t3, got)
	}
}

// TestSynthViewAttrFloorChainsPerWritePath pins mintAttrFloor's structural
// guarantees. Per writePath, incarnation floors chain on the recorded served
// values — one nanosecond past everything the previous incarnation served,
// with no wall clock involved — so back-to-back rebuilds never repeat a
// served-ctime baseline (the NFSv4 change attribute go-nfsv4 mints from it)
// even under clock ties or backward steps. And the chain is scoped: a first
// incarnation — no predecessor in this process — gets NO floor, and an
// unrelated entry is untouched by another entry's history, so production
// mounts serve genuine on-disk attrs instead of flooring every pre-existing
// file's timestamps to mount-start time.
func TestSynthViewAttrFloorChainsPerWritePath(t *testing.T) {
	writePath := filepath.Join(t.TempDir(), "absent")

	// First incarnation: no floor. With nothing on disk and nothing served, the
	// baseline is the zero time — real attrs would serve untouched.
	vA := newSynthView(".x", "d", deadClient(t), writePath, nil)
	if c := vA.servedCtime(time.Time{}); !c.IsZero() {
		t.Fatalf("first incarnation served-ctime baseline = %v, want zero — a first incarnation must have no floor", c)
	}
	// Serve a real value so successors have a baseline to clear.
	real := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	if got := vA.servedCtime(real); !got.Equal(real) {
		t.Fatalf("servedCtime(real %v) = %v, want the real value", real, got)
	}

	// Successive incarnations, deliberately back-to-back with no sleeps: each
	// baseline must clear its predecessor's — on-disk state unchanged (absent),
	// so only the floor can move it, and a repeat would validate the client's
	// stale pages under the reclaimed path-keyed fileid.
	prev := real
	for i := 0; i < 3; i++ {
		v := newSynthView(".x", "d", deadClient(t), writePath, nil)
		c := v.servedCtime(time.Time{})
		if !c.After(prev) {
			t.Fatalf("incarnation %d served-ctime baseline = %v, want strictly after the predecessor's %v", i+2, c, prev)
		}
		prev = c
	}

	// The chain is keyed by writePath: an unrelated entry's first incarnation
	// keeps the no-floor contract even after this entry's history.
	other := newSynthView(".y", "d", deadClient(t), filepath.Join(t.TempDir(), "absent"), nil)
	if c := other.servedCtime(time.Time{}); !c.IsZero() {
		t.Fatalf("unrelated entry's first-incarnation baseline = %v, want zero — floors must never leak across writePaths", c)
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
