package holderfs

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/fusekit/content"
)

// Spike-3 regressions: the shared holder's monotonic mtime stabilization used
// to SWALLOW a consumer's nanosecond cache defeat — a same-second version
// change whose reported mtime sat at or below the high-water mark served an
// UNCHANGED mtime, and a post-write canonical render lost a coin flip to the
// wall-clock write bump. Content change is now keyed on Entry.Version: the
// served mtime always changes when the version does, and never regresses.

func TestBumpVersionLocked(t *testing.T) {
	base := time.Unix(1_700_000_000, 500_000_000)
	cases := []struct {
		name        string
		prevVersion string
		hadStat     bool
		entry       content.Entry
		wantAdvance bool  // served mtime strictly above the prior HWM
		wantExactly int64 // when non-zero: the exact served UnixNano
	}{
		{
			name:        "reported mtime past the mark advances to it",
			prevVersion: "v1", hadStat: true,
			entry:       content.Entry{Version: "v2", Mtime: base.UnixNano() + 300},
			wantAdvance: true,
			wantExactly: base.UnixNano() + 300,
		},
		{
			name:        "same-second lower-nsec VERSION change bumps +1ns (the spike-3 clamp gap)",
			prevVersion: "v1", hadStat: true,
			entry:       content.Entry{Version: "v2", Mtime: base.UnixNano() - 400_000_000},
			wantAdvance: true,
			wantExactly: base.UnixNano() + 1,
		},
		{
			name:        "same version with a lower mtime keeps the clamp (W2 stabilization)",
			prevVersion: "v1", hadStat: true,
			entry:       content.Entry{Version: "v1", Mtime: base.UnixNano() - 400_000_000},
			wantExactly: base.UnixNano(),
		},
		{
			name:        "first verdict never bumps on the version alone",
			prevVersion: "", hadStat: false,
			entry:       content.Entry{Version: "v1", Mtime: base.UnixNano() - 400_000_000},
			wantExactly: base.UnixNano(),
		},
		{
			name:        "zero mtime with a version change still bumps",
			prevVersion: "v1", hadStat: true,
			entry:       content.Entry{Version: "v2"},
			wantAdvance: true,
			wantExactly: base.UnixNano() + 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := &treeNode{mtimeHWM: base}
			n.bumpVersionLocked(tc.prevVersion, tc.hadStat, tc.entry)
			got := n.mtimeHWM.UnixNano()
			if got < base.UnixNano() {
				t.Fatalf("served mtime regressed: %d < %d", got, base.UnixNano())
			}
			if tc.wantAdvance && got <= base.UnixNano() {
				t.Fatalf("served mtime did not advance: %d <= %d", got, base.UnixNano())
			}
			if got != tc.wantExactly {
				t.Fatalf("served mtime = %d, want %d", got, tc.wantExactly)
			}
		})
	}
}

// TestVersionChangeDefeatsClampOverTheWire is the end-to-end spike-3 clamp
// regression: a same-second LOWER-nsec version change, delivered by the real
// bridge, must change the served mtime (previously clamped away forever).
func TestVersionChangeDefeatsClampOverTheWire(t *testing.T) {
	shrinkTreeWaits(t, 2*time.Second, time.Nanosecond)
	f := newTreeFakeH()
	v := newTestView(t, f)
	const p = "/notes/p.md"
	const sec = int64(1_700_000_000)

	f.putVersioned(p, []byte("hi"), sec*1_000_000_000+900_000_000, "vHi")
	st, rc := v.getattr(p)
	if rc != 0 {
		t.Fatalf("getattr: %d", rc)
	}
	served0 := st.mtime.UnixNano()
	if served0 != sec*1_000_000_000+900_000_000 {
		t.Fatalf("first served mtime = %d, want the consumer's", served0)
	}

	// Same second, LOWER nsec, new version — the case the HWM clamp swallowed.
	f.putVersioned(p, []byte("lo"), sec*1_000_000_000+100_000_000, "vLo")
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, rc = v.getattr(p)
		if rc != 0 {
			t.Fatalf("getattr: %d", rc)
		}
		served := st.mtime.UnixNano()
		if served < served0 {
			t.Fatalf("served mtime regressed: %d -> %d", served0, served)
		}
		if served > served0 {
			return // cache defeat restored: the version change is visible
		}
		if time.Now().After(deadline) {
			t.Fatal("same-second version change never advanced the served mtime — the clamp gap is back")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestWriteBumpNeverSwallowsCanonicalRender is the spike-3 write-bump
// regression, modeled exactly as the spike did: the write path bumps the HWM
// to a wall-clock instant anywhere in the save second, then the post-close
// refresh absorbs the consumer's canonical render (same second, VersionNsec-
// derived nanoseconds, NEW version). Previously ~half the renders lost the
// coin flip and stayed invisible; version-keying makes suppression exactly 0.
func TestWriteBumpNeverSwallowsCanonicalRender(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	nowSec := time.Now().Unix()
	const n = 4096
	for i := range n {
		writeBump := time.Unix(nowSec, rng.Int63n(1_000_000_000))
		node := &treeNode{mtimeHWM: writeBump, statOK: true, stat: content.Entry{Version: "pre-save"}}
		canonical := content.Entry{Version: "post-save", Mtime: nowSec*1_000_000_000 + rng.Int63n(1_000_000_000)}
		node.bumpVersionLocked(node.stat.Version, node.statOK, canonical)
		served := node.mtimeHWM.UnixNano()
		if served <= writeBump.UnixNano() && served != canonical.Mtime {
			t.Fatalf("iteration %d: canonical render suppressed (write bump %d, canonical %d, served %d)",
				i, writeBump.UnixNano(), canonical.Mtime, served)
		}
		if served < writeBump.UnixNano() {
			t.Fatalf("iteration %d: served mtime regressed below the write bump", i)
		}
	}
}

// TestAbsorbSnapshotVersionKeyed pins the open-path absorption: an open
// snapshot with a new version but a clamped mtime still advances the served
// mtime, and records the version so the SAME snapshot never double-bumps.
func TestAbsorbSnapshotVersionKeyed(t *testing.T) {
	v := newTreeView("d", nil)
	base := time.Unix(1_700_000_000, 500_000_000)
	v.mu.Lock()
	n := v.nodeLocked("/x")
	n.statOK = true
	n.stat = content.Entry{Kind: content.EntrySynth, Version: "v1"}
	n.mtimeHWM = base

	v.absorbSnapshotLocked(n, "/x", content.Entry{Version: "v2", Mtime: base.UnixNano() - 100}, n.gen)
	afterFirst := n.mtimeHWM.UnixNano()
	v.absorbSnapshotLocked(n, "/x", content.Entry{Version: "v2", Mtime: base.UnixNano() - 100}, n.gen)
	afterSecond := n.mtimeHWM.UnixNano()
	v.mu.Unlock()

	if afterFirst != base.UnixNano()+1 {
		t.Fatalf("version-changed snapshot served %d, want +1ns above the mark %d", afterFirst, base.UnixNano())
	}
	if afterSecond != afterFirst {
		t.Fatalf("same-version re-absorb bumped again: %d -> %d", afterFirst, afterSecond)
	}
}

// TestVersionBumpSurvivesDeleteRecreate pins P-14 at the store layer: an
// observed ENOENT retains the node's version and HWM, so a recreate with a
// DIFFERENT version and a clamped mtime still bumps monotonically — and the
// minted ino stays, so the mtime is the client's only (and sufficient)
// change signal. A same-version recreate stays clamped (negative leg).
func TestVersionBumpSurvivesDeleteRecreate(t *testing.T) {
	v := newTreeView("d", nil)
	base := time.Unix(1_700_000_000, 500_000_000)
	v.mu.Lock()
	defer v.mu.Unlock()
	n := v.nodeLocked("/x")
	v.storeStatLocked(n, "/x", content.Entry{Kind: content.EntrySynth, Version: "v1", Mtime: base.UnixNano()})
	served0, ino0 := n.mtimeHWM, n.ino

	// Observed delete: fetchStat's ClassNotFound bookkeeping.
	n.statOK, n.notFound = false, true
	v.storeStatLocked(n, "/x", content.Entry{Kind: content.EntrySynth, Version: "v2", Mtime: base.UnixNano() - 100})
	if !n.mtimeHWM.After(served0) {
		t.Fatalf("recreate with a new version served %d, want a bump above %d", n.mtimeHWM.UnixNano(), served0.UnixNano())
	}
	if n.ino != ino0 {
		t.Fatalf("minted ino changed across delete/recreate: %d -> %d", ino0, n.ino)
	}

	// Same-version recreate keeps the clamp: no spurious bump.
	served1 := n.mtimeHWM
	n.statOK, n.notFound = false, true
	v.storeStatLocked(n, "/x", content.Entry{Kind: content.EntrySynth, Version: "v2", Mtime: base.UnixNano() - 100})
	if !n.mtimeHWM.Equal(served1) {
		t.Fatalf("same-version recreate bumped: %d -> %d", served1.UnixNano(), n.mtimeHWM.UnixNano())
	}
}

// TestDeleteRecreateBumpsOverTheWire is P-14 end to end: delete observed via
// the bridge (ENOENT served), recreate with a new version whose mtime sits at
// or below the high-water mark — the served mtime must still advance.
func TestDeleteRecreateBumpsOverTheWire(t *testing.T) {
	shrinkTreeWaits(t, 2*time.Second, time.Nanosecond)
	f := newTreeFakeH()
	v := newTestView(t, f)
	const p = "/notes/p.md"
	const sec = int64(1_700_000_000)

	f.putVersioned(p, []byte("one"), sec*1_000_000_000+900_000_000, "v1")
	st, rc := v.getattr(p)
	if rc != 0 {
		t.Fatalf("getattr: %d", rc)
	}
	served0 := st.mtime.UnixNano()

	f.mu.Lock()
	delete(f.files, p)
	delete(f.mtimes, p)
	delete(f.versions, p)
	f.mu.Unlock()
	waitTreeCond(t, "observed delete", func() bool {
		_, rc := v.getattr(p)
		return rc == -int(syscall.ENOENT)
	})

	// Recreate: same second, LOWER nsec, NEW version.
	f.putVersioned(p, []byte("two"), sec*1_000_000_000+100_000_000, "v2")
	waitTreeCond(t, "recreate served with a bumped mtime", func() bool {
		st, rc := v.getattr(p)
		if rc != 0 {
			return false
		}
		if st.mtime.UnixNano() < served0 {
			t.Fatalf("served mtime regressed: %d -> %d", served0, st.mtime.UnixNano())
		}
		return st.mtime.UnixNano() > served0
	})
}

// TestHandleMtimeNeverRegressesOnWrite pins P-15: with a consumer-supplied
// FUTURE mtime pinned at open, a local write must serve max(now, HWM+1ns) on
// the handle — never a bare wall clock that regresses below the pin.
func TestHandleMtimeNeverRegressesOnWrite(t *testing.T) {
	shrinkTreeWaits(t, 2*time.Second, time.Hour)
	f := newTreeFakeH()
	v := newTestView(t, f)
	const p = "/notes/future.md"
	future := time.Now().Add(4 * 365 * 24 * time.Hour).UnixNano()
	f.put(p, []byte("2030"), future, 0)

	st, rc := v.getattr(p)
	if rc != 0 || st.mtime.UnixNano() != future {
		t.Fatalf("getattr = (%d, rc %d), want the consumer's future mtime %d", st.mtime.UnixNano(), rc, future)
	}
	fh, rc := v.open(p, 0)
	if rc != 0 {
		t.Fatalf("open: %d", rc)
	}
	if rc := v.write(fh, []byte("2026 bytes"), 0); rc < 0 {
		t.Fatalf("write: %d", rc)
	}
	hst, rc := v.getattrHandle(fh)
	if rc != 0 {
		t.Fatalf("getattrHandle: %d", rc)
	}
	if hst.mtime.UnixNano() <= future {
		t.Fatalf("handle mtime after write = %d, want strictly above the pinned future %d (regressed to the wall clock)", hst.mtime.UnixNano(), future)
	}
	v.release(fh)
}

// TestReleaseRefreshNeverJoinsOlderStatFlight pins P-16: release's post-
// commit refresh must be a FRESH Stat RPC, never coalesced into an older
// in-flight stat that computed its (pre-commit) reply before the commit.
func TestReleaseRefreshNeverJoinsOlderStatFlight(t *testing.T) {
	shrinkTreeWaits(t, 2*time.Second, time.Hour)
	f := newTreeFakeH()
	v := newTestView(t, f)
	const p = "/notes/p.md"
	f.putVersioned(p, []byte("a"), 1_700_000_000_000_000_000, "v1")
	if _, rc := v.getattr(p); rc != 0 {
		t.Fatalf("warm getattr failed")
	}
	statKey := "stat:" + p
	warm := f.count(statKey)

	// Park an OLDER stat flight: its (pre-commit) reply is computed and held.
	exit := make(chan struct{})
	f.setStatExitBlock(exit)
	t.Cleanup(func() { close(exit) })
	go v.flights.Do("s:"+p, 0, func() int { return errnoOf(v.fetchStat(p)) })
	waitTreeCond(t, "older stat flight parked", func() bool { return f.parkedStats() == 1 })

	fh, rc := v.open(p, 0)
	if rc != 0 {
		t.Fatalf("open: %d", rc)
	}
	if rc := v.write(fh, []byte("bb"), 0); rc < 0 {
		t.Fatalf("write: %d", rc)
	}
	if rc := v.flush(fh); rc != 0 {
		t.Fatalf("flush: %d", rc)
	}
	if rc := v.release(fh); rc != 0 {
		t.Fatalf("release: %d", rc)
	}
	// The fresh refresh lands as a NEW Stat RPC while the old flight is still
	// parked — a coalesced refresh would never move the counter.
	waitTreeCond(t, "release issued a fresh stat", func() bool {
		return f.count(statKey) >= warm+2 // the parked flight + release's fresh fetch
	})
}

// waitTreeCond polls cond briefly; the tree view schedules refreshes off the
// caller's path, so state assertions must not race them.
func waitTreeCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestCompleteWriteConcurrentMergesMaxMtime pins the R3 handle-mtime merge:
// with writers racing on one handle, a writer holding a stale (lower) HWM
// snapshot must never overwrite a later writer's handle mtime — the merge is
// max(), so every observer sees the handle mtime advance monotonically and
// it lands on the node's final high-water mark.
func TestCompleteWriteConcurrentMergesMaxMtime(t *testing.T) {
	v := newTreeView("d", nil)
	v.mu.Lock()
	n := v.nodeLocked("/x")
	n.statOK = true
	n.openPins = 1
	v.mu.Unlock()
	h := &treeHandle{path: "/x", node: n}

	var wg sync.WaitGroup
	var regressed atomic.Bool
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last time.Time
			for j := 0; j < 300; j++ {
				v.completeWrite(h, func() {})
				v.hmu.Lock()
				cur := h.mtime
				v.hmu.Unlock()
				if cur.Before(last) {
					regressed.Store(true)
					return
				}
				last = cur
			}
		}()
	}
	wg.Wait()
	if regressed.Load() {
		t.Fatal("handle mtime REGRESSED under concurrent writers — a stale lower HWM overwrote a later merge")
	}

	v.mu.Lock()
	hwm := n.mtimeHWM
	v.mu.Unlock()
	v.hmu.Lock()
	got := h.mtime
	v.hmu.Unlock()
	if !got.Equal(hwm) {
		t.Fatalf("final handle mtime %d != node HWM %d", got.UnixNano(), hwm.UnixNano())
	}
}

// TestReleaseRefreshBoundedPerPath pins the R3 boundedness half of the
// post-commit refresh (freshness is TestReleaseRefreshNeverJoinsOlderStatFlight):
// a stalled consumer plus a burst of closes on one path costs ONE in-flight
// refresh — never a goroutine/conn per close — and once unstalled, exactly
// one rerun lands for the commits that arrived mid-flight.
func TestReleaseRefreshBoundedPerPath(t *testing.T) {
	shrinkTreeWaits(t, 2*time.Second, time.Hour)
	f := newTreeFakeH()
	v := newTestView(t, f)
	const p = "/notes/p.md"
	f.putVersioned(p, []byte("a"), 1_700_000_000_000_000_000, "v1")
	if _, rc := v.getattr(p); rc != 0 {
		t.Fatal("warm getattr failed")
	}
	statKey := "stat:" + p
	warm := f.count(statKey)

	exit := make(chan struct{})
	f.setStatExitBlock(exit)
	var closed bool
	t.Cleanup(func() {
		if !closed {
			close(exit)
		}
	})

	const n = 5
	fhs := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		fh, rc := v.open(p, 0)
		if rc != 0 {
			t.Fatalf("open: %d", rc)
		}
		fhs = append(fhs, fh)
	}
	for _, fh := range fhs {
		if rc := v.release(fh); rc != 0 {
			t.Fatalf("release: %d", rc)
		}
	}
	waitTreeCond(t, "first refresh in flight", func() bool { return f.parkedStats() >= 1 })
	time.Sleep(50 * time.Millisecond) // the old unbounded path piles up here
	if got := f.parkedStats(); got != 1 {
		t.Fatalf("parked stat flights = %d, want exactly 1 (bounded per path)", got)
	}
	if got := f.count(statKey); got != warm+1 {
		t.Fatalf("stat RPCs while stalled = %d, want 1", got-warm)
	}

	closed = true
	close(exit)
	waitTreeCond(t, "the single rerun landed", func() bool { return f.count(statKey) == warm+2 })
	time.Sleep(50 * time.Millisecond)
	if got := f.count(statKey); got != warm+2 {
		t.Fatalf("stat RPCs after settling = %d, want 2 (one flight + one rerun)", got-warm)
	}
}
