package holderfs

import (
	"math/rand"
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
