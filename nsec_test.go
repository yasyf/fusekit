package fusekit

import (
	"hash/fnv"
	"strings"
	"testing"
)

func TestVersionNsecDeterministic(t *testing.T) {
	for _, seed := range []string{"", "a", "deadbeef", "v:already-prefixed", "🦀", strings.Repeat("z", 4096)} {
		first := VersionNsec(seed)
		for i := 0; i < 5; i++ {
			if got := VersionNsec(seed); got != first {
				t.Fatalf("VersionNsec(%q) not deterministic: %d != %d", seed, got, first)
			}
		}
	}
}

func TestVersionNsecRange(t *testing.T) {
	for _, seed := range []string{"", "x", "tip-sha-0123456789abcdef", strings.Repeat("nano", 500)} {
		v := VersionNsec(seed)
		if v < 0 || v >= 1_000_000_000 {
			t.Errorf("VersionNsec(%q) = %d, want within [0, 1e9)", seed, v)
		}
	}
}

// TestVersionNsecFormula pins the exact wiring — the "v:" prefix and the 1e9
// modulus over FNV-1a — against an independent stdlib computation, so a change
// to either is caught.
func TestVersionNsecFormula(t *testing.T) {
	for _, seed := range []string{"", "abc", "tip-sha"} {
		h := fnv.New64a()
		h.Write([]byte("v:" + seed))
		want := int64(h.Sum64() % 1_000_000_000)
		if got := VersionNsec(seed); got != want {
			t.Errorf("VersionNsec(%q) = %d, want %d", seed, got, want)
		}
	}
}

// TestVersionNsecDistinct catches a constant/no-op implementation: distinct
// seeds overwhelmingly yield distinct nsec components.
func TestVersionNsecDistinct(t *testing.T) {
	if VersionNsec("alpha") == VersionNsec("beta") {
		t.Error("distinct seeds collided to the same nsec")
	}
}
