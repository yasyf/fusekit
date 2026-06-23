package fusekit

import "testing"

type pureDeclarer struct{}

func (pureDeclarer) FusePassthroughOnly() bool { return true }

type syntheticDeclarer struct{}

func (syntheticDeclarer) FusePassthroughOnly() bool { return false }

type noDeclarer struct{}

// TestPassthroughEligible pins the opt-in contract: only an FS that affirmatively
// declares FusePassthroughOnly() == true is eligible for the FSKit backend.
// Everything else — a false declaration, no marker at all, or nil — stays on the
// safe NFS backend.
func TestPassthroughEligible(t *testing.T) {
	cases := []struct {
		name string
		fs   any
		want bool
	}{
		{"declares pure passthrough", pureDeclarer{}, true},
		{"declares synthetic", syntheticDeclarer{}, false},
		{"no marker (safe default)", noDeclarer{}, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := passthroughEligible(tc.fs); got != tc.want {
				t.Fatalf("passthroughEligible(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
