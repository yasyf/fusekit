package proc

import (
	"testing"
	"time"
)

func TestBackoffAfter(t *testing.T) {
	b := Backoff{Base: time.Second, Cap: 10 * time.Second}
	cases := []struct {
		failures int
		want     time.Duration
	}{
		{failures: -1, want: time.Second}, // negative never shrinks below base
		{failures: 0, want: time.Second},  // zero never shrinks below base
		{failures: 1, want: time.Second},  // first failure is base
		{failures: 2, want: 2 * time.Second},
		{failures: 3, want: 4 * time.Second},
		{failures: 4, want: 8 * time.Second},
		{failures: 5, want: 10 * time.Second}, // 16s clamped to cap
		{failures: 9, want: 10 * time.Second}, // far past the cap stays capped
	}
	for _, tc := range cases {
		if got := b.After(tc.failures); got != tc.want {
			t.Errorf("After(%d) = %v, want %v", tc.failures, got, tc.want)
		}
	}
}

// TestBackoffBaseAboveCap pins the clamp when Base already exceeds Cap: every
// wait is Cap, never the larger Base.
func TestBackoffBaseAboveCap(t *testing.T) {
	b := Backoff{Base: 30 * time.Second, Cap: 5 * time.Second}
	for _, failures := range []int{0, 1, 2, 5} {
		if got := b.After(failures); got != 5*time.Second {
			t.Errorf("After(%d) with Base>Cap = %v, want the cap 5s", failures, got)
		}
	}
}
