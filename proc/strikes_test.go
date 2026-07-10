package proc

import (
	"testing"
	"time"
)

func TestStrikesWindow(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name    string
		offsets []time.Duration // strike offsets from base
		at      time.Duration   // Struck evaluated at base+at
		want    bool
	}{
		{name: "no strikes", offsets: nil, at: 0, want: false},
		{name: "under limit", offsets: []time.Duration{0, time.Minute}, at: time.Minute, want: false},
		{name: "at limit inside window", offsets: []time.Duration{0, time.Minute, 2 * time.Minute}, at: 2 * time.Minute, want: true},
		{name: "old strike pruned under limit", offsets: []time.Duration{0, 9 * time.Minute, 11 * time.Minute}, at: 11 * time.Minute, want: false},
		{name: "strike exactly window old is pruned", offsets: []time.Duration{0, 5 * time.Minute, 10 * time.Minute}, at: 10 * time.Minute, want: false},
		{name: "over limit", offsets: []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute, 4 * time.Minute}, at: 4 * time.Minute, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &Strikes{Limit: 3, Window: 10 * time.Minute}
			var last bool
			for _, off := range tc.offsets {
				last = s.Strike(base.Add(off))
			}
			if got := s.Struck(base.Add(tc.at)); got != tc.want {
				t.Fatalf("Struck = %v, want %v", got, tc.want)
			}
			// The final Strike's verdict at its own time must agree with a
			// same-time Struck.
			if len(tc.offsets) > 0 && tc.offsets[len(tc.offsets)-1] == tc.at && last != tc.want {
				t.Fatalf("final Strike = %v, want %v", last, tc.want)
			}
		})
	}
}

func TestStrikesPersistRoundTrip(t *testing.T) {
	base := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	s1 := &Strikes{Limit: 3, Window: 10 * time.Minute}
	s1.Strike(base)
	s1.Strike(base.Add(time.Minute))

	// A "successor process" restores the window and its third strike trips.
	s2 := &Strikes{Limit: 3, Window: 10 * time.Minute}
	s2.Load(s1.Times())
	if s2.Struck(base.Add(time.Minute)) {
		t.Fatal("restored window tripped early")
	}
	if !s2.Strike(base.Add(2 * time.Minute)) {
		t.Fatal("third strike after restore did not trip")
	}

	s2.Reset()
	if s2.Struck(base.Add(2 * time.Minute)) {
		t.Fatal("Reset kept the trip")
	}
	if got := s2.Times(); len(got) != 0 {
		t.Fatalf("Times after Reset = %v, want empty", got)
	}
}

func TestLadder(t *testing.T) {
	l := &Ladder{Steps: []time.Duration{time.Minute, 5 * time.Minute, time.Hour}}
	want := []time.Duration{time.Minute, 5 * time.Minute, time.Hour, time.Hour, time.Hour}
	for i, w := range want {
		if got := l.Next(); got != w {
			t.Fatalf("Next #%d = %s, want %s", i, got, w)
		}
	}
	l.Reset()
	if got := l.Next(); got != time.Minute {
		t.Fatalf("Next after Reset = %s, want %s", got, time.Minute)
	}

	empty := &Ladder{}
	if got := empty.Next(); got != 0 {
		t.Fatalf("empty ladder Next = %s, want 0", got)
	}
}
