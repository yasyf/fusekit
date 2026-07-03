//go:build darwin

package proc

import (
	"testing"

	"golang.org/x/sys/unix"
)

// TestNice pins the holder's politeness mechanism: Nice(n) lands the classic
// nice value n (a soft scheduling weight — NOT the Darwin background band's
// policy bit), and the change is one-way for an unprivileged process — the
// property that makes the startup value choice load-bearing. The test cannot
// restore the test process's priority afterwards for the same reason; the
// residual nice only softens scheduling of the remaining tests in this binary.
func TestNice(t *testing.T) {
	pre, err := unix.Getpriority(unix.PRIO_PROCESS, 0)
	if err != nil {
		t.Fatal(err)
	}
	if pre >= 5 {
		t.Skipf("pre-state: nice already %d; cannot lower priority to observe Nice(5)", pre)
	}

	if err := Nice(5); err != nil {
		t.Fatal(err)
	}
	got, err := unix.Getpriority(unix.PRIO_PROCESS, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != 5 {
		t.Fatalf("getpriority after Nice(5) = %d; want 5", got)
	}

	// The Darwin background band must stay untouched — Nice is a weight, not
	// the throttling band (getpriority(PRIO_DARWIN_PROCESS) reports the policy
	// bit: nonzero = backgrounded).
	if band, err := unix.Getpriority(4 /* PRIO_DARWIN_PROCESS */, 0); err != nil || band != 0 {
		t.Fatalf("darwin band after Nice(5) = %#x, %v; want foreground (0)", band, err)
	}

	// One-way: an unprivileged process cannot renice back down. Root (some CI
	// environments) legitimately can — only assert the failure when unprivileged.
	if unix.Getuid() != 0 {
		if err := unix.Setpriority(unix.PRIO_PROCESS, 0, pre); err == nil {
			t.Fatalf("renice %d -> %d unexpectedly succeeded for an unprivileged process", got, pre)
		}
	}
}
