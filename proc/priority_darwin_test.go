//go:build darwin

package proc

import (
	"testing"

	"golang.org/x/sys/unix"
)

// TestSetBackgroundPriority pins the holder's low-CPU demotion: the call moves
// the process into the Darwin background band from a foreground start, and
// revoking with 0 restores foreground — proving the state is not one-way and
// leaving the test process as it found it. getpriority(PRIO_DARWIN_PROCESS)
// reports the policy BIT (xnu get_background_proc: nonzero = backgrounded,
// 0 = foreground), not the PRIO_DARWIN_BG constant setpriority takes.
func TestSetBackgroundPriority(t *testing.T) {
	if prio, err := unix.Getpriority(prioDarwinProcess, 0); err != nil || prio != 0 {
		t.Fatalf("pre-state: getpriority(PRIO_DARWIN_PROCESS, 0) = %#x, %v; want a foreground (0) start", prio, err)
	}
	t.Cleanup(func() {
		if err := unix.Setpriority(prioDarwinProcess, 0, 0); err != nil {
			t.Fatalf("revoke background state: %v", err)
		}
		if prio, err := unix.Getpriority(prioDarwinProcess, 0); err != nil || prio != 0 {
			t.Fatalf("post-revoke: getpriority = %#x, %v; want foreground (0)", prio, err)
		}
	})

	if err := SetBackgroundPriority(); err != nil {
		t.Fatal(err)
	}
	got, err := unix.Getpriority(prioDarwinProcess, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got == 0 {
		t.Fatal("getpriority after SetBackgroundPriority = 0 (foreground); want the background bit set")
	}
}
