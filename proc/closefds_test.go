package proc

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/yasyf/fusekit/lease"
)

// TestCloseInheritedFDsReleasesParentLease pins P-11: a detached child
// spawned while the parent holds a session lease inherits the non-CLOEXEC
// lease descriptor; after CloseInheritedFDs the child no longer pins it. The
// no-sweep case is the negative control proving the leak (and this test)
// is real.
func TestCloseInheritedFDsReleasesParentLease(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sweep bool
	}{
		{name: "sweep releases the inherited lease", sweep: true},
		{name: "no sweep pins it (negative control)", sweep: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			const dir = "/x/mnt"
			h, err := lease.Acquire(root, dir, "parent")
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()

			cmd := exec.Command(os.Args[0], "-test.run", "^TestFDSweepHelperProcess$", "-test.v")
			cmd.Env = append(os.Environ(),
				"FDSWEEP_HELPER=1",
				fmt.Sprintf("FDSWEEP_SWEEP=%v", tc.sweep),
			)
			stdin, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				stdin.Close()
				_ = cmd.Wait()
			})
			waitHelperReady(t, stdout)

			// The parent's own share is gone; only the child's inherited fd
			// (if any survived the sweep) can keep the lease held.
			if err := h.Close(); err != nil {
				t.Fatal(err)
			}
			f, err := lease.Seize(root, dir)
			if tc.sweep {
				if err != nil {
					t.Fatalf("Seize after child swept = %v, want free (the child must not hold the parent's lease)", err)
				}
				_ = f.Release()
				return
			}
			if !errors.Is(err, lease.ErrBusy) {
				t.Fatalf("Seize with unswept child = %v, want ErrBusy (the inherited fd must pin — otherwise this test cannot catch the leak)", err)
			}
			// The pin dies with the child's descriptor.
			stdin.Close()
			_ = cmd.Wait()
			f, err = lease.Seize(root, dir)
			if err != nil {
				t.Fatalf("Seize after child exit = %v, want free", err)
			}
			_ = f.Release()
		})
	}
}

func waitHelperReady(t *testing.T, stdout io.Reader) {
	t.Helper()
	ready := make(chan error, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if sc.Text() == "fdsweep-ready" {
				ready <- nil
				return
			}
		}
		ready <- fmt.Errorf("helper exited before ready: %v", sc.Err())
	}()
	select {
	case err := <-ready:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("helper never reported ready")
	}
}

// TestFDSweepHelperProcess is the re-exec'd child body, inert unless
// FDSWEEP_HELPER=1: optionally sweep, report ready, then hold all inherited
// state until stdin closes.
func TestFDSweepHelperProcess(t *testing.T) {
	if os.Getenv("FDSWEEP_HELPER") != "1" {
		t.Skip("helper body; runs only re-exec'd")
	}
	if os.Getenv("FDSWEEP_SWEEP") == "true" {
		if err := CloseInheritedFDs(); err != nil {
			t.Fatalf("CloseInheritedFDs: %v", err)
		}
	}
	fmt.Println("fdsweep-ready")
	_, _ = io.Copy(io.Discard, os.Stdin)
}
