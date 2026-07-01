package fusekit

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMountAliveSkipsNonVisibleFirstEntry pins that a mount reads alive when
// ANY base entry is visible, even when the lexicographically-first (a
// holder-redirected dotfile) is not.
func TestMountAliveSkipsNonVisibleFirstEntry(t *testing.T) {
	base := t.TempDir()
	// ".credentials.json" must sort before "settings.json".
	for _, n := range []string{".credentials.json", "settings.json"} {
		if err := os.WriteFile(filepath.Join(base, n), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	acct := t.TempDir()
	if err := os.WriteFile(filepath.Join(acct, "settings.json"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if !MountAlive(base, acct) {
		t.Error("MountAlive = false when a later base entry is visible; a redirected first entry must not read the mount dead")
	}

	if MountAlive(base, t.TempDir()) {
		t.Error("MountAlive = true with no base entry visible, want false (dead/pre-mount)")
	}
}

// swapMountAlive seams mountAliveFn for one test; callers must not run in
// parallel.
func swapMountAlive(t *testing.T, fn func(base, accountDir string) bool) {
	t.Helper()
	prev := mountAliveFn
	mountAliveFn = fn
	t.Cleanup(func() { mountAliveFn = prev })
}

// swapStatProbeTimeout seams statProbeTimeout for one test; callers must not
// run in parallel.
func swapStatProbeTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	prev := statProbeTimeout
	statProbeTimeout = d
	t.Cleanup(func() { statProbeTimeout = prev })
}

// TestMountAliveWithin pins that a healthy stat's verdict passes through and
// that a parked stat (a wedged mirror) reads NOT alive within the bound.
func TestMountAliveWithin(t *testing.T) {
	cases := []struct {
		name string
		dir  string // unique per case: aliveProbes joins by dir
		park bool
		stat bool
		want bool
	}{
		{name: "healthy live probe reads alive", dir: "/probe/acct-live", stat: true, want: true},
		{name: "healthy dead probe reads not alive", dir: "/probe/acct-dead", stat: false, want: false},
		{name: "parked probe reads not alive within the bound", dir: "/probe/acct-parked", park: true, stat: true, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			swapStatProbeTimeout(t, 20*time.Millisecond)
			release := make(chan struct{})
			swapMountAlive(t, func(base, accountDir string) bool {
				if base != "base" || accountDir != tc.dir {
					t.Errorf("probe got (%q, %q), want (\"base\", %q)", base, accountDir, tc.dir)
				}
				if tc.park {
					<-release
				}
				return tc.stat
			})
			// Drain the parked probe body before swapMountAlive's cleanup
			// restores the seam it reads (cleanups run LIFO).
			t.Cleanup(func() {
				close(release)
				deadline := time.Now().Add(5 * time.Second)
				for aliveProbes.Inflight() != 0 {
					if time.Now().After(deadline) {
						t.Error("parked probe body never drained")
						return
					}
					time.Sleep(time.Millisecond)
				}
			})

			start := time.Now()
			got := MountAliveWithin("base", tc.dir)
			elapsed := time.Since(start)
			if got != tc.want {
				t.Errorf("MountAliveWithin = %v, want %v", got, tc.want)
			}
			// Returning at all while parked is the property; the margin keeps
			// it unflaky.
			if tc.park && elapsed >= time.Second {
				t.Errorf("MountAliveWithin returned after %v with a parked stat; the %v bound did not hold", elapsed, statProbeTimeout)
			}
		})
	}
}

// TestMountWaitErrSentinels pins mountWaitErr's grant split: an unproven
// mount-up timeout wraps ErrMountNotLive with grant-pending context, a proven
// one wraps ErrMountTimeout; the error stays backend-neutral — pane copy is
// the consumer's (overlay.Backend.Enablement) to surface.
func TestMountWaitErrSentinels(t *testing.T) {
	const grantPhrase = "one-time OS volume-access grant"
	const retryPhrase = "retry automatically"
	const provenPhrase = "the OS grant is proven"
	const dir = "/pool/accounts/acct-01"
	const waited = 8 * time.Second
	paneCopy := []string{"Network Volumes", "Privacy & Security", "System Settings"}
	cases := []struct {
		name             string
		proven           bool
		wantIs           error
		wantNotIs        error
		wantGrantPhrase  bool
		wantProvenPhrase bool
		wantWaited       bool
	}{
		{
			name:            "unproven grant reads as missing grant, factual + backend-neutral",
			proven:          false,
			wantIs:          ErrMountNotLive,
			wantNotIs:       ErrMountTimeout,
			wantGrantPhrase: true,
		},
		{
			name:             "proven grant reads as transient timeout, grant proven",
			proven:           true,
			wantIs:           ErrMountTimeout,
			wantNotIs:        ErrMountNotLive,
			wantProvenPhrase: true,
			wantWaited:       true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mountWaitErr(dir, waited, tc.proven)
			if err == nil {
				t.Fatal("mountWaitErr returned nil")
			}
			if !errors.Is(err, tc.wantIs) {
				t.Errorf("errors.Is(err, %v) = false, want true; err = %v", tc.wantIs, err)
			}
			if errors.Is(err, tc.wantNotIs) {
				t.Errorf("errors.Is(err, %v) = true, want false; err = %v", tc.wantNotIs, err)
			}
			msg := err.Error()
			if got := strings.Contains(msg, grantPhrase); got != tc.wantGrantPhrase {
				t.Errorf("message contains %q = %v, want %v; msg = %q", grantPhrase, got, tc.wantGrantPhrase, msg)
			}
			if got := strings.Contains(msg, retryPhrase); got != tc.wantGrantPhrase {
				t.Errorf("message contains %q = %v, want %v; msg = %q", retryPhrase, got, tc.wantGrantPhrase, msg)
			}
			if got := strings.Contains(msg, provenPhrase); got != tc.wantProvenPhrase {
				t.Errorf("message contains %q = %v, want %v; msg = %q", provenPhrase, got, tc.wantProvenPhrase, msg)
			}
			for _, pane := range paneCopy {
				if strings.Contains(msg, pane) {
					t.Errorf("message carries backend-specific pane copy %q; msg = %q", pane, msg)
				}
			}
			if strings.Contains(msg, "symlink is used until then") {
				t.Errorf("message carries the stale symlink-fallback claim; msg = %q", msg)
			}
			if !strings.Contains(msg, dir) {
				t.Errorf("message does not name the account dir %q; msg = %q", dir, msg)
			}
			if tc.wantWaited && !strings.Contains(msg, waited.String()) {
				t.Errorf("message does not mention the %v wait; msg = %q", waited, msg)
			}
		})
	}
}

// TestMountFailureErr pins that a serve-exit (host.Mount returned before the
// mount came live) is a hard ErrMountFailed carrying no pending-grant copy,
// while a plain timeout still routes to the proven/unproven mountWaitErr split.
func TestMountFailureErr(t *testing.T) {
	const dir = "/pool/accounts/acct-06"
	const waited = 5 * time.Second
	t.Run("serve-exit is a hard failure, never a missing grant", func(t *testing.T) {
		err := mountFailureErr(dir, waited, true, false)
		if !errors.Is(err, ErrMountFailed) {
			t.Errorf("errors.Is(err, ErrMountFailed) = false, want true; err = %v", err)
		}
		if errors.Is(err, ErrMountNotLive) || errors.Is(err, ErrMountTimeout) {
			t.Errorf("serve-exit must not read as a mount-up timeout; err = %v", err)
		}
		if msg := err.Error(); strings.Contains(msg, "one-time OS volume-access grant") || strings.Contains(msg, "Network Volumes") || strings.Contains(msg, "System Settings") {
			t.Errorf("hard failure must not carry the pending-grant walkthrough; msg = %q", msg)
		}
		if !strings.Contains(err.Error(), dir) {
			t.Errorf("message does not name the account dir %q; err = %v", dir, err)
		}
	})
	t.Run("unproven timeout still reads as missing grant", func(t *testing.T) {
		err := mountFailureErr(dir, waited, false, false)
		if !errors.Is(err, ErrMountNotLive) || errors.Is(err, ErrMountFailed) {
			t.Errorf("unproven timeout must read as ErrMountNotLive, not ErrMountFailed; err = %v", err)
		}
	})
	t.Run("proven timeout still reads as transient", func(t *testing.T) {
		err := mountFailureErr(dir, waited, false, true)
		if !errors.Is(err, ErrMountTimeout) || errors.Is(err, ErrMountFailed) {
			t.Errorf("proven timeout must read as ErrMountTimeout, not ErrMountFailed; err = %v", err)
		}
	})
}

// TestStatProbesJoinsConcurrentDo pins that concurrent same-key Do calls
// collapse onto ONE in-flight stat goroutine — a wedged carcass parks at most
// one goroutine, never one per caller.
func TestStatProbesJoinsConcurrentDo(t *testing.T) {
	var probes StatProbes[int]
	const key = "/probe/wedged"

	release := make(chan struct{})
	// Both the happy path and the cleanup close release, so a mid-test Fatal
	// cannot strand the parked probe.
	var closeOnce sync.Once
	closeRelease := func() { closeOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		closeRelease()
		deadline := time.Now().Add(5 * time.Second)
		for probes.Inflight() != 0 {
			if time.Now().After(deadline) {
				t.Error("parked probe never drained")
				return
			}
			time.Sleep(time.Millisecond)
		}
	})

	var firstCalls atomic.Int32
	started := make(chan struct{})
	firstOK := make(chan bool, 1)
	verdict := make(chan int, 1)
	go func() {
		v, ok := probes.Do(key, 5*time.Second, func() int {
			firstCalls.Add(1)
			close(started)
			<-release // simulates a wedged carcass
			return 7
		})
		firstOK <- ok
		verdict <- v
	}()
	<-started // the probe is in-flight and parked under key

	if got := probes.Inflight(); got != 1 {
		t.Fatalf("Inflight while parked = %d, want 1", got)
	}

	const joiners = 8
	var joinerStat atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < joiners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := probes.Do(key, 20*time.Millisecond, func() int {
				joinerStat.Add(1) // must NEVER run: joiners share the parked probe
				return 99
			}); ok {
				t.Errorf("joiner Do ok=true, want false: the parked probe never answered within the bound")
			}
		}()
	}
	wg.Wait()

	if got := joinerStat.Load(); got != 0 {
		t.Fatalf("joiner stat invocations = %d, want 0 — concurrent same-key Do must join the parked probe, not spawn its own", got)
	}
	if got := firstCalls.Load(); got != 1 {
		t.Fatalf("underlying stat invocations = %d, want exactly 1 (joined, not re-spawned)", got)
	}
	if got := probes.Inflight(); got != 1 {
		t.Fatalf("Inflight after the joiners timed out = %d, want still 1 (the one parked probe)", got)
	}

	closeRelease()
	if ok := <-firstOK; !ok {
		t.Fatal("first Do ok=false, want true: the released stat answered within its bound")
	}
	if got := <-verdict; got != 7 {
		t.Fatalf("first Do verdict = %d, want 7 (the stat's return value)", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for probes.Inflight() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("Inflight after release = %d, want 0 (probe goroutine exited)", probes.Inflight())
		}
		time.Sleep(time.Millisecond)
	}
}
