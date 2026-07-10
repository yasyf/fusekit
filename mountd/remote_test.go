package mountd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/proc"
)

func discardLog() *log.Logger { return log.New(io.Discard, "", 0) }

// fakeLocalState swaps the localState seam for one test; mounted is AND-ed into
// alive as in production. Package-var seam: no parallel tests.
func fakeLocalState(t *testing.T, mounted func(dir string) bool, alive func(base, dir string) bool) {
	t.Helper()
	prev := localState
	localState = func(base, dir string) (bool, bool) {
		m := mounted(dir)
		return m, m && alive(base, dir)
	}
	t.Cleanup(func() { localState = prev })
}

// deadEndHost returns a RemoteHost for which any holder contact — RPC or
// spawn — fails, so a nil return from its methods proves the zero-RPC path.
func deadEndHost(t *testing.T) *RemoteHost {
	t.Helper()
	socket := filepath.Join(shortSockDir(t), "m.sock")
	return &RemoteHost{
		Socket:         socket,
		LogPath:        filepath.Join(t.TempDir(), "missing", "holder.log"),
		Args:           holderArgs(socket),
		SpawnTimeout:   time.Second,
		CannotHostHint: testHostHint,
	}
}

// TestRemoteHostSetupAdoptsLiveMountWithZeroRPC: shallow-live suffices to
// adopt — partial-wedge detection is the daemon's deep probe, not Setup's.
func TestRemoteHostSetupAdoptsLiveMountWithZeroRPC(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeLocalState(t, func(string) bool { return true }, func(string, string) bool { return true })

	if err := deadEndHost(t).Setup(base, dir); err != nil {
		t.Fatalf("Setup of an already-live mirror = %v, want nil (adopt, zero RPC)", err)
	}
}

func TestRemoteHostSetupMountsViaHolder(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeLocalState(t, func(string) bool { return false }, func(string, string) bool { return false })
	fake := &fakeHost{}
	_, cl, _, _ := startServer(t, fake)

	p := &RemoteHost{Socket: cl.Socket, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(cl.Socket)}
	if err := p.Setup(base, dir); err != nil {
		t.Fatalf("Setup = %v, want nil", err)
	}
	setups, teardowns := fake.calls()
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(setups, want) {
		t.Errorf("holder Setup calls = %v, want %v", setups, want)
	}
	if len(teardowns) != 0 {
		t.Errorf("holder Teardown calls = %v, want none", teardowns)
	}
}

func TestOverlayClassTranslation(t *testing.T) {
	plain := errors.New("no class at all")
	tests := []struct {
		name    string
		in      error
		wantIs  []error
		wantNot []error
	}{
		{
			name:    "TCC gains the fusekit mount-not-live identity",
			in:      fmt.Errorf("%w: grant pending", ErrTCCDenied),
			wantIs:  []error{ErrTCCDenied, fusekit.ErrMountNotLive},
			wantNot: []error{fusekit.ErrUnmountWedged, fusekit.ErrMountTimeout, fusekit.ErrMountFailed},
		},
		{
			// A proven grant must never pick up the TCC identity.
			name:    "mount-timeout gains the fusekit mount-timeout identity, never mount-not-live",
			in:      fmt.Errorf("%w: still settling", ErrMountTimeout),
			wantIs:  []error{ErrMountTimeout, fusekit.ErrMountTimeout},
			wantNot: []error{fusekit.ErrMountNotLive, fusekit.ErrUnmountWedged, fusekit.ErrMountFailed, ErrTCCDenied},
		},
		{
			name:    "wedged gains the fusekit wedged identity",
			in:      fmt.Errorf("%w: still mounted", ErrUnmountWedged),
			wantIs:  []error{ErrUnmountWedged, fusekit.ErrUnmountWedged},
			wantNot: []error{fusekit.ErrMountNotLive, fusekit.ErrMountTimeout, fusekit.ErrMountFailed},
		},
		{
			// A hard mount(2) rejection must never classify as presumed-TCC
			// mount-not-live — the serve-exit split.
			name:    "mount-failed gains the fusekit mount-failed identity, never mount-not-live",
			in:      fmt.Errorf("%w: boom", ErrMountFailed),
			wantIs:  []error{ErrMountFailed, fusekit.ErrMountFailed},
			wantNot: []error{fusekit.ErrMountNotLive, fusekit.ErrMountTimeout, fusekit.ErrUnmountWedged},
		},
		{
			name:    "classless error passes through untouched",
			in:      plain,
			wantIs:  []error{plain},
			wantNot: []error{fusekit.ErrMountNotLive, fusekit.ErrMountTimeout, fusekit.ErrMountFailed, fusekit.ErrUnmountWedged},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := overlayClass(tc.in)
			for _, want := range tc.wantIs {
				if !errors.Is(got, want) {
					t.Errorf("overlayClass(%v) = %v, want errors.Is %v", tc.in, got, want)
				}
			}
			for _, not := range tc.wantNot {
				if errors.Is(got, not) {
					t.Errorf("overlayClass(%v) = %v, want NOT errors.Is %v", tc.in, got, not)
				}
			}
		})
	}
}

func TestRemoteHostSetupTranslatesTCCClass(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeLocalState(t, func(string) bool { return false }, func(string, string) bool { return false })
	fake := &fakeHost{setupFn: func(string, string) error {
		return fmt.Errorf("mount did not come live: %w", fusekit.ErrMountNotLive)
	}}
	_, cl, _, _ := startServer(t, fake)

	p := &RemoteHost{Socket: cl.Socket, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(cl.Socket)}
	err := p.Setup(base, dir)
	if err == nil {
		t.Fatal("Setup with a TCC-blocked holder mount succeeded, want error")
	}
	if !errors.Is(err, ErrTCCDenied) {
		t.Errorf("error = %v, want errors.Is mountd.ErrTCCDenied", err)
	}
	if !errors.Is(err, fusekit.ErrMountNotLive) {
		t.Errorf("error = %v, want errors.Is fusekit.ErrMountNotLive", err)
	}
}

func TestRemoteHostSetupCarcassNeedsTeardownThenRetry(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	// Carcass: kernel-mounted, mirror dead, no row in the fresh holder's
	// registry. Teardown's registry-miss path is the designed clear.
	var stillMounted atomic.Bool
	stillMounted.Store(true)
	mounted := func(string) bool { return stillMounted.Load() }
	alive := func(string, string) bool { return false }
	fake := &fakeHost{teardownFn: func(string, string) error {
		stillMounted.Store(false)
		return nil
	}}
	setState(fake, mounted, alive)
	fakeLocalState(t, mounted, alive)
	_, cl, _, _ := startServer(t, fake)
	p := &RemoteHost{Socket: cl.Socket, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(cl.Socket)}

	err := p.Setup(base, dir)
	if !errors.Is(err, ErrForeignMount) {
		t.Fatalf("Setup against a carcass = %v, want errors.Is ErrForeignMount", err)
	}
	if err := p.Teardown(base, dir); err != nil {
		t.Fatalf("Teardown of the carcass = %v, want nil", err)
	}
	if err := p.Setup(base, dir); err != nil {
		t.Fatalf("Setup after clearing the carcass = %v, want nil", err)
	}
	setups, teardowns := fake.calls()
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(teardowns, want) {
		t.Errorf("holder Teardown calls = %v, want %v", teardowns, want)
	}
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(setups, want) {
		t.Errorf("holder Setup calls = %v, want %v", setups, want)
	}
}

func TestRemoteHostTeardownNotMountedIsNoOpWithZeroRPC(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeLocalState(t, func(string) bool { return false }, func(string, string) bool { return false })

	if err := deadEndHost(t).Teardown(base, dir); err != nil {
		t.Fatalf("Teardown of an unmounted dir = %v, want nil (no holder contact)", err)
	}
}

func TestRemoteHostTeardownUnmountsViaHolder(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{}
	fake.setLive(dir, true) // the holder's registry-miss carcass path serves it
	fakeLocalState(t, fake.isLive, func(_, dir string) bool { return fake.isLive(dir) })
	_, cl, _, _ := startServer(t, fake)

	p := &RemoteHost{Socket: cl.Socket, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(cl.Socket)}
	if err := p.Teardown(base, dir); err != nil {
		t.Fatalf("Teardown = %v, want nil", err)
	}
	_, teardowns := fake.calls()
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(teardowns, want) {
		t.Errorf("holder Teardown calls = %v, want %v", teardowns, want)
	}
}

func TestRemoteHostTeardownTranslatesWedgedClass(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	mounted := func(string) bool { return true }
	alive := func(string, string) bool { return true }
	// The holder's unmount wedges: its Teardown fails with
	// fusekit.ErrUnmountWedged, which crosses the wire as ClassWedged.
	fake := &fakeHost{teardownFn: func(string, string) error {
		return fmt.Errorf("umount refused: %w", fusekit.ErrUnmountWedged)
	}}
	setState(fake, mounted, alive)
	fakeLocalState(t, mounted, alive)
	_, cl, _, _ := startServer(t, fake)

	p := &RemoteHost{Socket: cl.Socket, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(cl.Socket)}
	err := p.Teardown(base, dir)
	if err == nil {
		t.Fatal("Teardown with a wedged holder unmount succeeded, want error")
	}
	// Both identities, exactly like the local re-verify path: a wedge must
	// classify the same regardless of which process detected it.
	if !errors.Is(err, ErrUnmountWedged) {
		t.Errorf("error = %v, want errors.Is mountd.ErrUnmountWedged", err)
	}
	if !errors.Is(err, fusekit.ErrUnmountWedged) {
		t.Errorf("error = %v, want errors.Is fusekit.ErrUnmountWedged", err)
	}
}

func TestRemoteHostTeardownReVerifiesAfterOKReply(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	// The holder's fake Teardown "succeeds" (OK reply on the wire), but the
	// local kernel seam keeps reporting a mountpoint — a lost-unmount skew the
	// provider must refuse to call a clean teardown.
	mounted := func(string) bool { return true }
	alive := func(string, string) bool { return true }
	fake := &fakeHost{}
	setState(fake, mounted, alive)
	fakeLocalState(t, mounted, alive)
	_, cl, _, _ := startServer(t, fake)

	p := &RemoteHost{Socket: cl.Socket, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(cl.Socket)}
	err := p.Teardown(base, dir)
	if err == nil {
		t.Fatal("Teardown with a still-mounted dir after an OK reply succeeded, want error")
	}
	if !errors.Is(err, fusekit.ErrUnmountWedged) {
		t.Errorf("error = %v, want errors.Is ErrUnmountWedged", err)
	}
	if !strings.Contains(err.Error(), "still a mountpoint") {
		t.Errorf("error = %q, want it to say the dir is still a mountpoint", err)
	}
	_, teardowns := fake.calls()
	if want := []hostCall{{base, dir}}; !reflect.DeepEqual(teardowns, want) {
		t.Errorf("holder Teardown calls = %v, want %v (the RPC must have landed)", teardowns, want)
	}
}

func TestRemoteHostTeardownMountedButHolderUnreachable(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fakeLocalState(t, func(string) bool { return true }, func(string, string) bool { return true })

	err := deadEndHost(t).Teardown(base, dir)
	if err == nil {
		t.Fatal("Teardown of a mounted dir with no reachable or spawnable holder succeeded, want error")
	}
	if !strings.Contains(err.Error(), "unmount "+dir) {
		t.Errorf("error = %q, want it wrapped with the unmount %s context", err, dir)
	}
}

func TestRemoteHostHealthAndSync(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	tests := []struct {
		name           string
		mounted, alive bool
		wantErr        string // empty means healthy
	}{
		{name: "mounted and live is healthy", mounted: true, alive: true},
		{name: "not mounted", mounted: false, alive: false, wantErr: "not a mountpoint"},
		{name: "not mounted trumps an alive-looking dir", mounted: false, alive: true, wantErr: "not a mountpoint"},
		{name: "mounted but dead mirror", mounted: true, alive: false, wantErr: "dead"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m, a := tc.mounted, tc.alive
			fakeLocalState(t, func(string) bool { return m }, func(string, string) bool { return a })
			p := deadEndHost(t) // Health and Sync are local-only: zero RPC

			for method, err := range map[string]error{
				"Health": p.Health(base, dir),
				"Sync":   p.Sync(base, dir),
			} {
				if tc.wantErr == "" {
					if err != nil {
						t.Errorf("%s = %v, want nil", method, err)
					}
					continue
				}
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("%s = %v, want error containing %q", method, err, tc.wantErr)
				}
				// A definitive dead reading answers fast and must NOT wrap the
				// timeout sentinel — that distinction is what the daemon debounces on.
				if errors.Is(err, fusekit.ErrLivenessTimeout) {
					t.Errorf("%s = %v, a definitive dead reading must not wrap ErrLivenessTimeout", method, err)
				}
			}
		})
	}
}

// TestRemoteHostHealthLivenessTimeout proves a liveness stat that does not answer
// within the bound wraps ErrLivenessTimeout — unresponsive but not proven dead
// (the saturated-holder shape the daemon debounces).
func TestRemoteHostHealthLivenessTimeout(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	shrinkLiveProbeTimeout(t, 20*time.Millisecond)
	block := make(chan struct{})
	// alive blocks past the bound, so the whole localState probe times out (probeMount !ok).
	fakeLocalState(t, func(string) bool { return true }, func(string, string) bool {
		<-block
		return true
	})
	t.Cleanup(func() { releaseProbes(t, block) })

	if err := deadEndHost(t).Health(base, dir); !errors.Is(err, fusekit.ErrLivenessTimeout) {
		t.Fatalf("Health on a timed-out probe = %v, want ErrLivenessTimeout", err)
	}
}

// fakeSpawnHolder swaps the spawnHolder seam for one test and reports how many
// times Converge invoked it; body models the upgrade (a successor on the same
// socket). Package-var seam: no parallel tests.
func fakeSpawnHolder(t *testing.T, body func(h *RemoteHost) error) (spawns func() int) {
	t.Helper()
	prev := spawnHolder
	var n atomic.Int64
	spawnHolder = func(h *RemoteHost) error {
		n.Add(1)
		return body(h)
	}
	t.Cleanup(func() { spawnHolder = prev })
	return func() int { return int(n.Load()) }
}

// spawnSuccessorServer stands the successor Server up on the retiring holder's
// socket: that holder frees Socket+".lock" only in Run's last defer — after
// wg.Wait, unmountAll, and the journal drain — so a successor racing it can
// read dead-socket + contended-flock and refuse with proc.ErrPeerStarting.
// Production absorbs that race in Retire (RetirePlan.spawn retries the
// transient sentinels; retire_test covers it); this in-process helper IS the
// spawn, so it retries Run itself rather than failing back to Retire, keeping
// the converge tests' spawns()==1 assertions deterministic.
func spawnSuccessorServer(t *testing.T, socket string, host Host, version string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cl := NewClient(socket)
	deadline := time.Now().Add(10 * time.Second)
	for {
		s := &Server{Socket: socket, Host: host, Version: version, Log: discardLog()}
		exited := make(chan error, 1)
		go func() { exited <- s.Run(ctx) }()
		retry := false
		for !retry {
			select {
			case err := <-exited:
				if !errors.Is(err, proc.ErrPeerStarting) {
					t.Fatalf("successor holder exited before serving: %v", err)
				}
				if time.Now().After(deadline) {
					t.Fatalf("successor holder never won the flock race from the retiring holder: %v", err)
				}
				retry = true
			default:
				if cl.Available() {
					return
				}
				if time.Now().After(deadline) {
					t.Fatal("successor holder socket never came up")
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// shrinkConvergeWaits shrinks the converge socket-release bounds for one test so
// the wedged-holder path does not burn the real 5s+2s. Package-var seam: no
// parallel tests.
func shrinkConvergeWaits(t *testing.T, d time.Duration) {
	t.Helper()
	prevGone, prevKill := convergeWaitGone, convergeKillWait
	convergeWaitGone, convergeKillWait = d, d
	t.Cleanup(func() { convergeWaitGone, convergeKillWait = prevGone, prevKill })
}

// TestRemoteHostConvergeDisabledIsNoOp: an empty Version disables converge
// entirely — no Poll, no Shutdown, no respawn — even against a live holder.
func TestRemoteHostConvergeDisabledIsNoOp(t *testing.T) {
	fake := &fakeHost{}
	_, cl, _, _ := startServer(t, fake)
	spawns := fakeSpawnHolder(t, func(*RemoteHost) error {
		t.Fatal("spawnHolder called with converge disabled")
		return nil
	})

	h := &RemoteHost{Socket: cl.Socket, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(cl.Socket)}
	if err := h.Converge(context.Background()); err != nil {
		t.Fatalf("Converge with Version=\"\" = %v, want nil", err)
	}
	if spawns() != 0 {
		t.Errorf("spawnHolder invoked %d times, want 0", spawns())
	}
	if ver, herr := cl.Health(); herr != nil || ver != testVersion {
		t.Errorf("holder Health = (%q, %v), want it untouched at %q", ver, herr, testVersion)
	}
}

// TestRemoteHostConvergeSameVersionIsNoOp: a holder already at the consumer's
// version is the settled path — Converge polls once and stops, never retiring or
// respawning.
func TestRemoteHostConvergeSameVersionIsNoOp(t *testing.T) {
	fake := &fakeHost{}
	_, cl, _, _ := startServer(t, fake)
	spawns := fakeSpawnHolder(t, func(*RemoteHost) error {
		t.Fatal("spawnHolder called for a settled same-version holder")
		return nil
	})

	h := &RemoteHost{Socket: cl.Socket, Version: testVersion, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(cl.Socket)}
	if err := h.Converge(context.Background()); err != nil {
		t.Fatalf("Converge against a same-version holder = %v, want nil", err)
	}
	if spawns() != 0 {
		t.Errorf("spawnHolder invoked %d times, want 0 (settled)", spawns())
	}
	setups, teardowns := fake.calls()
	if len(setups) != 0 || len(teardowns) != 0 {
		t.Errorf("holder calls = (setups %v, teardowns %v), want none — Poll only", setups, teardowns)
	}
	if ver, herr := cl.Health(); herr != nil || ver != testVersion {
		t.Errorf("holder Health = (%q, %v), want it still alive at %q", ver, herr, testVersion)
	}
}

// TestRemoteHostConvergeUnreachableIsNoOp: with no reachable holder there is
// nothing to converge — the caller's subsequent Setup spawns a fresh one — so
// Converge returns nil without attempting a respawn.
func TestRemoteHostConvergeUnreachableIsNoOp(t *testing.T) {
	spawns := fakeSpawnHolder(t, func(*RemoteHost) error {
		t.Fatal("spawnHolder called against an unreachable holder")
		return nil
	})

	h := deadEndHost(t)
	h.Version = "v8.8.8 (upgraded)"
	if err := h.Converge(context.Background()); err != nil {
		t.Fatalf("Converge against an unreachable holder = %v, want nil", err)
	}
	if spawns() != 0 {
		t.Errorf("spawnHolder invoked %d times, want 0", spawns())
	}
}

// TestRemoteHostConvergeDegradedIsSpared: a holder alive at a skewed version but
// whose List failed (Degraded) is SPARED — its live-mount set is unreadable, so
// retiring it would lose the (base, dir) pairs the converge must remount.
func TestRemoteHostConvergeDegradedIsSpared(t *testing.T) {
	healthOK := `{"proto":1,"ok":true,"version":"` + testVersion + `"}`
	// A malformed List reply is a deterministic List failure regardless of
	// scheduler load — Health answers OK, so Poll reads a reachable, skewed,
	// degraded holder and the Degraded arm spares it before any retire leg.
	socket, requests := startRawHolder(t, func(req string) string {
		if strings.Contains(req, `"op":"health"`) {
			return healthOK
		}
		return `{"proto":1,"ok":false,"error":"list unavailable"}`
	})
	spawns := fakeSpawnHolder(t, func(*RemoteHost) error {
		t.Fatal("spawnHolder called for a degraded holder we must spare")
		return nil
	})

	h := &RemoteHost{Socket: socket, Version: "v8.8.8 (upgraded)", LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(socket)}
	if err := h.Converge(context.Background()); err != nil {
		t.Fatalf("Converge against a degraded holder = %v, want nil (spared)", err)
	}
	if spawns() != 0 {
		t.Errorf("spawnHolder invoked %d times, want 0 (degraded holder spared)", spawns())
	}
	for _, req := range requests() {
		if strings.Contains(req, `"op":"shutdown"`) {
			t.Fatalf("a degraded holder was sent Shutdown; requests = %v", requests())
		}
	}
}

// TestRemoteHostConvergeSkewReplacesAndRemountsAll: a shared holder serving two
// mounts meets a consumer at a differing Version. Converge retires the stale
// holder, respawns the consumer's binary, and remounts BOTH (base, dir) pairs so
// the other shared repos come back.
func TestRemoteHostConvergeSkewReplacesAndRemountsAll(t *testing.T) {
	const baseA, dirA = "/pool/base-a", "/pool/acct-a"
	const baseB, dirB = "/pool/base-b", "/pool/acct-b"
	const newVersion = "v9.9.10 (upgraded)"

	stale := &fakeHost{}
	socket := filepath.Join(shortSockDir(t), "m.sock")
	_, cl, staleDone, _ := startServerAt(t, stale, socket)
	if err := cl.Mount(baseA, dirA); err != nil {
		t.Fatalf("seed Mount A: %v", err)
	}
	if err := cl.Mount(baseB, dirB); err != nil {
		t.Fatalf("seed Mount B: %v", err)
	}

	// The successor: a fresh fakeHost reporting the NEW version on the SAME
	// socket — the upgrade. spawnHolder stands it up in place of the retired one.
	successor := &fakeHost{}
	spawns := fakeSpawnHolder(t, func(h *RemoteHost) error {
		spawnSuccessorServer(t, h.Socket, successor, newVersion)
		return nil
	})

	h := &RemoteHost{Socket: socket, Version: newVersion, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(socket)}
	if err := h.Converge(context.Background()); err != nil {
		t.Fatalf("Converge over a version-skewed holder = %v, want nil", err)
	}

	// The stale holder's Run must have exited (Shutdown cancelled its ctx).
	select {
	case <-staleDone:
	case <-time.After(5 * time.Second):
		t.Fatal("stale holder did not exit after Converge retired it")
	}
	if spawns() != 1 {
		t.Errorf("spawnHolder invoked %d times, want exactly 1", spawns())
	}
	setups, _ := successor.calls()
	want := []hostCall{{baseA, dirA}, {baseB, dirB}}
	if !sameCalls(setups, want) {
		t.Errorf("successor Setup calls = %v, want %v (both shared repos remounted)", setups, want)
	}
	if ver, herr := NewClient(socket).Health(); herr != nil || ver != newVersion {
		t.Errorf("post-converge Health = (%q, %v), want %q", ver, herr, newVersion)
	}
}

// TestRemoteHostConvergeWedgedHolderIsReaped: a stale holder that acks Shutdown
// but keeps its socket triggers the peer-gated Kill, and the successor still
// comes up. Peer seams record the kill without signalling a real process.
func TestRemoteHostConvergeWedgedHolderIsReaped(t *testing.T) {
	const newVersion = "v9.9.11 (upgraded)"
	healthOK := `{"proto":1,"ok":true,"version":"` + testVersion + `"}`
	listOK := `{"proto":1,"ok":true,"mounts":[]}`
	shutdownOK := `{"proto":1,"ok":true}`
	// A wedged holder: it answers every op (so Poll sees a reachable, skewed,
	// non-degraded holder and Shutdown acks) but never releases its socket.
	socket, _ := startRawHolder(t, func(req string) string {
		switch {
		case strings.Contains(req, `"op":"health"`):
			return healthOK
		case strings.Contains(req, `"op":"list"`):
			return listOK
		case strings.Contains(req, `"op":"shutdown"`):
			return shutdownOK
		default:
			return `{"proto":1,"ok":true}`
		}
	})

	const wedgedPID = 991234
	var killed killCall
	setPeerSeams(t,
		func(string) (int, error) { return wedgedPID, nil },
		func(pid int, sig syscall.Signal) error { killed = killCall{pid, sig}; return nil })

	// The wedged raw holder never frees the socket, so the override does not bind
	// one — it just reports the respawn (no mounts to replay here, so no socket
	// contention).
	successorUp := false
	spawns := fakeSpawnHolder(t, func(*RemoteHost) error {
		successorUp = true
		return nil
	})

	shrinkConvergeWaits(t, 10*time.Millisecond)

	h := &RemoteHost{Socket: socket, Version: newVersion, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(socket)}
	if err := h.Converge(context.Background()); err != nil {
		t.Fatalf("Converge over a wedged skewed holder = %v, want nil", err)
	}
	if killed.pid != wedgedPID || killed.sig != syscall.SIGKILL {
		t.Errorf("kill = %+v, want SIGKILL to the wedged peer %d", killed, wedgedPID)
	}
	if spawns() != 1 {
		t.Errorf("spawnHolder invoked %d times, want exactly 1 (successor still comes up)", spawns())
	}
	if !successorUp {
		t.Error("successor was not brought up after the wedged holder was reaped")
	}
}

// TestRemoteHostConvergeWedgedSparesSuccessor: the concurrent-converge race. The
// wedged holder's pid is captured before the graceful wait, but a legitimate
// successor rebinds the socket during that wait. The reap must re-read the
// socket's CURRENT peer at kill time, see it mismatches the captured wedged pid,
// and refuse — never signalling the successor.
func TestRemoteHostConvergeWedgedSparesSuccessor(t *testing.T) {
	const newVersion = "v9.9.13 (upgraded)"
	healthOK := `{"proto":1,"ok":true,"version":"` + testVersion + `"}`
	listOK := `{"proto":1,"ok":true,"mounts":[]}`
	shutdownOK := `{"proto":1,"ok":true}`
	socket, _ := startRawHolder(t, func(req string) string {
		switch {
		case strings.Contains(req, `"op":"health"`):
			return healthOK
		case strings.Contains(req, `"op":"list"`):
			return listOK
		case strings.Contains(req, `"op":"shutdown"`):
			return shutdownOK
		default:
			return `{"proto":1,"ok":true}`
		}
	})

	const wedgedPID = 992001
	const successorPID = 992002
	var peerCalls atomic.Int64
	var killed killCall
	setPeerSeams(t,
		func(string) (int, error) {
			// First resolve is the pre-wait capture (the wedged holder); every later
			// resolve — the kill-time re-resolve — sees the successor that rebound it.
			if peerCalls.Add(1) == 1 {
				return wedgedPID, nil
			}
			return successorPID, nil
		},
		func(pid int, sig syscall.Signal) error { killed = killCall{pid, sig}; return nil })

	spawns := fakeSpawnHolder(t, func(*RemoteHost) error { return nil })

	shrinkConvergeWaits(t, 10*time.Millisecond)

	h := &RemoteHost{Socket: socket, Version: newVersion, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(socket)}
	if err := h.Converge(context.Background()); err != nil {
		t.Fatalf("Converge over a wedged holder a successor rebound = %v, want nil", err)
	}
	if killed.pid == successorPID {
		t.Fatalf("the successor pid %d was signalled; KillPeer must refuse the mismatched peer (kill = %+v)", successorPID, killed)
	}
	if killed.pid != 0 {
		t.Errorf("kill = %+v, want no signal sent (a mismatched successor is refused)", killed)
	}
	if spawns() != 1 {
		t.Errorf("spawnHolder invoked %d times, want exactly 1", spawns())
	}
}

// TestRemoteHostConvergeRemountBestEffort: one snapshotted dir's remount fails
// on the successor; Converge still spawns and remounts the others, and returns a
// non-nil joined error naming the failed dir — not a hard failure.
func TestRemoteHostConvergeRemountBestEffort(t *testing.T) {
	const baseA, dirA = "/pool/base-a", "/pool/acct-a"
	const baseB, dirB = "/pool/base-b", "/pool/acct-b"
	const newVersion = "v9.9.12 (upgraded)"

	stale := &fakeHost{}
	socket := filepath.Join(shortSockDir(t), "m.sock")
	_, cl, _, _ := startServerAt(t, stale, socket)
	if err := cl.Mount(baseA, dirA); err != nil {
		t.Fatalf("seed Mount A: %v", err)
	}
	if err := cl.Mount(baseB, dirB); err != nil {
		t.Fatalf("seed Mount B: %v", err)
	}

	successor := &fakeHost{setupFn: func(_, dir string) error {
		if dir == dirA {
			return fmt.Errorf("mount %s refused: %w", dir, fusekit.ErrMountFailed)
		}
		return nil
	}}
	spawns := fakeSpawnHolder(t, func(h *RemoteHost) error {
		spawnSuccessorServer(t, h.Socket, successor, newVersion)
		return nil
	})

	h := &RemoteHost{Socket: socket, Version: newVersion, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(socket)}
	err := h.Converge(context.Background())
	if err == nil {
		t.Fatal("Converge with a failed remount = nil, want a non-nil joined remount error")
	}
	if !strings.Contains(err.Error(), dirA) {
		t.Errorf("joined remount error = %q, want it to name the failed dir %s", err, dirA)
	}
	if strings.Contains(err.Error(), dirB) {
		t.Errorf("joined remount error = %q, want it to NOT name the succeeded dir %s", err, dirB)
	}
	if spawns() != 1 {
		t.Errorf("spawnHolder invoked %d times, want exactly 1 (a remount failure is not a hard failure)", spawns())
	}
	setups, _ := successor.calls()
	if !containsCall(setups, hostCall{baseB, dirB}) {
		t.Errorf("successor Setup calls = %v, want them to include the succeeded remount %v", setups, hostCall{baseB, dirB})
	}
}

// TestRemoteHostConvergeSkipsMuxSubtrees pins that Converge SKIPS mux subtree
// rows: a MountInfo carries neither their MuxRoot wiring nor content-bridge
// fields, so re-issuing them as a plain Mount would serve wrong (raw-Base) bytes.
// Converge remounts the plain rows the shared holder served, never re-issues the
// mux row, and returns a joined error naming the skipped subtree.
func TestRemoteHostConvergeSkipsMuxSubtrees(t *testing.T) {
	const basePlain, dirPlain = "/pool/base-p", "/pool/acct-p"
	const base, root, muxDir = "/pool/base", "/pool/mnt", "/pool/mnt/acct-m"
	const newVersion = "v9.9.16 (upgraded)"

	stale := &fakeHost{}
	socket := filepath.Join(shortSockDir(t), "m.sock")
	_, cl, _, _ := startServerAt(t, stale, socket)
	if err := cl.Mount(basePlain, dirPlain); err != nil {
		t.Fatalf("seed plain Mount: %v", err)
	}
	if err := cl.AddMount(fusekit.MountSpec{Base: base, Dir: muxDir, MuxRoot: root}); err != nil {
		t.Fatalf("seed mux AddMount: %v", err)
	}

	successor := &fakeHost{}
	spawns := fakeSpawnHolder(t, func(h *RemoteHost) error {
		spawnSuccessorServer(t, h.Socket, successor, newVersion)
		return nil
	})
	setConvergeClearCarcass(t, func(string) {}) // carcass-clear is a separate concern here

	h := &RemoteHost{Socket: socket, Version: newVersion, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(socket)}
	err := h.Converge(context.Background())
	if err == nil {
		t.Fatal("Converge with a mux row = nil, want a joined error recording the skip")
	}
	if !strings.Contains(err.Error(), muxDir) {
		t.Errorf("joined error = %q, want it to name the skipped mux subtree %s", err, muxDir)
	}
	if spawns() != 1 {
		t.Errorf("spawnHolder invoked %d times, want exactly 1", spawns())
	}
	setups, _ := successor.calls()
	if !containsCall(setups, hostCall{basePlain, dirPlain}) {
		t.Errorf("successor Setup calls = %v, want the plain row remounted", setups)
	}
	for _, c := range setups {
		if c.dir == muxDir {
			t.Errorf("successor got a Setup for mux subtree %s; Converge must skip it (would serve raw-Base bytes)", muxDir)
		}
	}
}

// setConvergeClearCarcass swaps the convergeClearCarcass seam for one test so a
// converge test records carcass-clear calls without a real unmount. Package-var
// seam: no parallel tests.
func setConvergeClearCarcass(t *testing.T, fn func(dir string)) {
	t.Helper()
	prev := convergeClearCarcass
	convergeClearCarcass = fn
	t.Cleanup(func() { convergeClearCarcass = prev })
}

// TestRemoteHostConvergePIDCapturedBeforeShutdown: the wedged holder's pid is
// resolved (PeerPID) BEFORE Shutdown is sent, so a successor that rebinds during
// the graceful wait is later refused by KillPeer rather than shot.
func TestRemoteHostConvergePIDCapturedBeforeShutdown(t *testing.T) {
	const newVersion = "v9.9.14 (upgraded)"
	healthOK := `{"proto":1,"ok":true,"version":"` + testVersion + `"}`
	listOK := `{"proto":1,"ok":true,"mounts":[]}`

	var mu sync.Mutex
	var events []string
	record := func(ev string) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	}

	var h *closableHolder
	h = newClosableHolder(t, func(req string) string {
		switch {
		case strings.Contains(req, `"op":"health"`):
			return healthOK
		case strings.Contains(req, `"op":"list"`):
			return listOK
		case strings.Contains(req, `"op":"shutdown"`):
			record("shutdown")
			h.Close() // step down cleanly so WaitGone reports gone — no reap, one peerPID call
			return `{"proto":1,"ok":true}`
		default:
			return `{"proto":1,"ok":true}`
		}
	})

	const wedgedPID = 993001
	setPeerSeams(t,
		func(string) (int, error) { record("peerpid"); return wedgedPID, nil },
		func(int, syscall.Signal) error { t.Fatal("a clean step-down must not be reaped"); return nil })
	spawns := fakeSpawnHolder(t, func(*RemoteHost) error { return nil })
	setConvergeClearCarcass(t, func(string) {})
	shrinkConvergeWaits(t, 2*time.Second)

	h2 := &RemoteHost{Socket: h.socket, Version: newVersion, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(h.socket)}
	if err := h2.Converge(context.Background()); err != nil {
		t.Fatalf("Converge over a skewed holder = %v, want nil", err)
	}
	if spawns() != 1 {
		t.Errorf("spawnHolder invoked %d times, want exactly 1", spawns())
	}
	mu.Lock()
	defer mu.Unlock()
	peerIdx, shutIdx := -1, -1
	for i, e := range events {
		if e == "peerpid" && peerIdx == -1 {
			peerIdx = i
		}
		if e == "shutdown" && shutIdx == -1 {
			shutIdx = i
		}
	}
	if peerIdx == -1 || shutIdx == -1 {
		t.Fatalf("missing event (peerpid=%d shutdown=%d) in %v", peerIdx, shutIdx, events)
	}
	if peerIdx >= shutIdx {
		t.Errorf("peerPID resolve at %d did not precede the shutdown request at %d (events %v); the pid must be captured BEFORE Shutdown", peerIdx, shutIdx, events)
	}
}

// TestRemoteHostConvergeClearsCarcassesBeforeRemount pins the
// carcass-clear-before-remount invariant: for every dir the recorded
// ClearCarcass(dir) must precede the successor's remount Mount(base, dir).
func TestRemoteHostConvergeClearsCarcassesBeforeRemount(t *testing.T) {
	const baseA, dirA = "/pool/base-a", "/pool/acct-a"
	const baseB, dirB = "/pool/base-b", "/pool/acct-b"
	const newVersion = "v9.9.15 (upgraded)"

	stale := &fakeHost{}
	socket := filepath.Join(shortSockDir(t), "m.sock")
	_, cl, _, _ := startServerAt(t, stale, socket)
	if err := cl.Mount(baseA, dirA); err != nil {
		t.Fatalf("seed Mount A: %v", err)
	}
	if err := cl.Mount(baseB, dirB); err != nil {
		t.Fatalf("seed Mount B: %v", err)
	}

	var mu sync.Mutex
	var events []hostEvent
	record := func(kind, dir string) {
		mu.Lock()
		events = append(events, hostEvent{kind, dir})
		mu.Unlock()
	}
	setConvergeClearCarcass(t, func(dir string) { record("unmount", dir) })

	successor := &fakeHost{setupFn: func(_, dir string) error { record("remount", dir); return nil }}
	spawns := fakeSpawnHolder(t, func(h *RemoteHost) error {
		spawnSuccessorServer(t, h.Socket, successor, newVersion)
		return nil
	})

	h := &RemoteHost{Socket: socket, Version: newVersion, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(socket)}
	if err := h.Converge(context.Background()); err != nil {
		t.Fatalf("Converge over a skewed holder = %v, want nil", err)
	}
	if spawns() != 1 {
		t.Errorf("spawnHolder invoked %d times, want exactly 1", spawns())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, dir := range []string{dirA, dirB} {
		ui, ri := -1, -1
		for i, e := range events {
			if e.dir == dir && e.kind == "unmount" {
				ui = i
			}
			if e.dir == dir && e.kind == "remount" {
				ri = i
			}
		}
		if ui == -1 || ri == -1 {
			t.Fatalf("dir %s missing an event (unmount=%d remount=%d) in %v", dir, ui, ri, events)
		}
		if ui >= ri {
			t.Errorf("dir %s: ClearCarcass at %d did not precede the remount at %d (carcass-clear-before-remount); events %v", dir, ui, ri, events)
		}
	}
}

// TestRemoteHostConvergeDeferPolicyNeverCleared pins the end-to-end defer
// path: a stale holder's List carries each row's CarcassPolicy, and Converge
// never passes a defer-policy dir to the carcass clear — only the force row's.
func TestRemoteHostConvergeDeferPolicyNeverCleared(t *testing.T) {
	const baseF, dirF = "/pool/base-f", "/pool/acct-f"
	const baseD, dirD = "/pool/base-d", "/pool/acct-d"
	const newVersion = "v9.9.17 (upgraded)"

	stale := &fakeHost{}
	socket := filepath.Join(shortSockDir(t), "m.sock")
	_, cl, _, _ := startServerAt(t, stale, socket)
	if err := cl.Mount(baseF, dirF); err != nil {
		t.Fatalf("seed force Mount: %v", err)
	}
	if err := cl.AddMount(fusekit.MountSpec{Base: baseD, Dir: dirD, CarcassPolicy: fusekit.CarcassPolicyDefer}); err != nil {
		t.Fatalf("seed defer AddMount: %v", err)
	}

	var mu sync.Mutex
	var cleared []string
	setConvergeClearCarcass(t, func(dir string) { mu.Lock(); cleared = append(cleared, dir); mu.Unlock() })

	successor := &fakeHost{}
	spawns := fakeSpawnHolder(t, func(h *RemoteHost) error {
		spawnSuccessorServer(t, h.Socket, successor, newVersion)
		return nil
	})

	h := &RemoteHost{Socket: socket, Version: newVersion, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(socket)}
	if err := h.Converge(context.Background()); err != nil {
		t.Fatalf("Converge over a skewed holder = %v, want nil", err)
	}
	if spawns() != 1 {
		t.Errorf("spawnHolder invoked %d times, want exactly 1", spawns())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, d := range cleared {
		if d == dirD {
			t.Fatalf("ClearCarcass targeted defer-policy dir %s; the wire CarcassPolicy must reach Retire", dirD)
		}
	}
	if want := []string{dirF}; !equalStrs(cleared, want) {
		t.Errorf("ClearCarcass dirs = %v, want exactly %v", cleared, want)
	}
	setups, _ := successor.calls()
	if !containsCall(setups, hostCall{baseD, dirD}) {
		t.Errorf("successor Setup calls = %v, want the defer row remounted too (%v)", setups, hostCall{baseD, dirD})
	}
}

// hostEvent is one ordered side-effect (a carcass-clear or a remount) the
// converge ordering test records to assert the invariant.
type hostEvent struct{ kind, dir string }

// sameCalls reports whether got and want hold the same hostCalls regardless of
// order — Converge remounts in poll.Mounts order, which handleList sorts by dir,
// but the assertion should not be brittle to that.
func sameCalls(got, want []hostCall) bool {
	if len(got) != len(want) {
		return false
	}
	for _, w := range want {
		if !containsCall(got, w) {
			return false
		}
	}
	return true
}

func containsCall(calls []hostCall, want hostCall) bool {
	for _, c := range calls {
		if c == want {
			return true
		}
	}
	return false
}

// Copies the test binary into a temp dir; the copy is never executed.
func TestRemoteHostRefreshStableExe(t *testing.T) {
	dir := t.TempDir()
	h := &RemoteHost{Args: []string{"mount-holder"}, StableExecDir: dir}

	changed, err := h.RefreshStableExe()
	if err != nil {
		t.Fatalf("RefreshStableExe: %v", err)
	}
	if !changed {
		t.Error("changed = false, want true for a fresh stable copy")
	}
	fi, err := os.Stat(filepath.Join(dir, "mount-holder"))
	if err != nil {
		t.Fatalf("stat stable copy: %v", err)
	}
	if fi.Mode()&0o111 == 0 {
		t.Errorf("stable copy mode = %v, want executable", fi.Mode())
	}
	if changed, err := h.RefreshStableExe(); err != nil || changed {
		t.Errorf("second refresh: changed=%v err=%v, want false, nil", changed, err)
	}
}
