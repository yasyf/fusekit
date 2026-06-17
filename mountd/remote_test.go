package mountd

import (
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
)

// fakeLocalState swaps RemoteHost's local-kernel state seam for one test,
// restoring it after. It mirrors the production localState (alive AND-ed with
// mounted), so a not-mounted dir reads not-alive regardless of the alive func.
// Tests using it must not run in parallel (the seam is a package var).
func fakeLocalState(t *testing.T, mounted func(dir string) bool, alive func(base, dir string) bool) {
	t.Helper()
	prev := localState
	localState = func(base, dir string) (bool, bool) {
		m := mounted(dir)
		return m, m && alive(base, dir)
	}
	t.Cleanup(func() { localState = prev })
}

// deadEndHost returns a RemoteHost whose socket has no listener and whose log
// path cannot be opened, so ANY holder contact — an RPC or a spawn attempt —
// fails loudly in every build variant. A nil return from its methods proves the
// zero-RPC path was taken.
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

// TestRemoteHostSetupAdoptsLiveMountWithZeroRPC: a shallow-live mirror is
// adopted with zero RPC — the holder kept serving it across a daemon restart.
// Detecting a partial wedge is the daemon's job (its own deep probe), not
// Setup's, so Setup does no deep read here.
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
			wantNot: []error{fusekit.ErrUnmountWedged, fusekit.ErrMountTimeout},
		},
		{
			// The honest-timeout class: a proven grant means it must NEVER pick
			// up the TCC identity — that polarity is the whole point.
			name:    "mount-timeout gains the fusekit mount-timeout identity, never mount-not-live",
			in:      fmt.Errorf("%w: still settling", ErrMountTimeout),
			wantIs:  []error{ErrMountTimeout, fusekit.ErrMountTimeout},
			wantNot: []error{fusekit.ErrMountNotLive, fusekit.ErrUnmountWedged, ErrTCCDenied},
		},
		{
			name:    "wedged gains the fusekit wedged identity",
			in:      fmt.Errorf("%w: still mounted", ErrUnmountWedged),
			wantIs:  []error{ErrUnmountWedged, fusekit.ErrUnmountWedged},
			wantNot: []error{fusekit.ErrMountNotLive, fusekit.ErrMountTimeout},
		},
		{
			name:    "mount-failed has no fusekit equivalent",
			in:      fmt.Errorf("%w: boom", ErrMountFailed),
			wantIs:  []error{ErrMountFailed},
			wantNot: []error{fusekit.ErrMountNotLive, fusekit.ErrMountTimeout, fusekit.ErrUnmountWedged},
		},
		{
			name:    "classless error passes through untouched",
			in:      plain,
			wantIs:  []error{plain},
			wantNot: []error{fusekit.ErrMountNotLive, fusekit.ErrMountTimeout, fusekit.ErrUnmountWedged},
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
	// The holder's mount never comes live (the TCC grant case): its Setup
	// fails with fusekit.ErrMountNotLive, which crosses the wire as ClassTCC.
	fake := &fakeHost{setupFn: func(string, string) error {
		return fmt.Errorf("mount did not come live: %w", fusekit.ErrMountNotLive)
	}}
	_, cl, _, _ := startServer(t, fake)

	p := &RemoteHost{Socket: cl.Socket, LogPath: filepath.Join(t.TempDir(), "holder.log"), Args: holderArgs(cl.Socket)}
	err := p.Setup(base, dir)
	if err == nil {
		t.Fatal("Setup with a TCC-blocked holder mount succeeded, want error")
	}
	// Both identities must hold: the wire sentinel for mountd-aware callers
	// AND the fusekit sentinel — overlayClass promises classification across
	// the process boundary.
	if !errors.Is(err, ErrTCCDenied) {
		t.Errorf("error = %v, want errors.Is mountd.ErrTCCDenied", err)
	}
	if !errors.Is(err, fusekit.ErrMountNotLive) {
		t.Errorf("error = %v, want errors.Is fusekit.ErrMountNotLive", err)
	}
}

func TestRemoteHostSetupCarcassNeedsTeardownThenRetry(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	// A dead HOLDER's carcass: dir is still a mountpoint per the kernel but
	// the mirror is dead, and the fresh holder's registry has no row for it.
	// Setup must refuse it as foreign (the holder never stacks mounts); the
	// documented recovery is Teardown — whose registry-miss path clears the
	// carcass — then a Setup retry.
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
			}
		})
	}
}
