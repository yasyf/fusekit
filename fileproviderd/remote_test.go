package fileproviderd

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// newRemoteHost wires a RemoteDomainHost at the fake app's socket, stubbing
// launchApp to a no-op (the app already serves, so EnsureRunning short-circuits
// and never launches) and setting AppPath so the spawn arm is non-empty.
func newRemoteHost(t *testing.T, a *fakeApp) *RemoteDomainHost {
	t.Helper()
	withLaunchApp(t, func(context.Context, string) error { return nil })
	return &RemoteDomainHost{
		AppPath:       "/Apps/CCPoolStatus.app",
		ControlSocket: a.socket,
		SpawnTimeout:  time.Second,
	}
}

// TestRemoteEnsure pins that Ensure registers the domain and returns its
// user-visible root.
func TestRemoteEnsure(t *testing.T) {
	a := startFakeApp(t)
	a.setRegister(func(domain string) Response { return Response{OK: true, Path: "/cloud/" + domain} })
	h := newRemoteHost(t, a)

	path, err := h.Ensure(context.Background(), "acct-01")
	if err != nil {
		t.Fatalf("Ensure = %v, want nil", err)
	}
	if path != "/cloud/acct-01" {
		t.Fatalf("Ensure path = %q, want /cloud/acct-01", path)
	}
	// Exactly one register hit the app (the socket was already live, no probe).
	seen := a.seen()
	if len(seen) != 1 || seen[0].Op != OpRegister {
		t.Fatalf("app saw %+v, want one register", seen)
	}
}

// TestRemoteEnsureRetreatsOnNoEntitlement pins that a domain register the OS
// refuses for a missing entitlement surfaces ErrCannotControl — the ONLY
// condition that retreats an account.
func TestRemoteEnsureRetreatsOnNoEntitlement(t *testing.T) {
	a := startFakeApp(t)
	a.setRegister(func(string) Response {
		return Response{OK: false, ErrClass: ClassNoEntitlement, Error: "enable the extension"}
	})
	h := newRemoteHost(t, a)

	_, err := h.Ensure(context.Background(), "acct-01")
	if !errors.Is(err, ErrCannotControl) {
		t.Fatalf("Ensure err = %v, want errors.Is ErrCannotControl (the retreat condition)", err)
	}
	if errors.Is(err, ErrAppUnavailable) {
		t.Errorf("Ensure err = %v, want the retreat NOT confused with the transient blip", err)
	}
}

// TestRemoteEnsureTransientOnRegisterFailed pins that a non-entitlement
// register rejection is transient, never the retreat.
func TestRemoteEnsureTransientOnRegisterFailed(t *testing.T) {
	a := startFakeApp(t)
	a.setRegister(func(string) Response {
		return Response{OK: false, ErrClass: ClassRegisterFailed, Error: "duplicate"}
	})
	h := newRemoteHost(t, a)

	_, err := h.Ensure(context.Background(), "acct-01")
	if !errors.Is(err, ErrRegisterFailed) {
		t.Fatalf("Ensure err = %v, want errors.Is ErrRegisterFailed", err)
	}
	if errors.Is(err, ErrCannotControl) {
		t.Errorf("Ensure err = %v, want a transient register failure NOT the retreat", err)
	}
}

// TestRemoteRemove pins that Remove deregisters the domain.
func TestRemoteRemove(t *testing.T) {
	a := startFakeApp(t)
	a.setResponse(OpRemove, Response{OK: true})
	h := newRemoteHost(t, a)

	if err := h.Remove(context.Background(), "acct-01"); err != nil {
		t.Fatalf("Remove = %v, want nil", err)
	}
	seen := a.seen()
	if len(seen) != 1 || seen[0].Op != OpRemove || seen[0].Domain != "acct-01" {
		t.Fatalf("app saw %+v, want one remove of acct-01", seen)
	}
}

// TestRemoteSignalDoesNotSpawn pins that Signal goes straight to the socket
// (never spawns) and, against a dead socket, reports transient ErrAppUnavailable.
func TestRemoteSignalDoesNotSpawn(t *testing.T) {
	t.Run("live app signals", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpSignal, Response{OK: true})
		h := newRemoteHost(t, a)
		if err := h.Signal(context.Background(), "acct-01"); err != nil {
			t.Fatalf("Signal = %v, want nil", err)
		}
	})
	t.Run("dead app is transient, no spawn", func(t *testing.T) {
		socket := filepath.Join(shortSockDir(t), "absent.sock")
		var launched bool
		withLaunchApp(t, func(context.Context, string) error { launched = true; return nil })
		h := &RemoteDomainHost{AppPath: "/Apps/X.app", ControlSocket: socket, SpawnTimeout: time.Second}
		err := h.Signal(context.Background(), "acct-01")
		if !errors.Is(err, ErrAppUnavailable) {
			t.Fatalf("Signal against a dead app = %v, want errors.Is ErrAppUnavailable", err)
		}
		if launched {
			t.Error("Signal launched the app; it must NOT spawn (a no-latency nudge only)")
		}
	})
}

// TestRemoteState pins that State returns the registered root without spawning
// or re-registering, distinguishing ErrNoDomain (app up, unregistered) from
// ErrAppUnavailable (app down).
func TestRemoteState(t *testing.T) {
	t.Run("registered domain returns its root", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpPath, Response{OK: true, Path: "/cloud/acct-01"})
		h := newRemoteHost(t, a)
		path, err := h.State(context.Background(), "acct-01")
		if err != nil || path != "/cloud/acct-01" {
			t.Fatalf("State = %q, %v; want /cloud/acct-01", path, err)
		}
		seen := a.seen()
		if len(seen) != 1 || seen[0].Op != OpPath {
			t.Fatalf("app saw %+v, want one path op (no spawn, no register)", seen)
		}
	})
	t.Run("unknown domain is ErrNoDomain", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpPath, Response{OK: false, ErrClass: ClassNoDomain, Error: "not registered"})
		h := newRemoteHost(t, a)
		_, err := h.State(context.Background(), "acct-01")
		if !errors.Is(err, ErrNoDomain) {
			t.Fatalf("State err = %v, want errors.Is ErrNoDomain", err)
		}
	})
	t.Run("dead app is ErrAppUnavailable, no spawn", func(t *testing.T) {
		socket := filepath.Join(shortSockDir(t), "absent.sock")
		var launched bool
		withLaunchApp(t, func(context.Context, string) error { launched = true; return nil })
		h := &RemoteDomainHost{AppPath: "/Apps/X.app", ControlSocket: socket, SpawnTimeout: time.Second}
		_, err := h.State(context.Background(), "acct-01")
		if !errors.Is(err, ErrAppUnavailable) {
			t.Fatalf("State err = %v, want errors.Is ErrAppUnavailable", err)
		}
		if launched {
			t.Error("State spawned the app; it must be a zero-spawn probe")
		}
	})
}

// TestRemoteProbe pins that Probe spawns, then asks the app for the capability verdict.
func TestRemoteProbe(t *testing.T) {
	t.Run("capable", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpProbe, Response{OK: true, FPOK: true})
		h := newRemoteHost(t, a)
		ok, err := h.Probe(context.Background())
		if err != nil || !ok {
			t.Fatalf("Probe = %v, %v; want true", ok, err)
		}
	})
	t.Run("no entitlement is the retreat verdict", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpProbe, Response{OK: true, FPOK: false, ErrClass: ClassNoEntitlement, Error: "off"})
		h := newRemoteHost(t, a)
		ok, err := h.Probe(context.Background())
		if ok {
			t.Fatal("Probe = true, want false")
		}
		if !errors.Is(err, ErrCannotControl) {
			t.Errorf("Probe err = %v, want errors.Is ErrCannotControl", err)
		}
	})
}

// TestRemoteValidatesDomain pins that every domain op fails fast on an empty domain.
func TestRemoteValidatesDomain(t *testing.T) {
	h := &RemoteDomainHost{AppPath: "/Apps/X.app", ControlSocket: "/s.sock"}
	ctx := context.Background()
	if _, err := h.Ensure(ctx, ""); err == nil {
		t.Error("Ensure with empty domain accepted")
	}
	if err := h.Remove(ctx, ""); err == nil {
		t.Error("Remove with empty domain accepted")
	}
	if err := h.Signal(ctx, ""); err == nil {
		t.Error("Signal with empty domain accepted")
	}
	if _, err := h.State(ctx, ""); err == nil {
		t.Error("State with empty domain accepted")
	}
}
