package fileproviderd

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
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

// TestRemoteEnsureReport pins that EnsureReport registers like Ensure and reports
// fresh-vs-preexisting from a settle-confirmed Path pre-check.
func TestRemoteEnsureReport(t *testing.T) {
	t.Run("provably-absent domain reports fresh", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpPath, Response{OK: false, ErrClass: ClassNoDomain, Error: "not registered"})
		a.setRegister(func(domain string) Response { return Response{OK: true, Path: "/cloud/" + domain} })
		h := newRemoteHost(t, a)
		h.DomainLoadSettle = time.Nanosecond // one probe confirms absence, no real wait

		path, fresh, err := h.EnsureReport(context.Background(), "acct-01")
		if err != nil {
			t.Fatalf("EnsureReport = %v, want nil", err)
		}
		if path != "/cloud/acct-01" {
			t.Fatalf("EnsureReport path = %q, want /cloud/acct-01", path)
		}
		if !fresh {
			t.Fatalf("EnsureReport fresh = false, want true (the domain was absent across the settle window)")
		}
		// One or more Path pre-checks (the settle poll) then exactly one register last.
		seen := a.seen()
		if len(seen) < 2 || seen[len(seen)-1].Op != OpRegister {
			t.Fatalf("app saw %+v, want path pre-check(s) then a register", seen)
		}
		for _, r := range seen[:len(seen)-1] {
			if r.Op != OpPath {
				t.Fatalf("app saw %+v, want only path ops before the register", seen)
			}
		}
	})

	t.Run("already-registered domain reports not fresh", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpPath, Response{OK: true, Path: "/cloud/acct-01"})
		a.setRegister(func(domain string) Response { return Response{OK: true, Path: "/cloud/" + domain} })
		h := newRemoteHost(t, a)

		_, fresh, err := h.EnsureReport(context.Background(), "acct-01")
		if err != nil {
			t.Fatalf("EnsureReport = %v, want nil", err)
		}
		if fresh {
			t.Fatalf("EnsureReport fresh = true, want false (the domain pre-existed this call)")
		}
	})

	t.Run("cold appex revealing a pre-existing domain mid-settle is not fresh", func(t *testing.T) {
		a := startFakeApp(t)
		var probes atomic.Int32
		a.setPath(func(string) Response {
			if probes.Add(1) <= 3 {
				// Appex still loading its OS-persisted domain list: a false ErrNoDomain.
				return Response{OK: false, ErrClass: ClassNoDomain, Error: "domains not loaded"}
			}
			return Response{OK: true, Path: "/cloud/acct-01"} // the domain was there all along
		})
		a.setRegister(func(domain string) Response { return Response{OK: true, Path: "/cloud/" + domain} })
		h := newRemoteHost(t, a)
		h.DomainLoadSettle = 2 * time.Second // long enough to outlast the cold-start reveal
		h.DomainLoadPollInterval = 2 * time.Millisecond

		_, fresh, err := h.EnsureReport(context.Background(), "acct-01")
		if err != nil {
			t.Fatalf("EnsureReport = %v, want nil", err)
		}
		if fresh {
			t.Fatalf("EnsureReport fresh = true for a domain the cold appex revealed mid-settle, want false (removing it would tear down a live account)")
		}
		if n := probes.Load(); n < 4 {
			t.Errorf("path pre-check ran %d times, want the settle to outlast at least 3 false ErrNoDomain answers", n)
		}
	})

	t.Run("register failure reports not fresh and errors", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpPath, Response{OK: false, ErrClass: ClassNoDomain, Error: "not registered"})
		a.setRegister(func(string) Response {
			return Response{OK: false, ErrClass: ClassRegisterFailed, Error: "duplicate"}
		})
		h := newRemoteHost(t, a)
		h.DomainLoadSettle = time.Nanosecond

		_, fresh, err := h.EnsureReport(context.Background(), "acct-01")
		if !errors.Is(err, ErrRegisterFailed) {
			t.Fatalf("EnsureReport err = %v, want errors.Is ErrRegisterFailed", err)
		}
		if fresh {
			t.Errorf("EnsureReport fresh = true on a failed register, want false")
		}
	})

	t.Run("empty domain is rejected", func(t *testing.T) {
		h := &RemoteDomainHost{AppPath: "/Apps/X.app", ControlSocket: "/s.sock"}
		if _, _, err := h.EnsureReport(context.Background(), ""); err == nil {
			t.Error("EnsureReport with empty domain accepted")
		}
	})
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

// TestRemoteProbeDomain pins that ProbeDomain reports the domain verdict without
// spawning (a zero-spawn probe like State), distinguishing a serving domain, a
// not-yet-serving one (ErrDomainNotServing), and a down app (ErrAppUnavailable).
func TestRemoteProbeDomain(t *testing.T) {
	t.Run("serving domain returns its byte count without spawning", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpProbeDomain, Response{OK: true, JSONBytes: int64ptr(512)})
		h := newRemoteHost(t, a)
		v, err := h.ProbeDomain(context.Background(), "acct-01")
		if err != nil || v == nil || *v != 512 {
			t.Fatalf("ProbeDomain = %v, %v; want a pointer to 512", v, err)
		}
		seen := a.seen()
		if len(seen) != 1 || seen[0].Op != OpProbeDomain {
			t.Fatalf("app saw %+v, want one probe-domain (no spawn, no register)", seen)
		}
	})
	t.Run("registered but not yet serving is ErrDomainNotServing", func(t *testing.T) {
		a := startFakeApp(t)
		a.setResponse(OpProbeDomain, Response{OK: false, ErrClass: ClassDomainNotServing, Error: "materializing"})
		h := newRemoteHost(t, a)
		_, err := h.ProbeDomain(context.Background(), "acct-01")
		if !errors.Is(err, ErrDomainNotServing) {
			t.Fatalf("ProbeDomain err = %v, want errors.Is ErrDomainNotServing", err)
		}
	})
	t.Run("dead app is ErrAppUnavailable, no spawn", func(t *testing.T) {
		socket := filepath.Join(shortSockDir(t), "absent.sock")
		var launched bool
		withLaunchApp(t, func(context.Context, string) error { launched = true; return nil })
		h := &RemoteDomainHost{AppPath: "/Apps/X.app", ControlSocket: socket, SpawnTimeout: time.Second}
		_, err := h.ProbeDomain(context.Background(), "acct-01")
		if !errors.Is(err, ErrAppUnavailable) {
			t.Fatalf("ProbeDomain err = %v, want errors.Is ErrAppUnavailable", err)
		}
		if launched {
			t.Error("ProbeDomain spawned the app; it must be a zero-spawn probe")
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
	if _, err := h.ProbeDomain(ctx, ""); err == nil {
		t.Error("ProbeDomain with empty domain accepted")
	}
}
