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

// swapRemoveConfirmTiming shrinks RemoveConfirmed's window and poll interval for one
// test; callers must not run in parallel (both are package vars).
func swapRemoveConfirmTiming(t *testing.T, window, interval time.Duration) {
	t.Helper()
	pw, pi := removeConfirmWindow, removeConfirmPollInterval
	removeConfirmWindow, removeConfirmPollInterval = window, interval
	t.Cleanup(func() { removeConfirmWindow, removeConfirmPollInterval = pw, pi })
}

func removeCount(a *fakeApp) int {
	n := 0
	for _, r := range a.seen() {
		if r.Op == OpRemove {
			n++
		}
	}
	return n
}

// TestRemoteRemoveConfirmed pins RemoveConfirmed's absence-confirm contract: it
// removes the domain and confirms it left the list, re-issuing Remove once if a
// deferred add lands after the first Remove no-op'd, and reporting
// ErrDomainRemovalUnconfirmed when absence is never confirmed within the window.
func TestRemoteRemoveConfirmed(t *testing.T) {
	t.Run("absence confirmed on first Remove issues exactly one Remove", func(t *testing.T) {
		swapRemoveConfirmTiming(t, 2*time.Second, time.Millisecond)
		a := startFakeApp(t)
		a.setResponse(OpRemove, Response{OK: true})
		a.setResponse(OpPath, Response{OK: false, ErrClass: ClassNoDomain, Error: "not registered"})
		h := newRemoteHost(t, a)

		if err := h.RemoveConfirmed(context.Background(), "acct-01"); err != nil {
			t.Fatalf("RemoveConfirmed = %v, want nil", err)
		}
		if n := removeCount(a); n != 1 {
			t.Fatalf("issued %d Removes, want exactly 1 (absence confirmed on the first)", n)
		}
	})

	t.Run("re-issues Remove once when a deferred add lands after the first no-op", func(t *testing.T) {
		swapRemoveConfirmTiming(t, 5*time.Second, time.Millisecond)
		a := startFakeApp(t)
		a.setResponse(OpRemove, Response{OK: true})
		// The domain stays listed (a deferred add landed) until the SECOND Remove clears it.
		a.setPath(func(domain string) Response {
			if removeCount(a) >= 2 {
				return Response{OK: false, ErrClass: ClassNoDomain, Error: "not registered"}
			}
			return Response{OK: true, Path: "/cloud/" + domain}
		})
		h := newRemoteHost(t, a)

		if err := h.RemoveConfirmed(context.Background(), "acct-01"); err != nil {
			t.Fatalf("RemoveConfirmed = %v, want nil once the re-issued Remove clears the landed add", err)
		}
		if n := removeCount(a); n != 2 {
			t.Fatalf("issued %d Removes, want exactly 2 (the first no-op'd, one re-issue cleared the landed add)", n)
		}
	})

	t.Run("never-absent domain is ErrDomainRemovalUnconfirmed", func(t *testing.T) {
		swapRemoveConfirmTiming(t, 60*time.Millisecond, time.Millisecond)
		a := startFakeApp(t)
		a.setResponse(OpRemove, Response{OK: true})
		a.setResponse(OpPath, Response{OK: true, Path: "/cloud/acct-01"}) // always listed
		h := newRemoteHost(t, a)

		err := h.RemoveConfirmed(context.Background(), "acct-01")
		if !errors.Is(err, ErrDomainRemovalUnconfirmed) {
			t.Fatalf("RemoveConfirmed err = %v, want errors.Is ErrDomainRemovalUnconfirmed", err)
		}
	})

	t.Run("already-absent domain is immediate nil with exactly one Remove", func(t *testing.T) {
		swapRemoveConfirmTiming(t, 2*time.Second, time.Millisecond)
		a := startFakeApp(t)
		a.setResponse(OpRemove, Response{OK: true})
		a.setResponse(OpPath, Response{OK: false, ErrClass: ClassNoDomain, Error: "never registered"})
		h := newRemoteHost(t, a)

		if err := h.RemoveConfirmed(context.Background(), "acct-01"); err != nil {
			t.Fatalf("RemoveConfirmed on an already-absent domain = %v, want nil", err)
		}
		if n := removeCount(a); n != 1 {
			t.Fatalf("issued %d Removes, want exactly 1", n)
		}
	})

	t.Run("a deferred add landing after the first ErrNoDomain is not stable absence: re-Remove, then streak", func(t *testing.T) {
		swapRemoveConfirmTiming(t, 5*time.Second, time.Millisecond)
		a := startFakeApp(t)
		a.setResponse(OpRemove, Response{OK: true})
		var pathCalls atomic.Int32
		// Poll 1 answers ErrNoDomain (the deferred add has not landed yet); poll 2
		// reveals the landed add (listed); after the re-issued Remove clears it, absent
		// for good. A first-ErrNoDomain-wins impl would return nil at poll 1 with 1 Remove.
		a.setPath(func(domain string) Response {
			if pathCalls.Add(1) == 1 || removeCount(a) >= 2 {
				return Response{OK: false, ErrClass: ClassNoDomain, Error: "not registered"}
			}
			return Response{OK: true, Path: "/cloud/" + domain}
		})
		h := newRemoteHost(t, a)

		if err := h.RemoveConfirmed(context.Background(), "acct-01"); err != nil {
			t.Fatalf("RemoveConfirmed = %v, want nil once the re-issued Remove clears the landed add and absence holds", err)
		}
		if n := removeCount(a); n != 2 {
			t.Fatalf("issued %d Removes, want exactly 2 (the first ErrNoDomain was NOT stable; the landed add forced one re-issue)", n)
		}
	})

	t.Run("every poll ErrAppUnavailable joins the sentinel and the app-unavailable cause", func(t *testing.T) {
		swapRemoveConfirmTiming(t, 60*time.Millisecond, time.Millisecond)
		a := startFakeApp(t)
		a.setResponse(OpRemove, Response{OK: true})
		a.setResponse(OpPath, Response{OK: false, ErrClass: ClassAppUnreachable, Error: "cold"})
		h := newRemoteHost(t, a)

		err := h.RemoveConfirmed(context.Background(), "acct-01")
		if !errors.Is(err, ErrDomainRemovalUnconfirmed) {
			t.Fatalf("RemoveConfirmed err = %v, want errors.Is ErrDomainRemovalUnconfirmed", err)
		}
		if !errors.Is(err, ErrAppUnavailable) {
			t.Fatalf("RemoveConfirmed err = %v, want the last non-ErrNoDomain cause (ErrAppUnavailable) preserved in the chain", err)
		}
	})

	t.Run("ctx deadline mid-confirm joins context.DeadlineExceeded", func(t *testing.T) {
		swapRemoveConfirmTiming(t, 2*time.Second, 5*time.Millisecond)
		a := startFakeApp(t)
		a.setResponse(OpRemove, Response{OK: true})
		a.setResponse(OpPath, Response{OK: true, Path: "/cloud/acct-01"}) // always listed → never a streak
		h := newRemoteHost(t, a)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		err := h.RemoveConfirmed(ctx, "acct-01")
		if !errors.Is(err, ErrDomainRemovalUnconfirmed) {
			t.Fatalf("RemoveConfirmed err = %v, want errors.Is ErrDomainRemovalUnconfirmed", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("RemoveConfirmed err = %v, want the context cause preserved so errors.Is(context.DeadlineExceeded) holds", err)
		}
	})

	t.Run("empty domain is rejected", func(t *testing.T) {
		h := &RemoteDomainHost{AppPath: "/Apps/X.app", ControlSocket: "/s.sock"}
		if err := h.RemoveConfirmed(context.Background(), ""); err == nil {
			t.Error("RemoveConfirmed with empty domain accepted")
		}
	})
}

// TestRemoteHostCarriesLaunchTimeout pins that RemoteDomainHost.LaunchTimeout reaches
// the AppSpawn it constructs, so the consumer-configured launch bound is honored.
func TestRemoteHostCarriesLaunchTimeout(t *testing.T) {
	h := &RemoteDomainHost{AppPath: "/Apps/X.app", ControlSocket: "/s.sock", LaunchTimeout: 42 * time.Second}
	if got := h.appSpawn().LaunchTimeout; got != 42*time.Second {
		t.Errorf("appSpawn().LaunchTimeout = %v, want the host's 42s", got)
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

// TestRemoteProbeDomainShallow pins that ProbeDomainShallow forwards the shallow
// verdict without spawning (zero-spawn like ProbeDomain) and maps a not-serving
// verdict to ErrDomainNotServing.
func TestRemoteProbeDomainShallow(t *testing.T) {
	t.Run("listed verdict forwards without spawning", func(t *testing.T) {
		a := startFakeApp(t)
		a.setProbeShallow(func(string) Response { return Response{OK: true, Listed: boolptr(true)} })
		h := newRemoteHost(t, a)
		listed, err := h.ProbeDomainShallow(context.Background(), "acct-01")
		if err != nil || !listed {
			t.Fatalf("ProbeDomainShallow = %v, %v; want true", listed, err)
		}
		seen := a.seen()
		if len(seen) != 1 || seen[0].Op != OpProbeDomain || !seen[0].Shallow {
			t.Fatalf("app saw %+v, want one shallow probe-domain (no spawn)", seen)
		}
	})
	t.Run("not-serving is ErrDomainNotServing", func(t *testing.T) {
		a := startFakeApp(t)
		a.setProbeShallow(func(string) Response {
			return Response{OK: false, ErrClass: ClassDomainNotServing, Error: "materializing"}
		})
		h := newRemoteHost(t, a)
		_, err := h.ProbeDomainShallow(context.Background(), "acct-01")
		if !errors.Is(err, ErrDomainNotServing) {
			t.Fatalf("err = %v, want errors.Is ErrDomainNotServing", err)
		}
	})
	t.Run("dead app is ErrAppUnavailable, no spawn", func(t *testing.T) {
		socket := filepath.Join(shortSockDir(t), "absent.sock")
		var launched bool
		withLaunchApp(t, func(context.Context, string) error { launched = true; return nil })
		h := &RemoteDomainHost{AppPath: "/Apps/X.app", ControlSocket: socket, SpawnTimeout: time.Second}
		if _, err := h.ProbeDomainShallow(context.Background(), "acct-01"); !errors.Is(err, ErrAppUnavailable) {
			t.Fatalf("err = %v, want errors.Is ErrAppUnavailable", err)
		}
		if launched {
			t.Error("ProbeDomainShallow spawned the app; it must be a zero-spawn probe")
		}
	})
}

// TestRemotePrepareDomain pins that PrepareDomain forwards the deadline and maps a
// timed-out/failed materialization to ErrDomainNotServing, without spawning.
func TestRemotePrepareDomain(t *testing.T) {
	t.Run("completed materialization is nil, no spawn", func(t *testing.T) {
		a := startFakeApp(t)
		a.setPrepare(func(string) Response { return Response{OK: true} })
		h := newRemoteHost(t, a)
		if err := h.PrepareDomain(context.Background(), "acct-01", 5*time.Second); err != nil {
			t.Fatalf("PrepareDomain = %v, want nil", err)
		}
		seen := a.seen()
		if len(seen) != 1 || seen[0].Op != OpPrepareDomain || seen[0].DeadlineMS != 5000 {
			t.Fatalf("app saw %+v, want one prepare-domain with deadline_ms=5000 (no spawn)", seen)
		}
	})
	t.Run("download timeout is ErrDomainNotServing", func(t *testing.T) {
		a := startFakeApp(t)
		a.setPrepare(func(string) Response {
			return Response{OK: false, ErrClass: ClassDomainNotServing, Error: "download timed out"}
		})
		h := newRemoteHost(t, a)
		if err := h.PrepareDomain(context.Background(), "acct-01", 5*time.Second); !errors.Is(err, ErrDomainNotServing) {
			t.Fatalf("err = %v, want errors.Is ErrDomainNotServing", err)
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
	if _, err := h.ProbeDomainShallow(ctx, ""); err == nil {
		t.Error("ProbeDomainShallow with empty domain accepted")
	}
	if err := h.PrepareDomain(ctx, "", time.Second); err == nil {
		t.Error("PrepareDomain with empty domain accepted")
	}
}
