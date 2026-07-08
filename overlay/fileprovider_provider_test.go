package overlay

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit/fileproviderd"
)

// setupPhaseDurations matches Setup's phase-timing error suffix, e.g.
// "(register 0s, serve-wait 0s):".
var setupPhaseDurations = regexp.MustCompile(`\(register \S+, serve-wait \S+\):`)

// fpTestDirs returns a short-path base, account dir, and a domain root standing
// in for ~/Library/CloudStorage/<App>-<Name>/. The account dir's parent exists but
// the dir itself does not — Setup creates it as a symlink. Short /tmp paths keep
// socket and symlink ops off the long t.TempDir path.
func fpTestDirs(t *testing.T) (base, accountDir, domainRoot string) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "ccp-ov-fpp")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	base = filepath.Join(root, "base")
	domainRoot = filepath.Join(root, "cloud", "acct-01")
	accountDir = filepath.Join(root, "accounts", "acct-01")
	for _, d := range []string{base, domainRoot, filepath.Dir(accountDir)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	return base, accountDir, domainRoot
}

// TestFileProviderSetupCreatesBridgeAndPrivateStore pins Setup's three effects:
// the domain is registered, the account dir becomes a symlink into the returned
// domain root, and the private store is seeded.
func TestFileProviderSetupCreatesBridgeAndPrivateStore(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(domain string) fileproviderd.Response {
		if domain != "acct-01" {
			t.Errorf("register domain = %q, want acct-01", domain)
		}
		return fileproviderd.Response{OK: true, Path: domainRoot}
	})
	a.setProbe(func(string) fileproviderd.Response { return serving() })
	p := newFileProvider(fpSpecFor(a))

	if err := p.Setup(base, accountDir); err != nil {
		t.Fatalf("Setup = %v, want nil", err)
	}

	got, err := os.Readlink(accountDir)
	if err != nil {
		t.Fatalf("account dir is not a symlink: %v", err)
	}
	if got != domainRoot {
		t.Errorf("bridge symlink target = %q, want the domain root %q", got, domainRoot)
	}
	priv := p.PrivateRoot(accountDir)
	if priv != FusePrivateRoot(accountDir) {
		t.Errorf("PrivateRoot = %q, want %q", priv, FusePrivateRoot(accountDir))
	}
	if fi, err := os.Stat(priv); err != nil || !fi.IsDir() {
		t.Errorf("private store %q not seeded as a dir (err=%v)", priv, err)
	}
	if p.Backend() != BackendFileProvider {
		t.Errorf("Backend() = %q, want %q", p.Backend(), BackendFileProvider)
	}
	if err := p.Setup(base, accountDir); err != nil {
		t.Fatalf("second Setup = %v, want nil (idempotent)", err)
	}
}

// TestFileProviderSetupRefusesToClobberRealDir pins the fail-closed guard: a real
// (non-symlink) account dir holding account state must never be replaced by the
// bridge symlink.
func TestFileProviderSetupRefusesToClobberRealDir(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		t.Fatal(err)
	}
	realFile := filepath.Join(accountDir, ".credentials.json")
	if err := os.WriteFile(realFile, []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response {
		return fileproviderd.Response{OK: true, Path: domainRoot}
	})
	a.setProbe(func(string) fileproviderd.Response { return serving() })
	p := newFileProvider(fpSpecFor(a))

	if err := p.Setup(base, accountDir); err == nil {
		t.Fatal("Setup over a real account dir = nil, want a loud clobber-guard failure")
	}
	if fi, err := os.Lstat(accountDir); err != nil || fi.Mode()&os.ModeSymlink != 0 {
		t.Errorf("account dir was clobbered into a symlink (err=%v)", err)
	}
	if b, err := os.ReadFile(realFile); err != nil || string(b) != "secret" {
		t.Errorf("real account file lost or changed: %q, %v", b, err)
	}
}

// TestFileProviderSetupRetreatsOnNoEntitlement pins that a missing-entitlement
// register surfaces ErrCannotControl (the retreat condition), never the transient
// ErrAppUnavailable.
func TestFileProviderSetupRetreatsOnNoEntitlement(t *testing.T) {
	base, accountDir, _ := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response {
		return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassNoEntitlement, Error: "enable the extension"}
	})
	p := newFileProvider(fpSpecFor(a))

	err := p.Setup(base, accountDir)
	if !errors.Is(err, fileproviderd.ErrCannotControl) {
		t.Fatalf("Setup err = %v, want errors.Is ErrCannotControl (the retreat condition)", err)
	}
	if errors.Is(err, fileproviderd.ErrAppUnavailable) {
		t.Errorf("Setup err = %v, want the retreat NOT confused with the transient blip", err)
	}
	if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
		t.Errorf("account dir exists after a failed Setup, want none (lstat err=%v)", err)
	}
}

// swapFPReadyPollInterval shrinks the readiness poll interval for one test; callers
// must not run in parallel (fpReadyPollInterval is a package var).
func swapFPReadyPollInterval(t *testing.T, d time.Duration) {
	t.Helper()
	prev := fpReadyPollInterval
	fpReadyPollInterval = d
	t.Cleanup(func() { fpReadyPollInterval = prev })
}

// notServing is a canned probe-domain reply for a domain that registered but has
// not yet materialized enough to answer a read.
func notServing() fileproviderd.Response {
	return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassDomainNotServing, Error: "materializing"}
}

// TestFileProviderSetupWaitsForDomainToServe is the incident regression, now driven
// by the app-side ProbeDomain poll instead of a raw filesystem read: a domain that
// registers but never serves must fail Setup and leave NO bridge symlink — the
// account is never cut over to a domain that cannot answer reads (the pre-readiness
// cutover that crushed the File Provider host under a fleet migrate). The converses
// pin that a serving domain — including one whose .claude.json is absent — is cut
// over, and that a domain that serves after a few not-serving probes is cut over.
func TestFileProviderSetupWaitsForDomainToServe(t *testing.T) {
	t.Run("domain that never serves fails Setup and lays no symlink", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		swapFPReadyPollInterval(t, 5*time.Millisecond)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setProbe(func(string) fileproviderd.Response { return notServing() })
		spec := fpSpecFor(a)
		spec.ReadyTimeout = 60 * time.Millisecond
		p := newFileProvider(spec)

		err := p.Setup(base, accountDir)
		if !errors.Is(err, fileproviderd.ErrDomainNotServing) {
			t.Fatalf("Setup over a never-serving domain = %v, want errors.Is ErrDomainNotServing", err)
		}
		if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
			t.Errorf("Setup laid a bridge symlink over a domain that never served (lstat err=%v); the account was cut over pre-readiness — the incident", err)
		}
	})

	t.Run("serving domain is cut over", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setProbe(func(string) fileproviderd.Response { return serving() })
		spec := fpSpecFor(a)
		spec.ReadyTimeout = 5 * time.Second
		p := newFileProvider(spec)

		if err := p.Setup(base, accountDir); err != nil {
			t.Fatalf("Setup over a serving domain = %v, want nil", err)
		}
		got, err := os.Readlink(accountDir)
		if err != nil || got != domainRoot {
			t.Fatalf("bridge symlink = %q (err=%v), want a link to the serving domain root %q", got, err, domainRoot)
		}
	})

	t.Run("serving with .claude.json absent still counts as serving", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		// OK with a nil JSONBytes: the domain answers, .claude.json just does not exist yet.
		a.setProbe(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true} })
		spec := fpSpecFor(a)
		spec.ReadyTimeout = 5 * time.Second
		p := newFileProvider(spec)

		if err := p.Setup(base, accountDir); err != nil {
			t.Fatalf("Setup over a serving domain with no .claude.json = %v, want nil", err)
		}
		if got, err := os.Readlink(accountDir); err != nil || got != domainRoot {
			t.Fatalf("bridge symlink = %q (err=%v), want a link to %q", got, err, domainRoot)
		}
	})

	t.Run("domain that serves after two not-serving probes is cut over", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		swapFPReadyPollInterval(t, 5*time.Millisecond)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		var probes atomic.Int32
		a.setProbe(func(string) fileproviderd.Response {
			if probes.Add(1) <= 2 {
				return notServing()
			}
			return serving()
		})
		spec := fpSpecFor(a)
		spec.ReadyTimeout = 5 * time.Second
		p := newFileProvider(spec)

		if err := p.Setup(base, accountDir); err != nil {
			t.Fatalf("Setup = %v, want nil once the domain serves", err)
		}
		if got, err := os.Readlink(accountDir); err != nil || got != domainRoot {
			t.Fatalf("bridge symlink = %q (err=%v), want a link to %q", got, err, domainRoot)
		}
		if n := probes.Load(); n < 3 {
			t.Errorf("probe-domain called %d times, want at least 3 (two not-serving then serving)", n)
		}
	})
}

// TestFileProviderSetupSurvivesColdStart is the add-path incident regression: the
// readiness wait must outlast an appex not answering yet (the contact budget) and
// still succeed once it serves, without weakening the migrate-storm gate (which must
// still fire ErrDomainNotServing when the app never answers).
func TestFileProviderSetupSurvivesColdStart(t *testing.T) {
	t.Run("late-answering appex survives on the contact budget then serves", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		swapFPReadyPollInterval(t, 5*time.Millisecond)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		var probes atomic.Int32
		a.setProbe(func(string) fileproviderd.Response {
			if probes.Add(1) <= 6 {
				return notAnswering() // appex not answering yet — a cold start
			}
			return serving()
		})
		spec := fpSpecFor(a)
		spec.AppReadyTimeout = 5 * time.Second    // generous contact budget outlasts the cold start
		spec.ReadyTimeout = 20 * time.Millisecond // TINY serve budget: it must not run during contact
		p := newFileProvider(spec)

		if err := p.Setup(base, accountDir); err != nil {
			t.Fatalf("Setup = %v, want nil: a late-answering appex must survive on the contact budget, not the serve budget", err)
		}
		if got, err := os.Readlink(accountDir); err != nil || got != domainRoot {
			t.Fatalf("bridge symlink = %q (err=%v), want a link to %q once the appex served", got, err, domainRoot)
		}
		if n := probes.Load(); n < 7 {
			t.Errorf("probe-domain called %d times, want at least 7 (six not-answering then serving)", n)
		}
	})

	t.Run("materialization within the serve budget is cut over", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		swapFPReadyPollInterval(t, 5*time.Millisecond)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		var probes atomic.Int32
		a.setProbe(func(string) fileproviderd.Response {
			if probes.Add(1) <= 6 {
				return notServing() // app up, domain still materializing
			}
			return serving()
		})
		spec := fpSpecFor(a)
		spec.AppReadyTimeout = 20 * time.Millisecond // TINY contact budget: never engaged once the app answers
		spec.ReadyTimeout = 5 * time.Second          // serve budget covers the materialization stretch
		p := newFileProvider(spec)

		if err := p.Setup(base, accountDir); err != nil {
			t.Fatalf("Setup = %v, want nil once the materializing domain serves", err)
		}
		if got, err := os.Readlink(accountDir); err != nil || got != domainRoot {
			t.Fatalf("bridge symlink = %q (err=%v), want a link to %q", got, err, domainRoot)
		}
	})

	t.Run("app that never answers fails the gate and never cuts over", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		swapFPReadyPollInterval(t, 5*time.Millisecond)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setProbe(func(string) fileproviderd.Response { return notAnswering() })
		spec := fpSpecFor(a)
		spec.AppReadyTimeout = 60 * time.Millisecond // tiny contact budget: the gate must still fire
		spec.ReadyTimeout = 5 * time.Second
		p := newFileProvider(spec)

		err := p.Setup(base, accountDir)
		if !errors.Is(err, fileproviderd.ErrDomainNotServing) {
			t.Fatalf("Setup against a never-answering app = %v, want errors.Is ErrDomainNotServing (the migrate-storm gate)", err)
		}
		if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
			t.Errorf("Setup laid a bridge symlink over an app that never answered (lstat err=%v)", err)
		}
	})
}

// TestFileProviderSetupRemovesFreshDomainOnFailure pins the no-orphan rule: a
// post-registration failure removes a domain THIS Setup freshly registered but
// leaves a pre-existing one alone (removing it would tear down a live account).
func TestFileProviderSetupRemovesFreshDomainOnFailure(t *testing.T) {
	t.Run("fresh registration is removed when readiness fails", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		swapFPReadyPollInterval(t, 5*time.Millisecond)
		a := startFakeFPApp(t)
		// Pre-check finds no registration: THIS Setup freshly creates the domain.
		a.setPath(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassNoDomain, Error: "not registered"}
		})
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setProbe(func(string) fileproviderd.Response { return notServing() }) // never serves
		a.setResponse(fileproviderd.OpRemove, fileproviderd.Response{OK: true})
		spec := fpSpecFor(a)
		spec.ReadyTimeout = 40 * time.Millisecond
		p := newFileProvider(spec)
		p.host.DomainLoadSettle = time.Nanosecond // one probe confirms absence, no real settle wait

		err := p.Setup(base, accountDir)
		if !errors.Is(err, fileproviderd.ErrDomainNotServing) {
			t.Fatalf("Setup err = %v, want errors.Is ErrDomainNotServing", err)
		}
		var sawRemove bool
		for _, r := range a.seen() {
			if r.Op == fileproviderd.OpRemove && r.Domain == "acct-01" {
				sawRemove = true
			}
		}
		if !sawRemove {
			t.Errorf("Setup left a fresh domain registered after failing: ops = %v, want a remove of acct-01", a.ops())
		}
		if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
			t.Errorf("Setup laid a bridge symlink despite failing (lstat err=%v)", err)
		}
	})

	t.Run("pre-existing domain is NOT removed when readiness fails", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		swapFPReadyPollInterval(t, 5*time.Millisecond)
		a := startFakeFPApp(t)
		// Pre-check finds the domain already registered: it PRE-EXISTS this Setup.
		a.setPath(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setProbe(func(string) fileproviderd.Response { return notServing() }) // never serves
		a.setResponse(fileproviderd.OpRemove, fileproviderd.Response{OK: true})
		spec := fpSpecFor(a)
		spec.ReadyTimeout = 40 * time.Millisecond
		p := newFileProvider(spec)

		err := p.Setup(base, accountDir)
		if !errors.Is(err, fileproviderd.ErrDomainNotServing) {
			t.Fatalf("Setup err = %v, want errors.Is ErrDomainNotServing", err)
		}
		for _, r := range a.seen() {
			if r.Op == fileproviderd.OpRemove {
				t.Fatalf("Setup removed a PRE-EXISTING domain on failure: ops = %v, want no remove (it would tear down a live account)", a.ops())
			}
		}
	})
}

// TestFileProviderSetupSeedFailureLeavesNoSymlink pins the cutOver ordering rule:
// the private store is seeded before the account-dir symlink, so a seed failure
// fails Setup leaving no dangling symlink into a domain root the failure rolls back.
func TestFileProviderSetupSeedFailureLeavesNoSymlink(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	swapFPReadyPollInterval(t, 5*time.Millisecond)
	a := startFakeFPApp(t)
	// Pre-existing domain (Path answers ok) so EnsureReport skips the settle wait.
	a.setPath(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setProbe(func(string) fileproviderd.Response { return serving() }) // readiness passes
	// Block the private-store seed: a regular file sits where MkdirAll wants a dir.
	if err := os.WriteFile(FusePrivateRoot(accountDir), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	p := newFileProvider(fpSpecFor(a))

	err := p.Setup(base, accountDir)
	if err == nil {
		t.Fatal("Setup with a blocked private-store seed = nil, want a failure")
	}
	if !strings.Contains(err.Error(), "seed private store") {
		t.Errorf("Setup err = %v, want it to fail at the private-store seed", err)
	}
	if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
		t.Errorf("Setup left an account-dir symlink after the seed failed (lstat err=%v); the symlink must be the last, hardest-to-retract step", err)
	}
}

// TestFileProviderSetupUpgradeOnUnsupportedOp pins the no-fallback rule: an app too
// old to answer probe-domain (its unknown-op default arm: ok:false, empty err_class)
// fails Setup IMMEDIATELY — not at the deadline — with the operator upgrade hint and
// no bridge symlink. There is deliberately NO raw-filesystem read fallback; a silent
// read would resurrect the TCC prompt storm this op exists to prevent.
func TestFileProviderSetupUpgradeOnUnsupportedOp(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	// No setProbe: the fake answers probe-domain from its unknown-op default arm.
	spec := fpSpecFor(a)
	spec.ReadyTimeout = 10 * time.Second // large: a deadline-bound failure would take this long
	spec.UpgradeHint = "upgrade the cc-pool-status cask"
	p := newFileProvider(spec)

	start := time.Now()
	err := p.Setup(base, accountDir)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("Setup took %v against an old app; ErrOpUnsupported must fail immediately, never poll to the deadline", elapsed)
	}
	if !errors.Is(err, fileproviderd.ErrOpUnsupported) {
		t.Fatalf("Setup err = %v, want errors.Is ErrOpUnsupported", err)
	}
	if !strings.Contains(err.Error(), "upgrade the cc-pool-status cask") {
		t.Errorf("Setup err = %q, want it to carry the operator upgrade hint", err.Error())
	}
	if errors.Is(err, fileproviderd.ErrDomainNotServing) || errors.Is(err, fileproviderd.ErrAppUnavailable) {
		t.Errorf("Setup err = %v, want the loud upgrade path NOT read as a readiness miss or transient blip", err)
	}
	if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
		t.Errorf("Setup laid a bridge symlink despite an unsupported-op failure (lstat err=%v)", err)
	}
}

// TestFileProviderSetupErrorNamesPhaseDurations pins that a Setup failure names how
// long each phase (register, serve-wait) took, so an operator can see where a slow
// failure spent its time, while the error still errors.Is-matches the sentinel.
func TestFileProviderSetupErrorNamesPhaseDurations(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	swapFPReadyPollInterval(t, 5*time.Millisecond)
	a := startFakeFPApp(t)
	// Pre-check finds no registration so this Setup owns the fresh domain (rollback path).
	a.setPath(func(string) fileproviderd.Response {
		return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassNoDomain, Error: "not registered"}
	})
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setProbe(func(string) fileproviderd.Response { return notServing() }) // never serves → cutOver fails
	a.setResponse(fileproviderd.OpRemove, fileproviderd.Response{OK: true})
	spec := fpSpecFor(a)
	spec.ReadyTimeout = 40 * time.Millisecond
	p := newFileProvider(spec)
	p.host.DomainLoadSettle = time.Nanosecond // one probe confirms absence, no real settle wait

	err := p.Setup(base, accountDir)
	if !errors.Is(err, fileproviderd.ErrDomainNotServing) {
		t.Fatalf("Setup err = %v, want errors.Is ErrDomainNotServing", err)
	}
	if !setupPhaseDurations.MatchString(err.Error()) {
		t.Errorf("Setup err = %q, want it to name both phase durations, e.g. (register 0s, serve-wait 0s)", err.Error())
	}
}

// TestFileProviderSpecLaunchTimeoutReachesHost pins that FileProviderSpec.LaunchTimeout
// is plumbed through newFileProvider into the RemoteDomainHost that forwards it to the
// AppSpawn, so a consumer's launch bound is honored end to end.
func TestFileProviderSpecLaunchTimeoutReachesHost(t *testing.T) {
	spec := &FileProviderSpec{AppPath: "/Apps/X.app", ControlSocket: "/s.sock", LaunchTimeout: 7 * time.Second}
	if got := newFileProvider(spec).host.LaunchTimeout; got != 7*time.Second {
		t.Errorf("host.LaunchTimeout = %v, want the spec's 7s", got)
	}
}

// TestFileProviderHealth pins Health's verdict for intact, drifted-symlink, and
// removed-registration (ErrNoDomain) states.
func TestFileProviderHealth(t *testing.T) {
	t.Run("intact domain and symlink", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setPath(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setProbe(func(string) fileproviderd.Response { return serving() })
		a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
		p := newFileProvider(fpSpecFor(a))
		if err := p.Setup(base, accountDir); err != nil {
			t.Fatalf("Setup = %v", err)
		}
		if err := p.Health(base, accountDir); err != nil {
			t.Fatalf("Health = %v, want nil (intact)", err)
		}
	})
	t.Run("drifted symlink target fails", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setPath(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: true, Path: domainRoot + "-moved"}
		})
		a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
		if err := fileproviderd.AtomicSymlink(accountDir, domainRoot); err != nil {
			t.Fatal(err)
		}
		p := newFileProvider(fpSpecFor(a))
		if err := p.Health(base, accountDir); err == nil {
			t.Fatal("Health with a drifted symlink target = nil, want a failure")
		}
	})
	t.Run("removed registration is ErrNoDomain", func(t *testing.T) {
		base, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setPath(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassNoDomain, Error: "not registered"}
		})
		p := newFileProvider(fpSpecFor(a))
		err := p.Health(base, accountDir)
		if !errors.Is(err, fileproviderd.ErrNoDomain) {
			t.Fatalf("Health err = %v, want errors.Is ErrNoDomain", err)
		}
	})
}

// TestFileProviderSync pins that Sync re-registers, re-asserts the bridge
// symlink, and signals the enumerator.
func TestFileProviderSync(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("Sync = %v, want nil", err)
	}
	got, err := os.Readlink(accountDir)
	if err != nil || got != domainRoot {
		t.Fatalf("Sync did not assert the bridge symlink: %q, %v", got, err)
	}
	var sawRegister, sawSignal bool
	for _, op := range a.ops() {
		switch op {
		case fileproviderd.OpRegister:
			sawRegister = true
		case fileproviderd.OpSignal:
			sawSignal = true
		}
	}
	if !sawRegister || !sawSignal {
		t.Errorf("Sync ops = %v, want both a register and a signal", a.ops())
	}
}

// TestFileProviderTeardown pins that Teardown retracts the bridge symlink and
// deregisters the domain, leaving the private store in place.
func TestFileProviderTeardown(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setProbe(func(string) fileproviderd.Response { return serving() })
	a.setResponse(fileproviderd.OpRemove, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	if err := p.Setup(base, accountDir); err != nil {
		t.Fatalf("Setup = %v", err)
	}
	priv := p.PrivateRoot(accountDir)
	if err := p.Teardown(base, accountDir); err != nil {
		t.Fatalf("Teardown = %v, want nil", err)
	}
	if _, err := os.Lstat(accountDir); !os.IsNotExist(err) {
		t.Errorf("account dir still present after Teardown (lstat err=%v)", err)
	}
	var sawRemove bool
	for _, op := range a.ops() {
		if op == fileproviderd.OpRemove {
			sawRemove = true
		}
	}
	if !sawRemove {
		t.Errorf("Teardown ops = %v, want a remove", a.ops())
	}
	if fi, err := os.Stat(priv); err != nil || !fi.IsDir() {
		t.Errorf("Teardown removed the private store %q (err=%v)", priv, err)
	}
}

// TestFileProviderTeardownRefusesToRemoveRealDir pins the fail-closed guard:
// Teardown must never RemoveAll a real (non-symlink) account dir.
func TestFileProviderTeardownRefusesToRemoveRealDir(t *testing.T) {
	base, accountDir, _ := fpTestDirs(t)
	if err := os.MkdirAll(accountDir, 0o700); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(accountDir, "real.txt")
	if err := os.WriteFile(keep, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	a := startFakeFPApp(t)
	a.setResponse(fileproviderd.OpRemove, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	if err := p.Teardown(base, accountDir); err == nil {
		t.Fatal("Teardown over a real account dir = nil, want a fail-closed refusal")
	}
	if b, err := os.ReadFile(keep); err != nil || string(b) != "data" {
		t.Errorf("Teardown destroyed real account data: %q, %v", b, err)
	}
}

// TestFileProviderProbeDomain pins the exported ProbeDomain: it reports the
// account domain's .claude.json byte-count verdict (a pointer to the count, nil when
// the file is absent) and surfaces the domain classes for the caller to key on.
func TestFileProviderProbeDomain(t *testing.T) {
	t.Run("serving domain returns the byte count", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setProbe(func(domain string) fileproviderd.Response {
			if domain != "acct-01" {
				t.Errorf("probe domain = %q, want acct-01", domain)
			}
			n := int64(256)
			return fileproviderd.Response{OK: true, JSONBytes: &n}
		})
		p := newFileProvider(fpSpecFor(a))
		v, err := p.ProbeDomain(context.Background(), accountDir)
		if err != nil || v == nil || *v != 256 {
			t.Fatalf("ProbeDomain = %v, %v; want a pointer to 256", v, err)
		}
	})
	t.Run("serving domain with .claude.json absent returns nil, nil", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setProbe(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true} })
		p := newFileProvider(fpSpecFor(a))
		v, err := p.ProbeDomain(context.Background(), accountDir)
		if err != nil || v != nil {
			t.Fatalf("ProbeDomain = %v, %v; want a nil byte count and nil error", v, err)
		}
	})
	t.Run("unregistered domain is ErrNoDomain", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setProbe(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassNoDomain, Error: "not registered"}
		})
		p := newFileProvider(fpSpecFor(a))
		if _, err := p.ProbeDomain(context.Background(), accountDir); !errors.Is(err, fileproviderd.ErrNoDomain) {
			t.Fatalf("ProbeDomain err = %v, want errors.Is ErrNoDomain", err)
		}
	})
}

// TestFileProviderRemoveDomain pins that RemoveDomain deregisters the account's
// domain WITHOUT retracting the bridge symlink (unlike Teardown): the symlink at the
// account dir survives.
func TestFileProviderRemoveDomain(t *testing.T) {
	_, accountDir, domainRoot := fpTestDirs(t)
	if err := fileproviderd.AtomicSymlink(accountDir, domainRoot); err != nil {
		t.Fatal(err)
	}
	a := startFakeFPApp(t)
	a.setResponse(fileproviderd.OpRemove, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	if err := p.RemoveDomain(accountDir); err != nil {
		t.Fatalf("RemoveDomain = %v, want nil", err)
	}
	var sawRemove bool
	for _, r := range a.seen() {
		if r.Op == fileproviderd.OpRemove && r.Domain == "acct-01" {
			sawRemove = true
		}
	}
	if !sawRemove {
		t.Errorf("RemoveDomain sent %v, want a remove of acct-01", a.ops())
	}
	if got, err := os.Readlink(accountDir); err != nil || got != domainRoot {
		t.Errorf("RemoveDomain disturbed the bridge symlink: %q, %v (want it left at %q)", got, err, domainRoot)
	}
}

// TestFileProviderDomainRoot pins the exported DomainRoot: the zero-spawn State
// query returns a registered domain's root and surfaces ErrNoDomain for an
// unregistered one, never reading through the domain.
func TestFileProviderDomainRoot(t *testing.T) {
	t.Run("registered domain returns its root", func(t *testing.T) {
		_, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setResponse(fileproviderd.OpPath, fileproviderd.Response{OK: true, Path: domainRoot})
		p := newFileProvider(fpSpecFor(a))
		got, err := p.DomainRoot(context.Background(), accountDir)
		if err != nil || got != domainRoot {
			t.Fatalf("DomainRoot = %q, %v; want %q, nil", got, err, domainRoot)
		}
	})
	t.Run("unregistered domain is ErrNoDomain", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setResponse(fileproviderd.OpPath, fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassNoDomain, Error: "not registered"})
		p := newFileProvider(fpSpecFor(a))
		if _, err := p.DomainRoot(context.Background(), accountDir); !errors.Is(err, fileproviderd.ErrNoDomain) {
			t.Fatalf("DomainRoot err = %v, want errors.Is ErrNoDomain", err)
		}
	})
}

// TestProviderForFileProvider pins that ProviderFor returns the FP adapter for a
// wired spec.
func TestProviderForFileProvider(t *testing.T) {
	a := startFakeFPApp(t)
	spec := testSpec()
	spec.FileProvider = fpSpecFor(a)
	p, err := ProviderFor(BackendFileProvider, spec)
	if err != nil {
		t.Fatalf("ProviderFor(fileprovider) = %v", err)
	}
	fp, ok := p.(*FileProviderProvider)
	if !ok {
		t.Fatalf("ProviderFor(fileprovider) = %T, want *FileProviderProvider", p)
	}
	if fp.Backend() != BackendFileProvider {
		t.Errorf("Backend() = %q, want fileprovider", fp.Backend())
	}
	if got := fp.PrivateRoot("/x/acct-01"); got != FusePrivateRoot("/x/acct-01") {
		t.Errorf("PrivateRoot = %q, want %q", got, FusePrivateRoot("/x/acct-01"))
	}
}

// TestProviderForFileProviderWithoutSpecFails pins that the FP backend with no
// FileProvider wiring fails loudly, never downgrades.
func TestProviderForFileProviderWithoutSpecFails(t *testing.T) {
	spec := testSpec() // FileProvider is nil
	if _, err := ProviderFor(BackendFileProvider, spec); err == nil {
		t.Error("ProviderFor(fileprovider) with nil FileProvider = nil error, want a loud failure")
	}
}
