package overlay

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/fusekit/content"
	"github.com/yasyf/fusekit/fileproviderd"
)

// synthEntry builds a Freshness-free synth manifest entry, so Fingerprint is a pure
// function of the entry fields (no filesystem lstat) — hermetic for these tests.
func synthEntry(name, version string) content.Entry {
	return content.Entry{Name: name, Kind: content.EntrySynth, Version: version}
}

// TestFileProviderProbeDomainShallow pins the provider's shallow probe: it forwards
// the listed verdict and surfaces the domain classes for the caller to key on.
func TestFileProviderProbeDomainShallow(t *testing.T) {
	t.Run("listed verdict forwards", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setProbeShallow(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: true, Listed: boolptrOV(true)}
		})
		p := newFileProvider(fpSpecFor(a))
		listed, err := p.ProbeDomainShallow(context.Background(), accountDir)
		if err != nil || !listed {
			t.Fatalf("ProbeDomainShallow = %v, %v; want true", listed, err)
		}
	})
	t.Run("unregistered domain is ErrNoDomain", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setProbeShallow(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassNoDomain, Error: "not registered"}
		})
		p := newFileProvider(fpSpecFor(a))
		_, err := p.ProbeDomainShallow(context.Background(), accountDir)
		if !errors.Is(err, fileproviderd.ErrNoDomain) {
			t.Fatalf("err = %v, want errors.Is ErrNoDomain", err)
		}
	})
}

// TestFileProviderPrepareDomain pins the provider's prepare-domain: success, a
// not-serving verdict, and that ErrOpUnsupported (an app too old to know the op) is
// prefixed with the provider's upgradeHint.
func TestFileProviderPrepareDomain(t *testing.T) {
	t.Run("completed materialization is nil", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setPrepare(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true} })
		p := newFileProvider(fpSpecFor(a))
		if err := p.PrepareDomain(context.Background(), accountDir, 5*time.Second); err != nil {
			t.Fatalf("PrepareDomain = %v, want nil", err)
		}
	})
	t.Run("not-serving is ErrDomainNotServing", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setPrepare(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassDomainNotServing, Error: "download timed out"}
		})
		p := newFileProvider(fpSpecFor(a))
		if err := p.PrepareDomain(context.Background(), accountDir, 5*time.Second); !errors.Is(err, fileproviderd.ErrDomainNotServing) {
			t.Fatalf("err = %v, want errors.Is ErrDomainNotServing", err)
		}
	})
	t.Run("old app is ErrOpUnsupported prefixed with the upgrade hint", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t) // prepare-domain unscripted -> unknown-op default arm
		spec := fpSpecFor(a)
		spec.UpgradeHint = "upgrade the cc-pool-status cask"
		p := newFileProvider(spec)
		err := p.PrepareDomain(context.Background(), accountDir, 5*time.Second)
		if !errors.Is(err, fileproviderd.ErrOpUnsupported) {
			t.Fatalf("err = %v, want errors.Is ErrOpUnsupported", err)
		}
		if !strings.Contains(err.Error(), "upgrade the cc-pool-status cask") {
			t.Errorf("err = %q, want it to carry the upgrade hint", err)
		}
	})
}

// TestFileProviderSignalOnChange pins the fingerprint-gated signal: a changed
// manifest signals, an unchanged one skips, a manifest error fails loud with no
// signal, and a failed signal is not recorded so the next Sync retries.
func TestFileProviderSignalOnChange(t *testing.T) {
	t.Run("changed fingerprint signals, unchanged skips", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
		src := newFakeSource()
		src.setManifest(accountDir, []content.Entry{synthEntry("x", "v1")})
		spec := fpSpecFor(a)
		spec.Source = src
		p := newFileProvider(spec)

		if err := p.Sync(base, accountDir); err != nil {
			t.Fatalf("Sync #1 = %v", err)
		}
		if got := a.signalCount(); got != 1 {
			t.Fatalf("after first Sync signalCount = %d, want 1", got)
		}
		if err := p.Sync(base, accountDir); err != nil {
			t.Fatalf("Sync #2 = %v", err)
		}
		if got := a.signalCount(); got != 1 {
			t.Fatalf("unchanged manifest still signalled: signalCount = %d, want 1", got)
		}
		src.setManifest(accountDir, []content.Entry{synthEntry("x", "v2")})
		if err := p.Sync(base, accountDir); err != nil {
			t.Fatalf("Sync #3 = %v", err)
		}
		if got := a.signalCount(); got != 2 {
			t.Fatalf("changed manifest did not signal: signalCount = %d, want 2", got)
		}
	})

	t.Run("manifest error fails loud, no signal", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
		src := newFakeSource()
		boom := errors.New("manifest boom")
		src.setManErr(boom)
		spec := fpSpecFor(a)
		spec.Source = src
		p := newFileProvider(spec)

		err := p.Sync(base, accountDir)
		if !errors.Is(err, boom) {
			t.Fatalf("Sync = %v, want the manifest error surfaced (errors.Is boom)", err)
		}
		if got := a.signalCount(); got != 0 {
			t.Errorf("manifest error still signalled: signalCount = %d, want 0", got)
		}
	})

	t.Run("failed signal is not recorded, next Sync retries", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		var signals int
		a.setSignal(func(string) fileproviderd.Response {
			signals++
			if signals == 1 {
				return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassAppUnreachable, Error: "down"}
			}
			return fileproviderd.Response{OK: true}
		})
		src := newFakeSource()
		src.setManifest(accountDir, []content.Entry{synthEntry("x", "v1")})
		spec := fpSpecFor(a)
		spec.Source = src
		p := newFileProvider(spec)

		if err := p.Sync(base, accountDir); !errors.Is(err, fileproviderd.ErrAppUnavailable) {
			t.Fatalf("Sync #1 = %v, want the transient signal failure surfaced", err)
		}
		if err := p.Sync(base, accountDir); err != nil {
			t.Fatalf("Sync #2 = %v, want nil (retry after the failed, unrecorded signal)", err)
		}
		if got := a.signalCount(); got != 2 {
			t.Fatalf("signalCount = %d, want 2 (the unchanged fingerprint retried because the first signal was never recorded)", got)
		}
	})
}

// TestFileProviderNilSourceUnconditional pins the documented opt-in default: with no
// Source wired, every Sync signals unconditionally (no fingerprint gate).
func TestFileProviderNilSourceUnconditional(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a)) // no Source
	for i := 0; i < 3; i++ {
		if err := p.Sync(base, accountDir); err != nil {
			t.Fatalf("Sync #%d = %v", i, err)
		}
	}
	if got := a.signalCount(); got != 3 {
		t.Fatalf("nil-Source signalCount = %d, want 3 (unconditional signal-every-Sync)", got)
	}
}

// TestFileProviderSurfacesAppUnavailable pins contract item 4: Sync and Health no
// longer swallow a Signal against a down app — ErrAppUnavailable surfaces so callers
// errors.Is-classify it.
func TestFileProviderSurfacesAppUnavailable(t *testing.T) {
	appDown := func(string) fileproviderd.Response {
		return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassAppUnreachable, Error: "down"}
	}
	t.Run("Sync surfaces ErrAppUnavailable", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setSignal(appDown)
		p := newFileProvider(fpSpecFor(a))
		if err := p.Sync(base, accountDir); !errors.Is(err, fileproviderd.ErrAppUnavailable) {
			t.Fatalf("Sync = %v, want errors.Is ErrAppUnavailable (no longer swallowed)", err)
		}
	})
	t.Run("Health surfaces ErrAppUnavailable", func(t *testing.T) {
		base, accountDir, domainRoot := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setPath(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
		a.setSignal(appDown)
		if err := fileproviderd.AtomicSymlink(accountDir, domainRoot); err != nil {
			t.Fatal(err)
		}
		p := newFileProvider(fpSpecFor(a))
		if err := p.Health(base, accountDir); !errors.Is(err, fileproviderd.ErrAppUnavailable) {
			t.Fatalf("Health = %v, want errors.Is ErrAppUnavailable (no longer swallowed)", err)
		}
	})
}

// TestFileProviderSignalBypassesCache pins that the exported Signal is the
// UNCONDITIONAL nudge: it signals even when the fingerprint is unchanged and never
// disturbs the fingerprint cache (a subsequent unchanged Sync still skips).
func TestFileProviderSignalBypassesCache(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
	src := newFakeSource()
	src.setManifest("acct-01", []content.Entry{synthEntry("x", "v1")})
	spec := fpSpecFor(a)
	spec.Source = src
	p := newFileProvider(spec)

	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("Sync #1 = %v", err)
	}
	if got := a.signalCount(); got != 1 {
		t.Fatalf("after Sync signalCount = %d, want 1", got)
	}
	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("Sync #2 = %v", err)
	}
	if got := a.signalCount(); got != 1 {
		t.Fatalf("unchanged Sync signalled: signalCount = %d, want 1", got)
	}
	if err := p.Signal(accountDir); err != nil {
		t.Fatalf("Signal = %v, want nil", err)
	}
	if got := a.signalCount(); got != 2 {
		t.Fatalf("exported Signal did not fire on an unchanged fingerprint: signalCount = %d, want 2", got)
	}
	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("Sync #3 = %v", err)
	}
	if got := a.signalCount(); got != 2 {
		t.Fatalf("exported Signal disturbed the cache: signalCount = %d, want 2", got)
	}
}

// TestFileProviderSignalManifestKeyedOnAccountDir pins the Source domain contract:
// signalIfChanged must call Manifest with the VERBATIM accountDir, never the basename
// domain — a basename would make a consumer's freshness paths relative and lstat to a
// stable "absent", freezing change detection after the first recorded signal.
func TestFileProviderSignalManifestKeyedOnAccountDir(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	if filepath.Base(accountDir) == accountDir {
		t.Fatalf("test premise broken: accountDir %q has no distinct basename", accountDir)
	}
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
	src := newFakeSource()
	src.setManifest(accountDir, []content.Entry{synthEntry("x", "v1")})
	spec := fpSpecFor(a)
	spec.Source = src
	p := newFileProvider(spec)

	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("Sync = %v", err)
	}
	got := src.manifestDomains()
	if len(got) == 0 {
		t.Fatal("Source.Manifest was never called")
	}
	for _, d := range got {
		if d != accountDir {
			t.Fatalf("Source.Manifest domain = %q, want the verbatim accountDir %q (not the basename)", d, accountDir)
		}
	}
}

// TestFileProviderSignalFreshnessChangeReSignals pins that a freshness-file change
// under a real absolute path shape flips the fingerprint and re-signals: the manifest
// entry is unchanged, but its Freshness file's (mtime_ns, size) moves.
func TestFileProviderSignalFreshnessChangeReSignals(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
	fresh := filepath.Join(base, "settings.local.json")
	if err := os.WriteFile(fresh, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	src := newFakeSource()
	src.setManifest(accountDir, []content.Entry{{Name: "settings.json", Kind: content.EntrySynth, Version: "v1", Freshness: []string{fresh}}})
	spec := fpSpecFor(a)
	spec.Source = src
	p := newFileProvider(spec)

	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("Sync #1 = %v", err)
	}
	if got := a.signalCount(); got != 1 {
		t.Fatalf("first Sync signalCount = %d, want 1", got)
	}
	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("Sync #2 = %v", err)
	}
	if got := a.signalCount(); got != 1 {
		t.Fatalf("unchanged freshness still signalled: signalCount = %d, want 1", got)
	}
	// Advance the freshness file's mtime a whole second so the (mtime_ns, size) tuple moves.
	fi, err := os.Lstat(fresh)
	if err != nil {
		t.Fatal(err)
	}
	next := fi.ModTime().Add(time.Second)
	if err := os.Chtimes(fresh, next, next); err != nil {
		t.Fatal(err)
	}
	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("Sync #3 = %v", err)
	}
	if got := a.signalCount(); got != 2 {
		t.Fatalf("freshness change did not re-signal: signalCount = %d, want 2", got)
	}
}

// TestFileProviderSignalCASNoStaleOverwrite pins the CAS record guard: a slow
// goroutine that computed an older fingerprint must not overwrite a fresher one a
// concurrent call already recorded. G1 (older v1) parks in a blocked Signal after
// computing its fingerprint; G2 (fresher v2) runs to completion and records v2; when
// G1 is released, its record is dropped so v2 stays.
func TestFileProviderSignalCASNoStaleOverwrite(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })

	var once sync.Once
	blocked := make(chan struct{})
	release := make(chan struct{})
	a.setSignal(func(string) fileproviderd.Response {
		first := false
		once.Do(func() { first = true })
		if first {
			close(blocked)
			<-release
		}
		return fileproviderd.Response{OK: true}
	})

	v1 := []content.Entry{synthEntry("x", "v1")}
	v2 := []content.Entry{synthEntry("x", "v2")}
	var mcalls int32
	src := newFakeSource()
	src.setManifestFunc(func(string) ([]content.Entry, error) {
		if atomic.AddInt32(&mcalls, 1) == 1 {
			return v1, nil
		}
		return v2, nil
	})
	spec := fpSpecFor(a)
	spec.Source = src
	p := newFileProvider(spec)

	g1done := make(chan error, 1)
	go func() { g1done <- p.Sync(base, accountDir) }()
	<-blocked // G1 computed fp(v1), parked in Signal, not yet recorded.

	// G2 runs to completion while G1 is parked: its Signal (the second call) does not
	// block, and it records v2.
	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("G2 Sync = %v", err)
	}
	close(release) // release G1; the CAS guard must drop its stale v1 record.
	if err := <-g1done; err != nil {
		t.Fatalf("G1 Sync = %v", err)
	}
	if got := a.signalCount(); got != 2 {
		t.Fatalf("signalCount = %d, want 2 (G1 + G2)", got)
	}

	// v2 must be the recorded fingerprint: a Sync that still sees v2 skips. Had G1
	// overwritten with v1, this Sync would observe a v1->v2 change and signal again.
	if err := p.Sync(base, accountDir); err != nil {
		t.Fatalf("verify Sync = %v", err)
	}
	if got := a.signalCount(); got != 2 {
		t.Fatalf("stale overwrite: verify Sync re-signalled, signalCount = %d, want 2 (v2 must stay recorded)", got)
	}
}
