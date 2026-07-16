package overlay

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit/fileproviderd"
)

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
		listed, err := p.ProbeDomainShallow(t.Context(), accountDir)
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
		_, err := p.ProbeDomainShallow(t.Context(), accountDir)
		if !errors.Is(err, fileproviderd.ErrNoDomain) {
			t.Fatalf("err = %v, want errors.Is ErrNoDomain", err)
		}
	})
}

// TestFileProviderPrepareDomain pins the provider's prepare-domain: success, a
// not-serving verdict, and that ErrOpUnsupported is prefixed with the upgrade hint.
func TestFileProviderPrepareDomain(t *testing.T) {
	t.Run("completed materialization is nil", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setPrepare(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true} })
		p := newFileProvider(fpSpecFor(a))
		if err := p.PrepareDomain(t.Context(), accountDir, 5*time.Second); err != nil {
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
		if err := p.PrepareDomain(t.Context(), accountDir, 5*time.Second); !errors.Is(err, fileproviderd.ErrDomainNotServing) {
			t.Fatalf("err = %v, want errors.Is ErrDomainNotServing", err)
		}
	})
	t.Run("old app is ErrOpUnsupported prefixed with the upgrade hint", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		spec := fpSpecFor(a)
		spec.UpgradeHint = "upgrade the cc-pool-status cask"
		p := newFileProvider(spec)
		err := p.PrepareDomain(t.Context(), accountDir, 5*time.Second)
		if !errors.Is(err, fileproviderd.ErrOpUnsupported) {
			t.Fatalf("err = %v, want errors.Is ErrOpUnsupported", err)
		}
		if !strings.Contains(err.Error(), "upgrade the cc-pool-status cask") {
			t.Errorf("err = %q, want it to carry the upgrade hint", err)
		}
	})
}

// TestFileProviderReconcileAndCheckDoNotNotify pins the capability split: structural
// convergence may register and probe, Check may issue only the zero-spawn State op,
// and neither path signals the enumerator.
func TestFileProviderReconcileAndCheckDoNotNotify(t *testing.T) {
	base, accountDir, domainRoot := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setRegister(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setProbe(func(string) fileproviderd.Response { return serving() })
	a.setPath(func(string) fileproviderd.Response { return fileproviderd.Response{OK: true, Path: domainRoot} })
	a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	if err := p.Reconcile(t.Context(), base, accountDir); err != nil {
		t.Fatalf("Reconcile = %v", err)
	}
	before := len(a.ops())
	if err := p.Check(t.Context(), base, accountDir); err != nil {
		t.Fatalf("Check = %v", err)
	}
	if got := a.signalCount(); got != 0 {
		t.Fatalf("Reconcile/Check signalCount = %d, want 0", got)
	}
	if got := a.ops()[before:]; len(got) != 1 || got[0] != fileproviderd.OpPath {
		t.Fatalf("Check ops = %v, want only zero-spawn path lookup", got)
	}
}

// TestFileProviderNotifyContentUnconditional pins ordinary notification at the
// provider boundary. Durable gating belongs to the caller, so every call signals.
func TestFileProviderNotifyContentUnconditional(t *testing.T) {
	_, accountDir, _ := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	for i := range 3 {
		if err := p.NotifyContent(t.Context(), accountDir); err != nil {
			t.Fatalf("NotifyContent #%d = %v", i+1, err)
		}
	}
	if got := a.signalCount(); got != 3 {
		t.Fatalf("signalCount = %d, want 3", got)
	}
}

func TestFileProviderSignalUnconditional(t *testing.T) {
	_, accountDir, _ := fpTestDirs(t)
	a := startFakeFPApp(t)
	a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
	p := newFileProvider(fpSpecFor(a))

	for i := range 3 {
		if err := p.Signal(t.Context(), accountDir); err != nil {
			t.Fatalf("Signal #%d = %v", i+1, err)
		}
	}
	if got := a.signalCount(); got != 3 {
		t.Fatalf("signalCount = %d, want 3", got)
	}
}

// TestFileProviderNotificationErrorsAndCancellation pins both notification paths:
// app unavailability remains classifiable and an already-canceled context performs
// no control request.
func TestFileProviderNotificationErrorsAndCancellation(t *testing.T) {
	t.Run("app unavailable", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setSignal(func(string) fileproviderd.Response {
			return fileproviderd.Response{OK: false, ErrClass: fileproviderd.ClassAppUnreachable, Error: "down"}
		})
		p := newFileProvider(fpSpecFor(a))
		if err := p.NotifyContent(t.Context(), accountDir); !errors.Is(err, fileproviderd.ErrAppUnavailable) {
			t.Fatalf("NotifyContent = %v, want ErrAppUnavailable", err)
		}
		if err := p.Signal(t.Context(), accountDir); !errors.Is(err, fileproviderd.ErrAppUnavailable) {
			t.Fatalf("Signal = %v, want ErrAppUnavailable", err)
		}
	})

	t.Run("canceled context sends nothing", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		a.setResponse(fileproviderd.OpSignal, fileproviderd.Response{OK: true})
		p := newFileProvider(fpSpecFor(a))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := p.NotifyContent(ctx, accountDir); !errors.Is(err, context.Canceled) {
			t.Fatalf("NotifyContent = %v, want context.Canceled", err)
		}
		if err := p.Signal(ctx, accountDir); !errors.Is(err, context.Canceled) {
			t.Fatalf("Signal = %v, want context.Canceled", err)
		}
		if got := a.signalCount(); got != 0 {
			t.Fatalf("canceled calls sent %d signals, want 0", got)
		}
	})
}
