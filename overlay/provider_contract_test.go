package overlay

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestProviderCapabilities(t *testing.T) {
	providers := []Provider{
		&SymlinkProvider{},
		&RemoteFuseProvider{},
		&FileProviderProvider{},
	}
	for _, provider := range providers {
		_, notifies := provider.(ContentNotifier)
		want := provider.Backend() == BackendFileProvider
		if notifies != want {
			t.Errorf("%T ContentNotifier = %v, want %v", provider, notifies, want)
		}
		for _, old := range []string{"Setup", "Sync", "Health"} {
			if _, ok := reflect.TypeOf(provider).MethodByName(old); ok {
				t.Errorf("%T still exposes removed Provider method %s", provider, old)
			}
		}
	}
}

func TestProviderCanceledContextDoesNotMutate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("symlink", func(t *testing.T) {
		base := t.TempDir()
		accountDir := filepath.Join(t.TempDir(), "acct-01")
		p := &SymlinkProvider{Spec: Spec{
			IsPrivate: func(string) bool { return false },
			Shared:    map[string]bool{"projects": true},
		}}
		if err := p.Reconcile(ctx, base, accountDir); !errors.Is(err, context.Canceled) {
			t.Fatalf("Reconcile = %v, want context.Canceled", err)
		}
		if _, err := os.Lstat(filepath.Join(base, "projects")); !os.IsNotExist(err) {
			t.Fatalf("canceled Reconcile materialized shared entry: %v", err)
		}
		if err := p.Check(ctx, base, accountDir); !errors.Is(err, context.Canceled) {
			t.Fatalf("Check = %v, want context.Canceled", err)
		}
		if _, err := p.Teardown(ctx, base, accountDir); !errors.Is(err, context.Canceled) {
			t.Fatalf("Teardown = %v, want context.Canceled", err)
		}
	})

	t.Run("remote fuse", func(t *testing.T) {
		p := &RemoteFuseProvider{}
		if err := p.Reconcile(ctx, "/base", "/account"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Reconcile = %v, want context.Canceled", err)
		}
		if err := p.Check(ctx, "/base", "/account"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Check = %v, want context.Canceled", err)
		}
		if _, err := p.Teardown(ctx, "/base", "/account"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Teardown = %v, want context.Canceled", err)
		}
	})

	t.Run("file provider", func(t *testing.T) {
		_, accountDir, _ := fpTestDirs(t)
		a := startFakeFPApp(t)
		p := newFileProvider(fpSpecFor(a))
		if err := p.Reconcile(ctx, "/base", accountDir); !errors.Is(err, context.Canceled) {
			t.Fatalf("Reconcile = %v, want context.Canceled", err)
		}
		if err := p.Check(ctx, "/base", accountDir); !errors.Is(err, context.Canceled) {
			t.Fatalf("Check = %v, want context.Canceled", err)
		}
		if _, err := p.Teardown(ctx, "/base", accountDir); !errors.Is(err, context.Canceled) {
			t.Fatalf("Teardown = %v, want context.Canceled", err)
		}
		if got := len(a.seen()); got != 0 {
			t.Fatalf("canceled operations sent %d control requests, want 0", got)
		}
	})
}
