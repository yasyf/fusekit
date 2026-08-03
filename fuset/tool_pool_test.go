package fuset

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
)

func TestToolPoolOwnsRecoveryAndSettlement(t *testing.T) {
	pool, err := NewToolPool(toolPoolContext(t), ToolPoolConfig{
		ProcessStorePath: filepath.Join(t.TempDir(), "fuse-tools.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := pool.Run(toolPoolContext(t), daemonkit.Cmd{
		Path: "/usr/bin/true", Dir: "/", Exec: daemonkit.ServingSameUser(),
	})
	if err != nil || result.Exit.Code != 0 {
		t.Fatalf("Run true = %+v, %v", result, err)
	}
	if err := pool.Close(toolPoolContext(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Run(toolPoolContext(t), daemonkit.Cmd{
		Path: "/usr/bin/true", Dir: "/", Exec: daemonkit.ServingSameUser(),
	}); err == nil {
		t.Fatal("Run after Close succeeded")
	}
}

func TestToolPoolRequiresExactDurableIdentity(t *testing.T) {
	if _, err := NewToolPool(nil, ToolPoolConfig{}); err == nil {
		t.Fatal("NewToolPool accepted nil context")
	}
	if _, err := NewToolPool(context.Background(), ToolPoolConfig{
		ProcessStorePath: "relative.db",
	}); err == nil {
		t.Fatal("NewToolPool accepted relative process store")
	}
	if _, err := NewToolPool(context.Background(), ToolPoolConfig{
		ProcessStorePath: filepath.Join(t.TempDir(), "fuse-tools.db"),
	}); err == nil {
		t.Fatal("NewToolPool accepted a context without a deadline")
	}
}

func toolPoolContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
