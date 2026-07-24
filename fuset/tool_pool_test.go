package fuset

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

func TestToolWorkerConfigIsDedicatedInstallAndPackagingPolicy(t *testing.T) {
	want := worker.Config{
		Capacity: 1, QueueCapacity: 1, MaxTotalRun: 15 * time.Minute,
		MaxStdinBytes: 0, MaxStdoutBytes: 1 << 20, MaxStderrBytes: 1 << 20,
	}
	if got := toolWorkerConfig(); !reflect.DeepEqual(got, want) {
		t.Fatalf("toolWorkerConfig() = %+v, want %+v", got, want)
	}
}

func TestToolPoolOwnsRecoveryActivationAndSettlement(t *testing.T) {
	pool, err := NewToolPool(t.Context(), ToolPoolConfig{
		ProcessStorePath: filepath.Join(t.TempDir(), "fuse-tools.db"),
		Generation:       proc.OwnerGeneration{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := pool.Run(t.Context(), worker.CommandRequest{
		Path: "/usr/bin/true", Dir: "/", TotalTimeout: time.Minute,
	})
	if err != nil || result.ExitCode != 0 {
		t.Fatalf("Run true = %+v, %v", result, err)
	}
	if err := pool.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Run(t.Context(), worker.CommandRequest{
		Path: "/usr/bin/true", Dir: "/", TotalTimeout: time.Minute,
	}); !errors.Is(err, worker.ErrClosed) {
		t.Fatalf("Run after Close = %v, want worker.ErrClosed", err)
	}
}

func TestToolPoolRequiresExactDurableIdentity(t *testing.T) {
	if _, err := NewToolPool(nil, ToolPoolConfig{}); err == nil {
		t.Fatal("NewToolPool accepted nil context")
	}
	if _, err := NewToolPool(context.Background(), ToolPoolConfig{
		ProcessStorePath: "relative.db", Generation: proc.OwnerGeneration{1},
	}); err == nil {
		t.Fatal("NewToolPool accepted relative process store")
	}
	if _, err := NewToolPool(context.Background(), ToolPoolConfig{
		ProcessStorePath: filepath.Join(t.TempDir(), "fuse-tools.db"),
	}); err == nil {
		t.Fatal("NewToolPool accepted empty generation")
	}
}
