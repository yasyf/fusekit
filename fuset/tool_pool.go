package fuset

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"time"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/worker"
)

const (
	toolPoolCapacity       = 1
	toolPoolQueueCapacity  = 1
	toolPoolMaxTotalRun    = 15 * time.Minute
	toolPoolMaxOutputBytes = 1 << 20
)

// ToolPoolConfig binds one FUSE install and packaging worker pool to durable process identity.
type ToolPoolConfig struct {
	ProcessStorePath string
	Generation       proc.OwnerGeneration
}

// ToolPool owns the complete daemonkit worker and process lifecycle for FUSE install and packaging commands.
type ToolPool struct {
	claim *worker.RuntimeClaim
}

// NewToolPool recovers prior FUSE tool children and activates one exact worker generation.
func NewToolPool(ctx context.Context, config ToolPoolConfig) (*ToolPool, error) {
	if ctx == nil {
		return nil, errors.New("fuset: tool pool context is required")
	}
	if !filepath.IsAbs(config.ProcessStorePath) || filepath.Clean(config.ProcessStorePath) != config.ProcessStorePath ||
		strings.ContainsRune(config.ProcessStorePath, 0) {
		return nil, errors.New("fuset: tool process store path must be exact and absolute")
	}
	if config.Generation == (proc.OwnerGeneration{}) {
		return nil, errors.New("fuset: tool process generation is required")
	}
	reaper := &proc.Reaper{
		Store: &proc.FileStore{Path: config.ProcessStorePath}, Generation: config.Generation,
	}
	pool, err := worker.NewPool(toolWorkerConfig(), reaper)
	if err != nil {
		return nil, err
	}
	claim, err := pool.ClaimRuntime()
	if err != nil {
		return nil, err
	}
	if err := claim.Recover(ctx); err != nil {
		return nil, err
	}
	if err := claim.Activate(); err != nil {
		return nil, errors.Join(err, claim.Release(ctx))
	}
	return &ToolPool{claim: claim}, nil
}

// Run executes one FUSE tool command in the dedicated pool.
func (p *ToolPool) Run(ctx context.Context, request worker.CommandRequest) (worker.CommandResult, error) {
	if p == nil || p.claim == nil {
		return worker.CommandResult{}, errors.New("fuset: tool pool is required")
	}
	return p.claim.Product().Run(ctx, request)
}

// Close terminally settles every FUSE tool child.
func (p *ToolPool) Close(ctx context.Context) error {
	if p == nil || p.claim == nil {
		return errors.New("fuset: tool pool is required")
	}
	return p.claim.Close(ctx)
}

func toolWorkerConfig() worker.Config {
	return worker.Config{
		Capacity: toolPoolCapacity, QueueCapacity: toolPoolQueueCapacity,
		MaxTotalRun: toolPoolMaxTotalRun, MaxStdinBytes: 0,
		MaxStdoutBytes: toolPoolMaxOutputBytes, MaxStderrBytes: toolPoolMaxOutputBytes,
	}
}

var _ runner = (*ToolPool)(nil)
