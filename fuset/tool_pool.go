package fuset

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/daemonkit"
)

const (
	toolPoolMaxTotalRun    = 15 * time.Minute
	toolPoolMaxOutputBytes = 1 << 20
)

// ToolPoolConfig binds one FUSE install and packaging process scope to durable identity.
type ToolPoolConfig struct {
	ProcessStorePath string
}

// ToolPool owns the complete daemonkit process lifecycle for FUSE install and packaging commands.
type ToolPool struct {
	owned *daemonkit.Owned
	runMu sync.Mutex
}

// NewToolPool recovers prior FUSE tool children and opens a new ownership scope.
func NewToolPool(ctx context.Context, config ToolPoolConfig) (*ToolPool, error) {
	if ctx == nil {
		return nil, errors.New("fuset: tool pool context is required")
	}
	if !filepath.IsAbs(config.ProcessStorePath) || filepath.Clean(config.ProcessStorePath) != config.ProcessStorePath ||
		strings.ContainsRune(config.ProcessStorePath, 0) {
		return nil, errors.New("fuset: tool process store path must be exact and absolute")
	}
	owned, err := daemonkit.OwnProcesses(ctx, config.ProcessStorePath)
	if err != nil {
		return nil, err
	}
	return &ToolPool{owned: owned}, nil
}

// Run executes one FUSE tool command in the dedicated pool.
func (p *ToolPool) Run(ctx context.Context, command daemonkit.Cmd) (daemonkit.RunResult, error) {
	if p == nil || p.owned == nil {
		return daemonkit.RunResult{}, errors.New("fuset: tool pool is required")
	}
	runCtx, cancel := context.WithTimeout(ctx, toolPoolMaxTotalRun)
	defer cancel()
	p.runMu.Lock()
	defer p.runMu.Unlock()
	command.MaxOutput = toolPoolMaxOutputBytes
	return p.owned.Run(runCtx, command)
}

// Close terminally settles every FUSE tool child.
func (p *ToolPool) Close(ctx context.Context) error {
	if p == nil || p.owned == nil {
		return errors.New("fuset: tool pool is required")
	}
	return p.owned.Close(ctx)
}

var _ runner = (*ToolPool)(nil)
