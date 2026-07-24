package tenant

import (
	"context"
	"errors"
)

// SourceMutationPlanner supplies product-specific semantic source planning.
type SourceMutationPlanner interface {
	PrepareSourceMutation(context.Context, SourceMutationStep) (SourceMutationOperation, error)
	ApplySourceMutation(context.Context, SourceMutationStep, SourceMutationOperation, SourceMutationContent) (SourceMutationApplyResult, error)
	SourceMutationCommitted(context.Context, SourceMutationCommit) error
}

// SourceMutationCommitted announces an already-durable catalog commit.
func (p StandardPlanner) SourceMutationCommitted(ctx context.Context, commit SourceMutationCommit) error {
	if p.SourceMutation == nil {
		return errors.New("tenant: source mutation planner is required")
	}
	return p.SourceMutation.SourceMutationCommitted(ctx, commit)
}

// ApplySourceMutation delegates the complete FuseKit-owned semantic source operation.
func (p StandardPlanner) ApplySourceMutation(ctx context.Context, step SourceMutationStep, operation SourceMutationOperation, content SourceMutationContent) (SourceMutationApplyResult, error) {
	if p.SourceMutation == nil {
		return SourceMutationApplyResult{}, errors.New("tenant: source mutation planner is required")
	}
	return p.SourceMutation.ApplySourceMutation(ctx, step, operation, content)
}

// StandardPlanner runs generic catalog materialization and delegates external source mutation planning.
type StandardPlanner struct {
	SourceMutation SourceMutationPlanner
}

// PrepareSourceMutation delegates the external source operation without exposing catalog state.
func (p StandardPlanner) PrepareSourceMutation(ctx context.Context, step SourceMutationStep) (SourceMutationOperation, error) {
	if p.SourceMutation == nil {
		return SourceMutationOperation{}, errors.New("tenant: source mutation planner is required")
	}
	return p.SourceMutation.PrepareSourceMutation(ctx, step)
}

func (p StandardPlanner) validate() error {
	if p.SourceMutation == nil {
		return errors.New("tenant: source mutation planner is required")
	}
	return nil
}

var _ Planner = StandardPlanner{}
