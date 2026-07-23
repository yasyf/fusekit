package holder

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/yasyf/daemonkit/proc"
	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/catalogworker"
	"github.com/yasyf/fusekit/causal"
	"github.com/yasyf/fusekit/contentstream"
	"github.com/yasyf/fusekit/sourceauthority"
	"github.com/yasyf/fusekit/sourcedriverproto"
	"github.com/yasyf/fusekit/sourcedriverruntime"
	"github.com/yasyf/fusekit/sourcedriverservice"
	"github.com/yasyf/fusekit/tenant"
)

type semanticAuthorityFactory func(
	context.Context,
	SemanticDriverSpec,
	[]tenant.TenantSpec,
) (managedAuthority, error)

type semanticGenerationRuntime interface {
	Reconcile(context.Context) (sourcedriverruntime.ReconcileResult, error)
	ApplyPreparedMutation(context.Context, catalog.PreparedMutation, contentstream.Source) (sourcedriverruntime.MutationResult, error)
	SettleCommittedMutation(context.Context, catalog.MutationID) (sourcedriverruntime.MutationResult, error)
	RecoverCommittedReceipts(context.Context) error
	Close() error
}

type semanticGeneration struct {
	runtime *sourcedriverruntime.Runtime
	client  *sourcedriverservice.Client
	process *sourceChildProcess
	close   sync.Once
	err     error
}

func (g *semanticGeneration) Reconcile(ctx context.Context) (sourcedriverruntime.ReconcileResult, error) {
	return g.runtime.Reconcile(ctx)
}

func (g *semanticGeneration) ApplyPreparedMutation(
	ctx context.Context,
	prepared catalog.PreparedMutation,
	content contentstream.Source,
) (sourcedriverruntime.MutationResult, error) {
	return g.runtime.ApplyPreparedMutation(ctx, prepared, content)
}

func (g *semanticGeneration) SettleCommittedMutation(
	ctx context.Context,
	operation catalog.MutationID,
) (sourcedriverruntime.MutationResult, error) {
	return g.runtime.SettleCommittedMutation(ctx, operation)
}

func (g *semanticGeneration) RecoverCommittedReceipts(ctx context.Context) error {
	return g.runtime.RecoverCommittedReceipts(ctx)
}

func (g *semanticGeneration) Close() error {
	g.close.Do(func() {
		g.err = errors.Join(g.runtime.Close(), g.client.Close(), g.process.Stop(context.Background()))
	})
	return g.err
}

type semanticAuthority struct {
	store     *catalogworker.Manager
	authority causal.SourceAuthorityID
	launcher  sourceProcessLauncher
	fleet     SourceAuthorityFleet
	spec      SemanticDriverSpec

	lifecycle   sync.Mutex
	mu          sync.RWMutex
	generation  semanticGenerationRuntime
	targets     []catalog.SourceDriverTarget
	closing     bool
	open        func(context.Context, []catalog.SourceDriverTarget) (semanticGenerationRuntime, error)
	pending     func(context.Context, causal.SourceAuthorityID) (*catalog.SourceDriverCommittedReceipt, error)
	targetPage  func(context.Context, causal.SourceAuthorityID, causal.OperationID, catalog.TenantID, int) (catalog.SourceDriverTargetCheckpointPage, error)
	terminalErr error

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func newSemanticAuthority(
	ctx context.Context,
	store *catalogworker.Manager,
	launcher sourceProcessLauncher,
	fleet SourceAuthorityFleet,
	spec SemanticDriverSpec,
	tenants []tenant.TenantSpec,
) (*semanticAuthority, error) {
	authority := &semanticAuthority{
		store: store, authority: spec.Authority, launcher: launcher,
		fleet: fleet, spec: spec,
		closeDone: make(chan struct{}),
	}
	authority.open = authority.openGeneration
	authority.pending = store.PendingSourceDriverCommittedReceipt
	authority.targetPage = store.SourceDriverCommittedTargetCheckpoints
	targets, err := semanticTargets(tenants)
	if err != nil {
		return nil, err
	}
	if len(targets) == 0 {
		return authority, nil
	}
	generation, err := authority.open(ctx, targets)
	if err != nil {
		return nil, err
	}
	authority.generation = generation
	authority.targets = targets
	return authority, nil
}

func (a *semanticAuthority) openGeneration(
	ctx context.Context,
	targets []catalog.SourceDriverTarget,
) (semanticGenerationRuntime, error) {
	arguments, err := sourceDriverChildArguments(a.fleet, a.spec, targets)
	if err != nil {
		return nil, err
	}
	process, err := a.launcher.launch(ctx, arguments, proc.RecoverySourceDriver)
	if err != nil {
		return nil, err
	}
	client, err := sourcedriverservice.NewClient(ctx, wire.ClientConfig{
		Dial: process.Dial, WireBuild: sourcedriverproto.Build,
	})
	if err != nil {
		return nil, errors.Join(err, process.Stop(context.Background()))
	}
	runtime, err := sourcedriverruntime.NewRuntime(ctx, sourcedriverruntime.Config{
		Store: a.store, Driver: client, Authority: a.spec.Authority,
		FleetOwner: a.fleet.Owner, AuthorityGeneration: a.fleet.Generation,
		DeclarationDigest: a.spec.DeclarationDigest, Targets: targets,
	})
	if err != nil {
		return nil, errors.Join(err, client.Close(), process.Stop(context.Background()))
	}
	return &semanticGeneration{runtime: runtime, client: client, process: process}, nil
}

func semanticTargets(specs []tenant.TenantSpec) ([]catalog.SourceDriverTarget, error) {
	targets := make([]catalog.SourceDriverTarget, len(specs))
	for index, spec := range specs {
		targets[index] = catalog.SourceDriverTarget{Tenant: spec.ID, Generation: spec.Generation}
	}
	slices.SortFunc(targets, func(left, right catalog.SourceDriverTarget) int {
		return stringCompare(string(left.Tenant), string(right.Tenant))
	})
	if len(targets) == 0 {
		return nil, nil
	}
	if _, err := catalog.SourceDriverTargetsDigest(targets); err != nil {
		return nil, err
	}
	return targets, nil
}

func stringCompare(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func (a *semanticAuthority) Barrier(
	ctx context.Context,
	tenantID catalog.TenantID,
	generation catalog.Generation,
) (sourceauthority.BarrierResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.generation == nil || a.closing {
		return sourceauthority.BarrierResult{}, sourceauthority.ErrClosed
	}
	if _, err := a.generation.Reconcile(ctx); err != nil {
		return sourceauthority.BarrierResult{}, err
	}
	target, err := a.store.CurrentConvergenceTarget(ctx, tenantID, a.authority)
	if err != nil {
		return sourceauthority.BarrierResult{}, err
	}
	checkpoint, err := a.store.SourceDriverTargetCheckpoint(ctx, a.authority, tenantID, generation)
	if err != nil {
		return sourceauthority.BarrierResult{}, err
	}
	if target.Tenant != checkpoint.Tenant || target.CatalogRevision != checkpoint.CatalogRevision ||
		target.Change.SourceRevision != checkpoint.SourceRevision {
		return sourceauthority.BarrierResult{}, catalog.ErrIntegrity
	}
	return sourceauthority.BarrierResult{Target: target}, nil
}

func (a *semanticAuthority) PrepareSourceMutation(
	ctx context.Context,
	step tenant.SourceMutationStep,
) (tenant.SourceMutationOperation, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.generation == nil || a.closing {
		return tenant.SourceMutationOperation{}, sourceauthority.ErrClosed
	}
	prepared, err := a.store.PreparedMutation(ctx, step.TenantID, step.OperationID)
	if err != nil {
		return tenant.SourceMutationOperation{}, err
	}
	if err := validateSemanticStep(step, prepared); err != nil {
		return tenant.SourceMutationOperation{}, err
	}
	return tenant.SourceMutationOperation{
		OperationID: step.OperationID, SourceID: step.SourceID,
		SourceMetadata:     step.SourceMetadata,
		ExpectedSettlement: tenant.SourceMutationCatalogCommitted,
	}, nil
}

func (a *semanticAuthority) ApplySourceMutation(
	ctx context.Context,
	step tenant.SourceMutationStep,
	operation tenant.SourceMutationOperation,
	content tenant.SourceMutationContent,
) (tenant.SourceMutationApplyResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.generation == nil || a.closing {
		return tenant.SourceMutationApplyResult{}, sourceauthority.ErrClosed
	}
	prepared, err := a.store.PreparedMutation(ctx, step.TenantID, step.OperationID)
	if err != nil {
		return tenant.SourceMutationApplyResult{}, err
	}
	if err := validateSemanticStep(step, prepared); err != nil {
		return tenant.SourceMutationApplyResult{}, err
	}
	var sourceErr error
	var stream contentstream.Source
	if content != nil {
		defer func() { sourceErr = errors.Join(sourceErr, content.Close()) }()
		stream, sourceErr = content.Open(ctx)
		if sourceErr != nil {
			return tenant.SourceMutationApplyResult{}, sourceErr
		}
	}
	_, sourceErr = a.generation.ApplyPreparedMutation(ctx, prepared, stream)
	if sourceErr != nil {
		return tenant.SourceMutationApplyResult{}, sourceErr
	}
	return tenant.SourceMutationApplyResult{Settlement: tenant.SourceMutationCatalogCommitted}, nil
}

func validateSemanticStep(step tenant.SourceMutationStep, prepared catalog.PreparedMutation) error {
	if prepared.OperationID != step.OperationID || prepared.Tenant != step.TenantID ||
		prepared.Kind != step.Kind || prepared.ExpectedHead != step.ExpectedHead ||
		prepared.State != catalog.MutationApplying || prepared.Claim == nil || prepared.Source == nil ||
		prepared.Intent.SourceID != step.SourceID || prepared.Intent.SourceMetadata != step.SourceMetadata ||
		prepared.Intent.Origin != step.Origin || *prepared.Source != step.Source {
		return fmt.Errorf("%w: semantic source mutation step drifted", catalog.ErrIntegrity)
	}
	return nil
}

func (a *semanticAuthority) SourceMutationCommitted(ctx context.Context, commit tenant.SourceMutationCommit) error {
	if commit.OperationID == (catalog.MutationID{}) || commit.SourceID != string(a.authority) {
		return catalog.ErrIntegrity
	}
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.generation == nil || a.closing {
		return sourceauthority.ErrClosed
	}
	_, err := a.generation.SettleCommittedMutation(ctx, commit.OperationID)
	return err
}

func (a *semanticAuthority) RecoverCommittedReceipts(ctx context.Context) error {
	a.lifecycle.Lock()
	defer a.lifecycle.Unlock()
	a.mu.RLock()
	if a.closing {
		a.mu.RUnlock()
		return sourceauthority.ErrClosed
	}
	if a.generation != nil {
		defer a.mu.RUnlock()
		return a.generation.RecoverCommittedReceipts(ctx)
	}
	a.mu.RUnlock()
	if a.pending == nil {
		return errors.New("FuseKit runtime: semantic receipt lookup is unavailable")
	}
	receipt, err := a.pending(ctx, a.authority)
	if err != nil || receipt == nil {
		return err
	}
	targets, err := a.semanticReceiptTargets(ctx, *receipt)
	if err != nil {
		return err
	}
	recovery, err := a.open(ctx, targets)
	if err != nil {
		return err
	}
	return errors.Join(recovery.RecoverCommittedReceipts(ctx), closeSemanticGeneration(recovery))
}

func (a *semanticAuthority) semanticReceiptTargets(
	ctx context.Context,
	receipt catalog.SourceDriverCommittedReceipt,
) ([]catalog.SourceDriverTarget, error) {
	identity := receipt.Result.Identity
	if identity.TargetCount == 0 || identity.TargetCount > catalog.SourceDriverTargetLimit {
		return nil, catalog.ErrIntegrity
	}
	targets := make([]catalog.SourceDriverTarget, 0, identity.TargetCount)
	var after catalog.TenantID
	for {
		page, err := a.targetPage(
			ctx, identity.Authority, identity.Operation, after,
			catalog.SourceDriverTargetCheckpointPageLimit,
		)
		if err != nil {
			return nil, err
		}
		for _, checkpoint := range page.Targets {
			targets = append(targets, checkpoint.SourceDriverTarget)
		}
		if page.Next == "" {
			break
		}
		after = page.Next
	}
	if uint64(len(targets)) != identity.TargetCount {
		return nil, catalog.ErrIntegrity
	}
	digest, err := catalog.SourceDriverTargetsDigest(targets)
	if err != nil || digest != identity.TargetsDigest {
		return nil, errors.Join(catalog.ErrIntegrity, err)
	}
	return targets, nil
}

func (a *semanticAuthority) Reconfigure(ctx context.Context, tenants []tenant.TenantSpec) error {
	targets, err := semanticTargets(tenants)
	if err != nil {
		return err
	}
	a.lifecycle.Lock()
	defer a.lifecycle.Unlock()
	a.mu.Lock()
	if a.closing {
		a.mu.Unlock()
		return sourceauthority.ErrClosed
	}
	if slices.Equal(a.targets, targets) {
		a.mu.Unlock()
		return nil
	}
	prior := a.generation
	if prior != nil {
		if err := prior.RecoverCommittedReceipts(ctx); err != nil {
			a.mu.Unlock()
			return err
		}
	}
	a.generation, a.targets = nil, nil
	a.mu.Unlock()
	if err := closeSemanticGeneration(prior); err != nil {
		a.mu.Lock()
		a.closing = true
		a.terminalErr = errors.Join(a.terminalErr, err)
		a.mu.Unlock()
		return err
	}
	if len(targets) == 0 {
		return nil
	}
	generation, err := a.open(ctx, targets)
	if err != nil {
		return err
	}
	a.mu.Lock()
	if a.closing {
		a.mu.Unlock()
		closeErr := closeSemanticGeneration(generation)
		a.mu.Lock()
		a.terminalErr = errors.Join(a.terminalErr, closeErr)
		a.mu.Unlock()
		return errors.Join(sourceauthority.ErrClosed, closeErr)
	}
	a.generation = generation
	a.targets = append([]catalog.SourceDriverTarget(nil), targets...)
	a.mu.Unlock()
	return nil
}

func (a *semanticAuthority) Close(ctx context.Context) error {
	a.closeOnce.Do(func() {
		go func() {
			a.lifecycle.Lock()
			defer a.lifecycle.Unlock()
			a.mu.Lock()
			a.closing = true
			generation := a.generation
			terminalErr := a.terminalErr
			var recoveryErr error
			if generation != nil {
				recoveryErr = generation.RecoverCommittedReceipts(context.Background())
			}
			a.generation, a.targets = nil, nil
			a.mu.Unlock()
			a.closeErr = errors.Join(terminalErr, recoveryErr, closeSemanticGeneration(generation))
			close(a.closeDone)
		}()
	})
	select {
	case <-a.closeDone:
		return a.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *semanticAuthority) Cancel() {
	a.mu.Lock()
	a.closing = true
	generation := a.generation
	a.mu.Unlock()
	if generation != nil {
		if err := generation.Close(); err != nil {
			a.mu.Lock()
			a.terminalErr = errors.Join(a.terminalErr, err)
			a.mu.Unlock()
		}
	}
}

func (a *semanticAuthority) Wait(ctx context.Context) error { return a.Close(ctx) }

func closeSemanticGeneration(generation semanticGenerationRuntime) error {
	if generation == nil {
		return nil
	}
	return generation.Close()
}
