package sourcedriverruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/contentstream"
)

// NewRuntime recovers any catalog-owned stage before admitting new work.
func NewRuntime(ctx context.Context, config Config) (*Runtime, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	config = normalizeConfig(config)
	runtimeCtx, cancel := context.WithCancel(context.Background())
	r := &Runtime{
		config: config, ctx: runtimeCtx, cancel: cancel, done: make(chan struct{}),
		refreshes: make(chan refreshRequest, 64), mutations: make(chan mutationRequest, 64),
		settlements: make(chan settlementRequest, 64),
	}
	if err := r.recoverCommittedReceipts(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("source driver runtime: recover committed receipts: %w", err)
	}
	for {
		_, err := r.recoverPending(ctx)
		if errors.Is(err, errProgressPending) {
			continue
		}
		if err != nil {
			var pending *retainedMutationPendingError
			if !errors.As(err, &pending) {
				cancel()
				return nil, fmt.Errorf("source driver runtime: recover pending stage: %w", err)
			}
			mutation := pending.Mutation
			r.retainedMutation = &mutation
		}
		break
	}
	if _, err := r.alignCheckpoint(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("source driver runtime: align checkpoint: %w", err)
	}
	go r.run()
	return r, nil
}

// RecoverCommittedReceipts settles every durable catalog result before newer authority work.
func (r *Runtime) RecoverCommittedReceipts(ctx context.Context) error {
	_, err := r.submitSettlement(ctx, nil)
	return err
}

// SettleCommittedMutation replays and settles one exact committed mutation result.
func (r *Runtime) SettleCommittedMutation(ctx context.Context, mutation catalog.MutationID) (MutationResult, error) {
	if mutation == (catalog.MutationID{}) {
		return MutationResult{}, fmt.Errorf("%w: empty committed mutation", catalog.ErrInvalidObject)
	}
	return r.submitSettlement(ctx, &mutation)
}

func (r *Runtime) submitSettlement(ctx context.Context, mutation *catalog.MutationID) (MutationResult, error) {
	request := settlementRequest{mutation: mutation, result: make(chan settlementResponse, 1)}
	select {
	case <-ctx.Done():
		return MutationResult{}, ctx.Err()
	case <-r.done:
		return MutationResult{}, ErrClosed
	case r.settlements <- request:
	}
	select {
	case <-ctx.Done():
		return MutationResult{}, ctx.Err()
	case <-r.done:
		select {
		case response := <-request.result:
			return response.result, response.err
		default:
			return MutationResult{}, ErrClosed
		}
	case response := <-request.result:
		return response.result, response.err
	}
}

// Reconcile refreshes the source and waits for its latest immutable head to be
// durably published. Concurrent requests coalesce onto one refresh.
func (r *Runtime) Reconcile(ctx context.Context) (ReconcileResult, error) {
	request := refreshRequest{result: make(chan refreshResponse, 1)}
	select {
	case <-ctx.Done():
		return ReconcileResult{}, ctx.Err()
	case <-r.done:
		return ReconcileResult{}, ErrClosed
	case r.refreshes <- request:
	}
	select {
	case <-ctx.Done():
		return ReconcileResult{}, ctx.Err()
	case <-r.done:
		select {
		case response := <-request.result:
			return response.result, response.err
		default:
			return ReconcileResult{}, ErrClosed
		}
	case response := <-request.result:
		return response.result, response.err
	}
}

// ApplyPreparedMutation applies one claimed catalog mutation to the source and
// atomically settles its source delta, checkpoint, and namespace result.
// Content ownership transfers only after the request is admitted.
func (r *Runtime) ApplyPreparedMutation(
	ctx context.Context,
	prepared catalog.PreparedMutation,
	content contentstream.Source,
) (MutationResult, error) {
	if err := ctx.Err(); err != nil {
		return MutationResult{}, errors.Join(err, settleUnused(content, err))
	}
	request := mutationRequest{prepared: prepared, content: content, result: make(chan mutationResponse, 1)}
	select {
	case <-ctx.Done():
		return MutationResult{}, ctx.Err()
	case <-r.done:
		return MutationResult{}, ErrClosed
	case r.mutations <- request:
	}
	select {
	case <-ctx.Done():
		return MutationResult{}, ctx.Err()
	case <-r.done:
		select {
		case response := <-request.result:
			return response.result, response.err
		default:
			return MutationResult{}, ErrClosed
		}
	case response := <-request.result:
		return response.result, response.err
	}
}

// Close stops admission and joins the semantic lane.
func (r *Runtime) Close() error {
	r.closeOnce.Do(r.cancel)
	<-r.done
	return nil
}

func (r *Runtime) run() {
	defer close(r.done)
	var refreshes []refreshRequest
	var mutation *mutationRequest
	for {
		if mutation != nil {
			select {
			case <-r.ctx.Done():
				mutation.result <- mutationResponse{err: ErrClosed}
				r.rejectQueued()
				return
			default:
			}
			result, err := r.applyPreparedMutation(r.ctx, mutation.prepared, mutation.content)
			if errors.Is(err, errProgressPending) {
				continue
			}
			if err == nil {
				r.retainedMutation = nil
			}
			mutation.result <- mutationResponse{result: result, err: err}
			mutation = nil
			continue
		}
		if len(refreshes) != 0 {
			select {
			case <-r.ctx.Done():
				r.rejectRefreshes(refreshes, ErrClosed)
				r.rejectQueued()
				return
			case request := <-r.refreshes:
				refreshes = append(refreshes, request)
			default:
			}
			var result ReconcileResult
			var err error
			if r.retainedMutation != nil {
				err = ErrRetainedMutationLiability
			} else {
				result, err = r.reconcile(r.ctx)
			}
			if errors.Is(err, errProgressPending) {
				continue
			}
			for _, request := range refreshes {
				request.result <- refreshResponse{result: result, err: err}
			}
			refreshes = nil
			continue
		}
		select {
		case <-r.ctx.Done():
			r.rejectQueued()
			return
		case request := <-r.mutations:
			var result MutationResult
			var err error
			if r.retainedMutation != nil && request.prepared.OperationID != *r.retainedMutation {
				err = errors.Join(ErrRetainedMutationLiability, settleUnused(request.content, ErrRetainedMutationLiability))
			} else {
				result, err = r.applyPreparedMutation(r.ctx, request.prepared, request.content)
				if err == nil {
					r.retainedMutation = nil
				}
			}
			if errors.Is(err, errProgressPending) {
				mutation = &request
			} else {
				request.result <- mutationResponse{result: result, err: err}
			}
		case settlement := <-r.settlements:
			var result MutationResult
			var err error
			if settlement.mutation == nil {
				err = r.recoverCommittedReceipts(r.ctx)
			} else {
				result, err = r.settleCommittedMutation(r.ctx, *settlement.mutation)
			}
			settlement.result <- settlementResponse{result: result, err: err}
		case request := <-r.refreshes:
			refreshes = append(refreshes, request)
		}
	}
}

func (r *Runtime) rejectRefreshes(requests []refreshRequest, err error) {
	for _, request := range requests {
		request.result <- refreshResponse{err: err}
	}
}

func (r *Runtime) rejectQueued() {
	for {
		select {
		case request := <-r.refreshes:
			request.result <- refreshResponse{err: ErrClosed}
		case request := <-r.mutations:
			settleErr := settleUnused(request.content, ErrClosed)
			request.result <- mutationResponse{err: errors.Join(ErrClosed, settleErr)}
		case request := <-r.settlements:
			request.result <- settlementResponse{err: ErrClosed}
		default:
			return
		}
	}
}
