package sourceauthority

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"reflect"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

type mutationPayload struct {
	file *os.File
	size int64
	hash [32]byte
}

type mutationPayloadSet struct {
	actions []*mutationPayload
	request *mutationPayload
}

func (e *supervisedExecutor) ApplyMutation(ctx context.Context, task MutationTask) (_ MutationReceipt, resultErr error) {
	if task.Content != nil {
		defer func() { resultErr = errors.Join(resultErr, task.Content.Close()) }()
	}
	ctx, cancel := context.WithTimeout(ctx, e.operationDeadlines().Mutation)
	defer cancel()
	request, actionData, actionSizes, err := encodeMutationRequest(task)
	if err != nil {
		return MutationReceipt{}, err
	}
	emit := sourceTaskPageEmitterForMutation(task, actionSizes)
	process, client, temporary, err := e.start(ctx)
	if err != nil {
		return MutationReceipt{}, err
	}
	payload, err := encodeSourceTaskRequest(request)
	if err != nil {
		return MutationReceipt{}, e.failTask(process, client, temporary, err)
	}
	call, err := client.OpenStream(ctx, sourceTaskOpMutation, "", payload, false)
	if err != nil {
		return MutationReceipt{}, e.failTask(process, client, temporary, err)
	}
	if err := sendSourceTaskPages(ctx, call, request.Config, emit); err != nil {
		return MutationReceipt{}, e.failCall(process, client, call, temporary, err)
	}
	for index, data := range actionData {
		if len(data) == 0 {
			continue
		}
		if err := sendMutationBytes(ctx, call, sourceTaskChunkAction, uint32(index), data); err != nil {
			return MutationReceipt{}, e.failCall(process, client, call, temporary, err)
		}
	}
	if task.Content != nil {
		reader, err := task.Content.Open(ctx)
		if err != nil {
			return MutationReceipt{}, e.failCall(process, client, call, temporary, err)
		}
		err = sendMutationReader(ctx, call, sourceTaskChunkRequest, 0, reader)
		err = errors.Join(err, reader.Settle(err), reader.Wait(context.WithoutCancel(ctx)))
		if err != nil {
			return MutationReceipt{}, e.failCall(process, client, call, temporary, err)
		}
	}
	if err := call.CloseSend(ctx); err != nil {
		return MutationReceipt{}, e.failCall(process, client, call, temporary, err)
	}
	result, err := call.Response(ctx)
	if err != nil {
		return MutationReceipt{}, e.failTask(process, client, temporary, err)
	}
	if err := ctx.Err(); err != nil {
		return MutationReceipt{}, e.failTask(process, client, temporary, err)
	}
	var response sourceTaskMutationResponse
	if err := decodeSourceTaskResult(result, &response); err != nil {
		return MutationReceipt{}, e.failTask(process, client, temporary, err)
	}
	if response.Protocol != sourceTaskProtocol || response.Receipt.OperationID != task.OperationID ||
		response.Receipt.Digest == (Fingerprint{}) {
		return MutationReceipt{}, e.failTask(process, client, temporary, errors.New("sourceauthority: invalid source mutation response"))
	}
	if err := e.finishTask(process, client, temporary); err != nil {
		return MutationReceipt{}, err
	}
	return response.Receipt, nil
}

func (e *supervisedExecutor) AcknowledgeMutation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	authorityGeneration causal.Generation,
	operation catalog.MutationID,
	digest Fingerprint,
) error {
	return e.runMutationTerminal(ctx, sourceTaskOpMutationAck, MutationTerminalProof{
		Authority: authority, AuthorityGeneration: authorityGeneration,
		Operation: operation, Outcome: MutationAcknowledged, Digest: digest,
	})
}

func (e *supervisedExecutor) InspectMutation(
	ctx context.Context,
	request MutationInspectionRequest,
) (MutationInspection, error) {
	if err := validateMutationInspectionRequest(request); err != nil {
		return MutationInspection{}, err
	}
	var response sourceTaskMutationInspectionResponse
	if err := e.runUnaryWithin(ctx, e.operationDeadlines().Mutation, sourceTaskOpMutationGet,
		sourceTaskMutationInspectionRequest{Protocol: sourceTaskProtocol, Request: request}, &response); err != nil {
		return MutationInspection{}, err
	}
	if response.Protocol != sourceTaskProtocol || validateMutationInspection(request, response.Inspection) != nil {
		return MutationInspection{}, errors.New("sourceauthority: invalid mutation inspection response")
	}
	return response.Inspection, nil
}

func (e *supervisedExecutor) AbandonMutation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	authorityGeneration causal.Generation,
	operation catalog.MutationID,
) error {
	return e.runMutationTerminal(ctx, sourceTaskOpMutationDrop, MutationTerminalProof{
		Authority: authority, AuthorityGeneration: authorityGeneration,
		Operation: operation, Outcome: MutationAbandoned,
	})
}

func (e *supervisedExecutor) MutationTerminalProofPage(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	after catalog.MutationID,
	limit int,
) (MutationTerminalProofPage, error) {
	if authority == "" || len(authority) > sourceTaskStringByteLimit ||
		limit < 1 || limit > MutationTerminalProofPageLimit {
		return MutationTerminalProofPage{}, errors.New("sourceauthority: invalid mutation proof page request")
	}
	ctx, cancel := context.WithTimeout(ctx, e.operationDeadlines().Mutation)
	defer cancel()
	process, client, temporary, err := e.start(ctx)
	if err != nil {
		return MutationTerminalProofPage{}, err
	}
	payload, err := encodeSourceTaskRequest(
		sourceTaskMutationProofsRequest{
			Protocol: sourceTaskProtocol, Authority: authority, After: after, Limit: uint16(limit),
		},
	)
	if err != nil {
		return MutationTerminalProofPage{}, e.failTask(process, client, temporary, err)
	}
	call, err := client.OpenStream(ctx, sourceTaskOpMutationList, "", payload, true)
	if err != nil {
		return MutationTerminalProofPage{}, e.failTask(process, client, temporary, err)
	}
	proofs := make([]MutationTerminalProof, 0, limit)
	var cursor uint32
	var digest Fingerprint
	for chunk := range call.Chunks() {
		if chunk.End {
			continue
		}
		if cursor != 0 {
			return MutationTerminalProofPage{}, e.failCall(
				process, client, call, temporary, errors.New("sourceauthority: mutation proof page response is unbounded"),
			)
		}
		page, err := decodeSourceTaskMutationProofPage(chunk.Payload, cursor, digest)
		if err != nil {
			return MutationTerminalProofPage{}, e.failCall(process, client, call, temporary, err)
		}
		for _, proof := range page.Proofs {
			if validateMutationTerminalProof(proof) != nil || proof.Authority != authority ||
				proof.Operation.String() <= after.String() ||
				(len(proofs) > 0 && proofs[len(proofs)-1].Operation.String() >= proof.Operation.String()) {
				return MutationTerminalProofPage{}, e.failCall(process, client, call, temporary,
					errors.New("sourceauthority: invalid or unordered mutation terminal proof response"))
			}
			proofs = append(proofs, proof)
			if len(proofs) > limit {
				return MutationTerminalProofPage{}, e.failCall(process, client, call, temporary,
					errors.New("sourceauthority: mutation terminal proof count exceeds its limit"))
			}
		}
		cursor++
		digest = page.Digest
	}
	result, err := call.Response(ctx)
	if err != nil {
		return MutationTerminalProofPage{}, e.failTask(process, client, temporary, err)
	}
	var response sourceTaskMutationProofsResponse
	if err := decodeSourceTaskResult(result, &response); err != nil {
		return MutationTerminalProofPage{}, e.failTask(process, client, temporary, err)
	}
	if err := validateSourceTaskMutationProofTerminal(response, cursor, digest, len(proofs)); err != nil {
		return MutationTerminalProofPage{}, e.failTask(process, client, temporary,
			err)
	}
	if response.Error != "" {
		return MutationTerminalProofPage{}, e.failTask(process, client, temporary, errors.New(response.Error))
	}
	if response.Next != (catalog.MutationID{}) && response.Next.String() <= after.String() ||
		response.More && response.Next == (catalog.MutationID{}) ||
		response.Next == (catalog.MutationID{}) && len(proofs) != 0 ||
		len(proofs) != 0 && proofs[len(proofs)-1].Operation.String() > response.Next.String() {
		return MutationTerminalProofPage{}, e.failTask(
			process, client, temporary, errors.New("sourceauthority: invalid mutation proof continuation"),
		)
	}
	if err := e.finishTask(process, client, temporary); err != nil {
		return MutationTerminalProofPage{}, err
	}
	return MutationTerminalProofPage{Proofs: proofs, Next: response.Next, More: response.More}, nil
}

func sourceTaskProofPageCount(count uint32) uint32 {
	return (count + sourceTaskProofPageItemLimit - 1) / sourceTaskProofPageItemLimit
}

func validateSourceTaskMutationProofPartition(cursor uint32, previousCount int) error {
	if cursor >= sourceTaskPageLimit ||
		(cursor > 0 && previousCount != sourceTaskProofPageItemLimit) {
		return errors.New("sourceauthority: mutation proof pages are not canonically partitioned")
	}
	return nil
}

func decodeSourceTaskMutationProofPage(
	payload []byte,
	cursor uint32,
	previous Fingerprint,
) (sourceTaskMutationProofPage, error) {
	if len(payload) == 0 || len(payload) > sourceTaskPageByteLimit {
		return sourceTaskMutationProofPage{},
			errors.New("sourceauthority: mutation proof page exceeds its byte limit")
	}
	var page sourceTaskMutationProofPage
	if err := decodeSourceTaskBounded(payload, &page, sourceTaskPageByteLimit); err != nil {
		return sourceTaskMutationProofPage{}, err
	}
	expected, _, err := encodeSourceTaskMutationProofPage(cursor, previous, page.Proofs)
	if err != nil || page.Protocol != sourceTaskProtocol || page.Cursor != cursor ||
		page.Previous != previous || page.Digest != expected.Digest {
		return sourceTaskMutationProofPage{},
			errors.New("sourceauthority: mutation proof page identity is invalid")
	}
	return page, nil
}

func validateSourceTaskMutationProofTerminal(
	response sourceTaskMutationProofsResponse,
	pages uint32,
	digest Fingerprint,
	proofs int,
) error {
	if response.Protocol != sourceTaskProtocol || response.Count != uint32(proofs) ||
		pages != sourceTaskProofPageCount(response.Count) ||
		response.Digest != digest || len(response.Error) > sourceTaskErrorByteLimit ||
		validateSourceTaskStrings(reflect.ValueOf(response)) != nil {
		return errors.New("sourceauthority: invalid mutation terminal proof response")
	}
	return nil
}

func (e *supervisedExecutor) ForgetMutation(
	ctx context.Context,
	authority causal.SourceAuthorityID,
	proof MutationTerminalProof,
) error {
	if proof.Authority != authority || validateMutationTerminalProof(proof) != nil {
		return errors.New("sourceauthority: mutation forget escaped its authority")
	}
	return e.runMutationTerminal(ctx, sourceTaskOpMutationGC, proof)
}

func (e *supervisedExecutor) runMutationTerminal(ctx context.Context, op wire.Op, proof MutationTerminalProof) error {
	if err := validateMutationTerminalProof(proof); err != nil {
		return errors.New("sourceauthority: invalid mutation terminal proof identity")
	}
	var response sourceTaskMutationTerminalResponse
	if err := e.runUnaryWithin(ctx, e.operationDeadlines().Mutation, op,
		sourceTaskMutationTerminalRequest{Protocol: sourceTaskProtocol, Proof: proof}, &response); err != nil {
		return err
	}
	if response.Protocol != sourceTaskProtocol {
		return errors.New("sourceauthority: invalid mutation terminal response")
	}
	return nil
}

func validateMutationTerminalProof(proof MutationTerminalProof) error {
	if proof.Authority == "" || proof.AuthorityGeneration == 0 || proof.Operation == (catalog.MutationID{}) ||
		(proof.Outcome != MutationAcknowledged && proof.Outcome != MutationAbandoned) ||
		(proof.Outcome == MutationAcknowledged) != (proof.Digest != (Fingerprint{})) {
		return errors.New("sourceauthority: invalid mutation terminal proof")
	}
	return nil
}

func validateMutationInspectionRequest(request MutationInspectionRequest) error {
	if request.Authority == "" || len(request.Authority) > sourceTaskStringByteLimit ||
		request.AuthorityGeneration == 0 || request.Operation == (catalog.MutationID{}) ||
		request.ExpectationDigest == (Fingerprint{}) {
		return errors.New("sourceauthority: invalid mutation inspection identity")
	}
	return nil
}

func validateMutationInspection(request MutationInspectionRequest, inspection MutationInspection) error {
	if inspection.State == MutationInspectionNotFound {
		copy := inspection
		copy.State = 0
		if copy != (MutationInspection{}) {
			return errors.New("sourceauthority: not-found mutation inspection retains state")
		}
		return nil
	}
	if inspection.ExpectationDigest != request.ExpectationDigest || inspection.Intent == (Fingerprint{}) {
		return errors.New("sourceauthority: mutation inspection identity mismatch")
	}
	if inspection.RequestContent != nil &&
		(inspection.RequestContent.Size < 0 || inspection.RequestContent.Digest == (Fingerprint{})) {
		return errors.New("sourceauthority: invalid inspected mutation content digest")
	}
	switch inspection.State {
	case MutationInspectionActiveUnapplied:
		if inspection.Receipt != nil || inspection.Terminal != nil {
			return errors.New("sourceauthority: active mutation inspection retains terminal state")
		}
	case MutationInspectionApplied:
		if inspection.Receipt == nil || inspection.Terminal != nil ||
			inspection.Receipt.OperationID != request.Operation || inspection.Receipt.Digest == (Fingerprint{}) ||
			len(inspection.Receipt.Effects) == 0 || len(inspection.Receipt.Effects) > sourceTaskMutationActionLimit {
			return errors.New("sourceauthority: invalid applied mutation inspection")
		}
	case MutationInspectionTerminal, MutationInspectionConsumed:
		if inspection.Receipt != nil || inspection.Terminal == nil ||
			validateMutationTerminalProof(*inspection.Terminal) != nil ||
			inspection.Terminal.Authority != request.Authority ||
			inspection.Terminal.AuthorityGeneration != request.AuthorityGeneration ||
			inspection.Terminal.Operation != request.Operation {
			return errors.New("sourceauthority: invalid terminal mutation inspection")
		}
	default:
		return errors.New("sourceauthority: unknown mutation inspection state")
	}
	return nil
}

func encodeMutationRequest(task MutationTask) (sourceTaskMutationRequest, [][]byte, []int64, error) {
	if task.Fence.Authority == "" || task.Fence.AuthorityGeneration == 0 ||
		task.OperationID == (catalog.MutationID{}) || len(task.Program.Actions) == 0 || len(task.Expected) == 0 {
		return sourceTaskMutationRequest{}, nil, nil, errors.New("sourceauthority: incomplete source mutation task")
	}
	wireTask := task
	wireTask.Content = nil
	wireTask.Program.Actions = append([]MutationAction(nil), task.Program.Actions...)
	sizes := make([]int64, len(task.Program.Actions))
	data := make([][]byte, len(task.Program.Actions))
	var total int64
	for index := range wireTask.Program.Actions {
		value := append([]byte(nil), wireTask.Program.Actions[index].Data...)
		wireTask.Program.Actions[index].Data = nil
		sizes[index] = int64(len(value))
		total += sizes[index]
		if total < 0 || total > maxMutationPayload {
			return sourceTaskMutationRequest{}, nil, nil, errors.New("sourceauthority: mutation data exceeds its bounded size")
		}
		data[index] = value
	}
	emit := sourceTaskPageEmitterForMutation(wireTask, sizes)
	manifest, err := planSourceTaskPages(emit)
	if err != nil {
		return sourceTaskMutationRequest{}, nil, nil, err
	}
	if manifest.Roots != uint32(len(task.Roots)) || manifest.Roots == 0 ||
		manifest.Checkpoints != uint32(len(task.Fence.Streams)) ||
		manifest.Actions != uint32(len(task.Program.Actions)) || manifest.Actions == 0 ||
		manifest.ExpectedEffects != uint32(len(task.Expected)) || manifest.ExpectedEffects == 0 ||
		manifest.Tenants != 0 || manifest.Inputs != 0 || manifest.ExpectedEntries != 0 {
		return sourceTaskMutationRequest{}, nil, nil,
			errors.New("sourceauthority: mutation configuration exceeds the protocol limit")
	}
	fence := task.Fence
	fence.Streams = nil
	return sourceTaskMutationRequest{
		Protocol: sourceTaskProtocol, Fence: fence, OperationID: task.OperationID,
		ExpectationDigest: task.ExpectationDigest,
		HasRequestContent: task.Content != nil, Config: manifest,
	}, data, sizes, nil
}

func sendMutationBytes(ctx context.Context, call *wire.ClientCall, kind byte, index uint32, value []byte) error {
	for len(value) != 0 {
		length := min(len(value), sourceTaskChunkSize)
		if err := call.SendChunk(ctx, encodeStreamChunk(kind, index, value[:length])); err != nil {
			return err
		}
		value = value[length:]
	}
	return call.SendChunk(ctx, encodeStreamChunk(sourceTaskChunkMutationEnd, index, []byte{kind}))
}

func sendMutationReader(ctx context.Context, call *wire.ClientCall, kind byte, index uint32, reader io.Reader) error {
	buffer := make([]byte, sourceTaskChunkSize)
	var total int64
	for {
		count, err := reader.Read(buffer)
		if count != 0 {
			total += int64(count)
			if total > maxMutationPayload {
				return errors.New("sourceauthority: mutation request content exceeds its bounded size")
			}
			if sendErr := call.SendChunk(ctx, encodeStreamChunk(kind, index, buffer[:count])); sendErr != nil {
				return sendErr
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}
	}
	return call.SendChunk(ctx, encodeStreamChunk(sourceTaskChunkMutationEnd, index, []byte{kind}))
}

func (c *sourceTaskChild) handleMutation(ctx context.Context, request wire.Request) (any, error) {
	if err := c.claim(request); err != nil {
		return nil, err
	}
	var input sourceTaskMutationRequest
	if err := decodeSourceTaskBounded(request.Payload, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol ||
		input.Fence.Authority == "" || input.Fence.AuthorityGeneration == 0 || len(input.Fence.Streams) != 0 ||
		input.OperationID == (catalog.MutationID{}) || input.ExpectationDigest == (Fingerprint{}) ||
		input.Config.Roots == 0 || input.Config.Actions == 0 || input.Config.ExpectedEffects == 0 ||
		input.Config.Tenants != 0 || input.Config.Inputs != 0 || input.Config.ExpectedEntries != 0 {
		return nil, errors.New("sourceauthority: invalid source mutation request")
	}
	task := MutationTask{
		Fence: input.Fence, OperationID: input.OperationID, ExpectationDigest: input.ExpectationDigest,
		Roots:    make([]RootSpec, 0, input.Config.Roots),
		Program:  MutationProgram{Actions: make([]MutationAction, 0, input.Config.Actions)},
		Expected: make([]ExpectedEffect, 0, input.Config.ExpectedEffects),
	}
	sizes := make([]int64, 0, input.Config.Actions)
	phase := 0
	if err := receiveSourceTaskPages(ctx, request.Chunks, input.Config, func(page sourceTaskConfigPageBody) error {
		switch {
		case len(page.Roots) != 0:
			if phase != 0 {
				return errors.New("sourceauthority: mutation root page is out of order")
			}
			task.Roots = append(task.Roots, page.Roots...)
		case len(page.Checkpoints) != 0:
			if phase > 1 {
				return errors.New("sourceauthority: mutation checkpoint page is out of order")
			}
			phase = 1
			task.Fence.Streams = append(task.Fence.Streams, page.Checkpoints...)
		case len(page.Actions) != 0:
			if phase > 2 {
				return errors.New("sourceauthority: mutation action page is out of order")
			}
			phase = 2
			task.Program.Actions = append(task.Program.Actions, page.Actions...)
			sizes = append(sizes, page.ActionDataSizes...)
		case len(page.ExpectedEffects) != 0:
			phase = 3
			task.Expected = append(task.Expected, page.ExpectedEffects...)
		default:
			return errors.New("sourceauthority: mutation received an invalid configuration page")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := validateChildMutationTask(task, sizes, input.HasRequestContent); err != nil {
		return nil, err
	}
	payloads, err := receiveMutationPayloads(ctx, c.runtimeDir, request.Chunks, task, sizes, input.HasRequestContent)
	if err != nil {
		return nil, err
	}
	defer func() { _ = payloads.Close() }()
	receipt, err := applyMutationTask(ctx, c.journalRoot, task, payloads, nil)
	if err != nil {
		return nil, err
	}
	if c.afterMutation != nil {
		if err := c.afterMutation(ctx, receipt); err != nil {
			return nil, err
		}
	}
	response := sourceTaskMutationResponse{Protocol: sourceTaskProtocol, Receipt: receipt}
	if _, err := encodeSourceTaskRequest(response); err != nil {
		return nil, err
	}
	return response, nil
}

func (c *sourceTaskChild) handleMutationInspect(ctx context.Context, request wire.Request) (any, error) {
	if err := c.claim(request); err != nil {
		return nil, err
	}
	var input sourceTaskMutationInspectionRequest
	if err := decodeSourceTaskBounded(request.Payload, &input, sourceTaskJSONByteLimit); err != nil ||
		input.Protocol != sourceTaskProtocol || validateMutationInspectionRequest(input.Request) != nil {
		return nil, errors.New("sourceauthority: invalid mutation inspection request")
	}
	inspection, err := inspectMutationOperation(ctx, c.journalRoot, input.Request)
	if err != nil {
		return nil, err
	}
	if err := validateMutationInspection(input.Request, inspection); err != nil {
		return nil, err
	}
	return sourceTaskMutationInspectionResponse{Protocol: sourceTaskProtocol, Inspection: inspection}, nil
}

func (c *sourceTaskChild) handleMutationAcknowledge(ctx context.Context, request wire.Request) (any, error) {
	return c.handleMutationTerminal(ctx, request, MutationAcknowledged)
}

func (c *sourceTaskChild) handleMutationAbandon(ctx context.Context, request wire.Request) (any, error) {
	return c.handleMutationTerminal(ctx, request, MutationAbandoned)
}

func (c *sourceTaskChild) handleMutationTerminal(
	ctx context.Context,
	request wire.Request,
	outcome MutationCleanupOutcome,
) (any, error) {
	if err := c.claim(request); err != nil {
		return nil, err
	}
	var input sourceTaskMutationTerminalRequest
	if err := decodeSourceTaskBounded(request.Payload, &input, sourceTaskJSONByteLimit); err != nil || input.Protocol != sourceTaskProtocol ||
		validateMutationTerminalProof(input.Proof) != nil || input.Proof.Outcome != outcome {
		return nil, errors.New("sourceauthority: invalid mutation terminal request")
	}
	if err := settleMutationJournal(ctx, c.journalRoot, input.Proof); err != nil {
		return nil, err
	}
	return sourceTaskMutationTerminalResponse{Protocol: sourceTaskProtocol}, nil
}

func (c *sourceTaskChild) handleMutationProofs(ctx context.Context, request wire.Request) (any, error) {
	if err := c.claim(request); err != nil {
		return nil, err
	}
	var input sourceTaskMutationProofsRequest
	if err := decodeSourceTaskBounded(request.Payload, &input, sourceTaskJSONByteLimit); err != nil ||
		input.Protocol != sourceTaskProtocol || input.Authority == "" ||
		input.Limit == 0 || input.Limit > MutationTerminalProofPageLimit {
		return nil, errors.New("sourceauthority: invalid mutation proof listing request")
	}
	page, err := mutationTerminalProofPage(ctx, c.journalRoot, input.Authority, input.After, int(input.Limit))
	if err != nil {
		return nil, err
	}
	terminal := &sourceTaskMutationProofsResponse{
		Protocol: sourceTaskProtocol, Next: page.Next, More: page.More,
	}
	chunks := make(chan []byte)
	go streamMutationTerminalProofs(ctx, page.Proofs, terminal, chunks)
	return wire.StreamResponse{Chunks: chunks, Value: terminal}, nil
}

func streamMutationTerminalProofs(
	ctx context.Context,
	proofs []MutationTerminalProof,
	terminal *sourceTaskMutationProofsResponse,
	chunks chan<- []byte,
) {
	defer close(chunks)
	var cursor uint32
	var digest Fingerprint
	for start := 0; start < len(proofs); start += sourceTaskProofPageItemLimit {
		page, payload, err := encodeSourceTaskMutationProofPage(
			cursor, digest, proofs[start:min(start+sourceTaskProofPageItemLimit, len(proofs))],
		)
		if err != nil {
			terminal.Error = boundedSourceTaskError(err)
			return
		}
		if !sendSourceTaskChunk(ctx, chunks, payload) {
			return
		}
		cursor++
		digest = page.Digest
	}
	terminal.Count = uint32(len(proofs))
	terminal.Digest = digest
}

func (c *sourceTaskChild) handleMutationForget(ctx context.Context, request wire.Request) (any, error) {
	if err := c.claim(request); err != nil {
		return nil, err
	}
	var input sourceTaskMutationTerminalRequest
	if err := decodeSourceTaskBounded(request.Payload, &input, sourceTaskJSONByteLimit); err != nil ||
		input.Protocol != sourceTaskProtocol || validateMutationTerminalProof(input.Proof) != nil {
		return nil, errors.New("sourceauthority: invalid mutation forget request")
	}
	if err := forgetMutationJournal(ctx, c.journalRoot, input.Proof); err != nil {
		return nil, err
	}
	return sourceTaskMutationTerminalResponse{Protocol: sourceTaskProtocol}, nil
}

func validateChildMutationTask(task MutationTask, actionDataSizes []int64, hasRequestContent bool) error {
	if task.Content != nil || task.OperationID == (catalog.MutationID{}) || task.Fence.Authority == "" ||
		task.Fence.AuthorityGeneration == 0 || task.ExpectationDigest == (Fingerprint{}) ||
		len(task.Roots) == 0 || len(task.Program.Actions) == 0 || len(task.Expected) == 0 ||
		len(actionDataSizes) != len(task.Program.Actions) {
		return errors.New("sourceauthority: incomplete source mutation request")
	}
	if err := validateTaskRootFence(task.Fence, task.Roots); err != nil {
		return err
	}
	roots := make(map[RootID]RootSpec, len(task.Roots))
	for _, root := range task.Roots {
		if root.Authority != task.Fence.Authority {
			return errors.New("sourceauthority: mutation root escaped its authority")
		}
		roots[root.ID] = root
	}
	effects := make(map[PathRef]ExpectedEffect, len(task.Expected))
	for _, effect := range task.Expected {
		root, ok := roots[effect.Path.Root]
		if !ok || validateTaskRelative(root, effect.Path.Relative) != nil {
			return errors.New("sourceauthority: mutation effect escaped its roots")
		}
		if _, duplicate := effects[effect.Path]; duplicate {
			return errors.New("sourceauthority: duplicate mutation effect")
		}
		effects[effect.Path] = effect
	}
	requestActions := 0
	actionPaths := make(map[PathRef]struct{}, len(task.Program.Actions)*2)
	for index, action := range task.Program.Actions {
		root, ok := roots[action.Path.Root]
		if !ok || root.Kind != RootDirectory || action.Path.Relative == "." || validateTaskRelative(root, action.Path.Relative) != nil {
			return errors.New("sourceauthority: mutation action escaped its roots")
		}
		if _, ok := effects[action.Path]; !ok {
			return errors.New("sourceauthority: mutation action has no exact effect")
		}
		if _, duplicate := actionPaths[action.Path]; duplicate {
			return errors.New("sourceauthority: mutation path has more than one writer")
		}
		actionPaths[action.Path] = struct{}{}
		size := actionDataSizes[index]
		if size < 0 || size > maxMutationPayload || len(action.Data) != 0 {
			return errors.New("sourceauthority: invalid mutation action data size")
		}
		switch action.Kind {
		case MutationAtomicWriteFile:
			if action.From != nil || action.Mode == 0 || action.LinkTarget != "" || action.UseRequestContent == (size != 0) {
				return errors.New("sourceauthority: invalid atomic mutation write")
			}
		case MutationCreateDirectory:
			if action.From != nil || action.Mode == 0 || action.LinkTarget != "" || action.UseRequestContent || size != 0 {
				return errors.New("sourceauthority: invalid mutation directory create")
			}
		case MutationCreateSymlink:
			if action.From != nil || action.Mode != 0 || action.LinkTarget == "" || action.UseRequestContent || size != 0 {
				return errors.New("sourceauthority: invalid mutation symlink create")
			}
		case MutationRemove:
			if action.From != nil || action.Mode != 0 || action.LinkTarget != "" || action.UseRequestContent || size != 0 {
				return errors.New("sourceauthority: invalid mutation remove")
			}
		case MutationRename:
			if action.From == nil || action.Mode != 0 || action.LinkTarget != "" || action.UseRequestContent || size != 0 {
				return errors.New("sourceauthority: invalid mutation rename")
			}
			fromRoot, ok := roots[action.From.Root]
			if !ok || fromRoot.Kind != RootDirectory || action.From.Relative == "." || validateTaskRelative(fromRoot, action.From.Relative) != nil {
				return errors.New("sourceauthority: mutation rename source escaped its roots")
			}
			fromEffect, ok := effects[*action.From]
			if !ok || !fromEffect.Before.Exists {
				return errors.New("sourceauthority: mutation rename source has no exact existing effect")
			}
			if _, duplicate := actionPaths[*action.From]; duplicate {
				return errors.New("sourceauthority: mutation path has more than one writer")
			}
			actionPaths[*action.From] = struct{}{}
		default:
			return errors.New("sourceauthority: unknown mutation action")
		}
		if action.UseRequestContent {
			requestActions++
		}
	}
	if requestActions > 1 || (requestActions == 1) != hasRequestContent {
		return errors.New("sourceauthority: mutation request content ownership mismatch")
	}
	return nil
}

func receiveMutationPayloads(
	ctx context.Context,
	runtimeDir string,
	chunks <-chan wire.Chunk,
	task MutationTask,
	actionDataSizes []int64,
	hasRequestContent bool,
) (_ *mutationPayloadSet, resultErr error) {
	set := &mutationPayloadSet{actions: make([]*mutationPayload, len(task.Program.Actions))}
	defer func() {
		if resultErr != nil {
			_ = set.Close()
		}
	}()
	for index, size := range actionDataSizes {
		if size == 0 {
			continue
		}
		payload, err := receiveMutationPayload(ctx, runtimeDir, chunks, sourceTaskChunkAction, uint32(index), size)
		if err != nil {
			return nil, err
		}
		set.actions[index] = payload
	}
	if hasRequestContent {
		payload, err := receiveMutationPayload(ctx, runtimeDir, chunks, sourceTaskChunkRequest, 0, -1)
		if err != nil {
			return nil, err
		}
		set.request = payload
	}
	for chunk := range chunks {
		if !chunk.End {
			return nil, errors.New("sourceauthority: mutation request included trailing content")
		}
	}
	return set, nil
}

func receiveMutationPayload(
	ctx context.Context,
	runtimeDir string,
	chunks <-chan wire.Chunk,
	kind byte,
	index uint32,
	expected int64,
) (*mutationPayload, error) {
	file, err := os.CreateTemp(runtimeDir, "source-mutation-input-")
	if err != nil {
		return nil, err
	}
	path := file.Name()
	if err := os.Chmod(path, 0o600); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := os.Remove(path); err != nil {
		_ = file.Close()
		return nil, err
	}
	hash := sha256.New()
	var total int64
	for {
		select {
		case <-ctx.Done():
			_ = file.Close()
			return nil, ctx.Err()
		case chunk, ok := <-chunks:
			if !ok || chunk.End || len(chunk.Payload) < 5 ||
				len(chunk.Payload) > sourceTaskChunkSize+5 {
				_ = file.Close()
				return nil, errors.New("sourceauthority: mutation payload ended before its boundary")
			}
			chunkKind := chunk.Payload[0]
			chunkIndex := binary.BigEndian.Uint32(chunk.Payload[1:5])
			body := chunk.Payload[5:]
			if chunkKind == sourceTaskChunkMutationEnd {
				if chunkIndex != index || len(body) != 1 || body[0] != kind || (expected >= 0 && total != expected) {
					_ = file.Close()
					return nil, errors.New("sourceauthority: mutation payload boundary mismatch")
				}
				if err := file.Sync(); err != nil {
					_ = file.Close()
					return nil, err
				}
				var digest [32]byte
				copy(digest[:], hash.Sum(nil))
				return &mutationPayload{file: file, size: total, hash: digest}, nil
			}
			if chunkKind != kind || chunkIndex != index || len(body) == 0 {
				_ = file.Close()
				return nil, errors.New("sourceauthority: mutation payload sequence mismatch")
			}
			total += int64(len(body))
			if total < 0 || total > maxMutationPayload || (expected >= 0 && total > expected) {
				_ = file.Close()
				return nil, errors.New("sourceauthority: mutation payload exceeds its bounded size")
			}
			if _, err := file.Write(body); err != nil {
				_ = file.Close()
				return nil, err
			}
			_, _ = hash.Write(body)
		}
	}
}

func (s *mutationPayloadSet) Close() error {
	var result error
	for _, value := range s.actions {
		if value != nil {
			result = errors.Join(result, value.file.Close())
		}
	}
	if s.request != nil {
		result = errors.Join(result, s.request.file.Close())
	}
	return result
}
