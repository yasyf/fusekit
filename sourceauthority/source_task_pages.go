package sourceauthority

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/tenant"
)

const (
	sourceTaskPageItemLimit       = 128
	sourceTaskProofPageItemLimit  = MutationTerminalProofPageLimit
	sourceTaskPageByteLimit       = 128 << 10
	sourceTaskConfigByteLimit     = 64 << 20
	sourceTaskStringByteLimit     = 4 << 10
	sourceTaskErrorByteLimit      = 4 << 10
	sourceTaskJSONByteLimit       = 256 << 10
	sourceTaskRootLimit           = 10_000
	sourceTaskTenantLimit         = 10_000
	sourceTaskInputLimit          = 10_000
	sourceTaskMutationActionLimit = 16
	sourceTaskPageLimit           = 2*sourceTaskRootLimit + sourceTaskTenantLimit +
		sourceTaskInputLimit + 2*sourceTaskMutationActionLimit

	sourceTaskChunkConfig byte = 7
)

type sourceTaskConfigManifest struct {
	Pages           uint32      `json:"pages"`
	EncodedBytes    uint64      `json:"encoded_bytes"`
	Digest          Fingerprint `json:"digest"`
	Roots           uint32      `json:"roots"`
	Checkpoints     uint32      `json:"checkpoints"`
	Tenants         uint32      `json:"tenants"`
	Inputs          uint32      `json:"inputs"`
	ExpectedEntries uint32      `json:"expected_entries"`
	Actions         uint32      `json:"actions"`
	ExpectedEffects uint32      `json:"expected_effects"`
}

type sourceTaskConfigPageBody struct {
	Roots           []RootSpec          `json:"roots,omitempty"`
	Checkpoints     []StreamCheckpoint  `json:"checkpoints,omitempty"`
	Tenants         []tenant.TenantSpec `json:"tenants,omitempty"`
	Inputs          []PathRef           `json:"inputs,omitempty"`
	ExpectedEntries []PhysicalEntry     `json:"expected_entries,omitempty"`
	Actions         []MutationAction    `json:"actions,omitempty"`
	ActionDataSizes []int64             `json:"action_data_sizes,omitempty"`
	ExpectedEffects []ExpectedEffect    `json:"expected_effects,omitempty"`
}

type sourceTaskConfigPage struct {
	Protocol uint16                   `json:"protocol"`
	Cursor   uint32                   `json:"cursor"`
	Previous Fingerprint              `json:"previous"`
	Digest   Fingerprint              `json:"digest"`
	Body     sourceTaskConfigPageBody `json:"body"`
}

type sourceTaskMutationProofPage struct {
	Protocol uint16                  `json:"protocol"`
	Cursor   uint32                  `json:"cursor"`
	Previous Fingerprint             `json:"previous"`
	Digest   Fingerprint             `json:"digest"`
	Proofs   []MutationTerminalProof `json:"proofs"`
}

type sourceTaskPageEmitter func(func(sourceTaskConfigPageBody) error) error

func sourceTaskPageEmitterForScan(roots []RootSpec) sourceTaskPageEmitter {
	return func(yield func(sourceTaskConfigPageBody) error) error {
		return emitSourceTaskPages(len(roots), func(start, end int) sourceTaskConfigPageBody {
			return sourceTaskConfigPageBody{Roots: roots[start:end]}
		}, yield)
	}
}

func sourceTaskPageEmitterForMaterialization(task MaterializationTask) sourceTaskPageEmitter {
	return func(yield func(sourceTaskConfigPageBody) error) error {
		if err := emitSourceTaskPages(len(task.Roots), func(start, end int) sourceTaskConfigPageBody {
			return sourceTaskConfigPageBody{Roots: task.Roots[start:end]}
		}, yield); err != nil {
			return err
		}
		if err := emitSourceTaskPages(len(task.Fence.Streams), func(start, end int) sourceTaskConfigPageBody {
			return sourceTaskConfigPageBody{Checkpoints: task.Fence.Streams[start:end]}
		}, yield); err != nil {
			return err
		}
		if err := emitSourceTaskPages(len(task.Tenants), func(start, end int) sourceTaskConfigPageBody {
			return sourceTaskConfigPageBody{Tenants: task.Tenants[start:end]}
		}, yield); err != nil {
			return err
		}
		return emitSourceTaskPages(len(task.Request.Inputs), func(start, end int) sourceTaskConfigPageBody {
			return sourceTaskConfigPageBody{
				Inputs: task.Request.Inputs[start:end], ExpectedEntries: task.Expected[start:end],
			}
		}, yield)
	}
}

func sourceTaskPageEmitterForMutation(task MutationTask, sizes []int64) sourceTaskPageEmitter {
	return func(yield func(sourceTaskConfigPageBody) error) error {
		if err := emitSourceTaskPages(len(task.Roots), func(start, end int) sourceTaskConfigPageBody {
			return sourceTaskConfigPageBody{Roots: task.Roots[start:end]}
		}, yield); err != nil {
			return err
		}
		if err := emitSourceTaskPages(len(task.Fence.Streams), func(start, end int) sourceTaskConfigPageBody {
			return sourceTaskConfigPageBody{Checkpoints: task.Fence.Streams[start:end]}
		}, yield); err != nil {
			return err
		}
		if err := emitSourceTaskPages(len(task.Program.Actions), func(start, end int) sourceTaskConfigPageBody {
			actions := append([]MutationAction(nil), task.Program.Actions[start:end]...)
			for index := range actions {
				actions[index].Data = nil
			}
			return sourceTaskConfigPageBody{
				Actions: actions, ActionDataSizes: sizes[start:end],
			}
		}, yield); err != nil {
			return err
		}
		return emitSourceTaskPages(len(task.Expected), func(start, end int) sourceTaskConfigPageBody {
			return sourceTaskConfigPageBody{ExpectedEffects: task.Expected[start:end]}
		}, yield)
	}
}

func emitSourceTaskPages(
	count int,
	page func(int, int) sourceTaskConfigPageBody,
	yield func(sourceTaskConfigPageBody) error,
) error {
	for start := 0; start < count; {
		maximum, end := min(start+sourceTaskPageItemLimit, count), 0
		if fits, err := sourceTaskConfigPageBodyFits(page(start, maximum)); err == nil && fits {
			end = maximum
		} else {
			low, high := start+1, maximum-1
			for low <= high {
				middle := low + (high-low)/2
				fits, err := sourceTaskConfigPageBodyFits(page(start, middle))
				if err != nil {
					high = middle - 1
					continue
				}
				if fits {
					end = middle
					low = middle + 1
				} else {
					high = middle - 1
				}
			}
		}
		if end == 0 {
			body := page(start, start+1)
			if err := validateSourceTaskPageBody(body); err != nil {
				return err
			}
			return errors.New("sourceauthority: source task configuration item exceeds its page byte limit")
		}
		if err := yield(page(start, end)); err != nil {
			return err
		}
		start = end
	}
	return nil
}

func sourceTaskConfigPageBodyFits(body sourceTaskConfigPageBody) (bool, error) {
	if err := validateSourceTaskPageBody(body); err != nil {
		return false, err
	}
	var largest Fingerprint
	for index := range largest {
		largest[index] = 0xff
	}
	payload, err := json.Marshal(sourceTaskConfigPage{
		Protocol: ^uint16(0), Cursor: ^uint32(0), Previous: largest, Digest: largest, Body: body,
	})
	if err != nil {
		return false, err
	}
	return len(payload) <= sourceTaskPageByteLimit, nil
}

func planSourceTaskPages(emit sourceTaskPageEmitter) (sourceTaskConfigManifest, error) {
	var manifest sourceTaskConfigManifest
	var previous Fingerprint
	err := emit(func(body sourceTaskConfigPageBody) error {
		page, payload, err := encodeSourceTaskConfigPage(manifest.Pages, previous, body)
		if err != nil {
			return err
		}
		manifest.Pages++
		manifest.EncodedBytes += uint64(len(payload))
		if manifest.EncodedBytes > sourceTaskConfigByteLimit {
			return errors.New("sourceauthority: source task configuration exceeds its byte limit")
		}
		addSourceTaskPageCounts(&manifest, body)
		previous = page.Digest
		return nil
	})
	manifest.Digest = previous
	if err != nil {
		return sourceTaskConfigManifest{}, err
	}
	if err := validateSourceTaskManifest(manifest); err != nil {
		return sourceTaskConfigManifest{}, err
	}
	return manifest, nil
}

func sendSourceTaskPages(
	ctx context.Context,
	call *wire.ClientCall,
	manifest sourceTaskConfigManifest,
	emit sourceTaskPageEmitter,
) error {
	var cursor uint32
	var encoded uint64
	var previous Fingerprint
	var counts sourceTaskConfigManifest
	err := emit(func(body sourceTaskConfigPageBody) error {
		page, payload, err := encodeSourceTaskConfigPage(cursor, previous, body)
		if err != nil {
			return err
		}
		if err := call.SendChunk(ctx, encodeStreamChunk(sourceTaskChunkConfig, cursor, payload)); err != nil {
			return err
		}
		cursor++
		encoded += uint64(len(payload))
		addSourceTaskPageCounts(&counts, body)
		previous = page.Digest
		return nil
	})
	if err != nil {
		return err
	}
	counts.Pages, counts.EncodedBytes, counts.Digest = cursor, encoded, previous
	if counts != manifest {
		return errors.New("sourceauthority: source task configuration changed while streaming")
	}
	return nil
}

func receiveSourceTaskPages(
	ctx context.Context,
	chunks <-chan wire.Chunk,
	manifest sourceTaskConfigManifest,
	consume func(sourceTaskConfigPageBody) error,
) error {
	if err := validateSourceTaskManifest(manifest); err != nil {
		return err
	}
	var cursor uint32
	var encoded uint64
	var previous Fingerprint
	var counts sourceTaskConfigManifest
	for cursor < manifest.Pages {
		var chunk wire.Chunk
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case chunk, ok = <-chunks:
		}
		if !ok || chunk.End || len(chunk.Payload) < 5 ||
			chunk.Payload[0] != sourceTaskChunkConfig ||
			binary.BigEndian.Uint32(chunk.Payload[1:5]) != cursor {
			return errors.New("sourceauthority: source task configuration page sequence is invalid")
		}
		payload := chunk.Payload[5:]
		if len(payload) == 0 || len(payload) > sourceTaskPageByteLimit {
			return errors.New("sourceauthority: source task configuration page exceeds its byte limit")
		}
		var page sourceTaskConfigPage
		if err := decodeSourceTaskBounded(payload, &page, sourceTaskPageByteLimit); err != nil {
			return err
		}
		expected, _, err := encodeSourceTaskConfigPage(cursor, previous, page.Body)
		if err != nil || page.Protocol != sourceTaskProtocol || page.Cursor != cursor ||
			page.Previous != previous || page.Digest != expected.Digest {
			return errors.New("sourceauthority: source task configuration page proof is invalid")
		}
		if err := consume(page.Body); err != nil {
			return err
		}
		cursor++
		encoded += uint64(len(payload))
		if encoded > sourceTaskConfigByteLimit {
			return errors.New("sourceauthority: source task configuration exceeds its byte limit")
		}
		addSourceTaskPageCounts(&counts, page.Body)
		previous = page.Digest
	}
	counts.Pages, counts.EncodedBytes, counts.Digest = cursor, encoded, previous
	if counts != manifest {
		return errors.New("sourceauthority: source task configuration terminal proof is invalid")
	}
	return nil
}

func finishSourceTaskInput(chunks <-chan wire.Chunk) error {
	for chunk := range chunks {
		if !chunk.End {
			return errors.New("sourceauthority: source task request included trailing input")
		}
	}
	return nil
}

func encodeSourceTaskConfigPage(
	cursor uint32,
	previous Fingerprint,
	body sourceTaskConfigPageBody,
) (sourceTaskConfigPage, []byte, error) {
	if err := validateSourceTaskPageBody(body); err != nil {
		return sourceTaskConfigPage{}, nil, err
	}
	bodyPayload, err := json.Marshal(body)
	if err != nil {
		return sourceTaskConfigPage{}, nil, err
	}
	hash := sha256.New()
	_, _ = hash.Write(previous[:])
	var encodedCursor [4]byte
	binary.BigEndian.PutUint32(encodedCursor[:], cursor)
	_, _ = hash.Write(encodedCursor[:])
	_, _ = hash.Write(bodyPayload)
	var digest Fingerprint
	copy(digest[:], hash.Sum(nil))
	page := sourceTaskConfigPage{
		Protocol: sourceTaskProtocol, Cursor: cursor, Previous: previous, Digest: digest, Body: body,
	}
	payload, err := json.Marshal(page)
	if err != nil {
		return sourceTaskConfigPage{}, nil, err
	}
	if len(payload) == 0 || len(payload) > sourceTaskPageByteLimit {
		return sourceTaskConfigPage{}, nil, errors.New("sourceauthority: source task configuration page exceeds its byte limit")
	}
	return page, payload, nil
}

func validateSourceTaskPageBody(body sourceTaskConfigPageBody) error {
	kinds := 0
	for _, count := range []int{
		len(body.Roots), len(body.Checkpoints), len(body.Tenants), len(body.Inputs), len(body.Actions),
		len(body.ExpectedEffects),
	} {
		if count != 0 {
			kinds++
		}
		if count > sourceTaskPageItemLimit {
			return errors.New("sourceauthority: source task configuration page exceeds its item limit")
		}
	}
	if kinds != 1 || (len(body.ExpectedEntries) != 0 && len(body.ExpectedEntries) != len(body.Inputs)) ||
		(len(body.ActionDataSizes) != 0 && len(body.ActionDataSizes) != len(body.Actions)) {
		return errors.New("sourceauthority: source task configuration page shape is invalid")
	}
	return validateSourceTaskStrings(reflect.ValueOf(body))
}

func validateSourceTaskStrings(value reflect.Value) error {
	if !value.IsValid() {
		return nil
	}
	switch value.Kind() {
	case reflect.Interface, reflect.Pointer:
		if !value.IsNil() {
			return validateSourceTaskStrings(value.Elem())
		}
	case reflect.String:
		text := value.String()
		if len(text) > sourceTaskStringByteLimit || !utf8.ValidString(text) || strings.IndexByte(text, 0) >= 0 {
			return errors.New("sourceauthority: source task string exceeds its protocol limit")
		}
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if err := validateSourceTaskStrings(value.Field(index)); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		for index := 0; index < value.Len(); index++ {
			if err := validateSourceTaskStrings(value.Index(index)); err != nil {
				return err
			}
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			if err := validateSourceTaskStrings(iterator.Key()); err != nil {
				return err
			}
			if err := validateSourceTaskStrings(iterator.Value()); err != nil {
				return err
			}
		}
	}
	return nil
}

func addSourceTaskPageCounts(manifest *sourceTaskConfigManifest, body sourceTaskConfigPageBody) {
	manifest.Roots += uint32(len(body.Roots))
	manifest.Checkpoints += uint32(len(body.Checkpoints))
	manifest.Tenants += uint32(len(body.Tenants))
	manifest.Inputs += uint32(len(body.Inputs))
	manifest.ExpectedEntries += uint32(len(body.ExpectedEntries))
	manifest.Actions += uint32(len(body.Actions))
	manifest.ExpectedEffects += uint32(len(body.ExpectedEffects))
}

func validateSourceTaskManifest(manifest sourceTaskConfigManifest) error {
	minimumPages := sourceTaskPageCount(manifest.Roots) +
		sourceTaskPageCount(manifest.Checkpoints) +
		sourceTaskPageCount(manifest.Tenants) +
		sourceTaskPageCount(manifest.Inputs) +
		sourceTaskPageCount(manifest.Actions) +
		sourceTaskPageCount(manifest.ExpectedEffects)
	maximumPages := manifest.Roots + manifest.Checkpoints + manifest.Tenants +
		manifest.Inputs + manifest.Actions + manifest.ExpectedEffects
	if manifest.EncodedBytes > sourceTaskConfigByteLimit ||
		manifest.Pages > sourceTaskPageLimit ||
		manifest.Pages < minimumPages || manifest.Pages > maximumPages ||
		manifest.Roots > sourceTaskRootLimit ||
		manifest.Checkpoints > sourceTaskRootLimit ||
		manifest.Tenants > sourceTaskTenantLimit ||
		manifest.Inputs > sourceTaskInputLimit ||
		manifest.ExpectedEntries > sourceTaskInputLimit ||
		(manifest.ExpectedEntries != 0 && manifest.ExpectedEntries != manifest.Inputs) ||
		manifest.Actions > sourceTaskMutationActionLimit ||
		manifest.ExpectedEffects > sourceTaskMutationActionLimit ||
		(manifest.Pages == 0) != (manifest.EncodedBytes == 0) ||
		(manifest.Pages == 0) != (manifest.Digest == (Fingerprint{})) {
		return errors.New("sourceauthority: source task configuration manifest is invalid")
	}
	return nil
}

func sourceTaskPageCount(count uint32) uint32 {
	return (count + sourceTaskPageItemLimit - 1) / sourceTaskPageItemLimit
}

func decodeSourceTaskBounded(payload []byte, target any, limit int) error {
	if len(payload) == 0 || len(payload) > limit {
		return fmt.Errorf("sourceauthority: source task JSON exceeds its %d-byte limit", limit)
	}
	if err := decodeSourceTask(payload, target); err != nil {
		return err
	}
	return validateSourceTaskStrings(reflect.ValueOf(target))
}

func encodeSourceTaskRequest(value any) ([]byte, error) {
	if err := validateSourceTaskStrings(reflect.ValueOf(value)); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(payload) == 0 || len(payload) > sourceTaskJSONByteLimit {
		return nil, errors.New("sourceauthority: source task request header exceeds its byte limit")
	}
	return payload, nil
}

func boundedSourceTaskError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if !utf8.ValidString(value) {
		value = strings.ToValidUTF8(value, "?")
	}
	value = strings.ReplaceAll(value, "\x00", "?")
	if len(value) <= sourceTaskErrorByteLimit {
		return value
	}
	value = value[:sourceTaskErrorByteLimit]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func encodeSourceTaskMutationProofPage(
	cursor uint32,
	previous Fingerprint,
	proofs []MutationTerminalProof,
) (sourceTaskMutationProofPage, []byte, error) {
	if len(proofs) == 0 || len(proofs) > sourceTaskProofPageItemLimit {
		return sourceTaskMutationProofPage{}, nil,
			errors.New("sourceauthority: mutation proof page exceeds its item limit")
	}
	for _, proof := range proofs {
		if err := validateMutationTerminalProof(proof); err != nil {
			return sourceTaskMutationProofPage{}, nil, err
		}
	}
	if err := validateSourceTaskStrings(reflect.ValueOf(proofs)); err != nil {
		return sourceTaskMutationProofPage{}, nil, err
	}
	proofPayload, err := json.Marshal(proofs)
	if err != nil {
		return sourceTaskMutationProofPage{}, nil, err
	}
	hash := sha256.New()
	_, _ = hash.Write(previous[:])
	var encodedCursor [4]byte
	binary.BigEndian.PutUint32(encodedCursor[:], cursor)
	_, _ = hash.Write(encodedCursor[:])
	_, _ = hash.Write(proofPayload)
	var digest Fingerprint
	copy(digest[:], hash.Sum(nil))
	page := sourceTaskMutationProofPage{
		Protocol: sourceTaskProtocol, Cursor: cursor, Previous: previous, Digest: digest, Proofs: proofs,
	}
	payload, err := json.Marshal(page)
	if err != nil {
		return sourceTaskMutationProofPage{}, nil, err
	}
	if len(payload) == 0 || len(payload) > sourceTaskPageByteLimit {
		return sourceTaskMutationProofPage{}, nil,
			errors.New("sourceauthority: mutation proof page exceeds its byte limit")
	}
	return page, payload, nil
}
