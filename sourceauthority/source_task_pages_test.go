package sourceauthority

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/yasyf/daemonkit/wire"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
)

func TestSourceTaskConfigPagesAreBoundedDeterministicAndReplayIdentical(t *testing.T) {
	t.Parallel()
	roots := sourceTaskScaleRoots(sourceTaskRootLimit)
	emit := sourceTaskPageEmitterForScan(roots)
	first, err := planSourceTaskPages(emit)
	if err != nil {
		t.Fatal(err)
	}
	second, err := planSourceTaskPages(emit)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.Roots != sourceTaskRootLimit ||
		first.Pages != uint32((sourceTaskRootLimit+sourceTaskPageItemLimit-1)/sourceTaskPageItemLimit) ||
		first.EncodedBytes == 0 || first.EncodedBytes > sourceTaskConfigByteLimit ||
		first.Digest == (Fingerprint{}) {
		t.Fatalf("10k source-task plan is not exact: first=%+v second=%+v", first, second)
	}
	var cursor uint32
	var previous Fingerprint
	if err := emit(func(body sourceTaskConfigPageBody) error {
		page, encoded, err := encodeSourceTaskConfigPage(cursor, previous, body)
		if err != nil {
			return err
		}
		replay, replayEncoded, err := encodeSourceTaskConfigPage(cursor, previous, body)
		if err != nil {
			return err
		}
		if page.Digest != replay.Digest || page.Cursor != replay.Cursor ||
			string(encoded) != string(replayEncoded) ||
			len(encoded) > sourceTaskPageByteLimit || len(body.Roots) > sourceTaskPageItemLimit {
			t.Fatalf("page %d did not replay exactly", cursor)
		}
		cursor++
		previous = page.Digest
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if cursor != first.Pages || previous != first.Digest {
		t.Fatalf("stream proof = (%d,%x), want (%d,%x)", cursor, previous, first.Pages, first.Digest)
	}
}

func TestSourceTaskConfigPagesRejectEveryMaxPlusOneBoundary(t *testing.T) {
	t.Parallel()
	if _, err := planSourceTaskPages(sourceTaskPageEmitterForScan(
		sourceTaskScaleRoots(sourceTaskRootLimit + 1),
	)); err == nil {
		t.Fatal("root max+1 was accepted")
	}
	oversized := sourceTaskScaleRoots(1)
	oversized[0].Path = "/" + string(make([]byte, sourceTaskStringByteLimit+1))
	if _, err := planSourceTaskPages(sourceTaskPageEmitterForScan(oversized)); err == nil {
		t.Fatal("string max+1 was accepted")
	}
	oversized[0].Path = "/root\x00suffix"
	if _, err := planSourceTaskPages(sourceTaskPageEmitterForScan(oversized)); err == nil {
		t.Fatal("NUL-bearing string was accepted")
	}
	body := sourceTaskConfigPageBody{Roots: sourceTaskScaleRoots(sourceTaskPageItemLimit + 1)}
	if _, _, err := encodeSourceTaskConfigPage(0, Fingerprint{}, body); err == nil {
		t.Fatal("page item max+1 was accepted")
	}
	byteSplitRoots := sourceTaskScaleRoots(sourceTaskPageItemLimit)
	for index := range byteSplitRoots {
		byteSplitRoots[index].Path = "/" + strings.Repeat("x", sourceTaskStringByteLimit-1)
	}
	byteSplit, err := planSourceTaskPages(sourceTaskPageEmitterForScan(byteSplitRoots))
	if err != nil {
		t.Fatal(err)
	}
	if byteSplit.Pages <= 1 {
		t.Fatal("encoded-byte bound did not split a 128-item source-task page")
	}
	pageForPathLength := func(length int) sourceTaskConfigPageBody {
		roots := sourceTaskScaleRoots(sourceTaskPageItemLimit)
		for index := range roots {
			roots[index].Path = "/" + strings.Repeat("x", length-1)
		}
		return sourceTaskConfigPageBody{Roots: roots}
	}
	low, high := 1, sourceTaskStringByteLimit
	for low < high {
		middle := low + (high-low+1)/2
		if _, _, err := encodeSourceTaskConfigPage(0, Fingerprint{}, pageForPathLength(middle)); err == nil {
			low = middle
		} else {
			high = middle - 1
		}
	}
	if _, _, err := encodeSourceTaskConfigPage(0, Fingerprint{}, pageForPathLength(low)); err != nil {
		t.Fatalf("largest encoded source-task page rejected: %v", err)
	}
	if low == sourceTaskStringByteLimit {
		t.Fatal("source-task page did not reach its encoded byte limit before its string limit")
	}
	if _, _, err := encodeSourceTaskConfigPage(0, Fingerprint{}, pageForPathLength(low+1)); err == nil {
		t.Fatal("encoded source-task page max+1 was accepted")
	}
	manifest := sourceTaskConfigManifest{
		Pages: 2, EncodedBytes: 1, Digest: Fingerprint{1}, Roots: 1,
	}
	if err := validateSourceTaskManifest(manifest); err == nil {
		t.Fatal("non-canonical page partition was accepted")
	}
	manifest.Pages, manifest.Inputs, manifest.ExpectedEntries = 1, 1, 2
	manifest.Roots = 0
	if err := validateSourceTaskManifest(manifest); err == nil {
		t.Fatal("mismatched input/expected counts were accepted")
	}
	manifest = sourceTaskConfigManifest{
		Pages: sourceTaskPageLimit, EncodedBytes: 1, Digest: Fingerprint{1},
		Roots: sourceTaskRootLimit, Checkpoints: sourceTaskRootLimit,
		Tenants: sourceTaskTenantLimit, Inputs: sourceTaskInputLimit,
		ExpectedEntries: sourceTaskInputLimit, Actions: sourceTaskMutationActionLimit,
		ExpectedEffects: sourceTaskMutationActionLimit,
	}
	if err := validateSourceTaskManifest(manifest); err != nil {
		t.Fatalf("maximum canonical page manifest rejected: %v", err)
	}
	manifest.Pages++
	if err := validateSourceTaskManifest(manifest); err == nil {
		t.Fatal("page count max+1 was accepted")
	}
}

func TestSourceTaskErrorsAreUTF8SafeAndExactlyBounded(t *testing.T) {
	t.Parallel()
	exact := strings.Repeat("x", sourceTaskErrorByteLimit)
	if got := boundedSourceTaskError(errors.New(exact)); got != exact {
		t.Fatalf("exact error changed: %d bytes", len(got))
	}
	invalid := strings.Repeat("x", sourceTaskErrorByteLimit-1) + string([]byte{0xff, 0xff})
	got := boundedSourceTaskError(errors.New(invalid))
	if len(got) > sourceTaskErrorByteLimit || !utf8.ValidString(got) {
		t.Fatalf("bounded error bytes=%d valid=%v", len(got), utf8.ValidString(got))
	}
	if got := boundedSourceTaskError(errors.New("before\x00after")); strings.ContainsRune(got, 0) {
		t.Fatal("bounded error retained NUL")
	}
}

func TestSourceTaskConfigPageCursorDigestRejectsTamperAndReplaysWholeInput(t *testing.T) {
	t.Parallel()
	roots := sourceTaskScaleRoots(3)
	emit := sourceTaskPageEmitterForScan(roots)
	manifest, err := planSourceTaskPages(emit)
	if err != nil {
		t.Fatal(err)
	}
	var payload []byte
	if err := emit(func(body sourceTaskConfigPageBody) error {
		_, encoded, err := encodeSourceTaskConfigPage(0, Fingerprint{}, body)
		payload = encodeStreamChunk(sourceTaskChunkConfig, 0, encoded)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	replay := func(value []byte) error {
		chunks := make(chan wire.Chunk, 2)
		chunks <- wire.Chunk{Sequence: 1, Payload: value}
		chunks <- wire.Chunk{Sequence: 2, End: true}
		close(chunks)
		var received int
		err := receiveSourceTaskPages(context.Background(), chunks, manifest, func(body sourceTaskConfigPageBody) error {
			received += len(body.Roots)
			return nil
		})
		if err == nil {
			err = finishSourceTaskInput(chunks)
		}
		if err == nil && received != len(roots) {
			t.Fatalf("received %d roots, want %d", received, len(roots))
		}
		return err
	}
	if err := replay(payload); err != nil {
		t.Fatal(err)
	}
	if err := replay(payload); err != nil {
		t.Fatalf("whole-input replay changed result: %v", err)
	}
	tampered := append([]byte(nil), payload...)
	tampered[len(tampered)-2] ^= 1
	if err := replay(tampered); err == nil {
		t.Fatal("tampered configuration page was accepted")
	}
}

func TestSourceTaskConfigPagesRejectDuplicateSkipReorderTerminalMismatchCancelAndChildDeath(t *testing.T) {
	t.Parallel()
	roots := sourceTaskScaleRoots(sourceTaskPageItemLimit*2 + 1)
	emit := sourceTaskPageEmitterForScan(roots)
	manifest, err := planSourceTaskPages(emit)
	if err != nil {
		t.Fatal(err)
	}
	var payloads [][]byte
	var cursor uint32
	var previous Fingerprint
	if err := emit(func(body sourceTaskConfigPageBody) error {
		page, encoded, err := encodeSourceTaskConfigPage(cursor, previous, body)
		if err != nil {
			return err
		}
		payloads = append(payloads, encodeStreamChunk(sourceTaskChunkConfig, cursor, encoded))
		cursor++
		previous = page.Digest
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(payloads) != 3 {
		t.Fatalf("encoded %d pages, want 3", len(payloads))
	}
	replay := func(ctx context.Context, selected [][]byte, proof sourceTaskConfigManifest) error {
		chunks := make(chan wire.Chunk, len(selected)+1)
		for index, payload := range selected {
			chunks <- wire.Chunk{Sequence: uint32(index + 1), Payload: payload}
		}
		chunks <- wire.Chunk{Sequence: uint32(len(selected) + 1), End: true}
		close(chunks)
		if err := receiveSourceTaskPages(ctx, chunks, proof, func(sourceTaskConfigPageBody) error {
			return nil
		}); err != nil {
			return err
		}
		return finishSourceTaskInput(chunks)
	}
	if err := replay(context.Background(), payloads, manifest); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if err := replay(context.Background(), payloads, manifest); err != nil {
		t.Fatalf("second exact replay: %v", err)
	}
	for name, selected := range map[string][][]byte{
		"duplicate": {payloads[0], payloads[0], payloads[1], payloads[2]},
		"skip":      {payloads[0], payloads[2]},
		"reorder":   {payloads[1], payloads[0], payloads[2]},
	} {
		if err := replay(context.Background(), selected, manifest); err == nil {
			t.Fatalf("%s page sequence was accepted", name)
		}
	}
	countMismatch := manifest
	countMismatch.Roots++
	if err := replay(context.Background(), payloads, countMismatch); err == nil {
		t.Fatal("terminal count mismatch was accepted")
	}
	digestMismatch := manifest
	digestMismatch.Digest[0] ^= 1
	if err := replay(context.Background(), payloads, digestMismatch); err == nil {
		t.Fatal("terminal digest mismatch was accepted")
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	cancelChunks := make(chan wire.Chunk, 1)
	cancelChunks <- wire.Chunk{Sequence: 1, Payload: payloads[0]}
	err = receiveSourceTaskPages(cancelCtx, cancelChunks, manifest, func(sourceTaskConfigPageBody) error {
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("between-page cancellation = %v, want context.Canceled", err)
	}

	deathChunks := make(chan wire.Chunk, 1)
	deathChunks <- wire.Chunk{Sequence: 1, Payload: payloads[0]}
	close(deathChunks)
	if err := receiveSourceTaskPages(context.Background(), deathChunks, manifest, func(sourceTaskConfigPageBody) error {
		return nil
	}); err == nil {
		t.Fatal("child death between pages was accepted")
	}
}

func TestValidateRootsScalesBySortingAndDetectsAdjacentPrefix(t *testing.T) {
	roots := sourceTaskScaleRoots(sourceTaskRootLimit)
	if err := validateRoots("authority", roots); err != nil {
		t.Fatal(err)
	}
	allocations := testing.AllocsPerRun(5, func() {
		if err := validateRoots("authority", roots); err != nil {
			panic(err)
		}
	})
	if allocations > float64(sourceTaskRootLimit)+64 {
		t.Fatalf("validateRoots allocations = %.0f, want O(n) with a small constant", allocations)
	}
	overlap := append([]RootSpec(nil), roots...)
	overlap[len(overlap)-1].Path = roots[0].Path + "/child"
	if err := validateRoots("authority", overlap); err == nil {
		t.Fatal("sorted adjacent root prefix overlap was accepted")
	}
	rootOverlap := sourceTaskScaleRoots(2)
	rootOverlap[0].Path = "/"
	rootOverlap[1].Path = "/child"
	if err := validateRoots("authority", rootOverlap); err == nil {
		t.Fatal("filesystem root overlap was accepted")
	}
}

func TestMutationProofPagesBoundTenThousandProofsAndReplayExactly(t *testing.T) {
	t.Parallel()
	proofs := make([]MutationTerminalProof, maxMutationJournals)
	for index := range proofs {
		proofs[index] = MutationTerminalProof{
			Authority: "authority", AuthorityGeneration: 1,
			Operation: mutationIDForScale(index + 1), Outcome: MutationAbandoned,
		}
	}
	var cursor uint32
	var previous Fingerprint
	var encodedBytes uint64
	for start := 0; start < len(proofs); start += sourceTaskProofPageItemLimit {
		pageProofs := proofs[start:min(start+sourceTaskProofPageItemLimit, len(proofs))]
		page, encoded, err := encodeSourceTaskMutationProofPage(cursor, previous, pageProofs)
		if err != nil {
			t.Fatal(err)
		}
		replay, replayEncoded, err := encodeSourceTaskMutationProofPage(cursor, previous, pageProofs)
		if err != nil {
			t.Fatal(err)
		}
		if page.Digest != replay.Digest || page.Cursor != replay.Cursor ||
			string(encoded) != string(replayEncoded) || len(encoded) > sourceTaskPageByteLimit {
			t.Fatalf("proof page %d did not replay exactly", cursor)
		}
		cursor++
		previous = page.Digest
		encodedBytes += uint64(len(encoded))
	}
	if cursor != uint32((maxMutationJournals+sourceTaskProofPageItemLimit-1)/sourceTaskProofPageItemLimit) ||
		cursor != sourceTaskProofPageCount(uint32(len(proofs))) ||
		previous == (Fingerprint{}) || encodedBytes > sourceTaskConfigByteLimit {
		t.Fatalf("proof terminal = pages %d digest %x bytes %d", cursor, previous, encodedBytes)
	}
}

func TestMutationProofPagesRejectDuplicateSkipReorderAndTerminalMismatch(t *testing.T) {
	t.Parallel()
	proofs := make([]MutationTerminalProof, sourceTaskProofPageItemLimit*2+1)
	for index := range proofs {
		proofs[index] = MutationTerminalProof{
			Authority: "authority", AuthorityGeneration: 1,
			Operation: mutationIDForScale(index + 1), Outcome: MutationAbandoned,
		}
	}
	var pages [][]byte
	var cursor uint32
	var digest Fingerprint
	for start := 0; start < len(proofs); start += sourceTaskProofPageItemLimit {
		page, encoded, err := encodeSourceTaskMutationProofPage(
			cursor, digest, proofs[start:min(start+sourceTaskProofPageItemLimit, len(proofs))],
		)
		if err != nil {
			t.Fatal(err)
		}
		pages = append(pages, encoded)
		cursor++
		digest = page.Digest
	}
	replay := func(selected [][]byte) (uint32, Fingerprint, error) {
		var replayCursor uint32
		var replayDigest Fingerprint
		for _, encoded := range selected {
			page, err := decodeSourceTaskMutationProofPage(encoded, replayCursor, replayDigest)
			if err != nil {
				return 0, Fingerprint{}, err
			}
			replayCursor++
			replayDigest = page.Digest
		}
		return replayCursor, replayDigest, nil
	}
	for iteration := 0; iteration < 2; iteration++ {
		gotPages, gotDigest, err := replay(pages)
		if err != nil || gotPages != cursor || gotDigest != digest {
			t.Fatalf("exact proof replay %d = pages %d digest %x err %v", iteration, gotPages, gotDigest, err)
		}
	}
	for name, selected := range map[string][][]byte{
		"duplicate": {pages[0], pages[0], pages[1], pages[2]},
		"skip":      {pages[0], pages[2]},
		"reorder":   {pages[1], pages[0], pages[2]},
	} {
		if _, _, err := replay(selected); err == nil {
			t.Fatalf("%s mutation proof page sequence was accepted", name)
		}
	}
	if err := validateSourceTaskMutationProofPartition(1, sourceTaskProofPageItemLimit-1); err == nil {
		t.Fatal("non-terminal short mutation proof page was accepted")
	}
	tampered := append([]byte(nil), pages[1]...)
	tampered[len(tampered)-2] ^= 1
	if _, _, err := replay([][]byte{pages[0], tampered}); err == nil {
		t.Fatal("tampered mutation proof page was accepted")
	}
	terminal := sourceTaskMutationProofsResponse{
		Protocol: sourceTaskProtocol, Count: uint32(len(proofs)), Digest: digest,
	}
	if err := validateSourceTaskMutationProofTerminal(terminal, cursor, digest, len(proofs)); err != nil {
		t.Fatalf("valid mutation proof terminal: %v", err)
	}
	terminal.Count--
	if err := validateSourceTaskMutationProofTerminal(terminal, cursor, digest, len(proofs)); err == nil {
		t.Fatal("mutation proof terminal count mismatch was accepted")
	}
	terminal.Count = uint32(len(proofs))
	terminal.Digest[0] ^= 1
	if err := validateSourceTaskMutationProofTerminal(terminal, cursor, digest, len(proofs)); err == nil {
		t.Fatal("mutation proof terminal digest mismatch was accepted")
	}
}

func sourceTaskScaleRoots(count int) []RootSpec {
	roots := make([]RootSpec, count)
	for index := range roots {
		roots[index] = RootSpec{
			Authority:  causal.SourceAuthorityID("authority"),
			ID:         RootID(fmt.Sprintf("root-%05d", index)),
			Path:       fmt.Sprintf("/source-roots/root-%05d", index),
			Kind:       RootDirectory,
			Generation: 1,
		}
	}
	return roots
}

func mutationIDForScale(value int) catalog.MutationID {
	var operation catalog.MutationID
	binary.BigEndian.PutUint64(operation[len(operation)-8:], uint64(value))
	return operation
}
