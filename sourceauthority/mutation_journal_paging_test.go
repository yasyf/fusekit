package sourceauthority

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/causal"
	"golang.org/x/sys/unix"
)

func TestMutationJournalDirectoryScanPagesAllEntriesWithinBound(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(directoryFD) }()
	const extra = 17
	total := mutationJournalPageEntryLimit*2 + extra
	for index := range total {
		writeMutationJournalPagingArtifact(t, runtimeDir, index)
	}
	visited := make(map[string]struct{}, total)
	if err := scanMutationJournalDirectory(
		t.Context(), directoryFD, total,
		func(entry os.DirEntry) error {
			visited[entry.Name()] = struct{}{}
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if len(visited) != total {
		t.Fatalf("visited entries = %d, want %d", len(visited), total)
	}
}

func TestMutationTerminalProofsPageAcrossBoundedDirectory(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(directoryFD); err != nil {
		t.Fatal(err)
	}
	const extra = 17
	total := mutationJournalPageEntryLimit*8 + extra
	for index := range total {
		operation := mutationJournalPagingOperation(index)
		writeMutationJournalPagingTerminal(t, runtimeDir, operation, "paged-authority")
	}
	proofs, err := collectMutationTerminalProofPages(t, runtimeDir, "paged-authority")
	if err != nil {
		t.Fatal(err)
	}
	if len(proofs) != total {
		t.Fatalf("terminal proofs = %d, want %d", len(proofs), total)
	}
	for index, proof := range proofs {
		if proof.Operation != mutationJournalPagingOperation(index) {
			t.Fatalf("terminal proof %d operation = %s", index, proof.Operation)
		}
	}
	replayed, err := collectMutationTerminalProofPages(t, runtimeDir, "paged-authority")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(proofs, replayed) {
		t.Fatal("bounded terminal proof replay changed deterministic order")
	}
}

func TestMutationTerminalProofPageAdvancesAcrossSparseForeignEntries(t *testing.T) {
	runtimeDir := shortTaskRuntimeDir(t)
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(directoryFD); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < mutationJournalPageEntryLimit*2; index++ {
		writeMutationJournalPagingTerminal(
			t, runtimeDir, mutationJournalPagingOperation(index), "foreign-authority",
		)
	}
	wanted := mutationJournalPagingOperation(mutationJournalPageEntryLimit * 2)
	writeMutationJournalPagingTerminal(t, runtimeDir, wanted, "wanted-authority")
	page, err := mutationTerminalProofPage(
		t.Context(), runtimeDir, "wanted-authority", catalog.MutationID{}, MutationTerminalProofPageLimit,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Proofs) != 0 || !page.More || page.Next == (catalog.MutationID{}) {
		t.Fatalf("sparse first page = %+v", page)
	}
	for page.More && len(page.Proofs) == 0 {
		previous := page.Next
		page, err = mutationTerminalProofPage(
			t.Context(), runtimeDir, "wanted-authority", previous, MutationTerminalProofPageLimit,
		)
		if err != nil {
			t.Fatal(err)
		}
		if page.More && (page.Next == (catalog.MutationID{}) || page.Next.String() <= previous.String()) {
			t.Fatalf("sparse page did not advance: %+v", page)
		}
	}
	if len(page.Proofs) != 1 || page.Proofs[0].Operation != wanted {
		t.Fatalf("sparse terminal proof page = %+v", page)
	}
}

func collectMutationTerminalProofPages(
	t *testing.T,
	runtimeDir string,
	authority causal.SourceAuthorityID,
) ([]MutationTerminalProof, error) {
	t.Helper()
	var after catalog.MutationID
	var proofs []MutationTerminalProof
	for {
		page, err := mutationTerminalProofPage(
			t.Context(), runtimeDir, authority, after, MutationTerminalProofPageLimit,
		)
		if err != nil {
			return nil, err
		}
		if len(page.Proofs) > MutationTerminalProofPageLimit {
			return nil, errors.New("mutation terminal proof page exceeded its limit")
		}
		proofs = append(proofs, page.Proofs...)
		if !page.More {
			return proofs, nil
		}
		if page.Next == (catalog.MutationID{}) || page.Next.String() <= after.String() {
			return nil, errors.New("mutation terminal proof page cursor did not advance")
		}
		after = page.Next
	}
}

func TestMutationJournalDirectoryPagesRespectEntryAndByteCaps(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(directoryFD) }()
	total := mutationJournalPageEntryLimit*3 + 7
	for index := range total {
		writeMutationJournalPagingArtifact(t, runtimeDir, index)
	}
	after := ""
	visited := 0
	pages := 0
	for {
		page, more, err := pageMutationJournalDirectory(t.Context(), directoryFD, after)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 || len(page) > mutationJournalPageEntryLimit {
			t.Fatalf("page entries = %d, bound %d", len(page), mutationJournalPageEntryLimit)
		}
		pageBytes := 0
		for index, entry := range page {
			pageBytes += len(entry.digest)
			if entry.Name() <= after || index > 0 && entry.Name() <= page[index-1].Name() {
				t.Fatalf("page is not strictly ordered after %q: %+v", after, page)
			}
		}
		if pageBytes > mutationJournalPageByteLimit {
			t.Fatalf("page bytes = %d, bound %d", pageBytes, mutationJournalPageByteLimit)
		}
		visited += len(page)
		pages++
		after = page[len(page)-1].Name()
		if !more {
			break
		}
	}
	entryOnlyPages := (total + mutationJournalPageEntryLimit - 1) / mutationJournalPageEntryLimit
	if visited != total || pages <= entryOnlyPages {
		t.Fatalf("bounded pages visited %d entries in %d pages, want %d entries", visited, pages, total)
	}
}

func TestMutationJournalBoundsStreamBytesAcrossPages(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(directoryFD); err != nil {
		t.Fatal(err)
	}
	const extra = 17
	total := mutationJournalPageEntryLimit + extra
	var bytes int64
	for index := range total {
		bytes += writeMutationJournalPagingTerminal(
			t, runtimeDir, mutationJournalPagingOperation(index), "bounded-authority",
		)
	}
	directoryFD, err = openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(directoryFD) }()
	target := mutationJournalPagingOperation(total).String() + ".json"
	if err := enforceMutationJournalBounds(
		t.Context(), directoryFD, target, 1, total+1, bytes,
	); err == nil {
		t.Fatal("multi-page byte scan admitted one byte beyond capacity")
	}
	if err := enforceMutationJournalBounds(
		t.Context(), directoryFD, target, 1, total+1, bytes+1,
	); err != nil {
		t.Fatalf("multi-page exact byte boundary rejected: %v", err)
	}
}

func TestMutationJournalDirectoryScanHonorsCancellationBetweenEntries(t *testing.T) {
	t.Parallel()
	runtimeDir := shortTaskRuntimeDir(t)
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(directoryFD) }()
	total := mutationJournalPageEntryLimit * 2
	for index := range total {
		writeMutationJournalPagingArtifact(t, runtimeDir, index)
	}
	ctx, cancel := context.WithCancel(t.Context())
	visited := 0
	err = scanMutationJournalDirectory(ctx, directoryFD, total, func(os.DirEntry) error {
		visited++
		if visited == mutationJournalPageEntryLimit+1 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scan = %v, want context.Canceled", err)
	}
	if visited != mutationJournalPageEntryLimit+1 {
		t.Fatalf("visited entries after cancellation = %d, want %d",
			visited, mutationJournalPageEntryLimit+1)
	}
}

func TestMutationJournalDirectoryScanRejectsGrowthAndChurn(t *testing.T) {
	t.Run("entry growth exceeds hard scan cap", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		directoryFD, err := openMutationJournalDirectory(runtimeDir)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = unix.Close(directoryFD) }()
		for index := range 4 {
			writeMutationJournalPagingArtifact(t, runtimeDir, index)
		}
		err = enforceMutationJournalBounds(
			t.Context(), directoryFD, catalog.MutationID{90}.String()+".json",
			1, 1, 100,
		)
		if !errors.Is(err, errMutationJournalDirectoryEntryBound) {
			t.Fatalf("overgrown directory = %v, want entry bound", err)
		}
	})

	t.Run("same-name replacement changes stable scan", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		directoryFD, err := openMutationJournalDirectory(runtimeDir)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = unix.Close(directoryFD) }()
		writeMutationJournalPagingArtifact(t, runtimeDir, 0)
		replaced := false
		err = scanMutationJournalDirectory(
			t.Context(), directoryFD, 3,
			func(entry os.DirEntry) error {
				if replaced {
					return nil
				}
				path := filepath.Join(mutationJournalDirectory(runtimeDir), entry.Name())
				if err := os.Rename(path, path+".held"); err != nil {
					return err
				}
				if err := os.WriteFile(path, []byte("replacement"), 0o600); err != nil {
					return err
				}
				replaced = true
				return nil
			},
		)
		if !errors.Is(err, errMutationJournalDirectoryChanged) {
			t.Fatalf("churned directory = %v, want changed-directory failure", err)
		}
	})
}

func TestMutationJournalDirectoryRejectsOverlongAndCorruptEntriesWithoutBlocking(t *testing.T) {
	t.Run("overlong unknown name", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		prepareMutationJournalPagingDirectory(t, runtimeDir)
		name := strings.Repeat("x", mutationJournalNameByteLimit+1)
		if err := os.WriteFile(filepath.Join(mutationJournalDirectory(runtimeDir), name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := validateMutationJournalDirectory(t.Context(), runtimeDir); err == nil {
			t.Fatal("overlong non-journal entry was accepted")
		}
	})

	t.Run("invalid utf8 name", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		prepareMutationJournalPagingDirectory(t, runtimeDir)
		name := string([]byte{0xff, 0xfe})
		if err := os.WriteFile(filepath.Join(mutationJournalDirectory(runtimeDir), name), []byte("x"), 0o600); err != nil {
			t.Skipf("filesystem rejected invalid UTF-8 fixture: %v", err)
		}
		if err := validateMutationJournalDirectory(t.Context(), runtimeDir); err == nil ||
			!strings.Contains(err.Error(), "entry name") {
			t.Fatalf("invalid UTF-8 entry = %v", err)
		}
	})

	t.Run("corrupt temporary name", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		prepareMutationJournalPagingDirectory(t, runtimeDir)
		name := ".mutation-" + strings.Repeat("a", 31) + ".tmp"
		if err := os.WriteFile(filepath.Join(mutationJournalDirectory(runtimeDir), name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := validateMutationJournalDirectory(t.Context(), runtimeDir); err == nil ||
			!strings.Contains(err.Error(), "corrupt temporary") {
			t.Fatalf("corrupt temporary entry = %v", err)
		}
	})

	t.Run("fifo journal", func(t *testing.T) {
		runtimeDir := shortTaskRuntimeDir(t)
		prepareMutationJournalPagingDirectory(t, runtimeDir)
		path := mutationJournalPath(runtimeDir, mutationJournalPagingOperation(0))
		if err := unix.Mkfifo(path, 0o600); err != nil {
			t.Skipf("mkfifo unavailable: %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		err := validateMutationJournalDirectory(ctx, runtimeDir)
		if err == nil || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("FIFO journal validation = %v", err)
		}
	})
}

func prepareMutationJournalPagingDirectory(t *testing.T, runtimeDir string) {
	t.Helper()
	directoryFD, err := openMutationJournalDirectory(runtimeDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Close(directoryFD); err != nil {
		t.Fatal(err)
	}
}

func writeMutationJournalPagingArtifact(t *testing.T, runtimeDir string, index int) {
	t.Helper()
	path := filepath.Join(
		mutationJournalDirectory(runtimeDir),
		fmt.Sprintf(".mutation-%032x.tmp", index),
	)
	if err := os.WriteFile(path, []byte("temporary"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mutationJournalPagingOperation(index int) catalog.MutationID {
	var operation catalog.MutationID
	binary.BigEndian.PutUint64(operation[len(operation)-8:], uint64(index+1))
	return operation
}

func writeMutationJournalPagingTerminal(
	t *testing.T,
	runtimeDir string,
	operation catalog.MutationID,
	authority causal.SourceAuthorityID,
) int64 {
	t.Helper()
	proof := MutationTerminalProof{
		Authority: authority, AuthorityGeneration: 1, Operation: operation, Outcome: MutationAbandoned,
	}
	payload, err := json.Marshal(testTerminalMutationJournal(proof, sha256.Sum256(operation[:])))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mutationJournalPath(runtimeDir, operation), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	return int64(len(payload))
}
