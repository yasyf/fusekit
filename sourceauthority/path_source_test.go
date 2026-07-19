package sourceauthority

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestDirectScanCloseCancelsBlockedNext(t *testing.T) {
	t.Parallel()
	scanCtx, cancel := context.WithCancel(context.Background())
	values := make(chan directScanValue)
	done := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		defer close(done)
		<-scanCtx.Done()
		close(values)
		result <- nil
	}()
	session := &directScanSession{cancel: cancel, values: values, done: done, result: result}
	nextDone := make(chan error, 1)
	go func() {
		_, err := session.Next(context.Background(), 1)
		nextDone <- err
	}()
	closed := make(chan error, 1)
	go func() { closed <- session.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel a blocked direct scan Next")
	}
	if err := <-nextDone; err != nil && !errors.Is(err, ErrClosed) {
		t.Fatalf("blocked Next error = %v", err)
	}
}

func TestDirectScanCloseJoinsAndReplaysTerminalError(t *testing.T) {
	t.Parallel()
	terminalErr := errors.New("scan failed")
	scanCtx, cancel := context.WithCancel(context.Background())
	values := make(chan directScanValue)
	done := make(chan struct{})
	result := make(chan error, 1)
	canceled := make(chan struct{})
	release := make(chan struct{})
	go func() {
		defer close(done)
		<-scanCtx.Done()
		close(canceled)
		<-release
		close(values)
		result <- terminalErr
	}()
	session := &directScanSession{cancel: cancel, values: values, done: done, result: result}
	first := make(chan error, 1)
	second := make(chan error, 1)
	go func() { first <- session.Close() }()
	go func() { second <- session.Close() }()
	<-canceled
	select {
	case err := <-first:
		t.Fatalf("first Close returned before scan settlement: %v", err)
	case err := <-second:
		t.Fatalf("second Close returned before scan settlement: %v", err)
	default:
	}
	close(release)
	for index, result := range []<-chan error{first, second} {
		if err := <-result; !errors.Is(err, terminalErr) {
			t.Fatalf("Close[%d] = %v, want terminal error", index, err)
		}
	}
	if err := session.Close(); !errors.Is(err, terminalErr) {
		t.Fatalf("repeated Close = %v, want terminal error", err)
	}
}

func TestSecurePathSourceUsesCtimeInPinnedFence(t *testing.T) {
	t.Parallel()
	left := unix.Stat_t{Dev: 1, Ino: 2, Mode: unix.S_IFREG | 0o600, Size: 3}
	right := left
	right.Ctim.Nsec++
	if samePinnedStat(left, right) {
		t.Fatal("ctime-only replacement passed the pinned stat fence")
	}
}

func TestSecurePathSourceReadsSymlinkAtExactLstatSize(t *testing.T) {
	t.Parallel()
	rootPath := canonicalTemporaryDirectory(t)
	target := strings.Repeat("segment/", 100) + "target"
	if err := os.Symlink(target, filepath.Join(rootPath, "link")); err != nil {
		t.Fatal(err)
	}
	root := testPinnedRoot(t, RootSpec{Authority: "authority", ID: "root", Path: rootPath, Kind: RootDirectory, Generation: 1})
	entry, err := (securePathSource{}).Stat(t.Context(), root, "link")
	if err != nil {
		t.Fatal(err)
	}
	if entry.Kind != PhysicalSymlink || entry.LinkTarget != target || entry.Size != int64(len(target)) {
		t.Fatalf("symlink entry = kind %d size %d target %q", entry.Kind, entry.Size, entry.LinkTarget)
	}
}

func TestSecurePathSourceRejectsCrossDeviceDescendant(t *testing.T) {
	t.Parallel()
	if sameSourceDevice(1, unix.Stat_t{Dev: 2}) == nil {
		t.Fatal("cross-device descendant was accepted")
	}
}

func TestSecurePathSourceIteratesDeepTreeWithoutRecursiveWalk(t *testing.T) {
	t.Parallel()
	rootPath := canonicalTemporaryDirectory(t)
	fd, err := unix.Open(rootPath, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	current := fd
	const depth = 512
	for index := 0; index < depth; index++ {
		if err := unix.Mkdirat(current, "d", 0o700); err != nil {
			t.Fatal(err)
		}
		next, err := unix.Openat(current, "d", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			t.Fatal(err)
		}
		if current != fd {
			_ = unix.Close(current)
		}
		current = next
	}
	_ = unix.Close(current)
	_ = unix.Close(fd)
	root := testPinnedRoot(t, RootSpec{Authority: "authority", ID: "root", Path: rootPath, Kind: RootDirectory, Generation: 1})
	scan, err := (securePathSource{}).BeginScan(t.Context(), []RootSpec{root})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scan.Close() }()
	count := 0
	for {
		page, err := scan.Next(t.Context(), maxScanPageSize)
		if err != nil {
			t.Fatal(err)
		}
		count += len(page.Entries)
		if page.Next == "" {
			break
		}
	}
	if count != depth {
		t.Fatalf("deep scan count = %d, want %d", count, depth)
	}
}

func TestMaterializerInputIsUnlinkedImmutableSnapshot(t *testing.T) {
	t.Parallel()
	rootPath := canonicalTemporaryDirectory(t)
	path := filepath.Join(rootPath, "value")
	if err := os.WriteFile(path, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := testPinnedRoot(t, RootSpec{Authority: "authority", ID: "root", Path: rootPath, Kind: RootDirectory, Generation: 1})
	expected, err := (securePathSource{}).Stat(t.Context(), root, "value")
	if err != nil {
		t.Fatal(err)
	}
	task := MaterializationTask{
		Fence: testRootFence(t, []RootSpec{root}), Roots: []RootSpec{root}, Tenants: testSourceMaterializationTask(t, nil).Tenants,
		Request:  MaterializationRequest{Logical: "logical", Inputs: []PathRef{{Root: "root", Relative: "value"}}},
		Expected: []PhysicalEntry{expected},
	}
	policyTask, snapshots, err := prepareMaterializerTask(t.Context(), canonicalTemporaryDirectory(t), task)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = snapshots.Close() }()
	if err := os.WriteFile(path, []byte("after"), 0o600); err != nil {
		t.Fatal(err)
	}
	reader, err := policyTask.Inputs[0].Content.Open(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	if string(got) != "before" {
		t.Fatalf("immutable input = %q, want before", got)
	}
}

func TestMaterializerInputRejectsSymlinkReplacement(t *testing.T) {
	t.Parallel()
	rootPath := canonicalTemporaryDirectory(t)
	path := filepath.Join(rootPath, "value")
	if err := os.WriteFile(path, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	root := testPinnedRoot(t, RootSpec{Authority: "authority", ID: "root", Path: rootPath, Kind: RootDirectory, Generation: 1})
	expected, err := (securePathSource{}).Stat(t.Context(), root, "value")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("outside", path); err != nil {
		t.Fatal(err)
	}
	task := MaterializationTask{
		Fence: testRootFence(t, []RootSpec{root}), Roots: []RootSpec{root}, Tenants: testSourceMaterializationTask(t, nil).Tenants,
		Request:  MaterializationRequest{Logical: "logical", Inputs: []PathRef{{Root: "root", Relative: "value"}}},
		Expected: []PhysicalEntry{expected},
	}
	if _, _, err := prepareMaterializerTask(t.Context(), canonicalTemporaryDirectory(t), task); !errors.Is(err, ErrSourceChanged) {
		t.Fatalf("symlink replacement error = %v, want ErrSourceChanged", err)
	}
}

func canonicalTemporaryDirectory(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func testPinnedRoot(t *testing.T, root RootSpec) RootSpec {
	t.Helper()
	identity, err := (securePathSource{}).RootIdentity(t.Context(), root)
	if err != nil {
		t.Fatal(err)
	}
	root.ExpectedIdentity = identity
	return root
}

func testRootFence(t *testing.T, roots []RootSpec) Fence {
	t.Helper()
	return rootFenceForTest(roots)
}

func rootFenceForTest(roots []RootSpec) Fence {
	observed := make([]observedRoot, len(roots))
	for index, root := range roots {
		declared := root
		declared.ExpectedIdentity = FileIdentity{}
		observed[index] = observedRoot{Spec: declared, Identity: root.ExpectedIdentity}
	}
	digest, err := digestJSON(observed)
	if err != nil {
		panic(err)
	}
	return Fence{Authority: roots[0].Authority, AuthorityGeneration: 1, RootDigest: digest}
}
