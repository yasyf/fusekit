package holder

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/daemonkit"
	"github.com/yasyf/fusekit/catalog"
	"github.com/yasyf/fusekit/internal/recoveryid"
)

type recordingProcessCloser struct {
	calls atomic.Int32
	err   error
}

func (c *recordingProcessCloser) Close() error {
	c.calls.Add(1)
	return c.err
}

func testManagedProcessOwner(t *testing.T) *processOwner {
	t.Helper()
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	owned, err := daemonkit.OwnProcesses(ctx, filepath.Join(dir, "managed.records"))
	if err != nil {
		t.Fatalf("OwnProcesses: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := owned.Close(closeCtx); err != nil {
			t.Errorf("close process ownership: %v", err)
		}
	})
	ledger, err := openProcessLedger(filepath.Join(dir, "managed.json"))
	if err != nil {
		t.Fatalf("openProcessLedger: %v", err)
	}
	return &processOwner{
		spawner: owned, runner: owned,
		spawnSlots: newWorkerSlots(1), runSlots: newWorkerSlots(1),
		ledger: ledger,
	}
}

// managedProcessTestContext supplies the deadline daemonkit's Spawn and Stop
// require; t.Context() carries none, and an undeadlined call is refused before
// it reaches the behavior under test.
func managedProcessTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func testManagedProcess(
	t *testing.T,
	owner *processOwner,
	executable string,
	closer io.Closer,
) managedProcess {
	t.Helper()
	process, err := managedProcessPreparer{owner: owner}.Prepare(managedSpawnConfig{
		id: recoveryid.NativeMount,
		cmd: daemonkit.Cmd{
			Path: executable, Args: []string{"120"},
			Exec: daemonkit.ServingSameUser(), Session: true,
		},
		channel: daemonkit.ChannelNone,
	}, &ownedProcessWriter{Writer: io.Discard, closer: closer})
	if err != nil {
		t.Fatalf("prepare managed process: %v", err)
	}
	return process
}

func TestPreparedManagedProcessSettlementOwnsOutputLifetime(t *testing.T) {
	closer := &recordingProcessCloser{err: errors.New("close output")}
	owner := testManagedProcessOwner(t)
	process := testManagedProcess(t, owner, "/bin/sleep", closer)
	if managedProcessSettled(process) {
		t.Fatal("prepared managed process settled before dispatch")
	}
	if err := process.Start(managedProcessTestContext(t)); err != nil {
		t.Fatalf("start managed process: %v", err)
	}

	record := process.Record()
	if record.PID <= 1 || !record.ProcessGroup || record.SessionID != record.PID ||
		record.RecoveryID != recoveryid.NativeMount || record.Generation != owner.ledger.Generation() {
		t.Fatalf("dispatched process record = %#v", record)
	}
	select {
	case <-process.Done():
		t.Fatal("managed process settled while the child was live")
	default:
	}
	if got := closer.calls.Load(); got != 0 {
		t.Fatalf("output closed before child settlement: %d", got)
	}

	if err := process.Stop(managedProcessTestContext(t)); err != nil {
		t.Fatalf("stop managed process with a failing output closer: %v", err)
	}
	<-process.Done()
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("output closes = %d, want 1", got)
	}
	if _, ok := process.Exit(); !ok {
		t.Fatal("settled managed process has no exit result")
	}
	if !managedProcessSettled(process) {
		t.Fatal("settled managed process does not report settlement")
	}
}

func TestPreparedManagedProcessStopBeforeStartOwnsOutputExactlyOnce(t *testing.T) {
	closer := &recordingProcessCloser{}
	process := testManagedProcess(t, testManagedProcessOwner(t), "/bin/sleep", closer)
	if err := process.Stop(managedProcessTestContext(t)); err != nil {
		t.Fatalf("stop undispatched managed process: %v", err)
	}
	<-process.Done()
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("output closes = %d, want 1", got)
	}
	if err := process.Stop(managedProcessTestContext(t)); err != nil {
		t.Fatalf("replayed stop: %v", err)
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("output closes after replayed stop = %d, want 1", got)
	}
	if record := process.Record(); record != (catalog.ProcessRecord{}) {
		t.Fatalf("undispatched process record = %#v, want zero", record)
	}
	if _, ok := process.Exit(); ok {
		t.Fatal("undispatched managed process reported an exit result")
	}
	if !managedProcessSettled(process) {
		t.Fatal("stopped undispatched managed process does not report settlement")
	}
	if err := process.Start(managedProcessTestContext(t)); err == nil {
		t.Fatal("stopped managed process accepted a dispatch")
	}
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("output closes after refused dispatch = %d, want 1", got)
	}
}

func TestPreparedManagedProcessFailedDispatchOwnsOutputExactlyOnce(t *testing.T) {
	closer := &recordingProcessCloser{}
	absent := filepath.Join(t.TempDir(), "absent-executable")
	process := testManagedProcess(t, testManagedProcessOwner(t), absent, closer)
	err := process.Start(managedProcessTestContext(t))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("dispatch of an absent executable = %v, want %v", err, fs.ErrNotExist)
	}
	<-process.Done()
	if got := closer.calls.Load(); got != 1 {
		t.Fatalf("output closes = %d, want 1", got)
	}
	if record := process.Record(); record != (catalog.ProcessRecord{}) {
		t.Fatalf("failed dispatch process record = %#v, want zero", record)
	}
	if _, ok := process.Exit(); ok {
		t.Fatal("failed dispatch reported an exit result")
	}
}
