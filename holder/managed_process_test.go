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

// TestBudgetedAlwaysStatesADeadlineAndDefersToAStatedOne pins both halves of
// the contract every spawn and teardown reaching a daemonkit verb depends on:
// a caller that stated no deadline still gets one, and a caller that stated
// its own keeps it exactly.
func TestBudgetedAlwaysStatesADeadlineAndDefersToAStatedOne(t *testing.T) {
	const budget = 10 * time.Second
	stricter, cancelStricter := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStricter()
	looser, cancelLooser := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelLooser()

	tests := []struct {
		name   string
		parent context.Context
		want   time.Duration // 0 => the budget, otherwise the parent's own deadline
	}{
		{"deadline-less", context.Background(), 0},
		{"cancellation stripped", context.WithoutCancel(stricter), 0},
		{"stricter caller deadline", stricter, 5 * time.Second},
		{"looser caller deadline", looser, 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := budgeted(tt.parent, budget)
			defer cancel()
			deadline, stated := ctx.Deadline()
			if !stated {
				t.Fatal("budgeted returned a deadline-less context; every daemonkit verb refuses one")
			}
			want := tt.want
			if want == 0 {
				want = budget
			}
			if got := time.Until(deadline); got <= 0 || got > want {
				t.Fatalf("budget = %v, want (0, %v]", got, want)
			}
			if tt.want == 0 {
				return
			}
			parentDeadline, _ := tt.parent.Deadline()
			if !deadline.Equal(parentDeadline) {
				t.Fatalf("deadline = %v, want the caller's own %v", deadline, parentDeadline)
			}
		})
	}
}

// TestSpawnBudgetsADeadlinelessCaller proves a spawn dispatched on a context
// carrying no deadline still spawns: daemonkit's Spawn refuses one, and the
// broker relaunch loop reaches this path on exactly that.
func TestSpawnBudgetsADeadlinelessCaller(t *testing.T) {
	owner := testManagedProcessOwner(t)
	process := testManagedProcess(t, owner, "/bin/sleep", &recordingProcessCloser{})
	if err := process.Start(context.Background()); err != nil {
		t.Fatalf("dispatch with a deadline-less caller: %v", err)
	}
	if err := process.Stop(managedProcessTestContext(t)); err != nil {
		t.Fatalf("stop managed process: %v", err)
	}
	<-process.Done()
	if _, ok := process.Exit(); !ok {
		t.Fatal("settled managed process has no exit result")
	}
}

// TestPreparedManagedProcessStopBudgetsADeadlinelessCaller proves the teardown
// states its own budget rather than passing the caller's context straight
// through: every native and broker teardown hands this Stop a context carrying
// no deadline — context.Background(), or one stripped by context.WithoutCancel
// — which daemonkit's Stop refuses, turning the teardown into a no-op that
// leaves the child running.
func TestPreparedManagedProcessStopBudgetsADeadlinelessCaller(t *testing.T) {
	owner := testManagedProcessOwner(t)
	process := testManagedProcess(t, owner, "/bin/sleep", &recordingProcessCloser{})
	if err := process.Start(managedProcessTestContext(t)); err != nil {
		t.Fatalf("start managed process: %v", err)
	}
	stopped := make(chan error, 1)
	go func() { stopped <- process.Stop(context.WithoutCancel(managedProcessTestContext(t))) }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stop with a deadline-less caller: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("stop with a deadline-less caller did not settle")
	}
	<-process.Done()
	if _, ok := process.Exit(); !ok {
		t.Fatal("settled managed process has no exit result")
	}
}
