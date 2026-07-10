package mountd

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// closableHolder is a hand-wired raw holder for the Retire tests: it serves scripted
// replies and can close its listener on demand, so WaitGone can be driven to gone
// (clean step-down) or lingering (wedged holder). startRawHolder cannot close mid-test.
type closableHolder struct {
	socket   string
	ln       net.Listener
	mu       sync.Mutex
	requests []string
	closed   atomic.Bool
}

// newClosableHolder stands up a closable raw holder. respond maps a request line
// to a reply ("" hangs up without replying). Cleanup closes the listener.
func newClosableHolder(t *testing.T, respond func(reqLine string) string) *closableHolder {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ccp-retire")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "m.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	h := &closableHolder{socket: socket, ln: ln}
	t.Cleanup(h.Close)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadString('\n')
				if err != nil {
					return
				}
				h.mu.Lock()
				h.requests = append(h.requests, strings.TrimSuffix(line, "\n"))
				h.mu.Unlock()
				reply := respond(line)
				if reply == "" {
					return
				}
				_, _ = io.WriteString(conn, reply+"\n")
			}(conn)
		}
	}()
	return h
}

// Close stops the listener so a subsequent dial fails — WaitGone then reports
// the socket gone. Idempotent.
func (h *closableHolder) Close() {
	if h.closed.CompareAndSwap(false, true) {
		h.ln.Close()
	}
}

func (h *closableHolder) reqs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.requests...)
}

// shutdownOKReply is the canned OpShutdown ack the Retire tests script.
const shutdownOKReply = `{"proto":1,"ok":true}`

func retireMount(base, dir string) MountInfo { return MountInfo{Base: base, Dir: dir} }

// TestRetireHappyPath: a clean step-down (socket gone before the wait elapses) runs
// Spawn + carcass-clear + Remount and never reaps or kills.
func TestRetireHappyPath(t *testing.T) {
	var h *closableHolder
	h = newClosableHolder(t, func(req string) string {
		// Ack shutdown, then close the listener so the post-Shutdown WaitGone (dials
		// after this reply is written) reports the socket gone; closing does not disturb
		// this already-accepted connection's reply.
		if strings.Contains(req, `"op":"shutdown"`) {
			h.Close()
			return shutdownOKReply
		}
		return shutdownOKReply
	})
	var killed killCall
	setPeerSeams(t,
		func(string) (int, error) { return 12345, nil },
		func(pid int, sig syscall.Signal) error { killed = killCall{pid, sig}; return nil })

	var unmounted []string
	spawned := false
	remounted := false
	mounts := []MountInfo{retireMount("/b1", "/d1"), retireMount("/b2", "/d2")}

	err := Retire(context.Background(), RetirePlan{
		Client:       NewClient(h.socket),
		CapturedPID:  12345,
		WaitGone:     2 * time.Second,
		KillWait:     2 * time.Second,
		Mounts:       mounts,
		ClearCarcass: func(dir string) { unmounted = append(unmounted, dir) },
		Spawn:        func() error { spawned = true; return nil },
		Remount:      func() error { remounted = true; return nil },
	})
	if err != nil {
		t.Fatalf("Retire happy path = %v, want nil", err)
	}
	if killed.pid != 0 {
		t.Errorf("a kill was sent (%+v); a holder that stepped down cleanly must not be reaped", killed)
	}
	if !spawned {
		t.Error("Spawn was not called")
	}
	if !remounted {
		t.Error("Remount was not called")
	}
	if want := []string{"/d1", "/d2"}; !equalStrs(unmounted, want) {
		t.Errorf("ClearCarcass dirs = %v, want %v (every Mounts dir)", unmounted, want)
	}
}

// retireEvent is one ordered side-effect the carcass-ordering test records.
type retireEvent struct {
	kind string // "unmount" or "remount"
	dir  string
}

// TestRetireCarcassClearBeforeRemount asserts each dir's ClearCarcass is recorded
// before that dir's remount (the carcass-clear-before-remount invariant).
func TestRetireCarcassClearBeforeRemount(t *testing.T) {
	h := newClosableHolder(t, func(string) string { return shutdownOKReply })
	h.Close() // gone immediately: no reap leg; Shutdown hits a closed socket → ErrHolderUnavailable (tolerated)
	setPeerSeams(t,
		func(string) (int, error) { return 0, ErrUnreachable },
		func(int, syscall.Signal) error { t.Fatal("no kill expected"); return nil })

	mounts := []MountInfo{retireMount("/b1", "/d1"), retireMount("/b2", "/d2"), retireMount("/b3", "/d3")}
	var events []retireEvent
	var mu sync.Mutex
	record := func(kind, dir string) {
		mu.Lock()
		events = append(events, retireEvent{kind, dir})
		mu.Unlock()
	}

	err := Retire(context.Background(), RetirePlan{
		Client:       NewClient(h.socket),
		CapturedPID:  999,
		WaitGone:     50 * time.Millisecond,
		KillWait:     50 * time.Millisecond,
		Mounts:       mounts,
		ClearCarcass: func(dir string) { record("unmount", dir) },
		Spawn:        func() error { return nil },
		Remount: func() error {
			for _, m := range mounts {
				record("remount", m.Dir)
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Retire = %v, want nil", err)
	}
	for _, m := range mounts {
		ui, ri := -1, -1
		for i, e := range events {
			if e.dir == m.Dir && e.kind == "unmount" {
				ui = i
			}
			if e.dir == m.Dir && e.kind == "remount" {
				ri = i
			}
		}
		if ui == -1 || ri == -1 {
			t.Fatalf("dir %s missing an event (unmount=%d remount=%d) in %v", m.Dir, ui, ri, events)
		}
		if ui >= ri {
			t.Errorf("dir %s: ClearCarcass at %d did not precede remount at %d (carcass-clear-before-remount invariant)", m.Dir, ui, ri)
		}
	}
}

// TestRetireMuxCarcassClearsNativeRootOnce pins the root-aware carcass clear: a
// snapshot with mux subtree rows clears each distinct NATIVE root exactly
// once — the shared MuxRoot for subtree rows, the Dir for a plain row — and never
// a subtree path, so the real carcass at the root is cleared and the clear stays
// root-only.
func TestRetireMuxCarcassClearsNativeRootOnce(t *testing.T) {
	h := newClosableHolder(t, func(string) string { return shutdownOKReply })
	h.Close() // gone immediately: no reap leg
	setPeerSeams(t,
		func(string) (int, error) { return 0, ErrUnreachable },
		func(int, syscall.Signal) error { t.Fatal("no kill expected"); return nil })

	const root = "/pool/mnt"
	mounts := []MountInfo{
		{Base: "/pool/base", Dir: "/pool/mnt/acct-01", MuxRoot: root},
		{Base: "/pool/base", Dir: "/pool/mnt/acct-02", MuxRoot: root},
		{Base: "/pool/other", Dir: "/pool/plain"}, // a plain row clears its own Dir
	}
	var mu sync.Mutex
	var unmounted []string
	err := Retire(context.Background(), RetirePlan{
		Client:         NewClient(h.socket),
		CapturedPID:    0,
		CapturedPIDErr: errors.New("no pid"),
		WaitGone:       50 * time.Millisecond,
		KillWait:       50 * time.Millisecond,
		Mounts:         mounts,
		ClearCarcass:   func(dir string) { mu.Lock(); unmounted = append(unmounted, dir); mu.Unlock() },
		Spawn:          func() error { return nil },
		Remount:        func() error { return nil },
	})
	if err != nil {
		t.Fatalf("Retire = %v, want nil", err)
	}
	mu.Lock()
	defer mu.Unlock()
	countRoot, countPlain := 0, 0
	for _, d := range unmounted {
		switch d {
		case root:
			countRoot++
		case "/pool/plain":
			countPlain++
		case "/pool/mnt/acct-01", "/pool/mnt/acct-02":
			t.Errorf("ClearCarcass targeted subtree path %q; carcass clears must be root-only", d)
		default:
			t.Errorf("ClearCarcass targeted unexpected path %q", d)
		}
	}
	if countRoot != 1 {
		t.Errorf("native root %s cleared %d times, want exactly 1 (deduped across its subtree rows)", root, countRoot)
	}
	if countPlain != 1 {
		t.Errorf("plain dir cleared %d times, want exactly 1", countPlain)
	}
}

// TestRetireDeferPolicySkipsRoot pins the kernel-panic invariant on the legacy
// retire path: a root ANY row declares CarcassPolicy "defer" for — a plain
// defer row, or a mux root with one deferring tenant — is never passed to
// ClearCarcass, while force/absent-policy roots still clear. Remount runs
// regardless (a lingering defer carcass surfaces there as ErrForeignMount).
func TestRetireDeferPolicySkipsRoot(t *testing.T) {
	h := newClosableHolder(t, func(string) string { return shutdownOKReply })
	h.Close() // gone immediately: no reap leg
	setPeerSeams(t,
		func(string) (int, error) { return 0, ErrUnreachable },
		func(int, syscall.Signal) error { t.Fatal("no kill expected"); return nil })

	const muxRoot = "/pool/mnt"
	mounts := []MountInfo{
		{Base: "/b/legacy", Dir: "/d/legacy"},                                                  // absent policy = force (old holder)
		{Base: "/b/force", Dir: "/d/force", CarcassPolicy: "force"},                            // explicit force
		{Base: "/b/defer", Dir: "/d/defer", CarcassPolicy: "defer"},                            // plain defer row
		{Base: "/b/mux-f", Dir: "/pool/mnt/acct-01", MuxRoot: muxRoot, CarcassPolicy: "force"}, // force tenant...
		{Base: "/b/mux-d", Dir: "/pool/mnt/acct-02", MuxRoot: muxRoot, CarcassPolicy: "defer"}, // ...but one defer tenant defers the root
	}
	var mu sync.Mutex
	var cleared []string
	remounted := false
	err := Retire(context.Background(), RetirePlan{
		Client:         NewClient(h.socket),
		CapturedPID:    0,
		CapturedPIDErr: errors.New("no pid"),
		WaitGone:       50 * time.Millisecond,
		KillWait:       50 * time.Millisecond,
		Mounts:         mounts,
		ClearCarcass:   func(dir string) { mu.Lock(); cleared = append(cleared, dir); mu.Unlock() },
		Spawn:          func() error { return nil },
		Remount:        func() error { remounted = true; return nil },
	})
	if err != nil {
		t.Fatalf("Retire = %v, want nil", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if want := []string{"/d/legacy", "/d/force"}; !equalStrs(cleared, want) {
		t.Errorf("ClearCarcass dirs = %v, want exactly %v", cleared, want)
	}
	for _, d := range cleared {
		if d == "/d/defer" || d == muxRoot {
			t.Errorf("ClearCarcass targeted defer-policy root %q; CarcassPolicy defer forbids every autonomous clear", d)
		}
	}
	if !remounted {
		t.Error("Remount was not called; deferred roots must still surface through the remount")
	}
}

// TestRetireLingeringSocketReaps: a socket lingering past WaitGone with a captured
// pid (CapturedPIDErr==nil) fires KillPeer(CapturedPID) and a second WaitGone.
func TestRetireLingeringSocketReaps(t *testing.T) {
	h := newClosableHolder(t, func(req string) string {
		if strings.Contains(req, `"op":"shutdown"`) {
			return shutdownOKReply
		}
		return shutdownOKReply
	})
	// never closed → WaitGone always reports lingering
	const wedgedPID = 778899
	var killed killCall
	var peerCalls atomic.Int64
	setPeerSeams(t,
		func(string) (int, error) { peerCalls.Add(1); return wedgedPID, nil },
		func(pid int, sig syscall.Signal) error { killed = killCall{pid, sig}; return nil })

	err := Retire(context.Background(), RetirePlan{
		Client:       NewClient(h.socket),
		CapturedPID:  wedgedPID,
		WaitGone:     10 * time.Millisecond,
		KillWait:     10 * time.Millisecond,
		Mounts:       nil,
		ClearCarcass: func(string) {},
		Spawn:        func() error { return nil },
		Remount:      func() error { return nil },
	})
	if err != nil {
		t.Fatalf("Retire over a lingering socket = %v, want nil", err)
	}
	if killed.pid != wedgedPID || killed.sig != syscall.SIGKILL {
		t.Errorf("kill = %+v, want SIGKILL to the captured wedged pid %d", killed, wedgedPID)
	}
}

// TestRetireCapturedPIDErrDisablesReap: a lingering socket with a failed pid capture
// (CapturedPIDErr!=nil) disables the reap — KillPeer never fires (no identity to gate
// on; never a name kill).
func TestRetireCapturedPIDErrDisablesReap(t *testing.T) {
	h := newClosableHolder(t, func(string) string { return shutdownOKReply })
	setPeerSeams(t,
		func(string) (int, error) {
			t.Fatal("peerPIDFn called despite a capture error: the reap must be disabled")
			return 0, nil
		},
		func(int, syscall.Signal) error { t.Fatal("killProc called despite a capture error"); return nil })

	err := Retire(context.Background(), RetirePlan{
		Client:         NewClient(h.socket),
		CapturedPID:    0,
		CapturedPIDErr: errors.New("peer pid unresolved"),
		WaitGone:       10 * time.Millisecond,
		KillWait:       10 * time.Millisecond,
		Mounts:         nil,
		ClearCarcass:   func(string) {},
		Spawn:          func() error { return nil },
		Remount:        func() error { return nil },
	})
	if err != nil {
		t.Fatalf("Retire with a disabled reap = %v, want nil", err)
	}
}

// TestRetireShutdownUnavailableTolerated: an ErrHolderUnavailable Shutdown (holder
// already gone) is tolerated — Retire proceeds to spawn + remount, and the
// unreachable socket makes WaitGone report gone (no reap).
func TestRetireShutdownUnavailableTolerated(t *testing.T) {
	socket := filepath.Join(shortSockDir(t), "absent.sock") // no listener → ErrHolderUnavailable
	setPeerSeams(t,
		func(string) (int, error) { return 0, ErrUnreachable },
		func(int, syscall.Signal) error { t.Fatal("no kill expected for an already-gone holder"); return nil })

	spawned, remounted := false, false
	err := Retire(context.Background(), RetirePlan{
		Client:       NewClient(socket),
		CapturedPID:  111,
		WaitGone:     50 * time.Millisecond,
		KillWait:     50 * time.Millisecond,
		Mounts:       []MountInfo{retireMount("/b", "/d")},
		ClearCarcass: func(string) {},
		Spawn:        func() error { spawned = true; return nil },
		Remount:      func() error { remounted = true; return nil },
	})
	if err != nil {
		t.Fatalf("Retire with an ErrHolderUnavailable Shutdown = %v, want nil (tolerated)", err)
	}
	if !spawned || !remounted {
		t.Errorf("after a tolerated Shutdown: spawned=%v remounted=%v, want both true", spawned, remounted)
	}
}

// TestRetireShutdownErrorOtherThanUnavailable: a non-ErrHolderUnavailable Shutdown
// error returns wrapped "retire holder:" and never proceeds to spawn.
func TestRetireShutdownErrorOtherThanUnavailable(t *testing.T) {
	// Wedged error class → respErr maps to ErrUnmountWedged, which is NOT ErrHolderUnavailable.
	h := newClosableHolder(t, func(req string) string {
		if strings.Contains(req, `"op":"shutdown"`) {
			return fmt.Sprintf(`{"proto":1,"ok":false,"error":"sweep wedged","err_class":%q}`, ClassWedged)
		}
		return shutdownOKReply
	})
	spawned := false
	err := Retire(context.Background(), RetirePlan{
		Client:       NewClient(h.socket),
		CapturedPID:  222,
		WaitGone:     50 * time.Millisecond,
		KillWait:     50 * time.Millisecond,
		Mounts:       nil,
		ClearCarcass: func(string) {},
		Spawn:        func() error { spawned = true; return nil },
		Remount:      func() error { return nil },
	})
	if err == nil {
		t.Fatal("Retire with a non-unavailable Shutdown error = nil, want a wrapped error")
	}
	if !strings.Contains(err.Error(), "retire holder:") {
		t.Errorf("error = %q, want it wrapped with %q", err, "retire holder:")
	}
	if !errors.Is(err, ErrUnmountWedged) {
		t.Errorf("error = %v, want the underlying ErrUnmountWedged preserved", err)
	}
	if spawned {
		t.Error("Spawn ran despite a fatal Shutdown error; Retire must abort before spawn")
	}
	_ = fmt.Sprint(h.reqs()) // keep reqs referenced
}

func equalStrs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
