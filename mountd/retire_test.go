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

// closableHolder is a hand-wired raw holder for the Retire unit tests: it serves
// scripted replies AND lets the test close its listener on demand, so WaitGone
// can be driven to report the socket gone (a real holder stepping down) or
// lingering (a wedged holder that kept its socket). startRawHolder cannot close
// mid-test, so the Retire mechanics — graceful-gone vs reap — need this seam.
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

// reqs returns the recorded request lines.
func (h *closableHolder) reqs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.requests...)
}

// shutdownOKReply is the canned ack the Retire tests script for OpShutdown; an
// unreachable socket (no listener) drives ErrHolderUnavailable on its own.
const shutdownOKReply = `{"proto":1,"ok":true}`

// retireMount is a (base, dir) pair the Retire tests snapshot.
func retireMount(base, dir string) MountInfo { return MountInfo{Base: base, Dir: dir} }

// TestRetireHappyPath: Shutdown is sent and acked, the socket goes gone before the
// wait elapses (no reap), Spawn runs, every Mounts dir is force-unmounted, and
// Remount runs. The peer seams record any kill so the no-kill claim is asserted.
func TestRetireHappyPath(t *testing.T) {
	var h *closableHolder
	h = newClosableHolder(t, func(req string) string {
		// The holder acks shutdown, then closes its listener so the post-Shutdown
		// WaitGone (which dials AFTER this reply is written) reports the socket gone
		// — a clean step-down, no lingering socket. Closing the listener does not
		// disturb this already-accepted connection's reply.
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
		ForceUnmount: func(dir string) { unmounted = append(unmounted, dir) },
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
		t.Errorf("ForceUnmount dirs = %v, want %v (every Mounts dir)", unmounted, want)
	}
}

// retireEvent is one ordered side-effect the carcass-ordering test records.
type retireEvent struct {
	kind string // "unmount" or "remount"
	dir  string
}

// TestRetireCarcassClearBeforeRemount: the INVARIANT. Record an ordered interleave
// of every ForceUnmount(dir) and the remount of each dir; assert each dir's
// ForceUnmount is recorded before that dir's remount.
func TestRetireCarcassClearBeforeRemount(t *testing.T) {
	h := newClosableHolder(t, func(string) string { return shutdownOKReply })
	h.Close() // gone immediately: no reap leg, Shutdown still acks via... it is closed, so ErrHolderUnavailable (tolerated)
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
		ForceUnmount: func(dir string) { record("unmount", dir) },
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
	// For every dir, its unmount index must precede its remount index.
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
			t.Errorf("dir %s: ForceUnmount at %d did not precede remount at %d (carcass-clear-before-remount invariant)", m.Dir, ui, ri)
		}
	}
}

// TestRetireLingeringSocketReaps: the socket lingers past WaitGone and the pid was
// captured (CapturedPIDErr==nil), so KillPeer(CapturedPID) fires and a second
// WaitGone runs. A wedged raw holder never frees its socket.
func TestRetireLingeringSocketReaps(t *testing.T) {
	h := newClosableHolder(t, func(req string) string {
		if strings.Contains(req, `"op":"shutdown"`) {
			return shutdownOKReply
		}
		return shutdownOKReply
	})
	// Holder stays up (never closed) → WaitGone always reports lingering.
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
		ForceUnmount: func(string) {},
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

// TestRetireCapturedPIDErrDisablesReap: the socket lingers but the pid capture
// failed (CapturedPIDErr!=nil), so the reap is DISABLED — KillPeer must never be
// invoked (no identity to gate on; never a name kill).
func TestRetireCapturedPIDErrDisablesReap(t *testing.T) {
	h := newClosableHolder(t, func(string) string { return shutdownOKReply })
	// Holder stays up → socket lingers, but the capture error must suppress any
	// peer resolve/kill entirely.
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
		ForceUnmount:   func(string) {},
		Spawn:          func() error { return nil },
		Remount:        func() error { return nil },
	})
	if err != nil {
		t.Fatalf("Retire with a disabled reap = %v, want nil", err)
	}
}

// TestRetireShutdownUnavailableTolerated: an ErrHolderUnavailable Shutdown (the
// holder already gone) is NOT an error on its own — Retire proceeds to spawn +
// remount. The unreachable socket also makes the first WaitGone report gone, so
// no reap.
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
		ForceUnmount: func(string) {},
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

// TestRetireShutdownErrorOtherThanUnavailable: a Shutdown error that is NOT
// ErrHolderUnavailable (a real wire-class failure) returns wrapped "retire
// holder:" and never proceeds to spawn.
func TestRetireShutdownErrorOtherThanUnavailable(t *testing.T) {
	// The holder acks shutdown with a wedged error class → respErr maps it to
	// ErrUnmountWedged, which is NOT ErrHolderUnavailable.
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
		ForceUnmount: func(string) {},
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

// equalStrs reports slice equality for ordered string assertions.
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
