package mountd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/fusekit/content"
	"strings"
)

// These tests pin the additive bridge wire surface byte-for-byte (proto-1 is
// frozen; a failing golden is a protocol break) and exercise the bridge registry
// and runner.

func TestWireFreezeBridgeRequest(t *testing.T) {
	in := Request{
		Proto:           1,
		Op:              OpAddBridge,
		Owner:           "cc-pool",
		ContentSocket:   "/h/.cc-pool/bridge.sock",
		PrivatePrefixes: []string{"secret"},
		BridgeSocket:    "/grp/b.sock",
	}
	want := `{"proto":1,"op":"addbridge","owner":"cc-pool","content_socket":"/h/.cc-pool/bridge.sock","private_prefixes":["secret"],"bridge_socket":"/grp/b.sock"}`
	got, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("marshal = %s\nwant      %s\n(frozen proto-1 wire artifact)", got, want)
	}
	var back Request
	if err := json.Unmarshal([]byte(want), &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, in) {
		t.Fatalf("round-trip = %+v, want %+v", back, in)
	}
}

func TestWireFreezeBridgesResponse(t *testing.T) {
	tests := []struct {
		name string
		in   Response
		want string
	}{
		{
			name: "every bridge field set",
			in: Response{Proto: 1, OK: true, Bridges: []BridgeInfo{{
				Owner: "cc-pool", Socket: "/grp/b.sock", State: "serving",
				PendingWrites: 2, Upstream: "/h/.cc-pool/bridge.sock", LastErr: "boom",
			}}},
			want: `{"proto":1,"ok":true,"bridges":[{"owner":"cc-pool","socket":"/grp/b.sock","state":"serving","pending_writes":2,"upstream":"/h/.cc-pool/bridge.sock","last_err":"boom"}]}`,
		},
		{
			name: "BridgeInfo always carries owner, socket, state",
			in:   Response{Proto: 1, OK: true, Bridges: []BridgeInfo{{Owner: "o", Socket: "/s", State: "starting"}}},
			want: `{"proto":1,"ok":true,"bridges":[{"owner":"o","socket":"/s","state":"starting"}]}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tc.want {
				t.Fatalf("marshal = %s\nwant      %s\n(frozen proto-1 wire artifact)", got, tc.want)
			}
			var back Response
			if err := json.Unmarshal([]byte(tc.want), &back); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(back, tc.in) {
				t.Fatalf("round-trip = %+v, want %+v", back, tc.in)
			}
		})
	}
}

func TestWireFreezeEmptyBridgesOmitted(t *testing.T) {
	got, err := json.Marshal(Response{Proto: 1, OK: true, Bridges: []BridgeInfo{}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"proto":1,"ok":true}`; string(got) != want {
		t.Fatalf("marshal = %s, want %s", got, want)
	}
}

func TestUnknownOpReply(t *testing.T) {
	resp := newHandlerServer(t, &fakeHost{}).dispatch(Request{Op: "bogusop", Owner: "cc-pool"})
	if resp.OK {
		t.Fatal("bogus op dispatched OK")
	}
	if !strings.Contains(resp.Error, unknownOpPrefix) {
		t.Fatalf("unknown-op reply %q lacks the frozen prefix %q", resp.Error, unknownOpPrefix)
	}
}

// stubStartBridge replaces the runner spawn with a no-op that closes done, so
// registry-shape tests drive the bridges map without binding real sockets.
func stubStartBridge(t *testing.T) {
	t.Helper()
	prev := startBridge
	startBridge = func(_ *Server, row *bridgeRow) { close(row.done) }
	t.Cleanup(func() { startBridge = prev })
}

// redirectSpool points the durable spool off the real ~/.fusekit for the test.
func redirectSpool(t *testing.T) {
	t.Helper()
	prev := holderSpoolRoot
	holderSpoolRoot = filepath.Join(shortSockDir(t), "spool")
	t.Cleanup(func() { holderSpoolRoot = prev })
}

func addBridge(t *testing.T, s *Server, owner, bindSock, upstream string, prefixes []string) Response {
	t.Helper()
	return s.dispatch(Request{Op: OpAddBridge, Owner: owner, BridgeSocket: bindSock, ContentSocket: upstream, PrivatePrefixes: prefixes})
}

func TestAddBridgeAdoptKeepsCacheAndSpoolWarm(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})

	resp := addBridge(t, s, "cc-pool", "/grp/b.sock", "/up/a.sock", []string{"secret"})
	if !resp.OK {
		t.Fatalf("add: %s", resp.Error)
	}
	if len(resp.Bridges) != 1 || resp.Bridges[0].Owner != "cc-pool" || resp.Bridges[0].Socket != "/grp/b.sock" || resp.Bridges[0].Upstream != "/up/a.sock" {
		t.Fatalf("add listing = %+v", resp.Bridges)
	}

	relay := s.bridges["cc-pool"].relay
	if err := relay.WriteThrough("d", "a", []byte("v1")); err != nil { // upstream down → spooled
		t.Fatal(err)
	}
	if n := relay.PendingWrites(); n != 1 {
		t.Fatalf("seeded PendingWrites = %d, want 1", n)
	}

	resp2 := addBridge(t, s, "cc-pool", "/grp/b.sock", "/up/b.sock", []string{"secret", "other"})
	if !resp2.OK {
		t.Fatalf("adopt: %s", resp2.Error)
	}
	if s.bridges["cc-pool"].relay != relay {
		t.Fatal("adopt replaced the relay; cache and spool would be lost")
	}
	if got := s.bridges["cc-pool"].upstream; got != "/up/b.sock" {
		t.Fatalf("adopt upstream = %q, want /up/b.sock", got)
	}
	if n := relay.PendingWrites(); n != 1 {
		t.Fatalf("adopt lost the spool: PendingWrites = %d, want 1", n)
	}
	if resp2.Bridges[0].Upstream != "/up/b.sock" {
		t.Fatalf("adopt listing upstream = %q", resp2.Bridges[0].Upstream)
	}
}

func TestAddBridgeRejectsHostileOwner(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})

	// A path-traversal owner must never be accepted: "x/../victim" cleans to a
	// different tenant's spool dir but keys a distinct bridges entry, so the
	// foreign-owner refusal would miss it and the relay would load victim's spool.
	if resp := addBridge(t, s, "victim", "/grp/v.sock", "/up/v.sock", nil); !resp.OK {
		t.Fatalf("victim add: %s", resp.Error)
	}
	if resp := s.dispatch(Request{Op: OpAddBridge, Owner: "x/../victim", BridgeSocket: "/grp/att.sock", ContentSocket: "/up/att.sock"}); resp.OK || resp.ErrClass != ClassInvalidOwner {
		t.Fatalf("x/../victim adopt-collision = (ok=%v class=%q), want invalid-owner refusal", resp.OK, resp.ErrClass)
	}

	for _, owner := range []string{"", ".", "..", "a/b", "a/", "/abs", "a\x00b", "x/../victim", "./x"} {
		resp := s.dispatch(Request{Op: OpAddBridge, Owner: owner, BridgeSocket: "/grp/b.sock", ContentSocket: "/up/a.sock"})
		if resp.OK || resp.ErrClass != ClassInvalidOwner {
			t.Errorf("hostile owner %q = (ok=%v class=%q), want invalid-owner", owner, resp.OK, resp.ErrClass)
		}
		if _, ok := s.bridges[owner]; ok {
			t.Errorf("hostile owner %q left a registry row", owner)
		}
	}
	// removebridge validates the owner too.
	if resp := s.dispatch(Request{Op: OpRemoveBridge, Owner: "a/../b"}); resp.OK || resp.ErrClass != ClassInvalidOwner {
		t.Errorf("removebridge hostile owner = (ok=%v class=%q), want invalid-owner", resp.OK, resp.ErrClass)
	}
}

func TestAddBridgeRejectsRelativeSocket(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})
	if resp := s.dispatch(Request{Op: OpAddBridge, Owner: "o", BridgeSocket: "rel/b.sock", ContentSocket: "/up/a.sock"}); resp.OK {
		t.Error("relative bridge_socket accepted")
	}
}

func TestAddBridgeSameOwnerSocketChangeRefused(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})

	if resp := addBridge(t, s, "o", "/grp/b.sock", "/up/a.sock", nil); !resp.OK {
		t.Fatalf("add: %s", resp.Error)
	}
	// Same owner + same socket → adopt (upstream re-pointed).
	if resp := addBridge(t, s, "o", "/grp/b.sock", "/up/b.sock", nil); !resp.OK {
		t.Fatalf("same-socket adopt = %s, want OK", resp.Error)
	}
	// Same owner + different socket → refused non-retryably; the live bind stays.
	resp := addBridge(t, s, "o", "/grp/OTHER.sock", "/up/c.sock", nil)
	if resp.OK || resp.ErrClass != ClassBridgeSocketChanged {
		t.Fatalf("socket-change add = (ok=%v class=%q), want bridge-socket-changed", resp.OK, resp.ErrClass)
	}
	if got := s.bridges["o"].bindSock; got != "/grp/b.sock" {
		t.Fatalf("bind socket changed to %q despite refusal", got)
	}
}

func TestAddBridgeRefusesWhileStopping(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})
	addBridge(t, s, "o", "/grp/b.sock", "/up/a.sock", nil)

	// Simulate a reclaim in progress: the row stays but is marked stopping.
	s.bridgeMu.Lock()
	s.bridges["o"].stopping = true
	s.bridgeMu.Unlock()

	resp := addBridge(t, s, "o", "/grp/b.sock", "/up/a.sock", nil)
	if resp.OK || resp.ErrClass != ClassBusy {
		t.Fatalf("add while stopping = (ok=%v class=%q), want busy (no second relay on the spool dir)", resp.OK, resp.ErrClass)
	}
}

func TestBridgeInfosConcurrentWithAdopt(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})
	addBridge(t, s, "o", "/grp/b.sock", "/up/a.sock", nil)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 300; i++ {
			s.dispatch(Request{Op: OpAddBridge, Owner: "o", BridgeSocket: "/grp/b.sock", ContentSocket: fmt.Sprintf("/up/%d.sock", i)})
		}
		close(done)
	}()
	for i := 0; i < 300; i++ {
		_ = s.bridgeInfos("")
	}
	<-done
}

func TestAddBridgeForeignOwnerRefused(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})

	if resp := addBridge(t, s, "a", "/grp/b.sock", "/up/a.sock", nil); !resp.OK {
		t.Fatalf("add a: %s", resp.Error)
	}
	resp := addBridge(t, s, "b", "/grp/b.sock", "/up/b.sock", nil)
	if resp.OK || resp.ErrClass != ClassForeignBridge {
		t.Fatalf("foreign add = (ok=%v, class=%q), want foreign-bridge", resp.OK, resp.ErrClass)
	}
	if _, ok := s.bridges["b"]; ok {
		t.Error("refused foreign bridge left a registry row")
	}
}

func TestReclaimStopsBridge(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})

	if resp := addBridge(t, s, "o", "/grp/b.sock", "/up/a.sock", nil); !resp.OK {
		t.Fatalf("add: %s", resp.Error)
	}
	if resp := s.dispatch(Request{Op: OpReclaim, Owner: "o"}); !resp.OK {
		t.Fatalf("reclaim: %s", resp.Error)
	}
	if _, ok := s.bridges["o"]; ok {
		t.Fatal("bridge survived reclaim")
	}
	if got := s.dispatch(Request{Op: OpBridges}); len(got.Bridges) != 0 {
		t.Fatalf("bridges after reclaim = %+v, want none", got.Bridges)
	}
}

func TestRemoveBridge(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})

	addBridge(t, s, "o", "/grp/b.sock", "/up/a.sock", nil)
	resp := s.dispatch(Request{Op: OpRemoveBridge, Owner: "o"})
	if !resp.OK || len(resp.Bridges) != 0 {
		t.Fatalf("remove = (ok=%v, bridges=%+v), want ok + empty", resp.OK, resp.Bridges)
	}
	if _, ok := s.bridges["o"]; ok {
		t.Fatal("bridge survived removebridge")
	}
	if resp := s.dispatch(Request{Op: OpRemoveBridge, Owner: ""}); resp.OK {
		t.Error("removebridge with no owner = OK, want refused")
	}
}

func TestBridgesListScopingAndPending(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})

	addBridge(t, s, "a", "/grp/a.sock", "/up/a.sock", nil)
	addBridge(t, s, "b", "/grp/b.sock", "/up/b.sock", nil)
	if err := s.bridges["a"].relay.WriteThrough("d", "x", []byte("v")); err != nil {
		t.Fatal(err)
	}

	byOwner := func(owner string) []BridgeInfo { return s.dispatch(Request{Op: OpBridges, Owner: owner}).Bridges }
	if got := byOwner("a"); len(got) != 1 || got[0].Owner != "a" || got[0].PendingWrites != 1 {
		t.Fatalf("bridges(a) = %+v, want one a with pending 1", got)
	}
	if got := byOwner("b"); len(got) != 1 || got[0].Owner != "b" || got[0].PendingWrites != 0 {
		t.Fatalf("bridges(b) = %+v, want one b with pending 0", got)
	}
	// Owner is required; an ownerless bridges query is refused.
	if resp := s.dispatch(Request{Op: OpBridges}); resp.OK || resp.ErrClass != ClassInvalidOwner {
		t.Fatalf("ownerless bridges = (ok=%v class=%q), want invalid-owner", resp.OK, resp.ErrClass)
	}
	all := s.dispatch(Request{Op: OpBridges, Owner: "doctor", All: true}).Bridges
	if len(all) != 2 || all[0].Owner != "a" || all[1].Owner != "b" {
		t.Fatalf("bridges(all) = %+v, want [a b] sorted", all)
	}
}

func TestRunBridgeConsentPendingOnPermissionBind(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions; cannot force os.ErrPermission")
	}
	prevBackoff := bridgeBindBackoff
	bridgeBindBackoff = 5 * time.Millisecond
	t.Cleanup(func() { bridgeBindBackoff = prevBackoff })

	// A read-only parent makes the in-loop MkdirAll of the socket dir fail with
	// os.ErrPermission — the group-container consent signature.
	ro := shortSockDir(t)
	if err := os.Chmod(ro, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(ro, 0o700) }) // LIFO: restore before shortSockDir's RemoveAll
	bindSock := filepath.Join(ro, "sub", "b.sock")

	relay, err := content.NewRelaySource(content.RelayConfig{
		Owner:    "o",
		SpoolDir: filepath.Join(shortSockDir(t), "spool"),
		Upstream: filepath.Join(shortSockDir(t), "dead.sock"),
	})
	if err != nil {
		t.Fatal(err)
	}
	discard := log.New(io.Discard, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	row := &bridgeRow{
		owner:    "o",
		bindSock: bindSock,
		relay:    relay,
		server:   &content.BridgeServer{Socket: bindSock, Source: relay, Version: "t", Log: discard},
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
		state:    bridgeStarting,
	}
	s := newHandlerServer(t, &fakeHost{})
	// Drives the runner alone (startBridge's joiner owns done in production).
	go func() { s.runBridge(row); close(row.done) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if st, _ := row.snapshotState(); st == bridgeConsentPending {
			break
		}
		if time.Now().After(deadline) {
			st, _ := row.snapshotState()
			t.Fatalf("bridge never reached consent-pending; state = %q", st)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-row.done:
	case <-time.After(2 * time.Second):
		t.Fatal("runBridge did not exit on cancel")
	}
}

// TestRemoveBridgeParksUntilRunnersExit pins R3's one-live-relay ordering: a
// stop that outlives its grace does NOT delete the row — it stays (stopping)
// so a same-owner re-add keeps refusing ClassBusy, and only once the runner
// AND replay goroutines have exited (row.done) is the row removed and the
// removal journaled. A replacement bridge can never share the spool with a
// still-live predecessor.
func TestRemoveBridgeParksUntilRunnersExit(t *testing.T) {
	redirectSpool(t)
	// Live-runner sim: startBridge leaves done OPEN, as if the runner and
	// replay were still working the spool.
	prev := startBridge
	startBridge = func(*Server, *bridgeRow) {}
	t.Cleanup(func() { startBridge = prev })
	prevGrace := bridgeStopGrace
	bridgeStopGrace = 30 * time.Millisecond
	t.Cleanup(func() { bridgeStopGrace = prevGrace })

	fake := &fakeHost{}
	s, _ := newJournaledHandlerServer(t, fake)
	bindSock := filepath.Join(shortSockDir(t), "b.sock")
	if resp := addBridge(t, s, "own", bindSock, "/up.sock", nil); !resp.OK {
		t.Fatalf("addbridge: %s", resp.Error)
	}
	s.bridgeMu.Lock()
	row := s.bridges["own"]
	s.bridgeMu.Unlock()

	if resp := s.dispatch(Request{Op: OpRemoveBridge, Owner: "own"}); !resp.OK {
		t.Fatalf("removebridge: %s", resp.Error)
	}
	// Parked, not deleted: the row survives, marked stopping.
	s.bridgeMu.Lock()
	cur, ok := s.bridges["own"]
	stopping := ok && cur.stopping
	s.bridgeMu.Unlock()
	if !ok || cur != row || !stopping {
		t.Fatalf("row after timed-out stop = (ok=%v same=%v stopping=%v), want parked in place", ok, cur == row, stopping)
	}
	if _, jbridges := s.journal.counts(); jbridges != 1 {
		t.Fatal("journal row dropped before the runners exited")
	}
	// A same-owner re-add refuses while the old relay may still touch the spool.
	if resp := addBridge(t, s, "own", bindSock, "/up.sock", nil); resp.OK || resp.ErrClass != ClassBusy {
		t.Fatalf("re-add during parked stop = (ok=%v class=%q), want busy", resp.OK, resp.ErrClass)
	}

	// Both goroutines exit; the parked removal completes.
	close(row.done)
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.bridgeMu.Lock()
		_, still := s.bridges["own"]
		s.bridgeMu.Unlock()
		if !still {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("row never removed after the runners exited")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, jbridges := s.journal.counts(); jbridges != 0 {
		t.Fatal("journal row survived the completed removal")
	}
	// The owner is free again.
	if resp := addBridge(t, s, "own", bindSock, "/up.sock", nil); !resp.OK {
		t.Fatalf("re-add after completed removal = %s, want OK", resp.Error)
	}
}
