package mountd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yasyf/fusekit/content"
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

func TestIsUnknownOp(t *testing.T) {
	// The frozen dispatch text an old holder returns for a new op.
	resp := newHandlerServer(&fakeHost{}).dispatch(Request{Op: "bogusop"})
	if resp.OK {
		t.Fatal("bogus op dispatched OK")
	}
	if !IsUnknownOp(errors.New(resp.Error)) {
		t.Fatalf("IsUnknownOp did not match the dispatch reply %q", resp.Error)
	}
	if IsUnknownOp(errors.New("some other failure")) {
		t.Error("IsUnknownOp matched an unrelated error")
	}
	if IsUnknownOp(nil) {
		t.Error("IsUnknownOp(nil) = true")
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
	s := newHandlerServer(&fakeHost{})

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

func TestAddBridgeForeignOwnerRefused(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(&fakeHost{})

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
	s := newHandlerServer(&fakeHost{})

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
	s := newHandlerServer(&fakeHost{})

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
	s := newHandlerServer(&fakeHost{})

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
	all := byOwner("")
	if len(all) != 2 || all[0].Owner != "a" || all[1].Owner != "b" {
		t.Fatalf("bridges(all) = %+v, want [a b] sorted", all)
	}
}

func TestShutdownOwnerAccountingCountsBridges(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)

	// A mount owner and a distinct bridge owner: two owners → refused.
	mixed := newHandlerServer(&fakeHost{})
	mixed.dispatch(Request{Op: OpMount, Base: "/b", Dir: "/d", Owner: "mounter"})
	addBridge(t, mixed, "bridger", "/grp/b.sock", "/up/a.sock", nil)
	if resp := mixed.dispatch(Request{Op: OpShutdown}); resp.OK {
		t.Error("shutdown across a mount owner and a bridge owner = OK, want refused")
	}

	// A bridge-only owner is a single owner → allowed.
	solo := newHandlerServer(&fakeHost{})
	solo.triggerShutdown = func() {}
	addBridge(t, solo, "solo", "/grp/s.sock", "/up/s.sock", nil)
	if resp := solo.dispatch(Request{Op: OpShutdown}); !resp.OK {
		t.Errorf("bridge-only shutdown = %s, want OK", resp.Error)
	}

	// The same owner across a mount and a bridge is still one owner → allowed.
	same := newHandlerServer(&fakeHost{})
	same.triggerShutdown = func() {}
	same.dispatch(Request{Op: OpMount, Base: "/b", Dir: "/d", Owner: "x"})
	addBridge(t, same, "x", "/grp/x.sock", "/up/x.sock", nil)
	if resp := same.dispatch(Request{Op: OpShutdown}); !resp.OK {
		t.Errorf("same-owner mount+bridge shutdown = %s, want OK", resp.Error)
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
	s := newHandlerServer(&fakeHost{})
	go s.runBridge(row)

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
