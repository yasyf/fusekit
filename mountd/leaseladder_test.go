package mountd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/lease"
)

// swapClearCarcass records the pre-mount carcass-clear calls for one test and
// runs fn in place of the real fusekit.ClearCarcass.
func swapClearCarcass(t *testing.T, fn func(dir string) error) *[]string {
	t.Helper()
	cleared := &[]string{}
	prev := clearCarcass
	clearCarcass = func(dir string) error {
		*cleared = append(*cleared, dir)
		return fn(dir)
	}
	t.Cleanup(func() { clearCarcass = prev })
	return cleared
}

// TestUnmountLeaseLadder pins OpUnmount's ladder: a held session lease
// answers retryable ClassBusy with the acquirer's provenance and never
// reaches the provider; a free lease is seized across a graceful teardown.
func TestUnmountLeaseLadder(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{}
	s := newHandlerServer(t, fake)
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	h, err := lease.Acquire(s.LeaseDir, dir, "cc-pool")
	if err != nil {
		t.Fatal(err)
	}

	resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassBusy {
		t.Fatalf("unmount under a held lease = (ok=%v class=%q), want retryable busy", resp.OK, resp.ErrClass)
	}
	for _, want := range []string{"cc-pool", dir} {
		if !strings.Contains(resp.Error, want) {
			t.Errorf("busy error %q does not surface the lease provenance %q", resp.Error, want)
		}
	}
	if _, tears := fake.calls(); len(tears) != 0 {
		t.Fatalf("lease-held unmount tore down %v", tears)
	}
	if _, ok := s.registered(dir); !ok {
		t.Fatal("lease-held unmount dropped the registry row")
	}

	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("unmount after lease release: %s", resp.Error)
	}
	if _, tears := fake.calls(); !reflect.DeepEqual(tears, []hostCall{{base, dir}}) {
		t.Fatalf("teardowns = %v, want one graceful teardown", tears)
	}
	// The fence released and GC'd: the lease is seizable again.
	f, err := lease.Seize(s.LeaseDir, dir)
	if err != nil {
		t.Fatalf("lease not released after unmount: %v", err)
	}
	f.Release()
}

// TestUnmountMuxTenantSeizesRootLease pins the mux half of the ladder: a mux
// tenant's teardown seizes the ROOT lease too (mux-root busy = root lease
// held or any subtree's lease held), so a session pinned to the shared root
// defers the detach.
func TestUnmountMuxTenantSeizesRootLease(t *testing.T) {
	const base, root, dir = "/pool/base", "/pool/mnt", "/pool/mnt/acct-01"
	fake := &fakeHost{}
	s := newHandlerServer(t, fake)
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, MuxRoot: root, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	h, err := lease.Acquire(s.LeaseDir, root, "cc-pool")
	if err != nil {
		t.Fatal(err)
	}

	resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassBusy {
		t.Fatalf("detach under a held ROOT lease = (ok=%v class=%q), want busy", resp.OK, resp.ErrClass)
	}
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("detach after root lease release: %s", resp.Error)
	}
}

// TestRemountDeadMirrorLeaseLadder pins the remount-of-dead-mirror arm: a
// dead registered mirror under a live session lease defers with provenance;
// once the lease frees, the corpse comes down gracefully under the fence and
// the remount proceeds.
func TestRemountDeadMirrorLeaseLadder(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	fake := &fakeHost{}
	s := newHandlerServer(t, fake)
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}
	fake.setLive(dir, false) // the mirror died while the holder lived

	h, err := lease.Acquire(s.LeaseDir, dir, "cc-pool")
	if err != nil {
		t.Fatal(err)
	}
	resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassBusy {
		t.Fatalf("dead-mirror remount under a held lease = (ok=%v class=%q), want busy", resp.OK, resp.ErrClass)
	}
	if !strings.Contains(resp.Error, "cc-pool") {
		t.Errorf("busy error %q lacks lease provenance", resp.Error)
	}
	if _, tears := fake.calls(); len(tears) != 0 {
		t.Fatalf("lease-held remount tore down %v", tears)
	}

	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("remount after lease release: %s", resp.Error)
	}
	setups, tears := fake.calls()
	if !reflect.DeepEqual(tears, []hostCall{{base, dir}}) {
		t.Fatalf("teardowns = %v, want the dead mirror torn down once", tears)
	}
	if !reflect.DeepEqual(setups, []hostCall{{base, dir}, {base, dir}}) {
		t.Fatalf("setups = %v, want the original mount plus the remount", setups)
	}
}

// TestPreMountCarcassClearLadder pins the pre-mount force site: a rowless
// mountpoint is cleared via ClearCarcass UNDER the seized lease fence (a
// concurrent Seize inside the clear must read busy), a held lease defers the
// clear entirely, and a clear that leaves a healthy mountpoint reads as a
// live foreign mount.
func TestPreMountCarcassClearLadder(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"

	t.Run("proven-dead carcass cleared under the fence, then mounted", func(t *testing.T) {
		fake := &fakeHost{}
		s := newHandlerServer(t, fake)
		mounted := true
		setState(fake, func(string) bool { return mounted }, func(string, string) bool { return false })
		cleared := swapClearCarcass(t, func(string) error {
			// The fence must be held ACROSS the clear: seizing here must bounce.
			if _, err := lease.Seize(s.LeaseDir, dir); !errors.Is(err, lease.ErrBusy) {
				t.Errorf("Seize during the carcass clear = %v, want ErrBusy (fence not held)", err)
			}
			mounted = false // the force cleared the kernel mountpoint
			return nil
		})

		resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"})
		if !resp.OK {
			t.Fatalf("mount over a proven-dead carcass = %s, want OK", resp.Error)
		}
		if !reflect.DeepEqual(*cleared, []string{dir}) {
			t.Fatalf("carcass clears = %v, want exactly [%s]", *cleared, dir)
		}
		if setups, _ := fake.calls(); !reflect.DeepEqual(setups, []hostCall{{base, dir}}) {
			t.Fatalf("setups = %v, want the fresh mount", setups)
		}
	})

	t.Run("held lease defers the clear with provenance", func(t *testing.T) {
		fake := &fakeHost{}
		s := newHandlerServer(t, fake)
		setState(fake, func(string) bool { return true }, func(string, string) bool { return false })
		cleared := swapClearCarcass(t, func(string) error { return nil })
		h, err := lease.Acquire(s.LeaseDir, dir, "cc-notes")
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()

		resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"})
		if resp.OK || resp.ErrClass != ClassBusy {
			t.Fatalf("carcass clear under a held lease = (ok=%v class=%q), want busy", resp.OK, resp.ErrClass)
		}
		if !strings.Contains(resp.Error, "cc-notes") {
			t.Errorf("busy error %q lacks the holder's provenance", resp.Error)
		}
		if len(*cleared) != 0 {
			t.Fatalf("a held lease still cleared %v", *cleared)
		}
	})

	t.Run("undetermined verdict defers, never forces", func(t *testing.T) {
		fake := &fakeHost{}
		s := newHandlerServer(t, fake)
		setState(fake, func(string) bool { return true }, func(string, string) bool { return false })
		swapClearCarcass(t, func(dir string) error {
			return fusekit.ErrCarcassUndetermined
		})
		resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"})
		if resp.OK || resp.ErrClass != ClassWedged {
			t.Fatalf("undetermined carcass = (ok=%v class=%q), want deferred wedged", resp.OK, resp.ErrClass)
		}
		if setups, _ := fake.calls(); len(setups) != 0 {
			t.Fatalf("undetermined carcass still mounted: %v", setups)
		}
	})

	t.Run("healthy mountpoint after a no-op clear is a live foreign mount", func(t *testing.T) {
		fake := &fakeHost{}
		s := newHandlerServer(t, fake)
		setState(fake, func(string) bool { return true }, func(string, string) bool { return false })
		swapClearCarcass(t, func(string) error { return nil }) // healthy: ClearCarcass no-ops
		resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"})
		if resp.OK || resp.ErrClass != ClassForeignMount {
			t.Fatalf("live foreign mountpoint = (ok=%v class=%q), want foreign-mount", resp.OK, resp.ErrClass)
		}
	})
}

// TestMountHungStatNeverReachesTheClear is the server-side no-force-on-hang
// pin: a rowless dir whose stat does not answer is not proven dead — the
// handler defers loudly BEFORE the seize and the clear.
func TestMountHungStatNeverReachesTheClear(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	shrinkLiveProbeTimeout(t, 50*time.Millisecond)
	fake := &fakeHost{}
	s := newHandlerServer(t, fake)
	block := make(chan struct{})
	setState(fake, func(string) bool { <-block; return true }, func(string, string) bool { return false })
	t.Cleanup(func() { releaseProbes(t, block) })
	cleared := swapClearCarcass(t, func(string) error { return nil })

	resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "cc-pool"})
	if resp.OK || resp.ErrClass != ClassWedged {
		t.Fatalf("hung-stat mount = (ok=%v class=%q %q), want deferred wedged", resp.OK, resp.ErrClass, resp.Error)
	}
	if !strings.Contains(resp.Error, "not proven dead") {
		t.Errorf("deferral %q does not say the dir is unproven", resp.Error)
	}
	if len(*cleared) != 0 {
		t.Fatalf("a hanging stat reached the carcass clear: %v", *cleared)
	}
}

// TestIdempotentMountRewritesJournalRow pins the named deliverable: an
// idempotent OK on a live pair REWRITES the journal row when ANY spec field
// differs — before returning — so a successor never replays a stale spec.
func TestIdempotentMountRewritesJournalRow(t *testing.T) {
	fake := &fakeHost{}
	s, path := newJournaledHandlerServer(t, fake)
	spec := fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", ContentSocket: "/s/one.sock", ContentMode: "source", PrivateRoot: "/p"}
	if resp := s.dispatch(mountEntryOf(spec).mountRequest()); !resp.OK {
		t.Fatalf("mount: %s", resp.Error)
	}

	// Same spec: idempotent OK, journal byte-stable.
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if resp := s.dispatch(mountEntryOf(spec).mountRequest()); !resp.OK {
		t.Fatalf("idempotent mount: %s", resp.Error)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("unchanged spec rewrote the journal:\n%s\nvs\n%s", before, after)
	}

	// Changed spec: still idempotent OK (no re-Setup), but the row rewrites.
	changed := spec
	changed.ContentSocket = "/s/two.sock"
	changed.AttrCache = true
	changed.AttrCacheTimeout = 2 * time.Second
	changed.PrivatePrefixes = []string{".credentials.json"}
	if resp := s.dispatch(mountEntryOf(changed).mountRequest()); !resp.OK {
		t.Fatalf("idempotent mount with a changed spec: %s", resp.Error)
	}
	if setups, _ := fake.calls(); len(setups) != 1 {
		t.Fatalf("Setup calls = %d, want 1 (the live pair must not re-Setup)", len(setups))
	}
	f := readJournalFile(t, path)
	if want := []mountEntry{mountEntryOf(changed)}; !reflect.DeepEqual(f.Mounts, want) {
		t.Fatalf("journal after changed-spec idempotent mount = %+v, want %+v", f.Mounts, want)
	}
}

// TestOpenJournalIgnoresLegacyPolicyFields pins journal v2's decode contract:
// a legacy journal carrying idle_policy/carcass_policy decodes via Go's
// default unknown-field ignoring — no shim, no migration, no error.
func TestOpenJournalIgnoresLegacyPolicyFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "holder-specs.json")
	legacy := `{
  "mounts": [
    {
      "base": "/b/a",
      "dir": "/m/a",
      "owner": "cc-pool",
      "idle_policy": "attest",
      "carcass_policy": "defer"
    }
  ],
  "bridges": [
    {
      "owner": "cc-pool",
      "bridge_socket": "/grp/b.sock",
      "content_socket": "/up/c.sock"
    }
  ]
}`
	if err := os.WriteFile(path, []byte(legacy), 0o600); err != nil {
		t.Fatal(err)
	}
	j, err := openJournal(path)
	if err != nil {
		t.Fatalf("openJournal(legacy) = %v, want a clean decode", err)
	}
	mounts, bridges := j.snapshot()
	wantMounts := []mountEntry{{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}}
	if !reflect.DeepEqual(mounts, wantMounts) {
		t.Fatalf("legacy mounts = %+v, want %+v (policy fields ignored)", mounts, wantMounts)
	}
	if want := []bridgeEntry{{Owner: "cc-pool", BridgeSocket: "/grp/b.sock", ContentSocket: "/up/c.sock"}}; !reflect.DeepEqual(bridges, want) {
		t.Fatalf("legacy bridges = %+v, want %+v", bridges, want)
	}
}

// TestReplayDefersLeaseHeldRootAndKeepsEntries pins the replay carcass
// clear's ladder: a journaled root whose lease is held is neither cleared nor
// replayed this generation, and its entries survive for the next one.
func TestReplayDefersLeaseHeldRootAndKeepsEntries(t *testing.T) {
	fake := &fakeHost{}
	s, path := newJournaledHandlerServer(t, fake)
	capture := captureReapSeams(t)
	for _, dir := range []string{"/m/held", "/m/free"} {
		if err := s.journal.putMount(fusekit.MountSpec{Base: "/b" + dir, Dir: dir, Owner: "cc-pool"}); err != nil {
			t.Fatal(err)
		}
	}
	h, err := lease.Acquire(s.LeaseDir, "/m/held", "cc-pool")
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	s.replayJournal(context.Background())

	cleared, _ := capture.snapshot()
	if !reflect.DeepEqual(cleared, []string{"/m/free"}) {
		t.Fatalf("carcass clears = %v, want only the free root", cleared)
	}
	setups, _ := fake.calls()
	if !reflect.DeepEqual(setups, []hostCall{{"/b/m/free", "/m/free"}}) {
		t.Fatalf("replayed setups = %v, want only the free mount", setups)
	}
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/free", "/m/held"}) {
		t.Fatalf("journal after deferred replay = %v, want both entries kept", dirs)
	}
}

// TestWireRefusesProtoOne pins the server-side skew refusal: a proto-1
// request over the wire answers ClassProtoMismatch with a message naming the
// fix, and never dispatches.
func TestWireRefusesProtoOne(t *testing.T) {
	fake := &fakeHost{}
	_, cl, _, _ := startServer(t, fake)

	conn, err := net.Dial("unix", cl.Socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := json.NewEncoder(conn).Encode(map[string]any{"proto": 1, "op": "mount", "base": "/b", "dir": "/d", "owner": "cc-pool"}); err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.NewDecoder(bufio.NewReader(conn)).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK || resp.ErrClass != ClassProtoMismatch {
		t.Fatalf("proto-1 request = (ok=%v class=%q), want %q", resp.OK, resp.ErrClass, ClassProtoMismatch)
	}
	for _, want := range []string{"proto 2", "proto 1", "brew upgrade --cask fusekit-holder", "upgrade the consumer"} {
		if !strings.Contains(resp.Error, want) {
			t.Errorf("refusal %q does not name %q", resp.Error, want)
		}
	}
	if setups, _ := fake.calls(); len(setups) != 0 {
		t.Fatalf("a proto-1 request dispatched: %v", setups)
	}
}
