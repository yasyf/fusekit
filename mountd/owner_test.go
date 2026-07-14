package mountd

import (
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/fusekit"
	"github.com/yasyf/fusekit/lease"
)

func TestOwnerScopedListAndReclaim(t *testing.T) {
	s := newHandlerServer(t, &fakeHost{})
	mount := func(owner, base, dir string) {
		if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: owner}); !resp.OK {
			t.Fatalf("mount %s %s: %s", owner, dir, resp.Error)
		}
	}
	mount("cc-pool", "/p/base", "/p/a")
	mount("cc-pool", "/p/base", "/p/b")
	mount("cc-notes", "/n/base", "/n/x")

	list := func(owner string) []MountInfo { return s.dispatch(Request{Op: OpList, Owner: owner}).Mounts }
	if got := list("cc-pool"); len(got) != 2 {
		t.Errorf("list(cc-pool) = %d mounts, want 2", len(got))
	} else {
		for _, m := range got {
			if m.Owner != "cc-pool" {
				t.Errorf("list(cc-pool) leaked %s mount %s", m.Owner, m.Dir)
			}
		}
	}
	if got := list("cc-notes"); len(got) != 1 || got[0].Dir != "/n/x" {
		t.Errorf("list(cc-notes) = %v, want [/n/x]", got)
	}
	// Owner is required: an ownerless list is refused, never a cross-tenant view.
	if resp := s.dispatch(Request{Op: OpList}); resp.OK || resp.ErrClass != ClassInvalidOwner {
		t.Errorf("ownerless list = (ok=%v class=%q), want invalid-owner", resp.OK, resp.ErrClass)
	}
	// All: the read-only cross-tenant view (doctor).
	if got := s.dispatch(Request{Op: OpList, Owner: "doctor", All: true}).Mounts; len(got) != 3 {
		t.Errorf("list(all) = %d mounts, want 3", len(got))
	}

	if resp := s.dispatch(Request{Op: OpReclaim, Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("reclaim: %s", resp.Error)
	}
	if left := registryBases(s); len(left) != 1 || left["/n/x"] != "/n/base" {
		t.Errorf("after reclaim registry = %v, want only /n/x", left)
	}
}

// TestOwnerAliasRefusedAtDispatch pins R4-8 at the wire: "TENANT" is a
// case-fold alias of "tenant" on the APFS spool dir, so every owner-bearing
// op refuses it (ClassInvalidOwner) — the two can never coexist as distinct
// rows over one spool.
func TestOwnerAliasRefusedAtDispatch(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})
	if resp := addBridge(t, s, "tenant", "/grp/t.sock", "/up/t.sock", nil); !resp.OK {
		t.Fatalf("lowercase add: %s", resp.Error)
	}
	for _, op := range []Request{
		{Op: OpMount, Base: "/b", Dir: "/m", Owner: "TENANT"},
		{Op: OpAddBridge, Owner: "TENANT", BridgeSocket: "/grp/T.sock", ContentSocket: "/up/T.sock"},
		{Op: OpUnmount, Base: "/b", Dir: "/m", Owner: "TENANT"},
	} {
		resp := s.dispatch(op)
		if resp.OK || resp.ErrClass != ClassInvalidOwner {
			t.Fatalf("%s with owner TENANT = (ok=%v class=%q), want invalid-owner", op.Op, resp.OK, resp.ErrClass)
		}
	}
	s.bridgeMu.Lock()
	n := len(s.bridges)
	s.bridgeMu.Unlock()
	if n != 1 {
		t.Fatalf("bridges = %d rows, want only the lowercase tenant (no alias row over the same spool)", n)
	}
}

func TestCrossOwnerMountIsForeign(t *testing.T) {
	s := newHandlerServer(t, &fakeHost{})
	if resp := s.dispatch(Request{Op: OpMount, Base: "/base", Dir: "/d", Owner: "a"}); !resp.OK {
		t.Fatalf("mount a: %s", resp.Error)
	}
	resp := s.dispatch(Request{Op: OpMount, Base: "/base", Dir: "/d", Owner: "b"})
	if resp.OK || resp.ErrClass != ClassForeignMount {
		t.Errorf("cross-owner mount = (ok=%v, class=%q), want foreign-mount", resp.OK, resp.ErrClass)
	}
}

// TestCrossOwnerUnmountRefused pins the OpUnmount owner misfire guard: a row
// may only be unmounted by its owner, while rowless dirs stay open to any
// owner (carcass teardown must keep working).
func TestCrossOwnerUnmountRefused(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	cases := []struct {
		name      string
		rowOwner  string // "-" = no row at all
		reqOwner  string
		wantOK    bool
		wantClass string
	}{
		{name: "foreign owner refused", rowOwner: "cc-pool", reqOwner: "cc-notes", wantOK: false, wantClass: ClassOwnerMismatch},
		{name: "ownerless request refused at dispatch", rowOwner: "cc-pool", reqOwner: "", wantOK: false, wantClass: ClassInvalidOwner},
		{name: "matching owner allowed", rowOwner: "cc-pool", reqOwner: "cc-pool", wantOK: true},
		{name: "rowless unmounted dir is anyone's OK no-op", rowOwner: "-", reqOwner: "cc-notes", wantOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHost{}
			s := newHandlerServer(t, fake)
			if tc.rowOwner != "-" {
				if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: tc.rowOwner}); !resp.OK {
					t.Fatalf("mount: %s", resp.Error)
				}
			}
			resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: tc.reqOwner})
			if resp.OK != tc.wantOK || resp.ErrClass != tc.wantClass {
				t.Fatalf("unmount = (ok=%v class=%q %q), want (ok=%v class=%q)", resp.OK, resp.ErrClass, resp.Error, tc.wantOK, tc.wantClass)
			}
			_, teardowns := fake.calls()
			if !tc.wantOK {
				// A refusal must not touch the mount.
				if len(teardowns) != 0 {
					t.Fatalf("refused unmount tore down %v", teardowns)
				}
				if got := registryBases(s); !reflect.DeepEqual(got, map[string]string{dir: base}) {
					t.Fatalf("registry after refusal = %v, want the row intact", got)
				}
				if tc.wantClass == ClassOwnerMismatch && !strings.Contains(resp.Error, tc.rowOwner) {
					t.Fatalf("refusal %q does not name the owning consumer %q", resp.Error, tc.rowOwner)
				}
				assertClaimsReleased(t, s, 0)
				return
			}
			if len(registryBases(s)) != 0 {
				t.Fatalf("registry after allowed unmount = %v, want empty", registryBases(s))
			}
		})
	}
}

// TestCrossOwnerRemoveBridgeScopedToOwnRow pins the RemoveBridge side of the
// owner misfire guard: a remove only ever reaches the requester's own
// owner-keyed row — a foreign owner's bridge survives untouched — and a row
// whose recorded owner ever disagreed with its key would be refused, not
// reclaimed.
func TestCrossOwnerRemoveBridgeScopedToOwnRow(t *testing.T) {
	stubStartBridge(t)
	redirectSpool(t)
	s := newHandlerServer(t, &fakeHost{})
	if resp := addBridge(t, s, "cc-pool", "/grp/a.sock", "/up/a.sock", nil); !resp.OK {
		t.Fatalf("add: %s", resp.Error)
	}

	resp := s.dispatch(Request{Op: OpRemoveBridge, Owner: "cc-notes"})
	if !resp.OK || len(resp.Bridges) != 0 {
		t.Fatalf("foreign remove = (ok=%v bridges=%+v), want an empty OK no-op", resp.OK, resp.Bridges)
	}
	s.bridgeMu.Lock()
	_, survived := s.bridges["cc-pool"]
	s.bridgeMu.Unlock()
	if !survived {
		t.Fatal("a foreign owner's RemoveBridge tore down cc-pool's bridge")
	}

	// A row whose owner disagrees with its key (keying drift) is refused.
	s.bridgeMu.Lock()
	s.bridges["cc-notes"] = &bridgeRow{owner: "cc-pool", bindSock: "/grp/x.sock", done: make(chan struct{})}
	s.bridgeMu.Unlock()
	resp = s.dispatch(Request{Op: OpRemoveBridge, Owner: "cc-notes"})
	if resp.OK || resp.ErrClass != ClassOwnerMismatch {
		t.Fatalf("mismatched-row remove = (ok=%v class=%q), want %q", resp.OK, resp.ErrClass, ClassOwnerMismatch)
	}
	s.bridgeMu.Lock()
	delete(s.bridges, "cc-notes")
	s.bridgeMu.Unlock()

	if resp := s.dispatch(Request{Op: OpRemoveBridge, Owner: "cc-pool"}); !resp.OK || len(resp.Bridges) != 0 {
		t.Fatalf("own remove = (ok=%v bridges=%+v), want a clean removal", resp.OK, resp.Bridges)
	}
}

// TestReclaimSkipsCrossOwnerReplacement pins T-1: between Reclaim's snapshot
// and its per-dir claim, owner A's dir can be unmounted and REPLACED by owner
// B; the sweep must revalidate the row under the claim and skip the
// replacement instead of tearing down B's mount and deleting B's row.
func TestReclaimSkipsCrossOwnerReplacement(t *testing.T) {
	const base, first, dir = "/pool/base", "/pool/a", "/pool/z"
	fake := &fakeHost{}
	var s *Server
	replaced := false
	fake.teardownFn = func(_, d string) error {
		if d == first && !replaced {
			replaced = true
			// The sweep snapshotted both of A's dirs and is inside /pool/a's
			// teardown: A unmounts /pool/z and B mounts it before the sweep
			// reaches it.
			if resp := s.dispatch(Request{Op: OpUnmount, Base: base, Dir: dir, Owner: "owner-a"}); !resp.OK {
				t.Errorf("mid-sweep unmount: %s", resp.Error)
			}
			if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: dir, Owner: "owner-b"}); !resp.OK {
				t.Errorf("mid-sweep replacement mount: %s", resp.Error)
			}
		}
		return nil
	}
	s = newHandlerServer(t, fake)
	for _, d := range []string{first, dir} {
		if resp := s.dispatch(Request{Op: OpMount, Base: base, Dir: d, Owner: "owner-a"}); !resp.OK {
			t.Fatalf("mount %s: %s", d, resp.Error)
		}
	}

	resp := s.dispatch(Request{Op: OpReclaim, Owner: "owner-a"})
	if !resp.OK || len(resp.Mounts) != 0 {
		t.Fatalf("reclaim = (ok=%v failed=%v), want a clean skip", resp.OK, resp.Mounts)
	}
	row, ok := s.registered(dir)
	if !ok || row.Owner != "owner-b" {
		t.Fatalf("replacement row = (%+v, %v), want owner-b's mount to survive the stale snapshot", row, ok)
	}
	_, tears := fake.calls()
	want := []hostCall{{base, first}, {base, dir}} // A's sweep + A's own mid-sweep unmount — never B's
	if !reflect.DeepEqual(tears, want) {
		t.Fatalf("teardowns = %v, want %v (B's replacement never torn down)", tears, want)
	}
}

// TestRowlessJournalUnmountEnforcesOwner pins T-2: a journal row without a
// registry row (a lease-deferred replay) is owner-guarded exactly like a
// registry row — a foreign owner can neither tear the dir down nor delete
// the row.
func TestRowlessJournalUnmountEnforcesOwner(t *testing.T) {
	fake := &fakeHost{}
	s, path := newJournaledHandlerServer(t, fake)
	if err := s.journal.putMount(fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}); err != nil {
		t.Fatal(err)
	}

	resp := s.dispatch(Request{Op: OpUnmount, Base: "/b/a", Dir: "/m/a", Owner: "cc-notes"})
	if resp.OK || resp.ErrClass != ClassOwnerMismatch {
		t.Fatalf("foreign rowless unmount = (ok=%v class=%q), want owner-mismatch", resp.OK, resp.ErrClass)
	}
	if dirs := journaledMountDirs(t, path); !reflect.DeepEqual(dirs, []string{"/m/a"}) {
		t.Fatalf("journal after foreign unmount = %v, want the row kept", dirs)
	}
	if _, tears := fake.calls(); len(tears) != 0 {
		t.Fatalf("foreign rowless unmount tore down %v", tears)
	}

	if resp := s.dispatch(Request{Op: OpUnmount, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool"}); !resp.OK {
		t.Fatalf("owning rowless unmount = %s, want OK", resp.Error)
	}
	if dirs := journaledMountDirs(t, path); len(dirs) != 0 {
		t.Fatalf("journal after owning unmount = %v, want empty", dirs)
	}
}

// TestLeasesOwnerScoped pins T-3: OpLeases answers only the requesting
// owner's lease files by default; all:true is the read-only cross-tenant view.
func TestLeasesOwnerScoped(t *testing.T) {
	s := newHandlerServer(t, &fakeHost{})
	hp, err := lease.Acquire(s.LeaseDir, "/pool/a", "cc-pool")
	if err != nil {
		t.Fatal(err)
	}
	defer hp.Close()
	hn, err := lease.Acquire(s.LeaseDir, "/notes/b", "cc-notes")
	if err != nil {
		t.Fatal(err)
	}
	defer hn.Close()

	resp := s.dispatch(Request{Op: OpLeases, Owner: "cc-pool"})
	if !resp.OK || len(resp.Leases) != 1 || resp.Leases[0].Owner != "cc-pool" || resp.Leases[0].Dir != "/pool/a" {
		t.Fatalf("owner-scoped leases = %+v, want only cc-pool's", resp.Leases)
	}
	respAll := s.dispatch(Request{Op: OpLeases, Owner: "cc-pool", All: true})
	if !respAll.OK || len(respAll.Leases) != 2 {
		t.Fatalf("all leases = %+v, want both tenants", respAll.Leases)
	}
}
