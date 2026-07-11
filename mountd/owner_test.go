package mountd

import (
	"reflect"
	"strings"
	"testing"
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

// TestCrossOwnerUnmountRefused pins the OpUnmount owner misfire guard: an
// owned row may only be unmounted by its owner, while ownerless rows and
// rowless dirs stay open to any owner (carcass teardown must keep working).
func TestCrossOwnerUnmountRefused(t *testing.T) {
	const base, dir = "/pool/base", "/pool/acct-01"
	cases := []struct {
		name      string
		rowOwner  string // "" = ownerless row; "-" = no row at all
		reqOwner  string
		wantOK    bool
		wantClass string
	}{
		{name: "foreign owner refused", rowOwner: "cc-pool", reqOwner: "cc-notes", wantOK: false, wantClass: ClassOwnerMismatch},
		{name: "ownerless request refused at dispatch", rowOwner: "cc-pool", reqOwner: "", wantOK: false, wantClass: ClassInvalidOwner},
		{name: "matching owner allowed", rowOwner: "cc-pool", reqOwner: "cc-pool", wantOK: true},
		{name: "ownerless row open to any owner", rowOwner: "", reqOwner: "cc-notes", wantOK: true},
		{name: "rowless unmounted dir is anyone's OK no-op", rowOwner: "-", reqOwner: "cc-notes", wantOK: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHost{}
			s := newHandlerServer(t, fake)
			if tc.rowOwner != "-" {
				// An ownerless row can only arise from a legacy journal replay,
				// which calls handleMount directly — the dispatch owner gate
				// refuses an empty wire owner.
				if resp := s.handleMount(Request{Op: OpMount, Base: base, Dir: dir, Owner: tc.rowOwner}); !resp.OK {
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
