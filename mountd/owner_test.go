package mountd

import "testing"

func TestOwnerScopedListAndReclaim(t *testing.T) {
	s := newHandlerServer(&fakeHost{})
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
	if got := list(""); len(got) != 3 {
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
	s := newHandlerServer(&fakeHost{})
	if resp := s.dispatch(Request{Op: OpMount, Base: "/base", Dir: "/d", Owner: "a"}); !resp.OK {
		t.Fatalf("mount a: %s", resp.Error)
	}
	resp := s.dispatch(Request{Op: OpMount, Base: "/base", Dir: "/d", Owner: "b"})
	if resp.OK || resp.ErrClass != ClassForeignMount {
		t.Errorf("cross-owner mount = (ok=%v, class=%q), want foreign-mount", resp.OK, resp.ErrClass)
	}
}

func TestShutdownRefusedAcrossOwners(t *testing.T) {
	multi := newHandlerServer(&fakeHost{})
	multi.dispatch(Request{Op: OpMount, Base: "/ba", Dir: "/a", Owner: "a"})
	multi.dispatch(Request{Op: OpMount, Base: "/bb", Dir: "/b", Owner: "b"})
	if resp := multi.dispatch(Request{Op: OpShutdown}); resp.OK {
		t.Error("shutdown across 2 owners = OK, want refused")
	}

	solo := newHandlerServer(&fakeHost{})
	solo.triggerShutdown = func() {}
	solo.dispatch(Request{Op: OpMount, Base: "/b", Dir: "/d", Owner: "solo"})
	if resp := solo.dispatch(Request{Op: OpShutdown}); !resp.OK {
		t.Errorf("single-owner shutdown = %s, want OK", resp.Error)
	}
}
