package mountd

import (
	"io"
	"log"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/yasyf/fusekit"
)

func TestMountRejectsUnknownCarcassPolicy(t *testing.T) {
	s := newHandlerServer(&fakeHost{})
	cases := []struct {
		name string
		req  Request
	}{
		{name: "mount", req: Request{Op: OpMount, Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", CarcassPolicy: "bogus"}},
		{name: "unmount", req: Request{Op: OpUnmount, Base: "/b/a", Dir: "/m/a", CarcassPolicy: "bogus"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := s.dispatch(tc.req)
			if resp.OK || !strings.Contains(resp.Error, "carcass_policy") {
				t.Fatalf("bogus carcass_policy = (ok=%v, %q), want refusal naming carcass_policy", resp.OK, resp.Error)
			}
		})
	}
}

func TestAddMountCarriesCarcassPolicyOverWireAndJournal(t *testing.T) {
	fake := &fakeHost{}
	sockDir := shortSockDir(t)
	socket := filepath.Join(sockDir, "holder.sock")
	jpath := DefaultJournalPath(socket)
	s, _, _, _ := runServer(t, &Server{Socket: socket, Host: fake, Version: testVersion, Log: log.New(io.Discard, "", 0), JournalPath: jpath})

	cl := &Client{Socket: s.Socket, Owner: "cc-pool"}
	if err := cl.AddMount(fusekit.MountSpec{Base: "/b/d", Dir: "/m/d", Owner: "cc-pool", CarcassPolicy: fusekit.CarcassPolicyDefer}); err != nil {
		t.Fatalf("AddMount: %v", err)
	}
	specs := fake.capturedSpecs()
	if len(specs) != 1 || specs[0].CarcassPolicy != fusekit.CarcassPolicyDefer {
		t.Fatalf("holder-side spec = %+v, want carcass policy %q", specs, fusekit.CarcassPolicyDefer)
	}
	f := readJournalFile(t, jpath)
	if len(f.Mounts) != 1 || f.Mounts[0].CarcassPolicy != fusekit.CarcassPolicyDefer {
		t.Fatalf("journaled mounts = %+v, want carcass policy %q persisted", f.Mounts, fusekit.CarcassPolicyDefer)
	}
}

// TestUnmountThreadsCarcassPolicyToTeardown pins the policy each teardown
// carries: the journaled spec's own declaration wins; only an unjournaled
// carcass takes the requester's asserted policy; absent everywhere stays ""
// (the provider's force default).
func TestUnmountThreadsCarcassPolicyToTeardown(t *testing.T) {
	cases := []struct {
		name      string
		journaled string // policy journaled for /m/a; "" journals no entry
		reqPolicy string
		want      string
	}{
		{name: "journaled defer wins over a force request", journaled: fusekit.CarcassPolicyDefer, reqPolicy: fusekit.CarcassPolicyForce, want: fusekit.CarcassPolicyDefer},
		{name: "journaled force wins over a defer request", journaled: fusekit.CarcassPolicyForce, reqPolicy: fusekit.CarcassPolicyDefer, want: fusekit.CarcassPolicyForce},
		{name: "unjournaled carcass takes the request policy", journaled: "", reqPolicy: fusekit.CarcassPolicyDefer, want: fusekit.CarcassPolicyDefer},
		{name: "absent everywhere stays the force default", journaled: "", reqPolicy: "", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHost{}
			s, _ := newJournaledHandlerServer(t, fake)
			if tc.journaled != "" {
				if err := s.journal.putMount(fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", CarcassPolicy: tc.journaled}); err != nil {
					t.Fatal(err)
				}
			}
			// A rowless mountpoint (carcass): the probe reads mounted, so the
			// handler routes into Teardown.
			setState(fake, func(dir string) bool { return dir == "/m/a" }, func(_, _ string) bool { return false })

			if resp := s.dispatch(Request{Op: OpUnmount, Base: "/b/a", Dir: "/m/a", CarcassPolicy: tc.reqPolicy}); !resp.OK {
				t.Fatalf("unmount: %s", resp.Error)
			}
			if got := fake.capturedTeardownPolicies(); !reflect.DeepEqual(got, []string{tc.want}) {
				t.Fatalf("teardown policies = %v, want [%q]", got, tc.want)
			}
		})
	}
}

// TestSweepThreadsCarcassPolicyToTeardown pins the bulk-teardown half of the
// plumbing: shutdown/reclaim/final sweeps resolve each dir's policy from the
// journal first and the registry row second — never a hardcoded force
// default — so a journal-less holder still honors a spec's defer.
func TestSweepThreadsCarcassPolicyToTeardown(t *testing.T) {
	cases := []struct {
		name      string
		journaled bool
		policy    string
		want      string
	}{
		{name: "journal-less defer row", journaled: false, policy: fusekit.CarcassPolicyDefer, want: fusekit.CarcassPolicyDefer},
		{name: "journal-less force row", journaled: false, policy: fusekit.CarcassPolicyForce, want: fusekit.CarcassPolicyForce},
		{name: "journal-less absent stays the force default", journaled: false, policy: "", want: ""},
		{name: "journaled defer", journaled: true, policy: fusekit.CarcassPolicyDefer, want: fusekit.CarcassPolicyDefer},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeHost{}
			s := newHandlerServer(fake)
			if tc.journaled {
				s, _ = newJournaledHandlerServer(t, fake)
			}
			mountForTest(t, s, fusekit.MountSpec{Base: "/b/a", Dir: "/m/a", Owner: "cc-pool", CarcassPolicy: tc.policy})
			if failed := s.unmountAll(); len(failed) != 0 {
				t.Fatalf("sweep failed dirs = %+v", failed)
			}
			if got := fake.capturedTeardownPolicies(); !reflect.DeepEqual(got, []string{tc.want}) {
				t.Fatalf("sweep teardown policies = %v, want [%q]", got, tc.want)
			}
		})
	}
}

// TestReplayCarcassPolicyGatesForceClear pins the replay half of the
// kernel-panic invariant: a root any tenant journaled CarcassPolicyDefer for
// is NEVER force-cleared on replay; the carcass-confirmed orphan reap still
// covers every root.
func TestReplayCarcassPolicyGatesForceClear(t *testing.T) {
	shrinkReplayRetries(t)
	capture := captureReapSeams(t)
	sockDir := shortSockDir(t)
	socket := filepath.Join(sockDir, "m.sock")
	jpath := DefaultJournalPath(socket)

	seed := newJournal(jpath)
	for _, spec := range []fusekit.MountSpec{
		{Base: "/b/force", Dir: "/m/force", Owner: "cc-pool", CarcassPolicy: fusekit.CarcassPolicyForce},
		{Base: "/b/defer", Dir: "/m/defer", Owner: "cc-pool", CarcassPolicy: fusekit.CarcassPolicyDefer},
		{Base: "/b/legacy", Dir: "/m/legacy", Owner: "cc-pool"}, // absent = force
		// One deferring tenant defers its whole shared root.
		{Base: "/b/t1", Dir: "/mux/t1", Owner: "cc-pool", MuxRoot: "/mux", CarcassPolicy: fusekit.CarcassPolicyForce},
		{Base: "/b/t2", Dir: "/mux/t2", Owner: "cc-pool", MuxRoot: "/mux", CarcassPolicy: fusekit.CarcassPolicyDefer},
	} {
		if err := seed.putMount(spec); err != nil {
			t.Fatal(err)
		}
	}

	fake := &fakeHost{}
	_, cl := startJournaledServer(t, fake, socket, jpath)

	cleared, reaps := capture.snapshot()
	if want := []string{"/m/force", "/m/legacy"}; !reflect.DeepEqual(cleared, want) {
		t.Fatalf("force-cleared roots = %v, want %v — a deferring root must never be cleared", cleared, want)
	}
	wantRoots := []string{"/m/defer", "/m/force", "/m/legacy", "/mux"}
	if len(reaps) != 1 || !reflect.DeepEqual(reaps[0], wantRoots) {
		t.Fatalf("reap calls = %v, want one over every root %v", reaps, wantRoots)
	}
	// The deferring mounts still replayed — the policy gates the force-clear,
	// never the remount.
	gotBases, _ := listTopology(t, cl)
	want := map[string]string{"/m/force": "/b/force", "/m/defer": "/b/defer", "/m/legacy": "/b/legacy", "/mux/t1": "/b/t1", "/mux/t2": "/b/t2"}
	if !reflect.DeepEqual(gotBases, want) {
		t.Fatalf("replayed registry = %v, want %v", gotBases, want)
	}
}
